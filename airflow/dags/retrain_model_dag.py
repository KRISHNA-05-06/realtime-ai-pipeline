"""
DAG: retrain_anomaly_model
Runs weekly. Pulls the last 30 days of real events from ClickHouse, retrains
the Isolation Forest on that rolling window, and writes a fresh model artifact
to the shared volume the ai-service reads from. Solves the "frozen snapshot"
limitation - the model now adapts to traffic that has actually flowed through
the pipeline instead of being stuck on the Nov-2019 training snapshot.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

CH = dict(host="clickhouse", port=8123, username="admin", password="admin123", database="pipeline")
MODEL_OUT = "/opt/airflow/model/isolation_forest_real.joblib"
SAMPLE = 200_000


def retrain(**_):
    import clickhouse_connect, numpy as np, joblib, os
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import LabelEncoder

    c = clickhouse_connect.get_client(**CH)
    rows = c.query(f"""
        SELECT event_type, brand, category, price,
               toHour(event_ts) AS hour, 1 AS session_pos
        FROM pipeline.events
        WHERE event_ts >= now() - INTERVAL 30 DAY AND price > 0
        LIMIT {SAMPLE}
    """).result_rows
    if len(rows) < 1000:
        print(f"only {len(rows)} rows - skipping retrain, keeping current model")
        return

    ets   = [r[0] for r in rows]
    brands= [(r[1] or "unknown") for r in rows]
    cats  = [(r[2] or "unknown") for r in rows]
    enc = {"event_type": LabelEncoder().fit(ets + ["view","cart","remove_from_cart","purchase"]),
           "brand": LabelEncoder().fit(brands + ["other"]),
           "category": LabelEncoder().fit(cats + ["other"])}
    top_brands = list({b for b in brands})[:50]
    top_cats = list({c2 for c2 in cats})[:50]
    X = np.array([[
        enc["event_type"].transform([r[0]])[0],
        enc["brand"].transform([r[1] or "unknown"])[0],
        enc["category"].transform([r[2] or "unknown"])[0],
        min(float(r[3] or 0), 5000), float(r[4]), float(r[5]),
    ] for r in rows])
    model = IsolationForest(n_estimators=200, contamination=0.02,
                            max_samples=256, random_state=42, n_jobs=-1).fit(X)
    os.makedirs(os.path.dirname(MODEL_OUT), exist_ok=True)
    joblib.dump({"model": model, "encoders": enc, "top_brands": top_brands,
                 "top_categories": top_cats,
                 "feature_names": ["event_type","brand","category","price","hour","session_pos"],
                 "training_size": len(X),
                 "source": f"ClickHouse rolling 30d ({len(X)} events, retrained {datetime.utcnow():%Y-%m-%d})"},
                MODEL_OUT)
    print(f"retrained on {len(X)} events -> {MODEL_OUT}")
    # hot-swap the model in the running ai-service
    try:
        import urllib.request
        urllib.request.urlopen("http://ai-service:8001/model/reload", data=b"", timeout=10)
        print("triggered ai-service model reload")
    except Exception as e:
        print(f"reload trigger failed (ai-service will pick it up on next restart): {e}")


with DAG("retrain_anomaly_model",
         start_date=datetime(2024,1,1), schedule="@weekly",
         catchup=False, default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
         tags=["ml","training"]) as dag:
    PythonOperator(task_id="retrain", python_callable=retrain)
