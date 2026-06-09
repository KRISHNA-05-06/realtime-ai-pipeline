"""
DAG: evaluate_anomaly_model
Runs hourly. Compares the model's is_anomaly flag against the producer's
ground-truth label over the last 24h, computes precision/recall/F1, and
writes the result to pipeline.model_evaluation. This is what turns the
project's "no formal evaluation" limitation into real, tracked metrics.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

CH = dict(host="clickhouse", port=8123, username="admin", password="admin123", database="pipeline")
WINDOW_HOURS = 1


def evaluate(**_):
    import clickhouse_connect
    c = clickhouse_connect.get_client(**CH)
    tp, fp, tn, fn = c.query(f"""
        SELECT countIf(is_anomaly=1 AND is_anomaly_truth=1),
               countIf(is_anomaly=1 AND is_anomaly_truth=0),
               countIf(is_anomaly=0 AND is_anomaly_truth=0),
               countIf(is_anomaly=0 AND is_anomaly_truth=1)
        FROM pipeline.events
        WHERE event_ts >= now() - INTERVAL {WINDOW_HOURS} HOUR
    """).result_rows[0]
    precision = tp/(tp+fp) if (tp+fp) else 0.0
    recall = tp/(tp+fn) if (tp+fn) else 0.0
    f1 = 2*precision*recall/(precision+recall) if (precision+recall) else 0.0
    c.insert("pipeline.model_evaluation",
             [[datetime.utcnow(), WINDOW_HOURS, tp, fp, tn, fn, precision, recall, f1]],
             column_names=["eval_ts","window_hours","true_positive","false_positive",
                           "true_negative","false_negative","precision","recall","f1"])
    print(f"precision={precision:.3f} recall={recall:.3f} f1={f1:.3f} (tp={tp} fp={fp} fn={fn})")


with DAG("evaluate_anomaly_model",
         start_date=datetime(2024,1,1), schedule="@hourly",
         catchup=False, default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
         tags=["ml","evaluation"]) as dag:
    PythonOperator(task_id="evaluate", python_callable=evaluate)
