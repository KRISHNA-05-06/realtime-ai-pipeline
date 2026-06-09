"""
DAG: hourly_session_aggregates
Runs hourly. Materializes session-level aggregates into
pipeline.session_aggregates so the dashboard / BI tools query a small
pre-computed table instead of scanning raw events.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

CH = dict(host="clickhouse", port=8123, username="admin", password="admin123", database="pipeline")


def aggregate(**_):
    import clickhouse_connect
    c = clickhouse_connect.get_client(**CH)
    c.command("""
        INSERT INTO pipeline.session_aggregates
        (session_id, event_count, cart_value, converted, dominant_intent,
         max_anomaly, country, agg_date)
        SELECT session_id, count() AS event_count,
               sum(if(event_type='add_to_cart', price*quantity, 0)) AS cart_value,
               max(event_type='purchase') AS converted,
               topK(1)(intent_label)[1] AS dominant_intent,
               max(anomaly_score) AS max_anomaly,
               any(country) AS country, today() AS agg_date
        FROM pipeline.events
        WHERE event_ts >= now() - INTERVAL 1 HOUR
        GROUP BY session_id
    """)
    n = c.query("SELECT count() FROM pipeline.session_aggregates WHERE agg_date = today()").result_rows[0][0]
    print(f"session_aggregates now holds {n} rows for today")


with DAG("hourly_session_aggregates",
         start_date=datetime(2024,1,1), schedule="@hourly",
         catchup=False, default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
         tags=["analytics"]) as dag:
    PythonOperator(task_id="aggregate", python_callable=aggregate)
