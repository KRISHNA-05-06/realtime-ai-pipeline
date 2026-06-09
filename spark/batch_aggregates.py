"""
PySpark daily batch job.

Reads yesterday's enriched events from the S3 cold store (Parquet), computes
session-level and category-level aggregates, and writes a daily summary back.
This closes the Lambda architecture loop: the stream path serves real-time,
this batch path does heavier historical reprocessing on the full dataset.

Run locally:
    spark-submit spark/batch_aggregates.py --date 2026-06-08

In production this is triggered by an Airflow SparkSubmitOperator.
"""
import argparse
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(run_date: str, s3_bucket: str):
    spark = (SparkSession.builder
             .appName("daily-session-aggregates")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

    src = f"s3a://{s3_bucket}/events/dt={run_date}/*.parquet"
    print(f"reading {src}")
    df = spark.read.parquet(src)

    # session-level rollup
    sessions = (df.groupBy("session_id", "country")
                .agg(F.count("*").alias("event_count"),
                     F.sum(F.when(F.col("event_type") == "add_to_cart",
                                  F.col("price") * F.col("quantity")).otherwise(0)).alias("cart_value"),
                     F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("converted"),
                     F.max("anomaly_score").alias("max_anomaly")))

    # category-level anomaly rate
    categories = (df.groupBy("category")
                  .agg(F.count("*").alias("events"),
                       F.avg("anomaly_score").alias("avg_anomaly"),
                       F.sum("is_anomaly").alias("anomalies")))

    out_sessions = f"s3a://{s3_bucket}/aggregates/sessions/dt={run_date}/"
    out_categories = f"s3a://{s3_bucket}/aggregates/categories/dt={run_date}/"
    sessions.write.mode("overwrite").parquet(out_sessions)
    categories.write.mode("overwrite").parquet(out_categories)

    print(f"wrote session aggregates -> {out_sessions}")
    print(f"wrote category aggregates -> {out_categories}")
    print(f"sessions: {sessions.count()}, categories: {categories.count()}")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=str(date.today() - timedelta(days=1)))
    ap.add_argument("--bucket", default="your-pipeline-bucket")
    a = ap.parse_args()
    main(a.date, a.bucket)
