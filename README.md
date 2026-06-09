# Real-Time AI Event Intelligence Pipeline

An end-to-end, production-style streaming data platform. It ingests e-commerce
clickstream events, enriches each one in real time with an AI intent label and a
hybrid anomaly score, persists to hot (ClickHouse) and cold (S3) storage,
orchestrates retraining and evaluation with Airflow, and surfaces everything on a
live dashboard. The anomaly model is trained on 453K real events from the REES46
public dataset and evaluated against ground-truth labels.

## Architecture

```
                                 +----------------------+
 producer --> Kafka (raw) -----> | stream processor     |
 (50 ev/s, 5% anomalies,         | - async consumer     |
  ground-truth labels,           | - 60s IP window      |--> Kafka (enriched, alerts)
  client IP)                     | - calls AI service   |
                                 +----------+-----------+
                                            |
        +-----------------+-----------------+-----------------+
        v                 v                 v                 v
   ClickHouse          Redis             S3 (Parquet)     AI service
   (hot OLAP,          (live counters,   (cold storage,   (Isolation Forest
    90d TTL)            alert feed)       Lambda cold path) + rules + LLM intent)
        |                 |
        +--------+--------+
                 v
            FastAPI  --> Dashboard (Chart.js) + Grafana

   Airflow DAGs (orchestration):
     - evaluate_anomaly_model    (hourly)  -> precision/recall vs ground truth
     - hourly_session_aggregates (hourly)  -> pre-computed rollups
     - retrain_anomaly_model     (weekly)  -> retrain on rolling 30d, hot-reload

   Spark batch job: daily S3 Parquet -> session/category aggregates (Lambda batch path)
   dbt: staging + marts (conversion funnel, category anomaly rate)
```

## What's included

| Layer | Technology | Why |
|-------|-----------|-----|
| Event bus | Apache Kafka | Durable, replayable streaming with offset tracking and a DLQ |
| Stream processing | Python asyncio | Concurrent I/O; 60s sliding-window bot detection (stateful) |
| Anomaly detection | scikit-learn Isolation Forest + hard rules | Hybrid: rules for obvious fraud, model for subtle outliers |
| Intent classification | LLM (Claude/GPT) or mock | Real-time shopper intent labels, Redis-cached |
| Hot storage | ClickHouse | Columnar OLAP for fast analytical queries + materialized views |
| Feature store / counters | Redis | Sub-ms live metrics and the alert feed |
| Cold storage | AWS S3 (Parquet) | Long-term retention, Lambda architecture cold path |
| Orchestration | Apache Airflow | Scheduled retraining, evaluation, aggregation |
| Batch | PySpark | Daily heavy reprocessing of the S3 data lake |
| Transformations | dbt | SQL models: staging views + analytics marts |
| Serving | FastAPI | Read API over ClickHouse + Redis |
| Dashboards | Chart.js + Grafana | Live observability |
| Infra | Docker Compose | One-command stack, prod Kafka overlay for RF=3 |

## Quick start

```bash
cp .env.example .env
docker compose up -d --build
```

Wait ~60 seconds (ClickHouse + Airflow take time to initialize), then open:

| Service | URL |
|---------|-----|
| Dashboard | http://localhost:3001 |
| API docs | http://localhost:8000/docs |
| Airflow | http://localhost:8080 (admin/admin) |
| Kafka UI | http://localhost:8090 |
| Grafana | http://localhost:3000 (admin/admin) |
| AI service | http://localhost:8001/docs |

## How each former limitation is now resolved

| Original limitation | Resolution |
|--------------------|-----------|
| Live data simulated | Producer documented as a stand-in; swap in real click tracking that writes to Kafka. Architecture unchanged. |
| No formal evaluation | Producer emits `is_anomaly_truth`; `evaluate_anomaly_model` DAG computes precision/recall/F1 hourly into `model_evaluation`; live precision shows on the dashboard; notebook in `notebooks/`. |
| Per-event scoring only | Stream processor now keeps a 60-second sliding window per client IP and flags `bot_velocity` when an IP exceeds the threshold - stateful streaming the per-event model can't do. |
| Frozen model snapshot | `retrain_anomaly_model` DAG retrains weekly on the rolling 30-day ClickHouse window and hot-reloads the ai-service via `/model/reload`. |
| No cloud storage | `S3Sink` writes enriched events as partitioned Parquet to S3 every 60s (cold path). No-op without AWS creds so it still runs offline. |
| No orchestration | Apache Airflow with three scheduled DAGs (evaluate, aggregate, retrain). |
| Single-node Kafka | `docker-compose.prod.yml` overlay runs 3 brokers, RF=3, min in-sync 2 for fault tolerance. |

## Enabling the cloud / batch paths

S3 cold storage: set `S3_BUCKET` and AWS creds in `.env`, then restart the stream
processor. Events start landing as Parquet under `s3://<bucket>/events/dt=YYYY-MM-DD/`.

Spark batch job (after S3 is populated):
```bash
spark-submit spark/batch_aggregates.py --date 2026-06-08 --bucket your-bucket
```

dbt models:
```bash
cd dbt && dbt run    # builds stg_events, conversion_funnel, category_anomaly_rate
```

Production Kafka cluster (3 brokers):
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## Repo layout

```
producer/        clickstream generator (ground-truth labels, client IP)
flink_jobs/      async stream processor + s3_sink (windowed bot detection)
ai_service/      Isolation Forest + rules + LLM intent (FastAPI)
api/             read API incl. /metrics/model-precision
dashboard/       static Chart.js dashboard
airflow/dags/    evaluate / aggregate / retrain DAGs
spark/           PySpark daily batch job
dbt/             staging + marts SQL models
notebooks/       model evaluation notebook
infra/           ClickHouse schema + Grafana provisioning
```
