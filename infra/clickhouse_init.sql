CREATE DATABASE IF NOT EXISTS pipeline;

-- Main fact table: every enriched event
CREATE TABLE IF NOT EXISTS pipeline.events
(
    event_id        String,
    session_id      String,
    user_id         String,
    event_type      LowCardinality(String),
    page            String,
    product_id      Nullable(String),
    product_name    Nullable(String),
    price           Nullable(Float64),
    quantity        Nullable(UInt16),
    device          LowCardinality(String),
    country         LowCardinality(String),
    brand           LowCardinality(String) DEFAULT '',
    category        LowCardinality(String) DEFAULT '',
    client_ip       String DEFAULT '',
    intent_label    LowCardinality(String),
    sentiment_score Float32,
    anomaly_score   Float32,
    is_anomaly      UInt8,
    anomaly_reason  Nullable(String),
    -- ground truth from the producer, used for model evaluation
    is_anomaly_truth   UInt8 DEFAULT 0,
    anomaly_kind_truth LowCardinality(String) DEFAULT '',
    event_ts        DateTime64(3, 'UTC'),
    processed_ts    DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (event_ts, session_id, event_id)
TTL toDateTime(event_ts) + INTERVAL 90 DAY;

-- Anomaly alerts log
CREATE TABLE IF NOT EXISTS pipeline.anomaly_alerts
(
    alert_id        String,
    event_id        String,
    session_id      String,
    user_id         String,
    alert_type      LowCardinality(String),
    anomaly_score   Float32,
    reason          String,
    event_ts        DateTime64(3, 'UTC'),
    alerted_at      DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
ORDER BY alerted_at;

-- Daily session aggregates written back by the Spark batch job
CREATE TABLE IF NOT EXISTS pipeline.session_aggregates
(
    session_id      String,
    event_count     UInt32,
    cart_value      Float64,
    converted       UInt8,
    dominant_intent LowCardinality(String),
    max_anomaly     Float32,
    country         LowCardinality(String),
    agg_date        Date,
    computed_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (agg_date, session_id);

-- Model evaluation results written by the Airflow evaluation DAG
CREATE TABLE IF NOT EXISTS pipeline.model_evaluation
(
    eval_ts        DateTime DEFAULT now(),
    window_hours   UInt16,
    true_positive  UInt32,
    false_positive UInt32,
    true_negative  UInt32,
    false_negative UInt32,
    precision      Float32,
    recall         Float32,
    f1             Float32
)
ENGINE = MergeTree()
ORDER BY eval_ts;

-- 1-minute rollup, auto-computed on insert
CREATE MATERIALIZED VIEW IF NOT EXISTS pipeline.events_1min
ENGINE = SummingMergeTree()
ORDER BY (minute, event_type, country)
POPULATE
AS SELECT
    toStartOfMinute(event_ts) AS minute,
    event_type, country,
    count() AS event_count,
    sum(price * quantity) AS revenue,
    sum(is_anomaly) AS anomaly_count
FROM pipeline.events
GROUP BY minute, event_type, country;
