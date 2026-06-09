-- Cleaned, typed view over raw enriched events.
-- Drops malformed rows and normalizes nulls.
SELECT
    event_id,
    session_id,
    user_id,
    event_type,
    coalesce(brand, 'unknown')       AS brand,
    coalesce(category, 'unknown')    AS category,
    coalesce(price, 0)               AS price,
    coalesce(quantity, 0)            AS quantity,
    country,
    intent_label,
    anomaly_score,
    is_anomaly,
    is_anomaly_truth,
    event_ts
FROM pipeline.events
WHERE event_id != ''
