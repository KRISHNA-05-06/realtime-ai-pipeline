-- Anomaly rate per product category, for fraud monitoring.
SELECT
    category,
    count()                              AS total_events,
    sum(is_anomaly)                      AS flagged,
    round(sum(is_anomaly) / count(), 4)  AS anomaly_rate,
    avg(anomaly_score)                   AS avg_score
FROM {{ ref('stg_events') }}
GROUP BY category
HAVING total_events > 100
ORDER BY anomaly_rate DESC
