-- Daily conversion funnel by country: views -> carts -> purchases.
SELECT
    toDate(event_ts)                                      AS day,
    country,
    countIf(event_type = 'page_view')                     AS views,
    countIf(event_type = 'add_to_cart')                   AS carts,
    countIf(event_type = 'purchase')                      AS purchases,
    round(countIf(event_type='purchase')
          / nullIf(countIf(event_type='page_view'), 0), 4) AS view_to_purchase_rate
FROM {{ ref('stg_events') }}
GROUP BY day, country
ORDER BY day DESC, views DESC
