"""
Read API for the dashboard.

Thin query layer over ClickHouse (history) and Redis (live counters). No writes
happen here - the stream processor owns all the writes. Everything returns plain
JSON; the React app polls these endpoints.
"""
import json
import os

import clickhouse_connect
import redis.asyncio as aioredis
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Depends
from auth import verify_api_key

#app = FastAPI(title="Pipeline API")
app = FastAPI(
    title="Pipeline Read API",
    dependencies=[Depends(verify_api_key)]
)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "admin")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "admin123")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

redis_client = None
ch = None


@app.on_event("startup")
async def startup():
    global redis_client, ch
    redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)
    ch = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT,
                                       username=CH_USER, password=CH_PASS,
                                       database="pipeline")


@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.aclose()


@app.get("/stats/live")
async def live_stats():
    """Counters straight from Redis - this is the one that needs to be fast."""
    stats = await redis_client.hgetall("stats:live")
    raw = await redis_client.lrange("alerts:recent", 0, 9)
    return {
        "events_processed": int(stats.get("events_processed", 0)),
        "anomalies_detected": int(stats.get("anomalies_detected", 0)),
        "rate_per_sec": float(stats.get("rate_per_sec", 0)),
        "recent_alerts": [json.loads(a) for a in raw],
    }


@app.get("/metrics/events-per-minute")
async def events_per_minute(minutes: int = Query(30, le=1440)):
    rows = ch.query(f"""
        SELECT toStartOfMinute(event_ts) AS m, count(),
               sum(is_anomaly), avg(anomaly_score), avg(sentiment_score)
        FROM pipeline.events
        WHERE event_ts >= now() - INTERVAL {minutes} MINUTE
        GROUP BY m ORDER BY m
    """).result_rows
    return [{"minute": str(r[0]), "total": r[1], "anomalies": r[2],
             "avg_anomaly_score": round(r[3], 4), "avg_sentiment": round(r[4], 4)}
            for r in rows]


@app.get("/metrics/revenue-per-minute")
async def revenue_per_minute(minutes: int = Query(30, le=1440)):
    rows = ch.query(f"""
        SELECT toStartOfMinute(event_ts) AS m, sum(price * quantity), count()
        FROM pipeline.events
        WHERE event_ts >= now() - INTERVAL {minutes} MINUTE AND event_type = 'purchase'
        GROUP BY m ORDER BY m
    """).result_rows
    return [{"minute": str(r[0]), "revenue": round(r[1] or 0, 2), "purchases": r[2]}
            for r in rows]


@app.get("/metrics/intent-distribution")
async def intent_distribution(minutes: int = Query(60, le=1440)):
    rows = ch.query(f"""
        SELECT intent_label, count() FROM pipeline.events
        WHERE event_ts >= now() - INTERVAL {minutes} MINUTE
        GROUP BY intent_label ORDER BY count() DESC
    """).result_rows
    return [{"intent": r[0], "count": r[1]} for r in rows]


@app.get("/metrics/event-type-breakdown")
async def event_type_breakdown(minutes: int = Query(60, le=1440)):
    rows = ch.query(f"""
        SELECT event_type, count(), avg(anomaly_score) FROM pipeline.events
        WHERE event_ts >= now() - INTERVAL {minutes} MINUTE
        GROUP BY event_type ORDER BY count() DESC
    """).result_rows
    return [{"event_type": r[0], "count": r[1], "avg_anomaly": round(r[2], 4)} for r in rows]


@app.get("/metrics/country-heatmap")
async def country_heatmap(minutes: int = Query(60, le=1440)):
    rows = ch.query(f"""
        SELECT country, count(), sum(is_anomaly) FROM pipeline.events
        WHERE event_ts >= now() - INTERVAL {minutes} MINUTE
        GROUP BY country ORDER BY count() DESC LIMIT 20
    """).result_rows
    return [{"country": r[0], "events": r[1], "anomalies": r[2]} for r in rows]


@app.get("/anomalies/recent")
async def recent_anomalies(limit: int = Query(50, le=200),
                           min_score: float = Query(0.65, ge=0, le=1)):
    rows = ch.query(f"""
        SELECT event_id, session_id, user_id, event_type, country,
               anomaly_score, anomaly_reason, event_ts
        FROM pipeline.events
        WHERE is_anomaly = 1 AND anomaly_score >= {min_score}
          AND event_ts >= now() - INTERVAL 24 HOUR
        ORDER BY event_ts DESC LIMIT {limit}
    """).result_rows
    return [{"event_id": r[0], "session_id": r[1], "user_id": r[2],
             "event_type": r[3], "country": r[4], "anomaly_score": round(r[5], 4),
             "reason": r[6], "event_ts": str(r[7])} for r in rows]


@app.get("/anomalies/alerts")
async def recent_alerts(limit: int = Query(20, le=100)):
    rows = ch.query(f"""
        SELECT alert_id, alert_type, anomaly_score, reason,
               session_id, user_id, alerted_at
        FROM pipeline.anomaly_alerts ORDER BY alerted_at DESC LIMIT {limit}
    """).result_rows
    return [{"alert_id": r[0], "alert_type": r[1], "score": round(r[2], 4),
             "reason": r[3], "session_id": r[4], "user_id": r[5],
             "alerted_at": str(r[6])} for r in rows]


@app.get("/session/{session_id}")
async def session_detail(session_id: str):
    rows = ch.query(f"""
        SELECT event_type, intent_label, sentiment_score, anomaly_score,
               is_anomaly, event_ts, page, product_name, price, country, device
        FROM pipeline.events WHERE session_id = '{session_id}'
        ORDER BY event_ts LIMIT 100
    """).result_rows
    if not rows:
        raise HTTPException(404, "session not found")
    events = [{"event_type": r[0], "intent": r[1], "sentiment": round(r[2], 3),
               "anomaly_score": round(r[3], 3), "is_anomaly": bool(r[4]),
               "ts": str(r[5]), "page": r[6], "product": r[7], "price": r[8],
               "country": r[9], "device": r[10]} for r in rows]
    live = await redis_client.hgetall(f"session:{session_id}")
    return {"session_id": session_id, "event_count": len(events),
            "events": events, "live_features": live}



@app.get("/metrics/model-precision")
async def model_precision(minutes: int = Query(60, le=1440)):
    """Real precision/recall by comparing model's is_anomaly against the
    producer's is_anomaly_truth ground-truth label."""
    rows = ch.query(f"""
        SELECT
            countIf(is_anomaly = 1 AND is_anomaly_truth = 1) AS tp,
            countIf(is_anomaly = 1 AND is_anomaly_truth = 0) AS fp,
            countIf(is_anomaly = 0 AND is_anomaly_truth = 0) AS tn,
            countIf(is_anomaly = 0 AND is_anomaly_truth = 1) AS fn
        FROM pipeline.events
        WHERE event_ts >= now() - INTERVAL {minutes} MINUTE
    """).result_rows
    tp, fp, tn, fn = rows[0] if rows else (0, 0, 0, 0)
    precision = tp / (tp + fp) if (tp + fp) else None
    recall = tp / (tp + fn) if (tp + fn) else None
    f1 = (2 * precision * recall / (precision + recall)
          if precision and recall else None)
    return {"true_positive": tp, "false_positive": fp, "true_negative": tn,
            "false_negative": fn, "precision": precision, "recall": recall, "f1": f1}


@app.get("/metrics/evaluation-history")
async def evaluation_history(limit: int = Query(30, le=200)):
    """Historical evaluation snapshots written by the Airflow DAG."""
    rows = ch.query(f"""
        SELECT eval_ts, window_hours, precision, recall, f1,
               true_positive, false_positive, false_negative
        FROM pipeline.model_evaluation ORDER BY eval_ts DESC LIMIT {limit}
    """).result_rows
    return [{"eval_ts": str(r[0]), "window_hours": r[1], "precision": round(r[2],4),
             "recall": round(r[3],4), "f1": round(r[4],4), "tp": r[5], "fp": r[6], "fn": r[7]}
            for r in rows]


@app.get("/health")
async def health():
    try:
        ch.query("SELECT 1")
        ch_ok = True
    except Exception:
        ch_ok = False
    try:
        await redis_client.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {"api": "ok", "clickhouse": "ok" if ch_ok else "error",
            "redis": "ok" if redis_ok else "error"}
