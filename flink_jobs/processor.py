"""
Stream processor - core of the pipeline.

Reads Kafka batches, enriches via the AI service, and fans results out to:
  - ClickHouse (analytics)        - Redis (live counters + feed)
  - Kafka enriched/alert topics   - S3 (cold Parquet, every N seconds)

Adds a stateful 60-second sliding window over client_ip so it can flag bot
velocity (many events from one IP) - something per-event scoring can't catch.
"""
import asyncio
import json
import logging
import os
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone

import aiohttp
import clickhouse_connect
import redis.asyncio as aioredis
from confluent_kafka import Consumer, KafkaError, Producer

from s3_sink import S3Sink

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("processor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = "clickstream.raw"
TOPIC_ENRICHED = "clickstream.enriched"
TOPIC_ALERTS = "clickstream.alerts"
TOPIC_DLQ = "clickstream.dlq"
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "admin")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "admin123")
AI_URL = os.getenv("AI_SERVICE_URL", "http://localhost:8001")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
FLUSH_INTERVAL_S = float(os.getenv("FLUSH_INTERVAL_S", "2.0"))
ANOMALY_THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "0.65"))
BOT_WINDOW_S = int(os.getenv("BOT_WINDOW_S", "60"))
BOT_THRESHOLD = int(os.getenv("BOT_THRESHOLD", "40"))   # events/IP/window => bot


def parse_dt(s):
    if isinstance(s, datetime):
        return s
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


class IPVelocityWindow:
    """Sliding 60s window of event timestamps per IP. If an IP exceeds the
    threshold within the window, every further event from it is bot_velocity.
    This is the stateful streaming feature per-event models can't do."""
    def __init__(self, window_s=BOT_WINDOW_S, threshold=BOT_THRESHOLD):
        self.window = window_s
        self.threshold = threshold
        self._hits = defaultdict(deque)

    def record_and_check(self, ip, now=None):
        if not ip:
            return False, 0
        now = now or time.time()
        dq = self._hits[ip]
        dq.append(now)
        cutoff = now - self.window
        while dq and dq[0] < cutoff:
            dq.popleft()
        count = len(dq)
        # opportunistic cleanup
        if len(self._hits) > 20000:
            for k in [k for k, v in list(self._hits.items()) if not v or v[-1] < cutoff]:
                self._hits.pop(k, None)
        return count >= self.threshold, count


class SessionTracker:
    def __init__(self, max_sessions=5000, context_size=10):
        self._store = defaultdict(lambda: deque(maxlen=context_size))
        self._max = max_sessions

    def context(self, sid):
        return list(self._store[sid])

    def add(self, sid, event):
        self._store[sid].append({"event_type": event.get("event_type"),
                                 "page": event.get("page"), "event_ts": event.get("event_ts")})
        if len(self._store) > self._max:
            self._store.pop(next(iter(self._store)))


class ClickHouseWriter:
    EVENT_COLS = ["event_id","session_id","user_id","event_type","page","product_id",
                  "product_name","price","quantity","device","country","brand","category",
                  "client_ip","intent_label","sentiment_score","anomaly_score","is_anomaly",
                  "anomaly_reason","is_anomaly_truth","anomaly_kind_truth","event_ts","processed_ts"]
    ALERT_COLS = ["alert_id","event_id","session_id","user_id","alert_type",
                  "anomaly_score","reason","event_ts","alerted_at"]

    def __init__(self):
        self.client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT,
                        username=CH_USER, password=CH_PASS, database="pipeline")
        self._events, self._alerts = [], []
        self._last_flush = time.time()

    def add_event(self, e): self._events.append(e)
    def add_alert(self, a): self._alerts.append(a)
    def ready(self):
        return len(self._events) >= 500 or (self._events and time.time()-self._last_flush >= FLUSH_INTERVAL_S)

    def flush(self):
        if self._events: self._flush_events()
        if self._alerts: self._flush_alerts()
        self._last_flush = time.time()

    def _flush_events(self):
        rows = [[
            e.get("event_id") or "", e.get("session_id") or "", e.get("user_id") or "",
            e.get("event_type") or "", e.get("page") or "", e.get("product_id"),
            e.get("product_name"), e.get("price"), e.get("quantity"),
            e.get("device") or "", e.get("country") or "", e.get("brand") or "",
            e.get("category") or "", e.get("client_ip") or "",
            e.get("intent_label") or "browsing", float(e.get("sentiment_score") or 0.0),
            float(e.get("anomaly_score") or 0.0), int(bool(e.get("is_anomaly"))),
            e.get("anomaly_reason"), int(e.get("is_anomaly_truth") or 0),
            e.get("anomaly_kind_truth") or "", parse_dt(e.get("event_ts")),
            parse_dt(e.get("processed_ts")),
        ] for e in self._events]
        try:
            self.client.insert("pipeline.events", rows, column_names=self.EVENT_COLS)
        except Exception as ex:
            log.error("clickhouse event insert failed: %s", ex)
        finally:
            self._events.clear()

    def _flush_alerts(self):
        rows = [[
            a.get("alert_id") or "", a.get("event_id") or "", a.get("session_id") or "",
            a.get("user_id") or "", a.get("alert_type") or "", float(a.get("anomaly_score") or 0.0),
            a.get("reason") or "", parse_dt(a.get("event_ts")), parse_dt(a.get("alerted_at")),
        ] for a in self._alerts]
        try:
            self.client.insert("pipeline.anomaly_alerts", rows, column_names=self.ALERT_COLS)
        except Exception as ex:
            log.error("clickhouse alert insert failed: %s", ex)
        finally:
            self._alerts.clear()


def alert_type_for(event, reason):
    if reason and "bot" in reason: return "bot_velocity"
    if reason and "quantity" in reason: return "cart_fraud"
    if reason and "price" in reason: return "price_manipulation"
    if reason and "country" in reason: return "geo_spike"
    if event.get("event_type") == "session_drop": return "churn_risk"
    return "statistical_anomaly"


async def enrich_batch(events, tracker, http):
    payload = [{**e, "session_context": tracker.context(e["session_id"])} for e in events]
    try:
        async with http.post(f"{AI_URL}/enrich/batch", json=payload,
                              timeout=aiohttp.ClientTimeout(total=5.0)) as resp:
            resp.raise_for_status()
            return {r["event_id"]: r for r in await resp.json()}
    except Exception as e:
        log.warning("ai service error: %s - using defaults", e)
        return {}


async def main():
    log.info("waiting for dependencies...")
    await asyncio.sleep(20)

    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    writer = ClickHouseWriter()
    tracker = SessionTracker()
    ipwin = IPVelocityWindow()
    s3 = S3Sink()   # no-op if AWS creds aren't set

    consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "stream-processor-v1",
                         "auto.offset.reset": "latest", "enable.auto.commit": False,
                         "max.poll.interval.ms": 60000})
    consumer.subscribe([TOPIC_RAW])
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 10})

    log.info("processor running (batch=%d flush=%.1fs bot_window=%ds/%d)",
             BATCH_SIZE, FLUSH_INTERVAL_S, BOT_WINDOW_S, BOT_THRESHOLD)
    batch, processed, anomalies, start = [], 0, 0, time.time()

    async with aiohttp.ClientSession() as http:
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg is not None and not msg.error():
                try:
                    event = json.loads(msg.value().decode())
                    tracker.add(event["session_id"], event)
                    batch.append(event)
                except Exception as e:
                    log.warning("bad message: %s", e)
                    producer.produce(TOPIC_DLQ, msg.value())
            elif msg is not None and msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("kafka: %s", msg.error())

            if len(batch) >= BATCH_SIZE:
                enriched = await enrich_batch(batch, tracker, http)
                now = datetime.now(timezone.utc).isoformat()
                for event in batch:
                    r = enriched.get(event["event_id"], {})
                    score = r.get("anomaly_score", 0.0)
                    is_anom = r.get("is_anomaly", False)
                    reason = r.get("anomaly_reason")

                    # stateful windowed bot detection overrides per-event score
                    is_bot, ip_count = ipwin.record_and_check(event.get("client_ip"))
                    if is_bot:
                        is_anom, score = True, max(score, 0.9)
                        reason = f"bot velocity: {ip_count} events/min from one IP"

                    row = {**event, "intent_label": r.get("intent_label","browsing"),
                           "sentiment_score": r.get("sentiment_score",0.0),
                           "anomaly_score": score, "is_anomaly": is_anom and score >= ANOMALY_THRESHOLD,
                           "anomaly_reason": reason, "processed_ts": now}
                    writer.add_event(row)
                    s3.add(row)
                    producer.produce(TOPIC_ENRICHED, key=event["session_id"], value=json.dumps(row).encode())

                    if is_anom and score >= ANOMALY_THRESHOLD:
                        anomalies += 1
                        atype = alert_type_for(event, reason)
                        alert = {"alert_id": str(uuid.uuid4()), "event_id": event["event_id"],
                                 "session_id": event["session_id"], "user_id": event["user_id"],
                                 "alert_type": atype, "anomaly_score": score,
                                 "reason": reason or "unknown", "event_ts": event["event_ts"],
                                 "alerted_at": now}
                        writer.add_alert(alert)
                        producer.produce(TOPIC_ALERTS, key=event["session_id"], value=json.dumps(alert).encode())
                        await redis.lpush("alerts:recent", json.dumps({
                            "type": atype, "score": round(score,3), "reason": reason,
                            "session": event["session_id"][:8], "ts": now}))
                        await redis.ltrim("alerts:recent", 0, 99)

                consumer.commit(asynchronous=False)
                processed += len(batch)
                batch.clear()
                producer.poll(0)

            if writer.ready():
                writer.flush()
                s3.maybe_flush()
                elapsed = time.time() - start
                log.info("processed=%d anomalies=%d rate=%.0f/s", processed, anomalies, processed/elapsed)
                await redis.hset("stats:live", mapping={"events_processed": processed,
                    "anomalies_detected": anomalies, "rate_per_sec": round(processed/elapsed,1)})

            await asyncio.sleep(0)


if __name__ == "__main__":
    asyncio.run(main())
