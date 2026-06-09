"""
Enrichment service.

Two jobs:
  1. tag each event with a shopper "intent" label (browsing, purchase_ready, etc.)
  2. give each event an anomaly score using an Isolation Forest

Intent can come from a real LLM (OpenAI or Anthropic) or a deterministic
fallback so the whole thing runs offline with no API key. The stream processor
calls /enrich/batch.
"""
import hashlib
import json
import os
import time
import asyncio
import logging

import numpy as np
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder
import joblib

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ai-service")

app = FastAPI(title="AI Enrichment Service")

AI_PROVIDER = os.getenv("AI_PROVIDER", "mock")   # mock | openai | anthropic
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
MODEL_PATH = "/app/model/isolation_forest.joblib"
CACHE_TTL = 300  # seconds - same event shape gets the same intent for 5 min

redis_client = None
iso_forest = None
encoders = {}


class EnrichRequest(BaseModel):
    event_id: str
    session_id: str
    user_id: str
    event_type: str
    page: str
    product_id: str | None = None
    price: float | None = None
    quantity: int | None = None
    device: str
    country: str
    event_ts: str
    session_context: list[dict] = []


class EnrichResponse(BaseModel):
    event_id: str
    intent_label: str
    sentiment_score: float
    anomaly_score: float
    is_anomaly: bool
    anomaly_reason: str | None
    processing_ms: float


@app.on_event("startup")
async def startup():
    global redis_client, iso_forest, encoders
    redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)
    iso_forest, encoders = train_model()
    log.info("ready (provider=%s)", AI_PROVIDER)


@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.aclose()


# --- anomaly model -----------------------------------------------------------

def train_model():
    """Train (or retrain) the Isolation Forest on synthetic 'normal' traffic.
    In a real deployment you'd fit this on a window of historical events and
    reload it periodically - here we just generate a believable baseline."""
    os.makedirs("/app/model", exist_ok=True)

    event_types = ["page_view", "search", "add_to_cart", "remove_from_cart",
                   "checkout_start", "purchase", "session_drop"]
    devices = ["desktop", "mobile", "tablet"]
    # first 10 countries are "normal", the rest are the high-risk ones we flag
    countries = ["US", "UK", "DE", "CA", "AU", "FR", "IN", "BR", "JP", "SG",
                 "RU", "CN", "IR", "KP"]

    enc = {
        "event_type": LabelEncoder().fit(event_types),
        "device": LabelEncoder().fit(devices),
        "country": LabelEncoder().fit(countries),
    }

    rng = np.random.default_rng(42)
    n = 10_000
    normal = np.column_stack([
        rng.integers(0, 7, n),         # event type
        rng.integers(0, 3, n),         # device
        rng.integers(0, 10, n),        # country (normal range only)
        rng.exponential(200, n),       # price
        rng.integers(1, 5, n),         # quantity
        rng.integers(1, 25, n),        # session length
    ]).astype(float)

    model = IsolationForest(n_estimators=200, contamination=0.02,
                            random_state=42, n_jobs=-1)
    model.fit(normal)
    joblib.dump((model, enc), MODEL_PATH)
    log.info("isolation forest fitted on %d events", n)
    return model, enc


def to_features(event):
    et = event.get("event_type", "page_view")
    dev = event.get("device", "desktop")
    country = event.get("country", "US")

    def safe(col, val):
        classes = encoders[col].classes_
        return encoders[col].transform([val])[0] if val in classes else 0

    return [
        safe("event_type", et),
        safe("device", dev),
        safe("country", country),
        float(event.get("price") or 0.0),
        float(event.get("quantity") or 0.0),
        float(len(event.get("session_context", []))),
    ]


HIGH_RISK_COUNTRIES = {"RU", "CN", "IR", "KP"}


def rule_flags(event):
    """Hard rules for the obvious fraud signals. Real fraud systems do this -
    you don't make a model relearn 'quantity of 200 is bad' every time when a
    one-line rule nails it with zero false positives. The model handles the
    subtle stuff these rules miss."""
    qty = event.get("quantity") or 0
    price = event.get("price") or 0
    country = event.get("country", "US")

    if qty > 20:
        return 0.95, f"unusually large quantity: {qty}"
    if 0 < price < 1.0:
        return 0.9, f"suspiciously low price: ${price:.2f}"
    if country in HIGH_RISK_COUNTRIES:
        return 0.8, f"high-risk country: {country}"
    return None, None


def score_anomaly(event):
    # rules first - they're cheap and precise
    rule_score, rule_reason = rule_flags(event)
    if rule_score is not None:
        return rule_score, True, rule_reason

    # otherwise lean on the model for statistical outliers
    X = np.array(to_features(event)).reshape(1, -1)
    raw = iso_forest.decision_function(X)[0]   # higher = more normal
    score = float(np.clip(1.0 - (raw + 0.5), 0.0, 1.0))
    is_anom = iso_forest.predict(X)[0] == -1
    reason = "statistical outlier" if is_anom else None
    return score, is_anom, reason


# --- intent classification ---------------------------------------------------

SENTIMENT_BY_EVENT = {
    "purchase": 0.8, "add_to_cart": 0.5, "checkout_start": 0.6,
    "page_view": 0.1, "search": 0.2, "remove_from_cart": -0.4,
    "session_drop": -0.6,
}


async def intent_mock(event):
    """No API key needed. Picks a label from event type + recent history.
    Good enough to demo the pipeline and keeps CI fast."""
    et = event.get("event_type", "page_view")
    ctx = [e.get("event_type") for e in event.get("session_context", [])]

    if et == "purchase":
        label = "purchase_ready"
    elif et == "checkout_start":
        label = "purchase_ready" if "add_to_cart" in ctx else "browsing"
    elif et == "add_to_cart":
        label = "purchase_ready" if ctx.count("add_to_cart") >= 2 else "comparison_shopping"
    elif et == "remove_from_cart":
        label = "cart_abandonment_risk"
    elif et == "session_drop":
        label = "about_to_churn"
    elif et == "search":
        label = "comparison_shopping"
    elif len(ctx) > 10:
        label = "loyal_repeat"
    else:
        label = "browsing"

    import random
    sentiment = float(np.clip(SENTIMENT_BY_EVENT.get(et, 0.0) + random.gauss(0, 0.05), -1, 1))
    return label, sentiment


def build_prompt(event):
    recent = ", ".join(e.get("event_type", "") for e in event.get("session_context", [])[-5:]) or "none"
    return (
        "Classify this e-commerce event. Reply with JSON only.\n\n"
        f"Event: {event.get('event_type')} on {event.get('page')}\n"
        f"Product: {event.get('product_name', 'N/A')} @ ${event.get('price', 0)}\n"
        f"Recent session: {recent}\n"
        f"Device: {event.get('device')}, Country: {event.get('country')}\n\n"
        '{"intent": "comparison_shopping|purchase_ready|about_to_churn|'
        'browsing|cart_abandonment_risk|loyal_repeat", "sentiment": <float -1..1>}'
    )


def parse_llm(text):
    try:
        d = json.loads(text.strip())
        return d.get("intent", "browsing"), float(d.get("sentiment", 0.0))
    except Exception:
        # models occasionally wrap JSON in prose - fall back rather than crash
        return "browsing", 0.0


async def intent_openai(event):
    import openai
    client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    try:
        r = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": build_prompt(event)}],
            max_tokens=60, temperature=0.1,
        )
        return parse_llm(r.choices[0].message.content)
    except Exception as e:
        log.warning("openai failed (%s), using mock", e)
        return await intent_mock(event)


async def intent_anthropic(event):
    import anthropic
    client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    try:
        r = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=60,
            messages=[{"role": "user", "content": build_prompt(event)}],
        )
        return parse_llm(r.content[0].text)
    except Exception as e:
        log.warning("anthropic failed (%s), using mock", e)
        return await intent_mock(event)


async def classify_intent(event):
    # cache on the *shape* of the event, not its id - identical patterns
    # shouldn't cost a second API call
    fp = hashlib.md5(json.dumps({
        "et": event.get("event_type"),
        "page": event.get("page"),
        "ctx": [e.get("event_type") for e in event.get("session_context", [])[-3:]],
    }, sort_keys=True).encode()).hexdigest()
    key = f"intent:{fp}"

    if redis_client:
        cached = await redis_client.get(key)
        if cached:
            d = json.loads(cached)
            return d["label"], d["sentiment"]

    if AI_PROVIDER == "openai":
        label, sentiment = await intent_openai(event)
    elif AI_PROVIDER == "anthropic":
        label, sentiment = await intent_anthropic(event)
    else:
        label, sentiment = await intent_mock(event)

    if redis_client:
        await redis_client.setex(key, CACHE_TTL,
                                 json.dumps({"label": label, "sentiment": sentiment}))
    return label, sentiment


# --- endpoints ---------------------------------------------------------------

@app.post("/enrich", response_model=EnrichResponse)
async def enrich(req: EnrichRequest):
    t0 = time.perf_counter()
    event = req.model_dump()

    # kick off the (maybe-remote) intent call, score anomaly locally meanwhile
    intent_task = asyncio.create_task(classify_intent(event))
    anomaly_score, is_anom, reason = score_anomaly(event)
    intent_label, sentiment = await intent_task

    if redis_client:
        await redis_client.hset(f"session:{req.session_id}", mapping={
            "last_intent": intent_label,
            "last_anomaly": str(anomaly_score),
            "last_event": req.event_type,
        })
        await redis_client.expire(f"session:{req.session_id}", 3600)

    return EnrichResponse(
        event_id=req.event_id,
        intent_label=intent_label,
        sentiment_score=round(sentiment, 4),
        anomaly_score=round(anomaly_score, 4),
        is_anomaly=is_anom,
        anomaly_reason=reason,
        processing_ms=round((time.perf_counter() - t0) * 1000, 2),
    )


@app.post("/enrich/batch", response_model=list[EnrichResponse])
async def enrich_batch(events: list[EnrichRequest]):
    return await asyncio.gather(*(enrich(e) for e in events))



@app.post("/model/reload")
async def model_reload():
    """Reload the model from disk - called after the Airflow retrain DAG
    writes a fresh artifact to the shared volume."""
    global model_bundle
    if not os.path.exists(MODEL_PATH):
        raise HTTPException(404, "no model file on disk")
    model_bundle = joblib.load(MODEL_PATH)
    log.info("model reloaded: %s", model_bundle["source"])
    return {"status": "reloaded", "source": model_bundle["source"],
            "training_size": model_bundle["training_size"]}


@app.get("/health")
async def health():
    return {"status": "ok", "provider": AI_PROVIDER}


@app.get("/model/stats")
async def model_stats():
    if iso_forest is None:
        raise HTTPException(503, "model not loaded")
    return {
        "n_estimators": iso_forest.n_estimators,
        "contamination": iso_forest.contamination,
        "n_features": iso_forest.n_features_in_,
    }
