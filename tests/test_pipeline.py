"""
Integration tests for the Real-Time AI Pipeline.
Run with: pytest tests/ -v
"""
import json
import time
import uuid
import pytest
import requests

API_URL = "http://localhost:8000"
AI_URL  = "http://localhost:8001"


# Fixtures
@pytest.fixture
def sample_event():
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "user_id": "U12345",
        "event_type": "add_to_cart",
        "page": "/products/laptops/P001",
        "product_id": "P001",
        "product_name": "UltraBook Pro 15",
        "price": 1299.99,
        "quantity": 1,
        "device": "desktop",
        "country": "US",
        "event_ts": "2024-01-15T10:30:00Z",
        "session_context": [
            {"event_type": "page_view"},
            {"event_type": "search"},
        ],
    }


@pytest.fixture
def anomalous_event():
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "user_id": "U99999",
        "event_type": "add_to_cart",
        "page": "/products/laptops/P001",
        "product_id": "P001",
        "product_name": "UltraBook Pro 15",
        "price": 1299.99,
        "quantity": 150,  # suspicious
        "device": "desktop",
        "country": "RU",  # high-risk
        "event_ts": "2024-01-15T10:30:00Z",
        "session_context": [],
    }


# AI service tests
class TestAIService:
    def test_health(self):
        r = requests.get(f"{AI_URL}/health", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"

    def test_enrich_normal_event(self, sample_event):
        r = requests.post(f"{AI_URL}/enrich", json=sample_event, timeout=10)
        assert r.status_code == 200
        data = r.json()

        assert "intent_label" in data
        assert data["intent_label"] in [
            "comparison_shopping", "purchase_ready", "about_to_churn",
            "browsing", "cart_abandonment_risk", "loyal_repeat",
        ]
        assert -1.0 <= data["sentiment_score"] <= 1.0
        assert 0.0 <= data["anomaly_score"] <= 1.0
        assert isinstance(data["is_anomaly"], bool)
        assert data["processing_ms"] > 0

    def test_enrich_anomalous_event(self, anomalous_event):
        r = requests.post(f"{AI_URL}/enrich", json=anomalous_event, timeout=10)
        assert r.status_code == 200
        data = r.json()
        # High-quantity + high-risk country should score high
        assert data["anomaly_score"] > 0.4
        assert data["is_anomaly"] is True

    def test_batch_enrich(self, sample_event, anomalous_event):
        events = [sample_event, anomalous_event]
        # Give them different IDs
        events[0]["event_id"] = str(uuid.uuid4())
        events[1]["event_id"] = str(uuid.uuid4())
        r = requests.post(f"{AI_URL}/enrich/batch", json=events, timeout=15)
        assert r.status_code == 200
        results = r.json()
        assert len(results) == 2

    def test_model_stats(self):
        r = requests.get(f"{AI_URL}/model/stats", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert "n_estimators" in data
        assert data["n_estimators"] > 0


# API tests
class TestAPI:
    def test_health(self):
        r = requests.get(f"{API_URL}/health", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert data["api"] == "ok"

    def test_live_stats(self):
        r = requests.get(f"{API_URL}/stats/live", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert "events_processed" in data
        assert "rate_per_sec" in data
        assert "recent_alerts" in data
        assert isinstance(data["recent_alerts"], list)

    def test_events_per_minute(self):
        r = requests.get(f"{API_URL}/metrics/events-per-minute?minutes=5", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    def test_intent_distribution(self):
        r = requests.get(f"{API_URL}/metrics/intent-distribution?minutes=60", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    def test_country_heatmap(self):
        r = requests.get(f"{API_URL}/metrics/country-heatmap?minutes=60", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        if data:
            assert "country" in data[0]
            assert "events" in data[0]

    def test_recent_anomalies(self):
        r = requests.get(f"{API_URL}/anomalies/recent?limit=10", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    def test_session_not_found(self):
        r = requests.get(f"{API_URL}/session/nonexistent-session-id", timeout=5)
        assert r.status_code == 404


# Unit tests for producer logic
class TestProducerLogic:
    """Test event generation without Kafka."""

    def test_normal_event_structure(self):
        import sys
        sys.path.insert(0, "./producer")
        from producer import build_normal_event, SessionPool

        pool = SessionPool(size=5)
        session = pool.get_session()
        event = build_normal_event(session)

        required = ["event_id", "session_id", "user_id", "event_type",
                    "page", "device", "country", "event_ts"]
        for field in required:
            assert field in event, f"Missing field: {field}"

        assert event["event_type"] in [
            "page_view", "search", "add_to_cart", "remove_from_cart",
            "checkout_start", "purchase", "session_drop",
        ]

    def test_anomalous_event_has_hint(self):
        import sys
        sys.path.insert(0, "./producer")
        from producer import build_anomalous_event, SessionPool

        pool = SessionPool(size=5)
        session = pool.get_session()
        event = build_anomalous_event(session)
        assert event["_is_anomaly_hint"] is True
        assert "_anomaly_type" in event

    def test_anomaly_types_produce_detectable_signals(self):
        import sys
        sys.path.insert(0, "./producer")
        from producer import build_anomalous_event, SessionPool

        pool = SessionPool(size=5)
        seen_types = set()
        for _ in range(100):
            session = pool.get_session()
            event = build_anomalous_event(session)
            seen_types.add(event["_anomaly_type"])
        assert len(seen_types) >= 3, "Expected to see multiple anomaly types"


# End-to-end smoke test
class TestEndToEnd:
    """
    Smoke test: inject an event via the producer topic,
    wait, confirm it appears in ClickHouse via the API.
    Requires the full stack running.
    """

    def test_event_flows_through_pipeline(self):
        """
        This test is skipped in unit mode; run with --e2e flag.
        It checks that events ingested from the producer
        eventually surface in the API metrics.
        """
        import os
        if not os.getenv("RUN_E2E"):
            pytest.skip("Set RUN_E2E=1 to run end-to-end tests")

        # Take a baseline count
        r1 = requests.get(f"{API_URL}/stats/live").json()
        baseline = r1.get("events_processed", 0)

        # Wait for events to flow
        time.sleep(10)

        r2 = requests.get(f"{API_URL}/stats/live").json()
        new_count = r2.get("events_processed", 0)

        assert new_count > baseline, (
            f"No new events processed after 10s: {baseline} → {new_count}"
        )
        print(f" {new_count - baseline} new events in 10s")
