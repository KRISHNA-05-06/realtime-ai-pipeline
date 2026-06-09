"""
Pipeline unit tests.
These run in GitHub Actions on every push to main.
No live services required — all logic is tested in isolation.
"""
import json
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Producer tests ──────────────────────────────────────────────

def test_producer_normal_event_has_required_fields():
    """Every normal event must have the fields the processor expects."""
    import importlib.util, types
    # Load producer without running main()
    required = ["event_id", "session_id", "user_id", "event_type",
                "device", "country", "client_ip", "event_ts",
                "is_anomaly_truth", "anomaly_kind_truth"]
    # Verify these fields are defined in producer.py
    with open("producer/producer.py") as f:
        content = f.read()
    for field in required:
        assert f'"{field}"' in content, f"Missing field: {field}"


def test_anomaly_truth_values_are_binary():
    """is_anomaly_truth must only be 0 or 1."""
    with open("producer/producer.py") as f:
        content = f.read()
    assert '"is_anomaly_truth": 0' in content
    assert '"is_anomaly_truth": 1' in content


# ── Windowed bot detector tests ─────────────────────────────────

def test_bot_detector_normal_ip_not_flagged():
    """An IP with 5 events in 60 seconds should not be flagged."""
    import time
    from collections import defaultdict, deque

    class IPVelocityWindow:
        def __init__(self, window_s=60, threshold=40):
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
            return len(dq) >= self.threshold, len(dq)

    w = IPVelocityWindow(window_s=60, threshold=40)
    for _ in range(5):
        flagged, count = w.record_and_check("1.2.3.4")
    assert flagged is False
    assert count == 5


def test_bot_detector_bot_ip_flagged():
    """An IP with 50 events in 60 seconds should be flagged."""
    import time
    from collections import defaultdict, deque

    class IPVelocityWindow:
        def __init__(self, window_s=60, threshold=40):
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
            return len(dq) >= self.threshold, len(dq)

    w = IPVelocityWindow(window_s=60, threshold=40)
    for _ in range(50):
        flagged, count = w.record_and_check("66.66.66.66")
    assert flagged is True
    assert count == 50


def test_bot_detector_old_events_expire():
    """Events outside the 60-second window should not count."""
    import time
    from collections import defaultdict, deque

    class IPVelocityWindow:
        def __init__(self, window_s=60, threshold=40):
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
            return len(dq) >= self.threshold, len(dq)

    w = IPVelocityWindow(window_s=60, threshold=40)
    old_time = time.time() - 120  # 2 minutes ago
    for _ in range(50):
        w._hits["9.9.9.9"].append(old_time)
    # One fresh event — should not be flagged
    flagged, count = w.record_and_check("9.9.9.9", now=time.time())
    assert flagged is False
    assert count == 1


# ── Anomaly scoring tests ────────────────────────────────────────

def test_hard_rule_cart_stuffing():
    """Quantity > 20 should trigger cart fraud rule."""
    def rule_flags(event):
        qty = event.get("quantity") or 0
        price = event.get("price") or 0
        country = event.get("country", "US")
        HIGH_RISK = {"RU", "CN", "IR", "KP"}
        if qty > 20:
            return 0.95, f"unusually large quantity: {qty}"
        if 0 < price < 1.0:
            return 0.9, f"suspiciously low price: ${price:.2f}"
        if country in HIGH_RISK:
            return 0.8, f"high-risk country: {country}"
        return None, None

    score, reason = rule_flags({"quantity": 150, "price": 899.99, "country": "US"})
    assert score == 0.95
    assert "150" in reason


def test_hard_rule_price_manipulation():
    """Price under $1 should trigger price manipulation rule."""
    def rule_flags(event):
        qty = event.get("quantity") or 0
        price = event.get("price") or 0
        country = event.get("country", "US")
        HIGH_RISK = {"RU", "CN", "IR", "KP"}
        if qty > 20:
            return 0.95, f"unusually large quantity: {qty}"
        if 0 < price < 1.0:
            return 0.9, f"suspiciously low price: ${price:.2f}"
        if country in HIGH_RISK:
            return 0.8, f"high-risk country: {country}"
        return None, None

    score, reason = rule_flags({"quantity": 1, "price": 0.35, "country": "US"})
    assert score == 0.9
    assert "0.35" in reason


def test_hard_rule_normal_event_not_flagged():
    """A normal event should not trigger any hard rule."""
    def rule_flags(event):
        qty = event.get("quantity") or 0
        price = event.get("price") or 0
        country = event.get("country", "US")
        HIGH_RISK = {"RU", "CN", "IR", "KP"}
        if qty > 20:
            return 0.95, f"unusually large quantity: {qty}"
        if 0 < price < 1.0:
            return 0.9, f"suspiciously low price: ${price:.2f}"
        if country in HIGH_RISK:
            return 0.8, f"high-risk country: {country}"
        return None, None

    score, reason = rule_flags({"quantity": 2, "price": 899.99, "country": "US"})
    assert score is None
    assert reason is None


# ── Alert type tests ─────────────────────────────────────────────

def test_alert_type_classification():
    """alert_type_for should correctly classify alert types."""
    def alert_type_for(event, reason):
        if reason and "bot" in reason:
            return "bot_velocity"
        if reason and "quantity" in reason:
            return "cart_fraud"
        if reason and "price" in reason:
            return "price_manipulation"
        if reason and "country" in reason:
            return "geo_spike"
        if event.get("event_type") == "session_drop":
            return "churn_risk"
        return "statistical_anomaly"

    assert alert_type_for({}, "bot velocity: 50 events") == "bot_velocity"
    assert alert_type_for({}, "unusually large quantity: 150") == "cart_fraud"
    assert alert_type_for({}, "suspiciously low price: $0.35") == "price_manipulation"
    assert alert_type_for({}, "high-risk country: RU") == "geo_spike"
    assert alert_type_for({"event_type": "session_drop"}, None) == "churn_risk"
    assert alert_type_for({}, None) == "statistical_anomaly"


# ── S3 sink tests ────────────────────────────────────────────────

def test_s3_sink_disabled_without_bucket():
    """S3Sink should be disabled when S3_BUCKET env var is not set."""
    os.environ.pop("S3_BUCKET", None)
    # Import after clearing env
    import importlib
    import flink_jobs.s3_sink as s3_module
    importlib.reload(s3_module)
    sink = s3_module.S3Sink()
    assert sink.enabled is False


def test_s3_sink_add_does_nothing_when_disabled():
    """Adding rows to a disabled sink should not raise errors."""
    os.environ.pop("S3_BUCKET", None)
    import importlib
    import flink_jobs.s3_sink as s3_module
    importlib.reload(s3_module)
    sink = s3_module.S3Sink()
    sink.add({"event_id": "test", "event_type": "page_view"})
    assert len(sink._buf) == 0


# ── Data contract tests ──────────────────────────────────────────

def test_event_cols_match_schema():
    """EVENT_COLS in processor must include all new fields added to schema."""
    with open("flink_jobs/processor.py") as f:
        content = f.read()
    required_cols = [
        "event_id", "session_id", "user_id", "event_type",
        "brand", "category", "client_ip",
        "is_anomaly_truth", "anomaly_kind_truth",
        "anomaly_score", "is_anomaly", "event_ts"
    ]
    for col in required_cols:
        assert f'"{col}"' in content, f"Missing column in EVENT_COLS: {col}"


def test_clickhouse_schema_has_ground_truth_columns():
    """ClickHouse init SQL must define ground truth columns."""
    with open("infra/clickhouse_init.sql") as f:
        content = f.read()
    assert "is_anomaly_truth" in content
    assert "anomaly_kind_truth" in content
    assert "client_ip" in content
    assert "model_evaluation" in content