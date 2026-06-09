"""
Data quality validator for the stream processor.

Validates every event before it reaches the enrichment service.
Bad events go to the DLQ topic with a structured reason.
Quality metrics are tracked in Redis so the dashboard can show
the pass rate - a standard production observability pattern.
"""
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger("data-quality")

VALID_EVENT_TYPES = {
    "page_view", "view", "add_to_cart", "cart", "remove_from_cart",
    "checkout_start", "purchase", "session_drop", "search"
}
VALID_DEVICES  = {"desktop", "mobile", "tablet"}
VALID_COUNTRIES = {
    "US","UK","DE","CA","AU","FR","IN","BR","JP","SG",
    "RU","CN","IR","KP","MX","ES","IT","NL","SE","NO"
}
MAX_PRICE    = 50_000.0
MAX_QUANTITY = 10_000
MAX_AGE_S    = 3_600
MAX_FUTURE_S = 60


class DataQualityChecker:
    def __init__(self):
        self.total   = 0
        self.passed  = 0
        self.failed  = 0
        self.reasons = {}

    def check(self, event: dict) -> list:
        self.total += 1
        issues = []

        for field in ["event_id","session_id","user_id","event_type","event_ts"]:
            if not event.get(field):
                issues.append(f"missing_field:{field}")

        et = event.get("event_type","")
        if et and et not in VALID_EVENT_TYPES:
            issues.append(f"invalid_event_type:{et}")

        price = event.get("price")
        if price is not None:
            try:
                p = float(price)
                if p < 0:
                    issues.append(f"negative_price:{p:.2f}")
                elif p > MAX_PRICE:
                    issues.append(f"price_exceeds_max:{p:.2f}")
            except (TypeError, ValueError):
                issues.append(f"non_numeric_price:{price}")

        qty = event.get("quantity")
        if qty is not None:
            try:
                q = int(qty)
                if q < 0:
                    issues.append(f"negative_quantity:{q}")
                elif q > MAX_QUANTITY:
                    issues.append(f"quantity_exceeds_max:{q}")
            except (TypeError, ValueError):
                issues.append(f"non_integer_quantity:{qty}")

        ts_raw = event.get("event_ts")
        if ts_raw:
            try:
                ts = datetime.fromisoformat(str(ts_raw).replace("Z","+00:00"))
                now = datetime.now(timezone.utc)
                age_s = (now - ts).total_seconds()
                if age_s > MAX_AGE_S:
                    issues.append(f"late_data:{int(age_s)}s_old")
                elif age_s < -MAX_FUTURE_S:
                    issues.append(f"future_timestamp:{int(-age_s)}s_ahead")
            except Exception:
                issues.append(f"unparseable_timestamp")

        device  = event.get("device","")
        country = event.get("country","")
        if device and device not in VALID_DEVICES:
            issues.append(f"unknown_device:{device}")
        if country and country not in VALID_COUNTRIES:
            issues.append(f"unknown_country:{country}")

        if issues:
            self.failed += 1
            for reason in issues:
                key = reason.split(":")[0]
                self.reasons[key] = self.reasons.get(key, 0) + 1
        else:
            self.passed += 1

        return issues

    def pass_rate(self) -> float:
        return self.passed / self.total if self.total else 1.0

    def stats(self) -> dict:
        return {
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "pass_rate": round(self.pass_rate(), 4),
            "failure_reasons": dict(
                sorted(self.reasons.items(), key=lambda x: -x[1])[:10]
            ),
        }