"""
Clickstream event generator.

Simulates shopping sessions and pushes events to Kafka. A fraction are
deliberately anomalous. Each event carries:
  - is_anomaly_truth: ground-truth label so we can measure model precision/recall
  - client_ip: lets the windowed detector catch bot velocity (many events/IP/min)
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

fake = Faker()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "clickstream.raw"
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "50"))
ANOMALY_RATE = float(os.getenv("ANOMALY_RATE", "0.05"))

# Catalogue aligned with REES46 brands/categories so live events match training.
PRODUCTS = [
    {"id": "P001", "name": "Galaxy S24",      "category": "electronics.smartphone",        "brand": "samsung", "price": 899.99},
    {"id": "P002", "name": "iPhone 15",       "category": "electronics.smartphone",        "brand": "apple",   "price": 1099.00},
    {"id": "P003", "name": "Redmi Note 13",   "category": "electronics.smartphone",        "brand": "xiaomi",  "price": 249.99},
    {"id": "P004", "name": "MacBook Air",     "category": "computers.notebook",            "brand": "apple",   "price": 1299.00},
    {"id": "P005", "name": "ThinkPad X1",     "category": "computers.notebook",            "brand": "lenovo",  "price": 1599.99},
    {"id": "P006", "name": "Sony WH-1000XM5", "category": "electronics.audio.headphone",   "brand": "sony",    "price": 349.99},
    {"id": "P007", "name": "Bosch Dishwasher","category": "appliances.kitchen.dishwasher", "brand": "bosch",   "price": 689.00},
    {"id": "P008", "name": "Samsung Fridge",  "category": "appliances.kitchen.refrigerators","brand":"samsung","price": 1199.99},
    {"id": "P009", "name": "Apple Watch S9",  "category": "electronics.clocks",            "brand": "apple",   "price": 429.00},
    {"id": "P010", "name": "iPad Air",        "category": "electronics.tablet",            "brand": "apple",   "price": 899.99},
]
PAGES = ["/", "/products", "/cart", "/checkout", "/checkout/payment", "/account", "/search", "/deals"]
DEVICES = ["desktop", "mobile", "tablet"]
COUNTRIES = ["US", "UK", "DE", "CA", "AU", "FR", "IN", "BR", "JP", "SG"]
EVENT_WEIGHTS = {"page_view": 50, "search": 15, "add_to_cart": 15,
                 "remove_from_cart": 5, "checkout_start": 8, "purchase": 5, "session_drop": 2}


def rand_ip():
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


class SessionPool:
    def __init__(self, size=300):
        self.sessions = [self._new() for _ in range(size)]

    def _new(self):
        return {
            "session_id": str(uuid.uuid4()),
            "user_id": f"U{random.randint(10000, 99999)}",
            "device": random.choice(DEVICES),
            "country": random.choices(COUNTRIES, weights=[35,15,10,8,7,7,5,5,4,4])[0],
            "client_ip": rand_ip(),
            "event_count": 0,
        }

    def get(self):
        s = random.choice(self.sessions)
        s["event_count"] += 1
        if s["event_count"] > random.randint(5, 30):
            i = self.sessions.index(s)
            self.sessions[i] = self._new()
            return self.sessions[i]
        return s


def normal_event(session):
    et = random.choices(list(EVENT_WEIGHTS), weights=list(EVENT_WEIGHTS.values()))[0]
    product = random.choice(PRODUCTS) if et in ("add_to_cart","remove_from_cart","purchase") else None
    page = f"/products/{product['category']}/{product['id']}" if product else random.choice(PAGES)
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session["session_id"],
        "user_id": session["user_id"],
        "event_type": et,
        "page": page,
        "product_id": product["id"] if product else None,
        "product_name": product["name"] if product else None,
        "price": product["price"] if product else None,
        "quantity": random.randint(1,3) if product else None,
        "brand": product["brand"] if product else None,
        "category": product["category"] if product else None,
        "device": session["device"],
        "country": session["country"],
        "client_ip": session["client_ip"],
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "is_anomaly_truth": 0,
        "anomaly_kind_truth": "",
    }


def anomalous_event(session):
    kind = random.choice(["cart_stuffing","price_manipulation","geo_spike","bot_velocity"])
    e = normal_event(session)
    e["is_anomaly_truth"] = 1
    e["anomaly_kind_truth"] = kind
    if kind == "cart_stuffing":
        p = random.choice(PRODUCTS)
        e.update(event_type="add_to_cart", product_id=p["id"], product_name=p["name"],
                 brand=p["brand"], category=p["category"], price=p["price"],
                 quantity=random.randint(50,200))
    elif kind == "price_manipulation":
        p = random.choice(PRODUCTS)
        e.update(event_type="add_to_cart", product_id=p["id"], product_name=p["name"],
                 brand=p["brand"], category=p["category"], price=round(p["price"]*0.001,2), quantity=1)
    elif kind == "geo_spike":
        e["country"] = random.choice(["RU","CN","IR","KP"])
    elif kind == "bot_velocity":
        # reuse one IP across a burst so the windowed detector catches it
        e["client_ip"] = "66.66.66.66"
        e["event_type"] = "page_view"
    return e


def on_delivery(err, msg):
    if err:
        print(f"delivery failed: {err}")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "client.id": "clickstream-producer",
                         "linger.ms": 5, "batch.num.messages": 1000, "compression.type": "snappy"})
    pool = SessionPool()
    print(f"producer started: {EVENTS_PER_SECOND} ev/s, anomaly rate {ANOMALY_RATE:.0%} -> {TOPIC}")
    interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    start = time.time()
    while True:
        loop = time.time()
        s = pool.get()
        e = anomalous_event(s) if random.random() < ANOMALY_RATE else normal_event(s)
        producer.produce(TOPIC, key=e["session_id"], value=json.dumps(e).encode(), callback=on_delivery)
        producer.poll(0)
        sent += 1
        if sent % 1000 == 0:
            print(f"{sent:,} events sent ({sent/(time.time()-start):.0f}/s)")
        slp = interval - (time.time() - loop)
        if slp > 0:
            time.sleep(slp)


if __name__ == "__main__":
    time.sleep(15)
    main()
