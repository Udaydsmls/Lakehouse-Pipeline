import random
import sys
import time
import uuid
from datetime import datetime, timezone

from faker import Faker

from config import ProducerConfig
from utils import (
    hourly_traffic_multiplier,
    make_producer,
    pareto_product_id,
    serialize_avro,
    get_schema,
)

fake = Faker()

EVENT_TYPES = [
    "page_view",
    "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "purchase",
]

EVENT_WEIGHTS = [0.40, 0.30, 0.15, 0.08, 0.03, 0.02, 0.02]

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.60, 0.30, 0.10]

PRODUCT_EVENT_TYPES = {"product_view", "add_to_cart", "remove_from_cart", "checkout_start", "purchase"}

SEARCH_TERMS = [
    "shoes", "laptop", "headphones", "jacket", "watch", "camera",
    "phone case", "running shoes", "bluetooth speaker", "backpack",
    "sunglasses", "yoga mat", "coffee maker", "desk lamp", "gaming mouse",
]


def _make_page_url(event_type: str, product_id: str | None, query: str | None = None) -> str:
    if event_type == "page_view":
        pages = ["/", "/deals", "/new-arrivals", "/categories", "/about", "/contact"]
        return random.choice(pages)
    if event_type in ("product_view", "add_to_cart", "remove_from_cart"):
        return f"/products/{product_id}"
    if event_type == "search":
        term = query or random.choice(SEARCH_TERMS)
        return f"/search?q={term.replace(' ', '+')}"
    if event_type == "checkout_start":
        return "/checkout"
    if event_type == "purchase":
        return "/order/confirmation"
    return "/"


def _make_event(config: ProducerConfig, session_id: str) -> dict:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    user_id = f"usr_{random.randint(1, config.num_users)}"
    device_type = random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]

    product_id = None
    if event_type in PRODUCT_EVENT_TYPES:
        pid = pareto_product_id(config.num_products, config.pareto_alpha)
        product_id = f"prod_{pid}"

    query = random.choice(SEARCH_TERMS) if event_type == "search" else None
    page_url = _make_page_url(event_type, product_id, query)

    referrers = [
        "https://www.google.com",
        "https://www.facebook.com",
        "https://www.instagram.com",
        None,
        None,
        None,
    ]

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id,
        "page_url": page_url,
        "referrer": random.choice(referrers),
        "device_type": device_type,
        "user_agent": fake.user_agent(),
        "ip_address": fake.ipv4(),
        "country": fake.country_code(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(config: ProducerConfig):
    schema = get_schema("clickstream")
    producer = make_producer(config)
    topic = config.topics["clickstream_raw"]

    session_id = str(uuid.uuid4())
    events_in_session = 0
    session_length = random.randint(3, 20)

    count = 0
    try:
        while True:
            hour = datetime.now(timezone.utc).hour
            multiplier = hourly_traffic_multiplier(hour)
            sleep_seconds = (1.0 / max(config.events_per_second * multiplier, 1))

            if events_in_session >= session_length:
                session_id = str(uuid.uuid4())
                events_in_session = 0
                session_length = random.randint(3, 20)

            event = _make_event(config, session_id)
            payload = serialize_avro(schema, event)
            producer.produce(topic, key=event["user_id"].encode(), value=payload)
            producer.poll(0)

            events_in_session += 1
            count += 1

            if count % 1000 == 0:
                print(f"[clickstream] produced {count} events", file=sys.stderr)
                producer.flush()

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"[clickstream] shutting down after {count} events", file=sys.stderr)


if __name__ == "__main__":
    run(ProducerConfig())
