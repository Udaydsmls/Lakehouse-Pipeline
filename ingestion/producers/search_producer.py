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
    serialize_avro,
    get_schema,
)

fake = Faker()

CATEGORIES = [
    "electronics", "clothing", "shoes", "home", "garden", "sports",
    "beauty", "toys", "books", "automotive", "jewelry", "food",
]

ATTRIBUTES = [
    "waterproof", "wireless", "portable", "lightweight", "organic",
    "premium", "vintage", "handmade", "sale", "new arrival",
]

BRANDS = [
    "brand:Nike", "brand:Apple", "brand:Samsung", "brand:Adidas",
    "brand:Sony", "brand:LG", "brand:Zara", "brand:H&M",
]

PRICE_RANGES = [
    "price:0-25", "price:25-50", "price:50-100",
    "price:100-250", "price:250-500", "price:500+",
]

RATINGS = ["rating:4+", "rating:3+"]

ALL_FILTERS = BRANDS + PRICE_RANGES + RATINGS + [
    "shipping:free", "condition:new", "condition:refurbished",
]


def _make_query() -> str:
    strategy = random.random()
    if strategy < 0.40:
        return " ".join(fake.words(nb=random.randint(1, 3)))
    elif strategy < 0.70:
        return f"{random.choice(CATEGORIES)} {random.choice(ATTRIBUTES)}"
    else:
        return random.choice(CATEGORIES)


def _make_filters() -> list[str]:
    num_filters = random.choices([0, 1, 2, 3], weights=[0.50, 0.30, 0.15, 0.05], k=1)[0]
    return random.sample(ALL_FILTERS, min(num_filters, len(ALL_FILTERS)))


def _make_event(config: ProducerConfig) -> dict:
    user_id = f"usr_{random.randint(1, config.num_users)}"
    results_count = random.randint(0, 200)

    clicked_position = None
    if results_count > 0 and random.random() < 0.55:
        clicked_position = random.choices(
            range(1, 11),
            weights=[25, 18, 14, 10, 8, 7, 6, 5, 4, 3],
            k=1,
        )[0]

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "query": _make_query(),
        "results_count": results_count,
        "clicked_position": clicked_position,
        "filters_applied": _make_filters(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(config: ProducerConfig):
    schema = get_schema("search_event")
    producer = make_producer(config)
    topic = config.topics["search_events"]

    count = 0
    try:
        while True:
            hour = datetime.now(timezone.utc).hour
            multiplier = hourly_traffic_multiplier(hour)
            sleep_seconds = 1.0 / max(config.events_per_second * multiplier * 0.12, 1)

            event = _make_event(config)
            payload = serialize_avro(schema, event)
            producer.produce(topic, key=event["user_id"].encode(), value=payload)
            producer.poll(0)

            count += 1
            if count % 1000 == 0:
                print(f"[search] produced {count} events", file=sys.stderr)
                producer.flush()

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"[search] shutting down after {count} events", file=sys.stderr)


if __name__ == "__main__":
    run(ProducerConfig())
