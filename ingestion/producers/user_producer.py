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

EVENT_TYPES = [
    "user_registered",
    "profile_updated",
    "email_verified",
    "password_reset",
    "subscription_changed",
    "account_deleted",
]
EVENT_WEIGHTS = [0.30, 0.28, 0.18, 0.12, 0.08, 0.04]

ACQUISITION_CHANNELS = ["organic", "paid_search", "social", "email", "referral", "direct"]
ACQUISITION_WEIGHTS = [0.30, 0.22, 0.20, 0.12, 0.10, 0.06]

SEGMENTS = ["new", "returning", "vip", "at_risk", "churned"]
SEGMENT_WEIGHTS = [0.35, 0.30, 0.10, 0.15, 0.10]


def _make_event(config: ProducerConfig) -> dict:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    user_id = f"usr_{random.randint(1, config.num_users)}"
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "email": fake.email(),
        "country": fake.country_code(),
        "acquisition_channel": random.choices(ACQUISITION_CHANNELS, weights=ACQUISITION_WEIGHTS, k=1)[0],
        "segment": random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(config: ProducerConfig):
    schema = get_schema("user_event")
    producer = make_producer(config)
    topic = config.topics["user_events"]

    count = 0
    try:
        while True:
            hour = datetime.now(timezone.utc).hour
            multiplier = hourly_traffic_multiplier(hour)
            sleep_seconds = 1.0 / max(config.events_per_second * multiplier * 0.05, 0.5)

            event = _make_event(config)
            payload = serialize_avro(schema, event)
            producer.produce(topic, key=event["user_id"].encode(), value=payload)
            producer.poll(0)

            count += 1
            if count % 1000 == 0:
                print(f"[users] produced {count} events", file=sys.stderr)
                producer.flush()

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"[users] shutting down after {count} events", file=sys.stderr)


if __name__ == "__main__":
    run(ProducerConfig())
