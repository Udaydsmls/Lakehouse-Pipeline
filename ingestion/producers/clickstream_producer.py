"""Sends fake e-commerce clickstream events to Kafka as JSON.

Each user gets a session id that is reused for a handful of events so that the
Flink job downstream has something to group into sessions.

Usage:
    python ingestion/producers/clickstream_producer.py
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

import config

EVENT_TYPES = [
    "page_view",
    "product_view",
    "search",
    "add_to_cart",
    "checkout_start",
    "purchase",
]

# Most events are browsing, very few are purchases.
EVENT_WEIGHTS = [0.45, 0.30, 0.12, 0.08, 0.03, 0.02]

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
COUNTRIES = ["US", "GB", "CA", "DE", "FR", "IN", "AU", "JP", "BR"]

# Event types that are about a specific product.
PRODUCT_EVENTS = {"product_view", "add_to_cart", "checkout_start", "purchase"}


def make_event(session_id, user_id):
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]

    product_id = None
    if event_type in PRODUCT_EVENTS:
        product_id = "PROD-%03d" % random.randint(1, config.NUM_PRODUCTS)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id,
        "device_type": random.choice(DEVICE_TYPES),
        "country": random.choice(COUNTRIES),
        # Flink parses this format straight into a TIMESTAMP(3) column.
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    }


def main():
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    sleep_seconds = 1.0 / config.EVENTS_PER_SECOND

    # Start a session; after a few events we pretend the user left and start a new one.
    user_id = "USR-%04d" % random.randint(1, config.NUM_USERS)
    session_id = str(uuid.uuid4())
    events_left_in_session = random.randint(3, 15)

    sent = 0
    print("Sending events to topic '%s'. Press Ctrl+C to stop." % config.CLICKSTREAM_TOPIC)

    try:
        while True:
            if events_left_in_session == 0:
                user_id = "USR-%04d" % random.randint(1, config.NUM_USERS)
                session_id = str(uuid.uuid4())
                events_left_in_session = random.randint(3, 15)

            event = make_event(session_id, user_id)
            producer.produce(
                config.CLICKSTREAM_TOPIC,
                key=user_id,
                value=json.dumps(event),
            )
            producer.poll(0)

            events_left_in_session -= 1
            sent += 1
            if sent % 500 == 0:
                producer.flush()
                print("sent %d events" % sent)

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush()
        print("Total events sent: %d" % sent)


if __name__ == "__main__":
    main()
