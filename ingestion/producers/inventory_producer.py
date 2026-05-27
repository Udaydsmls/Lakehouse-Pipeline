import random
import sys
import time
import uuid
from datetime import datetime, timezone

from config import ProducerConfig
from utils import (
    make_producer,
    pareto_product_id,
    serialize_avro,
    get_schema,
)

EVENT_TYPES = ["stock_updated", "restock_alert", "out_of_stock", "discontinued"]
EVENT_WEIGHTS = [0.70, 0.15, 0.12, 0.03]

WAREHOUSES = ["WH_US_EAST", "WH_US_WEST", "WH_EU_WEST", "WH_APAC"]
WAREHOUSE_WEIGHTS = [0.35, 0.25, 0.25, 0.15]


def _make_event(config: ProducerConfig) -> dict:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    pid = pareto_product_id(config.num_products, config.pareto_alpha)
    product_id = f"prod_{pid}"
    warehouse_id = random.choices(WAREHOUSES, weights=WAREHOUSE_WEIGHTS, k=1)[0]

    if event_type == "out_of_stock":
        quantity_on_hand = 0
        quantity_reserved = 0
    elif event_type == "restock_alert":
        quantity_on_hand = random.randint(1, 20)
        quantity_reserved = random.randint(0, quantity_on_hand)
    elif event_type == "discontinued":
        quantity_on_hand = random.randint(0, 50)
        quantity_reserved = 0
    else:
        quantity_on_hand = random.randint(0, 5000)
        quantity_reserved = random.randint(0, min(quantity_on_hand, 500))

    reorder_point = random.randint(10, 200)

    return {
        "event_id": str(uuid.uuid4()),
        "product_id": product_id,
        "event_type": event_type,
        "warehouse_id": warehouse_id,
        "quantity_on_hand": quantity_on_hand,
        "quantity_reserved": quantity_reserved,
        "reorder_point": reorder_point,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(config: ProducerConfig):
    schema = get_schema("inventory_event")
    producer = make_producer(config)
    topic = config.topics["inventory_events"]

    count = 0
    try:
        while True:
            sleep_seconds = 1.0 / max(config.events_per_second * 0.08, 1)

            event = _make_event(config)
            payload = serialize_avro(schema, event)
            producer.produce(topic, key=event["product_id"].encode(), value=payload)
            producer.poll(0)

            count += 1
            if count % 1000 == 0:
                print(f"[inventory] produced {count} events", file=sys.stderr)
                producer.flush()

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"[inventory] shutting down after {count} events", file=sys.stderr)


if __name__ == "__main__":
    run(ProducerConfig())
