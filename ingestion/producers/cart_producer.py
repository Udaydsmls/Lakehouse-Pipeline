import random
import sys
import time
import uuid
from datetime import datetime, timezone

from config import ProducerConfig
from utils import (
    hourly_traffic_multiplier,
    make_producer,
    pareto_product_id,
    serialize_avro,
    get_schema,
)

_ACTIVE_CART_POOL_SIZE = 1000


def _init_carts(num_users: int) -> dict[str, dict]:
    carts: dict[str, dict] = {}
    for _ in range(_ACTIVE_CART_POOL_SIZE):
        cart_id = str(uuid.uuid4())
        carts[cart_id] = {
            "user_id": f"usr_{random.randint(1, num_users)}",
            "session_id": str(uuid.uuid4()),
            "items": {},
        }
    return carts


def _cart_value(items: dict) -> float:
    return round(sum(v["quantity"] * v["unit_price"] for v in items.values()), 2)


def _make_event(config: ProducerConfig, carts: dict) -> dict:
    cart_id = random.choice(list(carts.keys()))
    cart = carts[cart_id]

    pid = pareto_product_id(config.num_products, config.pareto_alpha)
    product_id = f"prod_{pid}"

    has_items = bool(cart["items"])
    if has_items and random.random() < 0.25:
        event_type = "remove_from_cart"
        existing_pid = random.choice(list(cart["items"].keys()))
        product_id = existing_pid
        item = cart["items"][existing_pid]
        quantity = item["quantity"]
        unit_price = item["unit_price"]
        del cart["items"][existing_pid]
    else:
        event_type = "add_to_cart"
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(5.0, 500.0), 2)
        if product_id in cart["items"]:
            cart["items"][product_id]["quantity"] += quantity
        else:
            cart["items"][product_id] = {"quantity": quantity, "unit_price": unit_price}

    if len(cart["items"]) == 0 and random.random() < 0.1:
        cart["user_id"] = f"usr_{random.randint(1, config.num_users)}"
        cart["session_id"] = str(uuid.uuid4())

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": cart["user_id"],
        "session_id": cart["session_id"],
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "cart_value": _cart_value(cart["items"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(config: ProducerConfig):
    schema = get_schema("cart_event")
    producer = make_producer(config)
    topic = config.topics["cart_events"]
    carts = _init_carts(config.num_users)

    count = 0
    try:
        while True:
            hour = datetime.now(timezone.utc).hour
            multiplier = hourly_traffic_multiplier(hour)
            sleep_seconds = 1.0 / max(config.events_per_second * multiplier * 0.15, 1)

            event = _make_event(config, carts)
            payload = serialize_avro(schema, event)
            producer.produce(topic, key=event["user_id"].encode(), value=payload)
            producer.poll(0)

            count += 1
            if count % 1000 == 0:
                print(f"[cart] produced {count} events", file=sys.stderr)
                producer.flush()

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"[cart] shutting down after {count} events", file=sys.stderr)


if __name__ == "__main__":
    run(ProducerConfig())
