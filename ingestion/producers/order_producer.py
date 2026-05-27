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

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "buy_now_pay_later", "crypto"]
PAYMENT_WEIGHTS = [0.45, 0.25, 0.18, 0.09, 0.03]

ORDER_LIFECYCLE: dict[str, list[tuple[str, float]]] = {
    "order_placed":     [("order_confirmed", 0.95), ("order_cancelled", 0.05)],
    "order_confirmed":  [("order_shipped", 0.92), ("order_cancelled", 0.08)],
    "order_shipped":    [("order_delivered", 0.97), ("return_requested", 0.03)],
    "order_delivered":  [("return_requested", 0.08)],
    "return_requested": [("return_completed", 1.0)],
    "order_cancelled":  [],
    "return_completed": [],
}

TERMINAL_STATES = {"order_delivered", "order_cancelled", "return_completed"}

_TRANSITION_DELAY_SECONDS = {
    "order_placed":     (2, 10),
    "order_confirmed":  (30, 120),
    "order_shipped":    (300, 900),
    "order_delivered":  (600, 3600),
    "return_requested": (60, 300),
}


def _new_order(config: ProducerConfig) -> dict:
    num_items = random.randint(1, 5)
    product_ids = [
        f"prod_{pareto_product_id(config.num_products, config.pareto_alpha)}"
        for _ in range(num_items)
    ]
    total = round(random.uniform(15.0, 800.0), 2)
    discount = round(total * random.uniform(0.0, 0.25), 2)
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": f"usr_{random.randint(1, config.num_users)}",
        "product_ids": product_ids,
        "total_amount": total,
        "discount_amount": discount,
        "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0],
        "shipping_address_country": fake.country_code(),
        "current_state": "order_placed",
        "next_event_at": time.time() + random.uniform(0.5, 5.0),
    }


def _make_event(order: dict, event_type: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": order["order_id"],
        "event_type": event_type,
        "user_id": order["user_id"],
        "product_ids": order["product_ids"],
        "total_amount": order["total_amount"],
        "discount_amount": order["discount_amount"],
        "payment_method": order["payment_method"],
        "shipping_address_country": order["shipping_address_country"],
        "status": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(config: ProducerConfig):
    schema = get_schema("order_event")
    producer = make_producer(config)
    topic = config.topics["order_events"]

    active_orders: list[dict] = []
    for _ in range(50):
        order = _new_order(config)
        active_orders.append(order)

    new_order_interval = 1.0 / max(config.events_per_second * 0.02, 0.5)
    last_new_order = time.time()

    count = 0
    try:
        while True:
            now = time.time()

            if now - last_new_order >= new_order_interval:
                hour = datetime.now(timezone.utc).hour
                multiplier = hourly_traffic_multiplier(hour)
                if random.random() < multiplier:
                    active_orders.append(_new_order(config))
                last_new_order = now

            pending = [o for o in active_orders if o["next_event_at"] <= now]
            for order in pending:
                state = order["current_state"]
                event = _make_event(order, state)
                payload = serialize_avro(schema, event)
                producer.produce(topic, key=order["order_id"].encode(), value=payload)
                producer.poll(0)
                count += 1

                transitions = ORDER_LIFECYCLE.get(state, [])
                if transitions:
                    next_states, weights = zip(*transitions)
                    next_state = random.choices(next_states, weights=weights, k=1)[0]
                    order["current_state"] = next_state
                    delay_range = _TRANSITION_DELAY_SECONDS.get(state, (5, 30))
                    order["next_event_at"] = now + random.uniform(*delay_range)
                else:
                    order["current_state"] = "__done__"

            active_orders = [
                o for o in active_orders if o["current_state"] != "__done__"
            ]

            if count % 500 == 0 and count > 0:
                print(f"[orders] produced {count} events, {len(active_orders)} active orders", file=sys.stderr)
                producer.flush()

            time.sleep(0.05)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"[orders] shutting down after {count} events", file=sys.stderr)


if __name__ == "__main__":
    run(ProducerConfig())
