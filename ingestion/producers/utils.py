import io
import json
import os
import sys
from pathlib import Path

import fastavro
import numpy as np
from confluent_kafka import Producer

_SCHEMAS_DIR = Path(__file__).parent.parent.parent / "streaming" / "schemas"

_HOURLY_MULTIPLIERS: dict[int, float] = {
    0: 0.2, 1: 0.2, 2: 0.2, 3: 0.2, 4: 0.2, 5: 0.2, 6: 0.2,
    7: 0.6, 8: 0.6, 9: 0.6,
    10: 1.2, 11: 1.2, 12: 1.2, 13: 1.2, 14: 1.2,
    15: 1.0, 16: 1.0, 17: 1.0, 18: 1.0,
    19: 1.4, 20: 1.4, 21: 1.4, 22: 1.4,
    23: 0.5,
}


def get_schema(schema_name: str) -> dict:
    schema_path = _SCHEMAS_DIR / f"{schema_name}.avsc"
    with open(schema_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def pareto_product_id(num_products: int, alpha: float = 1.5) -> int:
    sample = (np.random.pareto(alpha) + 1)
    product_index = int(num_products / sample)
    return max(1, min(product_index, num_products))


def hourly_traffic_multiplier(hour: int) -> float:
    return _HOURLY_MULTIPLIERS[hour % 24]


def _delivery_callback(err, msg):
    if err is not None:
        print(f"[kafka] delivery error: {err}", file=sys.stderr)


def make_producer(config) -> Producer:
    return Producer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "linger.ms": 5,
            "batch.num.messages": 1000,
            "compression.type": "snappy",
            "acks": "1",
        }
    )


def serialize_avro(schema_dict: dict, record: dict) -> bytes:
    parsed = fastavro.parse_schema(schema_dict)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    return buf.getvalue()
