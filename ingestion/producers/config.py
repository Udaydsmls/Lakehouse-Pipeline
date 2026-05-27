import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ProducerConfig:
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    schema_registry_url: str = field(
        default_factory=lambda: os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    )
    topics: dict = field(
        default_factory=lambda: {
            "clickstream_raw": "clickstream.raw",
            "cart_events": "cart.events",
            "order_events": "order.events",
            "user_events": "user.events",
            "search_events": "search.events",
            "inventory_events": "inventory.events",
        }
    )
    events_per_second: int = field(
        default_factory=lambda: int(os.environ.get("EVENTS_PER_SECOND", "100"))
    )
    num_products: int = field(
        default_factory=lambda: int(os.environ.get("NUM_PRODUCTS", "10000"))
    )
    num_users: int = field(
        default_factory=lambda: int(os.environ.get("NUM_USERS", "500000"))
    )
    pareto_alpha: float = field(
        default_factory=lambda: float(os.environ.get("PARETO_ALPHA", "1.5"))
    )
