"""Settings for the Kafka producers. Values come from .env (see .env.example)."""

import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLICKSTREAM_TOPIC = os.environ.get("CLICKSTREAM_TOPIC", "clickstream.events")

# How many clickstream events to send per second.
EVENTS_PER_SECOND = int(os.environ.get("EVENTS_PER_SECOND", "50"))

# Size of the fake catalogue / user base the generator picks from.
NUM_PRODUCTS = int(os.environ.get("NUM_PRODUCTS", "50"))
NUM_USERS = int(os.environ.get("NUM_USERS", "2000"))
