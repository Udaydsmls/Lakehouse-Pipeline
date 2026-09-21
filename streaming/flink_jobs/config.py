"""Settings shared by the Flink jobs, read from environment variables."""

import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CLICKSTREAM_TOPIC = os.environ.get("CLICKSTREAM_TOPIC", "clickstream.events")

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://lakehouse-raw/warehouse")

PARALLELISM = int(os.environ.get("FLINK_PARALLELISM", "2"))
CHECKPOINT_INTERVAL_MS = int(os.environ.get("CHECKPOINT_INTERVAL_MS", "60000"))

# A session ends after this much inactivity from the user.
SESSION_GAP_MINUTES = int(os.environ.get("SESSION_GAP_MINUTES", "30"))

# Window size for the product metrics job.
WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "5"))
