from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class FlinkJobConfig:
    kafka_bootstrap_servers: str
    schema_registry_url: str
    s3_endpoint: str
    s3_bucket: str
    aws_access_key_id: str
    aws_secret_access_key: str
    iceberg_catalog_uri: str
    iceberg_warehouse: str
    flink_parallelism: int = 4
    checkpoint_interval_ms: int = 60000
    watermark_max_out_of_orderness_ms: int = 5000
    window_size_minutes: int = 5
    session_gap_minutes: int = 30

    @classmethod
    def from_env(cls) -> FlinkJobConfig:
        return cls(
            kafka_bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            schema_registry_url=os.environ["SCHEMA_REGISTRY_URL"],
            s3_endpoint=os.environ["S3_ENDPOINT"],
            s3_bucket=os.environ["S3_BUCKET"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            iceberg_catalog_uri=os.environ["ICEBERG_CATALOG_URI"],
            iceberg_warehouse=os.environ["ICEBERG_WAREHOUSE"],
            flink_parallelism=int(os.environ.get("FLINK_PARALLELISM", "4")),
            checkpoint_interval_ms=int(os.environ.get("CHECKPOINT_INTERVAL_MS", "60000")),
            watermark_max_out_of_orderness_ms=int(os.environ.get("WATERMARK_MAX_OUT_OF_ORDERNESS_MS", "5000")),
            window_size_minutes=int(os.environ.get("WINDOW_SIZE_MINUTES", "5")),
            session_gap_minutes=int(os.environ.get("SESSION_GAP_MINUTES", "30")),
        )
