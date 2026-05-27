import os
from dataclasses import dataclass, field
from datetime import date, timedelta

from dotenv import load_dotenv

load_dotenv()


def _yesterday() -> str:
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


@dataclass
class SparkJobConfig:
    spark_master: str = field(
        default_factory=lambda: os.environ.get("SPARK_MASTER", "local[*]")
    )
    s3_endpoint: str = field(
        default_factory=lambda: os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    )
    s3_bucket: str = field(
        default_factory=lambda: os.environ.get("S3_BUCKET", "lakehouse-raw")
    )
    aws_access_key_id: str = field(
        default_factory=lambda: os.environ.get("AWS_ACCESS_KEY_ID", "")
    )
    aws_secret_access_key: str = field(
        default_factory=lambda: os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    )
    iceberg_catalog_uri: str = field(
        default_factory=lambda: os.environ.get("ICEBERG_CATALOG_URI", "http://localhost:8181")
    )
    iceberg_warehouse: str = field(
        default_factory=lambda: os.environ.get(
            "ICEBERG_WAREHOUSE",
            f"s3a://{os.environ.get('S3_BUCKET', 'lakehouse-raw')}/warehouse",
        )
    )
    delta_warehouse: str = field(
        default_factory=lambda: os.environ.get(
            "DELTA_WAREHOUSE",
            f"s3a://{os.environ.get('S3_BUCKET', 'lakehouse-raw')}/curated",
        )
    )
    postgres_host: str = field(
        default_factory=lambda: os.environ.get("POSTGRES_HOST", "localhost")
    )
    postgres_port: int = field(
        default_factory=lambda: int(os.environ.get("POSTGRES_PORT", "5432"))
    )
    postgres_db: str = field(
        default_factory=lambda: os.environ.get("POSTGRES_DB", "ecommerce")
    )
    postgres_user: str = field(
        default_factory=lambda: os.environ.get("POSTGRES_USER", "postgres")
    )
    postgres_password: str = field(
        default_factory=lambda: os.environ.get("POSTGRES_PASSWORD", "")
    )
    snowflake_account: str = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_ACCOUNT", "")
    )
    snowflake_user: str = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_USER", "")
    )
    snowflake_password: str = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_PASSWORD", "")
    )
    snowflake_database: str = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_DATABASE", "LAKEHOUSE")
    )
    snowflake_warehouse: str = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    )
    processing_date: str = field(default_factory=_yesterday)

    @classmethod
    def from_env(cls) -> "SparkJobConfig":
        return cls()

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
