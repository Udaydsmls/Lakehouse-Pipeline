"""Settings for the Spark batch jobs, read from environment variables."""

import os

from dotenv import load_dotenv

load_dotenv()

SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://localhost:8181")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3a://lakehouse-raw/warehouse")

# Where the curated Delta tables live.
DELTA_WAREHOUSE = os.environ.get("DELTA_WAREHOUSE", "s3a://lakehouse-curated")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ecommerce")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

JDBC_URL = "jdbc:postgresql://%s:%s/%s" % (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)

JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}
