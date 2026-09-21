"""Rolls clickstream events up into 5-minute counts per product.

This is the "real-time metrics" part of the pipeline: while the sessioniser
waits for a session to close, this job emits a row every 5 minutes so you can
see what is trending right now.

Submit with:
    docker exec flink-jobmanager flink run -py /opt/flink/jobs/flink_jobs/windowed_aggregations.py
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

import config
from iceberg_utils import create_iceberg_catalog, create_kafka_source

METRICS_TABLE = """
    CREATE TABLE IF NOT EXISTS lakehouse.raw.product_metrics_5min (
        window_start   TIMESTAMP(3),
        window_end     TIMESTAMP(3),
        event_date     DATE,
        product_id     STRING,
        views          BIGINT,
        add_to_cart    BIGINT,
        purchases      BIGINT,
        unique_users   BIGINT
    )
"""

# TUMBLE gives fixed, non-overlapping 5-minute buckets. Events without a
# product id (plain page views, searches) are skipped.
METRICS_QUERY = """
    INSERT INTO lakehouse.raw.product_metrics_5min
    SELECT
        window_start,
        window_end,
        CAST(window_start AS DATE)                            AS event_date,
        product_id,
        COUNT(*) FILTER (WHERE event_type = 'product_view')   AS views,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart')    AS add_to_cart,
        COUNT(*) FILTER (WHERE event_type = 'purchase')       AS purchases,
        COUNT(DISTINCT user_id)                               AS unique_users
    FROM TABLE(
        TUMBLE(TABLE clickstream, DESCRIPTOR(event_time), INTERVAL '{minutes}' MINUTE)
    )
    WHERE product_id IS NOT NULL
    GROUP BY window_start, window_end, product_id
"""


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.PARALLELISM)
    env.enable_checkpointing(config.CHECKPOINT_INTERVAL_MS)

    t_env = StreamTableEnvironment.create(env)

    create_kafka_source(t_env, "clickstream", consumer_group="flink-product-metrics")
    create_iceberg_catalog(t_env)
    t_env.execute_sql(METRICS_TABLE)

    t_env.execute_sql(METRICS_QUERY.format(minutes=config.WINDOW_MINUTES)).wait()


if __name__ == "__main__":
    main()
