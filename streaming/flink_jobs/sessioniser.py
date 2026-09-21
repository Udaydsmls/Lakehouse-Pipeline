"""Groups clickstream events into user sessions and writes them to Iceberg.

A session is all the events from one user with no gap longer than 30 minutes.
Flink's SESSION window does the grouping; the rest is counting event types and
labelling how the session ended.

Submit with:
    docker exec flink-jobmanager flink run -py /opt/flink/jobs/flink_jobs/sessioniser.py
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

import config
from iceberg_utils import create_iceberg_catalog, create_kafka_source

SESSIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS lakehouse.raw.user_sessions (
        session_id        STRING,
        user_id           STRING,
        session_start     TIMESTAMP(3),
        session_end       TIMESTAMP(3),
        session_date      DATE,
        duration_seconds  BIGINT,
        pages_viewed      BIGINT,
        products_viewed   BIGINT,
        searches          BIGINT,
        add_to_cart_count BIGINT,
        checkout_started  BOOLEAN,
        purchased         BOOLEAN,
        outcome           STRING,
        device_type       STRING,
        country           STRING
    )
"""

# One row per (user, session window). `outcome` is how far down the funnel the
# user got, which is what the conversion reports downstream are built on.
SESSION_QUERY = """
    INSERT INTO lakehouse.raw.user_sessions
    SELECT
        MIN(session_id)                                                   AS session_id,
        user_id,
        SESSION_START(event_time, INTERVAL '{gap}' MINUTE)                AS session_start,
        SESSION_END(event_time, INTERVAL '{gap}' MINUTE)                  AS session_end,
        CAST(SESSION_START(event_time, INTERVAL '{gap}' MINUTE) AS DATE)  AS session_date,
        TIMESTAMPDIFF(
            SECOND,
            SESSION_START(event_time, INTERVAL '{gap}' MINUTE),
            SESSION_END(event_time, INTERVAL '{gap}' MINUTE)
        )                                                                 AS duration_seconds,
        COUNT(*) FILTER (WHERE event_type = 'page_view')                  AS pages_viewed,
        COUNT(*) FILTER (WHERE event_type = 'product_view')               AS products_viewed,
        COUNT(*) FILTER (WHERE event_type = 'search')                     AS searches,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart')                AS add_to_cart_count,
        COUNT(*) FILTER (WHERE event_type = 'checkout_start') > 0         AS checkout_started,
        COUNT(*) FILTER (WHERE event_type = 'purchase') > 0               AS purchased,
        CASE
            WHEN COUNT(*) FILTER (WHERE event_type = 'purchase') > 0       THEN 'purchase'
            WHEN COUNT(*) FILTER (WHERE event_type = 'checkout_start') > 0 THEN 'checkout_abandoned'
            WHEN COUNT(*) FILTER (WHERE event_type = 'add_to_cart') > 0    THEN 'cart_abandoned'
            ELSE 'bounce'
        END                                                               AS outcome,
        MIN(device_type)                                                  AS device_type,
        MIN(country)                                                      AS country
    FROM clickstream
    GROUP BY user_id, SESSION(event_time, INTERVAL '{gap}' MINUTE)
"""


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.PARALLELISM)
    env.enable_checkpointing(config.CHECKPOINT_INTERVAL_MS)

    t_env = StreamTableEnvironment.create(env)

    create_kafka_source(t_env, "clickstream", consumer_group="flink-sessioniser")
    create_iceberg_catalog(t_env)
    t_env.execute_sql(SESSIONS_TABLE)

    t_env.execute_sql(SESSION_QUERY.format(gap=config.SESSION_GAP_MINUTES)).wait()


if __name__ == "__main__":
    main()
