from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable, Iterator, Tuple

import fastavro
import io

from pyflink.common import Row, Time, Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, Schema
from pyflink.table.catalog import ObjectPath
from pyflink.table.confluent import ConfluentSettings
from pyflink.table.types import DataTypes

from config import FlinkJobConfig
from iceberg_utils import (
    avro_deserializer,
    create_iceberg_catalog,
    create_windowed_aggregations_table,
    create_windowed_user_aggregations_table,
    ensure_database,
)

_SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")


def _load_schema(filename: str) -> dict:
    path = os.path.join(_SCHEMAS_DIR, filename)
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _parse_ts(ts_str: str) -> int:
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


class ClickstreamTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        try:
            return _parse_ts(value["timestamp"])
        except Exception:
            return record_timestamp


class CartEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        try:
            return _parse_ts(value["timestamp"])
        except Exception:
            return record_timestamp


class ProductWindowFunction(ProcessWindowFunction):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[Row],
    ) -> Iterator[Row]:
        window: TimeWindow = context.window()
        window_start = datetime.fromtimestamp(window.start / 1000, tz=timezone.utc)
        window_end = datetime.fromtimestamp(window.end / 1000, tz=timezone.utc)

        views = 0
        add_to_cart = 0
        cart_user_ids: set[str] = set()
        purchase_user_ids: set[str] = set()
        unique_users: set[str] = set()

        for row in elements:
            event_type = row["event_type"]
            user_id = row["user_id"]
            unique_users.add(user_id)

            if event_type in ("page_view", "product_view"):
                views += 1
            if event_type == "add_to_cart":
                add_to_cart += 1
                cart_user_ids.add(user_id)
            if event_type == "purchase":
                purchase_user_ids.add(user_id)

        abandoned_users = cart_user_ids - purchase_user_ids
        abandonment_rate = (
            len(abandoned_users) / len(cart_user_ids)
            if cart_user_ids
            else 0.0
        )

        yield Row(
            window_start=window_start,
            window_end=window_end,
            product_id=key,
            views_5m=views,
            add_to_cart_5m=add_to_cart,
            cart_abandonment_rate_5m=abandonment_rate,
            unique_users_5m=len(unique_users),
        )


class UserWindowFunction(ProcessWindowFunction):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[Row],
    ) -> Iterator[Row]:
        window: TimeWindow = context.window()
        window_start = datetime.fromtimestamp(window.start / 1000, tz=timezone.utc)
        window_end = datetime.fromtimestamp(window.end / 1000, tz=timezone.utc)

        total_events = 0
        page_views = 0
        product_views = 0
        searches = 0
        add_to_cart = 0
        purchases = 0

        for row in elements:
            total_events += 1
            et = row["event_type"]
            if et == "page_view":
                page_views += 1
            elif et == "product_view":
                product_views += 1
            elif et == "search":
                searches += 1
            elif et == "add_to_cart":
                add_to_cart += 1
            elif et == "purchase":
                purchases += 1

        yield Row(
            window_start=window_start,
            window_end=window_end,
            user_id=key,
            events_5m=total_events,
            page_views_5m=page_views,
            product_views_5m=product_views,
            searches_5m=searches,
            add_to_cart_5m=add_to_cart,
            purchases_5m=purchases,
        )


def _build_clickstream_row_type() -> Types:
    return Types.ROW_NAMED(
        [
            "event_id",
            "event_type",
            "user_id",
            "session_id",
            "product_id",
            "page_url",
            "referrer",
            "device_type",
            "user_agent",
            "ip_address",
            "country",
            "timestamp",
        ],
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
        ],
    )


def _make_clickstream_deserializer(schema_dict: dict):
    from pyflink.common.serialization import DeserializationSchema
    from pyflink.java_gateway import get_gateway

    raw_deserializer = avro_deserializer(schema_dict)
    row_type = _build_clickstream_row_type()

    class ClickstreamDeserializationSchema(DeserializationSchema):
        def deserialize(self, message: bytes) -> Row:
            record = raw_deserializer(message)
            return Row(
                event_id=record.get("event_id", ""),
                event_type=str(record.get("event_type", "")),
                user_id=record.get("user_id", ""),
                session_id=record.get("session_id", ""),
                product_id=record.get("product_id") or "",
                page_url=record.get("page_url", ""),
                referrer=record.get("referrer") or "",
                device_type=str(record.get("device_type", "")),
                user_agent=record.get("user_agent", ""),
                ip_address=record.get("ip_address", ""),
                country=record.get("country", ""),
                timestamp=record.get("timestamp", ""),
            )

        def is_end_of_stream(self, next_element) -> bool:
            return False

        def get_produced_type(self):
            return row_type

    return ClickstreamDeserializationSchema()


def main() -> None:
    config = FlinkJobConfig.from_env()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.flink_parallelism)
    env.enable_checkpointing(config.checkpoint_interval_ms, CheckpointingMode.AT_LEAST_ONCE)

    env.get_checkpoint_config().set_checkpoint_storage_uri(
        f"{config.iceberg_warehouse}/checkpoints/windowed-aggregations"
    )

    hadoop_conf = env.get_config().get_global_job_parameters() or {}
    env.get_config().set_global_job_parameters(
        {
            "fs.s3a.endpoint": config.s3_endpoint,
            "fs.s3a.access.key": config.aws_access_key_id,
            "fs.s3a.secret.key": config.aws_secret_access_key,
            "fs.s3a.path.style.access": "true",
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }
    )

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    create_iceberg_catalog(config, t_env)
    ensure_database(t_env, "lakehouse", "raw")
    create_windowed_aggregations_table(t_env)
    create_windowed_user_aggregations_table(t_env)

    clickstream_schema = _load_schema("clickstream.avsc")

    kafka_props = {
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "group.id": "flink-windowed-aggregations",
        "auto.offset.reset": "earliest",
    }

    clickstream_source = FlinkKafkaConsumer(
        topics="clickstream.raw",
        deserialization_schema=_make_clickstream_deserializer(clickstream_schema),
        properties=kafka_props,
    )

    out_of_orderness = Time.milliseconds(config.watermark_max_out_of_orderness_ms)

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(out_of_orderness)
        .with_timestamp_assigner(ClickstreamTimestampAssigner())
        .with_idleness(Time.seconds(30))
    )

    clickstream_stream = (
        env.add_source(clickstream_source)
        .assign_timestamps_and_watermarks(watermark_strategy)
    )

    product_agg_type = Types.ROW_NAMED(
        [
            "window_start",
            "window_end",
            "product_id",
            "views_5m",
            "add_to_cart_5m",
            "cart_abandonment_rate_5m",
            "unique_users_5m",
        ],
        [
            Types.SQL_TIMESTAMP(),
            Types.SQL_TIMESTAMP(),
            Types.STRING(),
            Types.LONG(),
            Types.LONG(),
            Types.DOUBLE(),
            Types.LONG(),
        ],
    )

    product_aggregations = (
        clickstream_stream
        .filter(lambda row: row["product_id"] != "")
        .key_by(lambda row: row["product_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(config.window_size_minutes)))
        .process(ProductWindowFunction(), output_type=product_agg_type)
    )

    product_table = t_env.from_data_stream(
        product_aggregations,
        Schema.new_builder()
        .column("window_start", DataTypes.TIMESTAMP(3))
        .column("window_end", DataTypes.TIMESTAMP(3))
        .column("product_id", DataTypes.STRING())
        .column("views_5m", DataTypes.BIGINT())
        .column("add_to_cart_5m", DataTypes.BIGINT())
        .column("cart_abandonment_rate_5m", DataTypes.DOUBLE())
        .column("unique_users_5m", DataTypes.BIGINT())
        .build(),
    )
    product_table.execute_insert("lakehouse.raw.windowed_product_aggregations")

    user_agg_type = Types.ROW_NAMED(
        [
            "window_start",
            "window_end",
            "user_id",
            "events_5m",
            "page_views_5m",
            "product_views_5m",
            "searches_5m",
            "add_to_cart_5m",
            "purchases_5m",
        ],
        [
            Types.SQL_TIMESTAMP(),
            Types.SQL_TIMESTAMP(),
            Types.STRING(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
        ],
    )

    user_aggregations = (
        clickstream_stream
        .key_by(lambda row: row["user_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(config.window_size_minutes)))
        .process(UserWindowFunction(), output_type=user_agg_type)
    )

    user_table = t_env.from_data_stream(
        user_aggregations,
        Schema.new_builder()
        .column("window_start", DataTypes.TIMESTAMP(3))
        .column("window_end", DataTypes.TIMESTAMP(3))
        .column("user_id", DataTypes.STRING())
        .column("events_5m", DataTypes.BIGINT())
        .column("page_views_5m", DataTypes.BIGINT())
        .column("product_views_5m", DataTypes.BIGINT())
        .column("searches_5m", DataTypes.BIGINT())
        .column("add_to_cart_5m", DataTypes.BIGINT())
        .column("purchases_5m", DataTypes.BIGINT())
        .build(),
    )
    user_table.execute_insert("lakehouse.raw.windowed_user_aggregations")

    env.execute("windowed-aggregations")


if __name__ == "__main__":
    main()
