from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime, timezone
from typing import Iterable, Iterator

from pyflink.common import Row, Time, Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import EventTimeSessionWindows, TimeWindow
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, Schema
from pyflink.table.types import DataTypes

from config import FlinkJobConfig
from iceberg_utils import (
    avro_deserializer,
    create_iceberg_catalog,
    create_user_sessions_table,
    ensure_database,
)

_SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")


def _load_schema(filename: str) -> dict:
    path = os.path.join(_SCHEMAS_DIR, filename)
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _parse_ts_ms(ts_str: str) -> int:
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def _ms_to_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.isoformat()


class ClickstreamTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        try:
            return _parse_ts_ms(value["timestamp"])
        except Exception:
            return record_timestamp


class SessionWindowFunction(ProcessWindowFunction):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[Row],
    ) -> Iterator[Row]:
        window: TimeWindow = context.window()

        events = list(elements)
        if not events:
            return

        events.sort(key=lambda r: r["timestamp"])

        first = events[0]
        session_id: str = first["session_id"]
        user_id: str = key
        country: str = first["country"]

        pages_viewed = 0
        products_viewed = 0
        searches = 0
        add_to_cart_count = 0
        checkout_started = False
        purchased = False
        device_counts: Counter[str] = Counter()

        for row in events:
            et = row["event_type"]
            device_counts[str(row["device_type"])] += 1

            if et == "page_view":
                pages_viewed += 1
            elif et == "product_view":
                products_viewed += 1
            elif et == "search":
                searches += 1
            elif et == "add_to_cart":
                add_to_cart_count += 1
            elif et == "checkout_start":
                checkout_started = True
            elif et == "purchase":
                purchased = True

        device_type = device_counts.most_common(1)[0][0] if device_counts else "unknown"

        session_start_ms = window.start
        session_end_ms = window.end
        duration_seconds = max(0, (session_end_ms - session_start_ms) // 1000)

        if purchased:
            outcome = "purchase"
        elif checkout_started:
            outcome = "checkout_abandoned"
        elif add_to_cart_count > 0:
            outcome = "cart_abandoned"
        else:
            outcome = "bounce"

        yield Row(
            session_id=session_id,
            user_id=user_id,
            session_start=_ms_to_iso(session_start_ms),
            session_end=_ms_to_iso(session_end_ms),
            duration_seconds=int(duration_seconds),
            pages_viewed=pages_viewed,
            products_viewed=products_viewed,
            searches=searches,
            add_to_cart_count=add_to_cart_count,
            checkout_started=checkout_started,
            purchased=purchased,
            outcome=outcome,
            device_type=device_type,
            country=country,
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


def _build_session_row_type() -> Types:
    return Types.ROW_NAMED(
        [
            "session_id",
            "user_id",
            "session_start",
            "session_end",
            "duration_seconds",
            "pages_viewed",
            "products_viewed",
            "searches",
            "add_to_cart_count",
            "checkout_started",
            "purchased",
            "outcome",
            "device_type",
            "country",
        ],
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
            Types.INT(),
            Types.INT(),
            Types.INT(),
            Types.INT(),
            Types.BOOLEAN(),
            Types.BOOLEAN(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
        ],
    )


def main() -> None:
    config = FlinkJobConfig.from_env()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.flink_parallelism)
    env.enable_checkpointing(config.checkpoint_interval_ms, CheckpointingMode.AT_LEAST_ONCE)

    env.get_checkpoint_config().set_checkpoint_storage_uri(
        f"{config.iceberg_warehouse}/checkpoints/sessioniser"
    )

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
    create_user_sessions_table(t_env)

    clickstream_schema = _load_schema("clickstream.avsc")

    kafka_props = {
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "group.id": "flink-sessioniser",
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

    session_row_type = _build_session_row_type()

    session_stream = (
        env.add_source(clickstream_source)
        .assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda row: row["user_id"])
        .window(
            EventTimeSessionWindows.with_gap(Time.minutes(config.session_gap_minutes))
        )
        .process(SessionWindowFunction(), output_type=session_row_type)
    )

    session_table = t_env.from_data_stream(
        session_stream,
        Schema.new_builder()
        .column("session_id", DataTypes.STRING())
        .column("user_id", DataTypes.STRING())
        .column("session_start", DataTypes.STRING())
        .column("session_end", DataTypes.STRING())
        .column("duration_seconds", DataTypes.INT())
        .column("pages_viewed", DataTypes.INT())
        .column("products_viewed", DataTypes.INT())
        .column("searches", DataTypes.INT())
        .column("add_to_cart_count", DataTypes.INT())
        .column("checkout_started", DataTypes.BOOLEAN())
        .column("purchased", DataTypes.BOOLEAN())
        .column("outcome", DataTypes.STRING())
        .column("device_type", DataTypes.STRING())
        .column("country", DataTypes.STRING())
        .build(),
    )

    session_table.execute_insert("lakehouse.raw.user_sessions")

    env.execute("sessioniser")


if __name__ == "__main__":
    main()
