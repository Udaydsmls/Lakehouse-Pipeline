import os
from datetime import datetime, timedelta, timezone

import great_expectations as gx
from great_expectations.core import ExpectationConfiguration


def build_raw_suite() -> None:
    context = gx.get_context(
        context_root_dir=os.path.join(os.path.dirname(__file__), "..", "great_expectations")
    )

    suite_name = "raw_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
        suite.expectations = []
    except Exception:
        suite = context.add_expectation_suite(suite_name)

    required_columns = ["event_id", "event_type", "user_id", "session_id", "timestamp"]
    for col in required_columns:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": col},
            )
        )

    not_null_columns = ["event_id", "event_type", "user_id", "timestamp"]
    for col in not_null_columns:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": col},
            )
        )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "event_type",
                "value_set": [
                    "page_view",
                    "product_view",
                    "search",
                    "add_to_cart",
                    "remove_from_cart",
                    "checkout_start",
                    "purchase",
                ],
            },
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_value_lengths_to_be_between",
            kwargs={
                "column": "event_id",
                "min_value": 36,
                "max_value": 36,
            },
            meta={"notes": "UUID v4 is always exactly 36 characters including hyphens"},
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_regex",
            kwargs={
                "column": "timestamp",
                "regex": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$",
            },
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 1000},
            meta={"notes": "Each micro-batch window should contain at least 1 000 events under normal load"},
        )
    )

    now_utc = datetime.now(tz=timezone.utc)
    window_min = (now_utc - timedelta(hours=24)).isoformat()
    window_max = (now_utc + timedelta(minutes=5)).isoformat()
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "timestamp",
                "min_value": window_min,
                "max_value": window_max,
                "parse_strings_as_datetimes": True,
            },
            meta={
                "notes": (
                    "Timestamps older than 24 h indicate a stalled producer or clock skew. "
                    "5-minute future buffer accounts for minor producer clock drift."
                )
            },
        )
    )

    context.save_expectation_suite(suite)
    print(f"Saved expectation suite '{suite_name}' with {len(suite.expectations)} expectations.")


if __name__ == "__main__":
    build_raw_suite()
