import os
from datetime import datetime, timezone

import great_expectations as gx
from great_expectations.core import ExpectationConfiguration


def build_warehouse_suite() -> None:
    context = gx.get_context(
        context_root_dir=os.path.join(os.path.dirname(__file__), "..", "great_expectations")
    )

    suite_name = "warehouse_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
        suite.expectations = []
    except Exception:
        suite = context.add_expectation_suite(suite_name)

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "overall_conversion_rate",
                "min_value": 0.005,
                "max_value": 0.15,
            },
            meta={"table": "mart_conversion_funnel"},
        )
    )

    for col in ["date", "revenue_per_user"]:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": col},
                meta={"table": "mart_conversion_funnel"},
            )
        )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "daily_revenue", "min_value": 0},
            meta={"table": "mart_conversion_funnel"},
        )
    )

    for col in ["order_id", "user_id", "ordered_at", "net_amount"]:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": col},
                meta={"table": "fct_orders"},
            )
        )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "net_amount", "min_value": 0},
            meta={"table": "fct_orders"},
        )
    )

    now_iso = datetime.now(tz=timezone.utc).isoformat()
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "ordered_at",
                "max_value": now_iso,
                "parse_strings_as_datetimes": True,
            },
            meta={
                "table": "fct_orders",
                "notes": "Future-dated orders indicate a producer clock error or a data load mistake",
            },
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "payment_method",
                "value_set": [
                    "credit_card",
                    "debit_card",
                    "paypal",
                    "apple_pay",
                    "google_pay",
                    "bank_transfer",
                    "gift_card",
                    "crypto",
                ],
            },
            meta={"table": "fct_orders"},
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "ltv_estimate", "min_value": 0},
            meta={"table": "dim_users"},
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "rfm_segment",
                "value_set": [
                    "champions",
                    "loyal_customers",
                    "potential_loyalists",
                    "recent_customers",
                    "promising",
                    "need_attention",
                    "about_to_sleep",
                    "at_risk",
                    "cant_lose_them",
                    "hibernating",
                    "lost",
                ],
            },
            meta={
                "table": "dim_users",
                "notes": "Segment names mirror the RFM scoring matrix defined in dbt/models/marts/dim_users.sql",
            },
        )
    )

    context.save_expectation_suite(suite)
    print(f"Saved expectation suite '{suite_name}' with {len(suite.expectations)} expectations.")


if __name__ == "__main__":
    build_warehouse_suite()
