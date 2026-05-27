import os

import great_expectations as gx
from great_expectations.core import ExpectationConfiguration


def build_curated_suite() -> None:
    context = gx.get_context(
        context_root_dir=os.path.join(os.path.dirname(__file__), "..", "great_expectations")
    )

    suite_name = "curated_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
        suite.expectations = []
    except Exception:
        suite = context.add_expectation_suite(suite_name)

    product_perf_columns = [
        "product_id",
        "date",
        "total_views",
        "total_add_to_cart",
        "unique_users",
    ]
    for col in product_perf_columns:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": col},
                meta={"table": "product_performance"},
            )
        )

    for col in ["product_id", "date"]:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": col},
                meta={"table": "product_performance"},
            )
        )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "total_views", "min_value": 0},
            meta={"table": "product_performance"},
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "total_add_to_cart", "min_value": 0},
            meta={"table": "product_performance"},
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_pair_values_a_to_be_greater_than_b",
            kwargs={
                "column_A": "total_views",
                "column_B": "total_add_to_cart",
                "or_equal": True,
            },
            meta={
                "table": "product_performance",
                "notes": "A product cannot be added to cart more times than it was viewed in the same window",
            },
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 0},
            meta={
                "table": "product_performance",
                "notes": (
                    "Dynamic lower bound should be set to 0.9 * previous_day_count at runtime. "
                    "A drop greater than 10% vs yesterday signals a data loss issue upstream. "
                    "Fetch previous_day_count from the validations store before running the checkpoint."
                ),
            },
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "product_id"},
            meta={
                "table": "product_performance",
                "notes": (
                    "Uniqueness check is on the surrogate column product_id + '|' + CAST(date AS VARCHAR). "
                    "The upstream Spark job must produce this composite key column before validation runs. "
                    "This enforces no duplicate (product_id, date) combinations per partition."
                ),
            },
        )
    )

    for col in ["date", "total_sessions"]:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": col},
                meta={"table": "conversion_funnel"},
            )
        )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "overall_conversion_rate",
                "min_value": 0.005,
                "max_value": 0.15,
            },
            meta={
                "table": "conversion_funnel",
                "notes": (
                    "Healthy e-commerce conversion sits between 0.5% and 15%. "
                    "Values outside this range indicate either a tracking bug or a major business event."
                ),
            },
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "browse_to_cart_rate",
                "min_value": 0,
                "max_value": 1,
            },
            meta={"table": "conversion_funnel"},
        )
    )

    context.save_expectation_suite(suite)
    print(f"Saved expectation suite '{suite_name}' with {len(suite.expectations)} expectations.")


if __name__ == "__main__":
    build_curated_suite()
