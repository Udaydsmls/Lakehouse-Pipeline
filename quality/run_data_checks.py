"""Data quality checks on the warehouse tables, using Great Expectations.

Each check pulls a table into pandas and runs a handful of expectations on it.
The script exits with a non-zero status if anything fails, so Airflow can stop
the pipeline when the numbers look wrong.

Usage:
    python quality/run_data_checks.py
"""

import os
import sys

import great_expectations as gx
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


def get_engine():
    url = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
        os.environ.get("POSTGRES_USER", "postgres"),
        os.environ.get("POSTGRES_PASSWORD", "postgres"),
        os.environ.get("POSTGRES_HOST", "localhost"),
        os.environ.get("POSTGRES_PORT", "5432"),
        os.environ.get("POSTGRES_DB", "ecommerce"),
    )
    return create_engine(url)


def make_validator(context, name, df):
    """Wrap a dataframe so expectations can be run against it."""
    datasource = context.sources.add_or_update_pandas(name)
    asset = datasource.add_dataframe_asset(name)
    suite = context.add_or_update_expectation_suite("%s_suite" % name)
    return context.get_validator(
        batch_request=asset.build_batch_request(dataframe=df),
        expectation_suite=suite,
    )


def check_orders(context, engine):
    df = pd.read_sql("SELECT * FROM marts.fct_orders", engine)
    validator = make_validator(context, "fct_orders", df)

    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_be_unique("order_id")
    validator.expect_column_values_to_not_be_null("user_id")
    validator.expect_column_values_to_be_between("net_amount", min_value=0)
    validator.expect_column_values_to_be_in_set(
        "payment_method",
        ["credit_card", "debit_card", "paypal", "bank_transfer"],
    )
    return validator.validate()


def check_funnel(context, engine):
    df = pd.read_sql("SELECT * FROM marts.mart_conversion_funnel", engine)
    validator = make_validator(context, "mart_conversion_funnel", df)

    validator.expect_column_values_to_not_be_null("date")
    validator.expect_column_values_to_be_unique("date")
    validator.expect_column_values_to_be_between("total_sessions", min_value=1)
    # A conversion rate outside 0-1 means the funnel maths is broken.
    validator.expect_column_values_to_be_between(
        "overall_conversion_rate", min_value=0, max_value=1
    )
    return validator.validate()


def check_user_features(context, engine):
    df = pd.read_sql("SELECT * FROM analytics.user_features", engine)
    validator = make_validator(context, "user_features", df)

    validator.expect_column_values_to_be_unique("user_id")
    validator.expect_column_values_to_be_between("rfm_recency_score", min_value=1, max_value=5)
    validator.expect_column_values_to_be_between("rfm_frequency_score", min_value=1, max_value=5)
    validator.expect_column_values_to_be_between("rfm_monetary_score", min_value=1, max_value=5)
    validator.expect_column_values_to_be_between("ltv_estimate", min_value=0)
    return validator.validate()


def main():
    engine = get_engine()
    # No great_expectations.yml in the repo, so this is an in-memory context.
    context = gx.get_context()

    checks = [
        ("fct_orders", check_orders),
        ("mart_conversion_funnel", check_funnel),
        ("user_features", check_user_features),
    ]

    failed = []
    for name, check in checks:
        result = check(context, engine)
        passed = sum(1 for r in result.results if r.success)
        total = len(result.results)
        status = "PASS" if result.success else "FAIL"
        print("%-25s %s (%d/%d expectations passed)" % (name, status, passed, total))

        if not result.success:
            failed.append(name)
            for r in result.results:
                if not r.success:
                    print("    failed: %s on %s" % (
                        r.expectation_config.expectation_type,
                        r.expectation_config.kwargs.get("column", "table"),
                    ))

    if failed:
        print("\nData quality checks failed for: %s" % ", ".join(failed))
        sys.exit(1)

    print("\nAll data quality checks passed.")


if __name__ == "__main__":
    main()
