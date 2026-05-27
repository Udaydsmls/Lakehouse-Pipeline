"""
Post-analysis module for A/B tests stored in Snowflake.
Reads ab_test_assignments table with columns:
  user_id, test_name, variant (control/treatment), assigned_at, acquisition_channel
Reads from fct_orders to get purchase outcomes.
"""

import argparse
import os
from typing import Any

import numpy as np
import pandas as pd
import snowflake.connector
from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test
from scipy import stats


def _snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "LAKEHOUSE"),
        schema="MARTS",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


def load_test_data(test_name: str, conn) -> pd.DataFrame:
    query = """
        SELECT
            a.user_id,
            a.variant,
            a.acquisition_channel,
            a.assigned_at,
            COUNT(DISTINCT o.order_id)                          AS purchase_count,
            COALESCE(SUM(o.net_amount), 0)                      AS total_spend,
            DATEDIFF(
                'day',
                a.assigned_at,
                MIN(o.ordered_at)
            )                                                    AS days_to_first_purchase,
            DATEDIFF(
                'day',
                MIN(o.ordered_at),
                NTH_VALUE(o.ordered_at, 2)
                    OVER (PARTITION BY a.user_id ORDER BY o.ordered_at)
            )                                                    AS days_to_second_purchase,
            CASE WHEN COUNT(DISTINCT o.order_id) >= 2
                 THEN TRUE ELSE FALSE END                        AS had_second_purchase
        FROM MARTS.ab_test_assignments a
        LEFT JOIN MARTS.fct_orders o
            ON  a.user_id    = o.user_id
            AND o.ordered_at >= a.assigned_at
        WHERE a.test_name = %(test_name)s
        GROUP BY
            a.user_id,
            a.variant,
            a.acquisition_channel,
            a.assigned_at
    """
    cursor = conn.cursor()
    cursor.execute(query, {"test_name": test_name})
    columns = [desc[0].lower() for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(rows, columns=columns)


def _cohens_d(a: pd.Series, b: pd.Series) -> float:
    pooled_std = np.sqrt((a.std() ** 2 + b.std() ** 2) / 2)
    if pooled_std == 0:
        return 0.0
    return float((a.mean() - b.mean()) / pooled_std)


def run_sequential_test(control: pd.Series, treatment: pd.Series, metric: str) -> dict[str, Any]:
    stat, p_value = stats.mannwhitneyu(treatment, control, alternative="two-sided")
    effect_size = _cohens_d(treatment, control)
    control_mean = control.mean()
    relative_lift = (
        ((treatment.mean() - control_mean) / control_mean * 100) if control_mean != 0 else float("nan")
    )
    return {
        "metric": metric,
        "control_mean": round(float(control.mean()), 4),
        "treatment_mean": round(float(treatment.mean()), 4),
        "statistic": round(float(stat), 4),
        "p_value": round(float(p_value), 6),
        "is_significant": bool(p_value < 0.05),
        "effect_size_cohens_d": round(effect_size, 4),
        "relative_lift_pct": round(float(relative_lift), 2),
    }


def run_causal_inference(df: pd.DataFrame) -> dict[str, Any]:
    try:
        import dowhy
        from dowhy import CausalModel
    except ImportError as exc:
        raise ImportError("dowhy is required for causal inference: pip install dowhy") from exc

    causal_graph = """
    digraph {
        acquisition_channel -> variant;
        acquisition_channel -> purchase_count;
        variant -> purchase_count;
    }
    """
    df_model = df[["variant", "acquisition_channel", "purchase_count"]].copy()
    df_model["variant_binary"] = (df_model["variant"] == "treatment").astype(int)

    channel_dummies = pd.get_dummies(df_model["acquisition_channel"], prefix="channel", drop_first=True)
    df_model = pd.concat([df_model, channel_dummies], axis=1)

    model = CausalModel(
        data=df_model,
        treatment="variant_binary",
        outcome="purchase_count",
        graph=causal_graph,
        common_causes=["acquisition_channel"],
    )

    identified_estimand = model.identify_effect(proceed_when_unidentifiable=True)
    estimate = model.estimate_effect(
        identified_estimand,
        method_name="backdoor.linear_regression",
        confidence_intervals=True,
    )

    refutation_random = model.refute_estimate(
        identified_estimand,
        estimate,
        method_name="random_common_cause",
    )
    refutation_placebo = model.refute_estimate(
        identified_estimand,
        estimate,
        method_name="placebo_treatment_refuter",
        placebo_type="permute",
    )

    refutation_passed = (
        refutation_random.new_effect is not None
        and abs(refutation_random.new_effect - estimate.value) < abs(estimate.value) * 0.2
        and refutation_placebo.new_effect is not None
        and abs(refutation_placebo.new_effect) < abs(estimate.value) * 0.1
    )

    ci = estimate.get_confidence_intervals()
    return {
        "ate": round(float(estimate.value), 4),
        "confidence_interval": [round(float(ci[0][0]), 4), round(float(ci[1][0]), 4)],
        "refutation_passed": refutation_passed,
        "refutation_random_new_effect": round(float(refutation_random.new_effect or 0), 4),
        "refutation_placebo_new_effect": round(float(refutation_placebo.new_effect or 0), 4),
    }


def run_survival_analysis(df: pd.DataFrame) -> dict[str, Any]:
    df_surv = df[["variant", "days_to_second_purchase", "had_second_purchase"]].copy()
    df_surv["duration"] = df_surv["days_to_second_purchase"].fillna(
        df_surv["days_to_second_purchase"].max() + 1
    )
    df_surv["event_observed"] = df_surv["had_second_purchase"].astype(bool)

    control_mask = df_surv["variant"] == "control"
    treatment_mask = df_surv["variant"] == "treatment"

    kmf_control = KaplanMeierFitter(label="control")
    kmf_control.fit(
        df_surv.loc[control_mask, "duration"],
        event_observed=df_surv.loc[control_mask, "event_observed"],
    )

    kmf_treatment = KaplanMeierFitter(label="treatment")
    kmf_treatment.fit(
        df_surv.loc[treatment_mask, "duration"],
        event_observed=df_surv.loc[treatment_mask, "event_observed"],
    )

    lr_result = logrank_test(
        df_surv.loc[control_mask, "duration"],
        df_surv.loc[treatment_mask, "duration"],
        event_observed_A=df_surv.loc[control_mask, "event_observed"],
        event_observed_B=df_surv.loc[treatment_mask, "event_observed"],
    )

    median_control = kmf_control.median_survival_time_
    median_treatment = kmf_treatment.median_survival_time_

    return {
        "median_ttp_control": float(median_control) if not np.isnan(median_control) else None,
        "median_ttp_treatment": float(median_treatment) if not np.isnan(median_treatment) else None,
        "logrank_p_value": round(float(lr_result.p_value), 6),
        "is_significant": bool(lr_result.p_value < 0.05),
    }


def run_full_analysis(test_name: str) -> dict[str, Any]:
    conn = _snowflake_conn()
    try:
        df = load_test_data(test_name, conn)
    finally:
        conn.close()

    if df.empty:
        raise ValueError(f"No data found for test '{test_name}'.")

    control = df[df["variant"] == "control"]
    treatment = df[df["variant"] == "treatment"]

    purchase_count_result = run_sequential_test(
        control["purchase_count"], treatment["purchase_count"], metric="purchase_count"
    )
    total_spend_result = run_sequential_test(
        control["total_spend"], treatment["total_spend"], metric="total_spend"
    )

    causal_result = run_causal_inference(df)
    survival_result = run_survival_analysis(df)

    results = {
        "test_name": test_name,
        "n_control": int(len(control)),
        "n_treatment": int(len(treatment)),
        "sequential_tests": {
            "purchase_count": purchase_count_result,
            "total_spend": total_spend_result,
        },
        "causal_inference": causal_result,
        "survival_analysis": survival_result,
    }

    _print_summary(results)
    return results


def _print_summary(results: dict[str, Any]) -> None:
    print(f"\n{'=' * 60}")
    print(f"A/B Test Analysis: {results['test_name']}")
    print(f"{'=' * 60}")
    print(f"Control n={results['n_control']}  |  Treatment n={results['n_treatment']}")

    print("\n--- Sequential Tests (Mann-Whitney U) ---")
    for metric, r in results["sequential_tests"].items():
        sig = "SIGNIFICANT" if r["is_significant"] else "not significant"
        print(
            f"  {metric}: control={r['control_mean']}  treatment={r['treatment_mean']}  "
            f"lift={r['relative_lift_pct']}%  p={r['p_value']}  [{sig}]"
        )

    print("\n--- Causal Inference (DoWhy backdoor) ---")
    ci = results["causal_inference"]
    ref_status = "PASSED" if ci["refutation_passed"] else "FAILED"
    print(
        f"  ATE={ci['ate']}  CI=[{ci['confidence_interval'][0]}, {ci['confidence_interval'][1]}]  "
        f"refutations={ref_status}"
    )

    print("\n--- Survival Analysis (Kaplan-Meier, time to 2nd purchase) ---")
    sa = results["survival_analysis"]
    sig = "SIGNIFICANT" if sa["is_significant"] else "not significant"
    print(
        f"  median_control={sa['median_ttp_control']} days  "
        f"median_treatment={sa['median_ttp_treatment']} days  "
        f"logrank_p={sa['logrank_p_value']}  [{sig}]"
    )
    print(f"{'=' * 60}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run post-analysis for an A/B test stored in Snowflake")
    parser.add_argument("--test-name", required=True, help="Value of ab_test_assignments.test_name to analyse")
    args = parser.parse_args()
    run_full_analysis(args.test_name)


if __name__ == "__main__":
    main()
