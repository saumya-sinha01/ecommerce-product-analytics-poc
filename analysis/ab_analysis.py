import numpy as np
import pandas as pd
from scipy import stats

from analysis.stats_utils import load_mart_user_outcomes


def conversion_summary(df):
    summary = (
        df.groupby("variant")
        .agg(
            users=("user_id", "count"),
            conversions=("purchased", "sum"),
            conversion_rate=("purchased", "mean"),
            revenue_per_user=("revenue", "mean"),
        )
        .reset_index()
    )
    return summary


def two_proportion_z_test(df):
    control = df[df.variant == "control"]
    treatment = df[df.variant == "treatment"]

    x1 = control.purchased.sum()
    n1 = len(control)

    x2 = treatment.purchased.sum()
    n2 = len(treatment)

    p1 = x1 / n1
    p2 = x2 / n2

    p_pool = (x1 + x2) / (n1 + n2)
    se = np.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2))
    z = (p2 - p1) / se
    p_value = 2 * (1 - stats.norm.cdf(abs(z)))

    return {
        "control_rate": p1,
        "treatment_rate": p2,
        "lift": p2 - p1,
        "z_score": z,
        "p_value": p_value,
    }


def confidence_interval(df, alpha=0.05):
    control = df[df.variant == "control"].purchased
    treatment = df[df.variant == "treatment"].purchased

    diff = treatment.mean() - control.mean()

    se = np.sqrt(
        treatment.var() / len(treatment)
        + control.var() / len(control)
    )

    z = stats.norm.ppf(1 - alpha / 2)
    lower = diff - z * se
    upper = diff + z * se

    return diff, (lower, upper)


def main():
    df = load_mart_user_outcomes()

    print("\n=== Conversion Summary ===")
    print(conversion_summary(df))

    print("\n=== Hypothesis Test (Two-Proportion Z-Test) ===")
    test = two_proportion_z_test(df)
    for k, v in test.items():
        print(f"{k}: {v:.4f}")

    print("\n=== 95% Confidence Interval (Lift) ===")
    diff, ci = confidence_interval(df)
    print(f"Lift: {diff:.4f}")
    print(f"95% CI: ({ci[0]:.4f}, {ci[1]:.4f})")


if __name__ == "__main__":
    main()
