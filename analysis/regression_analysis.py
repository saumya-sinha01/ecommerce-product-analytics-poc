import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

from analysis.stats_utils import load_mart_user_outcomes


def main():
    df = load_mart_user_outcomes().copy()

    # Encode treatment
    df["is_treatment"] = (df["variant"] == "treatment").astype(int)

    # Basic sanity
    df["events_in_window"] = pd.to_numeric(df.get("events_in_window", 0), errors="coerce").fillna(0)

    # IMPORTANT:
    # Do NOT control for post-treatment mediators (add_to_cart/begin_checkout) when modeling purchased.
    # That can cause separation + biased treatment estimates.
    formula = "purchased ~ is_treatment + events_in_window"

    model = smf.logit(formula, data=df).fit(disp=True)

    print("\n=== Logistic Regression: purchased ~ is_treatment + events_in_window ===")
    print(model.summary())

    # Convert treatment coefficient to odds ratio
    coef = model.params["is_treatment"]
    oratio = np.exp(coef)
    print(f"\nTreatment coef: {coef:.4f}")
    print(f"Treatment odds ratio: {oratio:.4f}")

    # Approx marginal effect at mean (rough, but fine for a POC)
    # (This avoids get_margeff sometimes failing under edge cases)
    p_mean = model.predict(df[["is_treatment", "events_in_window"]]).mean()
    print(f"Mean predicted purchase probability: {p_mean:.4f}")

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
