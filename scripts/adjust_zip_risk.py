import os
import numpy as np
import pandas as pd

SRC = os.environ.get("SDOH_SOURCE", "data/sdoh_data.csv")
OUT = os.environ.get("SDOH_OUT", "data/sdoh_data_adjusted.csv")

# Band targets for ZIP-level average risk_full
TARGETS = {
    "High": 2.25,
    "Moderate": 2.0,
    "Low": 1.7
}

# Floors/ceilings to reduce overlap within ZIPs
BAND_FLOOR = {"High": 2.1, "Moderate": 1.85, "Low": 1.5}
BAND_CEIL = {"High": 3.0, "Moderate": 2.25, "Low": 1.85}

# SDOH lift level thresholds
LIFT_THRESHOLDS = {
    "extreme": 0.2,
    "significant": 0.1,
    "mild": 0.0
}

LABELS = {
    "extreme": "Extreme SDOH Contribution",
    "significant": "Significant SDOH Contribution",
    "mild": "Mild SDOH Contribution",
    "protective": "SDOH Protective / No Impact"
}


def assign_lift_level(lift: float) -> str:
    if lift is None or pd.isna(lift):
        return "SDOH level pending"
    if lift >= LIFT_THRESHOLDS["extreme"]:
        return LABELS["extreme"]
    if lift >= LIFT_THRESHOLDS["significant"]:
        return LABELS["significant"]
    if lift >= LIFT_THRESHOLDS["mild"]:
        return LABELS["mild"]
    return LABELS["protective"]


def main():
    df = pd.read_csv(SRC)

    # Compute ZIP average risk_full
    zip_avg = df.groupby("zip")["risk_full"].mean().sort_values()

    # Assign bands by quantiles (low/mid/high)
    q1 = zip_avg.quantile(0.33)
    q2 = zip_avg.quantile(0.66)

    zip_band = {}
    for z, avg in zip_avg.items():
        if avg >= q2:
            zip_band[z] = "High"
        elif avg >= q1:
            zip_band[z] = "Moderate"
        else:
            zip_band[z] = "Low"

    # Apply per-ZIP adjustment
    def adjust_row(row):
        band = zip_band.get(row["zip"], "Moderate")
        target = TARGETS[band]
        # shift risk_full and risk_score_x
        return band, target

    # Build adjustments
    df["zip_band_target"] = df["zip"].map(zip_band)
    df["zip_target"] = df["zip_band_target"].map(TARGETS)
    zip_current_avg = df.groupby("zip")["risk_full"].transform("mean")
    delta = df["zip_target"] - zip_current_avg

    # Apply delta
    df["risk_full"] = df["risk_full"] + delta
    if "risk_score_x" in df.columns:
        df["risk_score_x"] = df["risk_score_x"] + delta

    # Apply band floors/ceilings to reduce overlap
    floor = df["zip_band_target"].map(BAND_FLOOR)
    ceil = df["zip_band_target"].map(BAND_CEIL)
    df["risk_full"] = df["risk_full"].clip(lower=floor, upper=ceil)
    if "risk_score_x" in df.columns:
        df["risk_score_x"] = df["risk_score_x"].clip(lower=floor, upper=ceil)

    # Recompute sdoh_lift for consistency
    if "risk_no_sdoh" in df.columns:
        df["sdoh_lift"] = df["risk_full"] - df["risk_no_sdoh"]
        df["sdoh_lift_level"] = df["sdoh_lift"].apply(assign_lift_level)

    # Cleanup helper columns
    df = df.drop(columns=["zip_band_target", "zip_target"])

    df.to_csv(OUT, index=False)
    print(f"Adjusted data written to {OUT}")


if __name__ == "__main__":
    main()
