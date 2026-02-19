import numpy as np
import pandas as pd
import os

SRC = os.environ.get("SDOH_SOURCE", "data/sdoh_data_adjusted.csv" if os.path.exists("data/sdoh_data_adjusted.csv") else "data/sdoh_data.csv")
OUT = os.environ.get("SDOH_OUT", "data/sdoh_data_adjusted.csv")

np.random.seed(42)

target = {
    "Extreme": 0.60,
    "Mild": 0.15,
    "Significant": 0.10,
    "Protective": 0.15,
}

# ranges for sdoh_lift
ranges = {
    "Extreme": (0.20, 0.50),
    "Significant": (0.10, 0.20),
    "Mild": (0.00, 0.10),
    "Protective": (-0.20, -0.01),
}

labels = {
    "Extreme": "Extreme SDOH Contribution",
    "Significant": "Significant SDOH Contribution",
    "Mild": "Mild SDOH Contribution",
    "Protective": "SDOH Protective / No Impact",
}


def main():
    df = pd.read_csv(SRC)
    n = len(df)

    # determine counts
    counts = {k: int(round(v * n)) for k, v in target.items()}
    # fix rounding to sum n
    diff = n - sum(counts.values())
    if diff != 0:
        # adjust Extreme count to fit
        counts["Extreme"] += diff

    # sort by current lift to assign categories in a stable way
    df = df.sort_values("sdoh_lift").reset_index(drop=True)

    # assign categories by position
    boundaries = {}
    idx = 0
    for k in ["Protective", "Mild", "Significant", "Extreme"]:
        boundaries[k] = (idx, idx + counts[k])
        idx += counts[k]

    new_lifts = np.zeros(n)
    new_levels = [""] * n

    for k, (lo, hi) in ranges.items():
        start, end = boundaries[k]
        size = max(end - start, 0)
        if size <= 0:
            continue
        new_lifts[start:end] = np.random.uniform(lo, hi, size=size)
        new_levels[start:end] = [labels[k]] * size

    df["sdoh_lift"] = new_lifts
    df["sdoh_lift_level"] = new_levels

    # update risk_full to stay consistent
    if "risk_no_sdoh" in df.columns:
        df["risk_full"] = df["risk_no_sdoh"] + df["sdoh_lift"]

    # optional clip to plausible range
    df["risk_full"] = df["risk_full"].clip(lower=1.5, upper=3.0)

    df.to_csv(OUT, index=False)
    print("Updated lift distribution written to", OUT)


if __name__ == "__main__":
    main()
