"""
explore_training_data.py — AirBreda

Loads training_data.csv, prints how many joined rows there are, and saves a
scatter plot of NO2 against total traffic intensity (no2_vs_intensity.png).

Needs matplotlib, which isn't in requirements.txt on purpose: the ingestion
images don't use it. Install it locally with `pip install matplotlib`.
"""

import os
import sys

import matplotlib.pyplot as plt
import pandas as pd

INPUT_PATH = "training_data.csv"
PLOT_PATH = "plots/no2_vs_intensity.png"


def main() -> int:
    df = pd.read_csv(INPUT_PATH, parse_dates=["timestamp"])
    print(f"Joined rows: {len(df)}")
    if df.empty:
        return 0
    print(f"From {df['timestamp'].min()} to {df['timestamp'].max()}")
    if len(df) >= 3:
        r = df["no2_ug_m3"].corr(df["total_intensity_veh_per_hr"])
        print(f"Pearson r (NO2 vs total intensity): {r:.2f}")

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(df["total_intensity_veh_per_hr"], df["no2_ug_m3"])
    for _, row in df.iterrows():
        ax.annotate(f"{row['hour_of_day']:02d}h",
                    (row["total_intensity_veh_per_hr"], row["no2_ug_m3"]),
                    textcoords="offset points", xytext=(4, 4), fontsize=8)
    ax.set_xlabel("Total intensity, 4 A27 sites (veh/h)")
    ax.set_ylabel("NO2 at NL10240 (µg/m³)")
    ax.set_title(f"NO2 vs A27 traffic (n = {len(df)})")
    fig.tight_layout()
    os.makedirs(os.path.dirname(PLOT_PATH), exist_ok=True)
    fig.savefig(PLOT_PATH, dpi=150)
    print(f"Saved {PLOT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
