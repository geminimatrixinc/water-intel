"""
Anomaly Driver Hints — Top Contributing Features

For each flagged anomaly, compute per-feature z-scores relative to the full
population and identify the top 3 features that drove the anomaly flag.

Adds two columns to anomalies.csv:
    top_features      — comma-separated feature names ranked by |z-score|
    top_feature_values — corresponding z-score values

Usage:
    python -m src.models.driver_hints
    python -m src.models.driver_hints --top-n 3
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # water-intel/
ML_ROOT = PROJECT_ROOT / "ml"
sys.path.insert(0, str(ML_ROOT))

# Defaults
DEFAULT_FEATURES = PROJECT_ROOT / "data" / "processed" / "grand_river_features.csv"
DEFAULT_ANOMALIES = PROJECT_ROOT / "outputs" / "anomalies.csv"

# Same feature columns used by the Isolation Forest
FEATURE_COLS = [
    "delta",
    "time_gap_days",
    "rate_of_change",
    "is_gap",
    "rolling_mean_7",
    "rolling_std_7",
    "rolling_mean_14",
    "rolling_std_14",
    "rolling_mean_30",
    "rolling_std_30",
    "zscore",
]


def compute_driver_hints(
    features_path: Path,
    anomalies_path: Path,
    top_n: int = 3,
) -> pd.DataFrame:
    """
    Enrich anomalies with driver hints.

    Approach:
    1. Load the full features matrix and compute population mean/std per feature
    2. Load anomalies and join back to features on (station_id, timestamp, parameter)
    3. For each anomaly row, compute per-feature z-scores vs population
    4. Rank features by |z-score| and keep top N
    5. Write enriched anomalies CSV
    """
    print(f"Loading features from {features_path}")
    features_df = pd.read_csv(features_path)
    print(f"  {len(features_df)} rows")

    print(f"Loading anomalies from {anomalies_path}")
    anomalies_df = pd.read_csv(anomalies_path)
    n_total = len(anomalies_df)
    n_flagged = anomalies_df["is_anomaly"].sum()
    print(f"  {n_total} rows, {n_flagged} anomalies")

    # Compute population statistics for each feature
    feat_matrix = features_df[FEATURE_COLS].fillna(0)
    pop_mean = feat_matrix.mean()
    pop_std = feat_matrix.std().replace(0, np.nan)

    # Join features onto anomalies using composite key
    merge_keys = ["station_id", "timestamp", "parameter"]
    enriched = anomalies_df.merge(
        features_df[merge_keys + FEATURE_COLS],
        on=merge_keys,
        how="left",
    )

    # Compute per-feature z-scores for every row
    feat_values = enriched[FEATURE_COLS].fillna(0)
    zscores = (feat_values - pop_mean) / pop_std

    # Initialize columns
    enriched["top_features"] = ""
    enriched["top_feature_values"] = ""

    # Only compute driver hints for flagged anomalies
    anomaly_mask = enriched["is_anomaly"] == 1
    anomaly_zscores = zscores.loc[anomaly_mask]

    print(f"Computing top-{top_n} driver features for {anomaly_mask.sum()} anomalies...")

    top_features_list = []
    top_values_list = []

    for idx in anomaly_zscores.index:
        row_z = anomaly_zscores.loc[idx].abs()
        top_idx = row_z.nlargest(top_n)
        names = top_idx.index.tolist()
        values = [f"{anomaly_zscores.loc[idx, name]:.2f}" for name in names]
        top_features_list.append(", ".join(names))
        top_values_list.append(", ".join(values))

    enriched.loc[anomaly_mask, "top_features"] = top_features_list
    enriched.loc[anomaly_mask, "top_feature_values"] = top_values_list

    # Drop the joined feature columns — keep output clean
    result = enriched.drop(columns=FEATURE_COLS)

    # Sort for deterministic output
    result = result.sort_values(merge_keys).reset_index(drop=True)

    return result


def build_driver_hints(
    features_path: Path = DEFAULT_FEATURES,
    anomalies_path: Path = DEFAULT_ANOMALIES,
    top_n: int = 3,
) -> pd.DataFrame:
    """
    Full pipeline: compute driver hints and overwrite anomalies.csv.
    """
    result = compute_driver_hints(features_path, anomalies_path, top_n)

    # Write back to anomalies.csv
    result.to_csv(anomalies_path, index=False)

    # Summary
    anomalies_only = result[result["is_anomaly"] == 1]
    print(f"\n✅ Driver hints written to {anomalies_path}")
    print(f"   Total readings:   {len(result)}")
    print(f"   Anomalies:        {len(anomalies_only)}")
    print(f"   Columns:          {result.columns.tolist()}")

    # Show top feature frequency
    all_features = anomalies_only["top_features"].str.split(", ").explode()
    feature_freq = all_features.value_counts().head(10)
    print(f"\n   Most common driver features:")
    for feat, count in feature_freq.items():
        pct = count / len(anomalies_only) * 100
        print(f"     {feat}: {count} ({pct:.0f}%)")

    # Sample output
    print(f"\n   Sample driver hints (first 5 anomalies):")
    sample = anomalies_only.head(5)
    for _, row in sample.iterrows():
        print(f"     {row['station_id']} | {row['timestamp']} | {row['parameter']}")
        print(f"       drivers: {row['top_features']}")
        print(f"       z-scores: {row['top_feature_values']}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Add driver hints (top contributing features) to anomalies"
    )
    parser.add_argument(
        "--features", "-f", type=Path, default=DEFAULT_FEATURES,
        help=f"Input features CSV (default: {DEFAULT_FEATURES})",
    )
    parser.add_argument(
        "--anomalies", "-a", type=Path, default=DEFAULT_ANOMALIES,
        help=f"Anomalies CSV to enrich (default: {DEFAULT_ANOMALIES})",
    )
    parser.add_argument(
        "--top-n", "-n", type=int, default=3,
        help="Number of top driver features per anomaly (default: 3)",
    )
    args = parser.parse_args()

    build_driver_hints(args.features, args.anomalies, args.top_n)


if __name__ == "__main__":
    main()
