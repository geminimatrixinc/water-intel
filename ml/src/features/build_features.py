"""
Feature Engineering v1 — Rolling Statistics + Deltas

Transforms the processed time-series into ML-ready features:
rolling statistics, deltas, rate of change, missingness indicators, and z-scores.

Usage:
    python -m src.features.build_features
    python -m src.features.build_features --input data/processed/eccc_processed.csv
    python -m src.features.build_features --output data/processed/eccc_features.csv
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Resolve project root so imports work when run as script or module
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # water-intel/
ML_ROOT = PROJECT_ROOT / "ml"
sys.path.insert(0, str(ML_ROOT))

# Defaults
DEFAULT_INPUT = PROJECT_ROOT / "data" / "processed" / "eccc_processed.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "processed" / "eccc_features.csv"

# Rolling window sizes (reading counts, not calendar days — sampling is irregular)
WINDOWS = [7, 14, 30]

# Flag readings with gaps exceeding this threshold (~2.5x the 14-day median)
GAP_THRESHOLD_DAYS = 35


def compute_group_features(group: pd.DataFrame) -> pd.DataFrame:
    """
    Compute features for a single station+parameter group.

    Expects rows sorted by timestamp with at least: value, timestamp.
    """
    df = group.copy()
    val = df["value"]

    # --- Delta: change from previous reading ---
    df["delta"] = val.diff()

    # --- Time gap in days ---
    df["time_gap_days"] = df["timestamp"].diff().dt.total_seconds() / 86400.0

    # --- Rate of change: delta / time gap (handles irregular sampling) ---
    df["rate_of_change"] = df["delta"] / df["time_gap_days"].replace(0, np.nan)

    # --- Missingness flag: 1 if gap exceeds threshold ---
    df["is_gap"] = (df["time_gap_days"] > GAP_THRESHOLD_DAYS).astype("Int8")

    # --- Rolling statistics per window ---
    for w in WINDOWS:
        df[f"rolling_mean_{w}"] = val.rolling(w, min_periods=1).mean()
        df[f"rolling_std_{w}"] = val.rolling(w, min_periods=2).std()

    # --- Z-score: standard deviations from 30-reading rolling mean ---
    std_30 = df["rolling_std_30"].replace(0, np.nan)
    df["zscore"] = (val - df["rolling_mean_30"]) / std_30

    return df


def build_features(input_path: Path, output_path: Path) -> pd.DataFrame:
    """
    Run the full feature engineering pipeline.

    1. Load processed data
    2. Sort by station + parameter + timestamp
    3. Compute per-group features (rolling, delta, z-score, gap flags)
    4. Write feature CSV
    """
    print(f"Loading processed data from {input_path}")
    df = pd.read_csv(input_path, parse_dates=["timestamp"])

    rows_in = len(df)
    n_stations = df["station_id"].nunique()
    n_params = df["parameter"].nunique()
    print(f"  {rows_in} rows, {n_stations} stations, {n_params} parameters")

    # Sort for deterministic group processing
    df = df.sort_values(["station_id", "parameter", "timestamp"]).reset_index(drop=True)

    # Compute features per station+parameter group
    print("Computing features per station+parameter group...")
    groups = []
    for _, group in df.groupby(["station_id", "parameter"]):
        groups.append(compute_group_features(group))
    featured = pd.concat(groups, ignore_index=True)

    # --- NaN summary for new feature columns ---
    feature_cols = [c for c in featured.columns if c not in df.columns]
    nan_counts = featured[feature_cols].isna().sum()

    print(f"\n  Feature columns added: {len(feature_cols)}")
    for col in feature_cols:
        nan_pct = nan_counts[col] / len(featured) * 100
        print(f"    {col:25s}  NaN: {nan_counts[col]:5d} ({nan_pct:.1f}%)")

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    featured.to_csv(output_path, index=False, date_format="%Y-%m-%dT%H:%M:%S")

    print(f"\n✅ Feature dataset written to {output_path}")
    print(f"   Rows: {len(featured)}  Columns: {len(featured.columns)}")

    return featured


def main():
    parser = argparse.ArgumentParser(
        description="Build ML features from processed ECCC data"
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input processed CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output feature CSV (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()
    build_features(args.input, args.output)


if __name__ == "__main__":
    main()
