"""
Baseline Anomaly Detection — Isolation Forest

Trains a global Isolation Forest on engineered features and scores every reading.
Anomaly scores are normalized to [0, 1] where higher = more anomalous.

Usage:
    python -m src.models.anomaly_iforest
    python -m src.models.anomaly_iforest --input data/processed/grand_river_features.csv
    python -m src.models.anomaly_iforest --contamination 0.05
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # water-intel/
ML_ROOT = PROJECT_ROOT / "ml"
sys.path.insert(0, str(ML_ROOT))

# Defaults
DEFAULT_INPUT = PROJECT_ROOT / "data" / "processed" / "grand_river_features.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "outputs" / "anomalies.csv"

# Feature columns used by the model (all engineered features from Day 7)
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

# Model hyperparameters
RANDOM_STATE = 42


def prepare_features(df: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray]:
    """
    Prepare the feature matrix for Isolation Forest.

    Strategy:
    - Use only the engineered feature columns (scale-independent by design)
    - Fill NaN with 0 (NaN occurs on first reading per group — structurally expected)
    - StandardScaler so all features contribute equally

    Returns:
        (original df, scaled feature matrix as ndarray)
    """
    X = df[FEATURE_COLS].copy()
    X = X.fillna(0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return df, X_scaled


def train_and_score(
    df: pd.DataFrame,
    X_scaled: np.ndarray,
    contamination: float = 0.05,
) -> pd.DataFrame:
    """
    Train Isolation Forest and produce anomaly scores.

    Returns DataFrame with columns:
        station_id, timestamp, parameter, value, unit,
        anomaly_score, is_anomaly
    """
    model = IsolationForest(
        contamination=contamination,
        n_estimators=200,
        max_samples="auto",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )

    model.fit(X_scaled)

    # decision_function: lower = more anomalous (negative = anomaly)
    raw_scores = model.decision_function(X_scaled)
    predictions = model.predict(X_scaled)  # 1 = normal, -1 = anomaly

    # Normalize scores to [0, 1] where 1 = most anomalous
    # decision_function returns negative for anomalies, positive for normal
    # Invert and scale: score = 1 - (raw - min) / (max - min)
    score_min = raw_scores.min()
    score_max = raw_scores.max()
    if score_max > score_min:
        anomaly_score = 1.0 - (raw_scores - score_min) / (score_max - score_min)
    else:
        anomaly_score = np.zeros_like(raw_scores)

    result = pd.DataFrame({
        "station_id": df["station_id"].values,
        "timestamp": df["timestamp"].values,
        "parameter": df["parameter"].values,
        "value": df["value"].values,
        "unit": df["unit"].values,
        "anomaly_score": np.round(anomaly_score, 4),
        "is_anomaly": (predictions == -1).astype(int),
    })

    return result


def build_anomalies(
    input_path: Path,
    output_path: Path,
    contamination: float = 0.05,
) -> pd.DataFrame:
    """
    Full pipeline: load features → train model → score → write CSV.
    """
    print(f"Loading features from {input_path}")
    df = pd.read_csv(input_path)
    print(f"  {len(df)} rows, {df['station_id'].nunique()} stations, "
          f"{df['parameter'].nunique()} parameters")

    # Prepare and train
    print(f"Training Isolation Forest (contamination={contamination})...")
    df, X_scaled = prepare_features(df)
    result = train_and_score(df, X_scaled, contamination)

    # Sort for deterministic output
    result = result.sort_values(
        ["station_id", "timestamp", "parameter"]
    ).reset_index(drop=True)

    # Write
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    # Summary
    n_anomalies = result["is_anomaly"].sum()
    anomaly_rate = n_anomalies / len(result) * 100
    print(f"\n✅ Anomalies written to {output_path}")
    print(f"   Total readings:  {len(result)}")
    print(f"   Anomalies:       {n_anomalies} ({anomaly_rate:.1f}%)")
    print(f"   Mean score:      {result['anomaly_score'].mean():.4f}")
    print(f"   Median score:    {result['anomaly_score'].median():.4f}")

    # Per-station breakdown
    print(f"\n   Per-station anomaly counts:")
    for sid, grp in result.groupby("station_id"):
        n = grp["is_anomaly"].sum()
        rate = n / len(grp) * 100
        print(f"     {sid}: {n}/{len(grp)} ({rate:.1f}%)")

    # Top anomalous readings
    top = result.nlargest(10, "anomaly_score")
    print(f"\n   Top 10 most anomalous readings:")
    for _, row in top.iterrows():
        print(f"     {row['station_id']} | {row['timestamp']} | "
              f"{row['parameter']}: {row['value']} {row['unit']} "
              f"| score={row['anomaly_score']:.4f}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Baseline anomaly detection (Isolation Forest)"
    )
    parser.add_argument(
        "--input", "-i", type=Path, default=DEFAULT_INPUT,
        help=f"Input features CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=DEFAULT_OUTPUT,
        help=f"Output anomalies CSV (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--contamination", "-c", type=float, default=0.05,
        help="Expected fraction of anomalies (default: 0.05)",
    )
    args = parser.parse_args()

    build_anomalies(args.input, args.output, args.contamination)


if __name__ == "__main__":
    main()
