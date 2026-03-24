"""
Risk Score v1 — 0–100 Composite Risk with Human-Readable Labels

Combines anomaly detection outputs into a single headline number for the
dashboard. Each station gets a risk_score (0–100) and risk_label
(Safe / Watch / Concern).

GUARDRAIL: This is a 2A proxy score based on public surface water monitoring
data. It does NOT predict ISC drinking water advisories. Labels are for
decision-support only, not compliance certification.

Formula (v1):
    base       = avg_anomaly_score normalized to 0–60
    frequency  = anomaly_rate normalized to 0–40
    risk_score = min(base + frequency, 100)

Usage:
    python -m src.models.risk_score
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
DEFAULT_SITE_SUMMARY = PROJECT_ROOT / "outputs" / "site_summary.csv"
DEFAULT_ANOMALIES = PROJECT_ROOT / "outputs" / "anomalies.csv"

# Score weights
BASE_WEIGHT = 60       # max points from avg anomaly score
FREQUENCY_WEIGHT = 40  # max points from anomaly rate

# Label thresholds
LABEL_THRESHOLDS = [
    (30, "Safe"),
    (60, "Watch"),
    (100, "Concern"),
]


def classify_label(score: int) -> str:
    """Map a 0–100 score to a human-readable risk label."""
    for threshold, label in LABEL_THRESHOLDS:
        if score <= threshold:
            return label
    return "Concern"


def compute_risk_scores(
    site_summary_path: Path = DEFAULT_SITE_SUMMARY,
    anomalies_path: Path = DEFAULT_ANOMALIES,
) -> pd.DataFrame:
    """
    Compute 0–100 risk scores for each station.

    Strategy:
    - Base component (0–60): avg_anomaly_score normalized across stations
      Higher avg anomaly score → higher base risk
    - Frequency component (0–40): anomaly_rate normalized across stations
      Higher anomaly rate → higher frequency boost
    - Final: min(base + frequency, 100), rounded to integer
    """
    print(f"Loading site summary from {site_summary_path}")
    df = pd.read_csv(site_summary_path)
    print(f"  {len(df)} stations")

    # --- Base component: avg_anomaly_score → 0–60 ---
    # Anomaly scores are already [0, 1], but normalize across stations
    # to use the full 0–60 range
    score_min = df["avg_anomaly_score"].min()
    score_max = df["avg_anomaly_score"].max()
    if score_max > score_min:
        base_normalized = (df["avg_anomaly_score"] - score_min) / (score_max - score_min)
    else:
        base_normalized = pd.Series(0.5, index=df.index)
    base_component = base_normalized * BASE_WEIGHT

    # --- Frequency component: anomaly_rate → 0–40 ---
    rate_min = df["anomaly_rate"].min()
    rate_max = df["anomaly_rate"].max()
    if rate_max > rate_min:
        freq_normalized = (df["anomaly_rate"] - rate_min) / (rate_max - rate_min)
    else:
        freq_normalized = pd.Series(0.5, index=df.index)
    freq_component = freq_normalized * FREQUENCY_WEIGHT

    # --- Combine ---
    raw_score = base_component + freq_component
    df["risk_score"] = np.clip(raw_score, 0, 100).round(0).astype(int)
    df["risk_label"] = df["risk_score"].apply(classify_label)

    # Sort by risk score descending
    df = df.sort_values("risk_score", ascending=False).reset_index(drop=True)

    return df


def build_risk_scores(
    site_summary_path: Path = DEFAULT_SITE_SUMMARY,
    anomalies_path: Path = DEFAULT_ANOMALIES,
) -> pd.DataFrame:
    """
    Full pipeline: compute risk scores and overwrite site_summary.csv.
    """
    result = compute_risk_scores(site_summary_path, anomalies_path)

    # Write back
    result.to_csv(site_summary_path, index=False)

    # Summary
    print(f"\n✅ Risk scores written to {site_summary_path}")
    print(f"   Columns:  {result.columns.tolist()}")
    print()

    # Display
    display_cols = [
        "station_id", "anomaly_rate", "avg_anomaly_score",
        "risk_score", "risk_label",
    ]
    print(result[display_cols].to_string(index=False))

    # Distribution
    print(f"\n   Score distribution:")
    print(f"     Min:    {result['risk_score'].min()}")
    print(f"     Max:    {result['risk_score'].max()}")
    print(f"     Mean:   {result['risk_score'].mean():.1f}")
    print(f"     Median: {result['risk_score'].median():.0f}")

    # Label counts
    print(f"\n   Label breakdown:")
    for label in ["Concern", "Watch", "Safe"]:
        count = (result["risk_label"] == label).sum()
        if count > 0:
            print(f"     {label}: {count}")

    print(f"\n   ⚠️  GUARDRAIL: This is a 2A proxy score based on public surface")
    print(f"   water monitoring data. It does NOT predict ISC drinking water advisories.")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Compute 0-100 risk scores for each monitoring station"
    )
    parser.add_argument(
        "--site-summary", "-s", type=Path, default=DEFAULT_SITE_SUMMARY,
        help=f"Site summary CSV (default: {DEFAULT_SITE_SUMMARY})",
    )
    parser.add_argument(
        "--anomalies", "-a", type=Path, default=DEFAULT_ANOMALIES,
        help=f"Anomalies CSV (default: {DEFAULT_ANOMALIES})",
    )
    args = parser.parse_args()

    build_risk_scores(args.site_summary, args.anomalies)


if __name__ == "__main__":
    main()
