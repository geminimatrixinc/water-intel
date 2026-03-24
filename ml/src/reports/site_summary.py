"""
Site Summary Report — One Row Per Station

Aggregates anomaly detection results into a per-station summary for the
dashboard site picker and overview cards.

Usage:
    python -m src.reports.site_summary
    python -m src.reports.site_summary --anomalies outputs/anomalies.csv
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # water-intel/
ML_ROOT = PROJECT_ROOT / "ml"
sys.path.insert(0, str(ML_ROOT))

# Defaults
DEFAULT_ANOMALIES = PROJECT_ROOT / "outputs" / "anomalies.csv"
DEFAULT_FEATURES = PROJECT_ROOT / "data" / "processed" / "grand_river_features.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "outputs" / "site_summary.csv"


def build_site_summary(
    anomalies_path: Path = DEFAULT_ANOMALIES,
    features_path: Path = DEFAULT_FEATURES,
    output_path: Path = DEFAULT_OUTPUT,
) -> pd.DataFrame:
    """
    Build one-row-per-station summary from anomaly results.

    Columns produced:
        station_id, total_readings, anomaly_count, anomaly_rate,
        last_reading_date, last_anomaly_date, avg_anomaly_score,
        parameter_count, date_range_start, date_range_end,
        top_anomaly_parameter, rolling_30d_anomaly_count
    """
    print(f"Loading anomalies from {anomalies_path}")
    anom_df = pd.read_csv(anomalies_path)
    anom_df["timestamp"] = pd.to_datetime(anom_df["timestamp"])
    print(f"  {len(anom_df)} rows, {anom_df['station_id'].nunique()} stations")

    print(f"Loading features from {features_path}")
    feat_df = pd.read_csv(features_path, usecols=["station_id", "timestamp", "parameter"])
    feat_df["timestamp"] = pd.to_datetime(feat_df["timestamp"])

    # --- Per-station aggregation ---
    summaries = []

    for sid, grp in anom_df.groupby("station_id"):
        anomalies = grp[grp["is_anomaly"] == 1]

        total_readings = len(grp)
        anomaly_count = len(anomalies)
        anomaly_rate = round(anomaly_count / total_readings * 100, 2) if total_readings > 0 else 0.0

        last_reading_date = grp["timestamp"].max().strftime("%Y-%m-%d")
        last_anomaly_date = anomalies["timestamp"].max().strftime("%Y-%m-%d") if len(anomalies) > 0 else ""
        avg_anomaly_score = round(anomalies["anomaly_score"].mean(), 4) if len(anomalies) > 0 else 0.0

        # Parameter count from features (broader than anomalies)
        feat_station = feat_df[feat_df["station_id"] == sid]
        parameter_count = feat_station["parameter"].nunique()

        # Date range
        date_range_start = grp["timestamp"].min().strftime("%Y-%m-%d")
        date_range_end = grp["timestamp"].max().strftime("%Y-%m-%d")

        # Top anomaly parameter (most anomalies at this station)
        if len(anomalies) > 0:
            top_anomaly_parameter = anomalies["parameter"].value_counts().index[0]
        else:
            top_anomaly_parameter = ""

        # Rolling 30-day anomaly count (anomalies in last 30 days of data)
        cutoff = grp["timestamp"].max() - pd.Timedelta(days=30)
        rolling_30d = len(anomalies[anomalies["timestamp"] >= cutoff])

        summaries.append({
            "station_id": sid,
            "total_readings": total_readings,
            "anomaly_count": anomaly_count,
            "anomaly_rate": anomaly_rate,
            "last_reading_date": last_reading_date,
            "last_anomaly_date": last_anomaly_date,
            "avg_anomaly_score": avg_anomaly_score,
            "parameter_count": parameter_count,
            "date_range_start": date_range_start,
            "date_range_end": date_range_end,
            "top_anomaly_parameter": top_anomaly_parameter,
            "rolling_30d_anomaly_count": rolling_30d,
        })

    result = pd.DataFrame(summaries)

    # Sort by anomaly rate descending (highest-risk stations first)
    result = result.sort_values("anomaly_rate", ascending=False).reset_index(drop=True)

    # Write
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    # Summary
    print(f"\n✅ Site summary written to {output_path}")
    print(f"   Stations:  {len(result)}")
    print(f"   Columns:   {result.columns.tolist()}")
    print()

    # Display table
    display_cols = [
        "station_id", "total_readings", "anomaly_count",
        "anomaly_rate", "avg_anomaly_score", "top_anomaly_parameter",
        "rolling_30d_anomaly_count",
    ]
    print(result[display_cols].to_string(index=False))

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Generate per-station site summary report"
    )
    parser.add_argument(
        "--anomalies", "-a", type=Path, default=DEFAULT_ANOMALIES,
        help=f"Input anomalies CSV (default: {DEFAULT_ANOMALIES})",
    )
    parser.add_argument(
        "--features", "-f", type=Path, default=DEFAULT_FEATURES,
        help=f"Input features CSV (default: {DEFAULT_FEATURES})",
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=DEFAULT_OUTPUT,
        help=f"Output site summary CSV (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    build_site_summary(args.anomalies, args.features, args.output)


if __name__ == "__main__":
    main()
