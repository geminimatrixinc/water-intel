"""
Build Processed ECCC Dataset

Takes raw ECCC water quality CSV(s) through the ingestion pipeline
and outputs a clean, normalized, analysis-ready CSV.

Usage:
    python -m src.ingest.build_processed
    python -m src.ingest.build_processed --input data/raw/somefile.csv
    python -m src.ingest.build_processed --output data/processed/custom_name.csv
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Resolve project root so imports work when run as script or module
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # water-intel/
ML_ROOT = PROJECT_ROOT / "ml"
sys.path.insert(0, str(ML_ROOT))

from src.ingest.eccc_loader import ECCCLoader
from src.ingest.schema import NORMALIZED_REQUIRED_COLUMNS

# Defaults
DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "Water-Qual-Eau-Okanagan-Similkameen-2000-present.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "processed" / "eccc_processed.csv"


def build_processed(input_path: Path, output_path: Path) -> pd.DataFrame:
    """
    Run the full raw → processed pipeline.

    1. Load & validate via ECCCLoader
    2. Drop rows missing required fields
    3. Deduplicate
    4. Normalize timestamps to ISO-8601
    5. Sort deterministically
    6. Write CSV

    Returns the processed DataFrame.
    """
    # --- load + validate + normalize columns ---
    loader = ECCCLoader(input_path)
    df = loader.process()

    rows_before = len(df)

    # --- drop rows where any required column is null ---
    df = df.dropna(subset=NORMALIZED_REQUIRED_COLUMNS)
    rows_after_null = len(df)
    dropped_null = rows_before - rows_after_null
    if dropped_null:
        print(f"  Dropped {dropped_null} rows with null required fields")

    # --- deduplicate (exact row match) ---
    df = df.drop_duplicates()
    rows_after_dedup = len(df)
    dropped_dup = rows_after_null - rows_after_dedup
    if dropped_dup:
        print(f"  Dropped {dropped_dup} duplicate rows")

    # --- ensure timestamp is datetime & format to ISO-8601 string for CSV ---
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    bad_ts = df["timestamp"].isna().sum()
    if bad_ts:
        print(f"  Warning: dropping {bad_ts} rows with unparseable timestamps")
        df = df.dropna(subset=["timestamp"])

    # --- sort for deterministic output ---
    df = df.sort_values(["station_id", "timestamp", "parameter"]).reset_index(drop=True)

    # --- write ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, date_format="%Y-%m-%dT%H:%M:%S")

    # --- summary ---
    print(f"\n✅ Processed dataset written to {output_path}")
    print(f"   Rows: {len(df)}  (from {rows_before} raw)")
    print(f"   Stations: {df['station_id'].nunique()}")
    print(f"   Parameters: {df['parameter'].nunique()}")
    print(f"   Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")

    return df


def main():
    parser = argparse.ArgumentParser(description="Build processed ECCC dataset")
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input CSV path (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    print(f"Building processed dataset...")
    print(f"  Input:  {args.input}")
    print(f"  Output: {args.output}")

    build_processed(args.input, args.output)


if __name__ == "__main__":
    main()
