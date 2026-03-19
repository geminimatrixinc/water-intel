"""
Ontario PWQMN Water Quality Data Loader

Loads Ontario Provincial (Stream) Water Quality Monitoring Network data,
converts to the normalized Water-Intel schema, and filters for target stations.

Data source: https://data.ontario.ca/dataset/provincial-stream-water-quality-monitoring-network
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Optional


# Grand River area stations near Six Nations of the Grand River
# Sorted by distance from Six Nations center (~43.06, -80.05)
GRAND_RIVER_STATIONS = {
    16018412802: "Big Creek at Hwy 54, NW of Caledonia (4.3 km)",
    16018409202: "Grand River at York, Haldimand Norfolk Rd 9 (13.5 km)",
    16018409302: "Fairchild Creek at Harris Rd, Brantford Twp (14.0 km)",
    16018402702: "Grand River at Cocksutts Bridge, Brantford (16.8 km)",
    16018400902: "Nith River at Paris (30.8 km)",
    16018401002: "Grand River at Glen Morris (34.1 km)",
    16018403502: "Grand River at Dunnville (39.2 km)",
    16018401202: "Grand River at Blair (45.5 km)",
}

# PWQMN has different column names across file vintages.
# We detect the schema and map accordingly.

PWQMN_SCHEMAS = {
    # 2023-2024 format
    "v3": {
        "detect": "Collected",
        "map": {
            "Collection Site": "station_id",
            "Collected": "_date",
            "Collection_Time": "_time",
            "Analyte": "parameter",
            "Results": "_raw_result",
            "Units": "unit",
            "Value Qualifier": "qualifier",
            "Lab Sample ID": "sample_id",
        },
    },
    # 2021-2022 format
    "v2": {
        "detect": "Collection Date",
        "map": {
            "Collection Site": "station_id",
            "Collection Date": "_date",
            "Collection Time": "_time",
            "Analyte": "parameter",
            "Result": "_raw_result",
            "Units": "unit",
            "Value Qualifier": "qualifier",
            "Lab Sample ID": "sample_id",
        },
    },
    # Pre-2021 format
    "v1": {
        "detect": "DATE_YYYYMMDD",
        "map": {
            "STATION": "station_id",
            "DATE_YYYYMMDD": "_date",
            "TIME_HH:MM": "_time",
            "PARM_DESCRIPTION": "parameter",
            "RESULT": "_raw_result",
            "UNITS": "unit",
            "VALUQUALIFI": "qualifier",
            "FIELD_NO": "sample_id",
        },
    },
}


def _detect_schema(columns):
    """Detect which PWQMN schema version a file uses."""
    col_set = set(columns)
    for version, spec in PWQMN_SCHEMAS.items():
        if spec["detect"] in col_set:
            return version, spec["map"]
    return None, None


def load_pwqmn_file(path: Path, station_filter: Optional[List[int]] = None) -> pd.DataFrame:
    """Load a single PWQMN CSV and return rows for target stations."""
    df = pd.read_csv(path)

    version, col_map = _detect_schema(df.columns)
    if version is None:
        return pd.DataFrame()

    # Get the station column name for this schema
    station_col = [k for k, v in col_map.items() if v == "station_id"][0]

    if station_filter:
        df = df[df[station_col].isin(station_filter)]

    if df.empty:
        return pd.DataFrame()

    # Keep only mapped columns that exist
    keep = [c for c in col_map if c in df.columns]
    df = df[keep].rename(columns=col_map)

    # Tag schema version for date parsing
    df["_schema"] = version

    return df


def normalize_pwqmn(df: pd.DataFrame) -> pd.DataFrame:
    """Convert PWQMN columns to the Water-Intel normalized schema."""
    if df.empty:
        return pd.DataFrame(columns=[
            "station_id", "timestamp", "parameter", "value",
            "unit", "qualifier", "qa_status", "sample_id",
        ])

    out = pd.DataFrame()

    # station_id — convert to string for consistency
    out["station_id"] = df["station_id"].astype(str)

    # timestamp — handle different date formats by schema version
    date_str = df["_date"].astype(str)
    time_str = df["_time"].fillna("00:00:00").astype(str)

    # v1 uses YYYYMMDD, v2/v3 use MM/DD/YYYY
    is_v1 = df["_schema"] == "v1"
    if is_v1.any():
        date_str = date_str.where(~is_v1, date_str.str[:4] + "-" + date_str.str[4:6] + "-" + date_str.str[6:8])

    out["timestamp"] = pd.to_datetime(
        date_str + " " + time_str,
        format="mixed",
        dayfirst=False,
        errors="coerce",
    )

    # parameter — upper-case to match ECCC convention
    out["parameter"] = df["parameter"].str.upper().str.strip()

    # value — extract numeric from Results (handles '<2', '>5', plain numbers)
    out["value"] = pd.to_numeric(
        df["_raw_result"].astype(str).str.replace(r"[<>~]", "", regex=True),
        errors="coerce",
    )

    # unit — normalize common unit variations
    unit = df["unit"].str.strip()
    unit = unit.replace({
        "MILLIGRAM PER LITER": "mg/L",
        "MICROGRAM PER LITER": "µg/L",
        "PH UNITS": "pH",
        "NEPHELOMETRIC TURBIDITY UNITS": "NTU",
        "MICROSIEMEN PER CENTIMETRE": "µS/cm",
        "DEGREE CELSIUS": "°C",
    })
    out["unit"] = unit

    # qualifier — '<' for BDL, '>' for above range, else NaN
    qualifier = df.get("qualifier", pd.Series(dtype="object"))
    result_call = df.get("_result_call", pd.Series(dtype="object"))
    out["qualifier"] = qualifier.where(qualifier.isin(["<", ">", "~"]), np.nan)

    # qa_status — PWQMN doesn't have this, set to NaN
    out["qa_status"] = np.nan

    # sample_id
    out["sample_id"] = df.get("sample_id", pd.Series(dtype="object"))

    # Drop rows without a value or timestamp
    out = out.dropna(subset=["value", "timestamp"])

    return out


def build_grand_river_processed(
    raw_dir: Path,
    output_path: Path,
    station_ids: Optional[List[int]] = None,
) -> pd.DataFrame:
    """
    Combine multiple PWQMN CSV files, filter for Grand River stations,
    normalize, and write a processed CSV matching the pipeline schema.
    """
    if station_ids is None:
        station_ids = list(GRAND_RIVER_STATIONS.keys())

    # Find all PWQMN files
    pwqmn_files = sorted(raw_dir.glob("pwqmn_*.csv"))
    if not pwqmn_files:
        raise FileNotFoundError(f"No pwqmn_*.csv files found in {raw_dir}")

    print(f"Found {len(pwqmn_files)} PWQMN file(s):")
    for f in pwqmn_files:
        print(f"  {f.name}")

    frames = []
    for f in pwqmn_files:
        raw = load_pwqmn_file(f, station_filter=station_ids)
        if raw.empty:
            print(f"  {f.name}: 0 rows for target stations")
            continue
        norm = normalize_pwqmn(raw)
        print(f"  {f.name}: {len(norm)} rows")
        frames.append(norm)

    if not frames:
        raise ValueError("No data found for target stations in any file")

    df = pd.concat(frames, ignore_index=True)

    # Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["station_id", "timestamp", "parameter"])
    dupes = before - len(df)
    if dupes:
        print(f"  Dropped {dupes} duplicate rows")

    # Sort deterministically
    df = df.sort_values(["station_id", "timestamp", "parameter"]).reset_index(drop=True)

    # Write
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, date_format="%Y-%m-%dT%H:%M:%S")

    print(f"\n✅ Grand River processed dataset written to {output_path}")
    print(f"   Rows: {len(df)}")
    print(f"   Stations: {df['station_id'].nunique()}")
    print(f"   Parameters: {df['parameter'].nunique()}")
    print(f"   Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")

    # Show station summary
    print("\n   Station breakdown:")
    for sid, count in df.groupby("station_id").size().items():
        name = GRAND_RIVER_STATIONS.get(int(sid), "Unknown")
        print(f"     {sid}: {count} rows — {name}")

    return df


if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
    RAW_DIR = PROJECT_ROOT / "data" / "raw"
    OUTPUT = PROJECT_ROOT / "data" / "processed" / "grand_river_processed.csv"

    build_grand_river_processed(RAW_DIR, OUTPUT)
