"""
Hydrometric Flow Loader — WSC HYDAT Daily Flow Spikes

Downloads and caches HYDAT SQLite, extracts daily flow readings for selected
gauges, and flags storm-flow proxy spikes from rolling statistics.

Usage:
    python -m src.ingest.hydrometric_loader
    python -m src.ingest.hydrometric_loader --gauges 02GB001 02GB007 02GB010 02GA003
"""

import argparse
import re
import sqlite3
import sys
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile

import pandas as pd

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # water-intel/
ML_ROOT = PROJECT_ROOT / "ml"
sys.path.insert(0, str(ML_ROOT))

# Defaults
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "processed" / "hydrometric_flow_spikes.csv"
DEFAULT_HYDAT_PAGE = (
    "https://www.canada.ca/en/environment-climate-change/services/water-overview/"
    "quantity/monitoring/survey/data-products-services/national-archive-hydat.html"
)
DEFAULT_HYDAT_DIRECTORY = "https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/"
DEFAULT_GAUGES = ["02GB001", "02GB007", "02GB010", "02GA003"]
ROLLING_WINDOW_DAYS = 30


def _resolve_hydat_zip_url(page_url: str = DEFAULT_HYDAT_PAGE) -> str | None:
    """
    Resolve the current HYDAT SQLite ZIP URL from the official HYDAT page.
    """
    try:
        with urlopen(page_url, timeout=30) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        print(f"Warning: could not load HYDAT page ({exc})")
        return None

    # Prefer explicit sqlite zip links.

    abs_matches = re.findall(
        r'https?://[^"\'<>\s]*Hydat_sqlite3[^"\'<>\s]*\.zip',
        html,
        flags=re.IGNORECASE,
    )
    if abs_matches:
        return abs_matches[0]

    rel_matches = re.findall(
        r'href=["\']([^"\']*Hydat_sqlite3[^"\']*\.zip)["\']',
        html,
        flags=re.IGNORECASE,
    )
    if rel_matches:
        return urljoin(page_url, rel_matches[0])

    # Fallback to any hydat zip if naming has shifted.
    any_abs = re.findall(r'https?://[^"\'<>\s]*hydat[^"\'<>\s]*\.zip', html, flags=re.IGNORECASE)
    if any_abs:
        return any_abs[0]

    any_rel = re.findall(r'href=["\']([^"\']*hydat[^"\']*\.zip)["\']', html, flags=re.IGNORECASE)
    if any_rel:
        return urljoin(page_url, any_rel[0])

    # Fallback: scrape the official hydrometrics directory listing.
    try:
        with urlopen(DEFAULT_HYDAT_DIRECTORY, timeout=30) as response:
            directory_html = response.read().decode("utf-8", errors="ignore")
    except Exception:
        return None

    directory_matches = re.findall(
        r'(Hydat_sqlite3_[0-9]{8}\.zip)',
        directory_html,
        flags=re.IGNORECASE,
    )
    if not directory_matches:
        return None

    # Keep the newest version by date suffix.
    newest = sorted(set(directory_matches))[-1]
    return urljoin(DEFAULT_HYDAT_DIRECTORY, newest)


def _ensure_hydat_sqlite(raw_dir: Path = DEFAULT_RAW_DIR) -> Path:
    """
    Ensure data/raw/Hydat.sqlite3 exists, downloading and extracting if needed.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    sqlite_path = raw_dir / "Hydat.sqlite3"
    if sqlite_path.exists():
        print(f"Using cached HYDAT SQLite: {sqlite_path}")
        return sqlite_path

    zip_path = raw_dir / "Hydat_sqlite3.zip"
    zip_url = _resolve_hydat_zip_url()
    if not zip_url:
        raise RuntimeError("Could not resolve HYDAT SQLite ZIP URL")

    print(f"Downloading HYDAT archive from {zip_url}")
    urlretrieve(zip_url, zip_path)
    print(f"  Saved: {zip_path}")

    print("Extracting HYDAT SQLite...")
    with ZipFile(zip_path, "r") as archive:
        members = archive.namelist()
        sqlite_members = [m for m in members if m.lower().endswith(".sqlite3")]
        if not sqlite_members:
            raise RuntimeError("No .sqlite3 file found inside HYDAT ZIP")

        source_member = sqlite_members[0]
        extracted = archive.extract(source_member, path=raw_dir)
        extracted_path = Path(extracted)
        extracted_path.replace(sqlite_path)

    print(f"  HYDAT SQLite ready: {sqlite_path}")
    return sqlite_path


def _extract_daily_flows(conn: sqlite3.Connection, gauge_id: str, min_year: int) -> pd.DataFrame:
    """
    Extract long-form daily flow rows for one gauge from HYDAT DLY_FLOWS.
    """
    query = """
        SELECT *
        FROM DLY_FLOWS
        WHERE STATION_NUMBER = ? AND YEAR >= ?
        ORDER BY YEAR, MONTH
    """
    wide = pd.read_sql(query, conn, params=[gauge_id, min_year])
    if len(wide) == 0:
        return pd.DataFrame(columns=["date", "gauge_id", "flow_m3s"])

    flow_cols = [c for c in wide.columns if c.startswith("FLOW") and "_" not in c]
    long_frames = []
    for day in range(1, 32):
        col = f"FLOW{day}"
        if col not in flow_cols:
            continue
        day_frame = wide[["STATION_NUMBER", "YEAR", "MONTH", col]].copy()
        day_frame["day"] = day
        day_frame = day_frame.rename(columns={col: "flow_m3s"})
        long_frames.append(day_frame)

    if not long_frames:
        return pd.DataFrame(columns=["date", "gauge_id", "flow_m3s"])

    long_df = pd.concat(long_frames, ignore_index=True)
    long_df["flow_m3s"] = pd.to_numeric(long_df["flow_m3s"], errors="coerce")
    long_df = long_df.dropna(subset=["flow_m3s"])

    long_df["date"] = pd.to_datetime(
        {
            "year": long_df["YEAR"],
            "month": long_df["MONTH"],
            "day": long_df["day"],
        },
        errors="coerce",
    )
    long_df = long_df.dropna(subset=["date"])

    out = pd.DataFrame(
        {
            "date": long_df["date"].dt.normalize(),
            "gauge_id": long_df["STATION_NUMBER"],
            "flow_m3s": long_df["flow_m3s"],
        }
    )
    out = out.sort_values(["gauge_id", "date"]).reset_index(drop=True)
    return out


def load_flow_spikes(
    gauge_ids: list[str],
    min_year: int = 2019,
    spike_threshold_std: float = 2.0,
) -> pd.DataFrame:
    """
    Load HYDAT daily flows and flag flow spikes per gauge.

    Returns columns:
        date, gauge_id, flow_m3s, is_flow_spike
    """
    try:
        sqlite_path = _ensure_hydat_sqlite(DEFAULT_RAW_DIR)
    except Exception as exc:
        print(f"Warning: HYDAT unavailable ({exc}); returning empty flow spike data")
        return pd.DataFrame(columns=["date", "gauge_id", "flow_m3s", "is_flow_spike"])

    all_frames = []
    with sqlite3.connect(sqlite_path) as conn:
        for gauge_id in gauge_ids:
            print(f"Loading HYDAT daily flows for gauge {gauge_id}")
            gauge_df = _extract_daily_flows(conn, gauge_id, min_year)
            if len(gauge_df) == 0:
                print(f"  No rows found for gauge {gauge_id}")
                continue
            all_frames.append(gauge_df)

    if not all_frames:
        return pd.DataFrame(columns=["date", "gauge_id", "flow_m3s", "is_flow_spike"])

    flows = pd.concat(all_frames, ignore_index=True)
    flows = flows.sort_values(["gauge_id", "date"]).reset_index(drop=True)

    # Rolling spike detection per gauge.
    frames = []
    for gauge_id, group in flows.groupby("gauge_id"):
        df = group.copy()
        rolling_mean = df["flow_m3s"].rolling(ROLLING_WINDOW_DAYS, min_periods=7).mean()
        rolling_std = df["flow_m3s"].rolling(ROLLING_WINDOW_DAYS, min_periods=7).std()
        threshold = rolling_mean + spike_threshold_std * rolling_std
        df["is_flow_spike"] = (
            rolling_std.notna() & (df["flow_m3s"] > threshold)
        ).astype("Int8")
        frames.append(df)

    result = pd.concat(frames, ignore_index=True)
    result = result[["date", "gauge_id", "flow_m3s", "is_flow_spike"]]
    result = result.sort_values(["gauge_id", "date"]).reset_index(drop=True)

    n_spikes = int(result["is_flow_spike"].sum())
    print(
        f"Detected {n_spikes} flow spikes across "
        f"{result['gauge_id'].nunique()} gauges"
    )

    return result


def build_flow_spikes(
    gauge_ids: list[str] = DEFAULT_GAUGES,
    min_year: int = 2019,
    spike_threshold_std: float = 2.0,
    output_path: Path = DEFAULT_OUTPUT,
) -> pd.DataFrame:
    """
    Load hydrometric flows, detect spikes, and persist CSV.
    """
    result = load_flow_spikes(
        gauge_ids=gauge_ids,
        min_year=min_year,
        spike_threshold_std=spike_threshold_std,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False, date_format="%Y-%m-%d")

    print(f"\n✅ Hydrometric flow spikes written to {output_path}")
    print(f"   Rows: {len(result)}")
    print(f"   Columns: {result.columns.tolist()}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Build hydrometric flow spike dataset from HYDAT"
    )
    parser.add_argument(
        "--gauges",
        nargs="+",
        default=DEFAULT_GAUGES,
        help=(
            "Gauge IDs to include (default: "
            f"{' '.join(DEFAULT_GAUGES)})"
        ),
    )
    parser.add_argument(
        "--min-year",
        type=int,
        default=2019,
        help="Minimum year to include (default: 2019)",
    )
    parser.add_argument(
        "--spike-threshold-std",
        type=float,
        default=2.0,
        help="Std-dev multiplier for flow spikes (default: 2.0)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    build_flow_spikes(
        gauge_ids=args.gauges,
        min_year=args.min_year,
        spike_threshold_std=args.spike_threshold_std,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
