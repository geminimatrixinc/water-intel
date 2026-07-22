"""
Ontario Spills Loader — Surface Water Event Subset

Downloads and caches Ontario spill records, then filters to surface-water
incidents for cross-referencing with anomaly dates.

Usage:
    python -m src.ingest.spills_loader
    python -m src.ingest.spills_loader --min-year 2019
"""

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import urlopen, urlretrieve

import pandas as pd

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # water-intel/
ML_ROOT = PROJECT_ROOT / "ml"
sys.path.insert(0, str(ML_ROOT))

# Defaults
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "processed" / "spills_surface_water.csv"
DEFAULT_SPILLS_2003_2022_URL = (
    "https://files.ontario.ca/moe_mapping/downloads/4Other/SAC/spill_occurrences_2003-2022.csv"
)
DEFAULT_SPILLS_DATASET_PAGE = (
    "https://data.ontario.ca/dataset/environmental-occurrences-and-spills"
)


def _download_if_missing(url: str, target_path: Path) -> Path:
    """
    Download URL to target path once; skip if already cached.
    """
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        print(f"Using cached file: {target_path}")
        return target_path

    print(f"Downloading {url}")
    urlretrieve(url, target_path)
    print(f"  Saved: {target_path}")
    return target_path


def _resolve_year_xlsx_url(year: int, dataset_page: str = DEFAULT_SPILLS_DATASET_PAGE) -> str | None:
    """
    Resolve the current download URL for a given year XLSX from the dataset page.

    The resource URLs on data.ontario.ca are versioned; this parser intentionally
    scans for any XLSX links containing the requested year.
    """
    try:
        with urlopen(dataset_page, timeout=20) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        print(f"Warning: could not load dataset page ({exc})")
        return None

    year_str = str(year)

    # Direct absolute XLSX links.
    abs_matches = re.findall(r'https?://[^"\'<>\s]+\.xlsx', html, flags=re.IGNORECASE)
    for url in abs_matches:
        if year_str in url:
            return url

    # Relative XLSX links.
    rel_matches = re.findall(r'href=["\']([^"\']+\.xlsx)["\']', html, flags=re.IGNORECASE)
    for rel in rel_matches:
        if year_str in rel:
            return urljoin(dataset_page, rel)

    return None


def _load_one_spill_file(path: Path) -> pd.DataFrame:
    """
    Read CSV/XLSX spill data into a DataFrame.
    """
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    if path.suffix.lower() == ".xlsx":
        try:
            return pd.read_excel(path)
        except ImportError:
            print(
                "Warning: openpyxl is not installed, skipping XLSX spill file "
                f"{path.name}."
            )
            return pd.DataFrame()
    print(f"Warning: unsupported spills file type: {path}")
    return pd.DataFrame()


def load_spills(raw_dir: Path = DEFAULT_RAW_DIR, min_year: int = 2019) -> pd.DataFrame:
    """
    Load and normalize Ontario spill records for surface-water correlation.

    Returns columns:
        date, municipality, contaminant, receiving_media,
        environmental_impact, source_type, incident_event
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    source_paths: list[Path] = []

    # Core historical file.
    core_path = raw_dir / "spills_2003_2022.csv"
    try:
        source_paths.append(_download_if_missing(DEFAULT_SPILLS_2003_2022_URL, core_path))
    except Exception as exc:
        print(f"Warning: failed to download core spills CSV ({exc})")

    # Optional 2023/2024 annual files.
    for year in (2023, 2024):
        yearly_path = raw_dir / f"spills_{year}.xlsx"
        if yearly_path.exists():
            print(f"Using cached file: {yearly_path}")
            source_paths.append(yearly_path)
            continue

        url = _resolve_year_xlsx_url(year)
        if not url:
            print(
                f"Warning: could not auto-resolve {year} XLSX URL from "
                f"{DEFAULT_SPILLS_DATASET_PAGE}; proceeding without {year}."
            )
            continue

        try:
            source_paths.append(_download_if_missing(url, yearly_path))
        except Exception as exc:
            print(f"Warning: failed to download {year} spills XLSX ({exc})")

    if not source_paths:
        print("Warning: no spills sources available; returning empty DataFrame")
        return pd.DataFrame(
            columns=[
                "date",
                "municipality",
                "contaminant",
                "receiving_media",
                "environmental_impact",
                "source_type",
                "incident_event",
            ]
        )

    frames = []
    for path in source_paths:
        frame = _load_one_spill_file(path)
        if len(frame) == 0:
            continue
        frames.append(frame)

    if not frames:
        print("Warning: spills files loaded but no readable rows found")
        return pd.DataFrame(
            columns=[
                "date",
                "municipality",
                "contaminant",
                "receiving_media",
                "environmental_impact",
                "source_type",
                "incident_event",
            ]
        )

    spills = pd.concat(frames, ignore_index=True)
    print(f"Loaded spills rows (all years): {len(spills)}")

    # Parse and filter dates.
    spills["date"] = pd.to_datetime(spills.get("Date Reported"), errors="coerce")
    spills = spills.dropna(subset=["date"])
    spills = spills[spills["date"].dt.year >= min_year]

    # Surface water subset only.
    receiving = spills.get("Receiving Media", pd.Series(dtype="string")).astype(str)
    spills = spills[receiving.str.contains("Surface Water", case=False, na=False)]

    # Normalize join key.
    municipality = spills.get("Site Municipality", pd.Series(dtype="string")).astype(str)
    spills["municipality"] = municipality.str.strip().str.upper()

    # Output columns requested in task plan.
    result = pd.DataFrame(
        {
            "date": spills["date"].dt.normalize(),
            "municipality": spills["municipality"],
            "contaminant": spills.get("Contaminant Name", "").fillna(""),
            "receiving_media": spills.get("Receiving Media", "").fillna(""),
            "environmental_impact": spills.get("Environmental Impact", "").fillna(""),
            "source_type": spills.get("Source Type", "").fillna(""),
            "incident_event": spills.get("Incident Event", "").fillna(""),
        }
    )

    result = result.sort_values(["date", "municipality"]).reset_index(drop=True)
    print(f"Surface-water spills rows since {min_year}: {len(result)}")

    return result


def build_spills_surface_water(
    raw_dir: Path = DEFAULT_RAW_DIR,
    min_year: int = 2019,
    output_path: Path = DEFAULT_OUTPUT,
) -> pd.DataFrame:
    """
    Load spills and persist the filtered subset to CSV.
    """
    result = load_spills(raw_dir=raw_dir, min_year=min_year)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False, date_format="%Y-%m-%d")

    print(f"\n✅ Surface-water spills written to {output_path}")
    print(f"   Rows: {len(result)}")
    print(f"   Columns: {result.columns.tolist()}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Build filtered Ontario surface-water spills dataset"
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help=f"Raw data directory (default: {DEFAULT_RAW_DIR})",
    )
    parser.add_argument(
        "--min-year",
        type=int,
        default=2019,
        help="Minimum year to include (default: 2019)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    build_spills_surface_water(
        raw_dir=args.raw_dir,
        min_year=args.min_year,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
