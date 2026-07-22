import os
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware


BASE_DIR = Path(__file__).resolve().parents[2]
APP_VERSION = (BASE_DIR / "VERSION").read_text(encoding="utf-8").strip()
APP_BUILD_ID = (
    os.getenv("WATER_INTEL_BUILD_ID")
    or os.getenv("VERCEL_GIT_COMMIT_SHA")
    or os.getenv("GITHUB_SHA")
    or ""
)[:7]
OUTPUTS_DIR = BASE_DIR / "outputs"
SITE_SUMMARY_PATH = OUTPUTS_DIR / "site_summary.csv"
ANOMALIES_PATH = OUTPUTS_DIR / "anomalies.csv"
ANNEX_PATH = OUTPUTS_DIR / "anomaly_event_annex.csv"


app = FastAPI(title="Water-Intel API", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise HTTPException(status_code=500, detail=f"Missing data file: {path.name}")

    try:
        return pd.read_csv(path)
    except Exception as exc:  # pragma: no cover - defensive path for local demo IO issues
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read data file: {path.name}",
        ) from exc


def _to_records(dataframe: pd.DataFrame) -> list[dict]:
    cleaned = dataframe.where(pd.notna(dataframe), None)
    return jsonable_encoder(cleaned.to_dict(orient="records"))


def _clean_mapping(mapping: dict) -> dict:
    cleaned: dict = {}
    for key, value in mapping.items():
        if pd.isna(value):
            cleaned[key] = None
        elif hasattr(value, "item"):
            cleaned[key] = value.item()
        else:
            cleaned[key] = value
    return cleaned


def _load_site_summaries() -> pd.DataFrame:
    dataframe = _load_csv(SITE_SUMMARY_PATH)
    if "station_id" in dataframe.columns:
        dataframe["station_id"] = dataframe["station_id"].astype(str)
    return dataframe


def _load_site_event_chips() -> dict[str, dict]:
    if not ANNEX_PATH.exists():
        return {}

    annex = _load_csv(ANNEX_PATH)
    if len(annex) == 0 or "station_id" not in annex.columns:
        return {}

    annex = annex.copy()
    annex["station_id"] = annex["station_id"].astype(str)

    if "confidence" not in annex.columns:
        annex["confidence"] = None
    if "matched_event_type" not in annex.columns:
        annex["matched_event_type"] = None

    per_station: dict[str, dict] = {}

    for station_id, group in annex.groupby("station_id"):
        corroborated_count = int(group["confidence"].isin(["High", "Possible"]).sum())
        has_data_quality_flag = bool((group["matched_event_type"] == "data_quality").any())

        per_station[station_id] = {
            "corroborated_event_count": corroborated_count,
            "has_data_quality_flag": has_data_quality_flag,
        }

    return per_station


def _load_anomalies() -> pd.DataFrame:
    dataframe = _load_csv(ANOMALIES_PATH)
    if "station_id" in dataframe.columns:
        dataframe["station_id"] = dataframe["station_id"].astype(str)
    if "is_anomaly" in dataframe.columns:
        dataframe = dataframe[dataframe["is_anomaly"] == 1]

    dataframe["event_confidence"] = None

    if ANNEX_PATH.exists() and len(dataframe) > 0:
        annex = _load_csv(ANNEX_PATH)
        if len(annex) > 0:
            annex["station_id"] = annex["station_id"].astype(str)
            annex["date_key"] = pd.to_datetime(annex["date"], errors="coerce").dt.strftime("%Y-%m-%d")

            anomalies_for_merge = dataframe.copy()
            anomalies_for_merge["date_key"] = pd.to_datetime(
                anomalies_for_merge["timestamp"],
                errors="coerce",
            ).dt.strftime("%Y-%m-%d")

            merge_cols = [
                "station_id",
                "date_key",
                "parameter",
                "confidence",
                "matched_event_type",
                "matched_event_date",
                "matched_event_description",
            ]
            merged = anomalies_for_merge.merge(
                annex[merge_cols],
                on=["station_id", "date_key", "parameter"],
                how="left",
            )
            merged["event_confidence"] = merged["confidence"]
            merged = merged.drop(columns=["date_key", "confidence"])
            dataframe = merged

    for column in [
        "matched_event_type",
        "matched_event_date",
        "matched_event_description",
    ]:
        if column not in dataframe.columns:
            dataframe[column] = None

    return dataframe.sort_values("timestamp", ascending=False)


def _get_site_row(site_id: str) -> pd.Series:
    site_summaries = _load_site_summaries()
    matches = site_summaries[site_summaries["station_id"] == site_id]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")
    return matches.iloc[0]


@app.get("/health")
def get_health() -> dict[str, str | None]:
    return {"status": "ok", "version": APP_VERSION, "build": APP_BUILD_ID or None}


@app.get("/sites")
def list_sites() -> list[dict]:
    site_summaries = _load_site_summaries().copy()
    site_summaries["corroborated_event_count"] = pd.NA
    site_summaries["has_data_quality_flag"] = pd.NA

    event_chips = _load_site_event_chips()

    if len(site_summaries) > 0 and event_chips:
        for index, station_id in site_summaries["station_id"].items():
            chip = event_chips.get(station_id)
            if chip is None:
                continue

            site_summaries.at[index, "corroborated_event_count"] = chip[
                "corroborated_event_count"
            ]
            site_summaries.at[index, "has_data_quality_flag"] = chip[
                "has_data_quality_flag"
            ]

    return _to_records(site_summaries)


@app.get("/sites/{site_id}")
def get_site(site_id: str) -> dict[str, object]:
    site_row = _get_site_row(site_id)
    anomalies = _load_anomalies()
    site_anomalies = anomalies[anomalies["station_id"] == site_id]

    return {
        "site": _clean_mapping(site_row.to_dict()),
        "anomalies": _to_records(site_anomalies),
    }


@app.get("/anomalies")
def list_anomalies(site_id: str | None = Query(default=None)) -> list[dict]:
    anomalies = _load_anomalies()
    if site_id:
        anomalies = anomalies[anomalies["station_id"] == site_id]
    return _to_records(anomalies)


@app.get("/risk/{site_id}")
def get_risk(site_id: str) -> dict[str, object]:
    site_row = _get_site_row(site_id)
    return _clean_mapping(
        {
            "site_id": site_row["station_id"],
            "score": site_row.get("risk_score"),
            "label": site_row.get("risk_label"),
            "last_updated": site_row.get("last_reading_date"),
        }
    )
