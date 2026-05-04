from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware


APP_VERSION = "0.1.0"
BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUTS_DIR = BASE_DIR / "outputs"
SITE_SUMMARY_PATH = OUTPUTS_DIR / "site_summary.csv"
ANOMALIES_PATH = OUTPUTS_DIR / "anomalies.csv"


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


def _load_anomalies() -> pd.DataFrame:
    dataframe = _load_csv(ANOMALIES_PATH)
    if "station_id" in dataframe.columns:
        dataframe["station_id"] = dataframe["station_id"].astype(str)
    if "is_anomaly" in dataframe.columns:
        dataframe = dataframe[dataframe["is_anomaly"] == 1]
    return dataframe.sort_values("timestamp", ascending=False)


def _get_site_row(site_id: str) -> pd.Series:
    site_summaries = _load_site_summaries()
    matches = site_summaries[site_summaries["station_id"] == site_id]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")
    return matches.iloc[0]


@app.get("/health")
def get_health() -> dict[str, str]:
    return {"status": "ok", "version": APP_VERSION}


@app.get("/sites")
def list_sites() -> list[dict]:
    return _to_records(_load_site_summaries())


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
