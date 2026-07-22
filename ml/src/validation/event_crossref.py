"""
Anomaly Event Cross-Reference — Preliminary Validation Annex

Cross-references top-scoring anomalies against Ontario spill records and
hydrometric flow spikes to produce an evidence annex for the technical brief.

Usage:
    python -m src.validation.event_crossref
    python -m src.validation.event_crossref --top-n 25 --window-days 7
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # water-intel/
ML_ROOT = PROJECT_ROOT / "ml"
sys.path.insert(0, str(ML_ROOT))

from src.ingest.hydrometric_loader import load_flow_spikes
from src.ingest.spills_loader import load_spills

# Defaults
DEFAULT_ANOMALIES = PROJECT_ROOT / "outputs" / "anomalies.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "outputs" / "anomaly_event_annex.csv"

# Station mapping from day20a task brief (verified 2026-07-21).
# Municipality keys are short word-boundary forms because the spills file spells
# locations inconsistently (e.g. "HALDIMAND COUNTY", "Haldimand",
# "HALDIMAND COUNTY;NORFOLK COUNTY", "Brant", "brant", "WATERLOO;WILMOT").
# Matching is \b<key>\b so "BRANT" does not match "BRANTFORD".
STATION_CONTEXT = {
    "16018402702": {"municipality": "BRANTFORD", "gauge_id": "02GB001"},
    "16018409302": {"municipality": "BRANT", "gauge_id": "02GB007"},
    "16018412802": {"municipality": "HALDIMAND", "gauge_id": "02GB010"},
    "16018409202": {"municipality": "HALDIMAND", "gauge_id": "02GB010"},
    "16018403502": {"municipality": "HALDIMAND", "gauge_id": "02GB010"},
    "16018401202": {"municipality": "WATERLOO", "gauge_id": "02GA003"},
    "16018401002": {"municipality": "BRANT", "gauge_id": None},
    "16018400902": {"municipality": "BRANT", "gauge_id": None},
}

# Spills rows containing these terms are a different municipality that would
# otherwise word-boundary match (e.g. "ALNWICK-HALDIMAND" township in
# Northumberland County is not Haldimand County).
MUNICIPALITY_EXCLUDE = {
    "HALDIMAND": ["ALNWICK"],
}

TARGET_GAUGES = sorted({v["gauge_id"] for v in STATION_CONTEXT.values() if v["gauge_id"]})

# Known York conductivity data-quality anomaly (resolved 2026-07-21)
YORK_STATION_ID = "16018409202"
YORK_DATE = pd.Timestamp("2021-01-27")

METAL_TERMS = {
    "ALUMIN",
    "ANTIMONY",
    "ARSENIC",
    "BARIUM",
    "BERYLLIUM",
    "BORON",
    "CADMIUM",
    "CALCIUM",
    "CHROM",
    "COBALT",
    "COPPER",
    "IRON",
    "LEAD",
    "MANGANESE",
    "MERCURY",
    "MOLYBD",
    "NICKEL",
    "SELEN",
    "SILVER",
    "SODIUM",
    "STRONTIUM",
    "URANIUM",
    "ZINC",
}


def _classify_parameter_category(text: str) -> str:
    """
    Coarse contaminant category used for high-confidence spill alignment.
    """
    normalized = str(text).upper()

    if any(term in normalized for term in METAL_TERMS):
        return "metal"
    if any(term in normalized for term in ["NITRATE", "NITRITE", "AMMON", "PHOSPH", "NITROGEN"]):
        return "nutrient"
    if "CONDUCT" in normalized:
        return "conductivity"
    if any(term in normalized for term in ["TURBID", "RESIDUE", "SOLID", "SEDIMENT"]):
        return "solids"
    if any(term in normalized for term in ["OIL", "PETROLEUM", "FUEL", "GASOLINE", "DIESEL"]):
        return "hydrocarbon"
    return "other"


def _get_top_drivers(anomaly_row: pd.Series) -> str:
    """
    Build top_drivers text from available anomaly columns.
    """
    features = str(anomaly_row.get("top_features", "")).strip()
    values = str(anomaly_row.get("top_feature_values", "")).strip()

    if features and features.lower() != "nan" and values and values.lower() != "nan":
        return f"{features} ({values})"
    if features and features.lower() != "nan":
        return features
    return ""


def _collapse_same_visit(flagged: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse multiple flagged parameters from the same station visit into one
    event row. The highest-scoring parameter becomes the primary row; the other
    co-flagged parameter names are kept in `co_flagged_parameters`.

    Iron and aluminium flagged together on the same date are one event, not two.
    """
    ordered = flagged.sort_values("anomaly_score", ascending=False).reset_index(drop=True)

    co_map = (
        ordered.groupby(["station_id", "date"])["parameter"]
        .apply(list)
        .to_dict()
    )

    primary = ordered.drop_duplicates(subset=["station_id", "date"], keep="first").copy()
    primary["co_flagged_parameters"] = primary.apply(
        lambda row: "; ".join(
            str(p).strip()
            for p in co_map[(row["station_id"], row["date"])]
            if p != row["parameter"]
        ),
        axis=1,
    )
    return primary


def _apply_station_context(anom_df: pd.DataFrame) -> pd.DataFrame:
    """
    Attach static municipality + gauge context from task mapping table.
    """
    enriched = anom_df.copy()
    enriched["station_id"] = enriched["station_id"].astype(str)
    enriched["mapped_municipality"] = enriched["station_id"].map(
        lambda sid: STATION_CONTEXT.get(sid, {}).get("municipality", "")
    )
    enriched["mapped_gauge_id"] = enriched["station_id"].map(
        lambda sid: STATION_CONTEXT.get(sid, {}).get("gauge_id", "")
    )
    return enriched


def _find_spill_matches(
    spills_df: pd.DataFrame,
    municipality: str,
    anomaly_date: pd.Timestamp,
    window_days: int,
) -> pd.DataFrame:
    """
    Return spills matching municipality and date window for one anomaly.

    Municipality matching is word-boundary substring containment, not equality,
    because the spills file spells locations inconsistently and uses
    semicolon-delimited multi-municipality rows. Known false-positive
    municipalities (MUNICIPALITY_EXCLUDE) are filtered out.
    """
    if len(spills_df) == 0 or not municipality:
        return pd.DataFrame(columns=spills_df.columns)

    pattern = rf"\b{re.escape(municipality)}\b"
    muni_mask = spills_df["municipality"].str.contains(
        pattern, case=False, na=False, regex=True
    )
    for exclude_term in MUNICIPALITY_EXCLUDE.get(municipality, []):
        muni_mask &= ~spills_df["municipality"].str.contains(
            exclude_term, case=False, na=False, regex=False
        )

    window = pd.Timedelta(days=window_days)
    mask = muni_mask & ((spills_df["date"] - anomaly_date).abs() <= window)
    return spills_df[mask].copy().sort_values("date")


def _find_flow_matches(
    flow_df: pd.DataFrame,
    gauge_id: str,
    anomaly_date: pd.Timestamp,
    window_days: int,
) -> pd.DataFrame:
    """
    Return flow spike matches for one anomaly.
    """
    if len(flow_df) == 0 or not gauge_id:
        return pd.DataFrame(columns=flow_df.columns)

    window = pd.Timedelta(days=window_days)
    mask = (
        (flow_df["gauge_id"] == gauge_id)
        & (flow_df["is_flow_spike"] == 1)
        & ((flow_df["date"] - anomaly_date).abs() <= window)
    )
    return flow_df[mask].copy().sort_values("date")


def _build_match_row(anomaly_row: pd.Series, spills: pd.DataFrame, flows: pd.DataFrame) -> dict:
    """
    Build one annex row for a single anomaly.
    """
    anomaly_date = anomaly_row["date"]
    parameter = str(anomaly_row.get("parameter", ""))
    parameter_category = _classify_parameter_category(parameter)

    # Best spill = the high-qualifying one if any (category match within 3 days),
    # otherwise the nearest in the window. The description must reference the
    # same event that justified the confidence tier.
    spill_high = False
    best_spill = None
    if len(spills) > 0:
        spills = spills.assign(_dist_days=(spills["date"] - anomaly_date).abs().dt.days)
        for _, spill_row in spills.sort_values("_dist_days").iterrows():
            spill_category = _classify_parameter_category(spill_row.get("contaminant", ""))
            if (
                spill_row["_dist_days"] <= 3
                and spill_category == parameter_category
                and spill_category != "other"
            ):
                spill_high = True
                best_spill = spill_row
                break
        if best_spill is None:
            best_spill = spills.sort_values("_dist_days").iloc[0]

    flow_high = False
    nearest_flow = None
    if len(flows) > 0:
        flows = flows.assign(_dist_days=(flows["date"] - anomaly_date).abs().dt.days)
        nearest_flow = flows.sort_values("_dist_days").iloc[0]
        flow_high = int(nearest_flow["_dist_days"]) <= 2

    if spill_high or flow_high:
        confidence = "High"
    elif len(spills) > 0 or len(flows) > 0:
        confidence = "Possible"
    else:
        confidence = "None"

    if confidence == "None":
        matched_event_type = "none"
        matched_event_date = ""
        matched_event_description = "No public spill or flow-spike match found in the window."
    elif spill_high and nearest_flow is not None:
        matched_event_type = "spill_and_flow_spike"
        matched_event_date = best_spill["date"].strftime("%Y-%m-%d")
        matched_event_description = (
            f"Spill in {best_spill['municipality']} (contaminant: {best_spill['contaminant']}) "
            f"and gauge {nearest_flow['gauge_id']} flow spike near anomaly date."
        )
    elif best_spill is not None:
        matched_event_type = "spill"
        matched_event_date = best_spill["date"].strftime("%Y-%m-%d")
        matched_event_description = (
            f"Spill in {best_spill['municipality']} (contaminant: {best_spill['contaminant']}; "
            f"event: {best_spill['incident_event']})."
        )
    else:
        matched_event_type = "flow_spike"
        matched_event_date = nearest_flow["date"].strftime("%Y-%m-%d")
        matched_event_description = (
            f"Hydrometric flow spike at gauge {nearest_flow['gauge_id']} "
            f"{int(nearest_flow['_dist_days'])} day(s) from anomaly date."
        )

    return {
        "station_id": str(anomaly_row["station_id"]),
        "date": anomaly_date.strftime("%Y-%m-%d"),
        "parameter": parameter,
        "value": anomaly_row.get("value", ""),
        "anomaly_score": anomaly_row.get("anomaly_score", ""),
        "top_drivers": _get_top_drivers(anomaly_row),
        "co_flagged_parameters": anomaly_row.get("co_flagged_parameters", ""),
        "matched_event_type": matched_event_type,
        "matched_event_date": matched_event_date,
        "matched_event_description": matched_event_description,
        "confidence": confidence,
    }


def build_event_crossref(
    anomalies_path: Path = DEFAULT_ANOMALIES,
    top_n: int = 25,
    window_days: int = 7,
    max_per_station: int = 5,
    output_path: Path = DEFAULT_OUTPUT,
) -> pd.DataFrame:
    """
    Build anomaly-to-event cross-reference table for the technical-brief annex.

    Selection order:
    1. Collapse same-visit multi-parameter flags into one event row each
    2. Cap events per station (max_per_station) so one station cannot dominate
    3. Take the overall top_n events by anomaly score
    """
    print(f"Loading anomalies from {anomalies_path}")
    anomalies_df = pd.read_csv(anomalies_path)
    anomalies_df["timestamp"] = pd.to_datetime(anomalies_df["timestamp"], errors="coerce")
    anomalies_df = anomalies_df.dropna(subset=["timestamp"])

    flagged = anomalies_df[anomalies_df["is_anomaly"] == 1].copy()
    flagged["station_id"] = flagged["station_id"].astype(str)
    flagged["date"] = flagged["timestamp"].dt.normalize()
    print(f"  Flagged anomaly readings: {len(flagged)}")

    flagged = _collapse_same_visit(flagged)
    print(f"  Distinct station-visit events: {len(flagged)}")

    flagged = (
        flagged.sort_values("anomaly_score", ascending=False)
        .groupby("station_id", sort=False)
        .head(max_per_station)
    )
    flagged = (
        flagged.sort_values("anomaly_score", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    print(
        f"  Selected top {len(flagged)} events "
        f"(cap {max_per_station}/station, {flagged['station_id'].nunique()} stations)"
    )

    flagged = _apply_station_context(flagged)

    print("Loading spills subset...")
    spills_df = load_spills(min_year=2019)
    if len(spills_df) > 0:
        spills_df["date"] = pd.to_datetime(spills_df["date"], errors="coerce").dt.normalize()
        spills_df = spills_df.dropna(subset=["date"]).copy()

    print("Loading hydrometric flow spikes...")
    flow_df = load_flow_spikes(gauge_ids=TARGET_GAUGES, min_year=2019, spike_threshold_std=2.0)
    if len(flow_df) > 0:
        flow_df["date"] = pd.to_datetime(flow_df["date"], errors="coerce").dt.normalize()
        flow_df = flow_df.dropna(subset=["date"]).copy()

    rows = []

    for _, anomaly_row in flagged.iterrows():
        station_id = str(anomaly_row["station_id"])
        anomaly_date = anomaly_row["date"]
        parameter = str(anomaly_row.get("parameter", ""))

        # Special-case York conductivity event as resolved data-quality finding.
        if (
            station_id == YORK_STATION_ID
            and anomaly_date == YORK_DATE
            and "CONDUCT" in parameter.upper()
        ):
            rows.append(
                {
                    "station_id": station_id,
                    "date": anomaly_date.strftime("%Y-%m-%d"),
                    "parameter": parameter,
                    "value": anomaly_row.get("value", ""),
                    "anomaly_score": anomaly_row.get("anomaly_score", ""),
                    "top_drivers": _get_top_drivers(anomaly_row),
                    "co_flagged_parameters": anomaly_row.get("co_flagged_parameters", ""),
                    "matched_event_type": "data_quality",
                    "matched_event_date": "2021-01-27",
                    "matched_event_description": (
                        "Same-visit field/lab conductivity divergence at York "
                        "(CONDAM 10900 vs COND25 1020, water temp 0.8C); "
                        "consistent with instrument/data-quality issue rather than "
                        "a confirmed contamination event."
                    ),
                    "confidence": "High",
                }
            )
            continue

        municipality = str(anomaly_row.get("mapped_municipality", ""))
        gauge_id = str(anomaly_row.get("mapped_gauge_id", ""))

        spills_match = _find_spill_matches(
            spills_df=spills_df,
            municipality=municipality,
            anomaly_date=anomaly_date,
            window_days=window_days,
        )
        flow_match = _find_flow_matches(
            flow_df=flow_df,
            gauge_id=gauge_id,
            anomaly_date=anomaly_date,
            window_days=window_days,
        )

        rows.append(_build_match_row(anomaly_row, spills_match, flow_match))

    result = pd.DataFrame(rows)
    result = result[
        [
            "station_id",
            "date",
            "parameter",
            "value",
            "anomaly_score",
            "top_drivers",
            "co_flagged_parameters",
            "matched_event_type",
            "matched_event_date",
            "matched_event_description",
            "confidence",
        ]
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    # Summary in the same style as other report modules.
    conf_counts = result["confidence"].value_counts().to_dict()
    type_counts = result["matched_event_type"].value_counts().to_dict()

    print(f"\n✅ Event cross-reference written to {output_path}")
    print(f"   Rows: {len(result)}")
    print(f"   Columns: {result.columns.tolist()}")
    print("\n   Confidence tier counts:")
    for tier in ["High", "Possible", "None"]:
        print(f"     {tier:8s} {conf_counts.get(tier, 0)}")
    print("\n   Matched event type counts:")
    for event_type, count in sorted(type_counts.items()):
        print(f"     {event_type:20s} {count}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Cross-reference top anomalies against spill/flow events"
    )
    parser.add_argument(
        "--anomalies",
        "-a",
        type=Path,
        default=DEFAULT_ANOMALIES,
        help=f"Input anomalies CSV (default: {DEFAULT_ANOMALIES})",
    )
    parser.add_argument(
        "--top-n",
        "-n",
        type=int,
        default=25,
        help="Number of highest-scoring anomalies to evaluate (default: 25)",
    )
    parser.add_argument(
        "--window-days",
        "-w",
        type=int,
        default=7,
        help="Date matching window in days (default: 7)",
    )
    parser.add_argument(
        "--max-per-station",
        "-m",
        type=int,
        default=5,
        help="Maximum events per station in the annex (default: 5)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output annex CSV path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    build_event_crossref(
        anomalies_path=args.anomalies,
        top_n=args.top_n,
        window_days=args.window_days,
        max_per_station=args.max_per_station,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
