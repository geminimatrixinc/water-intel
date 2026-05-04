# Day 7.5 — Ontario PWQMN Data Sourcing (Grand River)

**Sprint:** Week 1 — Pipeline + First ML Result  
**Status:** ✅ Done  
**Date:** 2026-03-19  
**Depends on:** Day 5 (build_processed), Day 7 (features)

---

## Objective
Replace the BC Okanagan-Similkameen sample data with Ontario Provincial Water Quality Monitoring Network (PWQMN) data from the Grand River watershed — the actual watershed surrounding Six Nations of the Grand River. This makes the demo directly relevant to our #1 pilot target.

## What Was Done

### Research & Discovery
- Confirmed ECCC national network has **zero Grand River stations** (Grand River is entirely within Ontario, not transboundary — ECCC only monitors border/interprovincial waters)
- ECCC Ontario has only 8 stations total — all on Great Lakes or St. Lawrence border
- Identified Ontario's **Provincial (Stream) Water Quality Monitoring Network (PWQMN)** as the correct data source
- Found **42 stations** in the Grand River drainage basin, 8 active through 2024
- Closest station to Six Nations: **Big Creek at Hwy 54, NW of Caledonia** — only **4.3 km** away

### Data Downloaded
- `data/raw/pwqmn_2019_2021.csv` — 2019 to March 2021
- `data/raw/pwqmn_2021_2022.csv` — 2021-2022
- `data/raw/pwqmn_2023.csv` — 2023
- `data/raw/pwqmn_2024.csv` — 2024
- `data/raw/pwqmn_coordinates.csv` — station lat/lon, names, active status
- `data/raw/eccc_sites_national.csv` — ECCC national site list (reference)
- Source: https://data.ontario.ca/dataset/provincial-stream-water-quality-monitoring-network
- License: Open Government Licence – Ontario

### Code Built
- **`ml/src/ingest/pwqmn_loader.py`** — full PWQMN loader with:
  - Auto-detection of 3 PWQMN schema versions (pre-2021, 2021-2022, 2023+)
  - Column mapping to Water-Intel normalized schema
  - Unit normalization (e.g., "MILLIGRAM PER LITER" → "mg/L")
  - Station filtering for Grand River area
  - Multi-file combining and deduplication

### Pipeline Output
- **`data/processed/grand_river_processed.csv`** — 13,194 rows, 8 stations, 103 parameters
- **`data/processed/grand_river_features.csv`** — 13,194 rows × 19 columns (feature engineering applied)
- Date range: 2019-01-15 → 2024-12-10

### 8 Active Stations Near Six Nations

| Station ID | Location | Distance from Six Nations |
|------------|----------|---------------------------|
| 16018412802 | Big Creek at Hwy 54, NW of Caledonia | 4.3 km |
| 16018409202 | Grand River at York, Haldimand Norfolk Rd 9 | 13.5 km |
| 16018409302 | Fairchild Creek at Harris Rd, Brantford Twp | 14.0 km |
| 16018402702 | Grand River at Cocksutts Bridge, Brantford | 16.8 km |
| 16018400902 | Nith River at Paris | 30.8 km |
| 16018401002 | Grand River at Glen Morris | 34.1 km |
| 16018403502 | Grand River at Dunnville | 39.2 km |
| 16018401202 | Grand River at Blair | 45.5 km |

## Key Decisions
- **Primary demo data is now Grand River** — all downstream pipeline work (Day 8+) should use `grand_river_features.csv`
- **ECCC pipeline preserved** — the original BC data and ECCC loader still work for reference/testing
- **Data gap acknowledged** — latest data is Dec 2024, ~15 months old as of March 2026. PWQMN publishes annually; 2025 data expected later in 2026
- **Demo framing** — "5 years of trend analysis on your actual watershed" rather than real-time monitoring

## Commit Message
```
feat: pwqmn loader + grand river data (8 stations near six nations)
```
