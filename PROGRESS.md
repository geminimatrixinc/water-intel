# Progress Log — Water-Intel (Phase 1 MVP)

> Updated daily. Each entry records what shipped, what was learned, and what's next.

---

### 2026-04-12 — Data Refresh + Demo Guardrail Copy
- ✅ Done: checked the Ontario PWQMN catalogue and confirmed there is still no published 2025 measurement file
- ✅ Done: refreshed the raw Ontario source files (`pwqmn_2019_2021.csv`, `pwqmn_2021_2022.csv`, `pwqmn_2023.csv`, `pwqmn_2024.csv`, `pwqmn_coordinates.csv`) from the latest published dataset resources
- ✅ Done: rebuilt `grand_river_processed.csv`, `grand_river_features.csv`, `outputs/anomalies.csv`, and `outputs/site_summary.csv` from the refreshed raw files
- ✅ Done: updated dashboard banner copy to explicitly state that the app uses public historical PWQMN data and that real-time monitoring requires Phase 2 data access
- ✅ Verified: refreshed Grand River coverage now reaches as late as Dec 10, 2024 for some stations; localhost renders the updated disclaimer copy
- 🧠 Learned: the right demo posture is not "almost real-time" but "historical proof of the intelligence layer pending operational data access"
- 🔥 Next: finish Day 14 packaging artifacts — `docs/demo_script.md`, `docs/PILOT_ONE_PAGER.md`, and current screenshots

### 2026-04-12 — Day 14: Dashboard Clarity Pass
- ✅ Done: added plain-language interpretation helpers in `web/app/app/lib/interpretation.ts` for risk legend text, recommended actions, site summaries, and humanized driver hints
- ✅ Done: updated the dashboard and site detail pages to explain what Safe/Watch/Concern mean and what operators should do next
- ✅ Done: added a timeline caption and replaced raw model-feature labels with readable anomaly driver language
- ✅ Verified: localhost dashboard and site detail pages render the new explanation text after restarting the Next.js dev server
- 🧠 Learned: the main usability gap was not the model output itself but the lack of narrative framing around risk and operator action
- 🔥 Next: finish Day 14 demo artifacts — demo script, pilot one-pager, and screenshots

### 2026-04-12 — Day 13: Dashboard Wiring
- ✅ Done: wired `web/app/` to the real FastAPI backend via Next.js proxy routes instead of mock API responses
- ✅ Done: added real TypeScript contracts in `web/app/app/lib/types.ts` and backend URL helpers in `web/app/app/lib/api.ts`
- ✅ Done: replaced mock readings UI with a real risk card, anomaly history table, and persistent historical-analysis banner
- ✅ Done: added anomaly timeline chart using `recharts` and loading states for dashboard routes
- ✅ Verified: `/dashboard` and `/dashboard/sites/BC08NL0005` render live data from FastAPI; invalid site routes return 404
- 🧠 Learned: the frontend should align to the anomaly-driven data model (`station_id`, `risk_label`, anomaly rows) rather than force the backend to imitate old turbidity/pH/chlorine mocks
- 🔥 Next: Day 14 — demo pack and pilot-ready one-pager built around the live dashboard flow

### 2026-04-12 — Day 12: FastAPI Backend
- ✅ Done: `api/main.py` — FastAPI app serving CSV-backed endpoints for health, sites, site detail, anomalies, and risk
- ✅ Done: `api/requirements.txt` — minimal API dependencies (`fastapi`, `uvicorn`, `pandas`)
- ✅ Verified: local server starts with `uvicorn api.main:app --reload`; `/health`, `/sites`, `/sites/{site_id}`, `/anomalies`, and `/risk/{site_id}` return valid JSON
- ✅ Verified: CORS allows `http://localhost:3000`; missing `site_id` returns 404
- 🧠 Learned: API should treat `station_id` as the canonical identifier and return real anomaly rows rather than mimic the old mock turbidity/ph/chlorine shape
- 🔥 Next: Day 13 — wire the dashboard to the real backend and replace mock route assumptions

### 2026-03-24 — Day 11: Risk Score v1 (0–100)
- ✅ Done: `ml/src/models/risk_score.py` — composite 0–100 risk score + Safe/Watch/Concern labels
- ✅ Output: `outputs/site_summary.csv` enriched with `risk_score` + `risk_label` (now 14 columns)
- ✅ Formula: base (avg_anomaly_score → 0–60) + frequency (anomaly_rate → 0–40) = 0–100
- ✅ Distribution: 24–76, mean 54 — 4 Concern / 3 Watch / 1 Safe — good spread for demo
- ✅ Guardrail documented: "2A proxy score — does not predict ISC drinking water advisories"
- 🔥 Next: Day 12 — FastAPI backend (serve ML outputs as JSON)

### 2026-03-24 — Day 10: Site Summary Report
- ✅ Done: `ml/src/reports/site_summary.py` — one-row-per-station aggregation from anomalies + features
- ✅ Output: `outputs/site_summary.csv` — 8 stations × 12 columns, zero NaN, sorted by anomaly rate
- ✅ Columns: station_id, total_readings, anomaly_count, anomaly_rate, last_reading_date, last_anomaly_date, avg_anomaly_score, parameter_count, date_range_start/end, top_anomaly_parameter, rolling_30d_anomaly_count
- ✅ Top risk: station 16018409202 (8.13% anomaly rate), station 16018403502 (7.75%, 284 anomalies)
- 🧠 Conductivity is the top anomaly parameter at 5 of 8 stations
- 🔥 Next: Day 11 — risk score (0–100 composite)

### 2026-03-23 — Day 9: Anomaly Driver Hints
- ✅ Done: `ml/src/models/driver_hints.py` — per-feature z-scores vs population, top 3 drivers per anomaly
- ✅ Output: `outputs/anomalies.csv` enriched with `top_features` + `top_feature_values` columns (660 anomalies, all with hints)
- ✅ Top drivers: rolling_mean_30 (57%), rolling_mean_14 (55%), rolling_mean_7 (53%) — rolling averages dominate, meaning anomalies are driven by sustained level shifts
- ✅ Secondary drivers: rolling_std (volatility), delta/rate_of_change (spikes) — makes physical sense
- 🧠 Learned: z-score approach is transparent and fast — no SHAP needed for MVP. Operators can read "rolling_mean_30: 3.01" as "this value is 3 standard deviations above the 30-reading average"
- 🔥 Next: Day 10 — site summary report (one row per station)

### 2026-03-19 — Day 8: Baseline Anomaly Detection (Isolation Forest)
- ✅ Done: `ml/src/models/anomaly_iforest.py` — full pipeline: load features → scale → IsolationForest → normalize scores → write CSV
- ✅ Output: `outputs/anomalies.csv` — 13,194 rows, 660 anomalies (5.0% rate, within 3–8% target)
- ✅ Config: contamination=0.05, n_estimators=200, random_state=42, n_jobs=-1
- ✅ Verified: deterministic output (identical MD5 on re-run)
- ✅ Top anomaly: CONDUCTIVITY 10,900 µS/cm at Grand River at York — real water quality event
- ✅ Per-station anomaly rates: 2.0% (Glen Morris) to 8.1% (York) — reasonable spread
- 🧠 Learned: fillna(0) is correct for structurally expected NaN (~4.5% from first readings per rolling window group)
- 🧠 Fixed: scipy DLL import error (`_propack`) — resolved via pip upgrade scipy + scikit-learn
- 🔥 Next: Day 9 — driver hints (top features contributing to each anomaly)

### 2026-03-19 — Day 7.5: Ontario PWQMN Data Sourcing (Grand River)
- ✅ Done: Switched from BC/ECCC sample data to **Ontario Grand River watershed** — directly relevant to Six Nations pilot
- ✅ Built: `ml/src/ingest/pwqmn_loader.py` — handles all 3 PWQMN schema versions (pre-2021, 2021-2022, 2023+)
- ✅ Downloaded: PWQMN 2019–2024 data from data.ontario.ca (Open Government Licence – Ontario)
- ✅ Found: **8 active stations within 45 km of Six Nations** — closest is Big Creek at 4.3 km
- ✅ Output: `grand_river_processed.csv` (13,194 rows, 8 stations, 103 parameters, 2019–2024)
- ✅ Output: `grand_river_features.csv` (13,194 × 19 — feature pipeline works on new data)
- 🧠 Learned: ECCC national network has **zero** Grand River stations (not transboundary). Ontario PWQMN is the data source for inland Ontario rivers. PWQMN publishes annually — 2025 data expected late 2026.
- 🧠 Impact: Demo now shows **their actual watershed** — transforms pitch from "generic tool" to "here's what your river has been doing for 5 years"
- ⚠️ Note: Latest data point is Dec 2024 (~15 month gap). Frame as trend analysis, not live monitoring.
- 🔥 Next: Day 8 — anomaly detection (Isolation Forest) on Grand River features

### 2026-03-18 — Day 7: Feature Engineering v1
- ✅ Done: `ml/src/features/build_features.py` — full pipeline: rolling stats (7/14/30), delta, rate of change, z-score, gap flags
- ✅ Output: `data/processed/eccc_features.csv` — 13,116 rows × 19 columns (11 new feature columns)
- ✅ NaN: minimal (~0.3%) — only first reading per group, as expected
- ✅ Verified: idempotent (identical MD5 on re-run), documented in DATA_DICTIONARY
- 🔥 Next: Day 8 — anomaly detection (Isolation Forest)

### 2026-03-16 — Day 5: Raw → Processed Pipeline
- ✅ Done: `ml/src/ingest/build_processed.py` — full pipeline: load → validate → deduplicate → normalize timestamps → sort → write CSV
- ✅ Output: `data/processed/eccc_processed.csv` — 13,116 rows, 4 stations, 12 parameters, ISO-8601 timestamps
- ✅ Verified: zero duplicates, zero nulls in required columns, idempotent (identical MD5 on re-run)
- 🔥 Next: Day 7 — feature engineering (Day 6 EDA is off critical path)

### 2026-03-14 — Project Planning & Structure
- ✅ Done: Full project audit, task breakdown created in `workplan/tasks/`, architecture doc, folder structure gaps closed
- 🧠 Learned: Days 1–4 deliverables are solid; ML pipeline (features → model → API) is the critical path
- 🔥 Next: Day 5 — build_processed.py (Raw → Processed pipeline)

---

### Day 4 (completed prior)
- ✅ Done: Ingestion skeleton (`eccc_loader.py`, `schema.py`, `validate.py`), validation framework, demo scripts
- 🧠 Learned: ECCC data has 13,116 records across 4 stations, 12 parameters; validation passes clean
- 🔥 Next: Normalize + persist processed output

### Day 3 (completed prior)
- ✅ Done: Folder structure created, sample data files placed, web app scaffolded (Next.js)
- 🔥 Next: Ingestion pipeline

### Day 2 (completed prior)
- ✅ Done: Python environment, Jupyter notebook started

### Day 1 (completed prior)
- ✅ Done: Repo created, initial structure committed, README with project vision
