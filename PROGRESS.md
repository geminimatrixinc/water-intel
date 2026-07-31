# Progress Log — Water-Intel (Phase 1 MVP)

> Updated daily. Each entry records what shipped, what was learned, and what's next.

---

### 2026-07-22 — Credentials Gate: IBD Confirmed, CCIB Membership Approved
- ✅ Done: IBD registration confirmed active and verified (Gemini Matrix Consulting Inc., address updated to Ohsweken)
- ✅ Done: CCIB (formerly CCAB) membership approved; CIB certification application submitted as the next step
- ✅ Done: IBD NAICS/description/website update email sent to **REA-IBD@sac-isc.gc.ca** (the portal has no self-serve edit — ISC staff process changes). Requested: NAICS **541514** (computer systems design, primary) + **518210** (data hosting) + **541620** (environmental consulting) added alongside existing codes; website geminimatrixinc.com; AI/ML business description
- ✅ Resolved: Shareholders Registry — the formal version prepared by the accountant was submitted (supersedes the unsigned draft concern); CIB certification now purely awaiting CCIB review
- 🔥 Next: deploy updated Water-Intel (validation work + regenerated outputs) to Hostinger → then send brief v2 (with Section 6 event validation) to Colin ~July 28; **~Aug 5**: verify public IBD directory shows NAICS 541514 — escalate to navigator@sac-isc.gc.ca if not

### 2026-07-21 — Day 20a: Event Cross-Reference Validation (revision pass complete)
- ✅ Done: fixed municipality join in `ml/src/validation/event_crossref.py` — word-boundary matching with short-form keys (BRANTFORD/BRANT/HALDIMAND/WATERLOO) + ALNWICK-HALDIMAND false-positive exclusion; previous exact-equality join silently missed all spill candidates for several stations
- ✅ Done: same-visit collapse (`co_flagged_parameters`) — iron+aluminum flagged together now count as one event; 660 flagged readings → 263 distinct station-visit events
- ✅ Done: 5-per-station cap — annex now covers 7 of 8 stations instead of being dominated by Fairchild Creek repeats
- ✅ Done: matched-event descriptions now reference the *nearest* event (with day distance) instead of the earliest in window — descriptions and confidence tiers are now consistent for reviewers who cross-check dates
- ✅ Done: regenerated `outputs/anomaly_event_annex.csv` (25 events: 9 High / 4 Possible / 12 None) and rewrote brief Section 6 (method, findings, honest caveat, new table); aligned Section 3 York sentence with the data-quality resolution
- 🧠 Learned: the Jan 13 2020 event strengthened under revision — now visible at **5 of 8 stations** with same-day flow spikes at 2 independent gauges; a second multi-station event (Mar 10–11 2020) surfaced; zero spill matches is now a *sound* null (3,749 surface-water records properly evaluated at county-level resolution)
- 🧠 Learned: Fairchild Creek's 5 uncorroborated events + dead flow gauge (02GB007, no post-2019 data) = the honest open question for the expert review — real local pattern vs global-model over-flagging (brief Limitation #5)
- 🔥 Next: regenerate brief PDF, draft Colin follow-up email (~Aug 5) leading with "you asked about event data"; leftover polish items parked in `planning/tasks/day20b_ui_and_general_improvements.md`

### 2026-07-14 — Technical Brief Shipped + Planning Recalibration
- ✅ Done: wrote `docs/TECHNICAL_BRIEF.md` (5 sections, hydrology-reviewer audience, all claims verified against pipeline code) and rendered it to a 3-page professional PDF (`docs/Water-Intel_Technical_Brief_July2026.pdf`)
- ✅ Done: sent the brief to Colin Gibson as a reply in the June 25 meeting thread — closes the offer made in the thank-you email; ball now in his court (no further nudges; ~Aug 4 fallback lifts the hold on parallel McMaster outreach)
- ✅ Done: full CTO-level planning review — corrected Innovations Canada model (challenge-based procurement, not open grant), flagged SR&ED expenditure trap (unpaid founder labour not claimable), added liability/E&O + program-change + solo-founder risks to the business plan, refreshed stale timeline
- ✅ Done: new gated tasks — day24 (real-time ECCC/GRCA data spike) and day25 (second-vertical proof on public well data); validation-annex upgrade added to day19_5
- 🧠 Learned: the expert meeting was only ever *mentioned*, never scheduled — gate follow-on work on dates, not on events other people control; a forwardable artifact is what converts a verbal intention into a meeting
- 🔥 Next: credentials time-box (IBD verify + CCAB submission), then validation annex as the second touchpoint

### 2026-05-03 — Corporate Site + Water-Intel Production Deployment
- ✅ Done: deployed the Gemini Matrix corporate website to Hostinger on the production domain
- ✅ Done: deployed the Water-Intel dashboard plus FastAPI backend to Hostinger and brought the live dashboard up at `water.geminimatrixinc.com`
- ✅ Done: updated the corporate site navigation so it links into the live Water-Intel dashboard
- 🧠 Learned: `outputs/*.csv` are runtime dependencies for the deployed MVP even though they remain generated artifacts, so deployment must either upload them or regenerate them on the server
- 🔥 Next: move into Day 18 outreach and Day 19 funding-prep work, and decide whether future deploys should generate `outputs/` automatically or track a committed demo artifact set

### 2026-05-03 — Repository Structure Refactor
- ✅ Done: moved the canonical FastAPI app to `services/api/main.py` and kept `api/main.py` plus `api/requirements.txt` as compatibility shims
- ✅ Done: moved the canonical dashboard launcher to `ops/scripts/run-dashboard.ps1` and kept the root `run-dashboard.ps1` as a forwarding wrapper
- ✅ Done: established `planning/`, `ai/`, and `packages/contracts/` scaffolding while documenting `web/app/` as a deliberate transitional exception
- ✅ Done: added agent-first scaffolding for prompts, instructions, workflows, MCP schemas, and future skills/hooks
- 🧠 Learned: the safest refactor path is to make canonical locations explicit while leaving legacy entrypoints in place for local developer workflows
- 🔥 Next: continue updating remaining references toward `planning/`, `services/api/`, and future `ai/mcp` ownership as new work lands

### 2026-04-15 — Dashboard/Detail Checkpoint + Focus Shift
- ✅ Done: treated the dashboard summary and site detail views as phase-complete for the current MVP pass
- ✅ Done: updated roadmap and Week 4 planning so the next emphasis is product packaging, company website, Ask Water-Intel positioning, and the technical/pilot brief
- ✅ Done: clarified the product posture as a complementary source-water intelligence suite rather than a competitor to monitoring portals or plant SCADA
- 🧠 Learned: additional dashboard/detail polish is now in diminishing-returns territory unless a real bug or credibility issue appears during packaging
- 🔥 Next: finish Day 14 packaging artifacts, then move directly into Day 16 website work, the brief, and the guided Ask Water-Intel experience

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
- ✅ Done: `services/api/main.py` — FastAPI app serving CSV-backed endpoints for health, sites, site detail, anomalies, and risk (`api/main.py` remains a compatibility shim)
- ✅ Done: `services/api/requirements.txt` — minimal API dependencies (`fastapi`, `uvicorn`, `pandas`) with `api/requirements.txt` preserved as a compatibility include
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
- ✅ Done: Full project audit, task breakdown created in `planning/tasks/`, architecture doc, folder structure gaps closed
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
