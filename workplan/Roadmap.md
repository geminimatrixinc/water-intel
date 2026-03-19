# Roadmap.md — Water-Intel (Gemini Matrix Consulting)
Owner: Mike (Gemini Matrix Consulting)  
Project: **Indigenous Water Safety Early-Warning (Phase 1 = 2A MVP using public/sample water-quality data)**  
Goal: Build a tangible, demo-able MVP that supports **funding + pilot** conversations.

---

## North Star Outcome (what “done” looks like)

By the end of **Week 3**, we can run:

**CSV → Ingest → Process → Feature → Anomaly Model → Risk Score → API → React Dashboard**

…and demo it in **90 seconds**:
- Pick a site
- See current risk score (0–100) + status label
- See anomaly timeline with timestamps + “what likely triggered it”
- (Optional) see forecast baseline

**Guardrail:** This is **2A** (proxy) and does *not* claim advisory prediction. It flags anomalies/trends and supports early-warning decisions.

---

## Repo Structure (target)

```
water-intel/
  README.md
  Roadmap.md
  PROGRESS.md
  .gitignore

  data/
    sample/
      sample_water_quality.csv     # canonical for Phase 1 ingestion
    raw/                           # gitignored
    processed/                     # gitignored (or keep small sample outputs)

  outputs/                         # gitignored (or commit small demo outputs)
    anomalies.csv
    site_summary.csv

  docs/
    screens/
    demo_script.md
    PILOT_ONE_PAGER.md

  src/
    ingest/
      eccc_loader.py
      schema.py
      validate.py
      build_processed.py
    features/
      build_features.py
    models/
      anomaly_iforest.py
      risk_score.py
    reports/
      site_summary.py

  api/
    main.py                        # FastAPI

  web/
    (React app)
```

---

## Working Agreements

### Definition of Done (daily)
Each day ends with:
1) A concrete deliverable (code/notebook/output)
2) `PROGRESS.md` updated (1–3 bullets)
3) Git commit pushed

### Naming conventions
- Inputs: `data/sample/*.csv` or `data/raw/*`
- Processed: `data/processed/*`
- Demo outputs: `outputs/*.csv`
- Screenshots: `docs/screens/*`

---

## Status

### Completed ✅
- **Day 1:** Repo created + initial structure committed
- **Day 2:** Python environment + Jupyter started + hello notebook committed
- **Day 3:** Folder structure finalized, `.gitignore` updated, web app scaffolded (Next.js)
- **Day 4:** Ingestion skeleton (`eccc_loader.py`, `schema.py`, `validate.py`) — 13,116 records validated clean
- **Day 5:** Raw → Processed pipeline (`build_processed.py`) — clean, deduplicated, ISO-8601
- **Day 7:** Feature engineering (`build_features.py`) — rolling stats, z-scores, gap flags
- **Day 7.5:** Ontario PWQMN data sourcing — switched to Grand River watershed data (8 stations near Six Nations, 13,194 rows, 103 params, 2019–2024)
- **Planning:** Task breakdown created in `workplan/tasks/`, PROGRESS.md started, architecture documented, pilot targets + AI outreach playbook

### Current focus ▶
- **Day 8:** Anomaly detection (Isolation Forest) on Grand River features

### Task Files
Detailed acceptance criteria for each day: `workplan/tasks/`

---

# Phase 1 MVP (2A) — Daily Plan

## Week 1 — Pipeline + First ML Result

### Day 3 — Finalize repo layout for Phase 1
**Deliverable**
- Create folders: `src/ingest`, `src/features`, `src/models`, `src/reports`, `docs/screens`, `data/sample`, `data/raw`, `data/processed`, `outputs`
- Ensure `.gitignore` covers: `data/raw`, `data/processed`, `outputs` (optionally keep a small subset committed)

**Commit**
- `chore: finalize phase1 folder structure`

**Checklist**
- [ ] Folders match target structure
- [ ] `.gitignore` updated
- [ ] `PROGRESS.md` updated


### Day 4 — Ingestion skeleton + schema contract
**Use this file for ingestion:** `data/sample/sample_water_quality.csv`

**Deliverable**
- `src/ingest/schema.py`: expected columns + basic type hints
- `src/ingest/validate.py`: validate required columns + print useful errors
- `src/ingest/eccc_loader.py`: load CSV, parse timestamp column, normalize column names

**Commit**
- `feat: eccc ingest + schema validation`

**Checklist**
- [ ] Loader reads CSV successfully
- [ ] Validation fails fast with clear messages
- [ ] Timestamp parsing works


### Day 5 — Raw → Processed pipeline (first pass)
**Deliverable**
- `src/ingest/build_processed.py`
  - Input: `data/sample/sample_water_quality.csv`
  - Output: `data/processed/eccc_processed.csv` (or parquet)
  - Standard schema (choose one):
    - Wide: `site_id, timestamp, ph, turbidity, chlorine, temp, ...`
    - Long: `site_id, timestamp, parameter, value`

**Commit**
- `feat: build processed eccc dataset`

**Checklist**
- [ ] Processed output generated
- [ ] Consistent schema documented in `README` or `data/DATA_DICTIONARY.md`


### Day 6 — EDA notebook (repeatable)
**Deliverable**
- `ml/notebooks/02_eda.ipynb` reads processed data
- Plots top signals over time (per site if possible)
- Saves 2–3 images in `docs/screens/`

**Commit**
- `docs: processed-data EDA + screenshots`

**Checklist**
- [ ] EDA runs end-to-end
- [ ] At least 2 screenshots exported


### Day 7 — Feature engineering v1 (rolling + deltas)
**Deliverable**
- `src/features/build_features.py`
  - rolling mean/std (7/14/30 window)
  - delta from previous reading
  - missingness flags
- Output: `data/processed/eccc_features.csv`

**Commit**
- `feat: feature engineering v1 (rolling + deltas)`

**Checklist**
- [ ] Features generated without NaN explosions
- [ ] Feature columns documented


---

## Week 2 — Model + Explanations + Reports

### Day 8 — Baseline anomaly model (Isolation Forest)
**Deliverable**
- `src/models/anomaly_iforest.py`
  - Train (global baseline first or per site)
  - Produce: `outputs/anomalies.csv`
    - `site_id, timestamp, anomaly_score, is_anomaly`

**Commit**
- `feat: baseline anomaly detection (isolation forest)`

**Checklist**
- [ ] Model runs deterministically enough for demos
- [ ] Output file created


### Day 9 — “What triggered it?” driver hints (pragmatic)
**Deliverable**
- Add driver hints using z-scores on features for each anomaly
- Update `outputs/anomalies.csv` with:
  - `top_features` (comma separated)

**Commit**
- `feat: anomaly driver hints (top contributing features)`

**Checklist**
- [ ] Top 3 driver features per anomaly
- [ ] Looks reasonable in output


### Day 10 — Site summary report
**Deliverable**
- `src/reports/site_summary.py`
  - anomalies per site (last 30 days)
  - last reading timestamp
  - rolling anomaly rate
- Output: `outputs/site_summary.csv`

**Commit**
- `feat: site summary report output`

**Checklist**
- [ ] Site summary generated
- [ ] Ready for dashboard dropdown


### Day 11 — Risk score v1 (0–100) + guardrails baked in
**Deliverable**
- `src/models/risk_score.py`
  - Combine anomaly_score + anomaly frequency into a 0–100 score
  - Map to labels: `Safe`, `Watch`, `Concern`
- Document guardrails in docstring + README (2A proxy)

**Commit**
- `feat: risk scoring v1 (2A proxy)`

**Checklist**
- [ ] Deterministic risk score
- [ ] No claims of “advisory prediction”


---

## Week 3 — API + UI + Demo Pack

### Day 12 — Minimal API (FastAPI) to serve risk + anomalies
**Deliverable**
- `api/main.py` (FastAPI)
  - `GET /health`
  - `GET /sites` → reads `outputs/site_summary.csv`
  - `GET /anomalies?site_id=...` → reads `outputs/anomalies.csv`
  - `GET /risk?site_id=...` → returns `{score, label, last_updated}`

**Commit**
- `feat: api endpoints for site risk + anomalies`

**Checklist**
- [ ] API runs locally
- [ ] Endpoints return JSON reliably


### Day 13 — React operator dashboard v1
**Deliverable**
- React app in `/web`
- Views:
  - Site picker
  - Risk card (score + label)
  - Anomaly table (timestamp + score + top_features)

**Commit**
- `feat: web dashboard v1 (risk + anomalies)`

**Checklist**
- [ ] UI loads and calls API
- [ ] Displays real data


### Day 14 — Demo pack + pilot-ready artifact
**Deliverable**
- `docs/demo_script.md` (60–90 sec walkthrough)
- `docs/PILOT_ONE_PAGER.md` (problem → solution → what funding enables)
- 2–3 screenshots in `docs/screens/`

**Commit**
- `docs: pilot one-pager + demo script + screenshots`

**Checklist**
- [ ] Demo is tight and repeatable
- [ ] One-pager makes the “Phase 2 ask” obvious

---

# Week 4 (Traction Week) — Company Presence + Credibility + Funding

> Goal: stop building new product features unless a partner requests it.
> Focus: company website, HostSigner deployment, outreach, funding applications.

### Day 15 — Record 90-second demo video + finalize business docs
**Deliverable**
- `docs/demo_video_link.md` (or `docs/demo_script.md` + extra screenshots)
- Review and finalize `docs/MISSION_STATEMENT.md` and `docs/BUSINESS_PLAN.md`

### Day 16 — Add product landing page on Gemini Matrix site
**Deliverable**
- Page content draft (copy) for: `/products/water-intelligence`
- Includes: problem, what it does, screenshots, pilot CTA

### Day 17 — Pilot outreach list (3–5 targets)
**Deliverable**
- `docs/pilot_targets.md` with:
  - org type
  - contact role (not names required yet)
  - what we’re asking for (30-min call, LOI)

### Day 18 — Soft outreach message + send to 3 targets
**Deliverable**
- `docs/outreach_email.md` (template)
- Send to 3 targets (track results in PROGRESS)

### Day 19 — Funding application skeleton
**Deliverable**
- `docs/funding_application_skeleton.md`
  - outcomes
  - budget categories
  - timeline
  - evaluation plan

---

# Exit Criteria (when we stop building and start pitching)

We are "Week 4 done" when all are true:
- [ ] 90-second demo works without explaining ML
- [ ] Dashboard shows real anomaly/risk output
- [ ] Company website live on HostSigner with custom domain
- [ ] Water-Intel product page on company site with screenshots/demo
- [ ] Mission statement and business plan finalized
- [ ] Clear Phase 2 plan exists (pilot + calibration + governance)
- [ ] Guardrails are explicit (2A proxy, not 2B advisory prediction)
- [ ] At least 1 funding application in progress

---

# Backlog (only if needed / requested by pilot partner)

- Authentication + roles (operator vs leadership)
- Multi-tenant data isolation (community-by-community)
- Map view / watershed visualization
- Stronger explainability (SHAP for supervised model, if we add labels)
- Forecasting (Prophet → LSTM)
- Dataset 1 (ISC advisories) analytics integration (not prediction)
- Sensor ingestion + streaming pipeline
- Production deployment (Docker, CI/CD, hosting)
- Company website CMS / blog for updates
- Gemini Pro AI integration for content and reporting

---

# PROGRESS.md Template (copy/paste)

## Progress Log

### YYYY-MM-DD
- ✅ Done:
- 🧠 Learned:
- 🔥 Next:

---

# Notes / Guardrails (keep consistent)

- Phase 1 is **2A**: public/sample data + anomaly/risk proxy.
- Phase 2 is where validation happens with real operator/community inputs.
- We do **not** claim medical/public health certainty.
- “Risk score” is decision support, not a compliance certification.
