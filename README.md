# Indigenous Water Safety — Data + ML Roadmap (MVP)

## North Star goal
Build a data-driven system that can **flag early risk of unsafe drinking water** in First Nations communities, so decision-makers can prioritize intervention **before** a crisis becomes long-term.

This repo intentionally starts with **public datasets** so we can ship an MVP, but the *true* end-goal requires **governance-approved potable-water system data**.

---

## What “solving the problem” actually means (in this project)
### We are NOT trying to predict river height or flooding.
We are trying to help reduce:
- **Boil Water Advisories (BWA)**
- **Do Not Consume (DNC)**
- **Do Not Use (DNU)**

These advisories are driven by:
- contamination (bacteria, metals, turbidity, etc.),
- treatment failures,
- operator capacity / staffing,
- infrastructure condition / underfunding.

---

## Datasets (current + target)

### Dataset 1 — ISC Advisories (Outcome / Label data)
**Purpose:** This is our “ground truth” for the crisis outcome: advisory type, start/lift dates, long-term classification, and basic context (system name, region, corrective measure, project phase, lat/lon).

**File(s):**
- `data/sample_water.csv`
- `data/DATA_DICTIONARY.md`

**How we use it:**
- Build analytics: counts, durations, trends, map views
- ML labels for later:
  - `status` (Active vs Lifted)
  - `is_long_term`
  - `days_under_advisory` (derived)

> NOTE: This dataset is often exported for mapping, but it is still a valid advisory fact table for analysis and modeling.

---

### Dataset 2A — Public surface water quality monitoring (Proxy signal data)
**Source:** Environment and Climate Change Canada (ECCC) “National Long-term Water Quality Monitoring Data”  
**Purpose:** Real measured readings over time (chemistry, bacteria, physical parameters) at monitoring sites.

**What it is:**
- **Public, time-series water quality readings**
- Typically **ambient surface water** (rivers/lakes monitoring sites)
- Great for modeling **water quality deterioration / anomalies**

**What it is NOT:**
- Not guaranteed to be **tap water** at a First Nation distribution system
- Not always directly tied to the water system that triggered an advisory

**Files:**
- `data/sample_water_quality.csv`
- `data/DATA_DICTIONARY_water_quality.md`

**What we can do with 2A right now:**
- Train ML models for:
  - anomaly detection (spikes in turbidity/E. coli/metals)
  - forecasting / trend detection (seasonality + drift)
  - site-level “risk score” time-series
- Build the pipeline + code foundation we’ll later reuse for 2B.

**What we cannot honestly claim with only 2A:**
- “We can predict ISC drinking water advisories in community systems.”
2A can support *proxy insights* and methodology, but it is not the authoritative potable-water dataset.

---

### Dataset 2B — The *real* target for drinking-water safety ML (Requires clearance)
**Goal:** Potable drinking water system data that reflects what causes advisories:
- treatment plant monitoring results (turbidity, residual chlorine, bacteria results, metals)
- system operational status, compliance and inspection outcomes
- risk rating assessments, operator certification/capacity
- maintenance logs, outages, infrastructure condition

**Why 2B matters:**
If we want to truly build an “early warning” model that is accurate at the **community water system** level, we need data collected *at or inside the potable water system*, not just nearby surface water.

**Constraint:**
Access likely requires:
- data-sharing agreements,
- privacy/security review,
- and Indigenous data governance considerations (OCAP® principles and community consent).

**Status:**
- Not included in this repo (public MVP only).
- The repo is structured so we can plug it in later with minimal refactor.

---

## MVP ML Objective (Phase 1)
**Anomaly Detection on Public Water Quality (2A)**

Goal: Build a system that can **flag unusual deterioration signals** in public water quality time-series (ECCC monitoring sites), such as spikes in:
- E. coli / fecal coliforms
- turbidity
- metals (lead/arsenic)
- conductivity / other supporting parameters

Output (per site + timestamp):
- `anomaly_score` (0–1 or z-score style)
- `is_anomaly` (boolean using a threshold)
- `top_contributing_features` (what triggered it)

Important: This Phase 1 model does **not** claim to predict ISC drinking-water advisories for specific First Nations systems.

That requires Phase 2 (2B) potable system data + governance-approved access.

---

## Data Strategy
### Dataset 1 — ISC Advisories (labels/outcomes, contextual)
Used for: policy/progress analytics, mapping, and future supervised learning once 2B is available.
Not used as the direct training label for Phase 1 anomaly detection.

### Dataset 2A — ECCC Water Quality (training + scoring for anomaly detection)
Used for: training and scoring site-level anomaly detection models.

We maintain two forms of 2A data:
- `data/sample/sample_water_quality.csv` → small, committed, used for dev/tests
- `data/raw/` → large real datasets (hundreds of files), NOT committed
- `data/processed/` → normalized merged dataset produced by scripts (parquet/csv), NOT committed

---

## Phase 1 Deliverables
1. Ingest + normalize raw ECCC files into a consistent schema
2. Feature engineering (rolling stats, seasonality hints, missingness, rate of change)
3. Train anomaly model(s)
4. Score latest data and export:
   - `outputs/anomalies.csv`
   - `outputs/site_summary.csv`
5. Simple visualization/dashboard (optional)

---

## Phase 2 Target (2B, requires clearance)
To **predict advisory risk at a community potable-water system level**, we will need 2B data such as:
- treatment plant monitoring / distribution testing results
- operational logs and compliance results
- system risk ratings, operator capacity indicators

Phase 2 is not possible with public 2A alone without overclaiming.


---

## Repo structure
- `data/`
  - `sample_water.csv` (ISC advisories)
  - `DATA_DICTIONARY.md`
  - `sample_water_quality.csv` (ECCC monitoring)
  - `DATA_DICTIONARY_water_quality.md`
- `src/` (coming next)
  - `ingest/` (load + validate datasets)
  - `features/` (feature engineering)
  - `models/` (baseline models)
  - `reports/` or `dashboard/` (outputs)

---

## Guardrails (so we don’t drift)
- ✅ Always state whether we’re using **2A (proxy)** or **2B (potable system)** data.
- ✅ Never claim we can predict community advisories from 2A alone.
- ✅ Treat 2B as the long-term target required for true advisory prediction.
- ✅ Respect Indigenous data governance and community consent if/when moving to 2B.

---

## Current status
- [x] Dataset 1 added (ISC advisories) + dictionary
- [x] Dataset 2A added (ECCC water quality readings) + dictionary
- [ ] Next: build ingestion + validation scripts
- [ ] Next: baseline anomaly detection / trend model on 2A
- [ ] Later: design 2B schema + integration plan (pending governance/clearance)

---

## Definitions (quick)
- **2A:** Public ambient/surface water monitoring (useful signals, not potable system truth)
- **2B:** Potable water system + operations data (needed for real advisory-risk prediction)
- **Label/Outcome:** advisories (what we want to prevent)
- **Signals/Features:** measurements + operational indicators (what predicts the outcome)

---

## License / data notes
This repo includes **public datasets** and derived samples for development.
If/when 2B is added, it may require separate storage, access control, and governance terms.
