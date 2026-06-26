# Water-Intel

An AI-powered early-warning platform for drinking water safety in Indigenous communities across Canada.

**[Live Dashboard](https://water.geminimatrixinc.com/dashboard)** · **[Repo](https://github.com/geminimatrixinc/water-intel)**

---

## What it does

Canada has a persistent drinking water crisis in First Nations communities. Boil Water Advisories (BWAs) can last years — sometimes decades — because deteriorating water quality isn't caught early enough to act on.

Water-Intel ingests public watershed monitoring data, runs an ML anomaly detection pipeline against it, and surfaces site-level risk scores and recommended actions through an interactive dashboard. The goal is to give decision-makers an early signal — before a contamination event becomes a long-term advisory.

The live dashboard currently monitors 8 sites across Ontario watersheds, scoring each site on a 0–100 risk scale with parameter-level driver analysis explaining what's driving the score.

---

## Live demo

**[water.geminimatrixinc.com/dashboard](https://water.geminimatrixinc.com/dashboard)**

Sites are scored and ranked by risk level. Each site shows:
- Risk score (0–100) with Safe / Watch / Concern classification
- Top contributing parameter (e.g. iron, strontium, conductivity)
- Anomaly count and last updated timestamp
- Plain-language recommended action

---

## Tech stack

| Layer | Stack |
|---|---|
| Frontend | Next.js 14, React, TypeScript, Tailwind CSS |
| Backend API | FastAPI (Python), served via `services/api/` |
| ML Pipeline | Python, scikit-learn, pandas, NumPy |
| Data | ECCC National Long-term Water Quality Monitoring (public) |
| AI layer | Agent scaffolding, MCP structure, prompt/eval organization (`ai/`) |
| Deployment | Cloud-hosted, CI/CD via GitHub Actions |

---

## How the ML pipeline works

The core problem is anomaly detection on multivariate environmental time-series data — not a clean or well-behaved dataset.

**Pipeline stages:**

1. **Ingest + normalize** — Raw ECCC files across hundreds of monitoring stations are ingested, validated, and merged into a consistent schema. Missing data, irregular sampling intervals, and unit inconsistencies are handled at this stage.

2. **Feature engineering** — Rolling statistics (mean, std, rate of change), seasonality indicators, missingness flags, and parameter interaction features are built per site per parameter.

3. **Anomaly detection** — Trained per-site models flag statistical outliers against historical baselines. The model outputs an `anomaly_score`, `is_anomaly` boolean, and `top_contributing_features` — the specific parameters that drove the flag.

4. **Risk scoring** — Site-level risk scores (0–100) aggregate anomaly history, severity, recency, and parameter weighting into a single interpretable signal.

5. **Driver analysis** — Each score is traceable. The dashboard shows which parameter triggered the score and why, so operators can act on the result rather than just react to a number.

**Why driver analysis matters:** An anomaly score without explainability is unshippable in a high-stakes context. A risk score of 76 on a water safety platform needs to mean something actionable, not just "the model flagged this."

---

## Architecture

The repo is organized as a product surface plus an AI control plane:

```
water-intel/
├── web/app/          # Next.js dashboard (frontend)
├── services/api/     # FastAPI backend (canonical service layer)
├── ml/               # Ingestion, feature engineering, models, scoring
├── data/             # Public datasets + dictionaries
├── outputs/          # Generated anomaly and site-summary artifacts
├── ai/
│   ├── agents/       # Agent prompts, skills, hooks, evals, workflows
│   └── mcp/          # MCP server/client scaffolding (Phase 2)
├── packages/contracts/ # Shared type contracts across surfaces
├── planning/         # Roadmap, sprint plans, task breakdowns
└── ops/scripts/      # Operational scripts and deployment tooling
```

The AI control plane (`ai/`, `packages/contracts`) is structured so agent logic and MCP integrations can grow independently of the application code — a pattern borrowed from micro-frontend architecture applied to agentic systems.

---

## Data sources and honesty

**What's powering Phase 1:**
Public ambient surface water monitoring from Environment and Climate Change Canada (ECCC) — watershed readings at rivers and lakes near communities. This is proxy data, not potable system data.

**What Phase 1 can and cannot claim:**
The current model detects deterioration signals in source water. It does not predict drinking water advisories at the community system level — that requires data collected inside the treatment plant and distribution system (Phase 2).

This distinction is built into the architecture. The platform is honest about what it sees.

**Phase 2 (requires governance clearance):**
True advisory-risk prediction requires potable water system data: treatment plant telemetry, distribution testing results, operational logs, compliance outcomes, and system risk ratings. Access requires data-sharing agreements and Indigenous data governance review under OCAP® principles. The repo is structured to plug this in without refactoring the existing pipeline.

---

## Current status

- [x] Public dataset ingestion (ISC advisories + ECCC water quality)
- [x] ML anomaly detection pipeline (per-site, per-parameter)
- [x] Risk scoring with driver analysis
- [x] Live dashboard deployed with 8 monitored sites
- [x] Agent scaffolding and MCP structure in place
- [ ] Phase 2: potable system data integration (pending governance/clearance)
- [ ] Phase 2: SCADA / real-time telemetry ingestion

---

## Built by

[Mike Denton](https://github.com/geminimatrixinc) — Senior Full Stack / AI Engineer  
Gemini Matrix Consulting Inc. · Ohsweken, ON, Canada

ML Practitioner Certificate — University of Waterloo (Supervised Learning, Unsupervised Learning, Neural Networks)
