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

  services/
    api/
      main.py                      # FastAPI (canonical)
      requirements.txt

  api/
    main.py                        # compatibility shim
    requirements.txt               # compatibility include

  ops/
    scripts/
      run-dashboard.ps1            # canonical launcher

  planning/
    Roadmap.md
    SPRINT_OVERVIEW.md
    tasks/

  ai/
    agents/
      water-intel/
    mcp/

  web/
    app/                           # transitional exception while the frontend stays in place
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
- **Day 8:** Isolation Forest anomaly model — `outputs/anomalies.csv` generated with stable anomaly scores
- **Day 9:** Driver hints — anomaly outputs enriched with top contributing features
- **Day 10:** Site summary report — `outputs/site_summary.csv` generated per station
- **Day 11:** Risk scoring — 0–100 risk score + Safe/Watch/Concern labels added to site summaries
- **Day 12:** FastAPI backend — CSV-backed API endpoints implemented and verified locally with CORS
- **Day 13:** Dashboard wiring — Next.js dashboard now renders live site summaries, risk details, anomaly table, and anomaly timeline from FastAPI
- **Planning:** Task breakdown created in `planning/tasks/`, PROGRESS.md started, architecture documented, pilot targets + AI outreach playbook

### Current focus ▶
- **Day 14:** Demo pack + pilot-ready artifacts built around the live dashboard flow

### Task Files
Detailed acceptance criteria for each day: `planning/tasks/`

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
- `services/api/main.py` (FastAPI canonical entrypoint)
  - `api/main.py` remains as a compatibility shim for `uvicorn api.main:app --reload`
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
> Focus: company website, HostSigner deployment, outreach, funding applications, and a public-facing story that makes it obvious Water-Intel is more than a chart portal.

### Day 15 — Record 90-second demo video + finalize business docs
**Deliverable**
- `docs/demo_video_link.md` (or `docs/demo_script.md` + extra screenshots)
- Review and finalize `docs/MISSION_STATEMENT.md` and `docs/BUSINESS_PLAN.md`

### Day 16 — Add product landing page on Gemini Matrix site
**Deliverable**
- Page content draft (copy) for: `/products/water-intelligence`
- Includes: problem, what it does, screenshots, pilot CTA, and an Ask Water-Intel teaser
- Distinguishes Water-Intel from a chart dashboard by emphasizing interpretation, prioritization, and plain-language explanation
- Includes 3–5 example operator questions such as "Why is this site flagged?" or "Which upstream sites changed the most?"
- Includes a concise technical brief / pilot brief link for funders, technical reviewers, and serious partners who want the deeper rationale behind the product

### Day 17 — HostSigner deployment + public demo surface
**Deliverable**
- Deploy company website and Water-Intel product page to a public URL
- Add a guided Ask Water-Intel section or mock conversational demo so a non-technical viewer can tell in under 30 seconds that the product is not just another monitoring portal
- Ensure public copy keeps the 2A proxy / no advisory-prediction guardrail explicit

### Day 18 — Pilot outreach list + soft outreach message
**Deliverable**
- `docs/pilot_targets.md` with:
  - org type
  - contact role (not names required yet)
  - what we’re asking for (30-min call, LOI)
- `docs/outreach_email.md` (template)
- Send to 3 targets (track results in PROGRESS)
- Outreach language positions the dashboard as proof of value and Ask Water-Intel as the future conversational layer

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
- [ ] Water-Intel product page on company site with screenshots/demo and Ask Water-Intel positioning
- [ ] Water-Intel technical brief or pilot brief is available from the product page
- [ ] Mission statement and business plan finalized
- [ ] Clear Phase 2 plan exists (pilot + calibration + governance)
- [ ] Guardrails are explicit (2A proxy, not 2B advisory prediction)
- [ ] At least 1 funding application in progress

---

# Week 5 — MCP Agent Architecture (Build)

> Goal: Transform Water-Intel from a dashboard into an **intelligent, queryable water-risk copilot** that other AI systems can query.  
> This is the technical moat and the showstopper demo for funding conversations.

### Day 20 — MCP Server: Expose Water-Intel as agent tools
**Deliverable**
- `ai/mcp/server/water_intel_server.py` — MCP server wrapping existing API logic
- Tools: `get_risk_score`, `get_anomalies`, `get_site_summary`, `get_site_list`, `get_parameter_trend`
- Resources: `water://sites`, `water://data-dictionary`
- Testable from Claude Desktop or VS Code Copilot

**Commit:** `feat: mcp server — expose water-intel tools for agent integration`

### Day 21 — MCP Client: Consume external data agents
**Deliverable**
- `ai/mcp/clients/weather_client.py` — weather data adapter
- `ai/mcp/clients/water_level_client.py` — hydrometric data adapter
- `ai/mcp/clients/advisory_client.py` — advisory status adapter
- `ai/mcp/server/demo_external_server.py` — simulated external MCP server for demo
- Optional: weather correlation in risk scoring

**Commit:** `feat: mcp clients — weather, water level, advisory data agents`

### Day 22 — Autonomous Water-Intel Agent (LLM + MCP orchestration)
**Deliverable**
- `ai/agents/water-intel/workflows/water_agent.py` — LLM agent that reasons across MCP sources
- `ai/agents/water-intel/workflows/briefing_generator.py` — automated daily water briefings
- Agent answers: "What's happening on the Grand River today?"
- Minimal Ask Water-Intel conversational interface for guided operator-style Q&A
- Generates operator-friendly briefings with recommendations

**Commit:** `feat: autonomous water-intel agent with LLM reasoning + daily briefings`

### Day 23 — Agent demo pack + federated architecture design
**Deliverable**
- `docs/agent_demo_script.md` — 90-second agent demo
- `docs/FEDERATED_AGENT_DESIGN.md` — community agent mesh architecture + OCAP® alignment
- Updated business plan + funding strategy with Phase 3 agent vision
- Screenshots in `docs/screens/`

**Commit:** `docs: mcp agent demo pack + federated architecture design`

---

# Exit Criteria (Full — including Week 5)

We are "fully demo-ready" when all are true:
- [ ] 90-second dashboard demo works without explaining ML
- [ ] 90-second agent demo shows multi-source reasoning
- [ ] Dashboard shows real anomaly/risk output
- [ ] MCP server queryable by external AI agents
- [ ] Water-Intel agent generates daily briefings
- [ ] Company website live with Water-Intel product page
- [ ] Federated agent architecture documented for Phase 3D
- [ ] Business plan + funding strategy include agent platform vision
- [ ] Guardrails are explicit (2A proxy, not 2B advisory prediction)
- [ ] At least 1 funding application in progress

---

# Phase 3 — MCP Agent Architecture (Vision)

> **Concept:** Build Water-Intel as an autonomous AI agent that exposes its intelligence via the **Model Context Protocol (MCP)** — enabling other agents, platforms, and tools to query water safety data in real time through a standardized protocol.

## What is MCP?
The **Model Context Protocol** is an open standard (originated by Anthropic, adopted by OpenAI, Google, and others) that lets AI agents expose tools, resources, and data to other AI systems. Think of it as an API — but designed for agent-to-agent communication rather than human-to-machine.

## The Vision: Water-Intel as an MCP Server

```
┌─────────────────────────────────────────────────────────────────────┐
│                    WATER-INTEL MCP SERVER                            │
│                                                                     │
│  Tools exposed via MCP:                                             │
│    get_risk_score(site_id)        → current risk 0-100 + label      │
│    get_anomalies(site_id, days)   → recent anomalies + drivers      │
│    get_site_summary(region)       → all sites with status           │
│    get_water_advisory_status()    → active advisories in coverage   │
│    get_upstream_alert(site_id)    → "what's coming" early warning   │
│    get_parameter_trend(site, param, window) → time-series trend     │
│                                                                     │
│  Resources exposed via MCP:                                         │
│    water://sites                  → list of monitored stations      │
│    water://advisories             → current advisory map            │
│    water://data-dictionary        → parameter definitions + units   │
│                                                                     │
└──────────────────────┬──────────────────────────────────────────────┘
                       │ MCP protocol (JSON-RPC over stdio/SSE)
         ┌─────────────┼─────────────────────────────┐
         │             │                             │
         ▼             ▼                             ▼
┌─────────────┐ ┌──────────────┐ ┌───────────────────────────┐
│ Community    │ │ Government   │ │ Other AI Agents            │
│ Health Agent │ │ Policy Agent │ │ (weather, infrastructure,  │
│              │ │              │ │  public health, emergency) │
│ "Is the     │ │ "Which       │ │                            │
│ water safe  │ │ communities  │ │ "Cross-reference water     │
│ at Site X?" │ │ need urgent  │ │ risk with weather forecast │
│              │ │ funding?"    │ │ and infrastructure age"    │
└─────────────┘ └──────────────┘ └───────────────────────────┘
```

## Water-Intel Also Consumes Other MCP Servers

```
Water-Intel Agent (MCP Client)
    │
    ├── → Environment Canada Weather MCP → real-time precip, temp, flood alerts
    ├── → ECCC Water Level MCP           → hydrometric station discharge data
    ├── → ISC Advisory Registry MCP      → live advisory status per community
    ├── → Provincial Sensor Networks MCP → SCADA / IoT sensor feeds
    ├── → Emergency Management MCP       → wildfires, floods, spills upstream
    └── → Infrastructure DB MCP          → plant age, capacity, last inspection
```

**The result:** Water-Intel becomes a node in a **mesh of specialized agents** that can cross-reference environmental, infrastructure, and health data in real time — far beyond what any single data source provides.

## Use Cases

| Scenario | Agent Interaction |
|----------|------------------|
| **Operator morning check** | Community health agent asks Water-Intel MCP for overnight risk changes |
| **Flood response** | Emergency agent detects flood warning → queries Water-Intel for downstream site risk |
| **Funding prioritization** | Government policy agent queries Water-Intel for communities with chronic high-risk scores |
| **Wildfire smoke impact** | Weather agent detects smoke event → Water-Intel correlates with turbidity/pH spikes |
| **Cross-community alerting** | Water-Intel detects upstream anomaly → notifies downstream community agents |
| **Grant reporting** | Funding agent pulls Water-Intel trend data to auto-generate impact reports |

## Implementation Roadmap

### Phase 3A — Water-Intel MCP Server (expose our data)
- Wrap existing FastAPI endpoints as MCP tools
- Define MCP resource URIs for sites, advisories, data dictionary
- Implement stdio transport (for local agent integration) + SSE transport (for remote)
- Auth layer: API keys for agent-to-agent, OCAP®-compliant data scoping per community

### Phase 3B — Water-Intel MCP Client (consume other agents)
- Build MCP client adapters for Environment Canada weather + water level data
- Integrate real-time hydrometric data into risk scoring
- Weather-correlated anomaly detection (rain events → turbidity lag prediction)

### Phase 3C — Autonomous Water-Intel Agent
- LLM-powered reasoning layer that orchestrates across multiple MCP sources
- Natural language interface: "What's the water situation on the Grand River today?"
- Automated daily briefings generated by the agent, pushed to operators
- Proactive alerting: agent detects converging risk factors across sources without being asked

### Phase 3D — Agent Mesh / Federation
- Multiple community Water-Intel agents sharing upstream/downstream intelligence
- Federated model: each community owns their agent + data, shares only what they consent to
- National intelligence layer aggregates anonymized risk patterns for policy & advocacy

## Technical Stack (Phase 3)

| Component | Technology | Notes |
|-----------|-----------|-------|
| MCP Server | Python `mcp` SDK | Wraps existing pipeline |
| MCP Client | Python `mcp` SDK | Connects to external data agents |
| Agent Reasoning | LLM (Claude / GPT) with tool use | Orchestrates MCP tool calls |
| Transport | stdio (local) + SSE (remote) | Standard MCP transports |
| Auth | OAuth 2.0 + OCAP® scoping | Community-consented data access |
| Deployment | Docker + edge (community-local option) | Data sovereignty: agent runs on-prem if needed |

## Data Sovereignty Alignment
MCP is uniquely suited for Indigenous data governance:
- **Each community runs their own agent** — data never leaves their infrastructure unless they choose to share
- **Tool-level permissions** — a community can expose `get_risk_score` while keeping raw data private
- **Federated, not centralized** — no single entity controls the network
- **Consent-based sharing** — agents only respond to queries from authorized peers

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
