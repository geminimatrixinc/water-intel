# Architecture Overview — Water-Intel Phase 1 MVP

> Last updated: 2026-03-19

---

## System Architecture (Phase 1)

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA LAYER                               │
│                                                                 │
│  data/raw/pwqmn_*.csv ──┐                                        │
│  (Ontario PWQMN)       │──→ PWQMNLoader ──→ grand_river_processed.csv│
│                        │   (pwqmn_loader.py)    (normalized)      │
│  data/raw/eccc_*.csv ──┤                                            │
│  (ECCC national)       │──→ ECCCLoader ───→ eccc_processed.csv    │
│                        │   (eccc_loader.py)     (normalized)      │
└──────────────────────┼──────────────────────────────────────────┘
                       │
┌──────────────────────┼──────────────────────────────────────────┐
│                      ▼   ML PIPELINE                            │
│                                                                 │
│  build_features.py ──→ grand_river_features.csv                  │
│        │                     │                                  │
│        ▼                     ▼                                  │
│  anomaly_iforest.py ──→ outputs/anomalies.csv                   │
│        │                     │                                  │
│        ▼                     ▼                                  │
│  risk_score.py ──────→ outputs/site_summary.csv                 │
│  site_summary.py             (risk_score, risk_label)           │
└──────────────────────────────────────────────────────────────────┘
                               │
┌──────────────────────────────┼──────────────────────────────────┐
│                              ▼  API LAYER                       │
│                                                                 │
│  services/api/main.py (FastAPI, canonical)                      │
│  api/main.py (compatibility shim)                               │
│    GET /health                                                  │
│    GET /sites ────────────→ site_summary.csv → JSON             │
│    GET /sites/{id} ───────→ filtered summary → JSON             │
│    GET /anomalies?site= ──→ anomalies.csv → JSON               │
│    GET /risk/{id} ────────→ {score, label, last_updated}        │
│                                                                 │
│  CORS: localhost:3000                                           │
└──────────────────────────────┼──────────────────────────────────┘
                               │
┌──────────────────────────────┼──────────────────────────────────┐
│                              ▼  PRESENTATION LAYER              │
│                                                                 │
│  web/app/ (Next.js + React + Tailwind, transitional exception)  │
│    /dashboard ────────────→ Site picker (list + risk badges)    │
│    /dashboard/sites/{id} ─→ Risk card + anomaly table + chart   │
│                                                                 │
└──────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```
Raw CSV ──→ Ingest ──→ Processed ──→ Features ──→ Model ──→ Scores ──→ API ──→ Dashboard
(PWQMN/     (parse,     (clean,       (rolling,    (IForest)  (0-100,    (JSON)   (React)
 ECCC)      validate)   normalize)    z-score,               labels)
                                      deltas)
```

**Primary demo data:** Ontario PWQMN Grand River watershed (8 stations near Six Nations, 2019–2024)  
**Secondary/reference data:** ECCC Okanagan-Similkameen (4 stations, 2000–2025)

---

## Technology Stack

| Layer | Technology | Rationale |
|-------|-----------|-----------|
| **Data** | CSV/Parquet | Simple, portable, no DB needed for MVP |
| **ML** | Python, pandas, scikit-learn | Industry standard, rapid prototyping |
| **API** | FastAPI + uvicorn | Fast, modern, auto-generates OpenAPI docs |
| **Web** | Next.js 16, React 19, Tailwind 4 | Production-ready, TypeScript, responsive |
| **Charts** | recharts or chart.js | Lightweight, React-native charting |

---

## Key Design Decisions

### 1. No Database (Phase 1)
The API reads CSV files directly. This keeps infrastructure zero and makes outputs easily inspectable. Database (PostgreSQL or DuckDB) comes in Phase 2 when we have streaming data or multi-user access.

### 2. Long Format for Processed Data
`station_id, timestamp, parameter, value, unit` — flexible for any parameter set, easy to pivot when needed for features. Same schema works for both ECCC and PWQMN data sources.

### 3. Isolation Forest for Anomaly Detection
Unsupervised, no labels needed, works well on tabular time-series features. Perfect for Phase 1 where we have no ground truth labels for the ECCC data.

### 4. Risk Score = Composite of Anomaly + Frequency
Simple, transparent formula. Not a black box. Operators can understand why a score is high.

### 5. Guardrails Baked In
Every score output includes the caveat: 2A proxy data, not advisory prediction. This is not optional — it's a design constraint.

---

## Phase 2 Architecture (Future)

```
Phase 2 adds:
  - PostgreSQL/TimescaleDB for time-series storage
  - Streaming ingestion (sensor feeds, scheduled ECCC pulls)
  - Authentication + role-based access (operator vs leadership)
  - Multi-tenant data isolation (community-by-community)
  - Supervised model training (if advisory labels from 2B become available)
  - Map view / watershed visualization
  - CI/CD + Docker deployment
```

## Phase 2.5 Architecture — API to MCP Transition

Phase 1 already produces the structured outputs the future agent layer needs: site summaries,
risk scores, anomalies, and driver hints. The next architectural step is not a pivot away from
the dashboard; it is an additional access layer that exposes the same intelligence to AI agents.

```
HTTP dashboard/app layer        MCP agent layer
-------------------------       -------------------------------
GET /sites                  ->  get_site_summary()
GET /sites/{id}             ->  get_risk_score(site_id)
GET /anomalies?site_id=     ->  get_anomalies(site_id, days)
Derived interpretation      ->  explain_station_risk(site_id)
```

This transition matters because it lets Water-Intel evolve from a visual dashboard into a
queryable intelligence service without rewriting the ML pipeline or changing the Phase 1
guardrails.

## Phase 3 Architecture — MCP Agent Mesh (Vision)

```
Phase 3 adds:
  - Water-Intel MCP Server: expose risk scores, anomalies, site data as MCP tools
  - Water-Intel MCP Client: consume weather, water level, advisory, emergency data from other agents
  - LLM-powered reasoning agent: orchestrates multi-source queries, generates briefings
  - Conversational operator surface: Ask Water-Intel chat and briefing interface grounded in MCP tools
  - Federated agent mesh: community-owned agents share upstream/downstream intel
  - OCAP®-aligned data scoping: tool-level permissions, consent-based sharing
  - Edge deployment option: agents run on-prem for full data sovereignty
```

```
┌──────────────────────────────────────────────────────────────────┐
│                    WATER-INTEL MCP SERVER                         │
│                                                                  │
│  MCP Tools:           MCP Resources:                             │
│    get_risk_score       water://sites                            │
│    get_anomalies        water://advisories                       │
│    get_site_summary     water://data-dictionary                  │
│    get_upstream_alert                                            │
│                                                                  │
│  ← consumes:  Weather MCP | Water Level MCP | Advisory MCP      │
│  → serves:    Community agents | Policy agents | Health agents   │
└──────────────────────────────────────────────────────────────────┘
```

See [Roadmap — Phase 3](../planning/Roadmap.md) for full implementation details.

### Repository Alignment Note
- `services/api/` is now the canonical backend location.
- `api/` remains as a compatibility bridge for existing local commands.
- `web/app/` intentionally stays in place during this transition to avoid breaking the current frontend workflow.

### Ask Water-Intel as the Operator Surface

Ask Water-Intel is the operator-facing surface of the MCP architecture, not a separate product.
Instead of asking a user to inspect multiple charts and infer meaning, the system should answer
plain-language questions such as:

- Which upstream sites changed the most this week?
- Why is this station flagged as Concern?
- What parameter appears to be driving the current risk score?
- Which sites deserve follow-up before the next briefing?

Those answers must be grounded in Water-Intel tools and constrained by the same guardrails as the
dashboard: Phase 1 remains historical 2A proxy analysis using public source-water data, not
advisory prediction or live plant control.

### 6. Dual Data Loaders
The pipeline supports two data sources through separate loaders that output the same normalized schema:
- `eccc_loader.py` — ECCC National Long-term Water Quality Monitoring Data (transboundary rivers)
- `pwqmn_loader.py` — Ontario Provincial (Stream) Water Quality Monitoring Network (inland Ontario rivers)

This design means adding a new provincial/territorial data source only requires a new loader function — the rest of the pipeline (features, model, API, dashboard) is source-agnostic.

### 7. Data Freshness Expectations
PWQMN data is published annually with ~12 month lag. The demo uses 2019–2024 data (latest: Dec 2024). This is framed as trend analysis, not real-time monitoring. Real-time monitoring requires Phase 2 (SCADA integration or community-operated sensors).

---

## Security & Governance Notes

- **No PII** in Phase 1 (all data is public ECCC/PWQMN monitoring)
- **Phase 2 (2B)** will require: data-sharing agreements, OCAP® compliance, community consent
- Data sources: ECCC (Open Government Licence – Canada), PWQMN (Open Government Licence – Ontario)
- API has no authentication in Phase 1 (local demo only)
- Production deployment will require: HTTPS, auth, audit logging
