# Day 24 — Real-Time Public Data Spike (ECCC Hydrometric + GRCA)

**Sprint:** Post-Week 5 / opportunistic
**Status:** 🔲 Not Started
**Depends on:** Nothing technical. **Gated:** only start after the technical brief + validation memo are done.
**Timebox:** 2 days max. This is a spike, not a rebuild.

---

## Objective

Kill the demo's biggest credibility gap — "latest data point is Dec 2024" — by adding one **live public data feed** for the Grand River. No community agreement, no OCAP® negotiation, no waiting on Phase 2.

## Why This Matters

1. **The stale-data caveat is the weakest moment in every demo.** "Historical proof pending Phase 2 access" is honest but makes the product feel frozen. One live feed changes the demo from "what your river did" to "what your river is doing."
2. **It proves the architecture claim.** The whole company thesis is "domain-agnostic after the loader stage — each new vertical is a new loader." Right now there is exactly one loader. A second, *live* loader is hard evidence for the claim, in front of both the expert and funders.
3. **It's directly relevant to the hydro expert.** Hydrometric data (flow/level) is their native language. Flow-correlated water quality (rain event → turbidity lag) is a known phenomenon they'll respect seeing in the roadmap.

## Candidate sources (pick ONE for the spike)

| Source | What | Freshness | Notes |
|--------|------|-----------|-------|
| **ECCC hydrometric (Water Office / datamart)** | Water level + discharge, Grand River stations | Near-real-time (~hourly) | National, stable, machine-readable. Likely best pick. |
| **GRCA river data** | Flow, level, precipitation, some quality params across the watershed | Near-real-time | Local authority — bonus relationship angle: GRCA is itself a future partner/buyer. Check terms of use. |
| Ontario PWQMN | Water quality (103 params) | Annual, ~15-month lag | Already integrated — this is the one being complemented |

## Deliverables

- [ ] `ml/src/ingest/hydrometric_loader.py` (or `grca_loader.py`) — second loader, same processed schema
- [ ] Live level/flow shown on the dashboard next to the historical quality analysis (a single "current conditions" strip is enough — do NOT redesign the dashboard)
- [ ] One sentence of framing copy: "Historical quality analysis (PWQMN) + live watershed conditions (ECCC/GRCA)"
- [ ] Note in `docs/TECHNICAL_BRIEF.md` Phase 2 section: flow-correlated anomaly detection as the roadmap item this feed enables

## Acceptance Criteria

- [ ] Second loader produces data in the standard processed schema
- [ ] Dashboard shows a timestamp from *today*, not Dec 2024
- [ ] Demo script updated: the stale-data caveat becomes a strength ("quality data is annual; conditions data is live; Phase 2 fuses them")

## Guardrails

- Timebox hard: 2 days. If a source's API fights back, fall back to the other source or stop.
- Do not attempt live anomaly scoring on the new feed in this spike — display only. Scoring live flow data is a Phase 2 feature with its own validation needs.
- Respect data licences (ECCC open data licence / GRCA terms) and attribute the source on the dashboard.

## Commit Message

```
feat: live hydrometric data loader (second vertical-ready ingest path)
```
