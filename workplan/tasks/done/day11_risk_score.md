# Day 11 — Risk Score v1 (0–100)

**Sprint:** Week 2 — Model + Explanations + Reports  
**Status:** ✅ Done
**Depends on:** Day 10 (site summary) ✅
**Blocked by:** —

---

## Objective
Combine anomaly scores and frequency into a single 0–100 risk score with human-readable labels. This is the "headline number" for the dashboard.

## Deliverables

### `ml/src/models/risk_score.py`
- Input: `outputs/anomalies.csv` + `outputs/site_summary.csv`
- Output: Updated `outputs/site_summary.csv` with added columns:
  - `risk_score` (0–100 integer)
  - `risk_label` — `Safe` (0–30), `Watch` (31–60), `Concern` (61–100)
- Logic (v1, simple):
  - Base: latest anomaly_score normalized to 0–60
  - Frequency boost: anomaly_rate in last 30 days scaled to 0–40
  - Combine: `min(base + frequency_boost, 100)`

### Guardrails
- Document in docstring + README: **"This is a 2A proxy score based on public monitoring data. It does not predict ISC drinking water advisories."**
- Labels are decision-support, not compliance certification

## Acceptance Criteria
- [x] Risk scores are deterministic and bounded [0, 100]
- [x] Labels map correctly to score ranges
- [x] Guardrail disclaimer is documented
- [x] Score distribution looks reasonable (not all 0 or all 100)

## Commit Message
```
feat: risk scoring v1 (2A proxy)
```

## Notes
- The scoring formula will evolve — v1 just needs to be "not wrong" for demos
- Operators should see: green/yellow/red with a number they can track over time
