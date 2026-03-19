# Day 9 — Anomaly Driver Hints

**Sprint:** Week 2 — Model + Explanations + Reports  
**Status:** 🔲 Not Started  
**Depends on:** Day 8 (anomaly model) 🔲  
**Blocked by:** Day 8

---

## Objective
For each flagged anomaly, identify which features contributed most ("what triggered it?"). This is critical for operator trust — a black-box anomaly flag is useless without context.

## Deliverables

### Update `ml/src/models/anomaly_iforest.py` (or new module)
- For each anomaly, compute per-feature z-scores
- Rank features by |z-score| and report top 3
- Update `outputs/anomalies.csv` with:
  - `top_features` — comma-separated list (e.g., "turbidity_zscore, ecoli_delta, ph_rolling_std")
  - `top_feature_values` — corresponding values

## Acceptance Criteria
- [ ] Top 3 driver features per anomaly populated
- [ ] Driver hints look reasonable (not random noise)
- [ ] Output is human-readable (operator can glance and understand)

## Commit Message
```
feat: anomaly driver hints (top contributing features)
```

## Notes
- z-score approach is simple and transparent — no SHAP needed yet
- This output feeds directly into the dashboard "What triggered this?" panel
- Future: add SHAP if we move to supervised models in Phase 2
