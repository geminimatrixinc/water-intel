# Day 8 — Baseline Anomaly Model (Isolation Forest)

**Sprint:** Week 2 — Model + Explanations + Reports  
**Status:** 🔲 Not Started  
**Depends on:** Day 7.5 (Grand River data) ✅  
**Blocked by:** Day 7.5

---

## Objective
Train a baseline anomaly detection model using Isolation Forest on the engineered features. Produce an anomaly-scored output for every reading.

## Deliverables

### `ml/src/models/anomaly_iforest.py`
- Input: `data/processed/grand_river_features.csv`
- Output: `outputs/anomalies.csv`
  - Columns: `station_id, timestamp, anomaly_score, is_anomaly`
- Model: `sklearn.ensemble.IsolationForest`
  - contamination = 0.05 (tunable)
  - Global model first (all 8 Grand River sites), per-site optional later
- Deterministic: set `random_state` for reproducibility

### `ml/notebooks/03_anomaly_baseline.ipynb` (optional)
- Visual inspection of anomaly results
- Plot anomaly score over time per site
- Verify flagged anomalies make intuitive sense

## Acceptance Criteria
- [ ] `outputs/anomalies.csv` generated with correct schema
- [ ] Model runs deterministically (same output on re-run)
- [ ] Anomaly rate is reasonable (~3–8% of readings)
- [ ] At least one "makes sense" anomaly visible in data

## Commit Message
```
feat: baseline anomaly detection (isolation forest)
```

## Notes
- Don't over-tune. This is a baseline — the goal is "does the pipeline work end-to-end?"
- Isolation Forest works well for unsupervised anomaly detection on tabular data
- Grand River data has 103 parameters — may want to train on a subset of key parameters (nutrients, metals, pH, turbidity) for cleaner results
- We'll add explanations (Day 9) and risk scoring (Day 11) on top of this
