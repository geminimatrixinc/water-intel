# Day 8 — Baseline Anomaly Model (Isolation Forest)

**Sprint:** Week 2 — Model + Explanations + Reports  
**Status:** ✅ Done  
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
- [x] `outputs/anomalies.csv` generated with correct schema (13,194 rows, 660 anomalies)
- [x] Model runs deterministically (same output on re-run) — MD5 verified
- [x] Anomaly rate is reasonable: 5.0% overall (2.0%–8.1% per station)
- [x] At least one "makes sense" anomaly: CONDUCTIVITY 10,900 µS/cm at York station

## Commit Message
```
feat: baseline anomaly detection (isolation forest)
```

## Notes
- Don't over-tune. This is a baseline — the goal is "does the pipeline work end-to-end?"
- Isolation Forest works well for unsupervised anomaly detection on tabular data
- Grand River data has 103 parameters — may want to train on a subset of key parameters (nutrients, metals, pH, turbidity) for cleaner results
- We'll add explanations (Day 9) and risk scoring (Day 11) on top of this
