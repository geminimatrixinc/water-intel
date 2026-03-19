# Day 10 — Site Summary Report

**Sprint:** Week 2 — Model + Explanations + Reports  
**Status:** 🔲 Not Started  
**Depends on:** Day 8 (anomaly model) 🔲  
**Blocked by:** Day 8

---

## Objective
Generate a per-site summary that powers the dashboard site picker and overview cards.

## Deliverables

### `ml/src/reports/site_summary.py`
- Input: `outputs/anomalies.csv` + `data/processed/grand_river_features.csv`
- Output: `outputs/site_summary.csv`
  - Columns: `station_id, total_readings, anomaly_count, anomaly_rate, last_reading_date, last_anomaly_date, avg_anomaly_score`
- Optional: rolling 30-day anomaly count for trend

## Acceptance Criteria
- [ ] `outputs/site_summary.csv` generated with correct schema
- [ ] One row per station
- [ ] Ready for API consumption (clean column names, no NaN in key fields)

## Commit Message
```
feat: site summary report output
```

## Notes
- This is the data that populates the dashboard site list and risk cards
- Keep it flat and simple — the API will serve this as-is
