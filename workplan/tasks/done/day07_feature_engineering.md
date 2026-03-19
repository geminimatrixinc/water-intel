# Day 7 — Feature Engineering v1

**Sprint:** Week 1 — Pipeline + First ML Result  
**Status:** 🔲 Not Started  
**Depends on:** Day 5 (processed data) 🔲  
**Blocked by:** Day 5

---

## Objective
Transform the processed time-series into ML-ready features: rolling statistics, deltas, and missingness indicators that capture "is something changing abnormally?"

## Deliverables

### `ml/src/features/build_features.py`
- Input: `data/processed/eccc_processed.csv`
- Output: `data/processed/eccc_features.csv`
- Feature groups:
  - **Rolling statistics:** mean, std over 7/14/30-reading windows (per station + parameter)
  - **Delta features:** value change from previous reading
  - **Rate of change:** delta / time gap (handles irregular sampling)
  - **Missingness flags:** is_gap (boolean if > expected gap between readings)
  - **Z-score:** how many standard deviations from rolling mean

### Feature documentation
- Column names and descriptions appended to `data/sample/DATA_DICTIONARY_water_quality.md`

## Acceptance Criteria
- [ ] Feature CSV generated without NaN explosions (handle edge cases)
- [ ] Rolling windows handle insufficient data gracefully (NaN or skip)
- [ ] Feature columns are documented
- [ ] Script is idempotent and CLI-runnable

## Commit Message
```
feat: feature engineering v1 (rolling + deltas)
```

## Design Decisions
- **Wide format required:** Pivot processed data to wide (one column per parameter) before computing cross-parameter features
- **Window = reading count** (not calendar days) since sampling is irregular
- **NaN strategy:** Allow NaN from rolling (model handles it or we impute later)

## Notes
- This is the most important step for model quality — clean features > complex models
- Keep it simple: rolling stats + z-score will catch most anomalies in Isolation Forest
