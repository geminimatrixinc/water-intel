# Day 5 — Raw → Processed Pipeline

**Sprint:** Week 1 — Pipeline + First ML Result  
**Status:** 🔲 Not Started  
**Depends on:** Day 4 (ingestion skeleton) ✅  
**Blocked by:** Nothing

---

## Objective
Take raw ECCC CSV data through the ingestion pipeline and output a clean, normalized, analysis-ready dataset.

## Deliverables

### `ml/src/ingest/build_processed.py`
- Input: `data/sample/sample_water_quality.csv` (dev) or `data/raw/*.csv` (full)
- Output: `data/processed/eccc_processed.csv` (or `.parquet`)
- Schema: Long format — `station_id, timestamp, parameter, value, unit`
- Handle: deduplication, null dropping, timestamp normalization
- CLI-runnable: `python -m src.ingest.build_processed`

### Update `data/sample/DATA_DICTIONARY_water_quality.md`
- Document the processed output schema (columns, types, constraints)

## Acceptance Criteria
- [ ] `data/processed/eccc_processed.csv` is generated and readable
- [ ] Schema matches documented format
- [ ] No duplicate rows in output
- [ ] Timestamps are ISO-8601 formatted
- [ ] Script is idempotent (re-runnable, same output)

## Commit Message
```
feat: build processed eccc dataset
```

## Notes
- Consider Parquet for performance if dataset grows beyond 100MB
- Keep CSV for now (easier to inspect during development)
- Wide format (pivot by parameter) may be needed later for feature engineering — don't pre-optimize
