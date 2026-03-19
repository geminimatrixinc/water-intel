# Day 12 — FastAPI Backend

**Sprint:** Week 3 — API + UI + Demo Pack  
**Status:** 🔲 Not Started  
**Depends on:** Day 11 (risk score) 🔲  
**Blocked by:** Day 10 minimum (site summary), Day 11 ideal

---

## Objective
Build a lightweight API that serves the ML pipeline outputs to the React dashboard. No database — reads CSV files directly for Phase 1.

## Deliverables

### `api/main.py` (FastAPI)
- `GET /health` → `{"status": "ok", "version": "0.1.0"}`
- `GET /sites` → reads `outputs/site_summary.csv`, returns JSON array
- `GET /sites/{site_id}` → single site summary + risk score
- `GET /anomalies?site_id=...` → reads `outputs/anomalies.csv`, returns filtered JSON
- `GET /risk/{site_id}` → `{score, label, last_updated}`

### `api/requirements.txt`
- fastapi, uvicorn, pandas

### CORS
- Allow `http://localhost:3000` (Next.js dev server)

## Acceptance Criteria
- [ ] API starts with `uvicorn api.main:app --reload`
- [ ] All 5 endpoints return valid JSON
- [ ] CORS allows frontend to connect
- [ ] Handles missing site_id gracefully (404)

## Commit Message
```
feat: api endpoints for site risk + anomalies
```

## Notes
- No database. Phase 1 reads CSV files directly.
- Future: switch to PostgreSQL or DuckDB when data volume grows
- Keep response shapes consistent — frontend will type these
