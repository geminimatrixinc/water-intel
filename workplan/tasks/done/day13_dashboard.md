# Day 13 — React Dashboard v1

**Sprint:** Week 3 — API + UI + Demo Pack  
**Status:** ✅ Complete  
**Depends on:** Day 12 (API) ✅  
**Blocked by:** None

---

## Objective
Wire the existing Next.js app to the FastAPI backend and build the three core views for the 90-second demo.

## Current State
- Next.js 16 app exists in `web/app/`
- Mock data in `web/app/app/lib/mockData.ts` is now deprecated from the production path
- Dashboard page and site detail page now render live data from FastAPI
- API routes proxy to the FastAPI backend instead of returning mock data

## Deliverables

### Wire to real API
- Update `web/app/app/lib/` to fetch from FastAPI (`http://localhost:8000`)
- Remove or deprecate mock data

### Views
1. **Site picker** — list all sites with risk score badge (color-coded)
2. **Risk card** — large score display + label + last updated
3. **Anomaly table** — timestamp, anomaly_score, is_anomaly, top_features
4. **Timeline chart** — anomaly score over time (simple line chart, use recharts or chart.js)
5. **Data mode banner** — persistent label at top of dashboard:
   > `Historical Analysis (2019–2024) · Live monitoring available with SCADA integration`
   - Makes it clear this is real historical data, not a toy demo
   - Plants the seed that the system can go real-time with sensor feeds
   - Key "imagine this live" moment for water plant managers during demo

### Types
- Define TypeScript interfaces matching API response shapes

## Acceptance Criteria
- [x] Dashboard loads and displays data from FastAPI
- [x] Site picker shows all sites with risk labels
- [x] Site detail shows risk score + anomaly history
- [x] No mock data in production path
- [x] Handles API errors gracefully (loading states, error messages)

## Implementation Notes
- Added `web/app/app/lib/types.ts` to model the real FastAPI response shapes.
- Added `recharts` for the anomaly score timeline chart.
- The UI now uses `station_id` as the canonical site identifier and displays `risk_label` values directly (`Safe`, `Watch`, `Concern`).
- The mock data file remains only as a reference and is no longer imported by the active dashboard or proxy routes.

## Commit Message
```
feat: web dashboard v1 (risk + anomalies)
```

## Notes
- The web skeleton is already ahead of schedule — this day is about wiring real data
- Keep styling minimal (Tailwind defaults) — polish later
- Demo needs to work in 90 seconds: click site → see risk → see anomalies