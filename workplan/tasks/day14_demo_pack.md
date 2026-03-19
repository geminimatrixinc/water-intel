# Day 14 — Demo Pack + Pilot Artifact

**Sprint:** Week 3 — API + UI + Demo Pack  
**Status:** 🔲 Not Started  
**Depends on:** Day 13 (dashboard) 🔲  
**Blocked by:** Day 13

---

## Objective
Package the MVP into a demo-ready artifact: scripted walkthrough, pitch one-pager, and screenshots. This is what we show to funders and pilot partners.

## Deliverables

### `docs/demo_script.md`
- 60–90 second scripted walkthrough
- Steps: start API → open dashboard → pick site → show risk score → show anomaly table → explain "what triggered it"
- Include which commands to run

### `docs/PILOT_ONE_PAGER.md`
- **Problem:** X First Nations communities under long-term drinking water advisories
- **Solution:** Early-warning system that flags water quality deterioration before it becomes a crisis
- **How it works:** AI anomaly detection on water quality monitoring data from the Grand River watershed
- **What we've built:** Working MVP analyzing 5 years of data from 8 stations near Six Nations (2019–2024, 103 water quality parameters)
- **Data note:** Trend analysis using Ontario PWQMN historical data; real-time monitoring is Phase 2
- **What Phase 2 needs:** Access to potable water system data, pilot community, $X funding
- **Ask:** 30-minute call or letter of support

### `docs/screens/` — 3+ screenshots
- Dashboard overview (site list with risk badges)
- Site detail (risk card + anomaly table)
- Anomaly timeline chart

## Acceptance Criteria
- [ ] Demo script works end-to-end without fumbling
- [ ] One-pager fits on one page (printable)
- [ ] Screenshots are current and well-cropped
- [ ] Non-technical person can understand the one-pager

## Commit Message
```
docs: pilot one-pager + demo script + screenshots
```

## Notes
- The one-pager is the most important non-code deliverable in Phase 1
- Test the demo with someone who hasn't seen the project — if they don't "get it" in 90 seconds, simplify
