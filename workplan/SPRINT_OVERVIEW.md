# Sprint Overview — Phase 1 MVP

> Water-Intel | Gemini Matrix Consulting  
> Updated: 2026-03-18

---

## Sprint Summary

| Week | Focus | Days | Status |
|------|-------|------|--------|
| **1** | Pipeline + First ML Result | 3–7.5 | 🟡 In Progress (Days 3–5, 7, 7.5 done) |
| **2** | Model + Explanations + Reports | 8–11 | 🔲 Not Started |
| **3** | API + UI + Demo Pack | 12–14 | 🔲 Not Started (web skeleton exists) |
| **4** | Company Website + Traction + Funding | 15–19 | 🔲 Not Started |

---

## Critical Path

The longest dependency chain that determines when the demo is ready:

```
Day 5: build_processed.py
  └──→ Day 7: build_features.py
    └──→ Day 7.5: PWQMN data sourcing (Grand River)
         └──→ Day 8: anomaly_iforest.py
                └──→ Day 9: driver hints
                └──→ Day 10: site_summary.py
                       └──→ Day 11: risk_score.py
                              └──→ Day 12: FastAPI
                                     └──→ Day 13: Dashboard wiring
                                            └──→ Day 14: Demo pack
```

**Day 6 (EDA notebook)** is off the critical path — it can be done anytime after Day 5.

---

## Daily Task Index

| Day | Task | File | Status |
|-----|------|------|--------|
| 1–2 | Repo + environment setup | — | ✅ Done |
| 3 | Folder structure | — | ✅ Done |
| 4 | Ingestion + schema + validation | — | ✅ Done |
| **5** | **Raw → Processed pipeline** | [day05](tasks/done/day05_build_processed.md) | ✅ Done |
| 6 | EDA notebook | [day06](tasks/day06_eda_notebook.md) | 🔲 |
| **7** | **Feature engineering** | [day07](tasks/done/day07_feature_engineering.md) | ✅ Done |
| **7.5** | **Ontario PWQMN data sourcing** | [day07.5](tasks/done/day07_5_data_sourcing.md) | ✅ Done |
| 8 | Anomaly model (IForest) | [day08](tasks/day08_anomaly_model.md) | 🔲 |
| 9 | Driver hints | [day09](tasks/day09_driver_hints.md) | 🔲 |
| 10 | Site summary report | [day10](tasks/day10_site_summary.md) | 🔲 |
| 11 | Risk score (0–100) | [day11](tasks/day11_risk_score.md) | 🔲 |
| 12 | FastAPI backend | [day12](tasks/day12_fastapi.md) | 🔲 |
| 13 | Dashboard wiring | [day13](tasks/day13_dashboard.md) | 🟡 skeleton exists |
| 14 | Demo pack + pilot one-pager | [day14](tasks/day14_demo_pack.md) | 🔲 |
| 15 | Demo video + business doc review | [days15–19](tasks/days15-19_traction.md) | 🔲 |
| 16 | Gemini Matrix company website | [day16](tasks/day16_company_website.md) | 🔲 |
| 17 | HostSigner deploy + domain | [day17](tasks/day17_hostsigner_deploy.md) | 🔲 |
| 18 | Pilot outreach + grant research | [days15–19](tasks/days15-19_traction.md) | 🔲 |
| 19 | Funding application skeleton | [days15–19](tasks/days15-19_traction.md) | 🔲 |

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Feature engineering produces NaN-heavy output | Blocks model | Use `min_periods` in rolling, document NaN handling strategy |
| Anomaly model flags everything or nothing | Demo looks broken | Tune contamination param, validate with EDA |
| ~~ECCC data has too few readings per site~~ | ~~Weak features~~ | RESOLVED: Switched to Ontario PWQMN Grand River data — 8 stations, 103 params, 13K+ rows |
| Data freshness gap (~15 months) | Demo feels dated | Frame as "5-year trend analysis"; 2025 PWQMN data expected late 2026; emphasize the engine, not the data vintage |
| Dashboard can't connect to API | Demo broken | Test CORS early, have mock fallback |
| Scope creep (adding features before demo works) | Never ships | Strict daily plan, scrum discipline, "ship then polish" |
| Company website delays MVP pipeline work | Demo not ready | Website is Week 4 (after pipeline); don't start early |
| HostSigner deployment issues | No public presence | Have Vercel free tier as fallback |

---

## Definition of Done (Phase 1 MVP)

- [ ] CSV → Ingest → Process → Feature → Model → Score → API → Dashboard pipeline works end-to-end
- [ ] 90-second demo without explaining ML internals
- [ ] Risk scores displayed with color-coded labels
- [ ] Anomaly table with "what triggered it" hints
- [ ] Pilot one-pager ready for distribution
- [ ] Company website live on HostSigner with Water-Intel product page
- [ ] Mission statement and business plan finalized
- [ ] Funding strategy documented with target programs
- [ ] All guardrails (2A proxy disclaimer) in place
- [ ] Code committed with meaningful commit messages
- [ ] PROGRESS.md up to date
