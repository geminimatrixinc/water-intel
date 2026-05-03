# Sprint Overview — Phase 1 MVP

> Water-Intel | Gemini Matrix Consulting  
> Updated: 2026-04-15

---

## Sprint Summary

| Week | Focus | Days | Status |
|------|-------|------|--------|
| **1** | Pipeline + First ML Result | 3–8 | ✅ Complete |
| **2** | Model + Explanations + Reports | 8–11 | ✅ Complete |
| **3** | API + UI + Demo Pack | 12–14 | 🟡 In Progress |
| **4** | Company Website + Traction + Funding | 15–19 | 🔲 Not Started |
| **5** | MCP Agent Architecture | 20–23 | 🔲 Not Started |

## Current Focus Reminder

- The dashboard and detail views are good enough for this phase unless a real bug or credibility issue appears.
- Additional UI polish now has diminishing returns compared with the remaining packaging and traction work.
- Current priority order: Day 14 packaging, Day 16 company site, technical/pilot brief, guided Ask Water-Intel experience, then MCP build.

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
                                                   └──→ Day 20: MCP Server
                                                          └──→ Day 21: MCP Client
                                                                 └──→ Day 22: Water Agent
                                                                        └──→ Day 23: Agent Demo Pack
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
| **8** | **Anomaly model (IForest)** | [day08](tasks/done/day08_anomaly_model.md) | ✅ Done |
| **9** | **Driver hints** | [day09](tasks/done/day09_driver_hints.md) | ✅ Done |
| **10** | **Site summary report** | [day10](tasks/done/day10_site_summary.md) | ✅ Done |
| **11** | **Risk score (0–100)** | [day11](tasks/done/day11_risk_score.md) | ✅ Done |
| 12 | FastAPI backend | [day12](tasks/done/day12_fastapi.md) | ✅ Done |
| 13 | Dashboard wiring | [day13](tasks/done/day13_dashboard.md) | ✅ Done |
| 14 | Demo pack + pilot one-pager | [day14](tasks/day14_demo_pack.md) | 🟡 |
| 15 | Demo video + business doc review | [days15–19](tasks/days15-19_traction.md) | 🔲 |
| 16 | Gemini Matrix company website | [day16](tasks/day16_company_website.md) | 🔲 |
| 17 | HostSigner deploy + domain | [day17](tasks/day17_hostsigner_deploy.md) | 🔲 |
| 18 | Pilot outreach + grant research | [days15–19](tasks/days15-19_traction.md) | 🔲 |
| 19 | Funding application skeleton | [days15–19](tasks/days15-19_traction.md) | 🔲 |
| **20** | **MCP Server (expose tools)** | [day20](tasks/day20_mcp_server.md) | 🔲 |
| **21** | **MCP Client (consume agents)** | [day21](tasks/day21_mcp_client.md) | 🔲 |
| **22** | **Autonomous Water Agent** | [day22](tasks/day22_water_agent.md) | 🔲 |
| **23** | **Agent Demo Pack + Federation** | [day23](tasks/day23_agent_demo_pack.md) | 🔲 |

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Feature engineering produces NaN-heavy output | Blocks model | Use `min_periods` in rolling, document NaN handling strategy |
| Anomaly model flags everything or nothing | Demo looks broken | Tune contamination param, validate with EDA |
| ~~ECCC data has too few readings per site~~ | ~~Weak features~~ | RESOLVED: Switched to Ontario PWQMN Grand River data — 8 stations, 103 params, 13K+ rows |
| Data freshness gap (not real-time) | Demo feels dated | Latest published PWQMN data now reaches Dec 2024; frame clearly as historical trend analysis until Phase 2 SCADA or community sensor access exists |
| Dashboard can't connect to API | Demo broken | Test CORS early, have mock fallback |
| Scope creep (adding features before demo works) | Never ships | Strict daily plan, scrum discipline, "ship then polish" |
| Continuing to polish dashboard/detail after the product story is already clear enough | Slows traction work | Treat dashboard/detail as phase-complete and only reopen for bugs or credibility gaps discovered during packaging |
| Company website delays MVP pipeline work | Demo not ready | Website is Week 4 (after pipeline); don't start early |
| HostSigner deployment issues | No public presence | Have Vercel free tier as fallback |
| MCP SDK breaking changes | Agent work blocked | Pin SDK version; MCP spec is stable as of 2026 |
| LLM API costs for agent reasoning | Budget concern | Use Claude Haiku for dev/demo (~$0.01/query); Sonnet for prod |
| External data sources unavailable as MCP | Can't show mesh | Build simulated external MCP servers for demo (Day 21) |

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
- [ ] MCP server exposes Water-Intel tools for agent integration
- [ ] Water-Intel agent generates daily briefing from multi-source data
- [ ] Agent demo runs in 90 seconds alongside dashboard demo
- [ ] Code committed with meaningful commit messages
- [ ] PROGRESS.md up to date

## Discipline Note

When in doubt, choose forward motion over polish. At this point, the highest-return work is packaging, positioning, website credibility, and the next interaction layer, not more dashboard/detail refinement.
