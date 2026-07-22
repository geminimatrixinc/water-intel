# Day 19.5 — POST-MEETING: Prepare for Hydrological Expert Review

**Sprint:** Week 4.5 → Week 5 (between meeting outcome and expert meeting)
**Status:** 🟢 Substantially complete (2026-07-14) — thank-you emails ✅, IRAP call ✅ (ineligible), **technical brief written and PDF sent to Colin July 14** (reply in the "Thanks for the meeting" thread, brief attached). Remaining: validation annex (optional second touchpoint) + expert-meeting readiness items below. Fallback unchanged: if silent to ~Aug 4, lift the hold on parallel McMaster outreach.
**Depends on:** 2026-06-25 McMaster meeting completed
**Next:** Hydrological expert review — **mentioned by Colin on June 25, never scheduled.** The brief is the catalyst that makes it happen, not prep for a booked meeting.
**Blocks:** Nothing — runs in parallel with credentials/funding work

---

## Context

Colin Gibson (McMaster/Ohneganos) said they want to book another meeting with a **hydrological modelling expert from another university** to validate the technical approach. This person will review the methodology before the next discussion. Your job this week: prepare materials and be ready to support that review.

---

## Immediate Actions (This Week — June 25–June 30)

- [ ] **Send thank-you email to Colin, Sara, and intern**
  - Same day or next business day (June 25–26)
  - Recap water-quality POC interest, mention drone/oil-well vertical, acknowledge hydrological expert next step
  - Offer: *"I'm happy to send a technical brief on the anomaly detection methodology if that would help the expert review"*
  - Keep it brief (under 150 words), warm, no heavy asks
  - CC the intern — they were in the room and relationships compound

- [ ] **Call NRC IRAP this week** (1-877-994-4727)
  - Your pitch now has real context: "in active technical discussions with McMaster Ohneganos on water-quality POC and hydrological validation; exploring adjacent verticals (environmental monitoring for abandoned infrastructure)"
  - This gives you "in active IRAP discussions" credibility for the expert call

- [ ] **Start SR&ED technical log** (parallel, independent)
  - Retroactive clock is ticking — work on Water-Intel ML pipeline is claimable
  - Three columns: date, what you tried, why it was technically uncertain
  - This is evidence for a future claim, not urgent but time-sensitive

---

## Technical Brief (June 26 – July 2)

Write a **2–3 page technical brief** on the anomaly detection methodology. Purpose: give the hydro expert enough context to understand the approach before Colin's next meeting.

**Section 1 — Problem & Philosophy (0.5 page)**
- Why you chose unsupervised anomaly detection (no labels available, real-world water systems need to detect the unknown)
- Why Isolation Forest specifically (multivariate, fast, interpretable contamination assumption)

**Section 2 — The Pipeline (0.75 page)**
- Data input: time-series water quality readings (station ID, timestamp, parameter, value)
- Feature engineering: rolling stats (7/14/30 windows), delta, rate-of-change, z-score, gap flags
- Model: per-station, per-parameter Isolation Forest (contamination=5%, n_estimators=200)
- Output: anomaly score (0–1) per reading, risk score (0–100) per station

**Section 3 — Why This Works for Water (0.75 page)**
- Water systems have stable baselines (rolling statistics capture local "normal")
- Multi-parameter view catches patterns single-threshold systems miss
- Driver hints (z-score ranking of contributing features) give operators actionable signals
- Unsupervised approach works with any water system's data format (no retraining needed per site)

**Section 4 — Honest Limitations (0.5 page)**
- Phase 1 is historical-only (annual data, 15 months stale)
- Model learns from numbers, not Indigenous knowledge or local context
- Requires Phase 2 validation: does it catch real events? False-positive rate acceptable?
- GIS/spatial context not yet in pipeline (Phase 2 roadmap item)

**Section 5 — Next Steps (Phase 2) (0.25 page)**
- Real-time sensor data integration
- Supervised model training (if they can label historical events: "was an advisory issued?")
- Seasonal / climate context (correlate with weather, water level, treatment changes)

---

## Prepare for Expert Meeting (3–4 weeks out)

While waiting for Colin to coordinate the expert meeting, have these ready:

- [ ] Live dashboard up and accessible (already is — water.geminimatrixinc.com/dashboard)
- [ ] Jupyter notebook (`04_driver_hints_explorer.ipynb`) ready to share if the expert wants to see model details
- [ ] One concrete water quality event from Grand River data where the model flagged something real (have a story ready: "In [date], the model flagged conductivity at [station], which later correlated with [known event]")
- [ ] **Mini validation memo (upgrade, added 2026-07-14):** cross-reference the top 5–10 highest-scoring anomalies against public records (GRCA flood/flow bulletins, Ontario spills registry, news archives) and produce a half-page table — date · station · what was flagged · corroborating record (or "none found"). Attach as an annex to the brief. This converts "false-positive rate unknown" into "preliminary event correlation, formal validation in Phase 2" — the single strongest thing you can put in front of a hydrological expert.
  → **Promoted to a full task, completed and moved to done 2026-07-21: [done/day20a_event_crossref.md](done/day20a_event_crossref.md)** — annex built, revised, and shipped (9 High / 4 Possible / 12 None matches, brief Section 6 v2). UI polish (timeline event markers + list-view corroboration chip) also completed and verified 2026-07-21: [done/day20b_ux_improvements.md](done/day20b_ux_improvements.md). The Colin ~Aug 5 follow-up email is still open — see this file's own follow-up checklist below, and now mention the dashboard is live-updated with the corroboration chips/markers, not just the PDF.
- [ ] Data dictionary (parameter definitions, units, ranges) for the 103 PWQMN parameters
- [ ] Honest answers to likely expert questions:
  - *"Why not ARIMA / Prophet for forecasting?"* → We're doing anomaly detection (classification), not time-series forecasting. Unsupervised because we don't have labeled events to train on.
  - *"How do you handle seasonality?"* → Rolling windows are local baselines — if summer always has higher conductivity, that becomes the "normal," and we flag deviations from that local normal, not a global threshold.
  - *"What's your false-positive rate?"* → Unknown without labeled validation data. That's Phase 2 work — we need to test against known events (advisories, spill reports, treatment changes).
  - *"Why 5% contamination assumption?"* → Configurable. We picked 5% as a reasonable starting point for water systems. The expert may recommend different based on their domain knowledge.

---

## Acceptance Criteria (Week done when…)

- [ ] Thank-you email sent to Colin, Sara, and intern
- [ ] NRC IRAP call completed (or call booked)
- [ ] SR&ED technical log started
- [ ] Technical brief drafted (2–3 pages, covers sections 1–5 above)
- [ ] Ready for expert call (dashboard, notebook, data dictionary, honest Q&A ready)

---

## Notes

- The technical brief is written for a **hydrogeologist/hydrological modeller**, not a software engineer. Emphasize water domain knowledge, not implementation details.
- This is not a sales document — it's evidence of rigor. It should make the expert *more* confident, not less, because you're honest about limitations.
- If Colin asks for the brief before sending it, send within 24 hours. First impressions matter.
- Reference: `planning/indigenous-track/meeting_prep_six_nations.md` (detailed technical bridge).
