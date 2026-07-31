# GATE — Credentials & Funding (index)

**Sprint:** Week 4.5 → Week 5 — Convert capability into access and revenue
**Status:** 🟡 In Progress (Track A started, post-meeting expert prep in motion)
**Position:** Ongoing, runs alongside technical roadmap

> **Updated 2026-07-14:** McMaster meeting completed June 25. Colin **mentioned** wanting to involve an external hydrological expert — but no meeting was ever scheduled, and the thread has been silent since. Treat the expert review as an *unconfirmed intention*, not a pending event: the technical brief is the artifact that can make it real (send it ~July 17–18). Do not event-gate other work on a meeting that may never materialize — use the date-based fallback (~Aug 4) instead.

---

## The Honest Timeline

Reference, grants, and first revenue realistically take **~12 months.** That's fine — but it means:

1. **Don't wait on the slow stuff to do the fast stuff.** Split the work into two tracks.
2. **Don't freeze all building for a year either.** Build the *cheap* technical asset that strengthens the funding pitch (lean MCP server); defer only the *expensive* speculative work (federated agent mesh) until a funded pilot asks.

---

## Track A — DO NOW (solo, no references, weeks not months)

| Task file | Outcome | Status |
|-----------|---------|--------|
| [day19_government_ai_support.md](day19_government_ai_support.md) | NRC IRAP call OR find alternative government AI support · **SR&ED** log started | 🟡 In progress (IRAP ineligible — T4 employee required; pivot to Innovations Canada) |
| [day19a_gate_credentials.md](day19a_gate_credentials.md) | IBD verified (correct AI/ML NAICS) + CCAB submitted | 🔲 Not started |
| [day19e_two_rivers_local_network.md](day19e_two_rivers_local_network.md) | Two Rivers CDC meeting held (AEP fit answered + local network opened) | 🔲 Not started (added 2026-07-15) |
| [day19f_ai_source_list.md](day19f_ai_source_list.md) | Government of Canada AI Source List — ITQ document pulled + go/no-go decided | 🔲 Not started (added 2026-07-22). Real deadline **30 Sep 2026** — pull the actual mandatory-criteria doc by mid-Aug, don't let it crowd out Colin/Two Rivers/deploy now |

**Update on Track A — Government AI Support (revised 2026-07-14):**
- ❌ **NRC IRAP:** Does not qualify (requires 1 full-time T4 employee; solo founder ineligible)
- 🟡 **Innovations Canada:** Challenge-based procurement, not an open grant — you respond to posted challenges, you don't apply with your own project. Posture = monitor the challenge portal monthly; Testing Stream (prototype purchase) is the likelier fit. See corrected plan in [day19_government_ai_support.md](day19_government_ai_support.md).
- 🟡 **SR&ED:** Only refunds expenditures actually incurred (T4 salaries, contractor invoices). Unpaid founder labour is NOT claimable — if nothing has been paid out, the retroactive claim is ~$0. Keep the log anyway (valuable post-funding + doubles as technical evidence), but do not budget SR&ED money until an accountant confirms eligible expenditures.
- **Next:** Portal monitor set up + SR&ED log started with corrected expectations

---

## Post-Meeting Actions (NEW — immediate)

| Task file | Outcome | Timing |
|-----------|---------|--------|
| [day19_5_post_meeting_expert_prep.md](day19_5_post_meeting_expert_prep.md) | Thank-you emails sent · technical brief drafted (~2–3 pages) · ready for hydro expert review | This week (June 25–July 2) |

Colin is coordinating a meeting with an external hydrological expert (timeframe: ~3–4 weeks). Your job: prepare materials so the expert has everything needed to validate the approach.

---

## Track B — ~12-month (relationship-dependent, work in order)

| Step | Task file | Outcome | Status |
|------|-----------|---------|--------|
| **1** | [day19b_gate_reference.md](day19b_gate_reference.md) | One letter of support **or** pilot/co-applicant commitment | 🟡 In progress (Colin/Sara/intern meeting happened; expert meeting pending) |
| **2** | [day19c_gate_funding.md](day19c_gate_funding.md) | Mitacs / freshwater grant application in active progress | 🔲 Hold until expert review happens **or ~Aug 4**, whichever comes first — then pursue Mitacs via Jawed/Li directly (don't wait forever on an unscheduled meeting) |
| **3** | [day19d_gate_first_revenue.md](day19d_gate_first_revenue.md) | One sole-source / set-aside conversation open | 🔲 Hold until credentials + reference established |

> For focus, complete and check off one file at a time.

---

## How this gates the technical roadmap (relaxed)

- **Day 20a (event cross-reference / validation annex)** — ✅ **DONE, moved to [done/day20a_event_crossref.md](done/day20a_event_crossref.md)** (2026-07-21). Cross-referenced top anomalies against Ontario spill records + Grand River flow data; found a watershed-wide Jan 2020 storm event corroborated at 5 stations and a probable field-instrument fault at York. Powers the ~Aug 5 Colin follow-up.
- **Day 20b (UX: timeline markers + list-view corroboration chip)** — ✅ **DONE, moved to [done/day20b_ux_improvements.md](done/day20b_ux_improvements.md)** (2026-07-21). The live dashboard now visibly shows event corroboration, not just the PDF brief — worth mentioning in the Colin follow-up.
- **Day 20 (lean MCP server)** — OK to build once **Track A is in motion.** It's low-lift (wraps existing endpoints) and *strengthens* the IRAP/Mitacs pitch ("network-ready AI infrastructure").
- **Days 21–23 (MCP clients, autonomous agent, federated mesh)** — **deferred until a funded pilot or buyer asks for it.** This is the expensive, speculative work.

---

## Key Meeting Outcomes (June 25)

- ✅ Colin Gibson, Sara Smith (Six Nations Environment), McMaster intern met with Mike
- ✅ Interest confirmed in **water-quality POC** with real data
- ✅ **Separate vertical discovered:** drone/abandoned-oil-well infrastructure (reuses same architecture)
- ✅ **Expert validation planned:** Colin booking meeting with hydrological modelling expert from another university
- ✅ **Timeline:** "These things take time, more than 6 months" — permission to be patient

---

## Cross-project: Grant-Intel (added 2026-07-15)

The second Gemini Matrix product (C:\_GIT\grant-intel — see its `ARCHITECTURE.md` + `WORKPLAN.md`)
automates the opportunity-discovery side of this gate:

- **Grant-intel Days 3–12** build an automated CanadaBuys ingest + match pipeline for Gemini
  Matrix's own profile → replaces the manual saved-search setup in
  `indigenous-track/phase2_weeks05-08_pipeline.md` Week 5 (keep the 45-min CanadaBuys email
  alerts as a stopgap until then).
- **Grant-intel Day 10** (dogfood match run) feeds **day19d's** sole-source/set-aside shortlist
  with real, ranked opportunities instead of hand-hunting.
- Programs discovered through 19e / outreach flow back into grant-intel's curated directory.

One system: credentials (19a) open the doors, Two Rivers (19e) opens the local network,
grant-intel finds what's behind the doors, day19d walks through them.

## Notes
- These are business tasks, not engineering tasks. The product is good enough to pitch.
- IRAP + SR&ED are non-dilutive and reward the work itself — pull them first.
- Keep it part-time and patient. Go full-time only at **2–3 signed contracts or one major multi-year grant / standing offer** — not before.
- Reference: `planning/indigenous-track/00_strategy_overview.md` (the procurement thesis + source URLs).
