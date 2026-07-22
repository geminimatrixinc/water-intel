# Day 25 — Second Vertical Spike (Abandoned Wells / Infrastructure Monitoring)

**Sprint:** Opportunistic / backlog
**Status:** 🔲 Not Started
**Depends on:** Technical brief done + expert meeting held.
**Gated:** Only start if (a) Colin's drone/oil-well thread stays warm, or (b) a funder asks for proof the engine is reusable. Do not start speculatively.
**Timebox:** 2–3 days max.

---

## Objective

Prove the "same engine, new loader" claim with a **second vertical on public data** — abandoned/orphan well infrastructure, the exact vertical Colin's team already works in.

## Why This Matters

- The company thesis (fundable *company*, not contractable *project*) rests on the engine being domain-agnostic. Today that's a slide claim. One afternoon of a working second vertical makes it a demo claim.
- Colin's team *has this problem right now* (drone surveys for abandoned oil wells). Showing up to a future conversation with their own vertical running on the engine is the strongest possible "we should work together" move.

## Candidate public datasets

- [ ] Alberta Energy Regulator ST37 (well status list — all wells in Alberta, public)
- [ ] Orphan Well Association inventory (Alberta)
- [ ] Ontario abandoned-well records (MNDM/petroleum well databases)
- [ ] Ask Colin (casually, per THIS_WEEK plan): "any public datasets from the drone work I could explore the architecture against?"

## Deliverable (minimum viable proof)

- [ ] One new loader: `ml/src/ingest/wells_loader.py` → standard processed schema
- [ ] Engine run end-to-end: features → anomaly/pattern detection → risk-style scoring on well attributes (age, status, inactivity duration, inspection gaps)
- [ ] One screenshot + half-page writeup: "Same pipeline, zero model changes, new domain" → `docs/screens/` + a short section in the company site or pitch material

## Explicit non-goals

- No new dashboard. No new product. No drone-image processing (that's a real CV project — different engine).
- This proves *tabular infrastructure-records anomaly detection*, which is what the current engine actually does. Be precise about that in any conversation — overclaiming to a technical audience is worse than not demoing.

## Acceptance Criteria

- [ ] Second vertical runs end-to-end on the existing pipeline with only a loader + config changes
- [ ] Written proof artifact exists (screenshot + explanation)
- [ ] Total time spent ≤ 3 days
