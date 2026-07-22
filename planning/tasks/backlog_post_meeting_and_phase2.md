# Backlog — Post-Meeting / Phase 2 Ideas (not on the near-term radar)

**Status:** 🗄️ Parked — deliberately not day-numbered, deliberately not sequenced
**Trigger to revisit:** the hydrological expert meeting happens, or a Phase 2/Mitacs scope
conversation starts. Not before.
**Companion (completed 2026-07-21):** [done/day20b_ux_improvements.md](done/day20b_ux_improvements.md)

---

## Why this file exists

These are real, legitimate ideas that surfaced during the day20a event-cross-reference work —
but none of them are worth doing on spec. Each has a specific trigger below. Don't work these
just because they're sitting here; wait for the trigger.

---

## 1. Precipitation enrichment

- ECCC precipitation data could explain anomalies at stations with no working flow gauge
  (Fairchild Creek 16018409302 — 5 uncorroborated events, gauge 02GB007 has no data after 2019)
- **Trigger:** only pursue if the expert review specifically asks "did you check rainfall?" — flow
  spikes already integrate precipitation for the stations that do have gauge coverage, so the
  marginal value is low

## 2. PDF build script for the technical brief

- Brief PDFs are manual exports with no in-repo build script
- Naming convention already established (2026-07-21): `*_v1*` = sent July 14;
  `Water-Intel_Technical_Brief_v2_July2026.pdf` = the current follow-up version
- **Trigger:** the third time you manually export a brief PDF — at that point automate it
  (`ops/scripts/build-brief-pdf.ps1` or pandoc), reading the version from the markdown header

## 3. Fairchild Creek investigation (per-station vs global model)

- Fairchild contributes the most uncorroborated events and is the most-flagged station overall —
  either a real local runoff pattern or the global model over-flagging a divergent station
  (brief Limitation #5)
- This is already named as a Phase 2 experiment in the brief itself
- **Trigger:** a Phase 2 / Mitacs scoping conversation — Fairchild is the natural test case to
  propose for per-station vs global model comparison

## 4. Spills matching at finer-than-county granularity

- Ontario's public spills registry only publishes municipality/county-level location — no finer
  matching is possible without either FOI-level data or community-held records
- **Trigger:** a community partner brings local event knowledge — this gap *is* the Phase 2 pitch,
  not a problem to solve alone

## 5. News-archive citation for the Jan 13, 2020 event

- Quick manual search ("January 11–13 2020 storm/thaw/rainfall southern Ontario Grand River" +
  GRCA flood bulletins archive) could turn "flow spikes corroborate" into "a documented storm
  corroborates" — a nice-to-have one-line strengthening of brief Section 6
- **Trigger:** none urgent — ~10 minutes whenever convenient, or skip it. The flow-spike evidence
  already stands on its own without a news citation.

---

## Notes
- Origin: closing review of day20a (2026-07-21) and the day20b UX split (2026-07-21).
- Nothing here blocks anything. Revisit this file when its trigger fires, not on a schedule.
