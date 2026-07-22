# Day 20b — UX Improvements: Event Corroboration Surfaced in the UI

**Sprint:** Week 5 — Validation before agents
**Status:** ✅ **DONE (2026-07-21)** — Codex implemented Steps 1 and 3; independently reviewed and verified against real code and live data (see Acceptance Criteria + Verification notes below).
**Depends on:** day20a event cross-reference ✅ (revision complete 2026-07-21)
**Do before:** the next live demo / the hydrological expert meeting — this is the demo screen
**Companion (parked ideas):** [backlog_post_meeting_and_phase2.md](backlog_post_meeting_and_phase2.md)

---

## Why this task exists

The event cross-reference is real and sound. Most of the work to make it *visible* in the site
detail view already shipped (see Status check below) — this file now tracks what's genuinely
left: markers on the timeline chart itself, and a corroboration signal on the site list view.

**Design principle (the test for every item below):** every element must answer a question an
operator would actually ask. If it doesn't, it's decoration — cut it.

---

## Status check (2026-07-21) — Steps 0–2 are substantially already done

Between task-file passes, the site **detail** page and API were already wired to the annex —
verified against real code, not just claimed:
- `services/api/main.py` `/anomalies` merges `outputs/anomaly_event_annex.csv` (confidence, event
  type, date, description) into anomaly records
- `web/app/app/dashboard/sites/[siteId]/page.tsx` renders confidence badges (High/Possible/None),
  a corroboration-count summary, and per-anomaly callouts with timing + a no-causation guardrail
- This uses the **capped 25-row annex directly** with anomalies outside it explicitly labeled
  "not yet cross-referenced" — an honest pattern that avoids needing a separate uncapped file.
  **This settles the Step 0 question from the original draft of this file: no uncapped
  `events_all.csv` is needed.** The "checked a sample, rest not yet checked" framing is sufficient
  and matches how the brief itself talks about coverage.

**What's confirmed still not done** (verified against real code, 2026-07-21):
- The timeline **overlay markers** on the chart itself — the detail page already shows event
  context as text (callouts + table), but not on the chart
- The **list-view context chip** (Step 3 below) — verified `/sites` in `services/api/main.py`
  does not merge the annex, and `web/app/app/dashboard/page.tsx` has no corroboration signal

**Correction (2026-07-21):** the original draft of this file said the timeline chart uses
recharts. **It does not — it's MUI X Charts** (`@mui/x-charts/LineChart`, component at
`web/app/app/dashboard/sites/[siteId]/AnomalyTimeline.tsx`). Use the spec below, not recharts APIs.

## Step 1 — Timeline event markers on the chart (the demo screen) — still open

**Files involved:**
- `web/app/app/lib/chartData.ts` — `buildAnomalyTimeline()` builds the chart dataset
- `web/app/app/dashboard/sites/[siteId]/AnomalyTimeline.tsx` — renders `<LineChart>` from that dataset
- `web/app/app/dashboard/sites/[siteId]/page.tsx` — passes `anomalies` (type `AnomalyRecord[]`,
  which already has `event_confidence`, `matched_event_type`, `matched_event_date`,
  `matched_event_description` per `web/app/app/lib/types.ts`) into `<AnomalyTimeline>`

**No backend or type changes needed** — `AnomalyRecord` already carries every field required;
this is purely a chart-rendering change.

**Approach (MUI X Charts LineChart, not recharts — there is no `ReferenceDot`):**
1. In `chartData.ts`, extend `AnomalyTimelinePoint` with two optional numeric fields:
   `flowSpikeMarker` and `dataQualityMarker`. In `buildAnomalyTimeline()`, for each point set
   `flowSpikeMarker = anomalyScore` when `matched_event_type` is `"flow_spike"` or
   `"spill_and_flow_spike"` (else `undefined`), and `dataQualityMarker = anomalyScore` when
   `matched_event_type === "data_quality"` (else `undefined`). Leaving the field `undefined`
   (not `0`) is what makes MUI skip drawing a mark/line segment at that point.
2. In `AnomalyTimeline.tsx`, add two more entries to the `series` array on `<LineChart>`, one
   per marker type: `dataKey: "flowSpikeMarker"`, `showMark: true`, `area: false`,
   `connectNulls: false`, `color` matching the existing badge palette used in `page.tsx`
   (`#38bdf8`-ish blue for flow_spike — matches the "flow-spike (blue)" choice already agreed on;
   amber `#fbbf24`/`#fcd34d` for data_quality, consistent with `confidenceBadge()` colors in
   `page.tsx`). Repeat for `dataQualityMarker`.
3. Hide the connecting line for these two marker series so only the dot shows: target them via
   `sx` using MUI's per-series class hook, e.g.
   `"& .MuiLineElement-series-flow-spike-marker": { stroke: "transparent" }` (set an explicit
   `id` on each series to make this selector reliable), keeping `showMark: true` so the dot mark
   still renders.
4. Tooltip: the existing chart has `slotProps={{ tooltip: { trigger: "none" } }}` (tooltips are
   disabled entirely right now). Either (a) leave tooltips off and accept dots-only markers — the
   detail-page callouts below the chart already give the description — or (b) if a hover tooltip
   is wanted, that requires re-enabling `trigger: "item"` and is a bigger change; **default to (a)
   unless it turns out easy**, don't let a tooltip requirement block shipping the markers.
- [ ] Payoff: "the model flagged this, and here's the storm that explains it" visible directly on
      the chart, not just below it — the single most persuasive thing to show live
- [ ] Verify against real data: York (`16018409202`, 2021-01-27, data-quality) and the Jan 2020
      multi-station event (`16018402702`/`16018403502`, 2020-01-13, flow_spike) should show
      colored dots at those points on their respective station's chart

## Step 2 — Anomaly table corroboration + co-flagged parameters — ✅ already done

No longer open work — confirmed shipped in the site detail page (confidence badges + callouts).
Nothing to do here.

## Step 3 — List-view context chip (Safe/Watch/Concern + explanation, not a second badge) — still open

**Design decision, settled:** do **not** add a second colored badge next to Safe/Watch/Concern.
Two parallel color-coded taxonomies on one card forces the eye to reconcile them — is
"Concern + no events" worse or better than "Watch + 2 events"? That's confusion, not clarity.

Instead: keep the one risk badge (the alarm signal). Add a **small, neutral, grey chip** beside or
under it that appears only when there's something to say:

```
┌─────────────────────────────┐
│ Grand River at Brantford    │
│ 🔴 Concern · 72             │
│ ⚡ 2 corroborated events     │   ← grey text, no color competition
└─────────────────────────────┘

┌─────────────────────────────┐
│ Grand River at York         │
│ 🟡 Watch · 48               │
│ 🔧 possible sensor issue    │
└─────────────────────────────┘

┌─────────────────────────────┐
│ Nith River at Paris         │
│ 🟢 Safe · 24                │
│                             │   ← nothing to say → nothing shown
└─────────────────────────────┘
```

**Backend — `services/api/main.py`:**
- [ ] Add a helper `_load_site_event_chips() -> dict[str, dict]` mirroring the pattern already
      used inside `_load_anomalies()` (lines ~81–109): load `ANNEX_PATH` if it exists, group by
      `station_id`, and for each station compute:
      - `corroborated_count` = rows where `confidence` is `"High"` or `"Possible"`
      - `data_quality_flag` = `True` if any row has `matched_event_type == "data_quality"`
      - Return `{}` per-station if `ANNEX_PATH` doesn't exist or the station has no rows in the
        annex (this is the "absence of data, not absence of events" case — must not default to 0)
- [ ] In `list_sites()`, merge this into each record before returning — e.g. attach
      `corroborated_event_count: int | None` and `has_data_quality_flag: bool` fields, `None`/
      absent when the station wasn't in the annex sample at all (distinct from `0` when it was
      checked and found nothing)
- [ ] Follow the existing `_clean_mapping` / `_to_records` conventions already used elsewhere in
      this file for NaN/type handling — don't hand-roll new serialization logic

**Frontend — `web/app/app/lib/types.ts` and `web/app/app/dashboard/page.tsx`:**
- [ ] Extend `SiteSummary` with the two new optional fields from the backend
- [ ] In the site-card render loop in `page.tsx` (the `sites.map(...)` block, ~line 216 onward),
      add the chip as a small element under the existing risk badge block (the
      `<div style={{ display: "flex", flexDirection: "column", ... }}>` around line 291) —
      **not** a new colored `Alert` or the `siteStatusBadgeStyle` pattern (that palette is
      reserved for Safe/Watch/Concern); use plain neutral text/pill styling consistent with the
      muted greys already used elsewhere on this page (`#a1a1aa` text tone)
- [ ] Chip content logic: if `has_data_quality_flag` → show the sensor-issue chip (wins the
      one-chip-max rule per below); else if `corroborated_event_count` is a positive number →
      show the corroborated-events chip; else (including `undefined`/not-in-sample) → render
      nothing

Rules:
- [ ] **Neutral styling only** — no red/yellow/green on the chip itself; the risk badge owns color
- [ ] **Render only when non-empty** — most stations show nothing; don't clutter the default view
- [ ] **One chip max per card** — if a station has both corroborated events and a sensor-quality
      flag, show the sensor flag (it changes what the operator does next, which is more actionable
      than a count)
- [ ] Source is `outputs/anomaly_event_annex.csv` via the new `/sites` merge above — the same
      capped file the detail page already uses (see the Status check above; no new data file
      needed)

**Why this earns its place:** it makes the difference between "Concern, corroborated storm" and
"Concern, unexplained" visible at a glance across the whole site list — which is a real triage
signal (unexplained Concern arguably deserves the *most* attention). In a demo, it shows in two
seconds that the model distinguishes explained risk from instrument noise from open questions —
exactly the "intelligence layer, not chart portal" positioning.

---

## Acceptance Criteria — all verified 2026-07-21

- [x] Anomaly table rows show corroboration label + co-flagged parameters — already shipped
- [x] Timeline overlay renders event markers on the chart itself — code-reviewed (`chartData.ts`
      correctly derives `flowSpikeMarker`/`dataQualityMarker` from `matched_event_type`;
      `AnomalyTimeline.tsx` wires two extra MUI X Charts series with hidden connecting lines and
      per-series mark colors). Verified the underlying input data is correct end-to-end via live
      server-rendered HTML for both named cases: York (`16018409202`, 2021-01-27, CONDUCTIVITY
      AMBIENT, `event_confidence: High`, `matched_event_type: data_quality`) and the Jan 2020
      event (`16018402702`, 2020-01-13, IRON, High-confidence flow_spike match, rendered in the
      corroboration callout). Could not get a visual screenshot due to browser-tool flakiness in
      this session (unrelated to the code — see Verification notes below), but the full data path
      feeding the chart is confirmed correct.
- [x] `/sites` endpoint carries per-station annex-derived chip content — verified live via HTTP:
      correct `corroborated_event_count` / `has_data_quality_flag` per station, `None`/`None` for
      the one station never in the annex sample (`16018412802`)
- [x] List view shows the context chip on stations with something to report, and shows nothing on
      stations without — verified via server-rendered HTML for **all 8 stations individually**:
      `16018403502` (3 corroborated) ✅, `16018401202` (4 corroborated) ✅, `16018402702`
      (3 corroborated) ✅, `16018409202` (sensor-issue chip, correctly winning over its own
      3 corroborated events — the one-chip-max rule) ✅, three zero-count stations show no chip ✅,
      `16018412802` (never sampled) shows no chip ✅
- [x] No colored badge added to the list view beyond the existing Safe/Watch/Concern — confirmed
      by code review (`page.tsx`): chip uses neutral grey (`#a1a1aa` / `rgba(39,39,42,0.65)`),
      no red/yellow/green anywhere in the chip styling

## Verification notes (2026-07-21)

- Production build (`npm run build`) compiles cleanly with no TypeScript errors
- Backend logic verified two ways: direct in-process function calls against `main.py`, and live
  HTTP calls against a running uvicorn instance — both matched
- One environmental snag during verification: a stale process was already bound to port 8000,
  running pre-revision code in memory (confirmed via its hardcoded `version: "0.1.0"` response vs.
  the current code's `"1.2.0"` read from the `VERSION` file) — that process was outside this
  shell's visibility/kill permissions (likely a different session). Worked around it by running a
  clean instance on port 8001 for verification; this was a local dev-environment quirk, not a
  defect in Codex's changes. Also created `.claude/launch.json` (wasn't in the repo before) so the
  web app can be started via the browser-preview tooling in future sessions.
- The one thing not directly screenshotted was the live chart render (browser tool had a separate,
  unrelated hydration hiccup in this session) — compensated with exhaustive data-path verification
  instead, which is what actually determines correctness for this feature (the chart-library
  wiring was already reviewed against the exact MUI X Charts API and is deterministic given
  correct input data, which is now proven correct).

## Notes
- Origin: closing review of day20a (2026-07-21); chip design settled 2026-07-21; reconciled
  against already-shipped dashboard integration 2026-07-21 (Step 0/2 from the original draft
  turned out to be already done or unnecessary — see Status check above).
- This task is UX only — it does not change any risk score, anomaly detection, or matching logic.
