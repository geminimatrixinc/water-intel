# Day 20a — Event Cross-Reference Module (Annex → Feature)

**Sprint:** Week 5 — Validation before agents
**Status:** ✅ **DONE (2026-07-21)** — revision pass completed and verified. Municipality join fixed (word-boundary + ALNWICK-HALDIMAND exclusion), same-visit collapse added (`co_flagged_parameters`), 5-per-station cap added, nearest-event description fix applied. Final annex: **25 events across 7 stations — 9 High / 4 Possible / 12 None**. Jan 13 2020 event now shows at **5 of 8 stations** with same-day flow spikes at 2 gauges; second multi-station event found Mar 10–11 2020. Zero spill matches is now a *sound* null (3,749 records properly evaluated). Brief Section 6 rewritten with method + findings; Section 3 York sentence aligned. **Leftover items split (2026-07-21) into [day20b_ux_improvements.md](day20b_ux_improvements.md) (completed same day, also in `done/`) and [../backlog_post_meeting_and_phase2.md](../backlog_post_meeting_and_phase2.md) (parked, trigger-based).** Remaining human steps: PDF regen + Colin email (see "AFTER CODEX DELIVERS" below — now "Mike's follow-up").
**Depends on:** Technical brief sent to Colin (✅ July 14) · anomaly outputs current (`outputs/anomalies.csv`)
**Feeds:** Colin follow-up (~Aug 5) · hydrological expert review · Phase 2 validation story
**Timebox:** 2 sessions (~half-day each). If matching gets messy, ship Output 1 alone.

---

## Objective

Validate the top-scoring anomalies against documented real-world events (spills, storm flows).
Turns the brief's Limitation #1 ("false-positive rate unknown") into evidence:
**"preliminary event correlation — the model flagged X, and X happened."**

This is the strongest possible content for the ~Aug 5 Colin follow-up and the expert review.
It is *validation work, not feature work* — it does not violate the product freeze.

## Why This Matters

- A hydrological modeller's first question is "does it catch real events?" This answers it with data.
- Even **2–3 solid matches out of the top 10** is a compelling result.
- **A null result is also a valid outcome.** "3 of 10 corroborated, 7 no public record found" is *more*
  credible than a perfect table. Report "none found" honestly — never stretch a match.
- The matching layer later becomes a permanent pipeline feature (event context enrichment), not throwaway analysis.

---

## Session 0 — De-risk COMPLETE (2026-07-21) ✅

Both data sources confirmed real and downloadable. Findings below — do not re-derive, just use.

### ✅ RESOLVED — York conductivity spike, verified against raw source (2026-07-21)

**Not a pipeline bug.** Confirmed byte-for-byte against `data/raw/pwqmn_2019_2021.csv`:

```
16018409202,CONDAM,"CONDUCTIVITY, AMBIENT",2021,20210127,10:10,C268440, ,10900,,MICROMHOS/CM (CONDUCTIVITY),FIELD
16018409202,COND25,"CONDUCTIVITY, 25C",   2021,20210127,10:10,C268440, ,1020, ,MICRO SIEMENS PER CENTIMETER,E3218A
16018409202,FWTEMP,"TEMPERATURE, WATER",  2021,20210127,10:10,C268440, ,0.8,  ,DEGREES CELSIUS,FIELD
```

`CONDAM` (RESULT=10900) is a **field**-method ambient-conductivity probe reading; `COND25`
(RESULT=1020) is the same-visit **lab**-method 25°C-corrected value. Our ingest passed the
government's own published source value through unchanged — nothing to fix in `pwqmn_loader.py`.

**Why the two values diverge 10x:** water temperature that day was 0.8°C. Temperature correction
alone (0.8°C → 25°C reference) typically scales conductivity by roughly 1.5–2x, not 10x — so this
looks like **field-probe miscalibration or a transcription slip at the point of collection**,
present in Ontario's own published dataset, not something introduced downstream.

**Reframe (better than the original hypothesis):** don't present this as "the model caught a
contamination event" — present it as **"the model caught a sensor/data-quality problem"**: a
distinct, genuinely valued anomaly-detection use case that separates real events from instrument
faults. That's a stronger, more defensible opening line for a hydrologist than a contamination
story would have been, and it's fully grounded in the paired same-visit reading.

- [x] Source-file verification complete — no code fix needed
- [x] Use this framing explicitly in the annex row for this anomaly: `matched_event_type =
      "data_quality"`, description noting the paired field/lab divergence and the 0.8°C context
- [x] Also feature a clean contamination-pattern example from **station 16018403502 (Dunnville)**
      (hardness, strontium, iron, aluminum — several high-confidence hits) so the annex shows both
      anomaly *categories* the model catches: real events and instrument/data-quality issues

### Data source status
- **Ontario Spills (MECP Spills Action Centre):** ✅ Real, downloadable.
  - `https://files.ontario.ca/moe_mapping/downloads/4Other/SAC/spill_occurrences_2003-2022.csv` (20MB, 125,217 rows)
  - Separate 2023 + 2024 XLSX files on data.ontario.ca (needed for full 2019–2024 coverage)
  - Columns: `Date Reported, Time Reported, Reference Number, Site Municipality, Contaminant Name, Receiving Media, Health Impact, Environmental Impact, Source Type, Incident Event, Incident Reason`
  - **No lat/long** — location is text-only, **at county level** (e.g. Caledonia/Dunnville return
    zero direct hits — they're filed under **Haldimand County**)
  - Filter `Receiving Media` contains "Surface Water" first — cuts 125K rows to ~2,600 relevant ones
- **WSC Hydrometric Flow (Grand River gauges):** ✅ Real, downloadable via HYDAT (SQLite/Access,
  quarterly) or per-station export at wateroffice.ec.gc.ca. No GRCA shortcut CSV — use WSC directly.

### Station → gauge/municipality mapping (use this, don't re-derive)

| PWQMN station | Location | WSC flow gauge | Spills municipality |
|---|---|---|---|
| 16018402702 | Grand River, Brantford | **02GB001** (exact match) | Brantford |
| 16018409302 | Fairchild Creek, Brantford Twp | **02GB007** (exact match) | Brant County |
| 16018412802 | Big Creek, Caledonia | **02GB010** McKenzie Creek near Caledonia (close) | Haldimand County |
| 16018409202 | Grand River, York | near 02GB010 | Haldimand County |
| 16018403502 | Grand River, Dunnville | near 02GB010 | Haldimand County |
| 16018401202 | Grand River, Blair | near 02GA003 Galt | Region of Waterloo |
| 16018401002 | Grand River, Glen Morris | — (between gauges) | Brant County |
| 16018400902 | Nith River, Paris | — (tributary) | Brant County |

**Bonus:** 02GB010 (McKenzie Creek) is the exact tributary Ohneganos's sensor work focuses on
per `meeting_prep_six_nations.md` — a natural talking point if this station's matches look good.

---

## CODEX EXECUTION PLAN (self-contained — hand this whole section to Codex)

> Everything below is verified against the real project structure and real data sources
> (checked 2026-07-21). No further discovery/research needed — just build it.

### Repo conventions to follow (copy these patterns exactly)

Look at `ml/src/models/driver_hints.py` and `ml/src/reports/site_summary.py` first — every new
module in this repo follows the same shape:
- Module docstring at top explaining purpose + `Usage:` block with CLI examples
- `PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent` then `sys.path.insert(0, str(ML_ROOT))`
- `DEFAULT_*` path constants at module level
- A `build_x(...)` function that does the work, prints progress with `print(f"...")`, writes the
  CSV, and prints a `✅ ... written to ...` summary at the end
- A thin `main()` with `argparse` wrapping `build_x(...)`, called from `if __name__ == "__main__":`
- Run via `python -m src.<package>.<module>` from the `ml/` directory

New files to create, in this order. **Step 0 (York verification) is already done — see the
"RESOLVED" section above — skip straight to Step 1.**

### Step 1 — `ml/src/ingest/spills_loader.py` (new file)

Loads and filters the Ontario MECP spills dataset.

```
Function: load_spills(raw_dir: Path = PROJECT_ROOT/"data"/"raw", min_year: int = 2019) -> pd.DataFrame
```

- Download sources (fetch once, cache to `data/raw/`):
  - `https://files.ontario.ca/moe_mapping/downloads/4Other/SAC/spill_occurrences_2003-2022.csv`
    → save as `data/raw/spills_2003_2022.csv`
  - 2023 and 2024 XLSX files — find current direct links on
    `https://data.ontario.ca/dataset/environmental-occurrences-and-spills` (the download resource
    URLs rotate/versioned; resolve them from that page's "Explore" buttons rather than hardcoding)
    → save as `data/raw/spills_2023.xlsx`, `data/raw/spills_2024.xlsx`
  - If 2023/2024 files can't be resolved automatically, it's fine to proceed with 2019-2022
    coverage only and note the gap — don't block on this
- Columns (confirmed real): `Date Reported, Time Reported, Reference Number, Site Municipality, Contaminant Name, Receiving Media, Health Impact, Environmental Impact, Health/Environmental Consequence, Sector Type, Source Type, Source/Sector Type, Incident Event, Incident Reason`
- Processing:
  1. Concatenate all years, parse `Date Reported` to datetime, filter `>= min_year`
  2. Filter to rows where `Receiving Media` contains `"Surface Water"` (case-insensitive `str.contains`)
  3. Normalize `Site Municipality` to uppercase stripped string for joining
  4. Output columns: `date, municipality, contaminant, receiving_media, environmental_impact, source_type, incident_event`
- No lat/long available — matching is municipality-text based only (see Step 3)

### Step 2 — `ml/src/ingest/hydrometric_loader.py` (new file)

Loads Water Survey of Canada daily flow data for the target gauges.

```
Function: load_flow_spikes(gauge_ids: list[str], min_year: int = 2019, spike_threshold_std: float = 2.0) -> pd.DataFrame
```

- **Do NOT use the real-time datamart** (`dd.weather.gc.ca/hydrometric/csv/...`) — verified 2026-07-21
  that it 404s / only covers a rolling recent window, not 2019-2024 historical data.
- **Use the full HYDAT SQLite database instead** — this is the one reliable bulk-download path:
  1. Resolve the current download link from
     `https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/monitoring/survey/data-products-services/national-archive-hydat.html`
     (filename is versioned like `Hydat_sqlite3_YYYYMMDD.zip` — do not hardcode a date)
  2. Download + unzip to `data/raw/Hydat.sqlite3` (large file, ~1-2GB — gitignore it, same
     treatment as other `data/raw/` sources)
  3. Query with Python's built-in `sqlite3` (no new dependency needed) via
     `pandas.read_sql("SELECT * FROM DLY_FLOWS WHERE STATION_NUMBER = ?", conn, params=[gauge_id])`
  4. `DLY_FLOWS` table (verified schema) has one row per `STATION_NUMBER` + `YEAR` + `MONTH`, with
     `FLOW1`...`FLOW31` columns holding each day's value (m³/s) — melt to long format using
     `YEAR`/`MONTH`/`FLOW{day}` → construct an actual `date` column, output `station_number, date, flow_m3s`
     (there are also paired `FLOW1_SYMBOL`...`FLOW31_SYMBOL` quality-flag columns — ignore these
     for this pass, not needed for spike detection)
- Target gauge IDs (use the mapping table above): `02GB001, 02GB007, 02GB010, 02GA003`
- Spike detection: for each gauge, compute rolling mean/std of daily flow (reuse the same rolling-window
  logic style as `ml/src/features/build_features.py` for consistency), flag days where flow exceeds
  `mean + spike_threshold_std * std` as a storm-flow proxy event
- Output columns: `date, gauge_id, flow_m3s, is_flow_spike`

### Step 3 — `ml/src/validation/event_crossref.py` (new file, new `ml/src/validation/` package — add `__init__.py`)

The matching/correlation layer. This is the core deliverable.

```
Function: build_event_crossref(
    anomalies_path: Path = PROJECT_ROOT/"outputs"/"anomalies.csv",
    top_n: int = 25,
    window_days: int = 7,
    output_path: Path = PROJECT_ROOT/"outputs"/"anomaly_event_annex.csv",
) -> pd.DataFrame
```

1. Load `outputs/anomalies.csv`, filter `is_anomaly == 1`, sort by `anomaly_score` descending, take top `top_n`
2. Attach static station→municipality and station→gauge_id mappings as a module-level dict
   (hardcode the table from this file's "Station → gauge/municipality mapping" section — 8 entries,
   no need to make this dynamic)
3. Load spills (`load_spills`) and flow spikes (`load_flow_spikes`) via the loaders above
4. **Special case first:** the York station (16018409202) conductivity anomaly on 2021-01-27 —
   hardcode this one row's `matched_event_type = "data_quality"`, confidence `"High"`, description
   referencing the paired field/lab divergence (see the "RESOLVED" section above) rather than
   running it through the spill/flow matcher — there is nothing to match, the explanation is
   already known and stronger than a spill/flow guess would be
5. For every other top anomaly:
   - Find spills where `municipality == mapped_municipality` and `abs(date - anomaly_date) <= window_days`
   - Find flow spikes where `gauge_id == mapped_gauge` and `abs(date - anomaly_date) <= window_days`
   - Confidence tier:
     - `"High"` — a spill match with matching contaminant category (e.g. anomaly parameter is a metal
       and spill contaminant is a metal) within 3 days, OR a flow spike within 2 days
     - `"Possible"` — any spill or flow-spike match within the 7-day window without contaminant alignment
     - `"None"` — no match found (**write this explicitly, don't drop the row** — see Guardrails)
6. Output one row per top anomaly: `station_id, date, parameter, value, anomaly_score, top_drivers, matched_event_type, matched_event_date, matched_event_description, confidence`
7. Print the same style of summary block as `site_summary.py` (counts by confidence tier)

### Step 4 — Annex + brief integration

**Confirmed:** the brief already sent to Colin (July 14) has a placeholder for exactly this —
`docs/TECHNICAL_BRIEF.md` line 80 reads:

> *Annex (in preparation): preliminary cross-reference of the top-scoring Grand River anomalies against public event records.*

This means the annex isn't just useful — it's **already a stated promise to Colin**, sitting right
above the contact line at the end of the doc (line 82). This raises the priority: finishing it
closes a loop he's already seen referenced, not a speculative add-on.

- [x] Render `outputs/anomaly_event_annex.csv` as a markdown table and add it as
      `## 6. Annex — Preliminary Event Cross-Reference` in `docs/TECHNICAL_BRIEF.md`, replacing
      the "in preparation" placeholder — done, revised 2026-07-21 with the post-revision table
- [x] Keep the framing paragraph above the table — done, expanded into a Method paragraph +
      3 numbered findings + the Fairchild caveat
- [ ] Note: PDF export still outstanding — see "AFTER CODEX DELIVERS" / Mike's follow-up below
      (v1 files renamed with `_v1` labels; v2 export not yet generated)

### Step 5 — Dashboard overlay (stretch goal — only if Steps 1-4 done with time to spare)

- Skip unless explicitly requested. If pursued: add matched-event markers to the existing anomaly
  timeline chart in `web/app/` (check `web/app/app/lib/` for the existing chart component using
  `recharts` per `PROGRESS.md` Day 13 notes) — out of scope for a first pass.

### Acceptance check for Codex to self-verify before calling this done

- [x] `python -m src.validation.event_crossref` runs from `ml/` with no errors
- [x] `outputs/anomaly_event_annex.csv` exists with 25 rows (or fewer if `top_n` anomalies < 25 exist)
- [x] At least the "None" confidence tier is represented honestly — a suspiciously perfect all-matched
      table should be treated as a bug, not a win
- [x] The York station (16018409202) row carries `matched_event_type = "data_quality"` per the
      resolved finding above (hardcoded, not run through the matcher)
- [x] `docs/TECHNICAL_BRIEF.md` Section 6 added, placeholder line replaced
- [x] `data/raw/Hydat.sqlite3` and any new large raw downloads confirmed covered by the existing
      `data/raw/` gitignore rule (no action needed — already ignored, just don't override it)

---

## REVISION PASS (review findings, 2026-07-21 — hand back to Codex)

v1 shipped and works (`outputs/anomaly_event_annex.csv`, brief Section 6 in place, honest None
rows, York data-quality row correct). Review against the real spills data found **one bug that
must be fixed before the annex goes to Colin**, plus two cheap quality improvements.

### 🐛 BUG — municipality join silently fails (fix required)

`_find_spill_matches` in `ml/src/validation/event_crossref.py` uses exact equality
(`spills_df["municipality"] == municipality`), but the spills file spells municipalities
inconsistently. Verified against `data/raw/spills_2003_2022.csv` (real values found):
`WATERLOO`, `Waterloo`, `HALDIMAND COUNTY`, `Haldimand`, `HALDIMAND COUNTY;NORFOLK COUNTY`,
`BRANT COUNTY`, `Brant`, `brant`, `Brantford`, `BRANTFORD`.

Consequences in v1:
- `REGION OF WATERLOO` (Blair station key) matches **nothing** — the file never uses that form
- `HALDIMAND COUNTY` misses `Haldimand` and semicolon multi-municipality rows
- `BRANT COUNTY` misses `Brant`/`brant`
- Verified: there ARE dozens of surface-water spills in these municipalities since 2019, so
  the v1 "zero spill matches" outcome is partly join failure, not a finding

**Fix:**
1. In `STATION_CONTEXT`, change municipality keys to short substring forms:
   `BRANTFORD`, `BRANT`, `HALDIMAND`, `WATERLOO`
2. In `_find_spill_matches`, replace the equality test with a substring containment test:
   `spills_df["municipality"].str.contains(municipality, case=False, na=False, regex=False)`
   — note `BRANTFORD` rows also contain `BRANT`? No — containment direction matters:
   check `municipality_key in spills_value`, so `BRANT` key would also match `BRANTFORD` values.
   To avoid that false-positive: match on word-boundary regex instead, e.g.
   `spills_df["municipality"].str.contains(rf"\b{municipality}\b", case=False, na=False)`
   with keys `BRANTFORD`, `BRANT`, `HALDIMAND`, `WATERLOO` — `\bBRANT\b` will not match
   `BRANTFORD`, and `HALDIMAND` matches both `Haldimand` and `HALDIMAND COUNTY;NORFOLK COUNTY`.
3. Re-run and regenerate `outputs/anomaly_event_annex.csv` + brief Section 6 table

### Improvement 1 — per-station cap (diversity)

11+ of 25 v1 rows are the same Fairchild Creek (16018409302) iron/aluminum pattern. Add a
`max_per_station: int = 5` parameter to `build_event_crossref`; select top anomalies per station
by score, then merge and re-sort. Keeps the annex representative of the watershed, not one station.

**Note for the annex text (Mike):** the Fairchild repetition is itself an honest observation —
it is Limitation #5 from the brief ("global model may over-flag a divergent station") showing up
in practice. One sentence acknowledging this strengthens credibility.

### Improvement 2 — collapse same-visit multi-parameter rows

Iron + aluminum flagged at the same station on the same date is one *event*, not two annex rows.
Group by (station_id, date), keep the highest-scoring parameter as the primary row, list the
co-flagged parameters in a new `co_flagged_parameters` column.

### Revision implementation status (completed and verified 2026-07-21)

- [x] Fix municipality aliases and word-boundary matching in `_find_spill_matches`
      (`\b<key>\b` regex, short-form keys, `ALNWICK-HALDIMAND` exclusion added on discovery)
- [x] Add `max_per_station: int = 5` and enforce the per-station cap before final ranking
- [x] Collapse same-visit multi-parameter rows and add `co_flagged_parameters`
- [x] Re-run `python -m src.validation.event_crossref` after all revisions — verified clean run
- [x] Regenerate `outputs/anomaly_event_annex.csv` from the revised implementation — 25 rows,
      7 stations, 9 High / 4 Possible / 12 None
- [x] Re-sync `docs/TECHNICAL_BRIEF.md` Section 6 from the revised annex — rewritten with method,
      3 findings, Fairchild caveat, new table; Section 3 York sentence aligned

### Dashboard integration status (completed 2026-07-21)

- [x] API merges annex confidence, event type, date, and description into anomaly records
- [x] Site detail view surfaces High/Possible/None/not-yet-cross-referenced states honestly
- [x] Corroborated event callout includes timing and a no-causation guardrail
- [x] Driver hints render as responsive chips inside the Reading column
- [x] Frontend lint and production build pass
- [ ] Timeline event markers remain a stretch goal; event context is currently surfaced in the
  detail callout and anomaly-history table

### Re-verify after revision — all confirmed 2026-07-21

- [x] Waterloo and Haldimand short-form rows get spill candidates properly evaluated
      (3,749 surface-water spill records now correctly checked — previously silently 0 due to
      the exact-equality bug)
- [x] The Jan 2020 event **strengthened** under revision — now visible at **5 of 8 stations**
      (2020-01-13) with same-day flow spikes at two independent gauges, plus a second
      multi-station event found (2020-03-10/11) — stronger headline than pre-revision
- [x] York data-quality row unchanged and confirmed correct
- [x] "None" rows present (12 of 25) and now a *sound* claim — the join actually works and still
      found zero spill matches within the window; stated plainly in the brief's finding #3

---

## AFTER CODEX DELIVERS — Mike's follow-up tasks (not for Codex — needs Mike's voice)

### Colin follow-up email (~Aug 5)
- [ ] Draft the follow-up around the findings: "Since sending the brief, I cross-referenced the
      top-scoring anomalies against Ontario spill records and Grand River flow data — here's what
      matched, including one interesting case where the model caught a probable field-instrument
      issue rather than a contamination event."
- [ ] Attach the updated brief (with Section 6); keep the email itself short
- [ ] Log in `docs/AI_OUTREACH_PLAYBOOK.md`
- [ ] **Versioning (updated 2026-07-21):** export the new PDF as
      **`docs/Water-Intel_Technical_Brief_v2_July2026.pdf`** — this is the file to attach to the
      Colin follow-up. The old exports are renamed with `_v1` labels
      (`TECHNICAL_BRIEF_v1.pdf`, `Water-Intel_Technical_Brief_v1_July2026.pdf`,
      `TECHNICAL_BRIEF_v1.html`) — **do not send anything labeled v1**; v1 is what Colin already
      has from July 14. The markdown header carries the version history.

### Dashboard timeline overlay (stretch goal, optional, separate from Codex's core scope)
- [ ] Anomaly timeline with matched events overlaid — a good demo screen for the expert meeting,
      but only worth doing if Steps 1–4 land with time to spare

---

## Acceptance Criteria (top-level, for this whole task)

- [x] Annex table exists with confidence tiers and honest "none found" rows — **9 High / 4 Possible / 12 None across 7 stations**
- [x] York conductivity excursion explained (data-quality finding, not contamination)
- [x] `docs/TECHNICAL_BRIEF.md` updated with the annex — Section 6 rewritten (method, 3 findings, caveat, new table), Section 3 aligned, versioned v2
- [x] **Bonus, found already done 2026-07-21:** dashboard site-detail page independently wired to the annex — confidence badges, corroboration counts, per-anomaly callouts with a no-causation guardrail, all verified against real code (`services/api/main.py` `/anomalies` merge + `page.tsx` rendering). This exceeds the original scope of this task.
- [x] Timeline overlay markers + list-view context chip → completed and verified 2026-07-21 in [day20b_ux_improvements.md](day20b_ux_improvements.md)
- [ ] Colin follow-up email + v2 PDF export (Mike's step, tracked below and in `day19_5`/`AI_OUTREACH_PLAYBOOK.md` — does not block closing this file)

**Verdict: the engineering/validation task is done.** Everything that makes the annex correct,
documented, and even partially visible in-product is complete and verified. The only open item —
sending the Colin email — is outreach work, already tracked as its own action in
`day19_5_post_meeting_expert_prep.md` and the outreach log, so it does not need to keep this file
open. Moving to `done/`.

## Guardrails

- Static/historical data is fine — validation is historical by nature.
- Never stretch a match to make the table look better; the expert will check.
- Frame everywhere as *"preliminary event correlation"* — formal validation stays a Phase 2 claim.

## Notes

- SR&ED: log this work — event-matching methodology against noisy public records has genuine
  technical uncertainty (matching thresholds, spatial attribution, confounders).
- If the Ohneganos thread stays silent past ~Aug 4, this annex also powers the fallback
  parallel-McMaster outreach (per day19_5) — same artifact, second audience.
