# Water-Intel — Meeting Study Sheet
### For: Mike · Meeting with Dawn Martin-Hill, Sara Smith (Six Nations Environment), Colin Gibson (McMaster/Ohneganos)

---

## 1. What you built (one sentence each)

**The pipeline:**
Raw public water-quality CSV data → cleaned & normalized → feature engineering → anomaly detection → risk scores → API → web dashboard.

**The data:**
- **Primary demo:** Ontario PWQMN (Provincial Water Quality Monitoring Network), Grand River watershed. 8 monitoring stations within or near Six Nations territory, 13,194 rows, 103 parameters, 2019–2024.
- **Secondary/reference:** ECCC Okanagan-Similkameen, 4 stations, 2000–2025.
- All data is **publicly available, open government licence** — not community-owned data.

**The models:**
- **Isolation Forest** — unsupervised anomaly detection. Learns "normal" baseline for each station/parameter, flags readings that deviate unexpectedly. Produces an anomaly score (0–1) per reading.
- **Driver Hints** — for every flagged anomaly, ranks the top 3 features that drove the flag (e.g., spike in rate-of-change? unusual zscore? large time gap?). Tells you *why* something was flagged.
- **Risk Score** — combines anomaly score + anomaly frequency into a 0–100 composite number per station. Labels: **Safe (0–30) / Watch (31–60) / Concern (61–100)**.

**The features (what the model actually learns from):**
`delta` (change from last reading), `rate_of_change`, `time_gap_days`, `is_gap` (was data missing?), rolling mean/std over 7/14/30 readings, `zscore`. All computed *per station per parameter* — so "normal" is local to each station's own history.

**The API:** FastAPI. Endpoints: `/sites`, `/sites/{id}`, `/anomalies`, `/risk/{id}`. Serves JSON to the dashboard.

**The dashboard:** Next.js/React. Shows a site list with risk badges, and a detail view with a risk card, anomaly table, and chart.

---

## 2. What Ohneganos / the McMaster team are actually doing (study this)

> Know their work before you walk in. They will notice.

**The two projects:**

- **Co-Creation of Indigenous Water Quality Tools** — Interdisciplinary McMaster team (engineers, biologists, social scientists) working *with* Six Nations and Lubicon Lake Band to identify contaminants and co-develop solutions. Has three named subteams: Traditional Ecological Knowledge, Ecosystem Health, and **Sensor Systems & Data Synthesis** (the one most relevant to you).
- **Ohneganos** — Builds on Co-Creation findings. Focused on youth empowerment, mental wellness around water anxiety, water governance and Indigenous law, bilingual educational resources. This is Dawn's broader mandate.

**What Colin Gibson specifically works on:**
Colin's most recent published paper (2026, co-authored with Sara Smith and Michael Montour from Six Nations Public Works, plus de Lannoy from McMaster): **"Performance assessment of peat-based advanced treatment devices in Six Nations of the Grand River"** — *Canadian Water Resources Journal.* He is literally testing the performance of water treatment technology on the ground at Six Nations, with sensors, before/after measurements, in partnership with the same Public Works team you're trying to reach. This is your direct connection point.

**What "Sensor Systems & Data Synthesis" means in practice:**
Based on their published research and project descriptions, the sensor work likely includes:
- Field water quality sensors deployed in creeks/tributaries (McKenzie Creek watershed is a named focus)
- Tap water testing for bacteria (E. coli, fecal coliforms) and heavy metals (lead, mercury, arsenic)
- Remote sensing for habitat mapping (ecological and cultural significance)
- Turtle monitoring as a mercury biomarker — after Six Nations Elders suggested turtles, a biologist confirmed they are a reliable contamination indicator. This is a real active data stream.
- IoT/time-series data from deployed sensors (a 2019 paper from the team compares time-series databases for water quality storage — they've been solving this exact infrastructure problem)

**Key watershed research you should know:**
- Deen et al. (2021, 2023, 2025) — three papers on McKenzie Creek watershed / Grand River, covering climate change impacts on streamflow, and blue/green water scarcity. **McKenzie Creek is part of the Six Nations watershed your demo data already covers.** Your anomaly detection runs on Grand River data from stations near McKenzie Creek.
- Mercury is a documented contaminant at Six Nations, traced downstream from multiple urban centres. Only 12% of Six Nations homes use the on-site treatment plant.
- Dawn lives on Six Nations and has first-hand knowledge of the water crisis — this is not abstract research to her.

**The framing Dawn has used publicly:**
*"There's a wealth of Indigenous Knowledge accumulated over thousands of years of observation and experimentation. We need to bring these two knowledge systems together."*
She is not looking for a tech vendor. She is looking for a collaborator who understands that Western science and Indigenous knowledge need to work together, and who won't extract data from the community. That's why OCAP® is non-negotiable in your pitch.

---

## 3. How the Water-Intel pipeline connects to their actual work

This is the technical bridge. Study it — it's your most credible talking point.

| Their work | Your pipeline | What changes |
|-----------|--------------|--------------|
| Sensor readings from McKenzie Creek / field stations | Your ingest layer already handles time-series water quality data in normalized format | **New loader** for their sensor data format — same architecture as `eccc_loader.py`, different schema |
| Treatment device performance (before/after readings, Colin's peat-device work) | Anomaly detection flags when a station's readings deviate from baseline | **No model changes needed** — point the IForest at their treatment monitoring data and it learns what "normal operation" looks like, flags degradation |
| Heavy metals monitoring (mercury, lead, arsenic, already in your ECCC data) | Driver hints identify which parameter drove the anomaly flag | Already built — just add Mercury as a tracked parameter |
| McKenzie Creek watershed / climate model data (Deen et al.) | External features could augment your rolling-statistics baseline | **Phase 2 enhancement** — don't promise this now, but name it |
| Time-series sensor databases (Fadhel et al. 2019) | Your pipeline currently reads CSVs; could be pointed at a proper time-series DB | **Compatible** — your feature engineering is DB-agnostic |
| Community data sovereignty / OCAP® requirement | Currently all public data — OCAP® compliant data layer is Phase 2 | Name it explicitly — you already know the framework |
| GIS mapping of habitat and cultural sites | Not in your pipeline yet | **Honest gap** — name it; spatial context layer is a Phase 2 roadmap item |

**The two-sentence technical pitch for the meeting:**
*"Our pipeline ingests time-series water quality data, engineers anomaly-detection features, and produces risk scores and driver hints per station. Pointing it at your sensor data instead of public PWQMN data is a loader swap — the model, the API, and the dashboard are all already built."*

---

## 4. What the system is NOT (be honest in the meeting — this builds trust)

> Say this plainly. It's a strength, not a weakness.

- **This is Phase 1 — source water (2A), not drinking water (2B).** The PWQMN stations monitor surface water (rivers, streams) — they are *upstream indicators*, not tap-water measurements. The system does not monitor what comes out of a treatment plant or a tap.
- **It does NOT predict or issue ISC drinking water advisories.** It flags statistical anomalies. An anomaly means "something unusual happened here" — it is a signal for follow-up, not a compliance determination.
- **The model does not know Indigenous-specific context** — it learned from numbers alone. It has no knowledge of what activities happen near a station (agricultural runoff, industrial discharge, ceremony).
- **Data is ~15 months stale** — PWQMN publishes annually; latest point is Dec 2024. Real-time is a Phase 2 capability.
- **No community data sovereignty (yet)** — all current data is publicly sourced. OCAP® principles are the framework for Phase 2, not built into Phase 1.

---

## 3. What Phase 2 looks like (where community partnership matters)

Phase 2 is what you'd build *with* a community partner, not *for* them. It requires:
- **2B data** — actual potable water system data (treatment plant outputs, distribution, household)
- **Community-owned data governance** — OCAP® (Ownership, Control, Access, Possession). The Nation owns the data. The Nation controls what's shared and with whom. You build the infrastructure that honours that.
- **Real-time sensor integration** — replacing annual CSV pulls with live feeds
- **Community-relevant parameters** — the Nation's Environment Department knows which parameters matter locally (e.g., specific industrial contaminants upstream, seasonal patterns, traditional knowledge about the watershed)

**The pitch for this meeting is not "buy our software."**
It is: "We have a working Phase 1 platform that can demonstrate early-warning analytics on your watershed data. What would Phase 2 need to look like to be genuinely useful to you?"

---

## 4. The six parameters the model currently tracks (know at least these)

From the ECCC sample data / PWQMN data: **Turbidity, E. coli, Fecal Coliforms, Lead (total + dissolved), Arsenic (total + dissolved), pH, Nitrite/Nitrate, Phosphorus (total), Specific Conductance, Water Temperature.**

**What they mean in plain language:**
- **Turbidity** — cloudiness. High turbidity = sediment/runoff. Often first signal after rain event or upstream disturbance.
- **E. coli / Fecal Coliforms** — bacterial contamination. Most direct drinking-water risk indicator.
- **Lead/Arsenic** — heavy metals. Can come from aging infrastructure, mining, industrial discharge. Low-and-slow risk — exactly what anomaly detection is good at catching.
- **Nitrate** — agricultural runoff signal. Health risk at high levels (especially infants).
- **pH** — acidity/alkalinity. Extreme values affect all other chemistry.
- **Specific Conductance** — dissolved ions. Useful as a "something changed" sentinel — spikes often precede specific contaminant readings.

---

## 5. Key numbers to have ready

| Fact | Number |
|------|--------|
| Stations in demo dataset | 8 (Grand River watershed near Six Nations) |
| Total readings | 13,194 |
| Parameters tracked | 103 |
| Years of data | 2019–2024 |
| Closest station to Six Nations | Big Creek at Hwy 54, NW of Caledonia (~4.3 km) |
| Risk labels | Safe / Watch / Concern |
| Anomaly contamination assumption | 5% (configurable) |

---

## 6. What to listen for in the meeting

These answers will shape Phase 2 and your IRAP grant narrative. Take notes.

- What data does the Six Nations Environment Department currently have? (sensors, lab results, spreadsheets?)
- What decisions are they making that better water intelligence would help? (advisories? infrastructure investment? environmental stewardship reports?)
- What does Dawn/Sara's team currently do when a water concern arises? What's the gap?
- Are there specific parameters, locations, or seasonal events that worry them most?
- What would OCAP®-compliant data governance look like for a partnership like this?
- Is there any existing relationship with PWQMN or ECCC data already?

---

## 7. The one framing line to come back to

> "Phase 1 gives us the foundation. Phase 2 is something we'd build together — with community ownership of the data and the platform."

That line closes every answer you're not sure how to answer, and it's true.

---

## 8. After the meeting — immediate actions

- [ ] Send a thank-you email same day (brief)
- [ ] Log the outcome in `docs/AI_OUTREACH_PLAYBOOK.md`
- [ ] If interest is confirmed → draft a 1-page pilot brief (problem · delivery · timeline · price)
- [ ] Book the NRC IRAP ITA call — "in active discussion with Six Nations Environment Dept and McMaster Ohneganos" is your opening line
