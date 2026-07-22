# Water-Intel — Technical Brief: Anomaly Detection Methodology

**Prepared by:** Mike, Gemini Matrix Consulting
**Date:** July 2026
**Version:** v2 (updated 2026-07-21) — adds Section 6 event cross-reference annex. *(v1, sent July 14, ended at Section 5 with the annex "in preparation.")*
**Audience:** Hydrology / hydrogeology reviewers
**Live prototype:** https://water.geminimatrixinc.com/dashboard
**Status:** Phase 1 proof of concept on public historical data. This brief describes what the system does, why, and — equally important — what it does not do yet.

---

## 1. Problem & Philosophy

Water-Intel asks a narrow question: **given routine water-quality monitoring data, can we automatically surface the readings and periods that deserve human attention?**

We deliberately chose **unsupervised anomaly detection** rather than supervised prediction or forecasting, for three reasons:

1. **No labels exist.** There is no dataset of Grand River readings tagged "this preceded a real event." Without labels, supervised training is not honestly possible — and pretending otherwise produces models that overfit to proxies.
2. **The events that matter most are the ones nobody anticipated.** Threshold-based systems detect known failure modes against fixed limits. An unsupervised approach flags *any* multivariate departure from a station's own recent behaviour, including patterns no one thought to write a rule for.
3. **It transfers.** An unsupervised pipeline works on any station's data without per-site training labels, which matters for communities whose monitoring history is thin.

We are explicit about the corollary: an anomaly is **statistically unusual, not necessarily hazardous**. The system is a prioritization layer for human judgment — it does not predict drinking water advisories, and we do not claim otherwise anywhere in the product.

**Why Isolation Forest specifically:** it is multivariate (sees combinations of signals, not one parameter at a time), efficient on modest data volumes, deterministic under a fixed seed (important for reproducible review), and its one structural assumption — that anomalies are few and different — is stated openly as a tunable contamination parameter rather than hidden in the method.

## 2. The Pipeline

**Data.** Ontario Provincial Water Quality Monitoring Network (PWQMN) public data, 2019–2024: **8 active stations in the Grand River watershed within 45 km of Six Nations** (nearest ~4 km), 13,194 station-parameter readings across **103 parameters** (conductivity, turbidity, nutrients, metals, ions, physical parameters). The loader normalizes three PWQMN schema generations into one canonical long format: `station_id, timestamp, parameter, value, unit`.

**Feature engineering — where "local normal" is defined.** Features are computed independently for every **station + parameter** group, so each series is compared only against its own history:

| Feature | Definition |
|---|---|
| `delta` | change from the previous reading |
| `time_gap_days` | days since the previous reading |
| `rate_of_change` | delta ÷ time gap (normalizes irregular sampling intervals) |
| `is_gap` | flag when the gap exceeds 35 days |
| `rolling_mean_{7,14,30}`, `rolling_std_{7,14,30}` | rolling statistics over the last 7/14/30 **readings** |
| `zscore` | deviation from the 30-reading rolling mean, in units of the 30-reading rolling std |

Two honest details a reviewer should know: windows are defined in **reading counts, not calendar time**, because PWQMN sampling is periodic and irregular (roughly monthly in the monitoring season, and not every parameter at every visit). At that frequency a 7-reading window spans months and a 30-reading window can span years — so the rolling baselines capture a station's *local longer-term normal* and partially absorb seasonality, but they do **not** explicitly model the seasonal cycle. Explicit seasonal decomposition is a Phase 2 item (Section 5).

**Model.** A single **global Isolation Forest** (200 trees, `contamination = 0.05`, fixed random seed) is trained on the 11 standardized features across all stations and parameters. The architecture is therefore: *local baselines, global anomaly criterion* — each reading is described relative to its own series' history, and the forest learns one consistent notion of "how unusual is this deviation" across the watershed. Raw scores are min-max normalized to [0, 1]; the output is deterministic (identical file hash on re-run).

**Driver hints — interpretability without a black box.** For every flagged reading we compute per-feature z-scores against the population and report the top 3 by magnitude. An operator reads "rolling_mean_30: +3.0" as "this value sits three standard deviations above the station's 30-reading average." No SHAP or surrogate models — at this scale, plain z-scores are transparent and auditable.

**Risk score.** Each station gets a 0–100 score: mean anomaly score (normalized across stations, weight 60) plus anomaly rate (normalized across stations, weight 40), mapped to Safe / Watch / Concern. Because both components are normalized **across the 8 stations**, this is a **relative prioritization within the monitored set** — "which stations most deserve attention" — not an absolute probability of harm. The dashboard presents it as decision support with that guardrail stated.

## 3. Why This Design Suits Water Monitoring

- **Local baselines respect that every station is different.** A conductivity level that is normal at one station can be exceptional at another; per-series rolling statistics encode that without hand-set thresholds.
- **The multivariate view catches what single-parameter limits miss.** A reading can be individually unremarkable on every parameter while the *combination* — a sustained level shift plus rising volatility plus an abrupt delta — is unusual. That combination is what the forest sees.
- **Irregular sampling is handled structurally, not ignored.** Rate-of-change normalizes by elapsed time; gap flags mark records whose context is weak.
- **Results so far are physically sensible.** At a fixed 5% contamination, per-station flag rates spread from 2.0% to 8.1% — the model discriminates between stations rather than flagging uniformly. Conductivity is the leading anomaly parameter at 5 of 8 stations, consistent with road-salt and effluent influence in this watershed. The strongest flagged reading is a conductivity excursion to ~10,900 µS/cm at Grand River at York — roughly an order of magnitude above typical river values; cross-referencing (Section 6) later resolved it as a probable field-instrument/data-quality issue rather than a water event, itself a useful catch. Driver analysis shows most flags are driven by **sustained level shifts** (rolling means) rather than single-reading spikes, i.e., the model is mostly detecting regime changes, not blips.

## 4. Honest Limitations

We would rather state these ourselves than have a reviewer discover them.

1. **No validation against labeled events yet.** The false-positive and false-negative rates are unknown. Until flagged anomalies are cross-referenced against known events (spills, storm events, treatment changes, advisories), "the model flags plausible things" is an informed judgment, not a measured result. This is the single most important Phase 2 task, and the reason we are seeking expert and community collaboration.
2. **The 5% contamination rate is an assumption, not a finding.** Isolation Forest requires an expected anomaly fraction a priori; we chose 5% as a starting point. The *ranking* of scores is informative regardless, but the binary flag threshold is arbitrary until calibrated against real events. We would welcome a domain-informed prior here.
3. **Historical data only.** The latest published PWQMN data ends December 2024. Phase 1 demonstrates the intelligence layer on historical records; it is not live monitoring, and the product says so explicitly.
4. **Seasonality is absorbed only implicitly.** Reading-count windows at roughly monthly sampling blur seasonal structure into the baseline rather than modeling it. Slow seasonal transitions can inflate baseline variance and mask genuine anomalies (or occasionally create spurious ones at season boundaries).
5. **Global model trade-off.** One forest across all stations gives cross-station consistency and more training data, but a station whose dynamics differ strongly from the pool could be systematically over- or under-flagged. Comparing global vs per-station models is a planned Phase 2 experiment.
6. **The risk score is relative.** It ranks the monitored stations against each other; it is not an absolute measure and would need re-interpretation as stations are added.
7. **The model learns from numbers only.** It has no knowledge of land use, upstream activity, weather, flow conditions, or Indigenous knowledge of the river. It is a statistical instrument intended to sit alongside — never replace — human and community expertise.

## 5. Phase 2 — What Validation and Improvement Look Like

In priority order:

1. **Event cross-referencing (validation).** Compare flagged anomalies against documented events — provincial spill records, storm/flood records, treatment interventions — to estimate real precision and recall. Community and expert input on which historical events to test against is the most valuable contribution a partner can make.
2. **Hydrological context as model inputs.** Correlate with flow and precipitation (ECCC hydrometric and weather data are public and near-real-time): rain-event → turbidity lag structure, flow-normalized concentrations, and flow-aware baselines.
3. **Explicit seasonal handling.** Seasonal decomposition or season-conditioned baselines, so seasonal transitions stop consuming baseline variance.
4. **Contamination calibration and per-station model comparison,** informed by (1).
5. **Supervised layer where labels emerge.** If a partner can label even a modest set of historical events, a supervised classifier can complement the unsupervised layer rather than replace it.
6. **Near-real-time operation** on sensor or SCADA feeds where a community chooses to share them — under data-governance terms (OCAP®) in which the community retains full ownership and control of its data.

---

## 6. Annex — Preliminary Event Cross-Reference

This annex reports preliminary correlation against public records only; it is not formal model validation. It directly addresses Limitation #1 and aligns with Phase 2 item #1 by documenting which top anomalies were corroborated, and where no matching public record was found.

**Method.** Flagged readings were first collapsed into station-visit events (multiple parameters flagged at the same station on the same date count as one event; co-flagged parameters are listed alongside the highest-scoring one). The top 25 events by anomaly score — capped at 5 per station so no single station dominates, covering 7 of the 8 stations — were cross-referenced against two public registries within a ±7-day window: (a) Ontario Spills Action Centre surface-water occurrence records, 2019–2024, matched at the municipality/county level (the finest location resolution that dataset provides); and (b) Water Survey of Canada HYDAT daily flows at Grand River gauges 02GB001, 02GB010, and 02GA003, with flow spikes defined as daily flow exceeding the 30-day rolling mean by more than 2 standard deviations (gauge 02GB007 has no published daily flows after 2019; two stations have no nearby gauge). "High" confidence requires a flow spike within 2 days or a contaminant-category-matched spill within 3 days; the matched event shown is always the nearest one in the window.

**Summary: 9 High-confidence matches, 4 Possible, 12 with no public-record match.** Three observations stand out:

1. **A watershed-wide event on January 13, 2020.** The model independently flagged multi-parameter events (iron, aluminium, phosphorus, conductivity, particulates) at **five of the eight stations** on the same date. All three of those stations with nearby gauge coverage are corroborated by **same-day flow spikes** at two independent gauges; the remaining two stations have no gauge to check against. A second multi-station event on March 10–11, 2020 shows the same pattern (same-day flow spikes at two gauges). This is the storm-driven mobilization signature the system is designed to surface.
2. **A probable instrument/data-quality catch at York.** The highest-scoring anomaly in the entire dataset (conductivity, 10,900 µS/cm, January 27, 2021) turns out to be a same-visit divergence between the field probe (10,900) and the lab-corrected value (1,020) at 0.8 °C water temperature — a ~10x gap consistent with a field-instrument or transcription issue present in the province's own published dataset. The model flagged it without any knowledge of instruments. Distinguishing sensor faults from genuine water events is itself an operationally valuable capability.
3. **No spill-record matches — and that is a sound null.** All 25 events were checked against 3,749 surface-water spill records with municipality-level matching; none coincided within the window. Given that spill locations are only published at county granularity and most anomalies are flow-driven, this is unsurprising, and we prefer reporting it plainly over stretching a match.

One honest caveat visible in the table: Fairchild Creek (16018409302) contributes five uncorroborated events. Its paired gauge has no post-2019 flow data, so flow corroboration was impossible there — and it is also the station the global model flags most often, consistent with Limitation #5 (a station whose dynamics differ from the pool may be over-flagged). Resolving which explanation holds requires exactly the local knowledge a community partner brings.

| station | date | primary parameter | value | score | co-flagged same visit | matched event | confidence |
|---|---|---|---|---|---|---|---|
| 16018409202 | 2021-01-27 | CONDUCTIVITY, AMBIENT | 10900.0 | 1.0 | CONDUCTIVITY, 25C | data_quality (2021-01-27) | High |
| 16018409202 | 2021-02-23 | CONDUCTIVITY, AMBIENT | 1208.0 | 0.9641 | CONDUCTIVITY, 25C | flow_spike (2021-02-26) | Possible |
| 16018409302 | 2019-09-11 | IRON, UNFILTERED TOTAL | 3580.0 | 0.9588 | ALUMINIUM, UNFILTERED TOTAL; PHOSPHORUS,UNFILTERED TOTAL; CONDUCTIVITY, 25C; CONDUCTIVITY, AMBIENT | none | None |
| 16018409302 | 2020-10-08 | IRON, UNFILTERED TOTAL | 486.0 | 0.9563 | ALUMINIUM, UNFILTERED TOTAL; CONDUCTIVITY, 25C; CONDUCTIVITY, AMBIENT; PHOSPHORUS,UNFILTERED TOTAL; STRONTIUM, UNFILTERED TOTAL | none | None |
| 16018409302 | 2019-10-09 | IRON, UNFILTERED TOTAL | 559.0 | 0.9556 | ALUMINIUM, UNFILTERED TOTAL; CONDUCTIVITY, 25C; CONDUCTIVITY, AMBIENT | none | None |
| 16018401202 | 2019-06-13 | IRON, UNFILTERED TOTAL | 142.0 | 0.9517 | ALUMINIUM, UNFILTERED TOTAL; MANGANESE,UNFILTERED TOTAL; PHOSPHORUS,UNFILTERED TOTAL | none | None |
| 16018409302 | 2020-01-13 | IRON, UNFILTERED TOTAL | 973.0 | 0.9371 | ALUMINIUM, UNFILTERED TOTAL; CONDUCTIVITY, 25C; CONDUCTIVITY, AMBIENT; PHOSPHORUS,UNFILTERED TOTAL; STRONTIUM, UNFILTERED TOTAL | none | None |
| 16018409302 | 2020-03-11 | IRON, UNFILTERED TOTAL | 753.0 | 0.9261 | ALUMINIUM, UNFILTERED TOTAL; CONDUCTIVITY, 25C; CONDUCTIVITY, AMBIENT | none | None |
| 16018409202 | 2021-03-09 | CONDUCTIVITY, AMBIENT | 981.0 | 0.9184 | CONDUCTIVITY, 25C | flow_spike (2021-03-05) | Possible |
| 16018402702 | 2020-01-13 | IRON, UNFILTERED TOTAL | 1750.0 | 0.9018 | ALUMINIUM, UNFILTERED TOTAL; PHOSPHORUS,UNFILTERED TOTAL; CONDUCTIVITY, 25C; CONDUCTIVITY, AMBIENT; STRONTIUM, UNFILTERED TOTAL; RESIDUE,PARTICULATE | flow_spike (2020-01-13) | High |
| 16018403502 | 2020-01-13 | IRON, UNFILTERED TOTAL | 1710.0 | 0.8891 | ALUMINIUM, UNFILTERED TOTAL; CONDUCTIVITY, AMBIENT; STRONTIUM, UNFILTERED TOTAL; CONDUCTIVITY, 25C; PHOSPHORUS,UNFILTERED TOTAL | flow_spike (2020-01-13) | High |
| 16018402702 | 2020-10-08 | IRON, UNFILTERED TOTAL | 56.6 | 0.8792 | ALUMINIUM, UNFILTERED TOTAL; STRONTIUM, UNFILTERED TOTAL; CONDUCTIVITY, 25C; CONDUCTIVITY, AMBIENT; PHOSPHORUS,UNFILTERED TOTAL; RESIDUE,PARTICULATE | none | None |
| 16018401202 | 2019-07-17 | IRON, UNFILTERED TOTAL | 585.0 | 0.8708 | ALUMINIUM, UNFILTERED TOTAL; PHOSPHORUS,UNFILTERED TOTAL | flow_spike (2019-07-17) | High |
| 16018403502 | 2024-10-31 | HARDNESS | 1460.0 | 0.8697 | IRON; CALCIUM; ALUMINUM; STRONTIUM; CONDUCTIVITY | flow_spike (2024-10-31) | High |
| 16018403502 | 2023-02-22 | STRONTIUM | 502.0 | 0.862 | CONDUCTIVITY; ALUMINUM | none | None |
| 16018401202 | 2020-11-23 | IRON, UNFILTERED TOTAL | 122.0 | 0.8605 | ALUMINIUM, UNFILTERED TOTAL; CONDUCTIVITY, AMBIENT; CONDUCTIVITY, 25C; PHOSPHORUS,UNFILTERED TOTAL; STRONTIUM, UNFILTERED TOTAL | flow_spike (2020-11-26) | Possible |
| 16018403502 | 2023-04-19 | STRONTIUM | 978.0 | 0.855 | ALUMINUM; IRON; CONDUCTIVITY | none | None |
| 16018402702 | 2020-03-11 | IRON, UNFILTERED TOTAL | 889.0 | 0.8517 | ALUMINIUM, UNFILTERED TOTAL; PHOSPHORUS,UNFILTERED TOTAL; CONDUCTIVITY, 25C; CONDUCTIVITY, AMBIENT; STRONTIUM, UNFILTERED TOTAL | flow_spike (2020-03-11) | High |
| 16018403502 | 2023-04-03 | IRON | 1360.0 | 0.8517 | STRONTIUM; ALUMINUM; CONDUCTIVITY; PHOSPHORUS; TOTAL; SOLIDS; SUSPENDED | flow_spike (2023-04-03) | High |
| 16018401202 | 2020-03-10 | IRON, UNFILTERED TOTAL | 832.0 | 0.8333 | ALUMINIUM, UNFILTERED TOTAL; PHOSPHORUS,UNFILTERED TOTAL; CONDUCTIVITY, AMBIENT; CONDUCTIVITY, 25C | flow_spike (2020-03-10) | High |
| 16018401202 | 2019-08-13 | IRON, UNFILTERED TOTAL | 186.0 | 0.8222 | ALUMINIUM, UNFILTERED TOTAL; PHOSPHORUS,UNFILTERED TOTAL | flow_spike (2019-08-19) | Possible |
| 16018402702 | 2023-04-03 | ALUMINUM | 954.0 | 0.8207 | CONDUCTIVITY; IRON; STRONTIUM; PHOSPHORUS; TOTAL | flow_spike (2023-04-02) | High |
| 16018401002 | 2020-01-13 | ALUMINIUM, UNFILTERED TOTAL | 803.0 | 0.7965 | IRON, UNFILTERED TOTAL; CONDUCTIVITY, AMBIENT; CONDUCTIVITY, 25C | none | None |
| 16018400902 | 2020-01-13 | PHOSPHORUS,UNFILTERED TOTAL | 777.0 | 0.7928 | RESIDUE,PARTICULATE; CONDUCTIVITY, AMBIENT; RESIDUE,TOTAL; RESIDUE,FILTERED | none | None |
| 16018401002 | 2021-02-23 | CONDUCTIVITY, AMBIENT | 1413.0 | 0.7849 | CONDUCTIVITY, 25C | none | None |

*Contact: Mike · Gemini Matrix Consulting · geminimatrixinc@gmail.com · Live dashboard: water.geminimatrixinc.com/dashboard*
