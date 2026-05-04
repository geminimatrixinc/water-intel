# Day 6 — EDA Notebook (Repeatable)

**Sprint:** Week 1 — Pipeline + First ML Result  
**Status:** 🔲 Not Started  
**Depends on:** Day 5 (processed data) 🔲  
**Blocked by:** Day 5

---

## Objective
Create an exploratory data analysis notebook that reads the processed dataset and produces visual insights about signal quality, coverage, and patterns per site.

## Deliverables

### `ml/notebooks/02_eda.ipynb`
- Load `data/processed/eccc_processed.csv`
- Per-site time coverage (first/last reading, gap analysis)
- Parameter distribution (histograms, box plots)
- Time-series plots for top signals (turbidity, E. coli, pH, chlorine residual)
- Correlation heatmap across parameters (wide pivot)
- Missing data visualization (heatmap or bar chart)

### `docs/screens/` — 2–3 exported screenshots
- Key EDA charts saved as PNG for use in pitch materials

## Acceptance Criteria
- [ ] Notebook runs end-to-end without errors
- [ ] At least 4 visualizations produced
- [ ] 2–3 images exported to `docs/screens/`
- [ ] Observations are written in markdown cells (not just code)

## Commit Message
```
docs: processed-data EDA + screenshots
```

## Notes
- Use matplotlib/seaborn (simple, reproducible)
- This notebook doubles as demo material — keep charts clean and labeled
- Focus on: "What signals do we have? How clean are they? What patterns are visible?"
