# Water Quality Sample Data Dictionary (ECCC National Long-term Water Quality Monitoring)

Source file used: `Water-Qual-Eau-Okanagan-Similkameen-2000-present.csv` (subset to key variables for an ML-ready sample).

This table is in **long format**: each row is one measurement of one variable at one site and datetime.

## Columns

| Column | Type | Meaning |
|---|---|---|
| site_no | string | ECCC monitoring site identifier. Multiple sites appear in this basin extract. |
| sample_datetime | datetime | Sample collection datetime (local time as provided in the source file). |
| variable | string | Variable name (English), e.g., `TURBIDITY`, `ESCHERICHIA COLI`, `LEAD TOTAL`. |
| variable_fr | string | Variable name (French). |
| value | number | Measured value for the variable. |
| unit | string | Units associated with `value` (e.g., `NTU`, `CFU/100ML`, `µG/L`, `MG/L`). |
| qualifier_flag | string | Qualifier associated with the reported value (often indicates values below/above reporting limits, e.g., `<`). Treat as metadata when modeling. |
| sdl | number | A reporting/detection-limit related field provided in the source (name in source: `SDL_LDE`). Use as metadata; do not assume all rows have it populated. |
| mdl | number | A method detection-limit related field provided in the source (name in source: `MDL_LDM`). Use as metadata. |
| variable_code | string | Code for the measured variable (source: `VMV_CODE`). Useful for joins to variable metadata tables. |
| qa_status | string | QA/processing status code from the source (e.g., `P`, `V`). Use as metadata/filters if desired. |
| sample_id | string | Sample identifier from the source file (can group multiple variables taken from the same sample). |

## Notes for ML

- Prefer filtering to `qa_status == "V"` (validated) if you want the cleanest training data; otherwise keep both for bigger volume.
- If you need a **wide** ML table (one row per site+time), pivot on `variable` and join columns.
- For classification around advisories, you’ll still need a **label table** (e.g., ISC advisories) and a join strategy (nearest site / watershed / region).

## Variables included in this sample

TURBIDITY, ESCHERICHIA COLI, FECAL COLIFORMS, LEAD TOTAL, LEAD DISSOLVED, ARSENIC TOTAL, ARSENIC DISSOLVED, PH (FIELD), DISSOLVED NITRITE/NITRATE, PHOSPHORUS TOTAL, TEMPERATURE (WATER), SPECIFIC CONDUCTANCE
