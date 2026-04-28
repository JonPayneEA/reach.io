# reach.io — Data Checks Summary

## 1. Schema Validation (Bronze → Silver promotion)

**Location:** `R/silver.R` — `promote_to_silver()`

Before any QC checks run, the Bronze input is validated:

| Check | What it verifies |
|---|---|
| Required columns present | `timestamp`, `value`, `supplier_flag`, `dataset_id`, `site_id`, `data_type` |
| File exists | If a path is supplied, the Parquet file must exist on disk |
| Input type | Input must be a file path or a `data.table` |

---

## 2. Structural / Integrity Checks (Bronze → Silver)

**Location:** `R/silver.R` — `promote_to_silver()`

| Check | Detail | Action |
|---|---|---|
| Deduplication | Detects duplicate `(site_id, timestamp)` pairs | Keeps last row; warns with count removed |
| Gap annotation | Infers expected interval from median timestep gap, counts missing steps per site | Attaches `attr(result, "gap_counts")`; warns if gaps found |

---

## 3. Flow QC Checks — Y-digit (UK-Flow15)

**Location:** `R/silver.R` — applied by `apply_y_digit()` via `promote_to_silver()` when `data_type == "Q"`

| Y-code | Function | What it detects | Default threshold | QC flag |
|---|---|---|---|---|
| Y=1 | `qc_negative()` | Flow < 0 | — | Rejected (4) |
| Y=2 | `qc_relative_spike()` | Single-step ten-fold increase | ratio=10, min_baseline=0.1 m³/s | Suspect (3) |
| Y=3 | `qc_absolute_spike()` | Step change > 5 MADs above median step | k=5, min_flow=1.0 m³/s | Suspect (3) |
| Y=4 | `qc_drop()` | Single-step 90% decrease | ratio=0.9, min_flow=1.0 m³/s | Suspect (3) |
| Y=5 | `qc_fluctuation()` | ≥4 direction reversals in 8-step window | window=8, reversals=4 | Suspect (3) |
| Y=6 | `qc_truncation_low()` | ≥4 identical values at ≤10th percentile | run=4, quantile=0.1 | Suspect (3) |
| Y=7 | `qc_truncation_high()` | ≥4 identical values at ≥90th percentile | run=4, quantile=0.9 | Suspect (3) |
| Y=8 | Combined (Y=2 AND Y=3) | Relative + absolute spike simultaneously | — | Rejected (4) |
| Y=9 | Combined (Y=4 AND Y=1/6) | Drop + negative or truncated low | — | Rejected (4) |

**Priority order (highest overwrites lower):** Y=8 > Y=9 > Y=1 > Y=2 > Y=3 > Y=4 > Y=6=Y=7 > Y=5

---

## 4. Stage / Level QC Checks — H-digit

**Location:** `R/silver.R` — applied by `apply_h_code()` via `promote_to_silver()` when `data_type == "H"`

| H-code | Function | What it detects | Default threshold | QC flag |
|---|---|---|---|---|
| H=1 | `qc_h_range()` | Value outside plausible datum bounds | min=-Inf, max=Inf | Suspect (3) |
| H=2 | `qc_h_spike()` | Spike-and-return within short window | k=5 MADs, return_window=3 steps | Suspect (3) |
| H=3 | `qc_h_rate_of_change()` | Single-step change > physical maximum | max_rise=0.5 m, max_fall=0.5 m per step | Suspect (3) |
| H=4 | `qc_h_flatline()` | ≥4 consecutive identical values (any level) | run=4 steps | Suspect (3) |
| H=5 | `qc_h_datum_shift()` | Persistent median offset between adjacent 24-step windows | window=24, threshold=0.1 m | Suspect (3) |

**Standalone utility (requires external data):**

| Function | What it detects |
|---|---|
| `qc_h_stage_flow_consistency()` | Stage and flow moving in opposite directions for ≥4 consecutive steps |

**Note:** All H-codes map to Suspect (3). No automated Rejection for stage data.

**Priority order:** H=1 > H=2 > H=3 > H=4 > H=5

---

## 5. Rainfall QC Checks — P-digit

**Location:** `R/silver.R` — applied by `apply_p_code()` via `promote_to_silver()` when `data_type == "P"`

| P-code | Function | What it detects | Default threshold | QC flag |
|---|---|---|---|---|
| P=1 | `qc_p_negative()` | Negative rainfall increment | — | Rejected (4) |
| P=2 | `qc_p_intensity()` | Single 15-min value > 50 mm | max=50 mm/15min | Suspect (3) |
| P=3 | `qc_p_daily_cap()` | Rolling 24-h total > 300 mm | max=300 mm, window=96 steps | Suspect (3) |
| P=5 | `qc_p_reset()` | Counter overflow spike with immediate return | k=10 MADs, return_window=2 steps | Rejected (4) |

**Standalone utilities (require external data):**

| Function | What it detects |
|---|---|
| `qc_p_dry_run()` | ≥12 consecutive zeros during a known wet period (requires wet-period mask) |
| `qc_p_temporal_consistency()` | Rain recorded while ≥80% of neighbouring gauges report zero |

**Priority order:** P=1 = P=5 > P=2 > P=3

---

## 6. S7 Object Validation (all reads)

**Location:** `R/classes.R` — `HydroData` validator, triggered on construction

| Check | What it verifies |
|---|---|
| `readings` type | Must be a `data.table` |
| Required columns | `dateTime` (POSIXct), `date` (Date), `value` (numeric), `measure_notation` (character) |
| `parameter` length | Must be scalar |
| `period_name` length | Must be scalar |

---

## 7. Bronze Store Integrity (Catalogue)

**Location:** `R/catalogue.R`

| Function | What it checks |
|---|---|
| `list_available_gauges()` | Reads governance register; returns all known gauge × data-type combinations |
| `summarise_coverage()` | Expected observation count vs. register-stated date range |
| `check_gaps()` | Reads Bronze Parquet timestamps, deduplicates, identifies missing timesteps |

---

## 8. Silver Store Integrity (Catalogue)

**Location:** `R/catalogue.R`

| Function | What it checks |
|---|---|
| `list_silver_gauges()` | Scans Silver folder tree; reports year range and file count per gauge |
| `summarise_silver_coverage()` | Reads `qc_flag` distribution per gauge (count of Good/Estimated/Suspect/Rejected, % Good) |
| `check_silver_gaps()` | Reads Silver timestamps, infers interval, identifies missing timesteps |

---

## 9. Not Yet Implemented

The following checks are deferred pending external reference data or peer review of UK-Flow15:

| Check | Reason deferred |
|---|---|
| X-digit (NRFA consistency) | Requires NRFA daily / AMAX / POT reference data |
| Z-digit (high-flow plausibility) | Requires rating curve store integration |
| Non-cardinality detection | Planned — distinguishes exact vs. conflicting duplicates |
