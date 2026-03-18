# Upcoming Features

This document tracks planned additions to **reach.io**, grouped by tier and priority.

---

## High Priority

### Silver Tier Processing
> `silver.R`

The Medallion Architecture is defined but only the Bronze tier is currently implemented. Silver tier processing will introduce:

- `promote_to_silver()` — Quality-controlled promotion of Bronze Parquet data
- QC flagging: range checks, rate-of-change limits, spike detection
- Unit normalisation (e.g. stage → flow via rating curves)
- Gap detection and annotation
- Deduplication baked into the promotion step (currently deferred to read time)

#### Data-Type-Specific QC Tests
> `tests/testthat/test-silver.R`

Silver QC rules and their tests should be parameterised by data type, as each has distinct physical characteristics.

##### Flow (Q)

| Test | Logic | QC Flag |
|------|-------|---------|
| Non-negative (configurable) | `value < 0` where tidal/ultrasonic influence is absent | 4 (Rejected) |
| Upper bound | `value > credible_max` (site-specific) | 3 (Suspect) |
| Rate of change | `abs(diff(value)) / value > threshold` over 15 min | 3 (Suspect) |
| Spike detection | Outlier relative to rolling median ± n×MAD | 3 (Suspect) |
| Flat-lining | N consecutive identical non-zero values (stuck sensor) | 3 (Suspect) |
| Rating extrapolation | Derived from stage outside valid rating range | `extrapolated = TRUE` |

> **Note:** Negative flow is physically valid in some settings (ultrasonic gauges, tidal reaches). The non-negative check should accept an `allow_negative` argument — defaulting to `FALSE` for standard fluvial gauges and `TRUE` where tidal or bidirectional flow is expected. Tests should cover both cases.

##### Stage / Level (H)

| Test | Logic | QC Flag |
|------|-------|---------|
| Plausible range | Outside `[min_datum, max_credible]` for that station | 3 (Suspect) |
| Spike detection | Sudden large jump then return (debris, ice, sensor contact) | 3 (Suspect) |
| Rate of change | Rivers rise and fall at physically bounded rates | 3 (Suspect) |
| Flat-lining | Identical value over extended period (frozen/stuck sensor) | 3 (Suspect) |
| Stage–flow consistency | Level rising while flow drops (or vice versa) sustained | 3 (Suspect) |
| Datum shift detection | Persistent step-change offset (sensor moved or recalibrated) | 3 (Suspect) / 2 (Estimated) |

##### Rainfall (P)

| Test | Logic | QC Flag |
|------|-------|---------|
| Non-negative | `value < 0` (reset or overflow artifact) | 4 (Rejected) or corrected |
| Intensity cap | 15-min value > credible intensity (e.g. >50 mm/15 min) | 3 (Suspect) |
| Daily accumulation cap | Rolling 24 h total > ~300 mm (UK context) | 3 (Suspect) |
| Extended dry run | Long zero sequences during known wet periods (blocked gauge) | 3 (Suspect) |
| Gauge reset handling | Negative increments from tipping bucket counter overflow | Correct or flag |
| Temporal consistency | Rain at site while all neighbouring gauges show zero | 3 (Suspect) |

##### Structural / Cross-Type Tests

```r
test_that("promote_to_silver() adds all required Silver columns", { ... })
test_that("qc_flag is never NA after promotion", { ... })
test_that("qc_value equals value where qc_flag is Good (1)", { ... })
test_that("negative flow flagged as Rejected when allow_negative = FALSE", { ... })
test_that("negative flow accepted when allow_negative = TRUE", { ... })
test_that("stage outside rating range sets extrapolated = TRUE", { ... })
test_that("extreme 15-min rainfall intensity is flagged as Suspect", { ... })
test_that("flat-lined stage series is flagged as Suspect", { ... })
```

#### Gridded NetCDF QC Tests
> `tests/testthat/test-silver-netcdf.R`

Gridded sources (e.g. `radarH19`) require a fundamentally different set of checks to point gauge data. Checks operate across three dimensions (time × lat × lon) rather than a single time series. Some known failures (beam blockage, range degradation) are permanent and spatially fixed — these are better handled via a static quality mask referenced during QC rather than re-detected each run.

##### Structural / Metadata

| Check | Detail |
|-------|--------|
| Expected dimensions | `time × lat × lon` (or `y × x`) all present |
| CRS defined | Projection metadata present and parseable |
| Units declared | Variable `units` attribute exists and is a recognised string |
| Fill value consistency | `_FillValue` / `missing_value` matches actual masked cells |
| CF conventions | `Conventions` attribute present; coordinate variables follow CF naming |
| Grid resolution | Cell spacing is uniform and matches registry metadata |
| Bounding box | Domain covers expected spatial extent (e.g. UK boundary) |

##### Temporal

| Check | Detail |
|-------|--------|
| No missing timesteps | Time coordinate is complete for expected frequency |
| No duplicate timesteps | Each time value appears exactly once |
| Monotonically increasing | Time coordinate is strictly increasing |
| Time zone / epoch | `units` string decodes correctly (e.g. `hours since 1970-01-01 00:00:00 UTC`) |
| Latency | Latest timestep is within expected lag of real time (freshness check) |

##### Spatial / Per-Cell Values

| Check | Detail |
|-------|--------|
| Physical range | e.g. rainfall ≥ 0; reflectivity within valid dBZ range |
| No all-NaN slices | A timestep where every cell is masked likely indicates a missing file stitched in |
| Isolated extreme cells | Single-cell spikes inconsistent with spatial neighbours (clutter artefacts) |
| Spatial continuity | Extreme value isolated from neighbours by > n standard deviations |
| Beam blockage zones | Known radar shadow areas consistently near-zero — flag rather than fail |
| Range degradation | Quality degrades with distance from radar origin — flag peripheral cells |
| Bright band contamination | Anomalous reflectivity layer at melting level (radar-specific) |
| Edge artefacts | Boundary cells from interpolation or mosaicking |

> **Note:** Spatial checks must inspect neighbouring cells in the same timestep, not just a cell's own time history. A spike in a point gauge is a temporal outlier; a spike in a grid is a spatial outlier.

##### Aggregation / Cross-Validation

| Check | Detail |
|-------|--------|
| Areal mean vs. gauge network | Spatial average over catchment compared to collocated gauge readings |
| Temporal totals vs. climatology | Daily/monthly accumulations within expected climatological bounds |
| Zero-rain coverage | % of cells reporting zero during a known rainfall event |
| Spatial correlation structure | Rain fields should be spatially correlated — uncorrelated fields indicate corruption |

##### Structural Tests

```r
test_that("NetCDF file has expected dimensions (time, lat, lon)", { ... })
test_that("time coordinate is monotonically increasing with no gaps", { ... })
test_that("no all-NaN time slices are present", { ... })
test_that("all cell values are within physical range", { ... })
test_that("isolated single-cell spikes are flagged", { ... })
test_that("CRS metadata is present and parseable", { ... })
test_that("grid resolution matches registry metadata", { ... })
test_that("areal mean correlates with collocated gauge for same period", { ... })
```

---

### Bronze Store Catalogue
> `catalogue.R`

Currently there is no programmatic way to query what data exists in the store without reading raw files directly. Planned functions:

- `list_available_gauges()` — Return all gauges present in the Bronze store
- `summarise_coverage()` — Date ranges, record counts, and completeness percentage per gauge
- `check_gaps()` — Identify missing timesteps per gauge and data type

---

### Delta / Revision Detection

When re-ingesting historical data there is no mechanism to detect values that have changed since the last ingestion. Upstream corrections are common in hydrometric data and are significant for flood modelling provenance. Planned:

- `detect_revisions()` — Compare incoming data against stored Bronze records and log any value changes with timestamps

---

## Medium Priority

### Validation Rules Engine
> `validate.R`

A pluggable framework for domain-specific data quality rules:

- Station-level threshold configuration (e.g. maximum credible flow for a given catchment)
- Seasonal range checks
- Cross-sensor consistency checks (e.g. level vs. flow against a rating curve)
- Configurable pass/warn/fail outcomes per rule

---

### Scheduler Integration
> `schedule.R`

`run_incremental()` currently requires an external scheduler. A thin wrapper will simplify production deployment:

- `schedule_incremental()` — Register the incremental sync with the system scheduler
- Support for Windows Task Scheduler (via `taskscheduleR`) and Linux cron
- Configurable frequency, log directory, and failure alerting

---

### Backfill Checkpoint / Resume

`run_backfill()` supports `failed_only = TRUE` but has no mid-run persistence. A crash part-way through a large backfill requires restarting from the beginning. Planned:

- Checkpoint file written after each successful gauge
- Automatic resume from last checkpoint on restart
- Option to clear checkpoint and restart clean

---

## Lower Priority / Quick Wins

### `summarise_register()`

A formatted, human-readable summary of `register.csv` for operator use:

- Active gauge count and list
- Last successful sync timestamp per gauge
- Running failure counts and last error per gauge

---

### `validate_gauge_csv()`

A pre-flight validation function to check a gauge list CSV before passing it to `build_gauge_registry()`. Provides clear, actionable error messages for:

- Missing mandatory columns
- Unrecognised source system values
- Duplicate site IDs
- Malformed date fields

---

### `plot_hydro()`

A quick diagnostic plot for inspecting ingested data directly from the Bronze store, without needing to read and format Parquet files manually. Planned support for base R and `ggplot2` output.

---

### YAML Pipeline Configuration

All pipeline configuration is currently defined in-code (`PARAMETER_CONFIG`, source system constants, etc.). A config layer would allow operators to manage settings without modifying R source:

- `read_pipeline_config(path)` — Load a YAML config file
- `write_pipeline_config(path, ...)` — Write a template config
- Configurable: source systems, store paths, parallelism, thresholds

---

## Notes

- Items in this document represent intentions, not commitments. Scope and design may change.
- Contributions and discussion welcome via the standard governance process.
- Silver tier processing is considered the most impactful near-term addition, as it directly enables operational use of the ingested data.
