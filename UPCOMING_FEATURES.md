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
