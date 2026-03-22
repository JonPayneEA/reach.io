# reach.io — Claude Code Guide

## Project Overview

**reach.io** is an R package providing hydrometric data pipeline tools for the
[Environment Agency Hydrology API](https://environment.data.gov.uk/hydrology/doc/reference).
It covers the full journey from raw API/bulk-file sources through to Bronze-tier
Parquet storage, following the Hydrometric Data Framework v1.3 and R Tool
Governance standards.

Target users: EA Flood Forecast Modelling team.

---

## Repository Layout

```
reach.io/
├── R/
│   ├── package.R       # Package constants: PARAMETER_CONFIG, VALID_SOURCES, VALID_CATEGORIES
│   ├── schema.R        # Bronze schema, dataset IDs, supplier codes, provenance
│   ├── setup.R         # setup_hydro_store()
│   ├── classes.R       # S7 HydroData classes (Rainfall_Daily, Flow_15min, etc.)
│   ├── lookup.R        # find_stations(), get_measures()
│   ├── download.R      # download_hydrology()
│   ├── sync.R          # fetch_readings(), make_date_chunks(), run_sync()
│   ├── batch.R         # submit_batch(), poll_batch(), run_batch()
│   ├── output.R        # handle_output()
│   ├── registry.R      # build_gauge_registry()
│   ├── router.R        # fetch_from_hde/wiski/bulk_file/wiski_all(), route_gauge()
│   ├── ingest.R        # ingest_bulk_file()
│   ├── ingest_all.R    # ingest_all_file(), fetch_from_wiski_all()
│   ├── backfill.R      # run_backfill()
│   ├── incremental.R   # run_incremental()
│   ├── rating.R        # Rating curve classes and lookup
│   ├── rating_store.R  # Rating curve store management
│   ├── catalogue.R     # list_available_gauges(), summarise_coverage(), check_gaps()
│   ├── double_mass_plot.R
│   └── silver.R        # promote_to_silver() — skeleton, in development
├── tests/testthat/
│   ├── test-classes.R
│   ├── test-core.R
│   ├── test-incremental.R
│   ├── test-pipeline.R
│   ├── test-rating.R
│   └── test-silver.R
├── vignettes/
│   ├── bronze-ingestion.Rmd
│   ├── rating-curve-lifecycle.Rmd
│   ├── rating-lifecycle-overview.Rmd
│   └── data-framework.md
├── DESCRIPTION
├── NAMESPACE
└── UPCOMING_FEATURES.md
```

---

## Architecture

### Medallion Storage Tiers

| Tier | Status | Description |
|------|--------|-------------|
| **Bronze** | Implemented | Raw ingested data in partitioned Parquet, one file per gauge per year |
| **Silver** | In development (`silver.R`) | QC-flagged, deduplicated, unit-normalised data |
| **Gold** | Planned | Derived/aggregated outputs, CAMELS-style attributes |

Bronze paths follow: `bronze/<CATEGORY>/<SUPPLIER>/<DATA_TYPE>/<YYYY>/<dataset_id>.parquet`

### Source Systems

Data is routed from three source systems:
- **HDE** — EA Hydrology API (primary)
- **WISKI** — Older EA system for some gauges
- **Bulk file** — Historical bulk downloads

The `router.R` functions (`route_gauge()`, `fetch_from_hde()`, etc.) dispatch
to the correct source.

### S7 Classes

All returned data objects are typed S7 classes defined in `classes.R`.
Classes encode parameter (Rainfall, Flow, Level), period (Daily, 15min, Hourly),
and supply metadata slots. Downstream code should work with S7 objects
regardless of which store tier data comes from.

---

## Key Functions

### Setup
- `setup_hydro_store(path)` — Create directory structure for all tiers

### Discovery
- `find_stations(...)` — Query EA API by WISKI ID, RLOIid, notation, name, or proximity
- `get_measures(station_id)` — List available measures for a station

### Download
- `download_hydrology(gauge_id, start, end, ...)` — Synchronous API download, returns S7 object
- `run_sync(gauge_ids, ...)` — Parallel sync across multiple gauges
- `run_batch(gauge_ids, ...)` — Batch submit + poll for large date ranges

### Pipeline
- `build_gauge_registry(csv_path)` — Build master gauge registry from CSV
- `route_gauge(gauge_id)` — Determine source system for a gauge
- `ingest_bulk_file(path, ...)` — Ingest historical bulk file to Bronze
- `run_backfill(registry, ...)` — Parallelised backfill across all gauges
- `run_incremental(registry, ...)` — Incremental sync (run via external scheduler)

### Catalogue
- `list_available_gauges()` — List all gauges in Bronze store
- `summarise_coverage()` — Date ranges and completeness per gauge
- `check_gaps()` — Identify missing timesteps

### Rating Curves
- Defined in `rating.R` and `rating_store.R`
- S7 class-based lifecycle; see `vignettes/rating-curve-lifecycle.Rmd`

### Silver (in development)
- `promote_to_silver(data, ...)` — QC-flag and promote Bronze data to Silver tier
- QC flags: 1=Good, 2=Estimated, 3=Suspect, 4=Rejected

---

## Development Conventions

### Language & Dependencies
- **R** package; uses `S7` for OOP, `data.table` for tabular data, `arrow` for Parquet I/O
- `future` / `future.apply` for parallelism
- `httr` for API calls, `ggplot2` for plots, `lubridate` for dates

### Testing
Run tests with:
```r
devtools::test()
# or for a specific file:
testthat::test_file("tests/testthat/test-silver.R")
```

Tests use `testthat` edition 3. All test files live in `tests/testthat/`.

### Documentation
```r
devtools::document()   # regenerates NAMESPACE and man/ from roxygen2
devtools::build_vignettes()
```

### Code Style
- Follow existing S7 class patterns in `classes.R` when adding new data types
- New data types require entries in `DATA_TYPE_CODES` (schema.R) and
  `setup_hydro_store()` (setup.R) before ingestion tools can be written
- Bronze schema columns: `timestamp`, `value`, `supplier_flag`, `dataset_id`,
  `site_id`, `data_type`
- Provenance records written to Hydrometric Data Register on each ingest

---

## Active Development Areas

See `UPCOMING_FEATURES.md` for full detail. High-priority items:

1. **Silver tier** (`silver.R`) — `promote_to_silver()` skeleton exists; Y-digit
   QC tests implemented for Flow; Stage/Level and Rainfall checks planned
2. **MOSES PE ingestion** — `ingest_moses_table()` / `ingest_moses_netcdf()` /
   `pe_sine.R` — requires schema changes first (add `PE` to `DATA_TYPE_CODES`)
3. **Silver/Gold read functions** — `read_silver()` / `read_gold()` — Arrow
   lazy reads, S7 return objects, `qc_flag` filtering
4. **CAMELS-GB signatures** — `signatures.R` / `camels.R` for catchment attributes

---

## Git Workflow

- Development branch: `claude/suggest-features-OHAsh`
- Push with: `git push -u origin claude/suggest-features-OHAsh`
- PRs target `main`
