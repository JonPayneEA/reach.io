# reach.io

<img src="man/figures/logo.svg" align="right" width="160"/>

**Hydrometric data pipeline tools for the EA Hydrology API and WISKI.**

reach.io is an R package for the Flood Forecast Modelling team. It covers
the full journey from raw data sources to Bronze-tier Parquet storage,
following the [Hydrometric Data Framework v1.3](docs/2c_Hydrometric_Data_v1_3.docx)
and the [R Tool Governance standards](docs/2a_R_Code_Governance_v1_3.docx).

It provides three groups of tools:

- **API download tools** - query and download rainfall, flow, and level data
  from the EA Hydrology API, returned as typed S7 objects
- **Schema and provenance tools** - generate dataset IDs, apply the Bronze
  Parquet schema, and write provenance records to the Hydrometric Data Register
- **Pipeline tools** - set up the store, build a gauge registry, ingest
  historical data from multiple source systems, and run parallelised backfills
  and incremental syncs

---

## Installation

```r
# Install dependencies
install.packages(c("httr", "data.table", "arrow",
                   "future", "future.apply", "lubridate", "S7"))

# Install reach.io from source
devtools::install("path/to/reach.io")

# Restore exact dependency versions from the lockfile
renv::restore()
```

---

## Package overview

```
reach.io/
├── R/
│   ├── package.R      # constants, PARAMETER_CONFIG, VALID_SOURCES, VALID_CATEGORIES
│   ├── schema.R       # Bronze schema, dataset IDs, supplier codes, provenance
│   ├── setup.R        # setup_hydro_store()
│   ├── classes.R      # S7 HydroData classes (Rainfall_Daily etc.)
│   ├── lookup.R       # find_stations(), get_measures()
│   ├── download.R     # download_hydrology()
│   ├── sync.R         # fetch_readings(), make_date_chunks(), run_sync()
│   ├── batch.R        # submit_batch(), poll_batch(), run_batch()
│   ├── output.R       # handle_output()
│   ├── registry.R     # build_gauge_registry()
│   ├── router.R       # fetch_from_hde/wiski/wiski_all(), route_gauge()
│   ├── ingest_all.R   # ingest_all_file(), fetch_from_wiski_all()
│   ├── backfill.R     # run_backfill()
│   └── incremental.R  # run_incremental()
└── tests/
    ├── test-core.R        # API download tool tests
    ├── test-pipeline.R    # pipeline tool tests
    └── test-incremental.R # incremental sync tests
```

---

## Part 1 - API download tools

These tools query the [EA Hydrology API](https://environment.data.gov.uk/hydrology/doc/reference)
directly. No registration or API key is required. Data is available under the
Open Government Licence v3.0.

### Step 1 - Find stations

Look up stations by WISKI ID, RLOIid, notation, name, or proximity. Results
are returned as one row per measure, with the measure notation, parameter,
value type, and period already parsed.

```r
library(reach.io)

# By WISKI ID
stns <- find_stations(wiski_ids = c("SS92F014", "S11512_FW"))

# By River Levels on the Internet ID
stns <- find_stations(rloi_ids = c("5022", "7001"))

# Fuzzy name search
stns <- find_stations(names = "Avon")

# Within 10 km of a point
stns <- find_stations(lat = 51.5, long = -1.8, dist = 10)
```

`find_stations()` returns a `data.table` with columns `label`, `notation`,
`wiskiID`, `RLOIid`, `lat`, `long`, `easting`, `northing`, `riverName`,
`station.notation`, `parameter`, `value_type`, and `period`. One row per
measure - a station with flow and level measures appears twice.

```r
# Unnest to see all measures across returned stations
stns[, .(label, wiskiID, parameter, value_type, period)]
```

### Step 2 - Inspect available measures (optional)

`find_stations()` returns measure notations directly, so `get_measures()` is
only needed when you want to browse what is available across all stations
for a parameter before deciding which ones to download.

```r
# All 15-minute instantaneous flow measures
get_measures("flow")

# All rainfall measures
get_measures("rainfall", value_type = "all")
```

### Step 3 - Download readings

`download_hydrology()` is the main entry point. Supply at least one station
identifier and a date range.

**Two download methods:**

| Method | Best for |
|--------|----------|
| `"sync"` (default) | Short date ranges, small station sets. Chunked annually. |
| `"batch"` | Large bulk downloads. Submits to the EA batch queue. |

**Two output modes:**

| Output | Returns |
|--------|---------|
| `"memory"` (default) | Named list of typed S7 objects (`Flow_15min` etc.) plus a summary. |
| `"disk"` | One CSV per measure under `out_dir/<parameter>/`. |

```r
# In-memory - returns typed S7 objects
result <- download_hydrology(
  parameters = c("flow", "level"),
  from_date  = "2022-01-01",
  to_date    = "2022-12-31",
  wiski_ids  = c("SS92F014", "S11512_FW")
)

result$flow              # <Flow_15min> object
result$flow@readings     # the readings data.table
as_data_table(result$flow)   # same, via generic
as_long(result$flow)         # adds a parameter column
result$summary

# To disk
download_hydrology(
  parameters = "flow",
  from_date  = "2022-01-01",
  to_date    = "2022-12-31",
  output     = "disk",
  out_dir    = "data/downloads",
  rloi_ids   = c("5022", "7001")
)

# Batch method for long date ranges
download_hydrology(
  parameters = c("rainfall", "flow", "level"),
  from_date  = "2000-01-01",
  to_date    = "2023-12-31",
  method     = "batch",
  output     = "disk",
  out_dir    = "data/downloads",
  wiski_ids  = stns$wiskiID
)
```

**S7 return types by parameter and period:**

| Parameter | Period | Class |
|-----------|--------|-------|
| `flow` | `15min` | `Flow_15min` |
| `flow` | `daily` | `Flow_Daily` |
| `level` | `15min` | `Level_15min` |
| `level` | `daily` | `Level_Daily` |
| `rainfall` | `15min` | `Rainfall_15min` |
| `rainfall` | `daily` | `Rainfall_Daily` |

All six inherit from the abstract `HydroData` class. Downstream code can
dispatch on the class name to handle daily and sub-daily data differently.

#### Working with S7 objects

Data and metadata are stored in slots accessed with `@`:

```r
obj <- result$flow   # a Flow_15min object

# ── Readings ──────────────────────────────────────────────────────────────────
obj@readings            # data.table of all readings
obj@readings$dateTime   # POSIXct timestamps
obj@readings$value      # numeric values
obj@readings$measure_notation  # EA measure ID string

# ── Metadata ──────────────────────────────────────────────────────────────────
obj@parameter    # "flow"
obj@period_name  # "15min"
obj@from_date    # "2022-01-01"
obj@to_date      # "2022-12-31"
obj@n_rows       # integer row count
obj@downloaded_at
```

Two helper generics avoid touching `@readings` directly:

```r
as_data_table(obj)          # returns readings as a plain data.table
as_long(obj)                # same, with a leading `parameter` column
                            # useful for rbind() across multiple objects
```

Common patterns:

```r
# Filter to a date range
obj@readings[date >= as.Date("2022-06-01")]

# Quick plot
with(obj@readings, plot(dateTime, value, type = "l"))

# Export to model input formats
format_for_pdm(obj@readings, measure = "flow")
format_for_fmp(list(MyGauge = obj), gauge_ids = "SS92F014")

# Add hydrological year columns in-place
add_hydro_year(obj@readings)
obj@readings[, .(hydro_year, value)]
```

#### EA API fair-use note

The EA asks that automated users issue one request at a time. Both download
methods are intentionally sequential. The batch method is preferred for large
jobs as it queues requests server-side and avoids timeouts.

---

## Part 2 - Schema and provenance tools

These tools implement the Bronze tier requirements from the Hydrometric Data
Framework. They are called internally by the pipeline tools but are also
exported for direct use.

### Dataset IDs

Every Bronze dataset gets a unique ID in the format
`[SupplierCode]_[SiteID]_[DataType]_[YYYYMMDD]` (Appendix A of the framework):

```r
make_dataset_id("EA", "39001", "Q")
# "EA_39001_Q_20260318"

# Map from registry values to framework codes
source_to_supplier("HDE")      # "EA"
source_to_supplier("WISKI")    # "EA"
source_to_supplier("NRFA")     # "CEH"
param_to_data_type("flow")     # "Q"
param_to_data_type("level")    # "H"
param_to_data_type("rainfall") # "P"
```

### Bronze Parquet schema

The mandatory Bronze schema (Section 7.2 of the framework) has six columns:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | POSIXct UTC | Observation datetime |
| `value` | float64 | Observed value as received |
| `supplier_flag` | character | Supplier quality code, or NA |
| `dataset_id` | character | Join key to the register |
| `site_id` | character | Supplier site identifier |
| `data_type` | character | Q, H, P, SM, or SWE |

```r
bronze_dt <- apply_bronze_schema(
  dt                = raw_dt,
  dataset_id        = "EA_39001_Q_20260115",
  site_id           = "39001",
  data_type         = "Q",
  timestamp_col     = "dateTime",
  value_col         = "value",
  supplier_flag_col = "quality"
)
```

### Bronze file path

```r
bronze_path("data/hydrometric", "hydrometric", "EA", "Q", "EA_39001_Q_20260115")
# "data/hydrometric/bronze/hydrometric/EA/Q/2026/EA_39001_Q_20260115.parquet"
```

### Provenance records

Every Bronze ingest appends a row to the Hydrometric Data Register CSV
(Section 4.2 of the framework):

```r
write_provenance_record(
  register_path       = "data/hydrometric/register/register.csv",
  dataset_id          = "EA_39001_Q_20260115",
  supplier            = "Environment Agency",
  supplier_code       = "EA",
  site_id             = "39001",
  data_type           = "Q",
  time_period_start   = "2020-01-01",
  time_period_end     = "2024-12-31",
  temporal_resolution = "15min",
  method_of_receipt   = "API pull",
  file_path           = "data/hydrometric/bronze/hydrometric/EA/Q/2026/EA_39001_Q_20260115.parquet"
)
```

---

## Part 3 - Pipeline tools

The pipeline tools move data from raw sources into Bronze-tier Parquet storage,
following the framework medallion structure. The pipeline tools call the schema
and provenance tools internally.

### Workflow overview

```
setup_hydro_store()      ← one-off: create directory tree
       │
       ▼
ea_gauge_list.csv
       │
       ▼
build_gauge_registry()   ← one-off: validate and write registry
       │
       ▼
gauge_registry.parquet   ← single source of truth
       │
       ├── Set env vars for source systems
       │
       ├── [optional] ingest_all_file()    ← WISKI_ALL .all files
       │
       ├── route_gauge() ──► fetch_from_hde()
       │                ──► fetch_from_wiski()   (draft — API not yet available)
       │                ──► fetch_from_wiski_all()
       │
       ├── run_backfill()    ← one-off historical load
       └── run_incremental() ← ongoing sync
               │
               ▼
data/hydrometric/
├── bronze/
│   ├── hydrometric/EA/Q/2024/EA_39001_Q_20240101.parquet
│   ├── radarH19/MO/P/2024/MO_235096_P_20240101.parquet
│   └── MOSES/MO/SM/2024/MO_UK_SM_20240101.parquet
├── silver/   ← QC and flagging (outside reach.io scope)
├── gold/     ← approved products (outside reach.io scope)
└── register/
    └── register.csv
```

### 1. Set up the data store

Run once to create the directory tree. Safe to re-run on an existing store.

```r
setup_hydro_store("data/hydrometric")
```

This creates the full structure for the three default categories:

```
data/hydrometric/
├── bronze/
│   ├── hydrometric/EA/Q/  EA/H/  EA/P/  WISKI/Q/  WISKI/H/  NRFA/Q/
│   ├── radarH19/MO/P/
│   └── MOSES/MO/SM/
├── silver/
│   ├── hydrometric/Q/  H/  P/
│   ├── radarH19/P/
│   └── MOSES/SM/
├── gold/
│   ├── hydrometric/Q/calibration/  Q/FFA/  P/catchment_average/
│   ├── radarH19/P/
│   └── MOSES/SM/
└── register/
```

Custom setup:

```r
setup_hydro_store(
  "data/hydrometric",
  categories = list(
    hydrometric = list(suppliers = c("EA", "WISKI"), data_types = c("Q", "H"))
  )
)
```

### 2. Build the gauge registry

Run once. Reads the raw gauge list CSV, validates it, and writes
`gauge_registry.parquet`.

**Required columns in the input CSV:**

| Column | Description |
|--------|-------------|
| `gauge_id` | Unique gauge identifier |
| `source_system` | One of `HDE`, `WISKI`, `WISKI_ALL` |
| `data_type` | `flow`, `level`, or `rainfall` |
| `category` | One of `hydrometric`, `radarH19`, `MOSES` |
| `catchment` | Catchment name |
| `ea_site_ref` | EA site reference |

```r
build_gauge_registry(
  input_csv   = "data/ea_gauge_list.csv",
  output_path = "data/hydrometric/register"
)
```

The registry gains metadata columns: `active`, `live`, `date_added`,
`backfill_done`, and `notes`. Set `live = FALSE` in your CSV for gauges that
are historical-only and should not be included in incremental syncs. Pass
`overwrite = FALSE` to merge new gauges into an existing registry without
losing metadata for gauges already registered.

### 3. Configure source system connections

Set environment variables before running any backfill or sync.

**HDE** - EA Hydrology API, no config needed.

**WISKI** - KiWIS REST API:

```r
Sys.setenv(WISKI_BASE_URL = "https://your-wiski-instance/KiWIS/KiWIS")
Sys.setenv(WISKI_API_KEY  = "your-key")  # omit if unauthenticated
```

**WISKI_ALL** - Kisters `.all` export files:

```r
Sys.setenv(WISKI_ALL_ROOT = "/mnt/wiski/exports")
```

### 4. Ingest WISKI .all export files

WISKI `.all` files are the default bulk import method for EA WISKI data.
Because the content of a `.all` file is not known until it is parsed,
the gauge list CSV approach does not apply. Instead, pass `registry_path`
to `ingest_all_file()` and every discovered station is auto-registered
into the gauge registry with `source_system = "WISKI_ALL"`,
`live = FALSE`, and `backfill_done = TRUE`.

```r
# Ingest and auto-register discovered stations
ingest_all_file(
  path          = "data/wiski/2008_2010.all",
  output_dir    = "data/hydrometric",
  category      = "hydrometric",
  registry_path = "data/hydrometric/register/gauge_registry.parquet"
)

# Multiple files covering different periods
for (f in list.files("data/wiski", pattern = "\\.all$", full.names = TRUE)) {
  ingest_all_file(
    f,
    output_dir    = "data/hydrometric",
    category      = "hydrometric",
    registry_path = "data/hydrometric/register/gauge_registry.parquet"
  )
}
```

`.all` files are processed with a two-pass approach: pass 1 parses each
station block in chunks (default 50,000 lines) to keep memory flat; pass 2
merges, deduplicates on timestamp, and writes Bronze Parquet. Files of 10GB+
are handled without loading the full file into memory.

### 5. Route a single gauge (optional / development use)

`route_gauge()` dispatches a one-row registry entry to the correct
`fetch_from_*` function. Useful for checking individual gauges before a
full backfill.

```r
registry_dt <- data.table::as.data.table(
  arrow::read_parquet("data/hydrometric/register/gauge_registry.parquet")
)

dt <- route_gauge(
  registry_dt[gauge_id == "39001"],
  start_date = "2020-01-01",
  end_date   = "2020-12-31"
)
```

Output follows the Bronze schema: `timestamp`, `value`, `supplier_flag`,
`dataset_id`, `site_id`, `data_type`.

**Adding a new source system:**
1. Write a `fetch_from_<n>()` function returning the Bronze schema
2. Add a case to `switch()` in `route_gauge()`
3. Add the name to `VALID_SOURCES` in `R/package.R`
4. Re-run `build_gauge_registry()` on the updated CSV

### 6. Run a parallelised backfill

Distributes all active registry gauges across CPU cores. Writes Bronze Parquet
to the framework path and appends a provenance record to the register for each
gauge. A result log CSV records status, row count, elapsed time, and any error
for every gauge.

```r
# Full backfill
log_dt <- run_backfill(
  registry_path = "data/hydrometric/register/gauge_registry.parquet",
  output_dir    = "data/hydrometric",
  register_path = "data/hydrometric/register/register.csv",
  start_date    = "2000-01-01",
  end_date      = "2024-12-31",
  log_path      = "logs/backfill_log.csv"
)

# Re-run failed gauges only
log_dt <- run_backfill(
  registry_path = "data/hydrometric/register/gauge_registry.parquet",
  output_dir    = "data/hydrometric",
  register_path = "data/hydrometric/register/register.csv",
  start_date    = "2000-01-01",
  end_date      = "2024-12-31",
  log_path      = "logs/backfill_log.csv",
  failed_only   = TRUE
)
```

Log columns: `gauge_id`, `status`, `rows`, `elapsed_s`, `error`. Status values
are `SUCCESS`, `EMPTY`, or `FAILED`. A steward warning is raised if more than
5% of gauges fail.

**Reading Bronze output:**

```r
# Read a dataset
dt <- data.table::as.data.table(
  arrow::collect(
    arrow::open_dataset(
      "data/hydrometric/bronze/hydrometric/EA/Q/2024"
    )
  )
)

# Deduplicate on timestamp (covers any overlap between backfill and
# incremental files)
data.table::setkeyv(dt, c("dataset_id", "timestamp"))
dt <- unique(dt, by = c("dataset_id", "timestamp"))
```

### 7. Run incremental syncs

After the initial backfill, `run_incremental()` fetches only new data for
each gauge. It finds the high watermark (latest `timestamp`) in the existing
Bronze files and fetches from that point forward.

New data is written as a separate dated file. Deduplication across files is
handled at read time with `data.table::unique()`.

```r
# Sync all active gauges to today
log_dt <- run_incremental(
  registry_path = "data/hydrometric/register/gauge_registry.parquet",
  output_dir    = "data/hydrometric",
  log_path      = "logs/incremental_log.csv"
)

# Sync a subset of gauges
log_dt <- run_incremental(
  registry_path = "data/hydrometric/register/gauge_registry.parquet",
  output_dir    = "data/hydrometric",
  gauge_ids     = c("39001", "39002"),
  log_path      = "logs/incremental_log.csv"
)
```

Log columns: `run_at`, `gauge_id`, `watermark`, `end_date`, `status`, `rows`,
`elapsed_s`, `error`. Status values: `SUCCESS`, `EMPTY`, `NO_NEW_DATA`, or
`FAILED`. Each run appends to the log - the full sync history is preserved.

Gauges with no existing Bronze files fall back to `default_start`
(default `"2000-01-01"`), so `run_incremental()` is safe to run on a
partially backfilled registry.

---

## Medallion framework scope

reach.io covers the Bronze tier only - raw ingestion from source systems with
full provenance labelling. Silver (QC flagging, gap assessment, rating curve
application) and Gold (approved analysis-ready products) are outside the
current scope and require Custodian and Steward review steps that cannot be
automated.

| Tier | Scope | In reach.io |
|------|-------|-------------|
| Bronze | Raw, immutable, provenance-labelled | Yes |
| Silver | QC flagged, gap-assessed, corrected | No |
| Gold | Formally approved analysis-ready products | No |

---

## Dependencies

| Package | Used for |
|---------|----------|
| `data.table` | All in-memory data manipulation and CSV I/O |
| `arrow` | Parquet read/write |
| `httr` | EA API and KiWIS HTTP requests |
| `future` | Parallel worker setup |
| `future.apply` | `future_lapply()` for backfill and incremental sync |
| `lubridate` | Multi-format datetime parsing |
| `parallel` | `detectCores()` default for `n_workers` |
| `S7` | Typed classes for downloaded hydrometric data |

Dependency versions are pinned in `renv.lock`. Run `renv::restore()` to
install the exact versions used during development.

---

## Running tests

```r
devtools::test("path/to/reach.io")
```

Tests cover validation logic, file I/O, Bronze schema conformance, and
incremental sync behaviour. No live API calls are made. Source system fetch
functions are mocked using `testthat::local_mocked_bindings()`.

---

## Licence

MIT. 

Data downloaded via the EA Hydrology API is available under the
[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

---

## Governance

`reach.io` is classified under the F&W Data and Digital Asset Governance Framework v1.2.
All ingested data follows the **Medallion Architecture** (Bronze → Silver → Gold).

- [Hydrometric Data Framework reference](inst/data-framework.md) - Bronze schema, dataset ID format, quality flags, storage structure
- [flode Governance](https://github.com/JonPayneEA/flode/blob/master/GOVERNANCE.md) - roles, tier classification, branching, versioning
- [Contributing](https://github.com/JonPayneEA/flode/blob/master/CONTRIBUTING.md) - coding standards, fastverse conventions, PR process

