# reach.io

**Hydrometric data pipeline tools for the Environment Agency Hydrology API.**

reach.io is an R package that covers the full journey from raw EA data sources
through to partitioned Parquet Bronze storage. It provides two groups of tools:

- **API download tools** - query and download rainfall, flow, and water level
  data directly from the EA Hydrology API
- **Pipeline tools** - build a gauge registry, route fetches to the correct
  source system, ingest historical bulk files, and orchestrate large parallelised
  backfills with per-gauge logging

-----

## Installation

```r
# Install dependencies
install.packages(c("httr", "data.table", "arrow",
                   "future", "future.apply", "lubridate"))

# Install reach.io from source
devtools::install("path/to/reach.io")
```

-----

## Package overview

```
reach.io/
├── R/
│   ├── package.R    # constants, %||% operator
│   ├── lookup.R     # find_stations(), get_measures()
│   ├── download.R   # download_hydrology()
│   ├── sync.R       # fetch_readings(), make_date_chunks(), run_sync()
│   ├── batch.R      # submit_batch(), poll_batch(), run_batch()
│   ├── output.R     # handle_output()
│   ├── registry.R   # build_gauge_registry()
│   ├── router.R     # fetch_from_hde/wiski/bulk_file(), route_gauge()
│   ├── ingest.R     # ingest_bulk_file()
│   └── backfill.R   # run_backfill()
└── tests/
    ├── test-core.R      # API download tool tests
    └── test-pipeline.R  # pipeline tool tests
```

-----

## Part 1 - API download tools

These tools query the [EA Hydrology API](https://environment.data.gov.uk/hydrology/doc/reference)
directly. No registration or API key is required. Data is available under the
Open Government Licence v3.0.

### Step 1 - Find stations

Look up stations by WISKI ID, RLOIid, notation, name, or proximity. Multiple
lookup types can be combined in one call - results are pooled and de-duplicated.

```r
library(reach.io)

# By WISKI ID
stns <- find_stations(wiski_ids = c("SS92F014", "S11512_FW"))

# By River Levels on the Internet ID
stns <- find_stations(rloi_ids = c("5022", "7001"))

# By station notation (SUID)
stns <- find_stations(notations = "c46d1245-e34a-4ea9-8c4c-410357e80e15")

# Fuzzy name search
stns <- find_stations(names = "Avon")

# Within 10 km of a point
stns <- find_stations(lat = 51.5, long = -1.8, dist = 10)

# Combined - results from all methods are pooled
stns <- find_stations(names = "Exe", wiski_ids = "SS92F014")
```

`find_stations()` returns a `data.table` with columns `label`, `notation`,
`wiskiID`, `RLOIid`, `lat`, `long`, and `riverName`. The `wiskiID` column
slots directly into `download_hydrology()`.

### Step 2 - Inspect available measures (optional)

```r
# All daily mean flow measures
all_flow <- get_measures("flow")

# See what's available for your stations before downloading
all_flow[`station.wiskiID` %in% c("SS92F014", "S11512_FW")]

# 15-minute rainfall measures
get_measures("rainfall", period_name = "15min")

# All level statistics - min, max, instantaneous
get_measures("level", value_type = "all")
```

### Step 3 - Download readings

`download_hydrology()` is the main entry point. Supply at least one station
identifier and a date range. It resolves station identifiers, fetches the
matching measures, and downloads readings.

**Two download methods:**

|Method            |Best for                                                                          |
|------------------|----------------------------------------------------------------------------------|
|`"sync"` (default)|Short date ranges, small station sets. Fetches directly, chunked annually.        |
|`"batch"`         |Large bulk downloads. Submits jobs to the EA batch queue and downloads S3 results.|

**Two output modes:**

|Output              |Returns                                                                       |
|--------------------|------------------------------------------------------------------------------|
|`"memory"` (default)|Named list of `data.table` objects, one per parameter, for immediate use in R.|
|`"disk"`            |One CSV per measure written to `out_dir/<parameter>/`.                        |

```r
# In-memory download by WISKI ID - returns result$flow, result$level, result$summary
result <- download_hydrology(
  parameters = c("flow", "level"),
  from_date  = "2020-01-01",
  to_date    = "2022-12-31",
  wiski_ids  = c("SS92F014", "S11512_FW")
)

result$flow
result$level
result$summary

# By RLOIid, to disk
download_hydrology(
  parameters = "flow",
  from_date  = "2020-01-01",
  to_date    = "2022-12-31",
  output     = "disk",
  out_dir    = "data/hydrology",
  rloi_ids   = c("5022", "7001")
)

# Batch method - better for many stations or long date ranges
download_hydrology(
  parameters = c("rainfall", "flow", "level"),
  from_date  = "2000-01-01",
  to_date    = "2023-12-31",
  method     = "batch",
  output     = "disk",
  out_dir    = "data/hydrology",
  wiski_ids  = stns$wiskiID
)

# Qualified data only, 15-minute resolution
download_hydrology(
  parameters       = "flow",
  from_date        = "2022-01-01",
  to_date          = "2022-12-31",
  period_name      = "15min",
  observation_type = "Qualified",
  wiski_ids        = "SS92F014"
)
```

**Disk output structure:**

```
data/hydrology/
├── flow/
│   ├── SS92F014-flow-m-86400-m3s-qualified.csv
│   └── S11512_FW-flow-m-86400-m3s-qualified.csv
├── rainfall/
│   └── ...
└── download_summary.csv
```

#### EA API fair-use note

The EA asks that automated users issue one request at a time. Both download
methods in reach.io are intentionally sequential. The batch method is preferred
for large jobs as it queues requests server-side and avoids timeouts.

-----

## Part 2 - Pipeline tools

These tools support a medallion-style Bronze storage architecture. Output is
written as Hive-partitioned Parquet (`gauge_id=<id>/`) compatible with both
`arrow::open_dataset()` and Spark partition discovery.

### Workflow overview

```
ea_gauge_list.csv
       │
       ▼
build_gauge_registry()   ← one-off setup
       │
       ▼
gauge_registry.parquet   ← single source of truth for all pipeline tools
       │
       ├── route_gauge() ──► fetch_from_hde()       ┐
       │                ──► fetch_from_wiski()      ├── returns data.table
       │                ──► fetch_from_bulk_file()  ┘
       │
       └── run_backfill()   ← parallelised, logged, resumable
               │
               ▼
       data/fw_bronze/
       └── gauge_id=39001/
           └── backfill_20240101.parquet
```

### 1. Build the gauge registry

Run once before any backfill. Reads the raw EA gauge list CSV, validates it,
and writes a Parquet registry that all other tools read from.

**Required columns in the input CSV:**

|Column         |Description                       |
|---------------|----------------------------------|
|`gauge_id`     |Unique gauge identifier           |
|`source_system`|One of `HDE`, `WISKI`, `BULK_FILE`|
|`data_type`    |e.g. `flow`, `level`, `rainfall`  |
|`catchment`    |Catchment name                    |
|`ea_site_ref`  |EA site reference                 |

```r
registry_dt <- build_gauge_registry(
  input_csv   = "data/ea_gauge_list.csv",
  output_path = "data/fw_bronze/gauge_registry"
)
```

The registry gains four metadata columns: `active`, `date_added`,
`backfill_done`, and `notes`.

### 2. Ingest historical bulk files

For gauges with `source_system = "BULK_FILE"`. Reads an EA bulk export,
standardises column names, parses dates, and writes partitioned Bronze Parquet.

Handles varying EA column naming conventions automatically:

|Input column name(s)                   |Mapped to |
|---------------------------------------|----------|
|`Date`, `date`, `timestamp`, `DateTime`|`datetime`|
|`Value`, `Measurement`                 |`value`   |
|`Quality`, `QualityCode`               |`flag`    |

```r
ingest_bulk_file(
  file_path   = "data/raw/EA_39001_flow_2000_2020.csv",
  gauge_id    = "39001",
  output_dir  = "data/fw_bronze/flow",
  file_format = "csv"   # also supports "tsv" and "fixed"
)
```

Output: `data/fw_bronze/flow/gauge_id=39001/bulk_20240101.parquet`

### 3. Route a single gauge (optional / development use)

`route_gauge()` dispatches a one-row registry entry to the correct
`fetch_from_*` function. Useful for testing individual gauges before running
a full backfill.

```r
registry_dt <- as.data.table(
  arrow::read_parquet("data/fw_bronze/gauge_registry/gauge_registry.parquet")
)

data_dt <- route_gauge(
  registry_dt[gauge_id == "39001"],
  start_date = "2020-01-01",
  end_date   = "2020-12-31"
)
```

**Adding a new source system:**

1. Write a `fetch_from_<name>()` function following the same signature as the
   existing stubs - it must return a `data.table` with columns `gauge_id`,
   `datetime`, `value`, `unit`, `flag`
1. Add a case to the `switch()` in `route_gauge()`
1. Add the new name to `VALID_SOURCES` in `R/package.R`
1. Re-run `build_gauge_registry()` on the updated gauge list CSV

### 4. Run a parallelised backfill

Distributes all active registry gauges across CPU cores. Writes per-gauge
Parquet to the Bronze output directory and a result log CSV with status,
row count, elapsed time, and error message for every gauge.

```r
# Full backfill
log_dt <- run_backfill(
  registry_path = "data/fw_bronze/gauge_registry/gauge_registry.parquet",
  output_dir    = "data/fw_bronze/flow",
  start_date    = "2000-01-01",
  end_date      = "2024-12-31",
  log_path      = "logs/backfill_log.csv"
)

# Re-run only the gauges that failed in the previous run
log_dt <- run_backfill(
  registry_path = "data/fw_bronze/gauge_registry/gauge_registry.parquet",
  output_dir    = "data/fw_bronze/flow",
  start_date    = "2000-01-01",
  end_date      = "2024-12-31",
  log_path      = "logs/backfill_log.csv",
  failed_only   = TRUE
)
```

The log CSV has columns `gauge_id`, `status`, `rows`, `elapsed_s`, `error`.
Status values are `SUCCESS`, `EMPTY` (fetch returned no rows), or `FAILED`.

A warning is raised if more than 5% of gauges fail, as this typically indicates
a systemic issue (API outage, auth change, schema change) rather than isolated
bad gauges.

**Reading the Bronze output with Arrow:**

```r
library(arrow)
library(data.table)

ds <- open_dataset("data/fw_bronze/flow")

# Lazy filter - only reads the relevant partition files
flow_dt <- as.data.table(
  ds |>
    filter(gauge_id == "39001") |>
    collect()
)
```

-----

## Dependencies

|Package       |Used for                                         |
|--------------|-------------------------------------------------|
|`httr`        |EA API HTTP requests                             |
|`data.table`  |All in-memory data manipulation and CSV I/O      |
|`arrow`       |Parquet read/write                               |
|`future`      |Parallel worker setup                            |
|`future.apply`|`future_lapply()` for parallelised backfill      |
|`lubridate`   |Multi-format datetime parsing in bulk file ingest|
|`parallel`    |`detectCores()` default for `n_workers`          |

-----

## Running tests

```r
devtools::test("path/to/reach.io")
```

Tests cover validation logic and file I/O for all pipeline tools and the core
API utilities. No live API calls are made during testing.

-----

## Licence

MIT. Data downloaded via the EA Hydrology API is available under the
[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
