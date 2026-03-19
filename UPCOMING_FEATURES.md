# Upcoming Features

This document tracks planned additions to **reach.io**, grouped by tier and priority.

---

## High Priority

### Silver Tier Processing
> `silver.R` 🚧 **In Development** — skeleton implementation, not for operational use

The Medallion Architecture is defined but only the Bronze tier is currently implemented. Silver tier processing introduces:

- `promote_to_silver()` — Quality-controlled promotion of Bronze Parquet data
- `silver_path()` — Framework-compliant Silver file path builder
- QC flagging: negative values, relative spikes, absolute spikes, sudden drops,
  recurrent fluctuations, low- and high-flow truncations
- Unit normalisation (e.g. stage → flow via rating curves) — planned
- Gap detection and annotation — planned
- Deduplication baked into the promotion step (currently deferred to read time) — planned

#### QC flag definitions (Silver schema)

| Flag | Meaning | Assigned by |
|------|---------|-------------|
| 1 — Good | No anomaly detected | `promote_to_silver()` |
| 2 — Estimated | Manually corrected or gap-filled value | Manual / downstream |
| 3 — Suspect | Anomaly detected; value may still be plausible | `promote_to_silver()` |
| 4 — Rejected | Value almost certainly erroneous | `promote_to_silver()` |

---

#### Option A — Internal QC check suite *(current implementation)*

A set of automated checks applied per gauge, parameterised by data type.
Checks are composable and threshold values are overridable.

##### Flow (Q)

| Check | Logic | Y-code | QC Flag |
|-------|-------|--------|---------|
| Negative value | `value < 0` where tidal influence absent | 1 | 4 (Rejected) |
| Relative spike | `(value − lag) / lag > ratio` where `lag > min_baseline` | 2 | 3 (Suspect) |
| Absolute spike | `\|diff\| > k × MAD(diff series)` where `value > min_spike_flow` | 3 | 3 (Suspect) |
| Drop | `(lag − value) / lag > ratio` where `lag > min_flow_for_drop` | 4 | 3 (Suspect) |
| Fluctuation | ≥ N direction reversals in rolling window of M steps | 5 | 3 (Suspect) |
| Truncated low | Run of ≥ N identical values at or below Q10 of series | 6 | 3 (Suspect) |
| Truncated high | Run of ≥ N identical values at or above Q90 of series | 7 | 3 (Suspect) |
| Rel + abs spike | Codes 2 and 3 coincide | 8 | 4 (Rejected) |
| Drop + neg/trunc | Code 4 coincides with code 1 or 6 | 9 | 4 (Rejected) |

> **Note on `allow_negative`:** Negative flow is physically valid in some
> settings (ultrasonic gauges, tidal reaches). `promote_to_silver()` accepts
> `allow_negative = TRUE` to suppress the negative-value check; tests cover
> both cases.

The Y-digit code scheme and priority rules follow the UK-Flow15 framework
(see Option B below). Only the traditional Y-digit checks are automated;
the X-digit (NRFA cross-validation) and Z-digit (high-flow plausibility)
require external reference data and are reserved for manual or
downstream annotation.

##### Stage / Level (H) — planned

| Test | Logic | QC Flag |
|------|-------|---------|
| Plausible range | Outside `[min_datum, max_credible]` for that station | 3 (Suspect) |
| Spike detection | Sudden large jump then return (debris, ice, sensor contact) | 3 (Suspect) |
| Rate of change | Rivers rise and fall at physically bounded rates | 3 (Suspect) |
| Flat-lining | Identical value over extended period (frozen/stuck sensor) | 3 (Suspect) |
| Stage–flow consistency | Level rising while flow drops (or vice versa) sustained | 3 (Suspect) |
| Datum shift detection | Persistent step-change offset (sensor moved or recalibrated) | 3 (Suspect) / 2 (Estimated) |

##### Rainfall (P) — planned

| Test | Logic | QC Flag |
|------|-------|---------|
| Non-negative | `value < 0` (reset or overflow artifact) | 4 (Rejected) or corrected |
| Intensity cap | 15-min value > credible intensity (e.g. >50 mm/15 min) | 3 (Suspect) |
| Daily accumulation cap | Rolling 24 h total > ~300 mm (UK context) | 3 (Suspect) |
| Extended dry run | Long zero sequences during known wet periods (blocked gauge) | 3 (Suspect) |
| Gauge reset handling | Negative increments from tipping bucket counter overflow | Correct or flag |
| Temporal consistency | Rain at site while all neighbouring gauges show zero | 3 (Suspect) |

##### Rating extrapolation flag — planned

Stage-derived flow values estimated beyond the valid rating curve range should
set an `extrapolated` column to `TRUE` rather than a numeric QC flag, so
callers can filter on this independently of the main flag.

---

#### Option B — UK-Flow15 three-digit XYZ code scheme *(reference framework)*

> **Reference:** Fileni, F. et al. (2026). *UK-Flow15 Part 1: Development of
> a coherent national-scale 15-min flow dataset.* Earth Syst. Sci. Data
> (preprint). https://doi.org/10.5194/essd-2026-152

The UK-Flow15 dataset uses a three-digit code `XYZ` per observation, where
each digit captures a distinct QC dimension. This scheme could replace or
supplement the binary `qc_flag` used in Option A, at the cost of a more
complex schema.

**X-digit — consistency with established UK products**

| Code | Meaning |
|------|---------|
| 0 | Sufficient agreement with all UK products |
| 1 | Mismatch >5% vs NRFA daily values |
| 2 | Mismatch >20% vs POT values |
| 3 | Mismatch >20% vs AMAX values |
| 4 | Codes 1 + 3 combined |
| 5 | Codes 1 + 2 combined |

*Requires:* NRFA daily, AMAX, and POT series per gauge. Mismatches flagged at
5% for daily and 20% for peak series. AMAX flag takes priority over POT where
both fire.

**Y-digit — traditional automated QC (implemented in Option A)**

| Code | Meaning |
|------|---------|
| 0 | No issues |
| 1 | Negative value |
| 2 | Relative spike |
| 3 | Absolute spike |
| 4 | Drop |
| 5 | Fluctuation |
| 6 | Truncated low flows |
| 7 | Truncated high flows |
| 8 | Codes 2 + 3 combined |
| 9 | Code 4 + code 1 or 6 combined |

**Z-digit — high-flow event plausibility**

| Code | Meaning |
|------|---------|
| 0 | Not a high flow |
| 1 | Unrealistically high (spurious) |
| 2 | Exceptionally high event |
| 3 | >0.1/yr event with no antecedent rainfall |
| 4 | >0.1/yr event with no concurrent regional high flow |
| 5 | Codes 3 + 4 combined |
| 6 | Validated: antecedent rainfall AND concurrent regional high flow |
| 7 | Code 2 + (code 3 or 4) |
| 8 | Code 2 + code 5 |
| 9 | Code 2 + code 6 (validated exceptionally high event) |

*Requires:* catchment-average rainfall series and concurrent flow records from
neighbouring gauges in the same hydrometric area. A fitted GEV is used to
classify events exceeding a 0.001 annual exceedance probability.

**Trade-offs vs Option A**

| Aspect | Option A (current) | Option B (XYZ) |
|--------|-------------------|----------------|
| Schema complexity | Simple: one `qc_flag` integer | Three separate digit columns |
| External data required | None | NRFA daily, AMAX, POT, rainfall, regional gauges |
| Automation | Fully automated | Y automated; X and Z need external data |
| Interoperability | reach.io native | Directly comparable to UK-Flow15 |
| Auditability | `qc_y_code` provides equivalent Y detail | Full three-digit traceability |

Option B would be the preferred approach for any national-scale or
NRFA-aligned workflow. Option A is appropriate for operational use where
external reference data are not routinely available.

---

#### Data-Type-Specific QC Tests
> `tests/testthat/test-silver.R` ✓ **Implemented** (Flow / Y-digit)

```r
test_that("promote_to_silver() adds all required Silver columns", { ... })
test_that("qc_flag is never NA after promotion", { ... })
test_that("qc_value equals value where qc_flag is Good (1)", { ... })
test_that("qc_value is NA where qc_flag is Rejected (4)", { ... })
test_that("negative flow flagged as Rejected when allow_negative = FALSE", { ... })
test_that("negative flow accepted when allow_negative = TRUE", { ... })
test_that("promote_to_silver applies checks independently per site_id", { ... })
# Planned:
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

### MOSES Potential Evapotranspiration (PE)
> `ingest_moses_pe.R` / `pe_sine.R`

MOSES is already a defined store category (`bronze/MOSES/MO/`) but PE is not yet registered as a data type — `DATA_TYPE_CODES` in `schema.R` only contains `SM` for the MOSES category. PE is fundamental to water balance modelling and is required before hydrological signatures such as `runoff_ratio` and `aridity` can be computed. Two source formats need supporting: **rectangular tables** (site-extracted or catchment-average tabular outputs) and **gridded NetCDF** (full MOSES grid). A **sine curve tool** is also needed for gap filling, ungauged catchments, and model spin-up.

#### Prerequisites (schema changes)

Before ingestion tools can be written, the following changes are needed:

- Add `pe = "PE"` to `DATA_TYPE_CODES` in `schema.R`
- Add `PE` to the MOSES entry in the default `categories` argument of `setup_hydro_store()` in `setup.R`
- This will create `bronze/MOSES/MO/PE/`, `silver/MOSES/PE/`, and `gold/MOSES/PE/` directories on store setup

#### Loading from Rectangular Tables
> `ingest_moses_table()`

MOSES PE is often extracted to tabular form — either a wide site × date matrix or a long-format CSV with one row per site per timestep.

```r
ingest_moses_table(
  path,
  format    = c("long", "wide"),   # long: site_id|date|pe; wide: date × site columns
  site_col  = "site_id",           # column name carrying site identifier (long format)
  date_col  = "date",
  pe_col    = "pe",
  units     = c("mm/day", "mm/hr", "kg/m2/s", "W/m2"),  # auto-converts to mm/day
  output_dir,
  category  = "MOSES",
  ...
)
```

Behaviours:
- Auto-detects wide vs. long if `format` omitted and column structure is unambiguous
- Converts all units to mm/day on ingest (store always holds mm/day)
- Applies Bronze schema: `timestamp`, `value`, `supplier_flag`, `dataset_id`, `site_id`, `data_type = "PE"`
- Writes one Parquet partition per site following `bronze/MOSES/MO/PE/<YYYY>/<dataset_id>.parquet`
- Logs a provenance record to the Hydrometric Data Register on completion

#### Loading from Gridded NetCDF
> `ingest_moses_netcdf()`

Full MOSES grid outputs are provided as NetCDF on the Met Office 1 km OSGB grid. Two extraction modes are needed: point extraction at gauge locations and catchment-average extraction over a polygon boundary.

```r
ingest_moses_netcdf(
  path,
  variable    = "pe",           # NetCDF variable name; common alternatives: "et", "evap", "LE"
  mode        = c("point", "catchment_avg"),
  sites       = NULL,           # data.frame with site_id, easting, northing (point mode)
  boundaries  = NULL,           # sf polygon layer with site_id column (catchment_avg mode)
  units       = c("mm/day", "kg/m2/s", "W/m2"),
  output_dir,
  ...
)
```

| Mode | Detail |
|------|--------|
| `"point"` | Bilinear interpolation to site coordinates (easting/northing OSGB or lon/lat WGS84) |
| `"catchment_avg"` | Area-weighted zonal mean over catchment polygon; respects partial cell overlap |

Behaviours:
- Handles common MOSES variable names and auto-detects units from the `units` NetCDF attribute
- Converts W m⁻² and kg m⁻² s⁻¹ to mm day⁻¹ using `λ = 2.45 MJ kg⁻¹`
- Reprojects site coordinates to match NetCDF CRS if needed (requires `sf`)
- Applies same Bronze schema and provenance logging as the table ingestor
- Validates grid against expected MOSES domain (warns if bounding box is unexpected)

#### Sine Curve Tools
> `pe_sine.R`

PE follows a strong annual cycle well approximated by a sine wave. Sine tools support gap filling, ungauged catchment estimation, model spin-up periods, and sensitivity analysis.

The standard form used throughout is:

```
PE(t) = B + A × sin(2π/365.25 × (t − φ))
```

where `B` = annual mean (mm day⁻¹), `A` = amplitude (mm day⁻¹), `φ` = phase offset (day of year of peak; ~172 for UK, i.e. ~21 June).

```r
# Fit parameters from an observed PE series
fit_pe_sine(
  pe,               # numeric vector of daily PE values
  dates,            # corresponding Date vector
  method = c("nls", "lm_fourier")  # nonlinear least squares or linearised Fourier fit
)
# Returns: list(B, A, phi, r_squared, rmse)

# Generate a synthetic PE series from fitted (or manually specified) parameters
generate_pe_sine(
  B, A, phi,
  start,            # Date
  end,              # Date
  as_s7 = TRUE      # wrap in appropriate S7 class?
)

# Regionalise sine parameters from nearby gauges (for ungauged catchments)
regionalise_pe_sine(
  gauge_ids,        # gauges to pool
  weights = NULL    # area or distance weights; equal weighting if NULL
)
```

| Function | Use case |
|----------|----------|
| `fit_pe_sine()` | Characterise seasonal PE behaviour; extract B, A, φ for a gauge |
| `generate_pe_sine()` | Synthetic PE for spin-up, ungauged sites, or scenario analysis |
| `regionalise_pe_sine()` | Pool parameters from neighbouring gauges to estimate PE at ungauged location |
| `compare_pe_sine()` | Overlay fitted curve against observed series; returns RMSE and bias |

> **Note:** The sine approximation works well for UK lowland catchments but degrades in upland or coastal sites where PE timing is shifted by elevation or sea surface temperature. `fit_pe_sine()` should return `r_squared` so the caller can assess goodness-of-fit before using the curve operationally.

---

### Silver & Gold Data Retrieval
> `read_silver.R` / `read_gold.R`

Currently there is no way to read data back out of the Silver or Gold stores. The Bronze tier has `download_hydrology()` for fetching from external APIs but nothing equivalent for reading QC'd or derived data from the local store. Retrieval functions should follow the same S7 class conventions as the rest of the package and support efficient lazy reads via Arrow for large date ranges.

#### Silver Retrieval

```r
read_silver(
  gauge_id,
  data_type,
  start     = NULL,   # Date or NULL (read all)
  end       = NULL,
  min_quality = 1L,   # Include flags 1 (Good) and better; set NULL for all
  cols      = NULL    # Column subset; NULL returns all Silver columns
)
```

Key behaviours:
- Returns the appropriate S7 class (`Flow_15min`, `Level_Daily`, etc.) consistent with Bronze reads
- `min_quality` maps to `qc_flag` thresholds: `1` = Good only, `2` = include Estimated, `3` = include Suspect, `NULL` = all flags including Rejected
- Uses Arrow lazy evaluation — only materialises the subset matching the date and quality filter
- Raises a clear error if no Silver data exists for the gauge (rather than silently returning empty)
- Falls back to `qc_value` rather than `value` as the primary reading column

| Function | Purpose |
|----------|---------|
| `read_silver(gauge_id, data_type, ...)` | Single gauge, single data type |
| `read_silver_multi(gauge_ids, data_type, ...)` | Multiple gauges, returns named list of S7 objects |
| `read_silver_catchment(catchment_id, data_type, ...)` | All gauges in a catchment by registry lookup |
| `list_silver_gauges(data_type = NULL)` | List all gauges present in the Silver store |

#### Gold Retrieval

Gold tier holds derived and aggregated outputs (daily aggregations, catchment means, modelled series). Read functions should reflect the different structure — Gold data is typically aggregated and may span multiple source gauges.

```r
read_gold(
  gauge_id,
  data_type,
  start   = NULL,
  end     = NULL,
  period  = c("15min", "hourly", "daily"),  # Aggregation level
  cols    = NULL
)
```

| Function | Purpose |
|----------|---------|
| `read_gold(gauge_id, data_type, ...)` | Single gauge Gold series |
| `read_gold_multi(gauge_ids, data_type, ...)` | Multiple gauges, returns named list |
| `read_gold_catchment(catchment_id, ...)` | All Gold outputs for a catchment |
| `list_gold_products(gauge_id)` | List available Gold products (data types + periods) for a gauge |

#### Design Considerations

- **Consistency with Bronze** — return values should be S7 objects so downstream code works unchanged regardless of which tier data comes from
- **qc_flag transparency** — returned objects should always carry `qc_flag` so callers can apply their own thresholds if needed
- **Arrow / DuckDB backend** — avoid loading full Parquet partitions into memory; push date and quality filters to the scan layer
- **Missing tier behaviour** — if Silver is requested but only Bronze exists, raise an informative error rather than silently falling back; callers should be explicit about which tier they want
- **Multi-type reads** — a `read_silver_event(catchment_id, start, end)` helper that returns flow, level, and rainfall together (as a named list) would support event analysis workflows

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

### Gold Tier: CAMELS-GB Style Catchment Attributes
> `camels.R` / `signatures.R`

Calculate a standardised set of static catchment attributes and hydrological signatures consistent with the [CAMELS-GB dataset](https://doi.org/10.5194/essd-12-2459-2020) (Coxon et al., 2020). This enables benchmarking against the 671-catchment CAMELS-GB reference dataset and supports large-sample hydrology workflows. Attributes split into two groups: **hydrological signatures** derived from the Silver/Gold time series, and **spatial attributes** derived from external GIS sources.

#### Hydrological Signatures *(derived from flow and climate time series)*

| Attribute | Description |
|-----------|-------------|
| `q_mean` | Mean daily discharge |
| `runoff_ratio` | Runoff ratio (Q / P) |
| `stream_elas` | Streamflow precipitation elasticity |
| `slope_fdc` | Slope of the flow duration curve |
| `baseflow_index` | Baseflow index (CAMELS method) |
| `baseflow_index_ceh` | Baseflow index (CEH / Gustard 1992 method) |
| `hfd_mean` | Mean half-flow date |
| `Q5` / `Q95` | 5th and 95th flow percentiles |
| `high_q_freq` / `high_q_dur` | Frequency and duration of high-flow events |
| `low_q_freq` / `low_q_dur` | Frequency and duration of low-flow events |
| `zero_q_freq` | Frequency of zero-flow days |

#### Climate Indices *(derived from precipitation and PET time series)*

| Attribute | Description |
|-----------|-------------|
| `p_mean` | Mean daily precipitation (mm d⁻¹) |
| `pet_mean` | Mean daily PET — Penman–Monteith (mm d⁻¹) |
| `aridity` | Aridity index (PET / P) |
| `p_seasonality` | Seasonality and timing of precipitation |
| `frac_snow` | Fraction of precipitation falling as snow |
| `high_prec_freq` / `high_prec_dur` / `high_prec_timing` | High-precipitation event statistics |
| `low_prec_freq` / `low_prec_dur` / `low_prec_timing` | Low-precipitation event statistics |

#### Topographic Attributes *(from DEM)*

| Attribute | Description |
|-----------|-------------|
| `elev_mean` / `elev_min` / `elev_max` | Catchment elevation statistics (m a.s.l.) |
| `elev_10` / `elev_50` / `elev_90` | Elevation percentiles |
| `dpsbar` | Mean drainage path slope (m km⁻¹) |

#### Land Cover *(from Land Cover Map 2015 or equivalent)*

| Attribute | Description |
|-----------|-------------|
| `dwood_perc` / `ewood_perc` | Deciduous and evergreen woodland (%) |
| `grass_perc` / `shrub_perc` | Grassland and shrub/heath (%) |
| `crop_perc` / `urban_perc` | Cropland and urban (%) |
| `inwater_perc` / `bogs_perc` | Inland water and bogs/peatland (%) |
| `dom_land_cover` | Dominant land cover class |

#### Soil Attributes *(from European Soil Database or equivalent)*

| Attribute | Description |
|-----------|-------------|
| `sand_perc` / `silt_perc` / `clay_perc` | Soil texture fractions (%) |
| `organic_perc` | Organic matter content (%) |
| `bulkdens` | Bulk density |
| `tawc` | Total available water content |
| `conductivity_cosby` / `conductivity_hypres` | Saturated hydraulic conductivity (two PTF methods) |
| `porosity_cosby` / `porosity_hypres` | Porosity (two PTF methods) |
| `soil_depth_pelletier` | Depth to bedrock (m) |

#### Hydrogeology *(from BGS Hydrogeology Map)*

Fractional area of nine hydrogeological productivity/flow-mechanism classes:

| Attribute | Description |
|-----------|-------------|
| `frac_high_perc_eff_aquifer` | High productivity, intergranular (%) |
| `frac_mod_perc_eff_aquifer` | Moderate productivity, intergranular (%) |
| `frac_low_perc_aquifer` | Low productivity, intergranular (%) |
| `frac_high_frac_eff_aquifer` | High productivity, fractured (%) |
| `frac_mod_frac_eff_aquifer` | Moderate productivity, fractured (%) |
| `frac_low_frac_aquifer` | Low productivity, fractured (%) |
| `frac_non_aquifer` | Non-aquifer (%) |

#### Human Management *(from NRFA, abstraction licences)*

| Attribute | Description |
|-----------|-------------|
| `num_reservoir` / `reservoir_cap` | Reservoir count and total storage capacity (Ml) |
| `reservoir_he` / `reservoir_nav` / `reservoir_drain` | Capacity by purpose: hydroelectric, navigation, drainage |
| `reservoir_wr` / `reservoir_fs` / `reservoir_env` | Capacity by purpose: water regulation, flood storage, environmental |
| `reservoir_year_first` / `reservoir_year_last` | First and last year of reservoir construction |
| `abs_agriculture_perc` / `abs_amenities_perc` | Abstraction breakdown by sector (%) |
| `abs_energy_perc` / `abs_industry_perc` / `abs_water_supply_perc` | Abstraction breakdown by sector (%) |
| `gw_abs` / `sw_abs` | Groundwater and surface water abstractions (Ml d⁻¹) |
| `discharges` | Total returns / discharges to rivers |

#### Planned Functions

- `compute_signatures(gauge_id, period)` — Calculate hydrological signatures from Silver flow series
- `compute_climate_indices(gauge_id, period)` — Calculate climate indices from precipitation and PET series
- `extract_spatial_attributes(catchment_boundary)` — Zonal statistics from raster layers (DEM, land cover, soils, BGS)
- `build_camels_table(gauge_ids)` — Assemble full CAMELS-style attribute table for a set of gauges
- `compare_to_camels_gb(gauge_id)` — Benchmark computed attributes against published CAMELS-GB values

> **Reference:** Coxon, G. et al. (2020). CAMELS-GB: hydrometeorological time series and landscape attributes for 671 catchments in Great Britain. *Earth System Science Data*, 12(4), 2459–2483. https://doi.org/10.5194/essd-12-2459-2020

---

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
