# Future Flode Packages

This document describes two planned packages that sit alongside **reach.io** in the
Flode ecosystem. Neither is part of reach.io itself — they are separate packages with
their own responsibilities and release cycles.

---

## Package Stack

```
┌─────────────────────────────────────────────────────┐
│   reach.hydro   — hydrological analysis & plotting   │
│   <spatial pkg> — catchment geometry & weighting     │
├─────────────────────────────────────────────────────┤
│   reach.io      — data ingestion, Bronze/Silver/Gold │
│                   pipeline, rating curves  (current) │
└─────────────────────────────────────────────────────┘
```

`reach.hydro` and the spatial package both depend on reach.io for S7 class
definitions and shared utilities (e.g. `cumsum_na()`). They do not depend on
each other.

---

## reach.hydro

**Purpose:** Hydrological analysis tools that sit above the ingestion pipeline.
Replaces and extends the analysis functions currently in `riskyData`.

**Dependency:** `reach.io`

### Planned Functions

| Function | Description | Replaces |
|---|---|---|
| `exceed(dt, threshold, gap_width, time_step)` | Threshold crossing detection with event merging over gaps | `riskyData::exceed()` |
| `detect_peaks(dt, min_separation_hrs)` | Flood peak identification and ranking by magnitude | Manual in operational script §3 |
| `seasonality_plot(events, value_col)` | Polar plot of event seasonality by calendar month (plotly) | Manual in operational script §3 |
| `plot_hydro_year(obj, ...)` | Faceted annual time series using `add_hydro_year()` (ggplot2) | Manual `facet_wrap` in operational script §4 |
| `plot_cumulative_rainfall(gauges, weights, ...)` | Multi-gauge cumulative rainfall by hydro year | Manual in operational script §5 |
| `gauge_aar(dt, rescale_na)` | Annual Average Rainfall with optional NA rescaling | `getGaugeAAR()` in operational script §0 |
| `combine_weighted_rainfall(ids, weights, from, to, ...)` | Weighted multi-gauge rainfall series for model input | Manual in operational script §2b |

### Design Notes

- All functions accept reach.io S7 objects (`Rainfall_Daily`, `Rainfall_15min`, etc.)
  and return either new S7 objects or plain `data.table`s as appropriate
- `exceed()` should match `riskyData::exceed()` behaviour for backward compatibility:
  events closer together than `gap_width` timesteps are merged into one
- `seasonality_plot()` wraps `plotly::plot_ly` with EA colour palette and month
  labels pre-configured — callers should not need to touch plotly directly
- `plot_hydro_year()` uses `add_hydro_year()` from reach.io and returns a `ggplot`
  object so callers can add their own layers

---

## Spatial Package

**Purpose:** Catchment geometry, gauge weighting, and spatial analysis tools.
Replaces the spatial functions in `mappER` with a clean `sf`-native
implementation that has no hidden external dependencies.

**Dependency:** `sf`; optionally `reach.io` for coordinate data from `find_stations()`

### Planned Functions

#### Catchment geometry & weighting

| Function | Description | Replaces |
|---|---|---|
| `load_catchment(filepath)` | Load and validate CRS of a catchment shapefile | `mappER::loadCatchment()` |
| `thiessen_weights(coords, catchment)` | Voronoi polygons + catchment intersection + area-weighted proportions | `mappER::teeSun()` + `mappER::intersectPoly()` + `mappER::gaugeProp()` |
| `saar_at_gauges(coords, saar_sf, dataset)` | Spatially weighted SAAR value at each gauge location (1 km² box average) | Manual in operational script §7 |
| `saar_adjusted_weights(thiessen_w, gauge_saar, catchment_saar)` | Three-method weight table: Thiessen, SAAR-adjusted, SAAR-adjusted rescaled | Manual in operational script §7 |
| `elevation_weights(gauges, hypsometric_data)` | Elevation-band weights derived from hypsometric curve | Manual in operational script §8 |
| `plot_hypsometric_curve(hyps_data, gauges)` | Hypsometric curve with gauge elevation overlaid as points | Manual in operational script §8 |
| `gauge_to_catchment_weights(gauges, catchment, method)` | Generalised weight calculator supporting Thiessen, elevation-weighted, and distance-decay in one interface | Manual combinations in operational script |
| `catchment_overlap_matrix(registry)` | Identify nested/overlapping catchments in the gauge registry; flags double-counting risk in areal rainfall estimates | — |

#### Rainfall spatial processing

| Function | Description | Notes |
|---|---|---|
| `extract_radar_rainfall(catchment, radar_stack)` | Extract areal rainfall from NIMROD or CEH-GEAR grids for a catchment polygon using partial-cell weighting | Requires `exactextractr` |
| `areal_reduction_factor(area_km2, duration_hr)` | FEH/NERC ARF lookup for design storm scaling | Needed when setting boundary conditions from design events |

#### Catchment descriptors

| Function | Description | Notes |
|---|---|---|
| `extract_catchment_attrs(catchment)` | Pull baseflow index, mean slope, dominant soil type, woodland fraction from national datasets (HOST, LCM, OS Terrain) | Feeds ungauged estimation workflows |
| `estimate_qmed(catchment)` | FEH statistical QMED from catchment descriptors | Useful for ungauged sites in the gauge registry |

#### Real-time situational awareness

| Function | Description | Notes |
|---|---|---|
| `spatial_interpolate_readings(readings_sf, grid, method)` | Kriging or IDW interpolation of current gauge readings onto a grid | Spatial picture of conditions across a region |
| `travel_time_matrix(gauges, network)` | Hydraulic travel times between gauges from river network geometry | Useful for lead-time calculations and flood routing checks |
| `assign_flood_warning_areas(gauges, fwa_polygons)` | Spatial join linking gauges to downstream Flood Warning Areas | Builds alert routing tables |

#### Model validation

| Function | Description | Notes |
|---|---|---|
| `compare_flood_extent(model_raster, observed_sf, threshold)` | Compute CSI, hit rate, and false alarm ratio between modelled inundation raster and observed extent | Observed extent from Sentinel-1 SAR or CEMS |
| `fetch_sentinel_extent(aoi, date_range)` | Pull flood extent polygons from Copernicus Emergency Management Service or local processed SAR archive | Requires STAC/API access |

### Design Notes

- `load_catchment()` should validate that the loaded shapefile contains exactly
  one polygon and warn if the CRS is not OSGB (EPSG:27700), since all EA
  hydrometric data uses British National Grid
- `thiessen_weights()` should error if any gauge falls outside the catchment
  bounding box rather than silently assigning zero weight — the current
  `mappER` behaviour of returning zero-weight gauges without warning has caused
  errors in operational workflows
- `saar_adjusted_weights()` should return all three method columns in a single
  `data.frame` so the user can compare them directly (as in §7 of the
  operational script) rather than constructing the table manually
- The `intersectPolyTest()` workaround in the current operational script exists
  because `mappER::intersectPoly()` does not handle multi-polygon shapefiles
  correctly. The new implementation should handle these natively via
  `sf::st_cast()` before intersecting
- `extract_radar_rainfall()` should use `exactextractr::exact_extract()` rather
  than `raster::extract()` — `exactextractr` handles partial cells at catchment
  boundaries correctly, which matters for small catchments relative to radar
  grid resolution (1 km NIMROD)
- `compare_flood_extent()` should return a named list with scalar skill scores
  and an `sf` object of the TP/FP/FN geometry so results can be mapped as well
  as tabulated
- `gauge_to_catchment_weights()` should accept a `method` argument
  (`"thiessen"`, `"elevation"`, `"distance_decay"`) and return a consistent
  named numeric vector, so callers can swap methods without changing downstream
  code

### Suggested Dependencies

| Package | Purpose |
|---|---|
| `sf` | Vector geometry (all spatial functions) |
| `terra` | Raster I/O and processing |
| `exactextractr` | Fast, area-weighted raster extraction to polygons |
| `whitebox` | DEM hydrological conditioning for catchment delineation |
| `rstac` | STAC API access for Sentinel/CEH-GEAR rainfall grids |

### SAAR Datasets

Three gridded SAAR shapefiles are referenced in the current operational
workflows. The spatial package should support all three via a `dataset`
argument:

| Key | Source | Period |
|---|---|---|
| `"HadUK_1991_2020"` | HadUK-Grid | 1991–2020 (default) |
| `"HadUK_1961_1990"` | HadUK-Grid | 1961–1990 |
| `"FEH_1961_1990"` | FEH | 1961–1990 (matches FEH webservice pre-Sep 2025) |

---

## Migration Path from riskyData / mappER

| Current | Replacement |
|---|---|
| `riskyData::loadAPI()` | `reach.io::download_hydrology()` + `find_stations()` |
| `riskyData::exceed()` | `reach.hydro::exceed()` |
| `HydroImportFactory$new()` | reach.io S7 constructors (`Rainfall_15min()` etc.) |
| `$hydroYearDay()` | `reach.io::add_hydro_year()` |
| `$postOrder()` | `data.table::setorder(dt, dateTime)` |
| `riskyData::cumsumNA()` | `reach.io::cumsum_na()` |
| `mappER::loadCatchment()` | `<spatial pkg>::load_catchment()` |
| `mappER::teeSun()` + `intersectPoly()` + `gaugeProp()` | `<spatial pkg>::thiessen_weights()` |
