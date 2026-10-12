# ============================================================
# Tool:        Package Constants and Utilities
# Description: Package-level constants, PARAMETER_CONFIG, VALID_SOURCES,
#              supplier/data-type lookup tables, and %||% operator.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      Internal constants used across all reach.io functions
# Outputs:     None
# Dependencies: data.table, arrow
# ============================================================

#' reach.io: Hydrometric Data Pipeline Tools for the Environment Agency API
#'
#' A hydrometric data pipeline toolkit covering the full journey from raw EA
#' data sources through to partitioned Parquet Bronze storage.
#'
#' ## API download tools
#' - [find_stations()] -- look up stations by identifier or location
#' - [get_measures()] -- fetch available measure time series metadata
#' - [download_hydrology()] -- download readings for one or more parameters
#'
#' ## Schema and provenance tools
#' - [make_dataset_id()] -- generate a Bronze dataset ID
#' - [source_to_supplier()] -- map source_system to supplier code
#' - [param_to_data_type()] -- map parameter name to framework data type code
#' - [apply_bronze_schema()] -- coerce a data.table to the Bronze Parquet schema
#' - [bronze_path()] -- build the Bronze file path following framework Appendix C
#' - [write_provenance_record()] -- append a provenance record to the register
#'
#' ## Setup
#' - [setup_hydro_store()] -- create the hydrometric data store directory structure
#'
#' ## Pipeline tools
#' - [build_gauge_registry()] -- build the master gauge registry Parquet table
#'
#' ## Schema and provenance tools (Hydrometric Data Framework v1.3)
#' - [make_dataset_id()] -- generate a Bronze dataset ID
#' - [source_to_supplier()] -- map source_system to supplier code
#' - [param_to_data_type()] -- map parameter name to framework data type code
#' - [apply_bronze_schema()] -- apply the mandatory Bronze Parquet schema
#' - [bronze_path()] -- construct the Bronze file path (Appendix C layout)
#' - [write_provenance_record()] -- append a provenance record to the register
#' - [route_gauge()] -- dispatch a gauge fetch to the correct source system
#' - [ingest_all_file()] -- ingest WISKI .all exports to partitioned Parquet
#' - [run_backfill()] -- parallelised backfill orchestrator with logging
#' - [run_incremental()] -- incremental sync from high watermark per gauge
#'
#' @keywords internal
"_PACKAGE"

# S7 generics (print, as_data_table, as_long) are defined in classes.R
#' @import S7
#' @importFrom stats mad median quantile
#'
## usethis namespace: start
#' @importFrom data.table :=
#' @importFrom data.table .BY
#' @importFrom data.table .EACHI
#' @importFrom data.table .GRP
#' @importFrom data.table .I
#' @importFrom data.table .N
#' @importFrom data.table .NGRP
#' @importFrom data.table .SD
#' @importFrom data.table data.table
## usethis namespace: end
NULL

# -- Package-level constants --------------------------------------------------

#' @noRd
BASE_URL <- "https://environment.data.gov.uk/hydrology"

#' @noRd
API_HARD_LIMIT <- 2000000L

#' @noRd
PARAMETER_CONFIG <- list(
  rainfall = list(
    observed_property = "rainfall",
    value_type        = NULL,          # only one type available: total
    default_period    = "15min"        # also available: daily
  ),
  flow = list(
    observed_property = "waterFlow",
    value_type        = "instantaneous", # also available: mean, max, min; NULL returns all
    default_period    = "15min"
  ),
  level = list(
    observed_property = "waterLevel",
    value_type        = "instantaneous", # also available: mean, max, min; NULL returns all
    default_period    = "15min"
  )
)

# Valid source systems recognised by the pipeline router
#' @noRd
VALID_SOURCES <- c("HDE", "WISKI", "WISKI_ALL")


# -- data.table non-standard evaluation ---------------------------------------
#
# Column names used inside data.table expressions (`dt[, x := ...]`,
# `dt[cond]`, `.SD`/`..cols`). Declared so R CMD check does not report them as
# undefined globals.
utils::globalVariables(c(
  ".", "..present_cols", "..val_cols", ".covered", ".series", ".snap_key",
  ".snapped", "C", "Time.stamp", "a", "active", "b", "backfill_done",
  "catchment", "category", "cum_x", "cum_y", "data_type", "dataset_id",
  "date_added", "dateTime", "datetime_str", "doubtful", "ea_site_ref",
  "easting", "file_path", "gauge_id", "hydro_year", "hydro_year_day", "label",
  "lat", "limb_no", "live", "lon", "lower", "measure_notation", "measures",
  "northing", "notation", "notes", "observed", "parameter", "pe_mm", "period",
  "qc_flag", "qc_flagged_at", "qc_h_code", "qc_p_code", "qc_value",
  "qc_y_code", "riverName", "RLOIid", "site_id", "snap_ts", "source_system",
  "station_name", "station_number", "station.notation", "station.wiskiID",
  "status", "temporal_resolution", "time_period_end", "time_period_start",
  "timestamp", "ts_name", "unit", "upper", "value", "value_type", "wiskiID",
  "x_val", "y_val", "year", "fitted", "date"
))

# -- Null-coalescing operator -------------------------------------------------

#' Null-coalescing operator
#'
#' Returns `x` if it is not `NULL`, otherwise returns `y`. Used internally
#' to substitute safe typed `NA` values when pulling fields from result lists
#' that may not have been populated due to an earlier error.
#'
#' @param x Left-hand side value.
#' @param y Right-hand side fallback, returned when `x` is `NULL`.
#'
#' @return `x` if `!is.null(x)`, otherwise `y`.
#' @noRd
`%||%` <- function(x, y) if (!is.null(x)) x else y


#' Convert a `start` / `end` bound to a UTC POSIXct instant
#'
#' Accepts `Date`, `POSIXct`, or character (`"YYYY-MM-DD"` or
#' `"YYYY-MM-DD HH:MM[:SS]"`). A date-only `end` covers the whole of that day;
#' a date-only `start` begins at midnight. A bound with a time of day is used
#' exactly as given.
#'
#' @param x Date, POSIXct or character scalar.
#' @param which `"start"` or `"end"`.
#' @return A POSIXct scalar (UTC).
#' @noRd
.bound_posixct <- function(x, which = c("start", "end")) {
  which <- match.arg(which)
  if (inherits(x, "POSIXct")) {
    attr(x, "tzone") <- "UTC"
    return(x)
  }
  end_of_day <- 86400 - 0.001
  if (inherits(x, "Date")) {
    t <- as.POSIXct(format(x, "%Y-%m-%d"), tz = "UTC")
    return(if (which == "end") t + end_of_day else t)
  }
  x <- sub("T", " ", as.character(x), fixed = TRUE)
  if (grepl("\\d{1,2}:\\d{2}", x)) {
    t <- as.POSIXct(x, tz = "UTC")
    if (is.na(t)) stop(sprintf("Could not parse date-time bound '%s'.", x), call. = FALSE)
    return(t)
  }
  d <- as.Date(x)
  if (is.na(d)) stop(sprintf("Could not parse date bound '%s'.", x), call. = FALSE)
  t <- as.POSIXct(format(d, "%Y-%m-%d"), tz = "UTC")
  if (which == "end") t + end_of_day else t
}


#' Keep rows where a column is in a set of values
#'
#' Safe replacement for `dt[dt$col %in% values]` inside functions whose
#' argument has the same name as a column. Within `[.data.table` a bare symbol
#' in `i` resolves to the *column* before the function argument, so
#' `reg[reg$category %in% category]` compares the column to itself and keeps
#' every row. Here the comparison is evaluated outside the data.table scope.
#'
#' @param dt A `data.table`.
#' @param col Character. Column name.
#' @param values Values to keep.
#' @return `dt` restricted to matching rows.
#' @noRd
.filter_rows <- function(dt, col, values) {
  rows <- which(dt[[col]] %in% values)
  dt[rows]
}


# -- S3 print method registration ---------------------------------------------
#
# S7's print.S7_object calls str() directly and never consults S7's internal
# method table, so S7::method(print, ...) assignments are silently ignored.
# Registering via registerS3method() puts the methods in R's own S3 dispatch
# table where they are found before print.S7_object.

.onLoad <- function(libname, pkgname) {
  registerS3method("print", "reach.io::FlodeRatingCurve",  .print_RatingCurve,  envir = asNamespace(pkgname))
  registerS3method("print", "reach.io::FlodeRatingSet",    .print_RatingSet,    envir = asNamespace(pkgname))
  registerS3method("print", "reach.io::FlodeHydroData",    .print_HydroData,    envir = asNamespace(pkgname))
  registerS3method("print", "reach.io::FlodePotEvapData",  .print_PotEvapData,  envir = asNamespace(pkgname))

  # Register this package's pipeline activities so reach.utils::run_pipeline()
  # can dispatch to them by name from a pipeline YAML config.
  reach.utils::register_activity("backfill",    run_backfill,    package = pkgname)
  reach.utils::register_activity("incremental", run_incremental, package = pkgname)
}
