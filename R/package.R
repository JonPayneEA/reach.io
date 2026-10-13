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

# Root of the EA Hydrology API; endpoint paths are appended to it
# (/id/stations.json, /id/measures.json, /id/measures/<measure>/readings.json
# and /data/batch-readings/batch).
#' @noRd
BASE_URL <- "https://environment.data.gov.uk/hydrology"

# Most rows the readings endpoint will return for one request. Sync downloads
# are split into date chunks so a single measure stays under this, and
# run_sync() warns when a chunk reaches it (the response would then be truncated).
#' @noRd
API_HARD_LIMIT <- 2000000L

# Per-parameter defaults used to choose which API measure counts as "the" series
# for a parameter. This is the single place those choices live: get_measures(),
# download_hydrology() and the HDE router all read it, and callers can override
# period/value_type per call.
#   observed_property  the API's observedProperty name for the parameter
#   value_type         NULL means "do not filter on valueType" (rainfall has
#                      only one); "all" is accepted by callers as an explicit
#                      "no filter" sentinel
#   default_period     the period selected unless the caller asks otherwise
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
# build_gauge_registry() rejects any other source_system, so the router never
# sees an unknown one. Adding a source means extending this vector, the router,
# and SUPPLIER_CODES in schema.R (which gives the source its directory code).
#' @noRd
VALID_SOURCES <- c("HDE", "WISKI", "WISKI_ALL")


# -- data.table non-standard evaluation ---------------------------------------
#
# Column names used inside data.table expressions (`dt[, x := ...]`,
# `dt[cond]`, `.SD`/`..cols`). Declared so R CMD check does not report them as
# undefined globals.
# This only silences the check; it does not resolve name clashes. Several of
# these names (site_id, data_type, category, ...) are also argument names in
# functions that subset tables, and inside dt[...] the column wins over the
# argument. Use .filter_rows() or which() first in those cases.
# Add a name here when new data.table code introduces another bare column name.
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
  # An exact instant: only the tzone attribute is relabelled to UTC, the moment
  # in time is unchanged. `which` makes no difference here.
  if (inherits(x, "POSIXct")) {
    attr(x, "tzone") <- "UTC"
    return(x)
  }
  # "End of day" is the last millisecond rather than the next midnight, so that
  # an inclusive `<=` test on the bound keeps the whole day without admitting
  # the following midnight reading.
  end_of_day <- 86400 - 0.001
  # Date: format() first so the calendar date is taken as written, whatever the
  # session time zone, then read it as midnight UTC
  if (inherits(x, "Date")) {
    t <- as.POSIXct(format(x, "%Y-%m-%d"), tz = "UTC")
    return(if (which == "end") t + end_of_day else t)
  }
  # Character: tolerate the ISO 8601 "T" separator. A string with a time of day
  # is an exact instant and used as given for both `start` and `end`; a bare
  # date falls through to the whole-day logic below.
  x <- sub("T", " ", as.character(x), fixed = TRUE)
  if (grepl("\\d{1,2}:\\d{2}", x)) {
    t <- as.POSIXct(x, tz = "UTC")
    if (is.na(t)) stop(sprintf("Could not parse date-time bound '%s'.", x), call. = FALSE)
    return(t)
  }
  # Bare date string: parse failures are reported against the user's text
  d <- as.Date(x)
  if (is.na(d)) stop(sprintf("Could not parse date bound '%s'.", x), call. = FALSE)
  t <- as.POSIXct(format(d, "%Y-%m-%d"), tz = "UTC")
  if (which == "end") t + end_of_day else t
}


#' Pause between requests to the EA API
#'
#' The Hydrology API fair-use guidance asks for one request at a time. All the
#' per-request pauses go through this one function so the delay is defined in a
#' single place and tests can replace it with a no-op (base `Sys.sleep()` itself
#' cannot be mocked).
#'
#' @param secs Numeric. Seconds to wait. Default 1.
#' @noRd
.pace <- function(secs = 1) Sys.sleep(secs)


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
  # The column is fetched by name with [[ and compared outside dt[...], so
  # `values` is always the caller's argument. %in% (rather than ==) gives FALSE
  # instead of NA for missing entries, and which() turns the result into plain
  # row numbers, so a NA never produces a phantom all-NA row.
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
  # S7 classes dispatch print through S3 under the name "<package>::<class>"
  registerS3method("print", "reach.io::FlodeRatingCurve",  .print_RatingCurve,  envir = asNamespace(pkgname))
  registerS3method("print", "reach.io::FlodeRatingSet",    .print_RatingSet,    envir = asNamespace(pkgname))
  registerS3method("print", "reach.io::FlodeHydroData",    .print_HydroData,    envir = asNamespace(pkgname))
  registerS3method("print", "reach.io::FlodePotEvapData",  .print_PotEvapData,  envir = asNamespace(pkgname))

  # Register this package's pipeline activities so reach.utils::run_pipeline()
  # can dispatch to them by name from a pipeline YAML config.
  # Done at load time so the names are available as soon as the package is
  # attached or loaded, with no setup step. The names ("backfill",
  # "incremental") are the strings a YAML step refers to.
  reach.utils::register_activity("backfill",    run_backfill,    package = pkgname)
  reach.utils::register_activity("incremental", run_incremental, package = pkgname)
}
