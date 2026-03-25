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


# -- S3 print method registration ---------------------------------------------
#
# S7's print.S7_object calls str() directly and never consults S7's internal
# method table, so S7::method(print, ...) assignments are silently ignored.
# Registering via registerS3method() puts the methods in R's own S3 dispatch
# table where they are found before print.S7_object.

.onLoad <- function(libname, pkgname) {
  registerS3method("print", "reach.io::RatingCurve",  .print_RatingCurve,  envir = asNamespace(pkgname))
  registerS3method("print", "reach.io::RatingSet",    .print_RatingSet,    envir = asNamespace(pkgname))
  registerS3method("print", "reach.io::HydroData",    .print_HydroData,    envir = asNamespace(pkgname))
  registerS3method("print", "reach.io::PotEvapData",  .print_PotEvapData,  envir = asNamespace(pkgname))
}
