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
#' ## Pipeline tools
#' - [build_gauge_registry()] -- build the master gauge registry Parquet table
#' - [route_gauge()] -- dispatch a gauge fetch to the correct source system
#' - [ingest_bulk_file()] -- ingest historical bulk files to partitioned Parquet
#' - [run_backfill()] -- parallelised backfill orchestrator with logging
#'
#' @keywords internal
"_PACKAGE"

# -- Package-level constants --------------------------------------------------

#' @noRd
BASE_URL <- "https://environment.data.gov.uk/hydrology"

#' @noRd
API_HARD_LIMIT <- 2000000L

#' @noRd
PARAMETER_CONFIG <- list(
  rainfall = list(
    observed_property = "rainfall",
    value_type        = NULL,   # only one type available: total
    default_period    = "15min" # another type available: daily
  ),
  flow = list(
    observed_property = "waterFlow",
    value_type        = "instantaneous", # Other types available: mean, max, min. NULL returns all.
    default_period    = "15min"
  ),
  level = list(
    observed_property = "waterLevel",
    value_type        = "instantaneous", # Other types available: mean, max, min. NULL returns all
    default_period    = "15min"
  )
)

# Valid source systems recognised by the pipeline router
#' @noRd
VALID_SOURCES <- c("HDE", "WISKI", "BULK_FILE")

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
