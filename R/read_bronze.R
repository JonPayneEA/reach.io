# ============================================================
# Tool:        Bronze Tier Data Retrieval
# Description: Functions to read raw Bronze Parquet data back
#              into an R session as typed S7 HydroData objects,
#              consistent with the output of download_hydrology().
#
# Functions:
#   read_bronze()           — single gauge + data type
#   read_bronze_multi()     — multiple gauges, returns named list
#   read_bronze_catchment() — all gauges for a catchment via registry
#
# Design notes:
#   - Files are filtered at the year-directory level before reading,
#     reducing I/O on large stores.
#   - Period is read from the `period` column stored in Bronze schema;
#     falls back to inference from median timestamp gap.
#   - All helpers are self-contained — no dependency on read_silver.R.
#   - Hard errors when Bronze data is absent; no silent fallback.
#
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-05-07
# Tier:        1 (Bronze / Read-only)
# Dependencies: data.table, arrow, S7
# ============================================================


# =============================================================================
# Internal helpers
# =============================================================================

#' @noRd
.data_type_to_param_b <- function(data_type) {
  switch(data_type,
    Q  = "flow",
    H  = "level",
    P  = "rainfall",
    stop(sprintf(
      "No parameter mapping for data_type '%s'. Supported: Q, H, P.",
      data_type
    ))
  )
}

#' @noRd
.resolve_period_b <- function(dt) {
  p <- unique(dt$period[!is.na(dt$period)])
  if (length(p) == 1L) return(p)

  # Fall back to inferring from median timestamp gap
  if (nrow(dt) < 2L) return("15min")
  diffs   <- as.numeric(diff(sort(dt$timestamp)), units = "secs")
  med_sec <- stats::median(diffs, na.rm = TRUE)
  if (med_sec <= 900)  return("15min")
  if (med_sec <= 3600) return("hourly")
  "daily"
}

#' @noRd
.build_bronze_readings <- function(dt) {
  data.table::data.table(
    dateTime         = dt$timestamp,
    date             = as.Date(dt$timestamp),
    value            = dt$value,
    measure_notation = dt$dataset_id,
    supplier_flag    = dt$supplier_flag,
    site_id          = dt$site_id,
    data_type        = dt$data_type
  )
}

#' @noRd
.locate_bronze_files <- function(root, gauge_id, data_type,
                                  supplier_code, category,
                                  start = NULL, end = NULL) {

  bronze_root <- file.path(root, "bronze")
  if (!dir.exists(bronze_root))
    stop(sprintf("No Bronze store found at: %s", bronze_root))

  dt_root <- file.path(bronze_root, category, supplier_code, data_type)
  if (!dir.exists(dt_root))
    stop(sprintf(
      "No Bronze data found for category='%s', supplier='%s', data_type='%s'.\nExpected: %s",
      category, supplier_code, data_type, dt_root
    ))

  year_dirs <- list.dirs(dt_root, recursive = FALSE, full.names = TRUE)

  # Narrow to the year range implied by start / end
  if (!is.null(start) || !is.null(end)) {
    years <- suppressWarnings(as.integer(basename(year_dirs)))
    lo    <- if (!is.null(start)) as.integer(format(as.Date(start), "%Y")) else -Inf
    hi    <- if (!is.null(end))   as.integer(format(as.Date(end),   "%Y")) else  Inf
    year_dirs <- year_dirs[!is.na(years) & years >= lo & years <= hi]
  }

  if (length(year_dirs) == 0L)
    stop(sprintf(
      "No Bronze files for gauge '%s' / '%s' in the requested date range.",
      gauge_id, data_type
    ))

  # Filter to files whose name contains _<gauge_id>_
  site_pat <- paste0("_", gauge_id, "_")
  files    <- list.files(year_dirs, pattern = "\\.parquet$",
                         full.names = TRUE, recursive = FALSE)
  files    <- files[grepl(site_pat, basename(files), fixed = TRUE)]

  if (length(files) == 0L)
    stop(sprintf(
      "No Bronze data found for gauge '%s', data_type '%s', supplier '%s'.\nHas download_hydrology(..., output = \"disk\") been run for this gauge?",
      gauge_id, data_type, supplier_code
    ))

  files
}


# =============================================================================
# read_bronze()
# =============================================================================

#' Read raw data for a single gauge from the Bronze store
#'
#' Locates all Bronze Parquet files for the specified gauge and data type,
#' applies optional date filters, and returns an S7 `HydroData` object
#' consistent with the output of [download_hydrology()].
#'
#' The returned object contains the raw observed `value` as stored in
#' Bronze, plus `supplier_flag`, `site_id`, and `data_type` as additional
#' columns in `@readings`. No QC has been applied — use [read_silver()] for
#' quality-filtered data.
#'
#' @param root Character. Root of the data store (the directory containing
#'   `bronze/`).
#' @param gauge_id Character. Site identifier matching the `site_id` column
#'   in the Bronze Parquet files and embedded in their filenames.
#' @param data_type Character. Framework data type code: `"Q"` (flow),
#'   `"H"` (stage/level), or `"P"` (rainfall).
#' @param supplier_code Character. Supplier code used when the data was
#'   ingested. Default `"EA"` (HDE downloads).
#' @param category Character. Data category. Default `"hydrometric"`.
#' @param start Date or character (`"YYYY-MM-DD"`) or `NULL`. Earliest
#'   timestamp to include. `NULL` reads from the beginning of the record.
#' @param end Date or character (`"YYYY-MM-DD"`) or `NULL`. Latest
#'   timestamp to include. `NULL` reads to the end of the record.
#'
#' @return An S7 object of the appropriate `HydroData` subclass
#'   ([Flow_15min], [Level_15min], [Rainfall_15min], [Flow_Daily], etc.),
#'   depending on the data type and stored period. The `@readings` slot is
#'   a `data.table` with columns `dateTime`, `date`, `value`,
#'   `measure_notation`, `supplier_flag`, `site_id`, and `data_type`.
#'
#' @seealso [read_silver()] for QC-filtered Silver-tier data;
#'   [download_hydrology()] for writing data to Bronze.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' obj <- read_bronze("data/hydro", "SS92F014", "P",
#'                    start = "2023-01-01", end = "2023-12-31")
#' obj@period_name  # "15min"
#' obj@readings     # data.table of raw observations
#' }
read_bronze <- function(root, gauge_id, data_type,
                        supplier_code = "EA",
                        category      = "hydrometric",
                        start         = NULL,
                        end           = NULL) {

  files <- .locate_bronze_files(root, gauge_id, data_type,
                                supplier_code, category, start, end)

  dt_list <- lapply(files, function(f) {
    tryCatch(
      data.table::as.data.table(arrow::read_parquet(f)),
      error = function(e) {
        warning(sprintf("Could not read Bronze file: %s\n  %s",
                        f, conditionMessage(e)), call. = FALSE)
        NULL
      }
    )
  })
  dt <- data.table::rbindlist(Filter(Negate(is.null), dt_list))

  if (nrow(dt) == 0L)
    stop(sprintf("No data could be read for gauge '%s' / '%s'.",
                 gauge_id, data_type))

  # Exact timestamp bounds (year-dir filter was approximate)
  if (!is.null(start))
    dt <- dt[timestamp >= as.POSIXct(as.Date(start), tz = "UTC")]
  if (!is.null(end))
    dt <- dt[timestamp <= as.POSIXct(as.Date(end), tz = "UTC") + 86399L]

  if (nrow(dt) == 0L)
    stop(sprintf(
      "No Bronze data for gauge '%s' / '%s' in the requested date range.",
      gauge_id, data_type
    ))

  data.table::setorder(dt, site_id, timestamp)

  readings <- .build_bronze_readings(dt)
  param    <- .data_type_to_param_b(data_type)
  period   <- .resolve_period_b(dt)
  cls      <- HYDRO_CLASS[[param]][[period]]

  if (is.null(cls)) {
    warning(sprintf(
      "No S7 class for parameter '%s', period '%s'. Returning data.table.",
      param, period
    ), call. = FALSE)
    return(readings)
  }

  cls(
    readings  = readings,
    from_date = format(min(dt$timestamp), "%Y-%m-%d"),
    to_date   = format(max(dt$timestamp), "%Y-%m-%d")
  )
}


# =============================================================================
# read_bronze_multi()
# =============================================================================

#' Read Bronze data for multiple gauges
#'
#' Calls [read_bronze()] for each supplied `gauge_id` and returns the results
#' as a named list. Gauges that fail (e.g. no Bronze data exists) are skipped
#' with a warning rather than stopping the whole call.
#'
#' @param root Character. Root of the data store.
#' @param gauge_ids Character vector. One or more site identifiers.
#' @param data_type Character. Framework data type code (`"Q"`, `"H"`,
#'   `"P"`).
#' @param ... Additional arguments passed to [read_bronze()] (e.g.
#'   `supplier_code`, `start`, `end`).
#'
#' @return A named list of S7 `HydroData` objects, one per successfully read
#'   gauge. Names match `gauge_ids`. Gauges with no data are omitted.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' objs <- read_bronze_multi("data/hydro", c("SS92F014", "SS92F015"), "P",
#'                           start = "2023-01-01")
#' objs[["SS92F014"]]
#' }
read_bronze_multi <- function(root, gauge_ids, data_type, ...) {
  if (length(gauge_ids) == 0L)
    stop("`gauge_ids` must contain at least one gauge identifier.")

  results <- vector("list", length(gauge_ids))
  names(results) <- gauge_ids

  for (gid in gauge_ids) {
    results[[gid]] <- tryCatch(
      read_bronze(root, gid, data_type, ...),
      error = function(e) {
        warning(sprintf("Skipping gauge '%s': %s", gid,
                        conditionMessage(e)), call. = FALSE)
        NULL
      }
    )
  }

  Filter(Negate(is.null), results)
}


# =============================================================================
# read_bronze_catchment()
# =============================================================================

#' Read Bronze data for all gauges in a catchment
#'
#' Looks up gauge identifiers for the specified catchment from the gauge
#' registry, then delegates to [read_bronze_multi()].
#'
#' @param root Character. Root of the data store.
#' @param catchment_id Character. Catchment identifier to filter the registry
#'   on.
#' @param data_type Character. Framework data type code.
#' @param registry A `data.table` or `data.frame` with at minimum columns
#'   `site_id` and `catchment_id`. Typically the output of
#'   [build_gauge_registry()].
#' @param catchment_col Character. Column name in `registry` that holds
#'   catchment identifiers. Default `"catchment_id"`.
#' @param ... Additional arguments passed to [read_bronze_multi()].
#'
#' @return A named list of S7 `HydroData` objects, one per gauge in the
#'   catchment that has Bronze data.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' reg  <- build_gauge_registry("gauges.csv")
#' objs <- read_bronze_catchment("data/hydro", "Thames_Upper", "P",
#'                               registry = reg)
#' }
read_bronze_catchment <- function(root, catchment_id, data_type,
                                   registry,
                                   catchment_col = "catchment_id",
                                   ...) {
  registry <- data.table::as.data.table(registry)

  if (!catchment_col %in% names(registry))
    stop(sprintf(
      "Column '%s' not found in registry. Available: %s",
      catchment_col, paste(names(registry), collapse = ", ")
    ))
  if (!"site_id" %in% names(registry))
    stop("Registry must contain a 'site_id' column.")

  gauge_ids <- registry[registry[[catchment_col]] == catchment_id, site_id]

  if (length(gauge_ids) == 0L)
    stop(sprintf("No gauges found in registry for catchment '%s'.",
                 catchment_id))

  read_bronze_multi(root, gauge_ids, data_type, ...)
}
