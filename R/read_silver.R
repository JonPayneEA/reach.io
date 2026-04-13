# ============================================================
# Tool:        Silver Tier Data Retrieval
# Description: Functions to read QC-flagged data back out of
#              the Silver store. Returns S7 HydroData objects
#              consistent with Bronze-tier reads so downstream
#              code works unchanged regardless of which tier
#              data comes from.
#
# Functions:
#   read_silver()           — single gauge + data type
#   read_silver_multi()     — multiple gauges, returns named list
#   read_silver_catchment() — all gauges for a catchment via registry
#
# Design notes:
#   - Files are filtered at the year-directory level before reading,
#     reducing I/O without requiring a dplyr dependency.
#   - Quality filtering uses qc_flag: flags ≤ min_quality are kept.
#     NULL min_quality returns all rows including Rejected (flag 4).
#   - The `readings` data.table returned inside the S7 object uses
#     qc_value as `value` (the QC-accepted reading) and retains
#     qc_flag and qc_y_code as additional columns for transparency.
#   - Hard errors (no silent Bronze fallback) when Silver data is
#     absent — callers must be explicit about which tier they want.
#
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-04-13
# Tier:        2 (Silver / Read-only)
# Dependencies: data.table, arrow, S7
# ============================================================


# =============================================================================
# Internal helpers
# =============================================================================

# Map a framework data_type code to the parameter name used by HYDRO_CLASS.
#' @noRd
.data_type_to_param <- function(data_type) {
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

# Infer a period label ("15min", "hourly", "daily") from the median timestep
# gap in a POSIXct vector. Falls back to "15min" for single-row inputs.
#' @noRd
.infer_period_silver <- function(timestamps) {
  if (length(timestamps) < 2L) return("15min")
  diffs   <- as.numeric(diff(sort(timestamps)), units = "secs")
  med_sec <- stats::median(diffs, na.rm = TRUE)
  if (med_sec <= 900)   return("15min")
  if (med_sec <= 3600)  return("hourly")
  "daily"
}

# Build a readings data.table conforming to the HydroData validator:
#   required columns: dateTime (POSIXct), date (Date), value (numeric),
#                     measure_notation (character)
# Extra Silver columns (qc_flag, qc_y_code, raw_value, etc.) are appended
# so the caller retains full QC transparency.
#' @noRd
.build_silver_readings <- function(dt) {
  readings <- data.table::data.table(
    dateTime         = dt$timestamp,
    date             = as.Date(dt$timestamp),
    value            = dt$qc_value,      # QC-accepted value is the primary reading
    measure_notation = dt$dataset_id,
    raw_value        = dt$value,         # original observed value preserved
    supplier_flag    = dt$supplier_flag,
    site_id          = dt$site_id,
    qc_flag          = dt$qc_flag,
    qc_flagged_at    = dt$qc_flagged_at
  )
  # Attach qc_y_code if present (Flow/Q data) — NA for other data types
  readings[, qc_y_code := if ("qc_y_code" %in% names(dt))
    dt$qc_y_code
  else
    NA_integer_]
  readings
}

# Find all Silver Parquet files for a gauge + data_type, optionally filtering
# to a year range derived from the start/end dates.
#' @noRd
.locate_silver_files <- function(root, gauge_id, data_type,
                                  start = NULL, end = NULL) {
  silver_root <- file.path(root, "silver")
  if (!dir.exists(silver_root))
    stop(sprintf("No Silver store found at: %s", silver_root))

  # Locate all data_type subdirectories (may be under multiple categories)
  all_dirs <- list.dirs(silver_root, recursive = TRUE, full.names = TRUE)
  dt_dirs  <- all_dirs[grepl(
    paste0("[/\\\\]", data_type, "$"), all_dirs
  )]

  if (length(dt_dirs) == 0L)
    stop(sprintf(
      "No Silver data found for data type '%s'. Has any Bronze data been promoted?",
      data_type
    ))

  # Narrow to year subdirectories matching the date range
  year_dirs <- unlist(lapply(dt_dirs, function(d) {
    yd <- list.dirs(d, recursive = FALSE, full.names = TRUE)
    if (!is.null(start) || !is.null(end)) {
      years <- suppressWarnings(as.integer(basename(yd)))
      lo    <- if (!is.null(start)) as.integer(format(as.Date(start), "%Y")) else -Inf
      hi    <- if (!is.null(end))   as.integer(format(as.Date(end),   "%Y")) else  Inf
      yd    <- yd[!is.na(years) & years >= lo & years <= hi]
    }
    yd
  }))

  if (length(year_dirs) == 0L)
    stop(sprintf(
      "No Silver files for gauge '%s' / '%s' in the requested date range.",
      gauge_id, data_type
    ))

  # Filter to files whose name contains _<gauge_id>_
  site_pat <- paste0("_", gauge_id, "_")
  files    <- list.files(year_dirs, pattern = "\\.parquet$",
                         full.names = TRUE, recursive = FALSE)
  files    <- files[grepl(site_pat, basename(files), fixed = TRUE)]

  if (length(files) == 0L)
    stop(sprintf(
      paste0("No Silver data found for gauge '%s', data type '%s'. ",
             "Has promote_to_silver() been run for this gauge?"),
      gauge_id, data_type
    ))

  files
}


# =============================================================================
# read_silver()
# =============================================================================

#' Read QC-flagged data for a single gauge from the Silver store
#'
#' Locates all Silver Parquet files for the specified gauge and data type,
#' applies optional date and quality filters, and returns an S7 `HydroData`
#' object consistent with the output of [download_hydrology()].
#'
#' The primary `value` column in `@readings` is `qc_value` (the QC-accepted
#' reading). The original observed `value` is retained as `raw_value`.
#' `qc_flag` and `qc_y_code` are always present in `@readings` so callers
#' can apply their own thresholds if needed.
#'
#' @param root Character. Root of the data store (the directory containing
#'   `silver/`).
#' @param gauge_id Character. Site identifier (as used in `site_id` and
#'   the `dataset_id` embedded in file names).
#' @param data_type Character. Framework data type code: `"Q"` (flow),
#'   `"H"` (stage/level), or `"P"` (rainfall).
#' @param start Date or character (`"YYYY-MM-DD"`) or `NULL`. Earliest
#'   timestamp to include. `NULL` reads from the beginning of the record.
#' @param end Date or character (`"YYYY-MM-DD"`) or `NULL`. Latest
#'   timestamp to include. `NULL` reads to the end of the record.
#' @param min_quality Integer or `NULL`. Maximum `qc_flag` value to include.
#'   `1` = Good only; `2` = Good + Estimated; `3` = Good + Estimated +
#'   Suspect; `NULL` = all flags including Rejected (4). Default `1L`.
#' @param cols Character vector or `NULL`. Additional columns from the Silver
#'   schema to include in `@readings` beyond the defaults. `NULL` returns all
#'   columns.
#'
#' @return An S7 object of the appropriate `HydroData` subclass
#'   ([Flow_15min], [Level_15min], [Rainfall_15min], [Flow_Daily], etc.),
#'   depending on the data type and observed timestep. The `@readings`
#'   slot is a `data.table` containing `dateTime`, `date`, `value`
#'   (`qc_value`), `measure_notation`, `raw_value`, `supplier_flag`,
#'   `site_id`, `qc_flag`, `qc_y_code`, and `qc_flagged_at`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Good-only 15-min flow for gauge 39001
#' obj <- read_silver("data/hydro", "39001", "Q",
#'                    start = "2024-01-01", end = "2024-12-31")
#'
#' # Include Suspect readings too
#' obj <- read_silver("data/hydro", "39001", "Q", min_quality = 3L)
#'
#' # All flags (including Rejected) for diagnostics
#' obj <- read_silver("data/hydro", "39001", "Q", min_quality = NULL)
#' }
read_silver <- function(root, gauge_id, data_type,
                        start       = NULL,
                        end         = NULL,
                        min_quality = 1L,
                        cols        = NULL) {

  files <- .locate_silver_files(root, gauge_id, data_type, start, end)

  # Read and combine all relevant files
  dt_list <- lapply(files, function(f) {
    tryCatch(
      data.table::as.data.table(arrow::read_parquet(f)),
      error = function(e) {
        warning(sprintf("Could not read Silver file: %s\n  %s", f,
                        conditionMessage(e)), call. = FALSE)
        NULL
      }
    )
  })
  dt <- data.table::rbindlist(Filter(Negate(is.null), dt_list))

  if (nrow(dt) == 0L)
    stop(sprintf("No data could be read for gauge '%s' / '%s'.",
                 gauge_id, data_type))

  # Post-filter by exact timestamp bounds (year-level filtering was approximate)
  if (!is.null(start))
    dt <- dt[timestamp >= as.POSIXct(as.Date(start), tz = "UTC")]
  if (!is.null(end))
    dt <- dt[timestamp <= as.POSIXct(as.Date(end),   tz = "UTC") + 86399L]

  # Quality filter
  if (!is.null(min_quality))
    dt <- dt[qc_flag <= as.integer(min_quality)]

  if (nrow(dt) == 0L)
    stop(sprintf(
      paste0("No Silver data for gauge '%s' / '%s' passes the quality filter",
             " (min_quality = %s)."),
      gauge_id, data_type,
      if (is.null(min_quality)) "NULL" else as.character(min_quality)
    ))

  data.table::setorder(dt, site_id, timestamp)

  # Build HydroData-compatible readings
  readings <- .build_silver_readings(dt)

  # Column subset (always keep the required HydroData columns)
  if (!is.null(cols)) {
    required_cols <- c("dateTime", "date", "value", "measure_notation")
    keep          <- unique(c(required_cols, cols))
    keep          <- intersect(keep, names(readings))
    readings      <- readings[, .SD, .SDcols = keep]
  }

  # Resolve S7 class
  param  <- .data_type_to_param(data_type)
  period <- .infer_period_silver(dt$timestamp)
  cls    <- HYDRO_CLASS[[param]][[period]]

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
# read_silver_multi()
# =============================================================================

#' Read Silver data for multiple gauges
#'
#' Calls [read_silver()] for each supplied `gauge_id` and returns the results
#' as a named list. Gauges that fail (e.g. no Silver data exists) are
#' skipped with a warning rather than stopping the whole call.
#'
#' @param root Character. Root of the data store.
#' @param gauge_ids Character vector. One or more site identifiers.
#' @param data_type Character. Framework data type code (`"Q"`, `"H"`,
#'   `"P"`).
#' @param ... Additional arguments passed to [read_silver()] (e.g. `start`,
#'   `end`, `min_quality`).
#'
#' @return A named list of S7 `HydroData` objects, one per successfully read
#'   gauge. Names match `gauge_ids`. Gauges with no data are omitted.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' objs <- read_silver_multi("data/hydro", c("39001", "39002"), "Q",
#'                           start = "2024-01-01", min_quality = 3L)
#' objs[["39001"]]
#' }
read_silver_multi <- function(root, gauge_ids, data_type, ...) {
  if (length(gauge_ids) == 0L)
    stop("`gauge_ids` must contain at least one gauge identifier.")

  results <- vector("list", length(gauge_ids))
  names(results) <- gauge_ids

  for (gid in gauge_ids) {
    results[[gid]] <- tryCatch(
      read_silver(root, gid, data_type, ...),
      error = function(e) {
        warning(sprintf("Skipping gauge '%s': %s", gid,
                        conditionMessage(e)), call. = FALSE)
        NULL
      }
    )
  }

  # Drop failed gauges
  Filter(Negate(is.null), results)
}


# =============================================================================
# read_silver_catchment()
# =============================================================================

#' Read Silver data for all gauges in a catchment
#'
#' Looks up gauge identifiers for the specified catchment from the gauge
#' registry, then delegates to [read_silver_multi()]. Requires the registry
#' to contain a `catchment_id` column (or equivalent) mapping sites to
#' catchments.
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
#' @param ... Additional arguments passed to [read_silver_multi()].
#'
#' @return A named list of S7 `HydroData` objects, one per gauge in the
#'   catchment that has Silver data.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' reg  <- build_gauge_registry("gauges.csv")
#' objs <- read_silver_catchment("data/hydro", "Thames_Upper", "Q",
#'                               registry = reg)
#' }
read_silver_catchment <- function(root, catchment_id, data_type,
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

  gauge_ids <- registry[
    registry[[catchment_col]] == catchment_id, site_id
  ]

  if (length(gauge_ids) == 0L)
    stop(sprintf("No gauges found in registry for catchment '%s'.",
                 catchment_id))

  read_silver_multi(root, gauge_ids, data_type, ...)
}
