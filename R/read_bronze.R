# ============================================================
# Tool:        Bronze Tier Data Retrieval
# Description: Functions to read raw Bronze Parquet data back
#              into an R session as typed S7 FlodeHydroData objects,
#              consistent with the output of download_hydrology().
#
# Functions:
#   read_bronze()           — single gauge + data type
#   read_bronze_multi()     — multiple gauges, returns named list
#   read_bronze_catchment() — all gauges for a catchment via registry
#
# Design notes:
#   - Files are filtered at the year-directory level before reading,
#     reducing I/O on large stores. Year directories are *observation*
#     years (see .write_bronze_by_year() in schema.R).
#   - Bronze is append-only. Re-ingesting a series adds a new file; the
#     overlap is resolved here, keeping the most recently received row for
#     each (series, timestamp).
#   - Period is read from the `period` column stored in Bronze schema;
#     falls back to inference from median timestamp gap.
#   - All helpers are self-contained — no dependency on read_silver.R.
#   - Hard errors when Bronze data is absent; no silent fallback.
#
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-05-07
# Tier:        1 - Bronze, read-only
# Dependencies: data.table, arrow, S7
# ============================================================


# =============================================================================
# Internal helpers
# =============================================================================

# Framework data type code -> parameter name used to pick the S7 class from
# HYDRO_CLASS. Deliberately a private copy of .data_type_to_param() in
# read_silver.R so this file has no dependency on the Silver reader. Only the
# three types with an S7 class are supported: SM, SWE and PE are valid codes in
# the store (DATA_TYPE_CODES) but cannot be read back through read_bronze() yet.
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

# Decide the series period ("15min", "hourly", "daily") that selects the S7
# class. The stored `period` column is trusted when it holds exactly one value.
# If it is absent/all NA, or the files disagree (e.g. a series re-ingested at a
# different resolution), the choice is made from the data instead.
#' @noRd
.resolve_period_b <- function(dt) {
  p <- unique(dt$period[!is.na(dt$period)])
  if (length(p) == 1L) return(p)

  # Fall back to inferring from median timestamp gap
  # A single row has no gap to measure; "15min" is the default assumption
  if (nrow(dt) < 2L) return("15min")
  diffs   <- as.numeric(diff(sort(dt$timestamp)), units = "secs")
  # The median (not the mean) so that gaps in the record do not shift the result
  med_sec <- stats::median(diffs, na.rm = TRUE)
  # Anything coarser than hourly is treated as daily
  if (med_sec <= 900)  return("15min")
  if (med_sec <= 3600) return("hourly")
  "daily"
}

# Reshape Bronze rows into the readings table the FlodeHydroData classes expect
# (dateTime, date, value, measure_notation) plus the Bronze extras. Bronze has no
# API measure notation, so `measure_notation` carries the dataset_id, which still
# identifies which ingest (and trace) each row came from.
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

#' Escape regex metacharacters in a literal string
#'
#' Gauge IDs and supplier codes are interpolated into a file-name pattern in
#' `.list_bronze_files()`; escaping stops characters such as `.` or `+` in an
#' ID matching more than themselves.
#' @noRd
.re_escape <- function(x) gsub("([][{}()+*^$|\\\\?.])", "\\\\\\1", x)

#' Received date (from the 8-digit suffix) of Bronze file names
#'
#' This is the ingest date baked into the `dataset_id`, not an observation
#' date. It is the sort key that makes "later file wins" work when duplicate
#' observations are resolved. A name without the suffix gives `NA`.
#' @noRd
.bronze_received_date <- function(files) {
  as.Date(sub(".*_([0-9]{8})\\.parquet$", "\\1", basename(files)), "%Y%m%d")
}

#' Trace code ("" when absent) embedded in Bronze file names
#'
#' File names are `<supplier>_<gauge>_<type>[_<trace>]_<YYYYMMDD>.parquet`.
#' @noRd
.bronze_file_trace <- function(files, gauge_id, data_type, supplier_code) {
  # Strip ".parquet" and the received date, leaving "<sup>_<gauge>_<type>[_<trace>]"
  base   <- sub("_[0-9]{8}$", "", sub("\\.parquet$", "", basename(files)))
  prefix <- paste(supplier_code, gauge_id, data_type, sep = "_")
  # Whatever follows the prefix and its joining underscore (+ 2L) is the trace;
  # an untraced file is exactly the prefix, so this yields "".
  substring(base, nchar(prefix) + 2L)
}

#' List Bronze files for a gauge (never errors; empty vector if none)
#'
#' Files are returned oldest-received first, so later rows win when duplicates
#' are resolved.
#'
#' Also used by `get_high_watermark()` in incremental.R, which is why this
#' returns an empty vector instead of raising an error: "no data yet" is a
#' normal state for a first incremental run.
#' @noRd
.list_bronze_files <- function(root, gauge_id, data_type,
                               supplier_code = "EA",
                               category      = "hydrometric",
                               start = NULL, end = NULL) {

  dt_root <- file.path(root, "bronze", category, supplier_code, data_type)
  if (!dir.exists(dt_root)) return(character(0))

  year_dirs <- list.dirs(dt_root, recursive = FALSE, full.names = TRUE)

  # Narrow to the year range implied by start / end. Year directories hold
  # *observation* years, so this prunes whole directories exactly (a file in
  # 2021/ cannot contain 2022 data). Both ends are inclusive at year level; the
  # exact timestamp cut is made later in read_bronze() once rows are loaded.
  # Directories whose names are not years are dropped when a range is given.
  if (!is.null(start) || !is.null(end)) {
    years <- suppressWarnings(as.integer(basename(year_dirs)))
    lo    <- if (!is.null(start)) as.integer(format(as.Date(start), "%Y")) else -Inf
    hi    <- if (!is.null(end))   as.integer(format(as.Date(end),   "%Y")) else  Inf
    year_dirs <- year_dirs[!is.na(years) & years >= lo & years <= hi]
  }
  if (length(year_dirs) == 0L) return(character(0))

  # <supplier>_<gauge>_<type>[_<trace>]_<YYYYMMDD>.parquet. Matching the whole
  # name (anchored, with the trace optional) stops gauge "39001" from picking up
  # files of a different gauge or type that merely start with the same text,
  # while still admitting every trace of this gauge.
  pat <- sprintf("^%s_%s_%s(_.+)?_[0-9]{8}\\.parquet$",
                 .re_escape(supplier_code), .re_escape(gauge_id),
                 .re_escape(data_type))
  files <- list.files(year_dirs, pattern = pat, full.names = TRUE)
  # Oldest received first; the path is the tie-breaker so the order (and hence
  # which duplicate wins) is deterministic across platforms
  files[order(.bronze_received_date(files), files)]
}

# Strict wrapper over .list_bronze_files() for the readers: the same search,
# but each way of coming up empty is turned into a distinct error, from
# coarsest (no store) to finest (no file for this gauge in the date range), so
# the message says which level of the layout is missing.
#' @noRd
.locate_bronze_files <- function(root, gauge_id, data_type,
                                 supplier_code, category,
                                 start = NULL, end = NULL) {

  if (!dir.exists(file.path(root, "bronze")))
    stop(sprintf("No Bronze store found at: %s", file.path(root, "bronze")))

  dt_root <- file.path(root, "bronze", category, supplier_code, data_type)
  if (!dir.exists(dt_root))
    stop(sprintf(
      "No Bronze data found for category='%s', supplier='%s', data_type='%s'.\nExpected: %s",
      category, supplier_code, data_type, dt_root
    ))

  files <- .list_bronze_files(root, gauge_id, data_type, supplier_code,
                              category, start, end)

  if (length(files) == 0L)
    stop(sprintf(
      paste0("No Bronze data found for gauge '%s', data_type '%s', supplier '%s'",
             " in the requested date range.\nHas download_hydrology(..., ",
             "output = \"disk\") been run for this gauge?"),
      gauge_id, data_type, supplier_code
    ))

  files
}


# =============================================================================
# -- read_bronze() --
# =============================================================================

#' Read raw data for a single gauge from the Bronze store
#'
#' Locates all Bronze Parquet files for the specified gauge and data type,
#' applies optional date filters, and returns an S7 `FlodeHydroData` object
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
#' @param start Date, POSIXct or character (`"YYYY-MM-DD"` or
#'   `"YYYY-MM-DD HH:MM:SS"`) or `NULL`. Earliest timestamp to include (a bare
#'   date means midnight UTC). `NULL` reads from the beginning of the record.
#' @param end Date, POSIXct or character or `NULL`. Latest timestamp to include;
#'   a bare date includes the whole of that day. `NULL` reads to the end of the
#'   record.
#' @param trace Character or `NULL`. Series discriminator for gauges that hold
#'   several series of the same data type (e.g. WISKI `"sg"` and `"sg_ds"`
#'   stage). `NULL` is fine when the gauge has a single series; if several are
#'   present an error lists them.
#'
#' @details
#' Bronze is append-only, so re-downloading a gauge adds new files rather than
#' rewriting old ones. `read_bronze()` resolves any overlap: for each series and
#' timestamp the row from the most recently received file is kept.
#'
#' Files are located by observation year, so stores written before Bronze was
#' partitioned by observation year (one file per ingest, filed under the ingest
#' year) should be re-ingested for date-range reads to be reliable.
#'
#' Rows are matched as "the same observation" by series and timestamp, where
#' the series is the `dataset_id` with its received-date suffix removed. That
#' keeps the supplier, site, data type and trace, so two traces of one gauge
#' are never merged into each other. Duplicates are resolved before `start` and
#' `end` are applied, and the result is sorted by time. The returned
#' `from_date` and `to_date` are the first and last timestamps actually
#' returned, not the bounds you asked for.
#'
#' `end` is inclusive: a bare date such as `"2023-12-31"` includes the whole of
#' that day. This differs from the `to_date`/`end_date` arguments of
#' [download_hydrology()], [run_backfill()] and [run_incremental()], which are
#' exclusive. Bounds are interpreted as UTC, like the stored timestamps.
#'
#' A file that cannot be read produces a warning and is skipped, so a damaged
#' year does not hide the rest; an error is raised only if nothing could be
#' read at all. Absent Bronze data is always an error (there is no fallback to
#' another tier). Each failure to find data (no store, no such data type, no
#' file for the gauge, nothing in the range) has its own message.
#'
#' If the series has more than one `trace` and none is chosen, the call stops
#' and names the available traces rather than guessing. The S7 class comes from
#' the stored `period` (or, if that is missing or inconsistent, the median
#' timestep). Only 15-minute and daily series have a class; for any other
#' period (e.g. hourly) a warning is issued and the plain `readings`
#' `data.table` is returned instead of an S7 object. No QC or unit conversion
#' is applied. See [read_bronze_multi()] and [read_bronze_catchment()] to read
#' several gauges at once.
#'
#' @return An S7 object of the appropriate `FlodeHydroData` subclass
#'   ([FlodeFlow_15min], [FlodeLevel_15min], [FlodeRainfall_15min], [FlodeFlow_Daily], etc.),
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
                        end           = NULL,
                        trace         = NULL) {

  # -- Locate files ------------------------------------------------------------
  # Validate the data type before touching the file system
  param <- .data_type_to_param_b(data_type)

  # Throws if nothing matches; files come back oldest-received first, an order
  # the de-duplication step below depends on
  files <- .locate_bronze_files(root, gauge_id, data_type,
                                supplier_code, category, start, end)

  # -- Select the series -------------------------------------------------------
  # A gauge can hold several series of one data type (WISKI traces)
  # Selecting here, before reading, means unwanted series cost no I/O. Filtering
  # `files` keeps the oldest-first order.
  traces <- .bronze_file_trace(files, gauge_id, data_type, supplier_code)
  if (!is.null(trace)) {
    files <- files[traces == trace]
    if (length(files) == 0L)
      stop(sprintf("No Bronze series with trace '%s' for gauge '%s' / '%s'.",
                   trace, gauge_id, data_type))
  } else if (length(unique(traces)) > 1L) {
    # Refuse to guess: silently picking one series (or merging several) would
    # return plausible-looking but wrong data
    stop(sprintf(
      "Gauge '%s' / '%s' holds several series (%s). Choose one with `trace`.",
      gauge_id, data_type,
      paste(sprintf("'%s'", ifelse(nzchar(unique(traces)), unique(traces), "<none>")),
            collapse = ", ")
    ))
  }

  # -- Read and combine files --------------------------------------------------
  # One unreadable file warns and is skipped rather than failing the whole read
  dt_list <- lapply(files, function(f) {
    tryCatch(
      data.table::as.data.table(.read_parquet(f)),
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

  # -- Resolve overlap between files -------------------------------------------
  # Resolve overlap between files: files are ordered oldest-received first, so
  # keeping the last occurrence keeps the most recently received row
  # rbindlist() preserves file order, which is what fromLast = TRUE relies on.
  # The series key is the dataset_id minus its received date, so a trace stays
  # part of the key. The key is taken from the stored dataset_id column rather
  # than the file name.
  dt[, .series := sub("_[0-9]{8}$", "", dataset_id)]
  dt <- unique(dt, by = c(".series", "timestamp"), fromLast = TRUE)
  dt[, .series := NULL]

  # -- Apply date bounds -------------------------------------------------------
  # Exact timestamp bounds (year-dir filter was approximate)
  # Comparisons use dt$timestamp explicitly. Note that `data_type` is both an
  # argument of this function and a Bronze column, so it must never be used
  # inside dt[...] here (the column would win; see .filter_rows() in package.R).
  # `end` is inclusive: .bound_posixct() turns a bare date into the last instant
  # of that day.
  if (!is.null(start)) {
    lo <- .bound_posixct(start, "start")
    dt <- dt[dt$timestamp >= lo]
  }
  if (!is.null(end)) {
    hi <- .bound_posixct(end, "end")
    dt <- dt[dt$timestamp <= hi]
  }

  if (nrow(dt) == 0L)
    stop(sprintf(
      "No Bronze data for gauge '%s' / '%s' in the requested date range.",
      gauge_id, data_type
    ))

  data.table::setorder(dt, site_id, timestamp)

  # -- Build the S7 object -----------------------------------------------------
  readings <- .build_bronze_readings(dt)
  period   <- .resolve_period_b(dt)
  cls      <- HYDRO_CLASS[[param]][[period]]

  # HYDRO_CLASS has no entry for some periods (e.g. "hourly"); hand back the
  # bare readings so the caller still gets the data, with a warning rather
  # than an error
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
# -- read_bronze_multi() --
# =============================================================================

#' Read Bronze data for multiple gauges
#'
#' Calls [read_bronze()] for each supplied `gauge_id` and returns the results
#' as a named list. Gauges that fail (e.g. no Bronze data exists) are skipped
#' with a warning rather than stopping the whole call.
#'
#' @details
#' Gauges are read one after another with identical arguments, so a single
#' `start`, `end`, `supplier_code`, `category` and `trace` applies to all of
#' them. Every failure becomes a warning, including the error raised when a
#' gauge holds several series and no `trace` was given; check
#' `names(result)` against `gauge_ids` to find what was skipped. If every
#' gauge fails the result is an empty list, not an error. Only an empty
#' `gauge_ids` stops the call.
#'
#' @param root Character. Root of the data store.
#' @param gauge_ids Character vector. One or more site identifiers.
#' @param data_type Character. Framework data type code (`"Q"`, `"H"`,
#'   `"P"`).
#' @param ... Additional arguments passed to [read_bronze()] (e.g.
#'   `supplier_code`, `start`, `end`).
#'
#' @return A named list of S7 `FlodeHydroData` objects, one per successfully read
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

  # Pre-named list so each result is stored under its gauge ID; assigning NULL
  # for a failed gauge removes that entry, and the final Filter() is a guard
  results <- vector("list", length(gauge_ids))
  names(results) <- gauge_ids

  # Sequential, so any skip warnings appear in gauge order
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
# -- read_bronze_catchment() --
# =============================================================================

#' Read Bronze data for all gauges in a catchment
#'
#' Looks up gauge identifiers for the specified catchment from the gauge
#' registry, then delegates to [read_bronze_multi()].
#'
#' @details
#' The registry needs an id column (`site_id`, or `gauge_id` as written by
#' [build_gauge_registry()]) and the catchment column named by `catchment_col`;
#' with the default `"catchment_id"`, a registry that has `catchment` instead
#' (again as [build_gauge_registry()] writes it) is accepted. A gauge appears
#' once per data type in the registry, so ids are de-duplicated. Gauges are
#' matched on exact equality with `catchment_id`; the registry's own `data_type`, `category` and
#' `source_system` are not consulted, so the single `data_type` you pass is
#' requested for every gauge, with the default `supplier_code = "EA"` unless
#' supplied through `...`.
#'
#' A catchment with no gauges in the registry is an error, whereas individual
#' gauges that have no Bronze data are skipped with a warning (see
#' [read_bronze_multi()]).
#'
#' @param root Character. Root of the data store.
#' @param catchment_id Character. Catchment identifier to filter the registry
#'   on.
#' @param data_type Character. Framework data type code.
#' @param registry A `data.table` or `data.frame` with an id column
#'   (`site_id` or `gauge_id`) and a catchment column (`catchment_id`, or
#'   `catchment`). Typically the output of [build_gauge_registry()].
#' @param catchment_col Character. Column name in `registry` that holds
#'   catchment identifiers. Default `"catchment_id"`.
#' @param ... Additional arguments passed to [read_bronze_multi()].
#'
#' @return A named list of S7 `FlodeHydroData` objects, one per gauge in the
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
  # Registry shape checks and the catchment match live in one shared helper
  # (see .gauges_in_catchment() in package.R)
  gauge_ids <- .gauges_in_catchment(registry, catchment_id, catchment_col)

  if (length(gauge_ids) == 0L)
    stop(sprintf("No gauges found in registry for catchment '%s'.",
                 catchment_id))

  read_bronze_multi(root, gauge_ids, data_type, ...)
}
