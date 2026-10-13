# ============================================================
# Tool:        Bronze Schema, Dataset IDs, and Provenance
# Description: Centralised Bronze Parquet schema definition,
#              dataset ID generation, supplier code mapping,
#              and provenance record writing for the
#              Hydrometric Data Register.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      Ingest metadata passed by calling functions
# Outputs:     dataset_id strings; provenance record CSV rows
# Dependencies: data.table, arrow
# ============================================================

# -- Supplier code lookup -----------------------------------------------------

# Maps source_system values in the registry to the EA-standard supplier codes
# used in dataset IDs and provenance records (Appendix A of the framework).
# The supplier code is also the third directory level of the Bronze path, so a
# change here moves where data is written AND where read_bronze() looks.
# WISKI and WISKI_ALL share one code (both deliver WISKI data), so they write
# to the same supplier directory and read_bronze(supplier_code = "WISKI")
# finds data from either route.
#' @noRd
SUPPLIER_CODES <- c(
  HDE       = "EA",
  WISKI     = "WISKI",
  WISKI_ALL = "WISKI",
  NRFA      = "NRFA",
  MO        = "MO",
  CEH       = "CEH"
)

# -- Data type code lookup ----------------------------------------------------

# Maps parameter names used internally to the framework data type codes:
# Q = flow, H = stage/water level, P = rainfall, SM = soil moisture,
# SWE = snow water equivalent, PE = potential evaporation
# The code is the fourth directory level of the Bronze path and a segment of
# every dataset_id. Only flow/level/rainfall have an S7 reader today
# (.data_type_to_param_b() in read_bronze.R), so a new code written here is not
# automatically readable back via read_bronze().
#' @noRd
DATA_TYPE_CODES <- c(
  flow          = "Q",
  level         = "H",
  rainfall      = "P",
  soil_moisture = "SM",
  snow          = "SWE",
  pe            = "PE"
)


# -- Data category lookup -----------------------------------------------------

# Valid data categories recognised by the pipeline. Category is a required
# column in the gauge registry and determines the second-level directory
# under each tier in the store structure. Checked against this vector by
# build_gauge_registry() and download_hydrology() so a typo fails early rather
# than creating a stray directory.
#' @noRd
VALID_CATEGORIES <- c("hydrometric", "radarH19", "MOSES")


# -- Dataset ID generation ----------------------------------------------------

#' Generate a Bronze dataset ID
#'
#' Produces a dataset ID in the format specified by the Hydrometric Data
#' Framework (Appendix A): `[SupplierCode]_[SiteID]_[DataType]_[YYYYMMDD]`
#' e.g. `EA_39001_Q_20260115`.
#'
#' When `trace` is supplied, a parameter-level discriminator is inserted before
#' the date: `[SupplierCode]_[SiteID]_[DataType]_[Trace]_[YYYYMMDD]`. This is
#' used by the WISKI `.all` ingestor to distinguish multiple time series for the
#' same station that share a data type — for example upstream stage (`sg`) and
#' downstream stage (`sg_ds`) at the same WISKI station number.
#'
#' @details
#' The trailing `YYYYMMDD` is the *received* date, not an observation date.
#' Because Bronze is append-only, the same series ingested on different days
#' gets different IDs (and so different files), whereas re-ingesting on the
#' same day produces the same ID, and the writer merges into that day's files. Readers
#' rely on this layout: [read_bronze()] orders files by the trailing date to
#' decide which copy of a duplicated observation is the most recent, and
#' strips it again to recognise rows of the same series.
#'
#' An empty-string `trace` is treated like `NULL`. Nothing is validated or
#' sanitised: the components are pasted as given, so a `_` or `/` inside one
#' would make the ID ambiguous to parse or alter the path built by
#' [bronze_path()].
#'
#' @param supplier_code Character. Supplier code, e.g. `"EA"`, `"WISKI"`,
#'   `"MO"`. Use [source_to_supplier()] to derive from `source_system`.
#' @param site_id Character. Gauge or site identifier.
#' @param data_type Character. Framework data type code: `"Q"`, `"H"`,
#'   `"P"`, `"SM"`, `"SWE"` or `"PE"`. Use [param_to_data_type()] to derive from
#'   parameter name.
#' @param received_date Date or character. Date the data was received or
#'   ingested. Defaults to today.
#' @param trace Character or `NULL`. Optional parameter-level trace code to
#'   disambiguate multiple series with the same station and data type. For
#'   WISKI `.all` data this is derived from the third segment of the
#'   `Time series Name` field (e.g. `"sg_ds"` from
#'   `"THM/3401TH/SG_DS/15m.Cmd.RelAbs.P"`). `NULL` (default) omits the
#'   trace and produces the standard four-part ID.
#'
#' @return A length-1 character string.
#'
#' @export
#'
#' @examples
#' make_dataset_id("EA", "39001", "Q")
#' make_dataset_id("WISKI", "2723TH", "H", received_date = "2026-01-15")
#' make_dataset_id("WISKI", "3401TH", "H", trace = "sg_ds")
make_dataset_id <- function(supplier_code,
                            site_id,
                            data_type,
                            received_date = Sys.Date(),
                            trace = NULL) {
  if (!is.null(trace) && nzchar(trace)) {
    sprintf("%s_%s_%s_%s_%s",
            supplier_code,
            site_id,
            data_type,
            trace,
            format(as.Date(received_date), "%Y%m%d"))
  } else {
    sprintf("%s_%s_%s_%s",
            supplier_code,
            site_id,
            data_type,
            format(as.Date(received_date), "%Y%m%d"))
  }
}


#' Map a source_system value to a supplier code
#'
#' Converts the `source_system` column values used in the gauge registry
#' to the EA-standard supplier codes used in dataset IDs.
#'
#' @details
#' A plain lookup in the internal supplier table: `"HDE"` maps to `"EA"`, and
#' both `"WISKI"` and `"WISKI_ALL"` map to `"WISKI"`. The orchestrators
#' ([run_backfill()], [run_incremental()]) treat `NA` as an error so that an
#' unrecognised `source_system` cannot create a stray supplier directory; check
#' for it yourself if you call this directly. Lookup is exact and
#' case-sensitive.
#'
#' @param source_system Character vector. One or more `source_system` values
#'   from the gauge registry.
#'
#' @return Character vector of supplier codes, NA for unrecognised values.
#'
#' @export
#'
#' @examples
#' source_to_supplier("HDE")     # "EA"
#' source_to_supplier("WISKI")   # "WISKI"
source_to_supplier <- function(source_system) {
  unname(SUPPLIER_CODES[source_system])
}


#' Map a parameter name to a framework data type code
#'
#' Converts parameter names used internally (`"flow"`, `"level"`,
#' `"rainfall"`) to the framework data type codes (`"Q"`, `"H"`, `"P"`).
#'
#' @details
#' The lookup table also covers `"soil_moisture"` (`"SM"`), `"snow"` (`"SWE"`)
#' and `"pe"` (`"PE"`), although only flow, level and rainfall can currently
#' be downloaded or read back as S7 objects. Lookup is exact and
#' case-sensitive, and returns `NA` rather than an error for unknown names;
#' the download and ingest paths that need a code turn that `NA` into an
#' error.
#'
#' @param parameter Character vector. One or more parameter names.
#'
#' @return Character vector of data type codes, NA for unrecognised values.
#'
#' @export
#'
#' @examples
#' param_to_data_type("flow")     # "Q"
#' param_to_data_type("rainfall") # "P"
param_to_data_type <- function(parameter) {
  unname(DATA_TYPE_CODES[parameter])
}


# -- Bronze Parquet schema ----------------------------------------------------

#' Apply the framework Bronze Parquet schema to a data.table
#'
#' Selects and coerces columns to match the mandatory Bronze schema defined
#' in Section 7.2 of the Hydrometric Data Framework. Input columns are
#' mapped from internal names to framework names. Any column not in the
#' schema is dropped.
#'
#' Mandatory output columns:
#' \describe{
#'   \item{`timestamp`}{POSIXct UTC. Full observation datetime, including
#'     sub-daily time for instantaneous readings (15-min, hourly). For daily
#'     aggregates (mean, max, min) the time component is midnight UTC — the
#'     time-of-occurrence (e.g. time of peak flow) is not available from the
#'     EA Hydrology API readings endpoint and must be derived from the
#'     sub-daily series separately.}
#'   \item{`value`}{float64. Observed value as received.}
#'   \item{`supplier_flag`}{character. Quality code from supplier, or NA.}
#'   \item{`dataset_id`}{character. Bronze dataset ID. Join key to register.}
#'   \item{`site_id`}{character. Supplier site identifier.}
#'   \item{`data_type`}{character. Framework code: Q, H, P, SM, SWE.}
#'   \item{`period`}{character. Temporal resolution of the series, e.g.
#'     `"15min"`, `"hourly"`, `"daily"`. `NA` when unknown.}
#' }
#'
#' @details
#' This is a pure reshaping step: it builds a new table, leaves `dt`
#' untouched and writes nothing. The API download path (`handle_output()`),
#' the fetchers behind [route_gauge()] and [ingest_all_file()] all call it, so
#' every Bronze file shares one column set and order whichever route produced
#' it.
#'
#' `dataset_id`, `site_id`, `data_type` and `period` are single values
#' recycled over all rows, so one call describes one series. A `timestamp_col`
#' that is not already `POSIXct` is converted as UTC; an existing `POSIXct` is
#' kept as is, and the writers partition by its UTC calendar year. The value
#' column is coerced with `as.double()`, so non-numeric text becomes `NA` with
#' a warning.
#'
#' Rows are not filtered here. Missing values pass through unchanged, and rows
#' with a missing timestamp are only dropped (with a warning) when the table
#' is written by `.write_bronze_by_year()`. A `supplier_flag_col` that is
#' `NULL` or absent from `dt` quietly gives an all-`NA` flag column, so a
#' misspelt column name is not reported.
#'
#' @param dt A `data.table` of raw readings.
#' @param dataset_id Character. The Bronze dataset ID for this dataset.
#' @param site_id Character. Supplier site identifier.
#' @param data_type Character. Framework data type code.
#' @param timestamp_col Character. Name of the datetime column in `dt`.
#'   Default `"dateTime"`.
#' @param value_col Character. Name of the value column in `dt`.
#'   Default `"value"`.
#' @param supplier_flag_col Character or NULL. Name of the supplier quality
#'   flag column in `dt`. NULL produces NA for all rows.
#' @param period Character or NA. Temporal resolution of the series, e.g.
#'   `"15min"`, `"hourly"`, `"daily"`. Stored verbatim in the `period` column.
#'   Default `NA_character_`.
#'
#' @return A `data.table` conforming to the Bronze schema.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' bronze_dt <- apply_bronze_schema(
#'   dt            = raw_dt,
#'   dataset_id    = "EA_39001_Q_20260115",
#'   site_id       = "39001",
#'   data_type     = "Q",
#'   timestamp_col = "dateTime",
#'   value_col     = "value",
#'   supplier_flag_col = "quality",
#'   period        = "15min"
#' )
#' }
apply_bronze_schema <- function(dt,
                                dataset_id,
                                site_id,
                                data_type,
                                timestamp_col     = "dateTime",
                                value_col         = "value",
                                supplier_flag_col = NULL,
                                period            = NA_character_) {

  # -- Validate inputs ---------------------------------------------------------
  # Fail with the offending column name rather than a cryptic NULL error later
  if (!timestamp_col %in% names(dt)) {
    stop(sprintf("timestamp column '%s' not found in dt.", timestamp_col))
  }
  if (!value_col %in% names(dt)) {
    stop(sprintf("value column '%s' not found in dt.", value_col))
  }

  # -- Timestamps ---------------------------------------------------------------
  ts <- dt[[timestamp_col]]
  if (!inherits(ts, "POSIXct")) {
    # Strings/Dates carry no zone of their own; Bronze is defined as UTC
    ts <- as.POSIXct(ts, tz = "UTC")
  }
  # Integer-backed POSIXct (e.g. from seq(<POSIXct>, by = "15 min") in recent R)
  # is written to Parquet by arrow as garbage timestamps; store as double.
  # This is the first of two guards: .write_parquet_atomic() repeats it for
  # tables that did not come through this function.
  if (typeof(ts) != "double") storage.mode(ts) <- "double"

  # -- Supplier flag and assembly -----------------------------------------------
  # A missing flag column is not an error: NA_character_ keeps the column present
  # and character-typed, so files from sources with and without quality codes
  # still share one schema.
  supplier_flag <- if (!is.null(supplier_flag_col) &&
                       supplier_flag_col %in% names(dt)) {
    as.character(dt[[supplier_flag_col]])
  } else {
    NA_character_
  }

  # Column order is the Bronze schema order; anything else in dt is dropped here
  data.table::data.table(
    timestamp     = ts,
    value         = as.double(dt[[value_col]]),
    supplier_flag = supplier_flag,
    dataset_id    = dataset_id,
    site_id       = site_id,
    data_type     = data_type,
    period        = as.character(period)
  )
}


# -- Bronze file path ---------------------------------------------------------

#' Build the Bronze Parquet file path for a dataset
#'
#' Constructs the file path following the framework folder structure:
#' `bronze/<Category>/<SupplierCode>/<DataType>/<YYYY>/<DatasetID>.parquet`
#'
#' @details
#' Only builds a string: nothing is created on disk and no component is
#' checked. The two possible meanings of `<YYYY>` matter here. With
#' `year = NULL` it is the *received* year, taken from the last eight
#' characters of `dataset_id`; every Bronze writer instead passes the
#' *observation* year explicitly (via `.write_bronze_by_year()`), which is
#' what [read_bronze()] expects when it narrows the search by `start` and
#' `end`. Use the default only if you really want a path keyed on ingest date.
#'
#' @param output_dir Character. Root store directory.
#' @param category Character. Data category from the gauge registry, e.g.
#'   `"hydrometric"`, `"radarH19"`, `"MOSES"`.
#' @param supplier_code Character. Supplier code, e.g. `"EA"`.
#' @param data_type Character. Framework data type code.
#' @param dataset_id Character. Full Bronze dataset ID.
#' @param year Character or integer or `NULL`. Year directory to use. When
#'   `NULL` (default) the year is extracted from the date portion of
#'   `dataset_id`. Supply an explicit value to partition by data year rather
#'   than ingest year (e.g. when writing one file per calendar year of
#'   observations).
#'
#' @return Character. Full file path including filename.
#'
#' @export
#'
#' @examples
#' bronze_path("data/hydrometric", "hydrometric", "EA", "Q", "EA_39001_Q_20260115")
#' bronze_path("data/hydrometric", "hydrometric", "EA", "Q", "EA_39001_Q_20260115", year = 2020)
bronze_path <- function(output_dir, category, supplier_code, data_type, dataset_id,
                        year = NULL) {
  if (is.null(year)) {
    year <- substr(dataset_id, nchar(dataset_id) - 7L, nchar(dataset_id) - 4L)
  }
  file.path(output_dir, "bronze", category, supplier_code, data_type, as.character(year),
            paste0(dataset_id, ".parquet"))
}


# -- Bronze writers -----------------------------------------------------------

#' Read a Parquet file without memory-mapping it
#'
#' All Parquet reads in the package go through this wrapper. arrow memory-maps
#' files by default (`mmap = TRUE`); on Windows a mapped file stays locked until
#' the mapping is garbage collected, so a file that has just been read cannot be
#' rewritten (error 1224, "user-mapped section open"). Several functions read a
#' file and then replace it (the registry, the rating store, the Bronze
#' merge-on-write), so every read is made with `mmap = FALSE`. The cost is a copy
#' into memory, which these callers do anyway.
#'
#' @param path Character. File to read.
#' @param ... Passed to [arrow::read_parquet()] (e.g. `col_select`).
#' @return What `arrow::read_parquet()` returns (a data frame by default).
#' @noRd
.read_parquet <- function(path, ...) {
  arrow::read_parquet(path, mmap = FALSE, ...)
}


#' Write a Parquet file atomically
#'
#' Writes to `<path>.tmp` then renames over `path`. On Windows `file.rename()`
#' can fail when the target is open (Arrow memory-maps files it reads), so a
#' copy is attempted before giving up. Errors if the target cannot be replaced.
#'
#' @param x A `data.frame` / `data.table`.
#' @param path Character. Destination file.
#'
#' @return `path`, invisibly.
#' @noRd
.write_parquet_atomic <- function(x, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)

  # -- Normalise timestamp storage ----------------------------------------------
  # arrow mis-writes integer-backed POSIXct columns (see apply_bronze_schema());
  # convert any such column to double on a copy so the caller's data is untouched
  if (is.data.frame(x)) {
    bad <- names(x)[vapply(x, function(col) {
      inherits(col, "POSIXct") && typeof(col) != "double"
    }, logical(1L))]
    if (length(bad) > 0L) {
      x <- data.table::copy(data.table::as.data.table(x))
      for (cn in bad) {
        v <- x[[cn]]
        storage.mode(v) <- "double"
        data.table::set(x, j = cn, value = v)
      }
    }
  }

  # -- Write then swap ----------------------------------------------------------
  # The temp file sits next to the target so the rename stays on one filesystem.
  # A failure part-way through arrow::write_parquet() leaves at most a stray
  # "<name>.parquet.tmp", which the readers ignore (they match "\\.parquet$"),
  # rather than a truncated Parquet file under the real name. Also used for the
  # gauge registry and Silver files, where a half-written file would be worse.
  tmp <- paste0(path, ".tmp")
  arrow::write_parquet(x, tmp)
  if (!isTRUE(suppressWarnings(file.rename(tmp, path)))) {
    # Rename failed (typically a locked/memory-mapped target on Windows). The
    # copy fallback is not atomic, but it is the only way to replace the file.
    ok <- isTRUE(suppressWarnings(file.copy(tmp, path, overwrite = TRUE)))
    # Remove the temp file whether or not the copy worked
    unlink(tmp)
    if (!ok) stop(sprintf("Could not write '%s' (target may be locked).", path),
                  call. = FALSE)
  }
  invisible(path)
}


#' Write a Bronze data.table as one Parquet file per calendar year
#'
#' The single place that decides how Bronze data lands on disk. Rows are split
#' by the UTC calendar year of `timestamp` and each year is written to
#' `bronze/<category>/<supplier>/<data_type>/<YYYY>/<dataset_id>.parquet`, so
#' the year directory always reflects the *observation* year (not the ingest
#' year). Bronze is append-only: re-ingesting the same series on a later day
#' adds a new file (new `dataset_id` date); duplicates are resolved on read by
#' [read_bronze()]. Re-ingesting on the same day merges into that day's file:
#' existing rows are kept and, for a timestamp present in both, the new row
#' wins. (Replacing the file would silently drop the earlier rows, for example
#' when an incremental run follows a backfill on the same day.)
#'
#' @param bronze_dt A `data.table` conforming to the Bronze schema.
#' @param output_dir,category,supplier_code,data_type,dataset_id See
#'   [bronze_path()].
#'
#' @return A `data.table` with one row per file written: `year`, `path`,
#'   `rows`, `period_start`, `period_end` (`"YYYY-MM-DD"`).
#' @noRd
.write_bronze_by_year <- function(bronze_dt, output_dir, category,
                                  supplier_code, data_type, dataset_id) {

  # Zero-row result with the same columns as a normal return, so callers can
  # always nrow()/rbind it and build provenance rows without special cases
  empty <- data.table::data.table(
    year = character(), path = character(), rows = integer(),
    period_start = character(), period_end = character()
  )

  # -- Drop rows that cannot be placed in a year -------------------------------
  # A NA timestamp has no observation year, so it cannot be filed; warn rather
  # than fail so one bad row does not discard the rest of the series.
  n_na <- sum(is.na(bronze_dt$timestamp))
  if (n_na > 0L) {
    warning(sprintf("%d row(s) with missing timestamp not written to Bronze (%s).",
                    n_na, dataset_id), call. = FALSE)
    bronze_dt <- bronze_dt[!is.na(bronze_dt$timestamp)]
  }
  if (nrow(bronze_dt) == 0L) return(empty)

  # -- One file per observation year -------------------------------------------
  # Year is taken in UTC to match the UTC timestamps, so an observation just
  # after midnight UTC never lands in the previous year's directory.
  # Every year file of a call shares the same dataset_id (only the directory
  # differs); that is what ties them together in the register.
  yrs   <- format(bronze_dt$timestamp, "%Y", tz = "UTC")
  files <- lapply(sort(unique(yrs)), function(yr) {
    part <- bronze_dt[which(yrs == yr)]
    # year is passed explicitly: the default would use the received year
    path <- bronze_path(output_dir, category, supplier_code, data_type,
                        dataset_id, year = yr)

    # The file name ends in the received DATE, so a second write of the same
    # series and year on the same day resolves to the same path (for example a
    # backfill followed by an incremental run). Replacing the file would
    # silently discard the earlier rows, so merge instead: existing rows are
    # kept, and where a timestamp appears in both the NEW row wins.
    merged <- part
    if (file.exists(path)) {
      existing <- data.table::as.data.table(.read_parquet(path))
      merged   <- data.table::rbindlist(list(existing, part),
                                        use.names = TRUE, fill = TRUE)
      merged   <- unique(merged, by = "timestamp", fromLast = TRUE)
      data.table::setorder(merged, timestamp)
    }
    .write_parquet_atomic(merged, path)

    # period_start/end describe the whole file as it now stands (including any
    # rows merged in); they become the time_period_start/end of that file's
    # provenance record. `rows` counts the rows written by THIS call.
    data.table::data.table(
      year         = yr,
      path         = path,
      rows         = nrow(part),
      period_start = format(min(merged$timestamp), "%Y-%m-%d", tz = "UTC"),
      period_end   = format(max(merged$timestamp), "%Y-%m-%d", tz = "UTC")
    )
  })
  data.table::rbindlist(files)
}


#' Build provenance rows (one per Bronze year file)
#'
#' Rows are built separately from writing them so that parallel workers can
#' return them and the main process can append to the register sequentially
#' (concurrent appends to one CSV are not safe). All rows share the
#' `dataset_id`; each has its own `file_path` and the period that file covers.
#'
#' @param files `data.table` returned by `.write_bronze_by_year()`.
#' @inheritParams write_provenance_record
#'
#' @return A `data.table` whose columns are arguments of
#'   [write_provenance_record()].
#' @noRd
.bronze_prov_rows <- function(files, dataset_id, supplier, supplier_code,
                              site_id, data_type, temporal_resolution,
                              method_of_receipt) {
  # A missing or non-scalar resolution is recorded as the word "unknown" (the
  # same value the .all ingestor uses when it cannot infer a period) rather than
  # leaving a blank cell or recycling a vector across the year files
  if (length(temporal_resolution) != 1L || is.na(temporal_resolution))
    temporal_resolution <- "unknown"
  # One row per year file: dataset_id and the descriptive fields repeat, while
  # period start/end and file_path come from `files` and differ per row
  data.table::data.table(
    dataset_id          = dataset_id,
    supplier            = supplier,
    supplier_code       = supplier_code,
    site_id             = site_id,
    data_type           = data_type,
    time_period_start   = files$period_start,
    time_period_end     = files$period_end,
    temporal_resolution = temporal_resolution,
    method_of_receipt   = method_of_receipt,
    file_path           = files$path
  )
}

#' Append provenance rows to the Hydrometric Data Register
#'
#' @param prov `data.table` from `.bronze_prov_rows()` (or `NULL`).
#' @param register_path Character. Register CSV path.
#' @return `prov`, invisibly.
#' @noRd
.write_provenance_rows <- function(prov, register_path) {
  # NULL is what workers return when a gauge produced nothing to store
  if (is.null(prov) || nrow(prov) == 0L) return(invisible(prov))
  # Appended one row at a time, sequentially. This must only be called from the
  # main process: concurrent appends to the one register CSV are not safe, which
  # is why workers return rows (see .bronze_prov_rows()) instead of writing them.
  for (i in seq_len(nrow(prov))) {
    do.call(write_provenance_record,
            c(list(register_path = register_path), as.list(prov[i])))
  }
  invisible(prov)
}


# -- Provenance record --------------------------------------------------------

#' Write a Bronze provenance record to the Hydrometric Data Register
#'
#' Appends a provenance record row to the register CSV file for a Bronze
#' file. This satisfies the Section 4.2 requirement that every Bronze dataset
#' has a complete provenance record before storage.
#'
#' The register CSV is created if it does not exist. Each row follows the
#' field list in Section 10.1 of the framework and describes one Bronze
#' \emph{file}: a dataset is written as one file per observation year, so
#' several rows can share a `dataset_id` (see Details).
#'
#' @details
#' Because Bronze is written one file per observation year, a single ingest
#' produces several records: they share `dataset_id` but each carries its own
#' `file_path` and the `time_period_start`/`time_period_end` of that file. Use
#' `file_path` (not `dataset_id`) to identify a physical file.
#'
#' `tier` (`"Bronze"`), `status` (`"Active"`) and `file_format_received`
#' (`"Parquet"`) are fixed, and `date_received` is always the date of this
#' call (`Sys.Date()`), not the date embedded in `dataset_id`; the two differ
#' if `make_dataset_id()` was given an earlier `received_date`.
#'
#' Rows are appended with `data.table::fwrite()`, which does not check the
#' existing header, so the column set and order must not change between calls.
#' The write is also not safe for concurrent use, so call it only from the main
#' R process; the orchestrators ([run_backfill()], [run_incremental()]) have
#' parallel workers return their records and append them afterwards. The parent
#' directory is created if needed, one message is printed per record, and the
#' record is returned invisibly. See [make_dataset_id()] and [bronze_path()]
#' for how `dataset_id` and `file_path` are normally derived.
#'
#' @param register_path Character. Path to the register CSV file. Typically
#'   `hydrometric/register/hydrometric_data_register.csv`.
#' @param dataset_id Character. Bronze dataset ID.
#' @param supplier Character. Supplier name, e.g. `"Environment Agency"`.
#' @param supplier_code Character. Supplier code, e.g. `"EA"`.
#' @param site_id Character. Gauge or site identifier.
#' @param data_type Character. Framework data type code.
#' @param time_period_start Character. Start date of the dataset (YYYY-MM-DD).
#' @param time_period_end Character. End date of the dataset (YYYY-MM-DD).
#' @param temporal_resolution Character. Time step, e.g. `"15min"`, `"daily"`.
#' @param method_of_receipt Character. How the data was obtained, e.g.
#'   `"API pull"`, `"FTP"`, `"bulk file"`, `"WISKI .all export"`.
#' @param file_path Character. Storage path of the Bronze Parquet file.
#' @param received_by Character. Name of the Custodian who ingested the data.
#'   Defaults to `Sys.info()[["user"]]`.
#' @param units Character or NULL. Units as stated by the supplier.
#' @param supplier_quality_flags Character or NULL. Description of any quality
#'   codes provided by the supplier.
#' @param known_limitations Character or NULL. Limitations stated by supplier.
#' @param notes Character or NULL. Any unusual aspects of the delivery.
#'
#' @return The provenance record as a one-row `data.table`, invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' write_provenance_record(
#'   register_path       = "data/hydrometric/register/register.csv",
#'   dataset_id          = "EA_39001_Q_20260115",
#'   supplier            = "Environment Agency",
#'   supplier_code       = "EA",
#'   site_id             = "39001",
#'   data_type           = "Q",
#'   time_period_start   = "2020-01-01",
#'   time_period_end     = "2024-12-31",
#'   temporal_resolution = "15min",
#'   method_of_receipt   = "API pull",
#'   file_path           = "data/hydrometric/bronze/hydrometric/EA/Q/2024/EA_39001_Q_20260115.parquet"
#' )
#' }
write_provenance_record <- function(register_path,
                                    dataset_id,
                                    supplier,
                                    supplier_code,
                                    site_id,
                                    data_type,
                                    time_period_start,
                                    time_period_end,
                                    temporal_resolution,
                                    method_of_receipt,
                                    file_path,
                                    received_by             = Sys.info()[["user"]],
                                    units                   = NA_character_,
                                    supplier_quality_flags  = NA_character_,
                                    known_limitations       = NA_character_,
                                    notes                   = NA_character_) {

  # Fields follow the Section 10.1 field list of the framework
  record <- data.table::data.table(
    dataset_id              = dataset_id,
    tier                    = "Bronze",
    status                  = "Active",
    supplier                = supplier,
    supplier_code           = supplier_code,
    site_id                 = site_id,
    data_type               = data_type,
    time_period_start       = time_period_start,
    time_period_end         = time_period_end,
    temporal_resolution     = temporal_resolution,
    units                   = units,
    file_format_received    = "Parquet",
    date_received           = as.character(Sys.Date()),
    received_by             = received_by,
    method_of_receipt       = method_of_receipt,
    supplier_quality_flags  = supplier_quality_flags,
    known_limitations       = known_limitations,
    file_path               = file_path,
    notes                   = notes
  )

  dir.create(dirname(register_path), recursive = TRUE, showWarnings = FALSE)

  # Append to register if it exists, otherwise create with header
  # (fwrite(append = TRUE) never rewrites the header, so the field order in
  # `record` above is effectively the register's column contract)
  data.table::fwrite(record, register_path,
                     append = file.exists(register_path))

  message(sprintf("  Provenance record written: %s -> %s",
                  dataset_id, register_path))
  invisible(record)
}
