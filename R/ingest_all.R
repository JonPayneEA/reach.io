# ============================================================
# Tool:        WISKI .all File Ingestor
# Description: Two-pass parser for WISKI/Kisters .all export
#              files. Pass 1 chunks the file into per-station
#              intermediate Parquet. Pass 2 merges, dedupes,
#              normalises to Bronze schema, writes provenance.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      WISKI .all export file
# Outputs:     Bronze Parquet per station; provenance records
# Dependencies: data.table, arrow, tools
# ============================================================

# -- WISKI .all File Ingestor -------------------------------------------------
#
# .all files are proprietary Kisters/WISKI exports containing vertically
# stacked station blocks. Each block has a metadata header followed by a
# time series data section. Files can be 10gb+ and span multiple years.
#
# Two-pass approach:
#
# Pass 1 - .parse_all_blocks():
#   Parse the .all file in line chunks. Each station block is written as its
#   own intermediate Parquet (<station>__<param>__<nnn>.parquet) in a temp
#   subdirectory. Memory use is bounded by the largest single station block
#   (a block is held in memory while it is being read), not by the file size.
#
# Pass 2 - .merge_all_parquets():
#   Group temp Parquet files by station and parameter, rbind each group,
#   deduplicate on timestamp (last block wins), normalise to the Bronze
#   schema, and write year-partitioned Bronze Parquet via
#   .write_bronze_by_year().
#
# The public entry point ingest_all_file() runs both passes and cleans up
# the temp directory.


# -- Pass 1 -------------------------------------------------------------------

#' Parse a .all file to intermediate per-station Parquet files
#'
#' Internal. Reads a WISKI .all export in chunks and writes one raw Parquet
#' per station per source file to a temp subdirectory. Called by
#' [ingest_all_file()].
#'
#' @param path Character. Path to the .all file.
#' @param tmp_dir Character. Directory to write intermediate Parquet files.
#' @param chunk_size Integer. Lines to read per chunk. Default 50000.
#' @param encoding Character. File encoding. Default `"latin1"`.
#' @param tz Character. Time zone the `Time stamp` column is expressed in.
#'   Timestamps are parsed in `tz` and stored as UTC instants.
#'
#' @return Invisibly NULL. Side effect: writes Parquet files to `tmp_dir`.
#'
#' @noRd
.parse_all_blocks <- function(path, tmp_dir, chunk_size = 50000L,
                               encoding = "latin1", tz = "UTC") {

  dir.create(tmp_dir, showWarnings = FALSE, recursive = TRUE)
  block_n <- 0L

  con <- file(path, open = "r", encoding = encoding)
  on.exit(close(con))
  
  carried <- character(0)
  
  # Parse one station block and write raw Parquet to tmp_dir. Columns are
  # kept as-is from the .all file at this stage — normalisation happens in
  # pass 2 so the merge step has full metadata available.
  write_block <- function(block) {
    
    meta_lines <- block[grepl(":\t", block) & !startsWith(block, "Time stamp")]
    meta <- do.call(c, lapply(strsplit(meta_lines, ":\t"), function(x) {
      stats::setNames(list(trimws(x[2L])), trimws(x[1L]))
    }))
    
    header_i <- which(startsWith(block, "Time stamp,"))
    if (!length(header_i)) return(invisible(NULL))
    
    data_rows <- block[(header_i + 1L):length(block)]
    data_rows <- data_rows[nzchar(data_rows)]
    if (!length(data_rows)) return(invisible(NULL))

    # Pre-process data rows: when the Remarks field (last column) contains
    # commas, fread splits it into extra fields and stops early. Fix: collapse
    # overflow pieces back into the Remarks field and wrap it in CSV double-
    # quotes so fread treats its internal commas as content, not separators.
    n_expected <- length(strsplit(block[header_i], ",", fixed = TRUE)[[1L]])
    clean_rows <- vapply(data_rows, function(row) {
      parts <- strsplit(row, ",", fixed = TRUE)[[1L]]
      if (length(parts) <= n_expected) return(row)
      remarks <- paste(parts[n_expected:length(parts)], collapse = ",")
      # CSV-quote the Remarks field; escape any internal double-quotes as ""
      remarks_quoted <- paste0('"', gsub('"', '""', remarks, fixed = TRUE), '"')
      paste(c(parts[seq_len(n_expected - 1L)], remarks_quoted), collapse = ",")
    }, character(1L), USE.NAMES = FALSE)

    dt <- data.table::fread(
      text       = c(block[header_i], clean_rows),
      sep        = ",",
      na.strings = c("---", "")
    )

    data.table::setnames(dt, make.names(names(dt), unique = TRUE))
    
    station_num <- meta[["Station Number"]]
    if (is.null(station_num) || is.na(station_num)) return(invisible(NULL))

    # Parse in the file's time zone, then store as a UTC instant
    ts <- as.POSIXct(dt[["Time.stamp"]], format = "%d/%m/%Y %H:%M:%S", tz = tz)
    attr(ts, "tzone") <- "UTC"
    dt[, timestamp      := ts]
    dt[, Time.stamp     := NULL]
    dt[, station_number := station_num]
    dt[, station_name   := meta[["Station Name"]]      %||% NA_character_]
    dt[, parameter      := meta[["Parameter Name"]]    %||% NA_character_]
    dt[, unit           := meta[["Time series Unit"]]  %||% NA_character_]
    dt[, ts_name        := meta[["Time series Name"]]  %||% NA_character_]
    dt[, lon            := suppressWarnings(as.numeric(meta[["Longitude"]]))]
    dt[, lat            := suppressWarnings(as.numeric(meta[["Latitude"]]))]

    # Derive a short parameter code from the structured WISKI ts_name field.
    # ts_name follows the pattern <catchment>/<station>/<param_code>/<resolution>…
    # e.g. "THM/3401TH/SG_DS/15m.Cmd.RelAbs.P" → param_code = "sg_ds".
    # This is used to disambiguate multiple series (e.g. upstream vs downstream
    # stage) that share the same station number and data_type code.
    raw_ts     <- meta[["Time series Name"]] %||% NA_character_
    param_code <- if (!is.na(raw_ts)) {
      ts_parts <- strsplit(raw_ts, "/", fixed = TRUE)[[1L]]
      if (length(ts_parts) >= 3L) {
        tolower(gsub("[^A-Za-z0-9]+", "_", ts_parts[3L]))
      } else {
        tolower(gsub("[^A-Za-z0-9]+", "_", raw_ts))
      }
    } else {
      "unknown"
    }
    # No leading/trailing underscores, so "__" only ever separates file-name parts
    param_code <- gsub("^_+|_+$", "", param_code)
    if (!nzchar(param_code)) param_code <- "unknown"

    # One intermediate file per block. "__" separates station, parameter and
    # block number; station numbers and (sanitised) parameter codes never
    # contain it. Pass 2 groups blocks of the same series and concatenates them.
    block_n <<- block_n + 1L
    out_path <- file.path(tmp_dir, sprintf("%s__%s__%06d.parquet",
                                           station_num, param_code, block_n))
    arrow::write_parquet(dt, out_path)
    invisible(NULL)
  }
  
  repeat {
    raw <- readLines(con, n = chunk_size, warn = FALSE)
    
    if (!length(raw)) {
      if (length(carried)) write_block(carried)
      break
    }
    
    lines  <- c(carried, raw)
    starts <- which(startsWith(lines, "Station Site:"))
    
    if (!length(starts)) {
      carried <- lines
      next
    }
    
    for (i in seq_along(starts)) {
      block_end <- if (i < length(starts)) starts[i + 1L] - 1L else length(lines)
      block     <- lines[starts[i]:block_end]
      
      if (i < length(starts)) {
        write_block(block)
      } else {
        carried <- block
      }
    }
  }
  
  invisible(NULL)
}


# -- Pass 2 -------------------------------------------------------------------

#' Map a WISKI parameter name to a framework data type code
#'
#' Returns `NA` for anything not recognised as flow, water level/stage or
#' rainfall (including groundwater, soil, temperature, snow and evaporation),
#' so unknown parameters are skipped rather than silently filed under flow.
#' @noRd
.wiski_param_to_type <- function(raw_param) {
  if (length(raw_param) != 1L || is.na(raw_param)) return(NA_character_)
  if (grepl("ground|soil|temp|snow|evap", raw_param, ignore.case = TRUE))
    return(NA_character_)
  if (grepl("flow|discharge", raw_param, ignore.case = TRUE)) return("Q")
  if (grepl("level|stage",    raw_param, ignore.case = TRUE)) return("H")
  if (grepl("rain",           raw_param, ignore.case = TRUE)) return("P")
  NA_character_
}

#' Choose the value column of a merged .all series
#'
#' Prefers a column whose name starts with "value" (any type, so a series that
#' is entirely missing is not mistaken for a numeric quality-code column).
#' Otherwise falls back to the first numeric non-metadata, non-quality column
#' and warns, naming the column chosen.
#' @noRd
.pick_value_column <- function(dt, meta_cols, flag_col, station) {
  cand  <- setdiff(names(dt), c(meta_cols, flag_col))
  named <- cand[grepl("^value", cand, ignore.case = TRUE)]
  if (length(named) > 0L) return(named[1L])

  is_num <- vapply(cand, function(cn) is.numeric(dt[[cn]]), logical(1L))
  num    <- cand[is_num]
  if (length(num) == 0L) return(NA_character_)
  warning(sprintf(
    "[%s] No 'Value' column found; using '%s' (numeric candidates: %s).",
    station, num[1L], paste(num, collapse = ", ")
  ), call. = FALSE)
  num[1L]
}

#' Merge intermediate Parquet files to final Bronze partitions
#'
#' Internal. Groups the per-block intermediate files by station and parameter,
#' rbinds, deduplicates on timestamp (the last block wins), normalises to the
#' Bronze schema, and writes year-partitioned Bronze Parquet. Called by
#' [ingest_all_file()].
#'
#' @param tmp_dir Character. Directory containing intermediate Parquet files
#'   from `.parse_all_blocks()`.
#' @param output_dir Character. Root of the data store.
#' @param run_date Date. Used to name output files.
#' @param register_path Character. Provenance register CSV.
#' @param category Character. Bronze data category.
#'
#' @return A `data.table` log with columns `gauge_id`, `data_type`,
#'   `dataset_id`, `rows`, `out_file`. Series that could not be ingested are
#'   listed in `attr(log, "skipped")` (`station`, `parameter`, `reason`).
#'
#' @noRd
.merge_all_parquets <- function(tmp_dir, output_dir, run_date,
                                register_path,
                                category = "hydrometric") {

  all_files <- list.files(tmp_dir, pattern = "\\.parquet$", full.names = TRUE)
  if (!length(all_files)) stop("No intermediate Parquet files found in: ", tmp_dir)

  # <station>__<param>__<block>.parquet -> group on <station>__<param>
  series_key  <- sub("__[0-9]+$", "", tools::file_path_sans_ext(basename(all_files)))
  file_groups <- split(all_files, series_key)

  message(sprintf("Merging %d series to Bronze...", length(file_groups)))

  log_rows <- vector("list", length(file_groups))
  skipped  <- list()
  prov_all <- list()

  for (k in seq_along(file_groups)) {
    key        <- names(file_groups)[k]
    key_parts  <- strsplit(key, "__", fixed = TRUE)[[1L]]
    station    <- key_parts[1L]
    param_code <- if (length(key_parts) > 1L && nzchar(key_parts[2L])) key_parts[2L] else NULL
    # Block files sort by their zero-padded counter, i.e. file order
    files      <- sort(file_groups[[key]])

    dt <- data.table::rbindlist(
      lapply(files, function(f) {
        data.table::as.data.table(arrow::read_parquet(f))
      }),
      use.names = TRUE, fill = TRUE
    )

    # Timestamps that failed to parse (wrong format, or a time that does not
    # exist / is ambiguous in the file's time zone)
    n_bad_ts <- sum(is.na(dt$timestamp))
    if (n_bad_ts > 0L) {
      warning(sprintf("[%s] %d row(s) had an unparseable timestamp and were dropped.",
                      station, n_bad_ts), call. = FALSE)
      dt <- dt[!is.na(timestamp)]
    }

    # Deduplicate on timestamp — overlapping records appear at export
    # boundaries. setorder() is stable, so ties stay in block order and
    # fromLast = TRUE keeps the row from the last block.
    data.table::setorder(dt, timestamp)
    dt <- unique(dt, by = "timestamp", fromLast = TRUE)

    # Quality flag column — named "Quality.Code" or similar in .all exports
    flag_col <- names(dt)[grepl("quality", names(dt), ignore.case = TRUE)][1L]
    if (is.na(flag_col)) flag_col <- NULL

    meta_cols <- c("timestamp", "station_number", "station_name",
                   "parameter", "unit", "ts_name", "lon", "lat")
    value_col <- .pick_value_column(dt, meta_cols, flag_col, station)

    # Derive the data_type code from the parameter metadata in the .all block
    raw_param <- if ("parameter" %in% names(dt)) dt$parameter[1L] else NA_character_
    data_type <- .wiski_param_to_type(raw_param)
    if (is.na(data_type)) {
      warning(sprintf(
        "[%s] Unrecognised parameter '%s' — series skipped (supported: flow/discharge, level/stage, rainfall).",
        station, raw_param), call. = FALSE)
      skipped[[length(skipped) + 1L]] <- data.table::data.table(
        station = station, parameter = raw_param, reason = "unrecognised parameter")
      next
    }
    if (is.na(value_col)) {
      warning(sprintf("[%s] No usable value column — series skipped.", station),
              call. = FALSE)
      skipped[[length(skipped) + 1L]] <- data.table::data.table(
        station = station, parameter = raw_param, reason = "no value column")
      next
    }

    # Detect temporal resolution — try ts_name metadata first (faster), then
    # fall back to estimating from the median timestep.
    # WISKI ts_name examples (Kisters style): "Mean 15min (Mrt0)", "Daily Mean (Mrt0)",
    # "Hourly Mean (Mrt0)", "15 Min Instantaneous"
    # WISKI ts_name examples (path style): "THM/3401TH/SG/15m.Cmd.RelAbs.P"
    # The \b15m\b pattern matches "15m" as a whole word (surrounded by / or .)
    # without also matching "315m" or the "15m" within "15min".
    raw_ts_name <- if ("ts_name" %in% names(dt)) dt$ts_name[1L] else NA_character_
    period_str  <- data.table::fcase(
      grepl("15[- ]?min|\\b15m\\b",        raw_ts_name, ignore.case = TRUE, perl = TRUE), "15min",
      grepl("hourly|1.?hour|60.?min",       raw_ts_name, ignore.case = TRUE), "hourly",
      grepl("daily|day",                    raw_ts_name, ignore.case = TRUE), "daily",
      default = NA_character_
    )

    if (is.na(period_str) && nrow(dt) > 1L) {
      # Estimate from median gap between sorted timestamps (seconds).
      # Guards: filter out zero-gaps (duplicates) before taking median.
      ts_sorted <- sort(unique(dt$timestamp))
      if (length(ts_sorted) > 1L) {
        gaps <- as.numeric(diff(ts_sorted), units = "secs")
        gaps <- gaps[gaps > 0]
        if (length(gaps) > 0) {
          med_gap <- stats::median(gaps, na.rm = TRUE)
          period_str <- data.table::fcase(
            med_gap <= 960,   "15min",
            med_gap <= 3900,  "hourly",
            med_gap <= 90000, "daily",
            default           = "unknown"
          )
        }
      }
    }
    period_str <- period_str %||% "unknown"
    if (is.na(period_str)) period_str <- "unknown"

    # Build a minimal pre-schema data.table for apply_bronze_schema()
    pre_dt <- data.table::data.table(
      datetime      = dt$timestamp,
      value         = suppressWarnings(as.numeric(dt[[value_col]])),
      supplier_flag = if (!is.null(flag_col))
        as.character(dt[[flag_col]]) else NA_character_
    )
    n_missing <- sum(is.na(pre_dt$value))
    if (n_missing > 0L) {
      message(sprintf("  [%s] %s of %s row(s) have no value and were dropped.",
                      station, format(n_missing, big.mark = ","),
                      format(nrow(pre_dt), big.mark = ",")))
    }
    pre_dt <- pre_dt[!is.na(value)]
    if (nrow(pre_dt) == 0L) {
      skipped[[length(skipped) + 1L]] <- data.table::data.table(
        station = station, parameter = raw_param, reason = "no values")
      next
    }

    # Guard: warn when off-grid timestamps appear in a 15-min series.
    # Bronze is written unchanged; resolution is the job of snap_offgrid_timestamps().
    if (identical(period_str, "15min")) {
      off_mins <- which(!as.integer(format(pre_dt[["datetime"]], "%M", tz = "UTC")) %in%
                          c(0L, 15L, 30L, 45L))
      if (length(off_mins) > 0L) {
        sample_ts <- head(
          format(pre_dt[["datetime"]][off_mins], "%Y-%m-%d %H:%M:%S", tz = "UTC"), 5L
        )
        warning(
          sprintf(
            "[%s] %d off-grid timestamp(s) in 15-min series (not :00/:15/:30/:45); written to Bronze unchanged. Sample: %s",
            station, length(off_mins), paste(sample_ts, collapse = ", ")
          ),
          call. = FALSE
        )
      }
    }

    # -- Apply Bronze schema and write to framework folder structure ----------
    supplier_code <- "WISKI"
    dataset_id    <- make_dataset_id(supplier_code, station, data_type,
                                     received_date = run_date,
                                     trace = param_code)

    bronze_dt <- apply_bronze_schema(
      dt                = pre_dt,
      dataset_id        = dataset_id,
      site_id           = station,
      data_type         = data_type,
      timestamp_col     = "datetime",
      value_col         = "value",
      supplier_flag_col = "supplier_flag",
      period            = period_str
    )

    out_files <- .write_bronze_by_year(bronze_dt, output_dir, category,
                                       supplier_code, data_type, dataset_id)

    prov_all[[length(prov_all) + 1L]] <- .bronze_prov_rows(
      out_files,
      dataset_id          = dataset_id,
      supplier            = "WISKI",
      supplier_code       = supplier_code,
      site_id             = station,
      data_type           = data_type,
      temporal_resolution = period_str,
      method_of_receipt   = "WISKI .all export"
    )

    log_rows[[k]] <- data.table::data.table(
      gauge_id   = station,
      data_type  = data_type,
      dataset_id = dataset_id,
      rows       = nrow(bronze_dt),
      out_file   = paste(out_files$path, collapse = "; ")
    )

    message(sprintf("  %s (%s): %s rows -> %s year file(s)",
                    station, dataset_id,
                    format(nrow(bronze_dt), big.mark = ","),
                    nrow(out_files)))
  }

  .write_provenance_rows(data.table::rbindlist(prov_all, fill = TRUE),
                         register_path)

  log_dt <- data.table::rbindlist(Filter(Negate(is.null), log_rows))
  attr(log_dt, "skipped") <- data.table::rbindlist(skipped, fill = TRUE)
  log_dt
}


# -- Public entry point -------------------------------------------------------

#' Ingest a WISKI .all export file to partitioned Bronze Parquet
#'
#' Parses a WISKI/Kisters `.all` export file and writes each station's series
#' to the Bronze tier as Parquet, one file per calendar year of observations:
#' `bronze/<category>/WISKI/<data_type>/<YYYY>/<dataset_id>.parquet`. Read the
#' result back with [read_bronze()] (`supplier_code = "WISKI"`).
#'
#' Uses a two-pass approach so files that may be 10GB+ and span multiple years
#' need not be loaded whole:
#'
#' Pass 1 reads the file in chunks of `chunk_size` lines, parsing each
#' station block and writing an intermediate Parquet per block to a temp
#' directory. Memory use is bounded by the largest single station block, which
#' is held in memory while it is read — not by the size of the file.
#'
#' Pass 2 groups the intermediate files by station and parameter (the station
#' number is used directly as `gauge_id`), deduplicates on timestamp (where
#' blocks overlap the later block wins), normalises to the Bronze schema, and
#' writes the final output with a provenance record per file.
#'
#' Series whose parameter is not recognised as flow/discharge, level/stage or
#' rainfall (for example groundwater or temperature) are **skipped with a
#' warning** rather than filed under another type; they are listed in
#' `attr(log, "skipped")`. Rows with an unparseable timestamp are dropped with
#' a warning giving the count.
#'
#' The temp directory is removed on completion unless `keep_tmp = TRUE`.
#'
#' Because `.all` files contain an arbitrary set of stations not known in
#' advance, passing `registry_path` will auto-register every discovered
#' station and data type into the gauge registry after ingestion, with
#' `source_system = "WISKI_ALL"`, `live = FALSE`, and `backfill_done = TRUE`.
#' Only (station, data type) combinations not already in the registry are
#' added; existing entries are left untouched.
#'
#' @param path Character. Path to the `.all` file.
#' @param output_dir Character. Root of the data store. Bronze files are
#'   written under `output_dir/bronze/`.
#' @param category Character. Data category from the gauge registry, e.g.
#'   `"hydrometric"`, `"radarH19"`, `"MOSES"`. Default `"hydrometric"`.
#' @param register_path Character. Provenance register CSV. Defaults to
#'   `<output_dir>/register/hydrometric_data_register.csv`.
#' @param chunk_size Integer. Lines to read per chunk in pass 1. Default
#'   50000. Reduce if memory is constrained; increase for faster I/O on
#'   large disks.
#' @param encoding Character. File encoding passed to [base::file()]. Default
#'   `"latin1"`, which covers Windows-1252 — the encoding used by WISKI
#'   exports from EA Windows systems. Change to `"UTF-8"` if your `.all`
#'   files are known to be UTF-8 encoded.
#' @param keep_tmp Logical. Keep intermediate Parquet files after pass 2?
#'   Default `FALSE`. Set `TRUE` for debugging or to re-run pass 2 without
#'   re-parsing.
#' @param registry_path Character or `NULL`. Path to an existing
#'   `gauge_registry.parquet` file. When supplied, every station discovered
#'   in the `.all` file is added to the registry if not already present (see
#'   above). The registry is created if the path does not yet exist. Writes use
#'   a write-to-temp-then-rename pattern to avoid Windows error 1224 (Arrow
#'   memory-maps the file on read, blocking a direct overwrite on Windows).
#' @param tz Character. Time zone in which the `Time stamp` column of the
#'   export is expressed. Timestamps are parsed in this zone and stored as UTC
#'   instants. Default `"UTC"`. If the export was produced in UK local time,
#'   use `"Europe/London"` (times that do not exist or are ambiguous at DST
#'   changes become `NA` and are dropped with a warning).
#'
#' @return A `data.table` log with columns `gauge_id`, `data_type`,
#'   `dataset_id`, `rows`, and `out_file` (the year files written, joined by
#'   `"; "`), one row per series written. Returned invisibly. Skipped series
#'   are in `attr(log, "skipped")`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Ingest a single .all file
#' log_dt <- ingest_all_file(
#'   path       = "data/wiski/2008_2010.all",
#'   output_dir = "data/hydrometric"
#' )
#'
#' # Ingest and auto-register discovered stations into the gauge registry
#' log_dt <- ingest_all_file(
#'   path          = "data/wiski/2008_2010.all",
#'   output_dir    = "data/hydrometric",
#'   registry_path = "data/hydrometric/register/gauge_registry.parquet"
#' )
#'
#' # Ingest multiple files covering different periods
#' for (f in list.files("data/wiski", pattern = "\\.all$", full.names = TRUE)) {
#'   ingest_all_file(f, output_dir = "data/hydrometric")
#' }
#'
#' # Read the Bronze output for one station back as an S7 object
#' obj <- read_bronze("data/hydrometric", "2723TH", "H", supplier_code = "WISKI")
#' }
ingest_all_file <- function(path,
                            output_dir,
                            category      = "hydrometric",
                            register_path = file.path(output_dir, "register",
                                                      "hydrometric_data_register.csv"),
                            chunk_size    = 50000L,
                            encoding      = "latin1",
                            keep_tmp      = FALSE,
                            registry_path = NULL,
                            tz            = "UTC") {

  if (!file.exists(path)) stop("File not found: ", path)

  run_date <- Sys.Date()
  tmp_dir  <- file.path(
    output_dir,
    paste0(".tmp_", tools::file_path_sans_ext(basename(path)))
  )
  on.exit(if (!keep_tmp) unlink(tmp_dir, recursive = TRUE), add = TRUE)

  message(sprintf("Pass 1: parsing %s", basename(path)))
  .parse_all_blocks(path, tmp_dir, chunk_size, encoding, tz)

  message("Pass 2: merging to Bronze Parquet")
  log_dt <- .merge_all_parquets(tmp_dir, output_dir, run_date,
                                register_path, category)

  if (nrow(log_dt) == 0L) {
    message("Ingest complete: no series were written.")
  } else {
    message(sprintf(
      "Ingest complete: %d series, %s total rows.",
      nrow(log_dt),
      format(sum(log_dt$rows), big.mark = ",")
    ))
  }

  # -- Auto-register discovered stations into the gauge registry --------------
  if (!is.null(registry_path) && nrow(log_dt) > 0L) {
    .register_wiski_all_gauges(log_dt, registry_path, category, run_date)
  }

  invisible(log_dt)
}


# -- Registry helper ----------------------------------------------------------

#' Auto-register stations discovered from a .all ingest into the gauge registry
#'
#' Internal. Called by [ingest_all_file()] when `registry_path` is supplied.
#' Adds new (station, data type) combinations — those not already in the
#' registry — with `source_system = "WISKI_ALL"`, `live = FALSE`, and
#' `backfill_done = TRUE`.
#'
#' @param log_dt data.table returned by `.merge_all_parquets()`.
#' @param registry_path Character. Path to `gauge_registry.parquet`.
#' @param category Character. Data category for the new rows.
#' @param run_date Date. Ingestion date used as `date_added`.
#'
#' @return Invisibly NULL.
#' @noRd
.register_wiski_all_gauges <- function(log_dt, registry_path, category,
                                       run_date) {

  dir.create(dirname(registry_path), recursive = TRUE, showWarnings = FALSE)

  # Registry data_type uses parameter names (flow / level / rainfall)
  code_to_param <- c(Q = "flow", H = "level", P = "rainfall")

  cand <- unique(data.table::data.table(
    gauge_id  = log_dt$gauge_id,
    data_type = unname(code_to_param[log_dt$data_type])
  ))

  new_rows <- data.table::data.table(
    gauge_id      = cand$gauge_id,
    source_system = "WISKI_ALL",
    data_type     = cand$data_type,
    category      = category,
    catchment     = NA_character_,
    ea_site_ref   = NA_character_,
    active        = TRUE,
    live          = FALSE,           # historical dump, not a live feed
    date_added    = run_date,
    backfill_done = TRUE,            # just ingested
    notes         = NA_character_
  )

  if (file.exists(registry_path)) {
    existing <- data.table::as.data.table(arrow::read_parquet(registry_path))

    # Ensure backward compatibility with registries built before `live` existed
    if (!"live" %in% names(existing)) existing[, live := TRUE]

    is_new   <- !.registry_key(new_rows$gauge_id, new_rows$data_type) %in%
                .registry_key(existing$gauge_id, existing$data_type)
    to_add   <- new_rows[is_new]
    registry <- data.table::rbindlist(
      list(existing, to_add),
      use.names = TRUE, fill = TRUE
    )
    message(sprintf(
      "Registry: %d new WISKI_ALL series registered (%d already present).",
      nrow(to_add), sum(!is_new)
    ))
  } else {
    registry <- new_rows
    message(sprintf(
      "Registry created with %d WISKI_ALL series: %s",
      nrow(registry), registry_path
    ))
  }

  .write_parquet_atomic(registry, registry_path)
  invisible(NULL)
}


# -- Route gateway ------------------------------------------------------------

#' Fetch data from a WISKI .all export file
#'
#' Locates the `.all` file for a gauge under `WISKI_ALL_ROOT`, runs
#' [ingest_all_file()] into a temporary store, and returns the resulting
#' Bronze-schema data for the requested data type and date range. The file
#' root is read from the environment variable `WISKI_ALL_ROOT`. Within that
#' root, files are expected at:
#' ```
#' <WISKI_ALL_ROOT>/<gauge_id>.all
#' ```
#'
#' Set the root path before use:
#' ```
#' Sys.setenv(WISKI_ALL_ROOT = "/mnt/wiski/exports")
#' ```
#'
#' The whole `.all` file is parsed on every call, so for large exports prefer
#' [ingest_all_file()] once rather than fetching gauge by gauge.
#'
#' @param gauge_id Character. Gauge identifier matching `gauge_id` in the
#'   registry, and the station number embedded in the `.all` file.
#' @param data_type Character. Parameter type: `"flow"`, `"level"`, or
#'   `"rainfall"`. A `.all` file carries all parameters for a station
#'   together; this selects which to return.
#' @param start_date Character. Start date (inclusive) in `"YYYY-MM-DD"`
#'   format.
#' @param end_date Character. End date (**exclusive**) in `"YYYY-MM-DD"`
#'   format.
#'
#' @return A `data.table` conforming to the Bronze schema (`timestamp`,
#'   `value`, `supplier_flag`, `dataset_id`, `site_id`, `data_type`, `period`),
#'   filtered to the requested date range. A station with several series of the
#'   requested type (e.g. upstream and downstream stage) returns all of them,
#'   distinguished by `dataset_id`. Returns an empty `data.table` if no file or
#'   matching series is found.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' Sys.setenv(WISKI_ALL_ROOT = "/mnt/wiski/exports")
#' fetch_from_wiski_all("2723TH", "flow", "2010-01-01", "2013-01-01")
#' }
fetch_from_wiski_all <- function(gauge_id, data_type, start_date, end_date) {

  message(sprintf("  [WISKI_ALL] %s | %s | %s to %s",
                  gauge_id, data_type, start_date, end_date))

  all_root <- Sys.getenv("WISKI_ALL_ROOT", unset = NA_character_)
  if (is.na(all_root)) {
    stop("[WISKI_ALL] WISKI_ALL_ROOT environment variable not set.")
  }

  dt_code <- param_to_data_type(data_type)
  if (length(dt_code) != 1L || is.na(dt_code)) {
    stop(sprintf("[WISKI_ALL] Unsupported data_type '%s'.", data_type))
  }

  all_file <- file.path(all_root, paste0(gauge_id, ".all"))
  if (!file.exists(all_file)) {
    warning(sprintf("[WISKI_ALL] No .all file found for gauge_id: %s at %s",
                    gauge_id, all_file))
    return(data.table::data.table())
  }

  # Ingest to a temporary store, then read the requested series back
  tmp_store <- tempfile("wiski_all_")
  on.exit(unlink(tmp_store, recursive = TRUE), add = TRUE)

  ingest_all_file(all_file, output_dir = tmp_store,
                  category = "hydrometric", keep_tmp = FALSE)

  files <- .list_bronze_files(tmp_store, gauge_id, dt_code,
                              supplier_code = "WISKI",
                              category      = "hydrometric")
  if (length(files) == 0L) return(data.table::data.table())

  dt <- data.table::rbindlist(lapply(files, function(f) {
    data.table::as.data.table(arrow::read_parquet(f))
  }))
  if (nrow(dt) == 0L) return(dt)

  from_dt <- as.POSIXct(start_date, tz = "UTC")
  to_dt   <- as.POSIXct(end_date,   tz = "UTC")
  dt[dt$timestamp >= from_dt & dt$timestamp < to_dt]
}
