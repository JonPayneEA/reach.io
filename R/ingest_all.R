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
#   Parse each .all file independently. Write one Parquet per station per
#   source file into a temp subdirectory. Files are read in chunks to keep
#   memory flat regardless of input size.
#
# Pass 2 - .merge_all_parquets():
#   Group temp Parquet files by station number (= gauge_id), rbind each
#   group, deduplicate on datetime, normalise to the pipeline schema, and
#   write to output_dir/gauge_id=<id>/all_YYYYMMDD.parquet.
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
#'
#' @return Invisibly NULL. Side effect: writes Parquet files to `tmp_dir`.
#'
#' @noRd
.parse_all_blocks <- function(path, tmp_dir, chunk_size = 50000L,
                               encoding = "latin1") {

  dir.create(tmp_dir, showWarnings = FALSE, recursive = TRUE)

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

    dt[, timestamp      := as.POSIXct(Time.stamp,
                                      format = "%d/%m/%Y %H:%M:%S",
                                      tz     = "UTC")]
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

    # Separator __ is safe: station numbers are alphanumeric, param codes
    # contain only [a-z0-9_] after sanitisation.
    out_path <- file.path(tmp_dir, paste0(station_num, "__", param_code, ".parquet"))
    
    # Append to existing file for this station if one already exists in this
    # pass (multiple blocks for the same station within one .all file)
    if (file.exists(out_path)) {
      existing <- data.table::as.data.table(arrow::read_parquet(out_path))
      dt       <- data.table::rbindlist(list(existing, dt),
                                        use.names = TRUE, fill = TRUE)
    }
    
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

#' Merge intermediate Parquet files to final Bronze partitions
#'
#' Internal. Groups temp Parquet files by station number, rbinds, deduplicates
#' on datetime, normalises to the pipeline schema, and writes to Hive-
#' partitioned Bronze Parquet. Called by [ingest_all_file()].
#'
#' @param tmp_dir Character. Directory containing intermediate Parquet files
#'   from `.parse_all_blocks()`.
#' @param output_dir Character. Root Bronze output directory.
#' @param run_date Date. Used to name output files.
#'
#' @return A `data.table` log with columns `gauge_id`, `rows`, `out_file`.
#'
#' @noRd
.merge_all_parquets <- function(tmp_dir, output_dir, run_date,
                                register_path,
                                category = "hydrometric") {
  
  all_files <- list.files(tmp_dir, pattern = "\\.parquet$", full.names = TRUE)
  if (!length(all_files)) stop("No intermediate Parquet files found in: ", tmp_dir)
  
  station_ids <- tools::file_path_sans_ext(basename(all_files))
  file_groups <- split(all_files, station_ids)
  
  message(sprintf("Merging %d station(s) to Bronze...", length(file_groups)))
  
  log_rows <- vector("list", length(file_groups))
  
  for (k in seq_along(file_groups)) {
    # Keys are now "<station_num>__<param_code>" — split to recover each part.
    key        <- names(file_groups)[k]
    key_parts  <- strsplit(key, "__", fixed = TRUE)[[1L]]
    station    <- key_parts[1L]
    param_code <- if (length(key_parts) > 1L && nzchar(key_parts[2L])) key_parts[2L] else NULL
    files      <- file_groups[[key]]
    
    dt <- data.table::rbindlist(
      lapply(files, function(f) {
        data.table::as.data.table(arrow::read_parquet(f))
      }),
      use.names = TRUE, fill = TRUE
    )
    
    # Deduplicate on timestamp — overlapping records appear at export boundaries
    data.table::setorder(dt, timestamp)
    dt <- unique(dt, by = "timestamp")
    
    # Identify the value column — .all files use varying names; take the first
    # numeric column that is not a known metadata column
    meta_cols <- c("timestamp", "station_number", "station_name",
                   "parameter", "unit", "ts_name", "lon", "lat")
    value_col <- names(dt)[
      !names(dt) %in% meta_cols & sapply(dt, is.numeric)
    ][1L]
    
    # Quality flag column — named "Quality.Code" or similar in .all exports
    flag_col <- names(dt)[grepl("quality", names(dt), ignore.case = TRUE)][1L]
    
    # Derive the data_type code from the parameter metadata in the .all block
    # (dt$parameter holds the raw Kisters parameter name; map to framework code)
    raw_param  <- if ("parameter" %in% names(dt)) dt$parameter[1L] else NA_character_
    data_type  <- data.table::fcase(
      grepl("flow|discharge",  raw_param, ignore.case = TRUE), "Q",
      grepl("level|stage",     raw_param, ignore.case = TRUE), "H",
      grepl("rain",            raw_param, ignore.case = TRUE), "P",
      default = "Q"  # fallback; log for review
    )

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
    
    # Build a minimal pre-schema data.table for apply_bronze_schema()
    pre_dt <- data.table::data.table(
      datetime      = dt$timestamp,
      value         = if (!is.na(value_col)) dt[[value_col]] else NA_real_,
      supplier_flag = if (!is.na(flag_col))
        as.character(dt[[flag_col]]) else NA_character_
    )
    pre_dt <- pre_dt[!is.na(datetime) & !is.na(value)]

    # Guard: warn when off-grid timestamps appear in a 15-min series.
    # Bronze is written unchanged; resolution is deferred to Silver tier.
    if (identical(period_str, "15min")) {
      off_mins <- which(!as.integer(format(pre_dt[["datetime"]], "%M")) %in%
                          c(0L, 15L, 30L, 45L))
      if (length(off_mins) > 0L) {
        sample_ts <- head(
          format(pre_dt[["datetime"]][off_mins], "%Y-%m-%d %H:%M:%S"), 5L
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

    out_file <- bronze_path(output_dir, category, supplier_code, data_type, dataset_id)
    dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
    arrow::write_parquet(bronze_dt, out_file)

    write_provenance_record(
      register_path       = register_path,
      dataset_id          = dataset_id,
      supplier            = "WISKI",
      supplier_code       = supplier_code,
      site_id             = station,
      data_type           = data_type,
      time_period_start   = as.character(min(as.Date(bronze_dt$timestamp))),
      time_period_end     = as.character(max(as.Date(bronze_dt$timestamp))),
      temporal_resolution = period_str,
      method_of_receipt   = "WISKI .all export",
      file_path           = out_file
    )
    
    log_rows[[k]] <- data.table::data.table(
      gauge_id   = station,
      dataset_id = dataset_id,
      rows       = nrow(bronze_dt),
      out_file   = out_file
    )
    
    message(sprintf("  %s (%s): %s rows -> %s",
                    station, dataset_id,
                    format(nrow(bronze_dt), big.mark = ","),
                    out_file))
  }
  
  data.table::rbindlist(log_rows)
}


# -- Public entry point -------------------------------------------------------

#' Ingest a WISKI .all export file to partitioned Bronze Parquet
#'
#' Parses a WISKI/Kisters `.all` export file and writes per-station data to
#' Hive-partitioned Bronze Parquet (`gauge_id=<id>/all_YYYYMMDD.parquet`),
#' ready for consumption by [run_backfill()] and [run_incremental()].
#'
#' Uses a two-pass approach to handle files that may be 10GB+ and span
#' multiple years without loading the entire file into memory:
#'
#' Pass 1 reads the file in chunks of `chunk_size` lines, parsing each
#' station block and writing an intermediate Parquet per station to a temp
#' directory.
#'
#' Pass 2 groups the intermediate files by station number (which is used
#' directly as `gauge_id`), deduplicates on datetime, normalises to the
#' pipeline schema, and writes the final Bronze output.
#'
#' The temp directory is removed on completion unless `keep_tmp = TRUE`.
#'
#' Because `.all` files contain an arbitrary set of stations not known in
#' advance, passing `registry_path` will auto-register every discovered
#' station into the gauge registry after ingestion, with
#' `source_system = "WISKI_ALL"`, `live = FALSE`, and `backfill_done = TRUE`.
#' Only stations not already in the registry are added; existing entries are
#' left untouched.
#'
#' Output schema: `gauge_id` (character), `datetime` (POSIXct, UTC),
#' `value` (numeric), `unit` (character), `flag` (integer).
#'
#' @param path Character. Path to the `.all` file.
#' @param output_dir Character. Root Bronze output directory. Per-station
#'   subdirectories (`gauge_id=<id>/`) are created inside this path.
#' @param category Character. Data category from the gauge registry, e.g.
#'   `"hydrometric"`, `"radarH19"`, `"MOSES"`. Default `"hydrometric"`.
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
#'   in the `.all` file is added to the registry if not already present, with
#'   `source_system = "WISKI_ALL"`, `live = FALSE`, and `backfill_done = TRUE`.
#'   The registry is created if the path does not yet exist. Writes use a
#'   write-to-temp-then-rename pattern to avoid Windows error 1224 (Arrow
#'   memory-maps the file on read, blocking a direct overwrite on Windows).
#'
#' @return A `data.table` log with columns `gauge_id`, `dataset_id`, `rows`,
#'   and `out_file`, one row per station written. Returned invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Ingest a single .all file
#' log_dt <- ingest_all_file(
#'   path       = "data/wiski/2008_2010.all",
#'   output_dir = "data/fw_bronze/flow"
#' )
#'
#' # Ingest and auto-register discovered stations into the gauge registry
#' log_dt <- ingest_all_file(
#'   path          = "data/wiski/2008_2010.all",
#'   output_dir    = "data/fw_bronze/flow",
#'   registry_path = "data/fw_bronze/gauge_registry/gauge_registry.parquet"
#' )
#'
#' # Ingest multiple files covering different periods
#' for (f in list.files("data/wiski", pattern = "\\.all$", full.names = TRUE)) {
#'   ingest_all_file(f, output_dir = "data/fw_bronze/flow")
#' }
#'
#' # Read the Bronze output for one station
#' dt <- data.table::as.data.table(
#'   arrow::collect(
#'     arrow::open_dataset("data/fw_bronze/flow/gauge_id=2723TH")
#'   )
#' )
#' data.table::setkeyv(dt, c("gauge_id", "datetime"))
#' dt <- unique(dt, by = c("gauge_id", "datetime"))
#' }
ingest_all_file <- function(path,
                            output_dir,
                            category      = "hydrometric",
                            register_path = file.path(output_dir, "register",
                                                      "hydrometric_data_register.csv"),
                            chunk_size    = 50000L,
                            encoding      = "latin1",
                            keep_tmp      = FALSE,
                            registry_path = NULL) {

  if (!file.exists(path)) stop("File not found: ", path)

  run_date <- Sys.Date()
  tmp_dir  <- file.path(
    output_dir,
    paste0(".tmp_", tools::file_path_sans_ext(basename(path)))
  )

  message(sprintf("Pass 1: parsing %s", basename(path)))
  .parse_all_blocks(path, tmp_dir, chunk_size, encoding)

  message("Pass 2: merging to Bronze Parquet")
  log_dt <- .merge_all_parquets(tmp_dir, output_dir, run_date,
                                register_path, category)

  if (!keep_tmp) {
    unlink(tmp_dir, recursive = TRUE)
    message("Temp files removed.")
  }

  message(sprintf(
    "Ingest complete: %d station(s), %s total rows.",
    nrow(log_dt),
    format(sum(log_dt$rows), big.mark = ",")
  ))

  # -- Auto-register discovered stations into the gauge registry --------------
  if (!is.null(registry_path)) {
    .register_wiski_all_gauges(log_dt, registry_path, category, run_date)
  }

  invisible(log_dt)
}


# -- Registry helper ----------------------------------------------------------

#' Auto-register stations discovered from a .all ingest into the gauge registry
#'
#' Internal. Called by [ingest_all_file()] when `registry_path` is supplied.
#' Adds new stations (not already in the registry) with
#' `source_system = "WISKI_ALL"`, `live = FALSE`, and `backfill_done = TRUE`.
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

  reg_dir <- dirname(registry_path)
  dir.create(reg_dir, recursive = TRUE, showWarnings = FALSE)

  # Build candidate rows from ingest log
  new_rows <- data.table::data.table(
    gauge_id      = log_dt$gauge_id,
    source_system = "WISKI_ALL",
    data_type     = NA_character_,   # .all files may hold multiple; left as NA
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

    new_ids  <- new_rows$gauge_id[!new_rows$gauge_id %in% existing$gauge_id]
    to_add   <- new_rows[gauge_id %in% new_ids]
    registry <- data.table::rbindlist(
      list(existing, to_add),
      use.names = TRUE, fill = TRUE
    )
    message(sprintf(
      "Registry: %d new WISKI_ALL station(s) registered (%d already present).",
      nrow(to_add),
      nrow(existing[source_system == "WISKI_ALL"])
    ))
  } else {
    registry <- new_rows
    message(sprintf(
      "Registry created with %d WISKI_ALL station(s): %s",
      nrow(registry), registry_path
    ))
  }

  tmp_path <- paste0(registry_path, ".tmp")
  arrow::write_parquet(registry, tmp_path)
  file.rename(tmp_path, registry_path)
  invisible(NULL)
}


# -- Route gateway ------------------------------------------------------------

#' Fetch data from a WISKI .all export file
#'
#' Locates the `.all` file for a gauge under `WISKI_ALL_ROOT`, runs
#' [ingest_all_file()], and returns the resulting data filtered to the
#' requested date range. The file root is read from the environment variable
#' `WISKI_ALL_ROOT`. Within that root, files are expected at:
#' ```
#' <WISKI_ALL_ROOT>/<gauge_id>.all
#' ```
#'
#' Set the root path before use:
#' ```
#' Sys.setenv(WISKI_ALL_ROOT = "/mnt/wiski/exports")
#' ```
#'
#' @param gauge_id Character. Gauge identifier matching `gauge_id` in the
#'   registry, and the station number embedded in the `.all` file.
#' @param data_type Character. Parameter type — used for logging only as
#'   `.all` files carry all parameters for a station together.
#' @param start_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param end_date Character. End date in `"YYYY-MM-DD"` format.
#'
#' @return A `data.table` with columns `gauge_id`, `datetime` (POSIXct),
#'   `value` (numeric), `unit` (character), `flag` (integer), filtered to
#'   the requested date range. Returns an empty `data.table` if no file
#'   is found.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' Sys.setenv(WISKI_ALL_ROOT = "/mnt/wiski/exports")
#' fetch_from_wiski_all("2723TH", "flow", "2010-01-01", "2012-12-31")
#' }
fetch_from_wiski_all <- function(gauge_id, data_type, start_date, end_date) {
  
  message(sprintf("  [WISKI_ALL] %s | %s | %s to %s",
                  gauge_id, data_type, start_date, end_date))
  
  all_root <- Sys.getenv("WISKI_ALL_ROOT", unset = NA_character_)
  if (is.na(all_root)) {
    stop("[WISKI_ALL] WISKI_ALL_ROOT environment variable not set.")
  }
  
  all_file <- file.path(all_root, paste0(gauge_id, ".all"))
  if (!file.exists(all_file)) {
    warning(sprintf("[WISKI_ALL] No .all file found for gauge_id: %s at %s",
                    gauge_id, all_file))
    return(data.table::data.table())
  }
  
  # Ingest to a temp Bronze directory then read back and filter
  tmp_bronze <- tempfile()
  on.exit(unlink(tmp_bronze, recursive = TRUE))
  
  ingest_all_file(all_file, output_dir = tmp_bronze,
                  category = "hydrometric", keep_tmp = FALSE)
  
  part_dir <- file.path(tmp_bronze, paste0("gauge_id=", gauge_id))
  if (!dir.exists(part_dir)) return(data.table::data.table())
  
  dt <- data.table::as.data.table(
    arrow::collect(arrow::open_dataset(part_dir))
  )
  
  if (nrow(dt) == 0) return(dt)
  
  # Filter to requested date range
  from_dt <- as.POSIXct(start_date, tz = "UTC")
  to_dt   <- as.POSIXct(end_date,   tz = "UTC")
  dt[datetime >= from_dt & datetime <= to_dt,
     .(gauge_id, datetime, value, unit, flag)]
}