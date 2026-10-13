# ============================================================
# Tool:        Orchestration Helpers
# Description: Internal helpers shared by run_backfill() and
#              run_incremental(): worker/plan management, registry
#              keys and flag updates, log appending, and storing a
#              fetched Bronze table with provenance rows.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-10-12
# Tier:        1
# Inputs:      Fetched Bronze data.tables; gauge registry rows
# Outputs:     Bronze Parquet files; provenance rows; registry updates
# Dependencies: data.table, arrow, future
# ============================================================

# -- Workers ------------------------------------------------------------------

#' Default number of parallel workers
#'
#' One fewer than the detected cores, capped at 4. The EA Hydrology API
#' fair-use guidance asks for one request at a time, so large parallelism is
#' deliberately not the default; raise `n_workers` only for sources that are
#' not rate limited (e.g. local WISKI `.all` files).
#' @noRd
.default_workers <- function() {
  n <- parallel::detectCores()
  # detectCores() can return NA on platforms where the count is unknown
  if (is.na(n)) n <- 1L
  # Leave one core for the main process (which owns all register writes), cap
  # at 4 for the API fair-use reason above, and never go below one worker
  max(1L, min(4L, n - 1L))
}

#' Set the future plan for an orchestrator run
#'
#' `n_workers <= 1` uses the sequential plan (no background R sessions, and
#' mocks / `browser()` work). Returns the previous plan so the caller can
#' restore it with `on.exit(future::plan(old))`.
#' @noRd
.plan_workers <- function(n_workers) {
  # Coerce first so "4" and 4.0 are accepted; anything that does not become a
  # single positive integer (including text, which coerces to NA) is rejected
  # before any plan is changed
  n_workers <- suppressWarnings(as.integer(n_workers))
  if (length(n_workers) != 1L || is.na(n_workers) || n_workers < 1L)
    stop("`n_workers` must be a single integer >= 1.", call. = FALSE)
  # The value of the if/else is the return value: future::plan() returns the
  # plan that was in force before it was replaced
  if (n_workers == 1L) {
    future::plan(future::sequential)
  } else {
    # multisession (separate background R sessions) works on all platforms,
    # including Windows, where forking is unavailable
    future::plan(future::multisession, workers = n_workers)
  }
}


# -- Registry -----------------------------------------------------------------

#' Registry row key: `gauge_id` + `data_type` (NA data_type -> "")
#' @noRd
.registry_key <- function(gauge_id, data_type) {
  # A gauge can have several rows (one per data type), so gauge_id alone is not
  # a key. NA is mapped to "" so a missing data_type gives "<id>|" rather than
  # "<id>|NA", and the same series always yields the same key.
  paste(gauge_id, ifelse(is.na(data_type), "", data_type), sep = "|")
}

#' Set a registry column to a value for the given keys
#'
#' Reads the registry, updates matching rows, and writes it back atomically.
#'
#' @param registry_path Character. `gauge_registry.parquet`.
#' @param keys Character. Keys from `.registry_key()`.
#' @param col Character. Column to set.
#' @param value Scalar value.
#'
#' The registry is a read-modify-write of the whole Parquet file, so it must
#' only be called from the main process (`run_backfill()` calls it between
#' batches, never from workers); two writers at once would lose an update.
#' @noRd
.update_registry_flags <- function(registry_path, keys, col, value) {
  # Nothing to flag: skip the read and rewrite entirely
  if (length(keys) == 0L) return(invisible(NULL))
  reg <- data.table::as.data.table(.read_parquet(registry_path))
  # If the flag column is not in the registry yet it is added here; rows not
  # updated below keep NA
  if (!col %in% names(reg)) reg[, (col) := NA]
  # Rows are located with a plain logical vector and which(), and written with
  # set(), so neither `col` nor `value` (arguments here) can be confused with
  # a registry column inside dt[...]
  hit <- .registry_key(reg$gauge_id, reg$data_type) %in% keys
  # Only rewrite the file when something changed
  if (any(hit)) {
    data.table::set(reg, i = which(hit), j = col, value = value)
    .write_parquet_atomic(reg, registry_path)
  }
  invisible(NULL)
}


# -- Logs ---------------------------------------------------------------------

#' Append a run log to an existing CSV log (union of columns)
#'
#' Reads the existing log, binds the new rows (filling columns that differ)
#' and rewrites it, so the header always matches the data even when the log
#' schema has changed between versions.
#' @noRd
.append_log <- function(log_dt, log_path) {
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  if (file.exists(log_path)) {
    # Everything is handled as character on both sides: the old file's column
    # types are whatever fread guessed, and a column such as gauge_id can be
    # numeric in one run and text in another. Character always binds; the cost
    # is that the rewritten log is all-character text.
    old <- data.table::fread(log_path, colClasses = "character")
    # use.names + fill: match columns by name, missing ones become NA
    log_dt <- data.table::rbindlist(
      list(old, log_dt[, lapply(.SD, as.character)]),
      use.names = TRUE, fill = TRUE
    )
  }
  # The whole log is rewritten (not appended), which is what lets the header
  # change when columns are added. Not atomic, so call from the main process.
  data.table::fwrite(log_dt, log_path)
  invisible(log_dt)
}


# -- Storing a fetched table --------------------------------------------------

#' Write a fetched Bronze table to the store and build its provenance rows
#'
#' `route_gauge()` returns data already in the Bronze schema. A table can hold
#' several series (e.g. WISKI traces), identified by `dataset_id`; each is
#' written separately. Provenance rows are *returned*, not written, so this is
#' safe to call from parallel workers.
#'
#' Series are written one after another with no rollback: if a later series
#' fails (for example an undeterminable data type), the files of earlier series
#' stay on disk, but no provenance rows are returned because the error escapes
#' before the function returns. The caller records the gauge as `FAILED`.
#'
#' @param data_dt Bronze-schema `data.table`.
#' @param gauge One-row registry `data.table`.
#' @param output_dir Character. Store root.
#' @param method_of_receipt Character. Recorded in the register.
#'
#' @return A `data.table` of provenance rows (see `.bronze_prov_rows()`).
#' @noRd
.store_bronze <- function(data_dt, gauge, output_dir, method_of_receipt) {

  # -- Resolve where the data belongs ------------------------------------------
  # The registry's source_system picks the supplier directory (HDE -> EA,
  # WISKI and WISKI_ALL -> WISKI). An unrecognised value stops here, before any
  # file is written, rather than creating a directory under an NA supplier.
  supplier_code <- source_to_supplier(gauge$source_system)
  if (length(supplier_code) != 1L || is.na(supplier_code))
    stop(sprintf("Unknown source_system '%s' for gauge %s.",
                 gauge$source_system, gauge$gauge_id), call. = FALSE)

  # A missing or NA category falls back to "hydrometric"
  category <- gauge$category
  if (is.null(category) || is.na(category)) category <- "hydrometric"

  # The registry stores a parameter name ("flow"); convert it to a framework
  # code only as a fallback for data that does not carry its own data_type
  reg_type <- gauge$data_type
  if (is.null(reg_type)) reg_type <- NA_character_
  fallback_type <- param_to_data_type(reg_type)

  # -- Write each series ---------------------------------------------------------
  # One table can hold several series (WISKI traces share a gauge and data type
  # but have different dataset_ids); each gets its own files and provenance.
  # Rows are picked with which() on a local vector so that `id` / `ids` can
  # never be read as columns of data_dt.
  ids   <- data_dt$dataset_id
  prov  <- vector("list", length(unique(ids)))
  for (k in seq_along(unique(ids))) {
    id   <- unique(ids)[k]
    part <- data_dt[which(ids == id)]

    # Prefer the data_type carried in the data (set by the fetcher); the
    # registry-derived code is used only when the data has none
    dt_code <- part$data_type[1L]
    if (is.na(dt_code)) dt_code <- fallback_type
    if (is.na(dt_code))
      stop(sprintf("Cannot determine data_type for gauge %s.", gauge$gauge_id),
           call. = FALSE)

    # Splits by observation year, so one series becomes one file per year; the
    # same dataset_id (carrying the fetch date) is reused for every year file
    files <- .write_bronze_by_year(part, output_dir, category, supplier_code,
                                   dt_code, id)
    # Rows are only built here; the caller appends them to the register from the
    # main process. The provenance `supplier` is the registry source_system (e.g.
    # "HDE"), distinct from the directory code (e.g. "EA"), and `site_id` is
    # the registry gauge_id. A NA period is recorded as "unknown" by
    # .bronze_prov_rows().
    prov[[k]] <- .bronze_prov_rows(
      files,
      dataset_id          = id,
      supplier            = gauge$source_system,
      supplier_code       = supplier_code,
      site_id             = gauge$gauge_id,
      data_type           = dt_code,
      temporal_resolution = part$period[1L],
      method_of_receipt   = method_of_receipt
    )
  }
  # One provenance row per file written, across all series
  data.table::rbindlist(prov)
}
