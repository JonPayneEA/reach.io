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
  if (is.na(n)) n <- 1L
  max(1L, min(4L, n - 1L))
}

#' Set the future plan for an orchestrator run
#'
#' `n_workers <= 1` uses the sequential plan (no background R sessions, and
#' mocks / `browser()` work). Returns the previous plan so the caller can
#' restore it with `on.exit(future::plan(old))`.
#' @noRd
.plan_workers <- function(n_workers) {
  n_workers <- suppressWarnings(as.integer(n_workers))
  if (length(n_workers) != 1L || is.na(n_workers) || n_workers < 1L)
    stop("`n_workers` must be a single integer >= 1.", call. = FALSE)
  if (n_workers == 1L) {
    future::plan(future::sequential)
  } else {
    future::plan(future::multisession, workers = n_workers)
  }
}


# -- Registry -----------------------------------------------------------------

#' Registry row key: `gauge_id` + `data_type` (NA data_type -> "")
#' @noRd
.registry_key <- function(gauge_id, data_type) {
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
#' @noRd
.update_registry_flags <- function(registry_path, keys, col, value) {
  if (length(keys) == 0L) return(invisible(NULL))
  reg <- data.table::as.data.table(arrow::read_parquet(registry_path))
  if (!col %in% names(reg)) reg[, (col) := NA]
  hit <- .registry_key(reg$gauge_id, reg$data_type) %in% keys
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
    old <- data.table::fread(log_path, colClasses = "character")
    log_dt <- data.table::rbindlist(
      list(old, log_dt[, lapply(.SD, as.character)]),
      use.names = TRUE, fill = TRUE
    )
  }
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
#' @param data_dt Bronze-schema `data.table`.
#' @param gauge One-row registry `data.table`.
#' @param output_dir Character. Store root.
#' @param method_of_receipt Character. Recorded in the register.
#'
#' @return A `data.table` of provenance rows (see `.bronze_prov_rows()`).
#' @noRd
.store_bronze <- function(data_dt, gauge, output_dir, method_of_receipt) {

  supplier_code <- source_to_supplier(gauge$source_system)
  if (length(supplier_code) != 1L || is.na(supplier_code))
    stop(sprintf("Unknown source_system '%s' for gauge %s.",
                 gauge$source_system, gauge$gauge_id), call. = FALSE)

  category <- gauge$category
  if (is.null(category) || is.na(category)) category <- "hydrometric"

  reg_type <- gauge$data_type
  if (is.null(reg_type)) reg_type <- NA_character_
  fallback_type <- param_to_data_type(reg_type)

  ids   <- data_dt$dataset_id
  prov  <- vector("list", length(unique(ids)))
  for (k in seq_along(unique(ids))) {
    id   <- unique(ids)[k]
    part <- data_dt[which(ids == id)]

    dt_code <- part$data_type[1L]
    if (is.na(dt_code)) dt_code <- fallback_type
    if (is.na(dt_code))
      stop(sprintf("Cannot determine data_type for gauge %s.", gauge$gauge_id),
           call. = FALSE)

    files <- .write_bronze_by_year(part, output_dir, category, supplier_code,
                                   dt_code, id)
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
  data.table::rbindlist(prov)
}
