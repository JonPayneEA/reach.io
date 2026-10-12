# ============================================================
# Tool:        Parallelised Backfill Orchestrator
# Description: Distributes historical gauge fetches across CPU
#              cores, writes Bronze Parquet, and logs per-gauge
#              outcomes. Supports failed-only reruns.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-10-12 - fetch failures now logged as FAILED;
#              year-partitioned Bronze; batched log checkpoints;
#              failed_only merges into previous log; plan restored
# Tier:        1
# Inputs:      Gauge registry Parquet; date range
# Outputs:     Bronze Parquet files; backfill log CSV
# Dependencies: data.table, arrow, future, future.apply, parallel
# ============================================================

# -- Parallelised Backfill Orchestrator ---------------------------------------

#' Fetch and store a single registry row (backfill worker)
#'
#' Never throws: every outcome is returned as a result list so that one bad
#' gauge cannot stop a run. Fetch errors are raised by `route_gauge(on_error =
#' "stop")` and recorded as `FAILED` — they are *not* confused with a
#' genuinely empty response (`EMPTY`).
#'
#' @return A list with `log` (one-row `data.table`) and `prov` (provenance
#'   rows or `NULL`).
#' @noRd
.backfill_one <- function(gauge, start_date, end_date, output_dir) {

  t_start <- proc.time()
  elapsed <- function() round((proc.time() - t_start)[["elapsed"]], 2)
  row <- function(status, rows = 0L, error = NA_character_) {
    data.table::data.table(
      gauge_id  = gauge$gauge_id,
      data_type = gauge$data_type %||% NA_character_,
      status    = status,
      rows      = as.integer(rows),
      elapsed_s = elapsed(),
      error     = error
    )
  }

  tryCatch({
    data_dt <- route_gauge(gauge, start_date, end_date, on_error = "stop")

    if (is.null(data_dt) || nrow(data_dt) == 0L) {
      return(list(log = row("EMPTY"), prov = NULL))
    }

    prov <- .store_bronze(data_dt, gauge, output_dir,
                          method_of_receipt = gauge$source_system %||% "unknown")
    list(log = row("SUCCESS", nrow(data_dt)), prov = prov)

  }, error = function(e) {
    list(log = row("FAILED", error = conditionMessage(e)), prov = NULL)
  })
}


#' Run a parallelised backfill across all active gauges
#'
#' Distributes all active gauges in the registry across workers using
#' `future.apply::future_lapply()`. Each gauge is routed to its source system
#' via [route_gauge()], and the resulting data is written to the Bronze tier as
#' Parquet, one file per calendar year of observations
#' (`bronze/<category>/<supplier>/<data_type>/<YYYY>/<dataset_id>.parquet`).
#' Per-gauge success, failure, and row counts are logged to a CSV file.
#'
#' Gauges are processed in batches. After every batch the log is rewritten,
#' provenance records are appended to the register, and successful gauges are
#' marked `backfill_done = TRUE` in the registry, so an interrupted run keeps
#' its progress.
#'
#' Supports re-running only previously failed gauges with `failed_only = TRUE`,
#' which restricts the run to rows recorded as `"FAILED"` in the existing log.
#' The new results replace those rows in the log; rows for gauges that
#' succeeded earlier are kept.
#'
#' A steward alert warning is raised if more than 5% of gauges fail, to
#' prompt investigation before results are promoted to Silver.
#'
#' @section EA fair use:
#' The EA Hydrology API fair-use guidance asks for one request at a time, and
#' each HDE worker issues its own requests. The default `n_workers` is
#' therefore capped at 4. Increase it only for sources that are not rate
#' limited, such as local WISKI `.all` files.
#'
#' @param registry_path Character. Path to the gauge registry Parquet file
#'   written by [build_gauge_registry()].
#' @param output_dir Character. Root of the data store. Bronze files are
#'   written under `output_dir/bronze/<category>/<SupplierCode>/<DataType>/<YYYY>/`.
#' @param start_date Character. Backfill start date (inclusive) in
#'   `"YYYY-MM-DD"` format.
#' @param end_date Character. Backfill end date (**exclusive**) in
#'   `"YYYY-MM-DD"` format.
#' @param register_path Character. Path to the Hydrometric Data Register CSV.
#'   A provenance record is appended for each Bronze file written. Defaults to
#'   `<output_dir>/register/hydrometric_data_register.csv`.
#' @param log_path Character. Path to write the per-gauge result log CSV.
#'   Directory is created if absent. Default `"logs/backfill_log.csv"`.
#' @param failed_only Logical. If `TRUE` and `log_path` exists, restrict the
#'   run to gauges recorded as `"FAILED"` in the previous log. Default
#'   `FALSE`.
#' @param n_workers Integer. Number of parallel workers; `1` runs sequentially
#'   in the current session. Default: one fewer than the detected cores,
#'   capped at 4 (see *EA fair use*).
#' @param batch_size Integer. Gauges per batch between log/register
#'   checkpoints. Default `max(50, 10 * n_workers)`.
#'
#' @return The log `data.table` invisibly, with columns `gauge_id`,
#'   `data_type`, `status` (`"SUCCESS"`, `"EMPTY"`, or `"FAILED"`), `rows`,
#'   `elapsed_s`, and `error`. The log is also written to `log_path`. With
#'   `failed_only = TRUE` the returned (and written) log is the previous log
#'   updated with the re-run results.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # First full run
#' log_dt <- run_backfill(
#'   registry_path = "data/hydrometric/register/gauge_registry.parquet",
#'   output_dir    = "data/hydrometric",
#'   start_date    = "2000-01-01",
#'   end_date      = "2025-01-01",
#'   log_path      = "logs/backfill_log.csv"
#' )
#'
#' # Re-run failed gauges only
#' log_dt <- run_backfill(
#'   registry_path = "data/hydrometric/register/gauge_registry.parquet",
#'   output_dir    = "data/hydrometric",
#'   start_date    = "2000-01-01",
#'   end_date      = "2025-01-01",
#'   log_path      = "logs/backfill_log.csv",
#'   failed_only   = TRUE
#' )
#' }
run_backfill <- function(registry_path,
                         output_dir,
                         start_date,
                         end_date,
                         register_path = file.path(output_dir, "register",
                                                   "hydrometric_data_register.csv"),
                         log_path    = "logs/backfill_log.csv",
                         failed_only = FALSE,
                         n_workers   = .default_workers(),
                         batch_size  = max(50L, 10L * as.integer(n_workers))) {

  if (failed_only && !file.exists(log_path)) {
    stop("failed_only = TRUE but no log file found at: ", log_path)
  }

  # -- Load registry and restrict to active gauges ----------------------------
  registry_dt <- data.table::as.data.table(
    arrow::read_parquet(registry_path)
  )
  registry_dt <- registry_dt[active == TRUE]
  if (!"data_type" %in% names(registry_dt))
    registry_dt[, data_type := NA_character_]

  # -- Filter to previously failed gauges if requested ------------------------
  prev_log_dt <- NULL
  if (failed_only) {
    prev_log_dt <- data.table::fread(log_path, colClasses = list(character = "gauge_id"))
    failed      <- prev_log_dt[status == "FAILED"]
    if (nrow(failed) == 0L) {
      message("No failed gauges in previous log. Nothing to re-run.")
      return(invisible(prev_log_dt))
    }
    keep <- if ("data_type" %in% names(failed)) {
      .registry_key(registry_dt$gauge_id, registry_dt$data_type) %in%
        .registry_key(failed$gauge_id, failed$data_type)
    } else {
      registry_dt$gauge_id %in% failed$gauge_id
    }
    registry_dt <- registry_dt[keep]
    message(sprintf("Re-run mode: %d previously failed gauge(s).",
                    nrow(registry_dt)))
  }

  n_gauges <- nrow(registry_dt)
  if (n_gauges == 0L) {
    message("No active gauges to process.")
    return(invisible(data.table::data.table()))
  }

  message(sprintf(
    "Starting backfill: %d gauges | %d workers | %s to %s",
    n_gauges, as.integer(n_workers), start_date, end_date
  ))

  # -- Workers: restore the caller's plan on exit -----------------------------
  old_plan <- .plan_workers(n_workers)
  on.exit(future::plan(old_plan), add = TRUE)

  # -- Process gauges in batches, checkpointing after each --------------------
  batch_size <- max(1L, as.integer(batch_size))
  batches    <- split(seq_len(n_gauges), ceiling(seq_len(n_gauges) / batch_size))
  new_logs   <- list()

  write_log <- function() {
    new_log <- data.table::rbindlist(new_logs, fill = TRUE)
    full    <- if (is.null(prev_log_dt)) {
      new_log
    } else {
      keep_old <- !(.registry_key(prev_log_dt$gauge_id, prev_log_dt$data_type %||% NA) %in%
                    .registry_key(new_log$gauge_id, new_log$data_type))
      data.table::rbindlist(list(prev_log_dt[keep_old], new_log), fill = TRUE)
    }
    dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
    data.table::fwrite(full, log_path)
    full
  }

  for (b in seq_along(batches)) {
    idx <- batches[[b]]
    res <- future.apply::future_lapply(
      idx,
      function(i) .backfill_one(registry_dt[i], start_date, end_date, output_dir),
      future.seed = TRUE   # suppress RNG warnings from future_lapply
    )

    batch_log <- data.table::rbindlist(lapply(res, `[[`, "log"), fill = TRUE)
    new_logs[[b]] <- batch_log

    # Register + registry updates happen here, in the main process only
    .write_provenance_rows(
      data.table::rbindlist(lapply(res, `[[`, "prov"), fill = TRUE),
      register_path
    )
    ok <- batch_log[status == "SUCCESS"]
    if (nrow(ok) > 0L) {
      .update_registry_flags(registry_path,
                             .registry_key(ok$gauge_id, ok$data_type),
                             "backfill_done", TRUE)
    }
    write_log()
    message(sprintf("  Batch %d/%d done (%d gauges).", b, length(batches), length(idx)))
  }

  # -- Collate ----------------------------------------------------------------
  run_log  <- data.table::rbindlist(new_logs, fill = TRUE)
  full_log <- write_log()

  n_ok     <- sum(run_log$status == "SUCCESS")
  n_empty  <- sum(run_log$status == "EMPTY")
  n_failed <- sum(run_log$status == "FAILED")

  message(sprintf("Backfill complete: %d success | %d empty | %d failed",
                  n_ok, n_empty, n_failed))

  # -- Steward alert if failure rate exceeds 5% -------------------------------
  # More than 5% failure typically indicates a systemic issue (API outage,
  # auth problem, schema change) rather than isolated bad gauges
  fail_pct <- n_failed / n_gauges
  if (fail_pct > 0.05) {
    warning(sprintf(
      "STEWARD ALERT: %.1f%% of gauges failed (threshold: 5%%). Check log for details: %s",
      fail_pct * 100, log_path
    ), call. = FALSE)
  }

  message(sprintf("Log saved: %s", log_path))
  invisible(full_log)
}
