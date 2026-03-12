# -- Parallelised Backfill Orchestrator ---------------------------------------

#' Run a parallelised backfill across all active gauges
#'
#' Distributes all active gauges in the registry across available cores using
#' `future.apply::future_lapply()`. Each gauge is routed to its source system
#' via [route_gauge()], and the resulting data is written to partitioned
#' Parquet in the Bronze output directory. Per-gauge success, failure, and
#' row counts are logged to a CSV file.
#'
#' Supports re-running only previously failed gauges by setting
#' `failed_only = TRUE`, which filters the registry to gauges recorded as
#' `"FAILED"` in an existing log file. This avoids re-fetching gauges that
#' already completed successfully.
#'
#' A steward alert warning is raised if more than 5% of gauges fail, to
#' prompt investigation before results are promoted to Silver.
#'
#' @param registry_path Character. Path to the gauge registry Parquet file
#'   written by [build_gauge_registry()].
#' @param output_dir Character. Root Bronze output directory. Per-gauge
#'   subdirectories (`gauge_id=<id>/`) are created inside this path.
#' @param start_date Character. Backfill start date in `"YYYY-MM-DD"` format.
#' @param end_date Character. Backfill end date in `"YYYY-MM-DD"` format.
#' @param log_path Character. Path to write the per-gauge result log CSV.
#'   Directory is created if absent. Default `"logs/backfill_log.csv"`.
#' @param failed_only Logical. If `TRUE` and `log_path` exists, restrict the
#'   run to gauges recorded as `"FAILED"` in the previous log. Default
#'   `FALSE`.
#' @param n_workers Integer. Number of parallel workers. Defaults to one
#'   fewer than the number of detected cores so the host remains responsive.
#'
#' @return The log `data.table` invisibly, with columns `gauge_id`,
#'   `status` (`"SUCCESS"`, `"EMPTY"`, or `"FAILED"`), `rows`,
#'   `elapsed_s`, and `error`. The log is also written to `log_path`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # First full run
#' log_dt <- run_backfill(
#'   registry_path = "data/fw_bronze/gauge_registry/gauge_registry.parquet",
#'   output_dir    = "data/fw_bronze/flow",
#'   start_date    = "2000-01-01",
#'   end_date      = "2024-12-31",
#'   log_path      = "logs/backfill_log.csv"
#' )
#'
#' # Re-run failed gauges only
#' log_dt <- run_backfill(
#'   registry_path = "data/fw_bronze/gauge_registry/gauge_registry.parquet",
#'   output_dir    = "data/fw_bronze/flow",
#'   start_date    = "2000-01-01",
#'   end_date      = "2024-12-31",
#'   log_path      = "logs/backfill_log.csv",
#'   failed_only   = TRUE
#' )
#' }
run_backfill <- function(registry_path,
                         output_dir,
                         start_date,
                         end_date,
                         log_path    = "logs/backfill_log.csv",
                         failed_only = FALSE,
                         n_workers   = parallel::detectCores() - 1L) {

  # -- Load registry and restrict to active gauges ----------------------------
  registry_dt <- data.table::as.data.table(
    arrow::read_parquet(registry_path)
  )
  registry_dt <- registry_dt[active == TRUE]

  # -- Filter to previously failed gauges if requested ------------------------
  if (failed_only) {
    if (!file.exists(log_path)) {
      stop("failed_only = TRUE but no log file found at: ", log_path)
    }
    prev_log_dt <- data.table::fread(log_path)
    failed_ids  <- prev_log_dt[status == "FAILED", gauge_id]
    if (length(failed_ids) == 0) {
      message("No failed gauges in previous log. Nothing to re-run.")
      return(invisible(prev_log_dt))
    }
    registry_dt <- registry_dt[gauge_id %in% failed_ids]
    message(sprintf("Re-run mode: %d previously failed gauge(s).",
                    nrow(registry_dt)))
  }

  message(sprintf(
    "Starting backfill: %d gauges | %d workers | %s to %s",
    nrow(registry_dt), n_workers, start_date, end_date
  ))

  # -- Set up parallel workers ------------------------------------------------
  # multisession spawns background R processes, which works on all platforms.
  # Switch to multicore on Linux/macOS if forking is acceptable (faster
  # startup, shared memory) by using plan(multicore, workers = n_workers).
  future::plan(future::multisession, workers = n_workers)

  # -- Process each gauge in parallel -----------------------------------------
  results <- future.apply::future_lapply(
    seq_len(nrow(registry_dt)),
    function(i) {

      gauge   <- registry_dt[i]
      t_start <- proc.time()

      out <- tryCatch({

        data_dt <- route_gauge(gauge, start_date, end_date)

        if (is.null(data_dt) || nrow(data_dt) == 0) {
          # Distinguish between fetch errors (caught below) and genuinely
          # empty responses — both should be investigated but for different
          # reasons
          list(gauge_id  = gauge$gauge_id,
               status    = "EMPTY",
               rows      = 0L,
               elapsed_s = NA_real_,
               error     = NA_character_)
        } else {
          # Write fetched data to Hive-partitioned Bronze Parquet
          part_dir <- file.path(output_dir,
                                paste0("gauge_id=", gauge$gauge_id))
          dir.create(part_dir, recursive = TRUE, showWarnings = FALSE)
          arrow::write_parquet(
            data_dt,
            file.path(part_dir,
                      paste0("backfill_",
                             format(Sys.Date(), "%Y%m%d"), ".parquet"))
          )

          elapsed <- (proc.time() - t_start)[["elapsed"]]
          list(gauge_id  = gauge$gauge_id,
               status    = "SUCCESS",
               rows      = nrow(data_dt),
               elapsed_s = round(elapsed, 2),
               error     = NA_character_)
        }

      }, error = function(e) {
        elapsed <- (proc.time() - t_start)[["elapsed"]]
        list(gauge_id  = gauge$gauge_id,
             status    = "FAILED",
             rows      = 0L,
             elapsed_s = round(elapsed, 2),
             error     = conditionMessage(e))
      })

      out
    },
    future.seed = TRUE   # suppress RNG warnings from future_lapply
  )

  # -- Return to sequential execution -----------------------------------------
  future::plan(future::sequential)

  # -- Collate log as data.table ----------------------------------------------
  log_dt <- data.table::rbindlist(
    lapply(results, data.table::as.data.table)
  )

  n_ok     <- nrow(log_dt[status == "SUCCESS"])
  n_empty  <- nrow(log_dt[status == "EMPTY"])
  n_failed <- nrow(log_dt[status == "FAILED"])

  message(sprintf("Backfill complete: %d success | %d empty | %d failed",
                  n_ok, n_empty, n_failed))

  # -- Steward alert if failure rate exceeds 5% -------------------------------
  # More than 5% failure typically indicates a systemic issue (API outage,
  # auth problem, schema change) rather than isolated bad gauges
  fail_pct <- n_failed / nrow(registry_dt)
  if (fail_pct > 0.05) {
    warning(sprintf(
      "STEWARD ALERT: %.1f%% of gauges failed (threshold: 5%%). ",
      "Check log for details: %s",
      fail_pct * 100, log_path
    ))
  }

  # -- Save log ---------------------------------------------------------------
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  data.table::fwrite(log_dt, log_path)
  message(sprintf("Log saved: %s", log_path))

  invisible(log_dt)
}
