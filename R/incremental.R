# ============================================================
# Tool:        Incremental Sync Orchestrator
# Description: Per-gauge high-watermark sync. Determines the
#              latest timestamp in each Bronze partition and
#              fetches only new data, appending to the
#              partition and writing a provenance record.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      Gauge registry Parquet; existing Bronze output
# Outputs:     Bronze Parquet per gauge; incremental log CSV
# Dependencies: data.table, arrow, future, future.apply,
#               parallel
# ============================================================

# -- Incremental Sync Orchestrator --------------------------------------------

#' Get the high-watermark datetime for a single gauge partition
#'
#' Opens the existing Parquet partition for a gauge and returns the maximum
#' `datetime` value present. Returns `NA` if no partition exists yet, which
#' causes [run_incremental()] to fall back to `default_start`.
#'
#' Deduplication of overlapping rows between existing and new data is handled
#' at read time by Arrow when consuming the partition directory — no rows are
#' rewritten.
#'
#' @param output_dir Character. Root Bronze output directory.
#' @param gauge_id Character. Gauge identifier.
#'
#' @return A `POSIXct` scalar (UTC) or `NA` if no existing data is found.
#'
#' @noRd
get_high_watermark <- function(output_dir, gauge_id) {

  part_dir <- file.path(output_dir, paste0("gauge_id=", gauge_id))

  if (!dir.exists(part_dir)) return(NA)

  parquet_files <- list.files(part_dir, pattern = "\\.parquet$",
                              full.names = TRUE)
  if (length(parquet_files) == 0) return(NA)

  tryCatch({
    # Read only the datetime column across all partition files then take the
    # max. arrow::open_dataset + select_columns avoids loading all data.
    dt <- data.table::as.data.table(
      arrow::open_dataset(part_dir) |>
        arrow::select(datetime) |>
        arrow::collect()
    )
    as.POSIXct(dt[, max(datetime, na.rm = TRUE)], tz = "UTC")

  }, error = function(e) {
    warning(sprintf("Could not read watermark for gauge %s: %s",
                    gauge_id, e$message))
    NA
  })
}


#' Run an incremental sync across all active gauges
#'
#' For each active gauge in the registry, determines the latest `datetime`
#' already present in the Bronze partition (the *high watermark*), then
#' fetches only the data from that point forward via [route_gauge()]. New
#' data is written as a dated Parquet file alongside the existing partition
#' files — deduplication of any overlap is handled at read time by Arrow
#' when `open_dataset()` reads the full partition directory.
#'
#' Gauges with no existing partition (i.e. not yet backfilled) fall back to
#' `default_start`, so `run_incremental()` is safe to run even on a partially
#' backfilled registry.
#'
#' Like [run_backfill()], work is distributed across CPU cores and every
#' outcome is logged to a CSV. A steward alert warning is raised if more than
#' 5% of gauges fail.
#'
#' ## Deduplication strategy
#'
#' Overlapping rows between existing and new Parquet files are deduplicated
#' at read time by Arrow. When consuming a partition directory with
#' `arrow::open_dataset()`, duplicate `datetime` rows across files are
#' eliminated with `data.table::unique()` after collecting. This avoids the cost of rewriting
#' historical files on every incremental run.
#'
#' @param registry_path Character. Path to the gauge registry Parquet file
#'   written by [build_gauge_registry()].
#' @param output_dir Character. Root Bronze output directory containing
#'   per-gauge Hive partitions (`gauge_id=<id>/`).
#' @param end_date Character. Fetch data up to this date in `"YYYY-MM-DD"`
#'   format. Defaults to today.
#' @param default_start Character. Fallback start date in `"YYYY-MM-DD"`
#'   format for gauges with no existing partition. Defaults to `"2000-01-01"`.
#' @param log_path Character. Path to write the per-gauge result log CSV.
#'   Each run appends a new timestamped block to this file if it already
#'   exists, preserving the full run history. Default
#'   `"logs/incremental_log.csv"`.
#' @param gauge_ids Character vector or `NULL`. Restrict the run to a subset
#'   of gauge IDs. `NULL` (default) runs all active gauges in the registry.
#' @param n_workers Integer. Number of parallel workers. Defaults to one
#'   fewer than the number of detected cores.
#'
#' @return The log `data.table` for this run invisibly, with columns
#'   `run_at` (POSIXct), `gauge_id`, `watermark` (the start date used),
#'   `end_date`, `status` (`"SUCCESS"`, `"EMPTY"`, `"NO_NEW_DATA"`, or
#'   `"FAILED"`), `rows`, `elapsed_s`, and `error`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Sync all active gauges up to today
#' log_dt <- run_incremental(
#'   registry_path = "data/fw_bronze/gauge_registry/gauge_registry.parquet",
#'   output_dir    = "data/fw_bronze/flow",
#'   log_path      = "logs/incremental_log.csv"
#' )
#'
#' # Sync a specific subset of gauges
#' log_dt <- run_incremental(
#'   registry_path = "data/fw_bronze/gauge_registry/gauge_registry.parquet",
#'   output_dir    = "data/fw_bronze/flow",
#'   gauge_ids     = c("39001", "39002"),
#'   log_path      = "logs/incremental_log.csv"
#' )
#'
#' # Read the full partition for a gauge. Deduplication of overlapping rows
#' # across backfill and incremental files is done with data.table after collect.
#' dt <- data.table::as.data.table(
#'   arrow::collect(arrow::open_dataset("data/fw_bronze/flow/gauge_id=39001"))
#' )
#' data.table::setkeyv(dt, c("gauge_id", "datetime"))
#' dt <- unique(dt, by = c("gauge_id", "datetime"))
#' }
run_incremental <- function(registry_path,
                            output_dir,
                            register_path = file.path(output_dir, "register",
                                                      "hydrometric_data_register.csv"),
                            end_date      = as.character(Sys.Date()),
                            default_start = "2000-01-01",
                            log_path      = "logs/incremental_log.csv",
                            gauge_ids     = NULL,
                            n_workers     = parallel::detectCores() - 1L) {

  run_at <- Sys.time()

  # -- Load registry and restrict to active gauges ----------------------------
  registry_dt <- data.table::as.data.table(
    arrow::read_parquet(registry_path)
  )
  registry_dt <- registry_dt[active == TRUE]

  # -- Optionally restrict to a gauge subset ----------------------------------
  if (!is.null(gauge_ids)) {
    unknown <- setdiff(gauge_ids, registry_dt$gauge_id)
    if (length(unknown) > 0) {
      warning(sprintf("gauge_ids not found in registry: %s",
                      paste(unknown, collapse = ", ")))
    }
    registry_dt <- registry_dt[gauge_id %in% gauge_ids]
    if (nrow(registry_dt) == 0) {
      stop("No matching active gauges found for the supplied gauge_ids.")
    }
  }

  message(sprintf(
    "Starting incremental sync: %d gauges | %d workers | up to %s",
    nrow(registry_dt), n_workers, end_date
  ))

  # -- Set up parallel workers ------------------------------------------------
  future::plan(future::multisession, workers = n_workers)

  # -- Process each gauge in parallel -----------------------------------------
  results <- future.apply::future_lapply(
    seq_len(nrow(registry_dt)),
    function(i) {

      gauge   <- registry_dt[i]
      t_start <- proc.time()

      out <- tryCatch({

        # -- Determine start date from high watermark -------------------------
        hwm <- get_high_watermark(output_dir, gauge$gauge_id)

        start_date <- if (is.na(hwm)) {
          # No existing partition — treat as a mini-backfill from default_start
          message(sprintf("  [%s] No existing data — using default_start: %s",
                          gauge$gauge_id, default_start))
          default_start
        } else {
          # Advance one second past the watermark to avoid fetching the last
          # known row again. The one-second step is safe for both daily and
          # 15-minute data since the API stores readings at fixed timestamps.
          as.character(as.Date(hwm) + 1L)
        }

        # -- Skip if already up to date ---------------------------------------
        if (!is.na(hwm) && as.Date(start_date) >= as.Date(end_date)) {
          elapsed <- (proc.time() - t_start)[["elapsed"]]
          return(list(
            run_at    = run_at,
            gauge_id  = gauge$gauge_id,
            watermark = as.character(as.Date(hwm)),
            end_date  = end_date,
            status    = "NO_NEW_DATA",
            rows      = 0L,
            elapsed_s = round(elapsed, 2),
            error     = NA_character_
          ))
        }

        message(sprintf("  [%s] Fetching %s to %s",
                        gauge$gauge_id, start_date, end_date))

        # -- Fetch via router --------------------------------------------------
        data_dt <- route_gauge(gauge, start_date, end_date)

        if (is.null(data_dt) || nrow(data_dt) == 0) {
          elapsed <- (proc.time() - t_start)[["elapsed"]]
          return(list(
            run_at    = run_at,
            gauge_id  = gauge$gauge_id,
            watermark = start_date,
            end_date  = end_date,
            status    = "EMPTY",
            rows      = 0L,
            elapsed_s = round(elapsed, 2),
            error     = NA_character_
          ))
        }

        # -- Apply Bronze schema and write to framework folder structure ------
        supplier_code <- source_to_supplier(gauge$source_system) %||% "EA"
        dt_code       <- param_to_data_type(gauge$data_type) %||% "Q"
        dataset_id    <- make_dataset_id(supplier_code, gauge$gauge_id,
                                         dt_code,
                                         received_date = as.Date(run_at))

        bronze_dt <- apply_bronze_schema(
          dt                = data_dt,
          dataset_id        = dataset_id,
          site_id           = gauge$gauge_id,
          data_type         = dt_code,
          timestamp_col     = "datetime",
          value_col         = "value",
          supplier_flag_col = if ("flag" %in% names(data_dt)) "flag" else NULL
        )

        # Include run timestamp in filename so successive incremental runs
        # never collide. Files sit alongside the backfill Parquet in the
        # same Bronze directory; deduplication on timestamp is done at read
        # time by the caller.
        out_file <- file.path(
          dirname(bronze_path(output_dir, supplier_code, dt_code, dataset_id)),
          sprintf("incremental_%s.parquet", format(run_at, "%Y%m%d_%H%M%S"))
        )
        dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
        arrow::write_parquet(bronze_dt, out_file)

        write_provenance_record(
          register_path       = register_path,
          dataset_id          = dataset_id,
          supplier            = supplier_code,
          supplier_code       = supplier_code,
          site_id             = gauge$gauge_id,
          data_type           = dt_code,
          time_period_start   = start_date,
          time_period_end     = end_date,
          temporal_resolution = "unknown",
          method_of_receipt   = paste0(gauge$source_system, " incremental"),
          file_path           = out_file
        )

        elapsed <- (proc.time() - t_start)[["elapsed"]]
        list(
          run_at     = run_at,
          gauge_id   = gauge$gauge_id,
          dataset_id = dataset_id,
          watermark  = start_date,
          end_date   = end_date,
          status     = "SUCCESS",
          rows       = nrow(bronze_dt),
          elapsed_s  = round(elapsed, 2),
          error      = NA_character_
        )

      }, error = function(e) {
        elapsed <- (proc.time() - t_start)[["elapsed"]]
        list(
          run_at    = run_at,
          gauge_id  = gauge$gauge_id,
          watermark = NA_character_,
          end_date  = end_date,
          status    = "FAILED",
          rows      = 0L,
          elapsed_s = round(elapsed, 2),
          error     = conditionMessage(e)
        )
      })

      out
    },
    future.seed = TRUE
  )

  # -- Return to sequential execution -----------------------------------------
  future::plan(future::sequential)

  # -- Collate log ------------------------------------------------------------
  log_dt <- data.table::rbindlist(
    lapply(results, data.table::as.data.table)
  )

  n_ok       <- nrow(log_dt[status == "SUCCESS"])
  n_empty    <- nrow(log_dt[status == "EMPTY"])
  n_nodata   <- nrow(log_dt[status == "NO_NEW_DATA"])
  n_failed   <- nrow(log_dt[status == "FAILED"])

  message(sprintf(
    "Incremental sync complete: %d success | %d empty | %d up-to-date | %d failed",
    n_ok, n_empty, n_nodata, n_failed
  ))

  # -- Steward alert if failure rate exceeds 5% -------------------------------
  fail_pct <- n_failed / nrow(registry_dt)
  if (fail_pct > 0.05) {
    warning(sprintf(
      "STEWARD ALERT: %.1f%% of gauges failed (threshold: 5%%). Check log: %s",
      fail_pct * 100, log_path
    ))
  }

  # -- Append to log (preserves full run history across multiple syncs) -------
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  data.table::fwrite(log_dt, log_path,
                     append = file.exists(log_path))
  message(sprintf("Log appended: %s", log_path))

  invisible(log_dt)
}
