# ============================================================
# Tool:        Incremental Sync Orchestrator
# Description: Per-gauge high-watermark sync. Finds the latest
#              timestamp already in the Bronze store for each
#              gauge and fetches only newer data, writing it as
#              new year-partitioned Bronze files with provenance.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-10-12 - rewritten for the Bronze path layout
#              (bronze/<cat>/<supplier>/<type>/<YYYY>/...), the
#              Bronze schema, and fetch-failure reporting
# Tier:        1
# Inputs:      Gauge registry Parquet; existing Bronze output
# Outputs:     Bronze Parquet per gauge/year; incremental log CSV
# Dependencies: data.table, arrow, future, future.apply,
#               parallel
# ============================================================

# -- Incremental Sync Orchestrator --------------------------------------------

#' Get the high-watermark timestamp for a gauge in the Bronze store
#'
#' Finds the Bronze files for a gauge and data type and returns the latest
#' `timestamp` present. Only files in the most recent observation-year
#' directory are opened, and only the `timestamp` column is read, so this is
#' cheap even for long records. Returns `NA` if the gauge has no Bronze data
#' yet, which causes [run_incremental()] to fall back to `default_start`.
#'
#' @param output_dir Character. Root of the data store (contains `bronze/`).
#' @param gauge_id Character. Gauge identifier (the Bronze `site_id`).
#' @param data_type Character. Framework data type code (`"Q"`, `"H"`, `"P"`).
#' @param supplier_code Character. Supplier code. Default `"EA"`.
#' @param category Character. Data category. Default `"hydrometric"`.
#'
#' @return A `POSIXct` scalar (UTC), or `NA` if no data is found.
#'
#' @noRd
get_high_watermark <- function(output_dir, gauge_id, data_type,
                               supplier_code = "EA",
                               category      = "hydrometric") {

  # Typed NA so callers can always test is.na() and format() the result
  na_ts <- as.POSIXct(NA_real_, origin = "1970-01-01", tz = "UTC")

  # Matches every file for the gauge + type, including all WISKI traces and
  # every received date, so the watermark is the max over all of them
  files <- .list_bronze_files(output_dir, gauge_id, data_type,
                              supplier_code, category)
  if (length(files) == 0L) return(na_ts)

  # Newest observation-year directory that holds this gauge. Files are
  # partitioned by observation year, so the latest timestamp must be in the
  # newest year directory; older years never need opening. Files whose parent
  # directory is not a year are ignored.
  yr    <- suppressWarnings(as.integer(basename(dirname(files))))
  files <- files[!is.na(yr) & yr == max(yr, na.rm = TRUE)]
  if (length(files) == 0L) return(na_ts)

  # A file that cannot be read is skipped with a warning rather than failing
  # the gauge. If every file in the directory is unreadable the result is NA
  # and the caller falls back to default_start.
  ts <- lapply(files, function(f) {
    tryCatch(
      .read_parquet(f, col_select = "timestamp")[["timestamp"]],
      error = function(e) {
        warning(sprintf("Could not read watermark from %s: %s",
                        f, conditionMessage(e)), call. = FALSE)
        NULL
      }
    )
  })
  # c() (not unlist) so the POSIXct class survives the combine
  ts <- do.call(c, Filter(Negate(is.null), ts))
  if (is.null(ts) || all(is.na(ts))) return(na_ts)

  as.POSIXct(max(ts, na.rm = TRUE), tz = "UTC")
}


#' Sync a single registry row from its high watermark (incremental worker)
#'
#' Never throws; returns a list with `log` (one-row `data.table`) and `prov`
#' (provenance rows or `NULL`).
#' @noRd
.incremental_one <- function(gauge, output_dir, end_date, default_start,
                             run_at) {

  t_start <- proc.time()
  elapsed <- function() round((proc.time() - t_start)[["elapsed"]], 2)
  # One log-row shape for every outcome. `watermark` is the value found
  # before this run, as text so a missing one logs as NA; FAILED rows carry
  # none (the failure may have happened before it was read). All rows from
  # one run share `run_at`, so the appended history can be grouped by run.
  row <- function(status, watermark = NA_character_, rows = 0L,
                  error = NA_character_) {
    data.table::data.table(
      run_at    = format(run_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
      gauge_id  = gauge$gauge_id,
      data_type = gauge$data_type %||% NA_character_,
      watermark = watermark,
      end_date  = end_date,
      status    = status,
      rows      = as.integer(rows),
      elapsed_s = elapsed(),
      error     = error
    )
  }

  tryCatch({

    # -- Resolve where this series lives in Bronze ----------------------------
    # Validate before any disk or network access. The watermark lookup needs a
    # concrete framework type code, so a registry row without a usable
    # data_type (e.g. an NA WISKI_ALL row) is logged FAILED, not guessed at.
    supplier_code <- source_to_supplier(gauge$source_system)
    dt_code       <- param_to_data_type(gauge$data_type %||% NA_character_)
    category      <- gauge$category
    if (is.null(category) || is.na(category)) category <- "hydrometric"

    if (length(supplier_code) != 1L || is.na(supplier_code))
      stop(sprintf("Unknown source_system '%s'.", gauge$source_system))
    if (is.na(dt_code))
      stop(sprintf("Registry data_type '%s' is not one of flow/level/rainfall.",
                   gauge$data_type %||% NA_character_))

    # -- Determine start date from the high watermark --------------------------
    hwm <- get_high_watermark(output_dir, gauge$gauge_id, dt_code,
                              supplier_code, category)

    start_date <- if (is.na(hwm)) {
      # No existing data — treat as a mini-backfill from default_start
      message(sprintf("  [%s] No existing data - using default_start: %s",
                      gauge$gauge_id, default_start))
      default_start
    } else {
      # Start on the watermark's own day: the API works at date granularity
      # and the remainder of that day may not have been fetched yet. Rows at
      # or before the watermark are discarded below.
      as.character(as.Date(hwm))
    }
    hwm_chr <- if (is.na(hwm)) NA_character_ else
      format(hwm, "%Y-%m-%d %H:%M:%S", tz = "UTC")

    # -- Skip if already up to date -------------------------------------------
    # end_date is exclusive and the API works in whole days, so once the
    # watermark's day reaches end_date there is nothing left to request. With
    # the default end_date (today) this also means today's partial day is
    # never fetched.
    if (!is.na(hwm) && as.Date(hwm) >= as.Date(end_date)) {
      return(list(log = row("NO_NEW_DATA", hwm_chr), prov = NULL))
    }

    message(sprintf("  [%s] Fetching %s to %s",
                    gauge$gauge_id, start_date, end_date))

    # -- Fetch via router (errors propagate -> FAILED) -------------------------
    data_dt <- route_gauge(gauge, start_date, end_date, on_error = "stop")

    # EMPTY: the source returned nothing at all for the requested days
    # (distinct from FAILED, which is an error, and NO_NEW_DATA below)
    if (is.null(data_dt) || nrow(data_dt) == 0L) {
      return(list(log = row("EMPTY", hwm_chr), prov = NULL))
    }

    # Fetching from the watermark's own day re-delivers rows already stored;
    # keep only strictly newer ones so Bronze is not given duplicates. The
    # one gauge-level watermark applies to every series (trace) in data_dt.
    # NO_NEW_DATA here means the source had nothing beyond what we hold.
    if (!is.na(hwm)) {
      data_dt <- data_dt[data_dt$timestamp > hwm]
      if (nrow(data_dt) == 0L) {
        return(list(log = row("NO_NEW_DATA", hwm_chr), prov = NULL))
      }
    }

    # Written by the worker; provenance rows are returned for the main
    # process to append. The " incremental" suffix tells these register
    # entries apart from backfill ones.
    prov <- .store_bronze(
      data_dt, gauge, output_dir,
      method_of_receipt = paste0(gauge$source_system %||% "unknown", " incremental")
    )
    list(log = row("SUCCESS", hwm_chr, nrow(data_dt)), prov = prov)

  }, error = function(e) {
    list(log = row("FAILED", error = conditionMessage(e)), prov = NULL)
  })
}


#' Run an incremental sync across all active gauges
#'
#' For each active, live gauge in the registry, finds the latest `timestamp`
#' already present in the Bronze store (the *high watermark*, see
#' [read_bronze()] for the layout), then fetches data from that day forward via
#' [route_gauge()] and keeps only rows strictly newer than the watermark. New
#' rows are written as year-partitioned Bronze files alongside the existing
#' ones, with a provenance record per file.
#'
#' Gauges with no existing Bronze data fall back to `default_start`, so
#' `run_incremental()` is safe to run on a partially backfilled registry.
#' Registry rows with `live = FALSE` (for example historical WISKI `.all`
#' dumps) are skipped.
#'
#' Like [run_backfill()], work is distributed across workers and every outcome
#' is logged to a CSV; a fetch error is logged as `FAILED`, never as `EMPTY`. A
#' steward alert warning is raised if more than 5% of gauges fail.
#'
#' ## Deduplication
#'
#' Bronze is append-only and rows at or before the watermark are discarded, so
#' normal runs do not create duplicates. If overlap does occur (for example a
#' gauge re-backfilled later), [read_bronze()] keeps the most recently received
#' row for each timestamp.
#'
#' @details
#' ## Watermark
#'
#' The watermark is the maximum Bronze `timestamp` for the gauge and data type,
#' taken from the newest observation-year directory only. If a gauge has
#' several series of one type (WISKI traces) the watermark is the maximum
#' across all of them, and it is applied to every series fetched. Because the
#' fetch starts on the watermark's own day, rows from earlier that day are
#' fetched again and discarded. Values that arrive late or are revised with a
#' timestamp at or before the watermark are not picked up; use
#' [run_backfill()] for those.
#'
#' ## Date window
#'
#' `end_date` is exclusive and the API works in whole days. A series whose
#' watermark day is on or after `end_date` is logged `NO_NEW_DATA` without a
#' request. With the default `end_date` (today) the current, incomplete day is
#' therefore never fetched.
#'
#' ## Status values
#'
#' - `SUCCESS`: new rows were written (`rows` counts them).
#' - `NO_NEW_DATA`: already up to date, or the source returned only rows at
#'   or before the watermark.
#' - `EMPTY`: the source returned nothing for the requested days.
#' - `FAILED`: an error was raised (including a registry row whose
#'   `source_system` or `data_type` cannot be resolved, such as an `NA`
#'   `data_type`). The message is in `error`.
#'
#' ## Order of work and side effects
#'
#' Workers read the watermark, fetch, filter and write Bronze files. Once
#' every gauge has finished, the main process appends the provenance rows to
#' the register CSV, then appends this run's rows to `log_path` (columns are
#' unioned, so older logs with a different layout stay readable). Unlike
#' [run_backfill()] there is no batching: if the run is interrupted, Bronze
#' files already written have no register records. The registry is not
#' modified, so `backfill_done` is untouched.
#'
#' Bronze file names end in the received date (see [read_bronze()]). If a
#' series is written twice for the same observation year on the same calendar
#' day, for example by a second run or by this function after a same-day
#' [run_backfill()], the new rows are merged into the existing file rather
#' than replacing it, so nothing already stored is lost.
#'
#' `gauge_ids` that are not in the registry at all, and ids that are registered
#' but skipped because the row is inactive or has `live = FALSE`, are warned
#' about separately. It is an error only if no requested gauge is left to run.
#'
#' @inheritSection run_backfill EA fair use
#'
#' @param registry_path Character. Path to the gauge registry Parquet file
#'   written by [build_gauge_registry()].
#' @param output_dir Character. Root of the data store (contains `bronze/`).
#' @param register_path Character. Path to the Hydrometric Data Register CSV.
#'   Defaults to `<output_dir>/register/hydrometric_data_register.csv`.
#' @param end_date Character. Fetch data up to this date (**exclusive**) in
#'   `"YYYY-MM-DD"` format. Defaults to today.
#' @param default_start Character. Fallback start date in `"YYYY-MM-DD"`
#'   format for gauges with no existing Bronze data. Defaults to
#'   `"2000-01-01"`.
#' @param log_path Character. Path to write the per-gauge result log CSV.
#'   Each run appends to the file if it already exists, preserving the full
#'   run history. Default `"logs/incremental_log.csv"`.
#' @param gauge_ids Character vector or `NULL`. Restrict the run to a subset
#'   of gauge IDs. `NULL` (default) runs all active, live gauges.
#' @param n_workers Integer. Number of parallel workers; `1` runs sequentially
#'   in the current session. Default: one fewer than the detected cores,
#'   capped at 4.
#'
#' @return The log `data.table` for this run invisibly, with columns
#'   `run_at` (character, UTC), `gauge_id`, `data_type`, `watermark` (the
#'   latest timestamp found before the run), `end_date`, `status`
#'   (`"SUCCESS"`, `"EMPTY"`, `"NO_NEW_DATA"`, or `"FAILED"`), `rows`,
#'   `elapsed_s`, and `error`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Sync all active gauges up to today
#' log_dt <- run_incremental(
#'   registry_path = "data/hydrometric/register/gauge_registry.parquet",
#'   output_dir    = "data/hydrometric",
#'   log_path      = "logs/incremental_log.csv"
#' )
#'
#' # Sync a specific subset of gauges
#' log_dt <- run_incremental(
#'   registry_path = "data/hydrometric/register/gauge_registry.parquet",
#'   output_dir    = "data/hydrometric",
#'   gauge_ids     = c("39001", "39002")
#' )
#'
#' # Read a gauge back (duplicates across files are resolved for you)
#' obj <- read_bronze("data/hydrometric", "39001", "Q")
#' }
run_incremental <- function(registry_path,
                            output_dir,
                            register_path = file.path(output_dir, "register",
                                                      "hydrometric_data_register.csv"),
                            end_date      = as.character(Sys.Date()),
                            default_start = "2000-01-01",
                            log_path      = "logs/incremental_log.csv",
                            gauge_ids     = NULL,
                            n_workers     = .default_workers()) {

  # One timestamp for the whole run, stamped on every log row
  run_at <- Sys.time()

  # -- Load registry and restrict to active, live gauges ----------------------
  # live = FALSE marks historical dumps (WISKI .all) that have no feed to
  # poll. An NA, or a registry with no `live` column at all, counts as live.
  registry_all <- data.table::as.data.table(
    .read_parquet(registry_path)
  )
  registry_dt <- registry_all[active == TRUE]
  if ("live" %in% names(registry_dt)) {
    registry_dt <- registry_dt[is.na(live) | live == TRUE]
  }

  # -- Optionally restrict to a gauge subset ----------------------------------
  # Two different problems are reported separately: ids that are not in the
  # registry at all, and ids that are registered but excluded above (inactive,
  # or live = FALSE). Warn for partial misses; stop only when nothing is left
  # to run.
  if (!is.null(gauge_ids)) {
    unknown  <- setdiff(gauge_ids, registry_all$gauge_id)
    excluded <- setdiff(intersect(gauge_ids, registry_all$gauge_id),
                        registry_dt$gauge_id)
    if (length(unknown) > 0) {
      warning(sprintf("gauge_ids not found in registry: %s",
                      paste(unknown, collapse = ", ")))
    }
    if (length(excluded) > 0) {
      warning(sprintf("gauge_ids skipped because they are inactive or not live: %s",
                      paste(excluded, collapse = ", ")))
    }
    registry_dt <- registry_dt[gauge_id %in% gauge_ids]
    if (nrow(registry_dt) == 0) {
      stop("No matching active gauges found for the supplied gauge_ids.")
    }
  }

  n_gauges <- nrow(registry_dt)
  message(sprintf(
    "Starting incremental sync: %d gauges | %d workers | up to %s",
    n_gauges, as.integer(n_workers), end_date
  ))

  # -- Workers: restore the caller's plan on exit -----------------------------
  old_plan <- .plan_workers(n_workers)
  on.exit(future::plan(old_plan), add = TRUE)

  # -- Process each gauge -----------------------------------------------------
  # One future per registry row, no batching or mid-run checkpoint (cf.
  # run_backfill()). Workers write Bronze files but must not touch the
  # register, so they return their provenance rows instead.
  res <- future.apply::future_lapply(
    seq_len(n_gauges),
    function(i) .incremental_one(registry_dt[i], output_dir, end_date,
                                 default_start, run_at),
    future.seed = TRUE
  )

  log_dt <- data.table::rbindlist(lapply(res, `[[`, "log"), fill = TRUE)

  # Register is appended from the main process only (concurrent appends to
  # one CSV are not safe), once all workers have finished
  .write_provenance_rows(
    data.table::rbindlist(lapply(res, `[[`, "prov"), fill = TRUE),
    register_path
  )

  n_ok     <- sum(log_dt$status == "SUCCESS")
  n_empty  <- sum(log_dt$status == "EMPTY")
  n_nodata <- sum(log_dt$status == "NO_NEW_DATA")
  n_failed <- sum(log_dt$status == "FAILED")

  message(sprintf(
    "Incremental sync complete: %d success | %d empty | %d up-to-date | %d failed",
    n_ok, n_empty, n_nodata, n_failed
  ))

  # -- Steward alert if failure rate exceeds 5% -------------------------------
  # More than 5% failing points to a systemic problem (API outage, schema
  # change) rather than isolated bad gauges. n_gauges can be 0 here when no
  # gauge_ids filter was given, hence the guard.
  fail_pct <- if (n_gauges > 0L) n_failed / n_gauges else 0
  if (fail_pct > 0.05) {
    warning(sprintf(
      "STEWARD ALERT: %.1f%% of gauges failed (threshold: 5%%). Check log: %s",
      fail_pct * 100, log_path
    ), call. = FALSE)
  }

  # -- Append to log (preserves full run history across multiple syncs) -------
  # Appended, not overwritten (unlike run_backfill()); the returned log_dt is
  # this run only, not the accumulated history.
  .append_log(log_dt, log_path)
  message(sprintf("Log appended: %s", log_path))

  invisible(log_dt)
}
