# ============================================================
# Tool:        EA Hydrology API Download
# Description: Main entry point for downloading rainfall,
#              flow, and level data from the EA Hydrology API
#              via synchronous or batch methods.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      Station identifiers, date range, parameters
# Outputs:     Named list of S7 objects or CSV files
# Dependencies: httr, data.table, S7
# ============================================================

# ── Main entry point ──────────────────────────────────────────────────────────

#' Download hydrology readings from the EA Hydrology API
#'
#' Single entry point for downloading rainfall, flow, and/or level data from
#' the Environment Agency Hydrology API. At least one station identifier
#' argument (`wiski_ids`, `rloi_ids`, or `notations`) should normally be
#' supplied — omitting all three will attempt to download every station, which
#' is a very large request.
#'
#' ## Methods
#' - **`"sync"`** — Calls the synchronous readings endpoint directly. The
#'   date range is split into annual chunks to stay within the API's hard
#'   limit of 2,000,000 rows. Best for modest date ranges or small station
#'   sets.
#' - **`"batch"`** — Submits requests to the asynchronous batch endpoint,
#'   polls for completion, and downloads the resulting S3 CSVs. Better
#'   suited to large bulk downloads where synchronous requests would time
#'   out.
#'
#' ## Output modes
#' - **`"memory"`** — Returns a named list with one `data.table` per
#'   parameter (e.g. `result$flow`, `result$rainfall`) plus a `summary`
#'   element. No files are written.
#' - **`"disk"`** — Writes one CSV per measure into per-parameter
#'   subdirectories under `out_dir` and returns the summary `data.table`
#'   invisibly.
#'
#' The EA API fair-use guidance asks that automated users issue only one
#' request at a time. Both methods are intentionally sequential.
#'
#' @param parameters Character vector. Any combination of `"rainfall"`,
#'   `"flow"`, and `"level"`. Default is all three.
#' @param from_date Character. Start date (inclusive) in `"YYYY-MM-DD"` format.
#' @param to_date Character. End date (exclusive) in `"YYYY-MM-DD"` format.
#' @param method Character. `"sync"` (default) or `"batch"`.
#' @param output Character. `"memory"` (default) or `"disk"`.
#' @param out_dir Character or `NULL`. Root output directory. Required when
#'   `output = "disk"`. Each parameter gets its own subdirectory, e.g.
#'   `out_dir/flow/`.
#' @param period_name Character. Temporal resolution: `"daily"` (default)
#'   or `"15min"`.
#' @param value_type Character or `NULL`. Value statistic filter, e.g.
#'   `"mean"`, `"max"`, `"total"`. `"all"` skips the filter. `NULL` uses
#'   each parameter's built-in default.
#' @param observation_type Character or `NULL`. `"Qualified"` for
#'   quality-controlled data, `"Measured"` for raw. `NULL` returns both.
#'   Applies to `method = "sync"` only.
#' @param wiski_ids Character vector or `NULL`. Filter to stations with
#'   these WISKI identifiers.
#' @param rloi_ids Character vector or `NULL`. Filter to stations with
#'   these RLOIid identifiers.
#' @param notations Character vector or `NULL`. Filter to stations with
#'   these notation (SUID) values.
#' @param chunk_by_year Logical. Split the date range into annual chunks?
#'   Default `TRUE`. Applies to `method = "sync"` only.
#' @param max_wait_s Numeric. Maximum seconds to wait per batch job.
#'   Default 600. Applies to `method = "batch"` only.
#'
#' @return In `"memory"` mode: a named list with one `data.table` per
#'   requested parameter plus a `summary` `data.table` with columns
#'   `parameter`, `measure`, `file`, `rows`, and `ok`. In `"disk"` mode:
#'   the summary `data.table` invisibly, with CSVs written to `out_dir`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # In-memory download by WISKI ID
#' result <- download_hydrology(
#'   parameters = c("flow", "level"),
#'   from_date  = "2022-01-01",
#'   to_date    = "2022-12-31",
#'   wiski_ids  = c("SS92F014", "S11512_FW")
#' )
#' result$flow
#' result$level
#' result$summary
#'
#' # By RLOIid, to disk
#' download_hydrology(
#'   parameters = "flow",
#'   from_date  = "2022-01-01",
#'   to_date    = "2022-12-31",
#'   output     = "disk",
#'   out_dir    = "hydrology_data",
#'   rloi_ids   = c("5022", "7001")
#' )
#'
#' # From find_stations() output
#' stns <- find_stations(names = "Avon")
#' result <- download_hydrology(
#'   parameters = "rainfall",
#'   from_date  = "2022-01-01",
#'   to_date    = "2022-12-31",
#'   wiski_ids  = stns$wiskiID
#' )
#'
#' # Batch method, qualified data only
#' download_hydrology(
#'   parameters       = c("flow", "level"),
#'   from_date        = "2015-01-01",
#'   to_date          = "2023-12-31",
#'   method           = "batch",
#'   output           = "disk",
#'   out_dir          = "hydrology_data",
#'   observation_type = "Qualified",
#'   wiski_ids        = "SS92F014"
#' )
#' }
download_hydrology <- function(
    parameters       = c("rainfall", "flow", "level"),
    from_date        = "2020-01-01",
    to_date          = "2023-12-31",
    method           = c("sync", "batch"),
    output           = c("memory", "disk"),
    out_dir          = NULL,
    period_name      = NULL,
    value_type       = NULL,
    observation_type = NULL,
    wiski_ids        = NULL,
    rloi_ids         = NULL,
    notations        = NULL,
    chunk_by_year    = TRUE,
    max_wait_s       = 600
) {

  method <- match.arg(method)
  output <- match.arg(output)

  if (output == "disk" && is.null(out_dir)) {
    stop("`out_dir` must be supplied when output = 'disk'.")
  }

  # ── Resolve station identifiers ───────────────────────────────────────────
  # find_stations() now returns one row per measure with station.notation,
  # parameter, value_type, and period columns already parsed. When station
  # filters are supplied, use this output directly rather than calling
  # get_measures() separately — it avoids a second API round trip and gives
  # richer filtering (by parameter, period, value_type) in one step.
  stns_dt <- NULL

  if (!is.null(wiski_ids) || !is.null(rloi_ids) || !is.null(notations)) {
    message("Resolving station identifiers...")
    stns_dt <- find_stations(wiski_ids = wiski_ids,
                             rloi_ids  = rloi_ids,
                             notations = notations)
    if (nrow(stns_dt) == 0) {
      stop("No stations found for the supplied identifiers.")
    }
    message(sprintf("Resolved to %d station(s) / %d measures.",
                    data.table::uniqueN(stns_dt$notation),
                    nrow(stns_dt)))
  }

  # ── Setup ──────────────────────────────────────────────────────────────────
  if (method == "sync") {
    chunks <- if (chunk_by_year) make_date_chunks(from_date, to_date)
              else data.table::data.table(chunk_from = from_date,
                                         chunk_to   = to_date)
    message(sprintf("Method: sync | Output: %s | %d chunk(s).",
                    output, nrow(chunks)))
  } else {
    message(sprintf("Method: batch | Output: %s.", output))
  }

  param_data    <- list()
  all_summaries <- list()

  # ── Loop over each requested parameter ────────────────────────────────────
  for (param in parameters) {

    message(sprintf("\n== Parameter: %s ==", toupper(param)))

    # Resolve period_name and value_type from PARAMETER_CONFIG, allowing
    # caller-supplied values to override the per-parameter defaults.
    config    <- PARAMETER_CONFIG[[param]]
    p_period  <- period_name %||% config$default_period
    p_valtype <- value_type  %||% config$value_type
    message(sprintf("  Config resolved: period='%s', value_type='%s'",
                    p_period %||% "NULL", p_valtype %||% "NULL"))

    if (!is.null(stns_dt)) {
      # Station filter supplied: use find_stations() output directly.
      # Filter to this parameter, then apply period and value_type filters.
      measures <- stns_dt[parameter == param]

      message(sprintf("  Found %d %s measure(s) before period/value_type filter.",
                      nrow(measures), param))

      # isTRUE guards against NULL or length-0 comparisons from config values
      if (isTRUE(nchar(p_period) > 0) && !isTRUE(p_period == "all")) {
        measures <- measures[period == p_period]
      }
      if (isTRUE(nchar(p_valtype) > 0) && !isTRUE(p_valtype == "all")) {
        measures <- measures[value_type == p_valtype]
      }

      message(sprintf("  %d measure(s) after filter (period='%s', value_type='%s').",
                      nrow(measures),
                      p_period  %||% "any",
                      p_valtype %||% "any"))

      if (nrow(measures) == 0) {
        warning(sprintf(
          "No %s measures found for the supplied stations after filtering.",
          param
        ))
        next
      }

      # Use station.notation as the measure ID for fetch_readings()
      measures[, notation := station.notation]

    } else {
      # No station filter: fall back to get_measures() to fetch all measures
      # for this parameter across all stations.
      measures <- get_measures(param,
                               period_name = p_period,
                               value_type  = p_valtype)
      if (nrow(measures) == 0) {
        message("  No measures — skipping.")
        next
      }
    }

    message(sprintf("  Processing %d measures.", nrow(measures)))

    measure_dts   <- list()
    param_summary <- list()

    # ── Loop over each measure ───────────────────────────────────────────────
    for (i in seq_len(nrow(measures))) {

      mid <- measures$notation[i]
      message(sprintf("\n  [%d/%d] %s", i, nrow(measures), mid))

      # Dispatch to the appropriate worker based on method
      result <- if (method == "sync") {
        run_sync(mid, chunks, output, out_dir, param, observation_type)
      } else {
        run_batch(mid, from_date, to_date, output, out_dir, param, max_wait_s)
      }

      if (output == "memory" &&
          !is.null(result$data) &&
          nrow(result$data) > 0) {
        measure_dts[[mid]] <- result$data
      }

      param_summary[[i]] <- data.table::data.table(
        parameter = param,
        measure   = mid,
        file      = result$file %||% NA_character_,
        rows      = result$rows %||% NA_integer_,
        ok        = result$ok   %||% FALSE
      )
    }

    # Combine all measure data.tables for this parameter into one, then wrap
    # in the appropriate S7 class so the caller gets a typed, validated object
    if (output == "memory" && length(measure_dts) > 0) {
      combined_dt <- data.table::rbindlist(measure_dts, fill = TRUE)

      # Resolve the period key used to look up the S7 constructor. p_period
      # should always be set by this point but guard against NULL just in case.
      period_key  <- p_period %||% "15min"
      constructor <- HYDRO_CLASS[[param]][[period_key]]

      if (is.null(constructor)) {
        warning(sprintf(
          "No S7 class for parameter='%s' period='%s' — returning plain data.table.",
          param, period_key
        ))
        param_data[[param]] <- combined_dt
      } else {
        param_data[[param]] <- constructor(
          readings  = combined_dt,
          from_date = from_date,
          to_date   = to_date
        )
      }
    }

    all_summaries[[param]] <- data.table::rbindlist(param_summary)
  }

  # ── Collate summary ────────────────────────────────────────────────────────
  summary_dt <- data.table::rbindlist(all_summaries)

  message("\n-- Summary --")
  for (param in parameters) {
    sub <- summary_dt[parameter == param]
    if (nrow(sub) == 0) next
    if (method == "sync") {
      message(sprintf("  %-10s  %d ok  /  %d failed  /  %d total rows",
                      param, sum(sub$ok), sum(!sub$ok),
                      sum(sub$rows, na.rm = TRUE)))
    } else {
      message(sprintf("  %-10s  %d ok  /  %d failed",
                      param, sum(sub$ok), sum(!sub$ok)))
    }
  }

  if (output == "disk") {
    data.table::fwrite(summary_dt,
                       file.path(out_dir, "download_summary.csv"))
    message(sprintf("Summary written to %s",
                    file.path(out_dir, "download_summary.csv")))
    return(invisible(summary_dt))
  }

  # Memory mode: return list with one data.table per parameter + summary
  c(param_data, list(summary = summary_dt))
}
