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
# Outputs:     Named list of S7 objects or Bronze Parquet files
# Dependencies: httr, data.table, S7
# ============================================================

# ── Main entry point ──────────────────────────────────────────────────────────

#' Series traces that keep several measures at one station apart
#'
#' Builds a short, file-name-safe trace from each measure's period and value
#' type (e.g. `"15min_instantaneous"`), taking the columns from either
#' [find_stations()] (`period`, `value_type`) or [get_measures()]
#' (`periodName`, `valueType`). Traces are made unique within the input, so two
#' measures that differ only in other respects still get different traces.
#'
#' @param m A `data.table` of measures that share a station.
#' @return A character vector of traces, one per row of `m`.
#' @noRd
.measure_traces <- function(m) {
  pick <- function(a, b) {
    if (a %in% names(m)) m[[a]] else if (b %in% names(m)) m[[b]] else rep(NA_character_, nrow(m))
  }
  per <- pick("period", "periodName")
  vt  <- pick("value_type", "valueType")
  tr  <- vapply(seq_len(nrow(m)), function(i) {
    parts <- c(per[i], vt[i])
    parts <- parts[!is.na(parts) & nzchar(parts)]
    if (length(parts) == 0L) "measure" else paste(parts, collapse = "_")
  }, character(1L))
  tr <- gsub("^_+|_+$", "", gsub("[^a-z0-9]+", "_", tolower(tr)))
  make.unique(tr, sep = "_")
}


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
#' - **`"disk"`** — Writes one Bronze-tier Parquet file per measure per
#'   observation year under `out_dir`, following the path convention
#'   `bronze/hydrometric/EA/<data_type>/<YYYY>/<dataset_id>.parquet`.
#'   Returns the summary `data.table` invisibly.
#'
#' The EA API fair-use guidance asks that automated users issue only one
#' request at a time. Both methods are intentionally sequential.
#'
#' @details
#' ## Station resolution
#' If any of `wiski_ids`, `rloi_ids` or `notations` is supplied,
#' [find_stations()] is called once up front (one request per identifier) and
#' its one-row-per-measure table is reused for every parameter, filtered to
#' that parameter's `period_name` and `value_type`. Identifiers that match no
#' station are warned about by [find_stations()]; the call stops only if none
#' of them resolve. The station identifier recorded in the Bronze `site_id`
#' is the WISKI ID, falling back to the station notation (SUID) when the
#' station has no WISKI ID.
#'
#' If no identifier is supplied, [get_measures()] is queried once per
#' parameter and every matching measure in the country is downloaded.
#'
#' ## Defaults and filters
#' `period_name = NULL` and `value_type = NULL` fall back, per parameter, to
#' the defaults in `PARAMETER_CONFIG` (currently 15-minute data; instantaneous
#' for flow and level, no value-type filter for rainfall). The value `"all"`
#' skips the corresponding filter, but `period_name = "all"` is only
#' meaningful when station identifiers are supplied: the [get_measures()]
#' path passes the period straight to the API.
#' `parameters` is not validated against `PARAMETER_CONFIG`; an unrecognised
#' name surfaces as "no measures" (or an error from [get_measures()]).
#'
#' ## Ordering and failure handling
#' Parameters are processed in the order given and, within a parameter,
#' measures in the order returned by the lookup. A failed fetch for one
#' measure (or one sync chunk) is reported as a message rather than an error:
#' the loop carries on and the failure is recorded as `ok = FALSE` in
#' `summary`. Errors while writing Parquet in disk mode are not caught. In
#' sync mode a measure with one failed chunk still keeps the chunks that
#' succeeded, so `ok = FALSE` can accompany partial data; always check
#' `summary$ok` before treating a download as complete. `rows` counts rows
#' fetched, and is `NA` for a failed batch job.
#'
#' ## Memory mode
#' Each parameter's readings are combined across measures and wrapped in the
#' S7 class for that parameter and period (see `HYDRO_CLASS`); if no class
#' exists for the combination a plain `data.table` is returned with a warning.
#' A parameter with no data is simply absent from the result list, so index it
#' defensively; `summary` is always present.
#'
#' ## Disk mode
#' - Each measure is written by `handle_output()` through the shared
#'   year-partitioned Bronze writer, so sync and batch give the same layout.
#'   `dataset_id` ends in the date of receipt, so Bronze is append-only:
#'   downloading the same series on a later day adds files, while repeating it
#'   on the same day merges the new rows into that day's files (new values
#'   win for timestamps present in both).
#' - A `dataset_id` carries the site and data type but not the period or value
#'   type. When one run selects several measures of a data type at the same
#'   station (for example with `period_name = "all"` or `value_type = "all"`),
#'   each is given a trace built from its period and value type (such as
#'   `"15min_instantaneous"`) so they are filed separately instead of mixing in
#'   one file. Read one back with `read_bronze(trace = )`. A station with a
#'   single selected measure keeps the plain, trace-free `dataset_id`.
#' - One provenance row per file written is appended to the Hydrometric Data
#'   Register at `register_path` (default
#'   `<out_dir>/register/hydrometric_data_register.csv`).
#' - `download_summary.csv` is written to `out_dir` and replaced on every
#'   call.
#' - Under `method = "batch"` each measure's whole CSV is held in memory
#'   while it is converted, so very large batch results need correspondingly
#'   large RAM.
#'
#' ## Network
#' Requests are strictly sequential, with a one second pause after each sync
#' chunk that returns data and after each batch download. Station lookup
#' costs one request per identifier. See [fetch_readings()] for the sync
#' request and [make_date_chunks()] for the annual chunking.
#'
#' @param parameters Character vector. Any combination of `"rainfall"`,
#'   `"flow"`, and `"level"`. Default is all three.
#' @param from_date Character. Start date (inclusive) in `"YYYY-MM-DD"` format.
#' @param to_date Character. End date (**exclusive**) in `"YYYY-MM-DD"` format;
#'   to include 31 December pass 1 January of the following year.
#' @param method Character. `"sync"` (default) or `"batch"`.
#' @param output Character. `"memory"` (default) or `"disk"`.
#' @param out_dir Character or `NULL`. Root store directory. Required when
#'   `output = "disk"`. Bronze Parquet files are written under
#'   `out_dir/bronze/hydrometric/EA/<data_type>/`.
#' @param period_name Character. Temporal resolution: `"daily"` or `"15min"`.
#'   `NULL` (default) uses each parameter's default from `PARAMETER_CONFIG`
#'   (currently `"15min"`).
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
#'   the summary `data.table` invisibly, with Bronze Parquet written under
#'   `out_dir` (`file` holds the paths written, joined by `"; "`).
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
    category         = "hydrometric",
    register_path    = NULL,
    period_name      = NULL,
    value_type       = NULL,
    observation_type = NULL,
    wiski_ids        = NULL,
    rloi_ids         = NULL,
    notations        = NULL,
    chunk_by_year    = TRUE,
    max_wait_s       = 600
) {

  # -- Validate arguments and defaults ----------------------------------------
  # Fail before any network request is made.
  method <- match.arg(method)
  output <- match.arg(output)

  if (output == "disk" && is.null(out_dir)) {
    stop("`out_dir` must be supplied when output = 'disk'.")
  }
  if (!category %in% VALID_CATEGORIES) {
    stop("`category` must be one of: ", paste(VALID_CATEGORIES, collapse = ", "))
  }
  # The register lives inside the store being written to, so a disk run is
  # self-contained. In memory mode register_path stays NULL and is never used.
  if (output == "disk" && is.null(register_path)) {
    register_path <- file.path(out_dir, "register", "hydrometric_data_register.csv")
  }

  # ── Resolve station identifiers ───────────────────────────────────────────
  # find_stations() returns one row per measure with measure_notation,
  # parameter, value_type, and period columns already parsed. When station
  # filters are supplied, use this output directly rather than calling
  # get_measures() separately — it avoids a second API round trip and gives
  # richer filtering (by parameter, period, value_type) in one step.
  # stns_dt stays NULL when no identifier is supplied; that NULL is the switch
  # between the find_stations() and get_measures() branches in the loop below.
  stns_dt <- NULL

  if (!is.null(wiski_ids) || !is.null(rloi_ids) || !is.null(notations)) {
    message("Resolving station identifiers...")
    stns_dt <- find_stations(wiski_ids = wiski_ids,
                             rloi_ids  = rloi_ids,
                             notations = notations)
    # Hard stop: with identifiers supplied, an empty lookup must not silently
    # fall through to the unfiltered "every station" download.
    if (nrow(stns_dt) == 0) {
      stop("No stations found for the supplied identifiers.")
    }
    message(sprintf("Resolved to %d station(s) / %d measures.",
                    data.table::uniqueN(stns_dt$notation),
                    nrow(stns_dt)))
  }

  # ── Setup ──────────────────────────────────────────────────────────────────
  # Chunks are built once and shared by every measure. Only sync needs them:
  # batch jobs take the whole range in a single request. With
  # chunk_by_year = FALSE the range is one chunk, which is only safe while a
  # single measure stays under the API row limit (run_sync() warns if hit).
  if (method == "sync") {
    chunks <- if (chunk_by_year) make_date_chunks(from_date, to_date)
              else data.table::data.table(chunk_from = from_date,
                                         chunk_to   = to_date)
    message(sprintf("Method: sync | Output: %s | %d chunk(s).",
                    output, nrow(chunks)))
  } else {
    message(sprintf("Method: batch | Output: %s.", output))
  }

  # param_data: S7 objects (or data.tables), memory mode only.
  # all_summaries: one summary table per parameter, in both modes.
  param_data    <- list()
  all_summaries <- list()

  # ── Loop over each requested parameter ────────────────────────────────────
  for (param in parameters) {

    message(sprintf("\n== Parameter: %s ==", toupper(param)))

    # Resolve period_name and value_type from PARAMETER_CONFIG, allowing
    # caller-supplied values to override the per-parameter defaults. These are
    # deliberately named p_period / p_valtype (and the loop variable `param`):
    # inside stns_dt[...] below, names that match a column (period, value_type,
    # parameter) would resolve to the column and give silent wrong filters.
    # A NULL p_valtype (rainfall) means "no value_type filter".
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
      # (nchar(NULL) > 0 is logical(0)): a NULL means "do not filter", and
      # "all" is the explicit sentinel for the same thing.
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

      # Nothing left for this parameter is not fatal to the run (other
      # parameters may still have data): warn, record a failed placeholder row
      # in the summary so the gap is visible, and move to the next parameter.
      if (nrow(measures) == 0) {
        warning(sprintf(
          "No %s measures found for the supplied stations after filtering.",
          param
        ))
        all_summaries[[param]] <- data.table::data.table(
          parameter = param, measure = NA_character_,
          file = NA_character_, rows = 0L, ok = FALSE
        )
        next
      }

      # Preserve the station identifier as site_id before overwriting notation
      # with the measure notation. wiskiID preferred; fall back to station SUID.
      # Order matters: the fallback reads the station `notation`, so site_id
      # must be computed first. After this, `notation` means "measure ID" in
      # both branches, which is what the per-measure loop relies on.
      # (`measures` is a filtered copy, so these := do not touch stns_dt.)
      measures[, site_id  := data.table::fifelse(
        !is.na(wiskiID) & nzchar(wiskiID), wiskiID, notation)]
      measures[, notation := measure_notation]

    } else {
      # No station filter: fall back to get_measures() to fetch all measures
      # for this parameter across all stations. Its `notation` column is
      # already the measure ID, so no rename is needed. Period and value_type
      # are applied server-side here, and only value_type = "all" is
      # understood by get_measures().
      measures <- get_measures(param,
                               period_name = p_period,
                               value_type  = p_valtype)
      if (nrow(measures) == 0) {
        message("  No measures - skipping.")
        all_summaries[[param]] <- data.table::data.table(
          parameter = param, measure = NA_character_,
          file = NA_character_, rows = 0L, ok = FALSE
        )
        next
      }
      # get_measures() returns station.wiskiID; use it as the site identifier
      if ("station.wiskiID" %in% names(measures)) {
        measures[, site_id := data.table::fifelse(
          !is.na(station.wiskiID) & nzchar(station.wiskiID),
          station.wiskiID, notation)]
      } else {
        measures[, site_id := notation]
      }
    }

    message(sprintf("  Processing %d measures.", nrow(measures)))

    # measure_dts is keyed by measure notation and only holds measures that
    # returned rows; param_summary has an entry for every measure attempted.
    measure_dts   <- list()
    param_summary <- list()

    # ── Series traces (disk mode) ────────────────────────────────────────────
    # A dataset_id carries the site and data type but not the period or value
    # type. If a station has several selected measures of this parameter (for
    # example instantaneous and daily maximum with value_type = "all"), they
    # would be written to the SAME file and mix. Give such measures a trace
    # built from period + value type so each is filed (and read back with
    # read_bronze(trace = )) separately. A single measure per station keeps the
    # plain, trace-free dataset_id.
    measure_traces <- rep(NA_character_, nrow(measures))
    if (output == "disk") {
      shared <- duplicated(measures$site_id) | duplicated(measures$site_id, fromLast = TRUE)
      if (any(shared)) {
        measure_traces[shared] <- .measure_traces(measures[which(shared)])
      }
    }

    # ── Loop over each measure ───────────────────────────────────────────────
    for (i in seq_len(nrow(measures))) {

      mid         <- measures$notation[i]
      site_id_val <- measures$site_id[i]
      trace_val   <- if (is.na(measure_traces[i])) NULL else measure_traces[i]

      # The Bronze `period` column should hold THIS measure's resolution. With
      # period_name = "all", p_period is the literal "all", which is a filter
      # setting rather than a resolution, so prefer the measure's own value
      # (find_stations() gives `period`, get_measures() gives `periodName`).
      period_val <- p_period
      for (pc in c("period", "periodName")) {
        if (pc %in% names(measures) && !is.na(measures[[pc]][i])) {
          period_val <- measures[[pc]][i]
          break
        }
      }
      message(sprintf("\n  [%d/%d] %s", i, nrow(measures), mid))

      # Dispatch to the appropriate worker based on method. Both workers
      # return the same list shape (measure, data, file, rows, ok). Fetch
      # errors are caught inside the workers and returned as ok = FALSE, so
      # one bad measure does not abort the run. (run_sync() does not catch
      # errors from the disk write itself, which would propagate from here.)
      # Each worker paces its own requests (.pace()) for EA fair use.
      result <- if (method == "sync") {
        run_sync(mid, chunks, output, out_dir, param, observation_type,
                 site_id = site_id_val, period = period_val,
                 category = category, register_path = register_path,
                 trace = trace_val)
      } else {
        run_batch(mid, from_date, to_date, output, out_dir, param, max_wait_s,
                  site_id = site_id_val, period = period_val,
                  category = category, register_path = register_path,
                  trace = trace_val)
      }

      if (output == "memory" &&
          !is.null(result$data) &&
          nrow(result$data) > 0) {
        measure_dts[[mid]] <- result$data
      }

      # In disk mode result$data is NULL (the worker already wrote the file),
      # so only the summary row is kept. %||% supplies NA / FALSE for any
      # field a worker leaves NULL, so a missing `ok` reads as a failure.
      param_summary[[i]] <- data.table::data.table(
        parameter = param,
        measure   = mid,
        file      = result$file %||% NA_character_,
        rows      = result$rows %||% NA_integer_,
        ok        = result$ok   %||% FALSE
      )
    }

    # Combine all measure data.tables for this parameter into one, then wrap
    # in the appropriate S7 class so the caller gets a typed, validated object.
    # fill = TRUE tolerates measures whose responses differ in columns. If no
    # measure returned rows, param_data gets no entry for this parameter.
    if (output == "memory" && length(measure_dts) > 0) {
      combined_dt <- data.table::rbindlist(measure_dts, fill = TRUE)

      # Resolve the period key used to look up the S7 constructor. p_period
      # should always be set by this point but guard against NULL just in case.
      period_key  <- p_period %||% "15min"
      constructor <- HYDRO_CLASS[[param]][[period_key]]

      # No class for this parameter/period pair (e.g. period "all"): degrade
      # to a plain data.table rather than losing the downloaded data.
      if (is.null(constructor)) {
        warning(sprintf(
          "No S7 class for parameter='%s' period='%s' - returning plain data.table.",
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

  # Console recap only. Row totals are shown for sync alone (batch rows are
  # NA after a failed job).
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

  # Disk mode returns the summary invisibly: the data itself is already in the
  # Bronze store. The CSV is a convenience log and is overwritten each call.
  if (output == "disk") {
    dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
    data.table::fwrite(summary_dt,
                       file.path(out_dir, "download_summary.csv"))
    message(sprintf("Summary written to %s",
                    file.path(out_dir, "download_summary.csv")))
    return(invisible(summary_dt))
  }

  # Memory mode: return list with one data.table per parameter + summary
  c(param_data, list(summary = summary_dt))
}
