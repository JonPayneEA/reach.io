# ============================================================
# Tool:        Source Router
# Description: Dispatches gauge fetches to the correct source
#              system and normalises output to the Bronze
#              Parquet schema. Supports HDE, WISKI, and WISKI_ALL
#              source systems.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: aligned to Bronze schema v1.3
# Tier:        1
# Inputs:      One-row registry data.table; date range
# Outputs:     data.table conforming to Bronze schema
# Dependencies: data.table, httr, arrow
# ============================================================

# -- HDE fetch ----------------------------------------------------------------

#' Fetch data from the EA Hydrology Data Explorer (HDE)
#'
#' Calls the EA Hydrology API for a single gauge and returns readings
#' normalised to the Bronze Parquet schema. Resolves the station and
#' measure notation via [find_stations()], selecting the measure that matches
#' `PARAMETER_CONFIG` (e.g. 15-minute instantaneous flow), then fetches
#' readings in annual chunks via [fetch_readings()].
#'
#' @details
#' - **Fixed resolution.** The measure is chosen from `PARAMETER_CONFIG`
#'   (its `default_period`, plus `value_type` where one is set), so this
#'   function cannot be pointed at a different resolution, such as daily
#'   means; use [download_hydrology()] for that. If more than one measure
#'   matches, the first returned by the API is used without warning.
#' - **Requests.** One station lookup ([find_stations()]), then one readings
#'   request per annual chunk from [make_date_chunks()], with a one second
#'   pause after every chunk. Nothing is written to disk: the caller (for
#'   example [run_backfill()] or [run_incremental()], via [route_gauge()])
#'   stores the result.
#' - **Missing data.** An unknown gauge, or a gauge with no matching measure,
#'   gives a warning (the latter lists the resolutions that do exist) and an
#'   empty `data.table` with `on_error = "warn"`; with `"stop"` it is an error,
#'   so an orchestrator records the gauge as `FAILED` rather than `EMPTY`. A
#'   chunk whose request fails is handled
#'   according to `on_error`: with `"warn"` (the default) each failure is a
#'   warning, the remaining chunks are still fetched and the data returned is
#'   incomplete; with `"stop"` all chunks are still attempted and then an error
#'   is raised naming the chunks that failed, so a partial series is never
#'   returned as if it were complete. [route_gauge()] passes its own `on_error`
#'   through, which is how [run_backfill()] and [run_incremental()] record such
#'   a gauge as `FAILED`. Errors from the station lookup itself (for example an
#'   HTTP failure) are always raised.
#' - **Timestamps.** `timestamp` comes from the readings' `dateTime`. For
#'   daily aggregates that is midnight UTC; the time of occurrence of a daily
#'   maximum or minimum is not available from this endpoint. If no valid
#'   `dateTime` is present, the date-only `date` column is used instead, with
#'   a warning.
#' - **Bronze fields.** `supplier_flag` is the EA `quality` code, `site_id` is
#'   `gauge_id`, and `dataset_id` uses supplier code `"EA"` and today's date as
#'   the received date.
#'
#' @param gauge_id Character. WISKI ID for HDE-sourced gauges.
#' @param data_type Character. Parameter: `"flow"`, `"level"`, or
#'   `"rainfall"`.
#' @param start_date Character. Start date (inclusive) in `"YYYY-MM-DD"` format.
#' @param end_date Character. End date (**exclusive**) in `"YYYY-MM-DD"` format.
#' @param on_error Character. `"warn"` (default) turns problems into warnings
#'   and returns what was fetched (or an empty table); `"stop"` raises an error
#'   instead, for an unknown gauge, a missing measure, or (after all chunks have
#'   been tried) one or more failed chunks.
#'
#' @return A `data.table` conforming to the Bronze schema (`timestamp`,
#'   `value`, `supplier_flag`, `dataset_id`, `site_id`, `data_type`),
#'   or an empty `data.table` if no data found.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' fetch_from_hde("SS92F014", "flow", "2020-01-01", "2021-01-01")
#' }
fetch_from_hde <- function(gauge_id, data_type, start_date, end_date,
                           on_error = c("warn", "stop")) {

  on_error <- match.arg(on_error)

  message(sprintf("  [HDE] %s | %s | %s to %s",
                  gauge_id, data_type, start_date, end_date))

  # -- Resolve the station and measure ---------------------------------------
  # Both "no station" and "no measure" are soft failures (warning + empty
  # result) so one unavailable gauge does not raise inside a larger run.
  stn <- find_stations(wiski_ids = gauge_id)
  if (nrow(stn) == 0) {
    msg <- sprintf("[HDE] No station found for gauge_id: %s", gauge_id)
    # "stop": a registered gauge the source does not know is a failure to
    # investigate, not an empty response (which the orchestrators log EMPTY)
    if (on_error == "stop") stop(msg, call. = FALSE)
    warning(msg)
    return(data.table::data.table())
  }

  # Filter to the correct parameter, period, and value_type from PARAMETER_CONFIG
  # so we reliably get e.g. 15-min instantaneous flow rather than daily mean
  # (`data_type` is a function argument: this is safe only because `stn` has
  # no column of that name, see .filter_rows() in package.R for the hazard).
  config <- PARAMETER_CONFIG[[data_type]]

  measures_dt <- stn[parameter == data_type &
                       period    == config$default_period]
  # rainfall has no value_type in its config (a single "total" type exists),
  # so the value_type filter is applied only where one is configured.
  if (!is.null(config$value_type)) {
    measures_dt <- measures_dt[value_type == config$value_type]
  }

  if (nrow(measures_dt) == 0) {
    # Warn with what IS available so the user can diagnose missing resolutions
    available <- stn[parameter == data_type,
                     paste(period, value_type, sep = "/")]
    msg <- sprintf(
      "[HDE] No %s %s %s measure found for gauge_id: %s. Available: %s",
      config$default_period,
      config$value_type %||% "(any)",
      data_type,
      gauge_id,
      if (length(available)) paste(available, collapse = ", ") else "none"
    )
    if (on_error == "stop") stop(msg, call. = FALSE)
    warning(msg)
    return(data.table::data.table())
  }

  # If several measures match, the first is taken; no ordering is imposed here.
  measure_notation <- measures_dt$measure_notation[1L]

  # -- Fetch annual chunks ---------------------------------------------------
  # Annual chunks keep each request under the API row limit. chunk_list is
  # pre-sized so a failed or empty chunk simply leaves a NULL slot, dropped
  # by Filter() below.
  chunks     <- make_date_chunks(start_date, end_date)
  chunk_list <- vector("list", nrow(chunks))
  failed     <- character()

  for (j in seq_len(nrow(chunks))) {
    # A chunk failure never aborts the loop: the other chunks are still
    # fetched. What happens afterwards depends on on_error: "warn" returns the
    # partial series (each failure was already a warning), "stop" raises once
    # the loop is done (below) so the caller can record the gauge as FAILED
    # instead of storing an incomplete series as if it were whole.
    dt <- tryCatch(
      fetch_readings(measure_notation,
                     chunks$chunk_from[j],
                     chunks$chunk_to[j]),
      error = function(e) {
        failed <<- c(failed, sprintf("%s to %s: %s", chunks$chunk_from[j],
                                     chunks$chunk_to[j], e$message))
        if (on_error == "warn") {
          warning(sprintf("[HDE] Chunk %d failed for %s: %s",
                          j, gauge_id, e$message))
        }
        NULL
      }
    )
    if (!is.null(dt) && nrow(dt) > 0L) chunk_list[[j]] <- dt
    # EA fair use: one request at a time. Unlike run_sync(), this pauses after
    # every chunk, including empty and failed ones.
    .pace()
  }

  if (length(failed) > 0L && on_error == "stop") {
    stop(sprintf("[HDE] %d of %d chunk(s) failed for %s (%s)",
                 length(failed), nrow(chunks), gauge_id,
                 paste(failed, collapse = "; ")), call. = FALSE)
  }

  chunk_list <- Filter(Negate(is.null), chunk_list)
  if (length(chunk_list) == 0L) return(data.table::data.table())

  dt <- data.table::rbindlist(chunk_list, fill = TRUE)

  # -- Normalise to the Bronze schema ----------------------------------------
  # Prefer dateTime (full sub-daily timestamp) over date (date only).
  # For instantaneous 15-min/hourly measures, dateTime carries HH:MM:SS.
  # For daily aggregate measures (mean, max, min), dateTime is midnight UTC
  # and no time-of-occurrence (e.g. time of peak flow) is available from
  # the HDE readings endpoint — derive it from the 15-min series if needed.
  has_datetime <- "dateTime" %in% names(dt) &&
    any(!is.na(dt[["dateTime"]]))
  ts_col <- if (has_datetime) {
    "dateTime"
  } else {
    warning(sprintf(
      "[HDE] No valid dateTime in response for %s - falling back to date-only timestamps.",
      gauge_id
    ))
    "date"
  }
  # The EA `quality` code becomes the Bronze supplier_flag; NULL (no flag
  # column) is accepted by apply_bronze_schema().
  flag_col <- if ("quality" %in% names(dt)) "quality" else NULL

  # The dataset_id ends in today's date (the received date), which is what
  # makes Bronze append-only across re-ingests. Supplier code is "EA" for the
  # Hydrology API.
  dataset_id <- make_dataset_id(
    supplier_code = "EA",
    site_id       = gauge_id,
    data_type     = param_to_data_type(data_type)
  )

  apply_bronze_schema(
    dt                = dt,
    dataset_id        = dataset_id,
    site_id           = gauge_id,
    data_type         = param_to_data_type(data_type),
    timestamp_col     = ts_col,
    value_col         = "value",
    supplier_flag_col = flag_col,
    period            = config$default_period
  )
}


# -- WISKI fetch --------------------------------------------------------------

#' Fetch data from WISKI / KiWIS
#'
#' **DRAFT — not yet operational.** The EA WISKI API is not currently
#' available. This function is implemented against the KiWIS REST API
#' specification (`getTimeseriesValues` endpoint) as documented in the
#' Kisters KiWIS QueryServices reference, and will be activated once the
#' endpoint is accessible. Calling it now raises an error.
#'
#' When live, connection details will be read from environment variables
#' `WISKI_BASE_URL` and optionally `WISKI_API_KEY`.
#'
#' @details
#' Because the function stops before doing anything else, it makes no network
#' request and reads no environment variables today. A registry row with
#' `source_system = "WISKI"` therefore fails: [route_gauge()] returns `NULL`
#' with a warning under `on_error = "warn"`, and [run_backfill()] and
#' [run_incremental()] record the gauge as `FAILED`. For WISKI data use
#' [fetch_from_wiski_all()] (`source_system = "WISKI_ALL"`) or
#' [ingest_all_file()].
#'
#' The source below the `stop()` is the intended KiWIS implementation and is
#' kept unreachable (and excluded from coverage) until the endpoint is
#' available. One point to revisit when it is activated: `gauge_id` is passed
#' straight through as the KiWIS `ts_id`, so it would have to be a time-series
#' id rather than a station number. (It already treats `end_date` as exclusive,
#' like the rest of the package, and records the series `period` in Bronze.)
#'
#' @inheritParams fetch_from_hde
#'
#' @return A `data.table` conforming to the Bronze schema, or an empty
#'   `data.table` if no data found.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Not yet operational — WISKI API endpoint not available
#' Sys.setenv(WISKI_BASE_URL = "https://wiski.example.com/KiWIS/KiWIS")
#' fetch_from_wiski("12345678", "level", "2020-01-01", "2020-12-31")
#' }
fetch_from_wiski <- function(gauge_id, data_type, start_date, end_date) {

  # Deliberate guard: fail loudly rather than return empty data, which an
  # orchestrator would log as EMPTY and could be mistaken for "no readings".
  # Everything after this stop() is unreachable until it is removed.
  stop(
    "[WISKI] fetch_from_wiski() is a draft function and is not yet ",
    "operational. The EA WISKI API endpoint is not currently available. ",
    "Use source_system = 'WISKI_ALL' to ingest data from .all export files."
  )

  # -- Draft implementation against KiWIS QueryServices specification ---------
  # Reference: Kisters KiWIS REST API, getTimeseriesValues endpoint.
  # To be activated once WISKI_BASE_URL is accessible.
  # Dead code by design (see the stop() above), hence the nocov markers so
  # it does not count against coverage. It has not been run against a live
  # KiWIS server, so treat it as a sketch rather than tested behaviour.
  # nocov start

  message(sprintf("  [WISKI] %s | %s | %s to %s",
                  gauge_id, data_type, start_date, end_date))

  # The base URL is required; the API key is optional (added only if set).
  base_url <- Sys.getenv("WISKI_BASE_URL", unset = NA_character_)
  api_key  <- Sys.getenv("WISKI_API_KEY",  unset = NA_character_)
  if (is.na(base_url)) stop("[WISKI] WISKI_BASE_URL environment variable not set.")

  # KiWIS expresses the range as a from(...)to(...) string in UTC.
  query <- list(
    service      = "kisters",
    type         = "queryServices",
    request      = "getTimeseriesValues",
    ts_id        = gauge_id,
    # end_date is EXCLUSIVE in this package; KiWIS' to() is inclusive, so ask
    # up to midnight of end_date and drop that instant after parsing (below)
    period       = sprintf("from(%sT00:00:00Z)to(%sT00:00:00Z)",
                           start_date, end_date),
    returnfields = "Timestamp,Value,Quality Code",
    format       = "json",
    dateformat   = "yyyy-MM-dd HH:mm:ss"
  )
  if (!is.na(api_key)) query$authToken <- api_key

  resp <- httr::GET(base_url, query = query)
  httr::stop_for_status(resp)
  body <- httr::content(resp, as = "parsed", simplifyVector = TRUE)

  if (length(body) == 0L || length(body[[1L]]$data) == 0L) {
    message(sprintf("  [WISKI] No data returned for ts_id: %s", gauge_id))
    return(data.table::data.table())
  }

  # The columns are renamed by position, so this relies on returnfields above
  # coming back in the order Timestamp, Value, Quality Code.
  raw <- data.table::as.data.table(body[[1L]]$data)
  data.table::setnames(raw, c("datetime_str", "value", "supplier_flag"))
  raw[, datetime_str := as.POSIXct(datetime_str,
                                   format = "%Y-%m-%d %H:%M:%S",
                                   tz     = "UTC")]
  # Unparseable timestamps or non-numeric values become NA and are dropped
  # (the as.numeric() warning is suppressed since the drop is intended).
  raw[, value        := suppressWarnings(as.numeric(value))]
  raw <- raw[!is.na(datetime_str) & !is.na(value) &
               datetime_str < as.POSIXct(end_date, tz = "UTC")]

  dataset_id <- make_dataset_id(
    supplier_code = "WISKI",
    site_id       = gauge_id,
    data_type     = param_to_data_type(data_type)
  )

  apply_bronze_schema(
    dt                = raw,
    dataset_id        = dataset_id,
    site_id           = gauge_id,
    data_type         = param_to_data_type(data_type),
    timestamp_col     = "datetime_str",
    value_col         = "value",
    supplier_flag_col = "supplier_flag",
    # Resolution of the configured series, as fetch_from_hde() records it
    period            = PARAMETER_CONFIG[[data_type]]$default_period
  )
  # nocov end
}


# -- Router -------------------------------------------------------------------

#' Route a single gauge fetch to the correct source system
#'
#' Reads the `source_system` field of a one-row registry `data.table` and
#' dispatches to [fetch_from_hde()], [fetch_from_wiski()],
#' or [fetch_from_wiski_all()].
#'
#' With the default `on_error = "warn"`, fetch errors are caught and `NULL` is
#' returned with a warning, which suits interactive use. Orchestrators such as
#' [run_backfill()] use `on_error = "stop"` so that a failed fetch is recorded
#' as `FAILED` rather than being mistaken for an empty response.
#'
#' To add a new source system: write a `fetch_from_<n>()` function returning
#' the Bronze schema, add a case to the `switch()` below, update
#' `VALID_SOURCES` in `package.R`, and re-run [build_gauge_registry()].
#'
#' @details
#' - **Dispatch.** `source_system` selects the client (`"HDE"`, `"WISKI"` or
#'   `"WISKI_ALL"`); any other value raises `No client for source_system`,
#'   which is then handled according to `on_error` like any fetch error.
#'   `gauge_id` and `data_type` are passed through unchanged along with the
#'   dates, so `end_date` keeps the package-wide exclusive meaning.
#'   Pass exactly one row, such as `registry_dt[gauge_id == "39001"]`; a
#'   multi-row input fails inside `switch()` and is handled as a fetch error.
#' - **Errors versus empty data.** An empty `data.table` is a normal result
#'   (the source had nothing) and is returned as is, whereas an error is a
#'   failure. With `on_error = "warn"` a failure comes back as `NULL`, which a
#'   caller that treats `NULL` and empty alike would read as "no data" (the
#'   orchestrators would log `EMPTY`). Batch workflows should therefore use
#'   `"stop"` and record the failure, as [run_backfill()] and
#'   [run_incremental()] do. The re-raised error is prefixed with the gauge ID
#'   and drops the original call.
#' - **Warnings are not errors.** Problems the clients report as warnings
#'   (for example an unknown gauge in [fetch_from_hde()]) are not intercepted
#'   here and do not trigger `on_error`. A failed HDE chunk is handled
#'   separately: `on_error` is passed down, so with `"stop"` it becomes an
#'   error here.
#' - **Side effects.** None of its own: the network or file access is done by
#'   the chosen client, and the result is not written to Bronze here.
#'   [fetch_from_wiski()] always errors at present.
#'
#' @param gauge_row A one-row `data.table` from the gauge registry. Must
#'   contain columns `gauge_id`, `source_system`, and `data_type`.
#' @param start_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param end_date Character. End date (**exclusive**) in `"YYYY-MM-DD"` format.
#' @param on_error Character. `"warn"` (default) returns `NULL` with a warning
#'   when the fetch fails; `"stop"` re-raises the error.
#'
#' @return A `data.table` conforming to the Bronze schema (empty if no data
#'   was found), or `NULL` if the fetch failed and `on_error = "warn"`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' registry_dt <- data.table::as.data.table(
#'   arrow::read_parquet(
#'     "data/hydrometric/register/gauge_registry.parquet"
#'   )
#' )
#' dt <- route_gauge(registry_dt[gauge_id == "39001"],
#'                   start_date = "2020-01-01",
#'                   end_date   = "2021-01-01")
#' }
route_gauge <- function(gauge_row, start_date, end_date,
                        on_error = c("warn", "stop")) {

  on_error <- match.arg(on_error)

  # Only errors are intercepted (tryCatch has no warning handler), so a
  # client's own warnings pass straight through to the caller.
  tryCatch({
    # switch() falls through to the final unnamed stop() for an unrecognised
    # source_system, so an unknown value is an error like any other failure.
    switch(
      gauge_row$source_system,
      "HDE"       = fetch_from_hde(
                                   gauge_row$gauge_id, gauge_row$data_type,
                                   start_date, end_date, on_error = on_error),
      "WISKI"     = fetch_from_wiski(
                                     gauge_row$gauge_id, gauge_row$data_type,
                                     start_date, end_date),
      "WISKI_ALL" = fetch_from_wiski_all(
                                         gauge_row$gauge_id, gauge_row$data_type,
                                         start_date, end_date, on_error = on_error),
      stop("No client for source_system: ", gauge_row$source_system)
    )
  }, error = function(e) {
    # "stop": re-raise with the gauge ID attached so the orchestrators' FAILED
    # log row says which gauge failed. call. = FALSE keeps the message clean.
    if (on_error == "stop") {
      stop(sprintf("Route failed for %s: %s", gauge_row$gauge_id, e$message),
           call. = FALSE)
    }
    # "warn": NULL (not an empty data.table) marks "failed", which callers can
    # tell apart from "fetched but empty".
    warning(sprintf("Route failed for %s: %s",
                    gauge_row$gauge_id, e$message))
    NULL
  })
}
