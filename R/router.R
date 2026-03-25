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
#' @param gauge_id Character. WISKI ID for HDE-sourced gauges.
#' @param data_type Character. Parameter: `"flow"`, `"level"`, or
#'   `"rainfall"`.
#' @param start_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param end_date Character. End date in `"YYYY-MM-DD"` format.
#'
#' @return A `data.table` conforming to the Bronze schema (`timestamp`,
#'   `value`, `supplier_flag`, `dataset_id`, `site_id`, `data_type`),
#'   or an empty `data.table` if no data found.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' fetch_from_hde("SS92F014", "flow", "2020-01-01", "2020-12-31")
#' }
fetch_from_hde <- function(gauge_id, data_type, start_date, end_date) {

  message(sprintf("  [HDE] %s | %s | %s to %s",
                  gauge_id, data_type, start_date, end_date))

  stn <- find_stations(wiski_ids = gauge_id)
  if (nrow(stn) == 0) {
    warning(sprintf("[HDE] No station found for gauge_id: %s", gauge_id))
    return(data.table::data.table())
  }

  # Filter to the correct parameter, period, and value_type from PARAMETER_CONFIG
  # so we reliably get e.g. 15-min instantaneous flow rather than daily mean
  config <- PARAMETER_CONFIG[[data_type]]

  measures_dt <- stn[parameter == data_type &
                     period    == config$default_period]
  if (!is.null(config$value_type)) {
    measures_dt <- measures_dt[value_type == config$value_type]
  }

  if (nrow(measures_dt) == 0) {
    # Warn with what IS available so the user can diagnose missing resolutions
    available <- stn[parameter == data_type,
                     paste(period, value_type, sep = "/")]
    warning(sprintf(
      "[HDE] No %s %s %s measure found for gauge_id: %s. Available: %s",
      config$default_period,
      config$value_type %||% "(any)",
      data_type,
      gauge_id,
      if (length(available)) paste(available, collapse = ", ") else "none"
    ))
    return(data.table::data.table())
  }

  measure_notation <- measures_dt$station.notation[1L]

  chunks     <- make_date_chunks(start_date, end_date)
  chunk_list <- vector("list", nrow(chunks))

  for (j in seq_len(nrow(chunks))) {
    dt <- tryCatch(
      fetch_readings(measure_notation,
                     chunks$chunk_from[j],
                     chunks$chunk_to[j]),
      error = function(e) {
        warning(sprintf("[HDE] Chunk %d failed for %s: %s",
                        j, gauge_id, e$message))
        NULL
      }
    )
    if (!is.null(dt) && nrow(dt) > 0L) chunk_list[[j]] <- dt
    Sys.sleep(1)
  }

  chunk_list <- Filter(Negate(is.null), chunk_list)
  if (length(chunk_list) == 0L) return(data.table::data.table())

  dt <- data.table::rbindlist(chunk_list, fill = TRUE)

  # Determine timestamp column — full datetime preferred over date only
  ts_col <- if ("dateTime" %in% names(dt)) "dateTime" else "date"
  flag_col <- if ("quality" %in% names(dt)) "quality" else NULL

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
    supplier_flag_col = flag_col
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

  stop(
    "[WISKI] fetch_from_wiski() is a draft function and is not yet ",
    "operational. The EA WISKI API endpoint is not currently available. ",
    "Use source_system = 'WISKI_ALL' to ingest data from .all export files."
  )

  # -- Draft implementation against KiWIS QueryServices specification ---------
  # Reference: Kisters KiWIS REST API, getTimeseriesValues endpoint.
  # To be activated once WISKI_BASE_URL is accessible.
  # nocov start

  message(sprintf("  [WISKI] %s | %s | %s to %s",
                  gauge_id, data_type, start_date, end_date))

  base_url <- Sys.getenv("WISKI_BASE_URL", unset = NA_character_)
  api_key  <- Sys.getenv("WISKI_API_KEY",  unset = NA_character_)
  if (is.na(base_url)) stop("[WISKI] WISKI_BASE_URL environment variable not set.")

  query <- list(
    service      = "kisters",
    type         = "queryServices",
    request      = "getTimeseriesValues",
    ts_id        = gauge_id,
    period       = sprintf("from(%sT00:00:00Z)to(%sT23:59:59Z)",
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

  raw <- data.table::as.data.table(body[[1L]]$data)
  data.table::setnames(raw, c("datetime_str", "value", "supplier_flag"))
  raw[, datetime_str := as.POSIXct(datetime_str,
                                   format = "%Y-%m-%d %H:%M:%S",
                                   tz     = "UTC")]
  raw[, value        := suppressWarnings(as.numeric(value))]
  raw <- raw[!is.na(datetime_str) & !is.na(value)]

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
    supplier_flag_col = "supplier_flag"
  )
  # nocov end
}


# -- Router -------------------------------------------------------------------

#' Route a single gauge fetch to the correct source system
#'
#' Reads the `source_system` field of a one-row registry `data.table` and
#' dispatches to [fetch_from_hde()], [fetch_from_wiski()],
#' or [fetch_from_wiski_all()]. Fetch errors are
#' caught and returned as `NULL` with a warning so the caller can log and
#' continue.
#'
#' To add a new source system: write a `fetch_from_<n>()` function returning
#' the Bronze schema, add a case to the `switch()` below, update
#' `VALID_SOURCES` in `package.R`, and re-run [build_gauge_registry()].
#'
#' @param gauge_row A one-row `data.table` from the gauge registry. Must
#'   contain columns `gauge_id`, `source_system`, and `data_type`.
#' @param start_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param end_date Character. End date in `"YYYY-MM-DD"` format.
#'
#' @return A `data.table` conforming to the Bronze schema, or `NULL` if the
#'   fetch failed.
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
#'                   end_date   = "2020-12-31")
#' }
route_gauge <- function(gauge_row, start_date, end_date) {

  tryCatch({
    switch(
      gauge_row$source_system,
      "HDE"       = fetch_from_hde(
                      gauge_row$gauge_id, gauge_row$data_type,
                      start_date, end_date),
      "WISKI"     = fetch_from_wiski(
                      gauge_row$gauge_id, gauge_row$data_type,
                      start_date, end_date),
      "WISKI_ALL" = fetch_from_wiski_all(
                      gauge_row$gauge_id, gauge_row$data_type,
                      start_date, end_date),
      stop("No client for source_system: ", gauge_row$source_system)
    )
  }, error = function(e) {
    warning(sprintf("Route failed for %s: %s",
                    gauge_row$gauge_id, e$message))
    NULL
  })
}
