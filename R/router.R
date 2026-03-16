# -- Source Router ------------------------------------------------------------
#
# Dispatches each gauge fetch to the correct source system based on the
# source_system column in the registry. Adding a new source only requires
# a registry update plus a new fetch_from_*() function and switch() case.

# -- HDE fetch ----------------------------------------------------------------

#' Fetch data from the EA Hydrology Data Explorer (HDE)
#'
#' Calls the EA Hydrology API readings endpoint for a single gauge and
#' returns readings as a `data.table`. Internally wraps [find_stations()]
#' to resolve the gauge notation and [get_measures()] to find the relevant
#' measure notation, then calls [fetch_readings()] for the date range.
#'
#' The date range is split into annual chunks via [make_date_chunks()] to
#' stay within the API row limit.
#'
#' @param gauge_id Character. Gauge identifier matching `gauge_id` in the
#'   registry. Expected to be a WISKI ID for HDE-sourced gauges.
#' @param data_type Character. Parameter type: `"flow"`, `"level"`, or
#'   `"rainfall"`.
#' @param start_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param end_date Character. End date in `"YYYY-MM-DD"` format.
#'
#' @return A `data.table` with columns `gauge_id`, `datetime` (POSIXct),
#'   `value` (numeric), `unit` (character), `flag` (integer).
#'   Returns an empty `data.table` if no data is found.
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
  
  # -- Resolve station to measure notation ------------------------------------
  # find_stations() returns one row per measure with station.notation,
  # parameter, value_type and period already parsed — no get_measures() call
  # needed.
  stn <- find_stations(wiski_ids = gauge_id)
  if (nrow(stn) == 0) {
    warning(sprintf("[HDE] No station found for gauge_id: %s", gauge_id))
    return(data.table::data.table())
  }
  
  measures_dt <- stn[parameter == data_type]
  if (nrow(measures_dt) == 0) {
    warning(sprintf("[HDE] No %s measures found for gauge_id: %s",
                    data_type, gauge_id))
    return(data.table::data.table())
  }
  
  # Use the first matching measure (typically only one for a given data_type
  # and the default value_type from PARAMETER_CONFIG)
  measure_notation <- measures_dt$station.notation[1L]
  
  # -- Fetch readings in annual chunks ----------------------------------------
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
    if (!is.null(dt) && nrow(dt) > 0) chunk_list[[j]] <- dt
    Sys.sleep(1)  # EA fair-use
  }
  
  chunk_list <- Filter(Negate(is.null), chunk_list)
  if (length(chunk_list) == 0) return(data.table::data.table())
  
  # -- Normalise to the standard pipeline schema ------------------------------
  dt <- data.table::rbindlist(chunk_list, fill = TRUE)
  dt <- dt[, .(
    gauge_id = gauge_id,
    datetime = if ("dateTime" %in% names(dt)) dateTime else
      as.POSIXct(as.character(date), tz = "UTC"),
    value    = value,
    unit     = NA_character_,  # unit not available from find_stations(); populate if needed via get_measures()
    flag     = if ("quality" %in% names(dt))
      as.integer(quality) else 1L
  )]
  
  dt
}


# -- WISKI fetch --------------------------------------------------------------

#' Fetch data from WISKI / KiWIS
#'
#' Calls the KiWIS REST API for a single gauge using the `getTimeseriesValues`
#' endpoint. WISKI uses a `ts_id`-based query structure with different
#' authentication from the EA HDE. The `gauge_id` is expected to be a WISKI
#' time series ID (ts_id) for WISKI-sourced gauges in the registry.
#'
#' The KiWIS base URL and authentication are read from environment variables
#' `WISKI_BASE_URL` and `WISKI_API_KEY` respectively. Set these before use:
#' ```
#' Sys.setenv(WISKI_BASE_URL = "https://your-wiski-instance/KiWIS/KiWIS")
#' Sys.setenv(WISKI_API_KEY  = "your-key")
#' ```
#'
#' @inheritParams fetch_from_hde
#'
#' @return A `data.table` with columns `gauge_id`, `datetime` (POSIXct),
#'   `value` (numeric), `unit` (character), `flag` (integer).
#'   Returns an empty `data.table` if no data is found.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' Sys.setenv(WISKI_BASE_URL = "https://wiski.example.com/KiWIS/KiWIS")
#' fetch_from_wiski("12345678", "level", "2020-01-01", "2020-12-31")
#' }
fetch_from_wiski <- function(gauge_id, data_type, start_date, end_date) {
  
  message(sprintf("  [WISKI] %s | %s | %s to %s",
                  gauge_id, data_type, start_date, end_date))
  
  # -- Read connection config from environment --------------------------------
  base_url <- Sys.getenv("WISKI_BASE_URL", unset = NA_character_)
  api_key  <- Sys.getenv("WISKI_API_KEY",  unset = NA_character_)
  
  if (is.na(base_url)) {
    stop("[WISKI] WISKI_BASE_URL environment variable not set.")
  }
  
  # -- Call KiWIS getTimeseriesValues endpoint --------------------------------
  # KiWIS returns JSON with a `data` array of [timestamp, value, quality]
  # triples. Period from/to use ISO 8601 format with time component.
  query <- list(
    service          = "kisters",
    type             = "queryServices",
    request          = "getTimeseriesValues",
    ts_id            = gauge_id,
    period           = sprintf("from(%sT00:00:00Z)to(%sT23:59:59Z)",
                               start_date, end_date),
    returnfields     = "Timestamp,Value,Quality Code",
    format           = "json",
    `dateformat`     = "yyyy-MM-dd HH:mm:ss"
  )
  
  if (!is.na(api_key)) query$authToken <- api_key
  
  resp <- httr::GET(base_url, query = query)
  httr::stop_for_status(resp)
  
  body <- httr::content(resp, as = "parsed", simplifyVector = TRUE)
  
  # KiWIS wraps the result in a list; the first element contains the data
  if (length(body) == 0 || length(body[[1L]]$data) == 0) {
    message(sprintf("  [WISKI] No data returned for ts_id: %s", gauge_id))
    return(data.table::data.table())
  }
  
  raw <- data.table::as.data.table(body[[1L]]$data)
  data.table::setnames(raw, c("datetime_str", "value", "flag"))
  
  # -- Normalise to pipeline schema -------------------------------------------
  dt <- raw[, .(
    gauge_id = gauge_id,
    datetime = as.POSIXct(datetime_str,
                          format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
    value    = suppressWarnings(as.numeric(value)),
    unit     = body[[1L]]$ts_unitname %||% NA_character_,
    flag     = suppressWarnings(as.integer(flag)) %||% 1L
  )]
  
  dt[!is.na(datetime) & !is.na(value)]
}


# -- Bulk file fetch ----------------------------------------------------------

#' Fetch data from a bulk file
#'
#' Locates a bulk export file for the gauge on local storage or a mounted
#' FTP path and delegates to [ingest_bulk_file()] to read and standardise
#' it. The bulk file directory root is read from the environment variable
#' `BULK_FILE_ROOT`. Within that root, files are expected at the path:
#' ```
#' <BULK_FILE_ROOT>/<data_type>/<gauge_id>.*
#' ```
#' The first matching file with a recognised extension (`.csv`, `.tsv`,
#' `.txt`) is used. The date range arguments are used to filter rows after
#' ingestion.
#'
#' Set the root path before use:
#' ```
#' Sys.setenv(BULK_FILE_ROOT = "/mnt/ftp/ea_bulk")
#' ```
#'
#' @inheritParams fetch_from_hde
#'
#' @return A `data.table` with columns `gauge_id`, `datetime` (POSIXct),
#'   `value` (numeric), `unit` (character), `flag` (integer).
#'   Returns an empty `data.table` if no file is found.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' Sys.setenv(BULK_FILE_ROOT = "/mnt/ftp/ea_bulk")
#' fetch_from_bulk_file("39001", "rainfall", "2000-01-01", "2020-12-31")
#' }
fetch_from_bulk_file <- function(gauge_id, data_type, start_date, end_date) {
  
  message(sprintf("  [BULK] %s | %s", gauge_id, data_type))
  
  # -- Locate file ------------------------------------------------------------
  bulk_root <- Sys.getenv("BULK_FILE_ROOT", unset = NA_character_)
  if (is.na(bulk_root)) {
    stop("[BULK_FILE] BULK_FILE_ROOT environment variable not set.")
  }
  
  search_dir <- file.path(bulk_root, data_type)
  candidates <- list.files(search_dir,
                           pattern  = sprintf("^%s\\.(csv|tsv|txt)$", gauge_id),
                           full.names = TRUE,
                           ignore.case = TRUE)
  
  if (length(candidates) == 0) {
    warning(sprintf("[BULK_FILE] No file found for gauge_id %s in %s",
                    gauge_id, search_dir))
    return(data.table::data.table())
  }
  
  file_path   <- candidates[1L]
  file_format <- tolower(tools::file_ext(file_path))
  if (file_format == "txt") file_format <- "csv"
  
  # -- Ingest via shared ingestor ---------------------------------------------
  tmp_out <- tempfile()
  dt <- ingest_bulk_file(file_path, gauge_id, tmp_out,
                         file_format = file_format)
  
  if (nrow(dt) == 0) return(data.table::data.table())
  
  # -- Filter to requested date range -----------------------------------------
  from_dt <- as.POSIXct(start_date, tz = "UTC")
  to_dt   <- as.POSIXct(end_date,   tz = "UTC")
  dt      <- dt[datetime >= from_dt & datetime <= to_dt]
  
  # -- Normalise to pipeline schema -------------------------------------------
  # ingest_bulk_file already produces gauge_id, datetime, value, flag;
  # add a unit column (bulk files rarely carry unit info so mark as unknown)
  if (!"unit" %in% names(dt)) dt[, unit := NA_character_]
  
  dt[, .(gauge_id, datetime, value, unit, flag)]
}


# -- Router -------------------------------------------------------------------

#' Route a single gauge fetch to the correct source system
#'
#' Reads the `source_system` field of a one-row registry `data.table` and
#' dispatches to [fetch_from_hde()], [fetch_from_wiski()], or
#' [fetch_from_bulk_file()]. Fetch errors are caught and returned as `NULL`
#' with a warning so the caller ([run_backfill()] or [run_incremental()])
#' can log the failure and continue.
#'
#' To add a new source system: write a `fetch_from_<n>()` function, add a
#' case to the `switch()` below, update `VALID_SOURCES` in `package.R`, and
#' re-run [build_gauge_registry()] on the updated gauge list CSV.
#'
#' @param gauge_row A one-row `data.table` from the gauge registry. Must
#'   contain columns `gauge_id`, `source_system`, and `data_type`.
#' @param start_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param end_date Character. End date in `"YYYY-MM-DD"` format.
#'
#' @return A `data.table` with columns `gauge_id`, `datetime`, `value`,
#'   `unit`, `flag`, or `NULL` if the fetch failed.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' registry_dt <- data.table::as.data.table(
#'   arrow::read_parquet(
#'     "data/fw_bronze/gauge_registry/gauge_registry.parquet"
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
      "BULK_FILE" = fetch_from_bulk_file(
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
