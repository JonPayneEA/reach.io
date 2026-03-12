# -- Source Router ------------------------------------------------------------
#
# Reads the gauge registry and dispatches each gauge to the correct fetch
# function based on its source_system. Adding a new source only requires a
# registry update, not a code change here -- just add a new case to the
# switch() in route_gauge() and a corresponding fetch_from_* function.

# -- Per-source fetch functions -----------------------------------------------
# These are stubs. Replace each body with a real API client call.
# Every function must return a data.table with columns:
#   gauge_id, datetime (POSIXct), value (numeric), unit (character),
#   flag (integer)

#' Fetch data from the EA Hydrology Data Explorer (HDE)
#'
#' Stub implementation. Replace with a real call to the EA HDE API. Must
#' return a `data.table` with columns `gauge_id`, `datetime`, `value`,
#' `unit`, and `flag`.
#'
#' @param gauge_id Character. Gauge identifier.
#' @param data_type Character. Type of data to fetch, e.g. `"flow"`.
#' @param start_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param end_date Character. End date in `"YYYY-MM-DD"` format.
#'
#' @return A `data.table` with columns `gauge_id`, `datetime`, `value`,
#'   `unit`, `flag`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' fetch_from_hde("39001", "flow", "2020-01-01", "2020-12-31")
#' }
fetch_from_hde <- function(gauge_id, data_type, start_date, end_date) {

  message(sprintf("  [HDE] Fetching %s | %s | %s to %s",
                  gauge_id, data_type, start_date, end_date))

  # Replace with a real HDE API call. The EA Hydrology API (reach.io's
  # download_hydrology()) can serve this role for rainfall, flow, and level.
  data.table::data.table(
    gauge_id = gauge_id,
    datetime = as.POSIXct(start_date, tz = "UTC"),
    value    = runif(1),
    unit     = "m3/s",
    flag     = 1L
  )
}


#' Fetch data from WISKI / KiWIS
#'
#' Stub implementation. Replace with a real call to the WISKI/KiWIS API,
#' which uses different authentication and a different response structure
#' from the EA HDE. Must return a `data.table` with columns `gauge_id`,
#' `datetime`, `value`, `unit`, and `flag`.
#'
#' @inheritParams fetch_from_hde
#'
#' @return A `data.table` with columns `gauge_id`, `datetime`, `value`,
#'   `unit`, `flag`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' fetch_from_wiski("SS92F014", "level", "2020-01-01", "2020-12-31")
#' }
fetch_from_wiski <- function(gauge_id, data_type, start_date, end_date) {

  message(sprintf("  [WISKI] Fetching %s | %s | %s to %s",
                  gauge_id, data_type, start_date, end_date))

  # Replace with a real KiWIS API call. WISKI uses its own REST endpoint
  # with a ts_id-based query structure distinct from the EA Hydrology API.
  data.table::data.table(
    gauge_id = gauge_id,
    datetime = as.POSIXct(start_date, tz = "UTC"),
    value    = runif(1),
    unit     = "m",
    flag     = 1L
  )
}


#' Fetch data from a bulk file via the Bulk File Ingestor
#'
#' Stub implementation. Delegates to [ingest_bulk_file()] for the actual
#' read. In practice this function would locate the correct file for the
#' gauge on an FTP drop or shared storage path. Must return a `data.table`
#' with columns `gauge_id`, `datetime`, `value`, `unit`, and `flag`.
#'
#' @inheritParams fetch_from_hde
#'
#' @return A `data.table` with columns `gauge_id`, `datetime`, `value`,
#'   `unit`, `flag`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' fetch_from_bulk_file("39001", "rainfall", "2000-01-01", "2020-12-31")
#' }
fetch_from_bulk_file <- function(gauge_id, data_type, start_date, end_date) {

  message(sprintf("  [BULK] Fetching %s | %s", gauge_id, data_type))

  # Replace with a call to ingest_bulk_file() pointed at the correct file
  # path for this gauge on FTP or shared storage.
  data.table::data.table(
    gauge_id = gauge_id,
    datetime = as.POSIXct(start_date, tz = "UTC"),
    value    = runif(1),
    unit     = "mm",
    flag     = 1L
  )
}


# -- Router -------------------------------------------------------------------

#' Route a single gauge fetch to the correct source system
#'
#' Reads the `source_system` field of a one-row registry `data.table` and
#' dispatches to the appropriate `fetch_from_*` function. Any fetch errors
#' are caught and returned as `NULL` with a warning so the caller (typically
#' [run_backfill()]) can log the failure and continue with remaining gauges.
#'
#' To add a new source system: add a `fetch_from_<name>()` function and a
#' corresponding case to the `switch()` below, then update `VALID_SOURCES`
#' in `package.R` and re-run [build_gauge_registry()] on the updated CSV.
#'
#' @param gauge_row A one-row `data.table` from the gauge registry, as
#'   returned by [build_gauge_registry()]. Must contain columns `gauge_id`,
#'   `source_system`, and `data_type`.
#' @param start_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param end_date Character. End date in `"YYYY-MM-DD"` format.
#'
#' @return A `data.table` of readings (columns `gauge_id`, `datetime`,
#'   `value`, `unit`, `flag`), or `NULL` if the fetch failed.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' registry_dt <- as.data.table(
#'   arrow::read_parquet(
#'     "data/fw_bronze/gauge_registry/gauge_registry.parquet"
#'   )
#' )
#' data_dt <- route_gauge(registry_dt[1],
#'                        start_date = "2020-01-01",
#'                        end_date   = "2020-12-31")
#' }
route_gauge <- function(gauge_row, start_date, end_date) {

  result <- tryCatch({
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
      # Default branch — should never be reached if the registry was built
      # with build_gauge_registry(), which validates source_system values
      stop("No client for source_system: ", gauge_row$source_system)
    )
  }, error = function(e) {
    warning(sprintf("Route failed for %s: %s",
                    gauge_row$gauge_id, e$message))
    NULL
  })

  result
}
