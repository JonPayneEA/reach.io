# ============================================================
# Tool:        Synchronous Download Helpers
# Description: Internal helpers for the EA Hydrology sync
#              API: fetch readings for a single measure,
#              split date ranges into annual chunks, and
#              orchestrate chunked downloads.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      Measure notation, date range, chunk list
# Outputs:     data.table of readings or CSV file
# Dependencies: httr, data.table
# ============================================================

# ── Synchronous download helpers ─────────────────────────────────────────────

#' Fetch readings for a single measure (synchronous API)
#'
#' Calls `/hydrology/id/measures/{measure}/readings` and returns the result
#' as a `data.table`. The default API cap of 100,000 rows is overridden to
#' the hard maximum of 2,000,000. For larger requests use
#' [download_hydrology()] with `method = "batch"`.
#'
#' @param measure_notation Character. Unique measure ID — the `notation`
#'   column from [get_measures()], e.g.
#'   `"abc123-flow-m-86400-m3s-qualified"`.
#' @param from_date Character. Start date (inclusive) in `"YYYY-MM-DD"` format.
#' @param to_date Character. End date (exclusive) in `"YYYY-MM-DD"` format.
#' @param limit Integer. Maximum rows to return. Defaults to `API_HARD_LIMIT`
#'   (2,000,000). Reduce for quick spot checks.
#' @param observation_type Character or `NULL`. `"Qualified"` for
#'   quality-controlled data, `"Measured"` for raw. `NULL` returns both.
#'
#' @return A `data.table` of readings with columns `dateTime`, `date`,
#'   `value`, `quality`, `completeness`, and `measure_notation`. Returns
#'   an empty `data.table` if no readings exist for the period.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' fetch_readings(
#'   "abc123-flow-m-86400-m3s-qualified",
#'   from_date = "2022-01-01",
#'   to_date   = "2022-12-31"
#' )
#' }
fetch_readings <- function(measure_notation,
                           from_date,
                           to_date,
                           limit            = API_HARD_LIMIT,
                           observation_type = NULL) {

  url <- sprintf("%s/id/measures/%s/readings.json",
                 BASE_URL, measure_notation)

  query <- list(
    `mineq-date` = from_date,  # >= from_date
    `max-date`   = to_date,    # <  to_date
    `_limit`     = limit,      # override default 100k cap
    `_view`      = "full"      # include quality flags and completeness columns
  )

  if (!is.null(observation_type)) query$observationType <- observation_type

  resp  <- httr::GET(url, query = query)
  httr::stop_for_status(resp)
  items <- httr::content(resp, as = "parsed", simplifyVector = TRUE)$items

  if (length(items) == 0) return(data.table::data.table())

  dt <- data.table::as.data.table(items)
  dt[, measure_notation := measure_notation]

  # Parse date columns to proper R types for correct downstream filtering
  # e.g. dt[date >= as.Date("2022-06-01")]
  if ("date" %in% names(dt)) dt[, date := as.Date(date)]

  if ("dateTime" %in% names(dt)) {
    ts_raw <- dt[["dateTime"]]
    if (is.character(ts_raw)) {
      # The EA API returns ISO 8601 datetimes, e.g. "2022-01-01T00:15:00" or
      # "2022-01-01T00:15:00Z". R's as.POSIXct() without a format argument
      # does not reliably parse the T-separator or Z suffix on all platforms
      # (particularly Windows). Strip the trailing Z / +HH:MM offset and use
      # an explicit format so all 15-min sub-daily times are preserved.
      ts_clean <- sub("(Z|[+-]\\d{2}:\\d{2})$", "", ts_raw)
      dt[, dateTime := as.POSIXct(ts_clean,
                                   format = "%Y-%m-%dT%H:%M:%S",
                                   tz     = "UTC")]
    } else if (inherits(ts_raw, "POSIXct")) {
      # httr / jsonlite may auto-parse dateTime to POSIXct using the local
      # system timezone. Force the tzone attribute to UTC without shifting
      # the underlying numeric value — EA API timestamps are always UTC.
      attr(ts_raw, "tzone") <- "UTC"
      data.table::set(dt, j = "dateTime", value = ts_raw)
    }
  }

  dt
}


#' Split a date range into annual chunks
#'
#' Breaks a wide date range into year-by-year intervals to keep individual
#' synchronous API calls well within the 2,000,000-row hard limit. Especially
#' important for 15-minute data where a single year can be ~35,000 rows per
#' station.
#'
#' @param from_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param to_date Character. End date in `"YYYY-MM-DD"` format.
#'
#' @return A `data.table` with columns `chunk_from` and `chunk_to`, one row
#'   per annual chunk. The final chunk's `chunk_to` is capped at `to_date`.
#'
#' @export
#'
#' @examples
#' make_date_chunks("2019-06-01", "2022-03-01")
#' #    chunk_from   chunk_to
#' # 1: 2019-06-01 2020-06-01
#' # 2: 2020-06-01 2021-06-01
#' # 3: 2021-06-01 2022-03-01
make_date_chunks <- function(from_date, to_date) {

  from   <- as.Date(from_date)
  to     <- as.Date(to_date)
  starts <- seq(from, to, by = "year")

  # Drop the final boundary if it coincides with to_date to avoid a
  # zero-length chunk at the end
  starts <- starts[starts < to]
  ends   <- c(starts[-1], to)

  data.table::data.table(chunk_from = as.character(starts),
                         chunk_to   = as.character(ends))
}


#' Download one measure via the synchronous path, chunked annually
#'
#' Internal worker called by [download_hydrology()] for each measure when
#' `method = "sync"`. Iterates over annual date chunks, collecting readings
#' into a single `data.table` (memory mode) or appending to a CSV (disk
#' mode). Chunks are accumulated as a list and combined once at the end with
#' `rbindlist()` to avoid repeated full-table copies inside the loop.
#'
#' @param mid Character. Measure notation.
#' @param chunks `data.table`. Output of [make_date_chunks()].
#' @param output Character. `"memory"` or `"disk"`.
#' @param out_dir Character or `NULL`. Root output directory (disk only).
#' @param param Character. Parameter type.
#' @param observation_type Character or `NULL`. Passed to [fetch_readings()].
#'
#' @return A named list: `measure`, `data` (`data.table` or `NULL` in disk
#'   mode), `file` (`NA` in memory mode), `rows`, `ok`.
#'
#' @noRd
run_sync <- function(mid, chunks, output, out_dir, param, observation_type) {

  chunk_list <- list()
  total_rows <- 0L
  dest       <- NA_character_
  ok         <- TRUE

  for (j in seq_len(nrow(chunks))) {

    chunk_from <- chunks$chunk_from[j]
    chunk_to   <- chunks$chunk_to[j]

    message(sprintf("    Chunk %d/%d: %s -> %s",
                    j, nrow(chunks), chunk_from, chunk_to))

    tryCatch({

      dt <- fetch_readings(mid, chunk_from, chunk_to,
                           observation_type = observation_type)

      if (nrow(dt) == 0) {
        message("    No data for this chunk — skipping.")
        return()
      }

      if (nrow(dt) >= API_HARD_LIMIT) {
        warning(sprintf(
          "Chunk %d/%d for '%s' hit the API row limit (%d). ",
          "Data may be truncated — consider method = 'batch'.",
          j, nrow(chunks), mid, API_HARD_LIMIT
        ))
      }

      if (output == "memory") {
        # Defer combining until all chunks are done to avoid repeated copies
        chunk_list[[j]] <- dt
      } else {
        # Append immediately; first chunk writes the header
        dest <- handle_output(dt, output, out_dir, param, mid,
                              append = (total_rows > 0L))
      }

      total_rows <- total_rows + nrow(dt)
      message(sprintf("    %d rows (running total: %d)", nrow(dt), total_rows))

      Sys.sleep(1)  # EA fair-use: one request at a time

    }, error = function(e) {
      message("    ERROR: ", conditionMessage(e))
      ok <<- FALSE
    })
  }

  # Combine all in-memory chunks in one shot — much faster than incremental
  # rbind inside the loop
  combined <- if (output == "memory" && length(chunk_list) > 0) {
    data.table::rbindlist(chunk_list, fill = TRUE)
  } else {
    NULL
  }

  list(measure = mid, data = combined, file = dest,
       rows = total_rows, ok = ok)
}
