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
# Outputs:     data.table of readings or Bronze Parquet files
# Dependencies: httr, data.table
# ============================================================

# ── Synchronous download helpers ─────────────────────────────────────────────

#' Parse the date columns of an EA readings table in place
#'
#' Shared by the sync and batch paths so both produce identical types. `date`
#' becomes `Date`; `dateTime` becomes `POSIXct` in UTC. The EA API returns ISO
#' 8601 strings such as `"2022-01-01T00:15:00"` or `"2022-01-01T00:15:00Z"`;
#' `as.POSIXct()` without a format does not parse the `T` separator or `Z`
#' suffix reliably on all platforms (notably Windows), so the offset is
#' stripped and an explicit format used. EA timestamps are always UTC.
#'
#' @param dt A `data.table` of readings. Modified by reference.
#' @return `dt`, invisibly.
#' @noRd
.parse_reading_datetimes <- function(dt) {
  # Columns are optional: either may be absent, and an already-typed column is
  # left alone (fread returns IDate, which inherits "Date", so it passes the
  # inherits() test). Here `date` on the right-hand side is the column.
  if ("date" %in% names(dt) && !inherits(dt[["date"]], "Date"))
    dt[, date := as.Date(date)]

  if ("dateTime" %in% names(dt)) {
    ts_raw <- dt[["dateTime"]]
    if (is.character(ts_raw)) {
      # Drop the Z / +hh:mm offset WITHOUT converting: this is only correct
      # because EA timestamps are always UTC. A non-zero offset would be read
      # as UTC clock time. Strings that still do not fit the explicit format
      # become NA silently, so downstream NA-timestamp handling matters.
      # The second sub() accepts a space separator as well as "T".
      ts_clean <- sub("(Z|[+-]\\d{2}:\\d{2})$", "", ts_raw)
      ts_clean <- sub(" ", "T", ts_clean, fixed = TRUE)
      data.table::set(dt, j = "dateTime",
                      value = as.POSIXct(ts_clean,
                                         format = "%Y-%m-%dT%H:%M:%S",
                                         tz     = "UTC"))
    } else if (inherits(ts_raw, "POSIXct")) {
      # jsonlite / fread may auto-parse using the local timezone. Force the
      # tzone attribute to UTC without shifting the underlying numeric value
      # (relabels the zone, does not convert).
      attr(ts_raw, "tzone") <- "UTC"
      data.table::set(dt, j = "dateTime", value = ts_raw)
    }
  }
  # Modified by reference via set() / :=, so callers ignore the return value.
  invisible(dt)
}


#' Fetch readings for a single measure (synchronous API)
#'
#' Calls `/hydrology/id/measures/{measure}/readings` and returns the result
#' as a `data.table`. The default API cap of 100,000 rows is overridden to
#' the hard maximum of 2,000,000. For larger requests use
#' [download_hydrology()] with `method = "batch"`.
#'
#' @details
#' - **One request, no paging.** Exactly one HTTP request is made and its
#'   items are taken as they come. If the range holds more than `limit` rows
#'   the result is silently truncated; [make_date_chunks()] and
#'   [download_hydrology()] exist to keep each call below that.
#' - **No pacing or retries.** This function does not sleep or retry, so a
#'   caller looping over many measures or chunks must pause between calls to
#'   respect the EA fair-use request (one request at a time).
#' - **Errors.** An HTTP error status raises an error (via
#'   `httr::stop_for_status()`); an empty response is not an error and returns
#'   an empty `data.table`.
#' - **Date bounds.** `from_date` is sent as `mineq-date` (inclusive) and
#'   `to_date` as `max-date` (strictly before), so adjacent ranges that share
#'   a boundary neither overlap nor leave a gap.
#' - **Types.** `date` is returned as `Date` and `dateTime` as UTC `POSIXct`,
#'   identically to the batch path.
#' - Nothing is written to disk; for Bronze output use
#'   [download_hydrology()] with `output = "disk"`.
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
#'   to_date   = "2023-01-01"
#' )
#' }
fetch_readings <- function(measure_notation,
                           from_date,
                           to_date,
                           limit            = API_HARD_LIMIT,
                           observation_type = NULL) {

  url <- sprintf("%s/id/measures/%s/readings.json",
                 BASE_URL, measure_notation)

  # Date bounds are asymmetric by design: mineq-date is >=, max-date is
  # strict <. Keeping to_date exclusive package-wide means consecutive chunks
  # can share a boundary date with no duplicated or dropped readings.
  query <- list(
    `mineq-date` = from_date,  # >= from_date
    `max-date`   = to_date,    # <  to_date
    `_limit`     = limit,      # override default 100k cap
    `_view`      = "full"      # include quality flags and completeness columns
  )

  # Omitted (not sent as NULL) when unset, so the API returns both Qualified
  # and Measured observations.
  if (!is.null(observation_type)) query$observationType <- observation_type

  resp  <- httr::GET(url, query = query)
  # Raise on 4xx/5xx so an error page is never parsed as if it were readings.
  httr::stop_for_status(resp)
  items <- httr::content(resp, as = "parsed", simplifyVector = TRUE)$items

  # Zero-row result is a normal outcome (gap in the record, or a period before
  # the series began) and is distinguished from failure by not raising.
  if (length(items) == 0) return(data.table::data.table())

  dt <- data.table::as.data.table(items)
  # Tag every row with its measure so rows stay attributable once
  # download_hydrology() combines many measures. set() takes the function
  # argument itself, even if the response were to carry a column of the same
  # name (inside dt[...] that name would resolve to the column instead).
  data.table::set(dt, j = "measure_notation", value = measure_notation)

  # Parse date columns to proper R types for correct downstream filtering
  # e.g. dt[date >= as.Date("2022-06-01")]
  .parse_reading_datetimes(dt)
  dt
}


#' Split a date range into annual chunks
#'
#' Breaks a wide date range into year-by-year intervals to keep individual
#' synchronous API calls well within the 2,000,000-row hard limit. Especially
#' important for 15-minute data where a single year can be ~35,000 rows per
#' station.
#'
#' @details
#' Chunks are anchored to `from_date`, not to calendar years: each chunk
#' starts on the anniversary of `from_date`, so `"2019-06-01"` gives
#' June-to-June chunks. Each `chunk_to` equals the next `chunk_from`; because
#' the API treats the start as inclusive and the end as exclusive (see
#' [fetch_readings()]) the chunks tile the range with no overlap and no gap.
#' Bounds are returned as `"YYYY-MM-DD"` character strings ready to pass
#' straight to [fetch_readings()]. Pure date arithmetic: no network requests
#' and no side effects. [fetch_from_hde()] and [download_hydrology()] both use
#' it.
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
  # Each chunk ends where the next begins (exclusive end, inclusive start);
  # only the last end is capped at to_date rather than a full year on.
  ends   <- c(starts[-1], to)

  data.table::data.table(chunk_from = as.character(starts),
                         chunk_to   = as.character(ends))
}


#' Download one measure via the synchronous path, chunked annually
#'
#' Internal worker called by [download_hydrology()] for each measure when
#' `method = "sync"`. Iterates over annual date chunks, accumulating readings
#' as a list that is combined once at the end with `rbindlist()` (avoiding
#' repeated full-table copies). In memory mode the combined table is returned;
#' in disk mode it is written to the Bronze tier via `handle_output()`.
#'
#' @param mid Character. Measure notation.
#' @param chunks `data.table`. Output of [make_date_chunks()].
#' @param output Character. `"memory"` or `"disk"`.
#' @param out_dir Character or `NULL`. Root output directory (disk only).
#' @param param Character. Parameter type.
#' @param observation_type Character or `NULL`. Passed to [fetch_readings()].
#' @param site_id Character. Supplier site identifier used in the Bronze
#'   dataset ID (disk mode).
#' @param period Character. Temporal resolution stored in the Bronze `period`
#'   column (disk mode).
#' @param category Character. Bronze data category (disk mode).
#' @param register_path Character or `NULL`. Provenance register CSV (disk
#'   mode).
#' @param trace Character or `NULL`. Series discriminator for the Bronze
#'   `dataset_id` (disk mode), used when a station has several measures of one
#'   data type.
#'
#' @return A named list: `measure`, `data` (`data.table` or `NULL` in disk
#'   mode), `file` (`NA` in memory mode; Bronze paths joined by `"; "` in disk
#'   mode), `rows`, `ok`.
#'
#' @noRd
run_sync <- function(mid, chunks, output, out_dir, param, observation_type,
                     site_id = NA_character_, period = NA_character_,
                     category = "hydrometric", register_path = NULL,
                     trace = NULL) {

  # `ok` starts TRUE and is only ever flipped to FALSE: any failed chunk marks
  # the whole measure as not ok, even though chunks that succeeded are kept.
  chunk_list <- list()
  total_rows <- 0L
  dest       <- NA_character_
  ok         <- TRUE

  # -- Fetch annual chunks ----------------------------------------------------
  # Chunks are fetched strictly one after another (EA fair use).
  for (j in seq_len(nrow(chunks))) {

    chunk_from <- chunks$chunk_from[j]
    chunk_to   <- chunks$chunk_to[j]

    message(sprintf("    Chunk %d/%d: %s -> %s",
                    j, nrow(chunks), chunk_from, chunk_to))

    # Per-chunk isolation: a failure (HTTP error, parse error) is logged and
    # flagged via ok, and the loop moves on to the next chunk.
    tryCatch({

      dt <- fetch_readings(mid, chunk_from, chunk_to,
                           observation_type = observation_type)

      if (nrow(dt) == 0) {
        message("    No data for this chunk - skipping.")
      } else {
        # fetch_readings() makes a single unpaged request, so reaching the cap
        # means the API cut the response off. Data is kept but flagged; ok is
        # not set to FALSE.
        if (nrow(dt) >= API_HARD_LIMIT) {
          warning(sprintf(
            paste0("Chunk %d/%d for '%s' hit the API row limit (%d). ",
                   "Data may be truncated - consider method = 'batch'."),
            j, nrow(chunks), mid, API_HARD_LIMIT
          ))
        }

        # Append to a list (cheap); combined once after the loop.
        chunk_list[[length(chunk_list) + 1L]] <- dt
        total_rows <- total_rows + nrow(dt)
        message(sprintf("    %d rows (running total: %d)", nrow(dt), total_rows))
      }

    }, error = function(e) {
      message("    ERROR: ", conditionMessage(e))
      # <<- is needed: the handler runs in its own function scope.
      ok <<- FALSE
    })

    # EA fair use: one request at a time. The pause is outside the tryCatch so
    # EVERY chunk is paced - successful, empty and failed alike.
    .pace()
  }

  # -- Combine and write ------------------------------------------------------
  # Combine all chunks in one shot — much faster than incremental rbind
  combined <- if (length(chunk_list) > 0L) {
    data.table::rbindlist(chunk_list, fill = TRUE)
  } else {
    NULL
  }

  # Disk mode writes once per measure, after all chunks, so the Bronze writer
  # can split the full series by observation year (a chunk boundary need not
  # match a calendar year). Nothing is written, and no register rows are added,
  # when no chunk returned data. A write error is caught like a failed chunk
  # (message, ok = FALSE) so one measure that cannot be written does not abort
  # a long download of many measures; run_batch() behaves the same way.
  if (output == "disk" && !is.null(combined) && nrow(combined) > 0L) {
    tryCatch({
      paths <- handle_output(combined, "disk", out_dir, param, mid,
                             site_id = site_id, period = period,
                             category = category, register_path = register_path,
                             method_of_receipt = "API pull (sync)",
                             trace = trace)
      # One summary row per measure: collapse the per-year paths into one string.
      # Plain `<-`: the tryCatch() expression runs in this function's frame
      # (only the handler below needs `<<-`).
      dest <- paste(paths, collapse = "; ")
    }, error = function(e) {
      message("    ERROR writing Bronze: ", conditionMessage(e))
      ok <<- FALSE
    })
  }

  # `rows` is the number of rows fetched across chunks, not the number written
  # (the Bronze writer drops rows with a missing timestamp). `file` stays NA
  # in memory mode and when nothing was written.
  list(measure = mid,
       data    = if (output == "memory") combined else NULL,
       file    = dest,
       rows    = total_rows,
       ok      = ok)
}
