# ============================================================
# Tool:        Batch Download Helpers
# Description: Internal helpers for the EA Hydrology batch
#              API: submit jobs, poll for completion, and
#              download results.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      Measure notation, date range
# Outputs:     data.table of readings or Bronze Parquet files
# Dependencies: httr, data.table
# ============================================================

# ── Batch download helpers ────────────────────────────────────────────────────

#' Submit a single request to the EA Hydrology batch API
#'
#' Sends a GET to the batch endpoint for a specific measure and date range.
#' The API either redirects to a cached CSV (if the same query was run
#' before) or to a status page for a newly queued job. `httr` follows the
#' redirect automatically, so `resp$url` reflects the final destination.
#'
#' @param measure_notation Character. Unique measure ID — the `notation`
#'   column from [get_measures()].
#' @param from_date Character. Start date (inclusive) in `"YYYY-MM-DD"` format.
#' @param to_date Character. End date (exclusive) in `"YYYY-MM-DD"` format.
#'
#' @return A named list with `status_url` (the URL to poll or download from)
#'   and `measure` (the notation passed in, carried forward for tracking).
#'
#' @noRd
submit_batch <- function(measure_notation, from_date, to_date) {

  resp <- httr::GET(
    paste0(BASE_URL, "/data/batch-readings/batch"),
    query = list(
      measure      = measure_notation,
      `mineq-date` = from_date,  # >= from_date
      `max-date`   = to_date     # <  to_date
    )
  )

  httr::stop_for_status(resp)
  list(status_url = resp$url, measure = measure_notation)
}


#' Poll a batch status URL until the job completes or times out
#'
#' The EA batch API processes requests asynchronously. This function
#' repeatedly checks the status endpoint, sleeping between polls, until the
#' job reports `"Completed"` or an error or timeout occurs.
#'
#' Possible API status values: `"Pending"`, `"InProgress"`, `"Completed"`,
#' `"Failed"`.
#'
#' @param status_url Character. The status page URL from [reach.io::submit_batch()].
#' @param max_wait_s Numeric. Maximum seconds to wait before giving up.
#'   Default 600. Increase for large date ranges or busy queues.
#' @param poll_interval_s Numeric. Seconds between status checks. Default 15.
#'
#' @return Character. The S3 download URL for the completed CSV.
#'
#' @noRd
poll_batch <- function(status_url, max_wait_s = 600, poll_interval_s = 15) {

  deadline <- Sys.time() + max_wait_s

  repeat {
    resp <- httr::GET(paste0(status_url, ".json"))
    httr::stop_for_status(resp)

    info   <- httr::content(resp, as = "parsed", simplifyVector = TRUE)
    status <- info$status

    if (length(status) != 1L || is.na(status))
      stop("Batch status response had no usable 'status' field: ", status_url)

    message(sprintf("  [%s] %s", format(Sys.time(), "%H:%M:%S"), status))

    if (status == "Completed") {
      if (length(info$url) != 1L || is.na(info$url))
        stop("Batch completed but returned no download URL: ", status_url)
      return(info$url)
    }
    if (status == "Failed")    stop("Batch failed: ",  status_url)
    if (Sys.time() > deadline) stop("Timed out: ",     status_url)

    Sys.sleep(poll_interval_s)
  }
}


#' Download one measure via the batch path
#'
#' Internal worker called by [download_hydrology()] for each measure when
#' `method = "batch"`. Submits the job, polls for completion, downloads the
#' CSV to a temporary file (HTTP errors are raised, never written out as data)
#' and reads it with `data.table::fread()`. Date columns are parsed by the same
#' helper as the sync path so both methods return identical types. In disk
#' mode the readings are then written to the Bronze tier via
#' [handle_output()]; note that the whole CSV is held in memory while it is
#' converted, so very large batch results need correspondingly large RAM.
#'
#' @param mid Character. Measure notation.
#' @param from_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param to_date Character. End date (exclusive) in `"YYYY-MM-DD"` format.
#' @param output Character. `"memory"` or `"disk"`.
#' @param out_dir Character or `NULL`. Root store directory (disk only).
#' @param param Character. Parameter type.
#' @param max_wait_s Numeric. Max seconds to wait for the job. Default 600.
#' @param site_id Character. Supplier site identifier used in the Bronze
#'   dataset ID (disk mode).
#' @param period Character. Temporal resolution stored in the Bronze `period`
#'   column (disk mode).
#' @param category Character. Bronze data category (disk mode).
#' @param register_path Character or `NULL`. Provenance register CSV (disk
#'   mode).
#'
#' @return A named list: `measure`, `data` (`data.table` in memory mode,
#'   `NULL` in disk mode), `file` (`NA` in memory mode; Bronze paths joined by
#'   `"; "` in disk mode), `rows`, `ok`.
#'
#' @noRd
run_batch <- function(mid, from_date, to_date, output, out_dir, param,
                      max_wait_s = 600, site_id = NA_character_,
                      period = NA_character_, category = "hydrometric",
                      register_path = NULL) {

  tryCatch({

    batch      <- submit_batch(mid, from_date, to_date)
    status_url <- batch$status_url

    # If the API returned a cached result the redirect lands directly on the
    # S3 CSV URL — no polling needed in that case
    if (grepl("\\.csv", status_url, ignore.case = TRUE)) {
      dl_url <- status_url
      message("  Cached - skipping poll")
    } else {
      dl_url <- poll_batch(status_url, max_wait_s = max_wait_s)
    }

    tmp <- tempfile(fileext = ".csv")
    on.exit(unlink(tmp), add = TRUE)
    resp <- httr::GET(dl_url, httr::write_disk(tmp, overwrite = TRUE))
    httr::stop_for_status(resp)

    dt <- data.table::fread(tmp)
    dt[, measure_notation := mid]
    .parse_reading_datetimes(dt)

    Sys.sleep(1)  # EA fair-use: one request at a time

    if (output == "disk") {
      if (nrow(dt) == 0L) {
        message("  No data returned - nothing written.")
        return(list(measure = mid, data = NULL, file = NA_character_,
                    rows = 0L, ok = TRUE))
      }
      paths <- handle_output(dt, "disk", out_dir, param, mid,
                             site_id = site_id, period = period,
                             category = category, register_path = register_path,
                             method_of_receipt = "API pull (batch)")
      message("  Saved -> ", paste(paths, collapse = "; "))
      list(measure = mid, data = NULL, file = paste(paths, collapse = "; "),
           rows = nrow(dt), ok = TRUE)
    } else {
      list(measure = mid, data = dt, file = NA_character_,
           rows = nrow(dt), ok = TRUE)
    }

  }, error = function(e) {
    message("  ERROR: ", conditionMessage(e))
    list(measure = mid, data = NULL, file = NA_character_,
         rows = NA_integer_, ok = FALSE)
  })
}
