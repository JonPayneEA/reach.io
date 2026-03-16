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

    message(sprintf("  [%s] %s", format(Sys.time(), "%H:%M:%S"), status))

    if (status == "Completed") return(info$url)
    if (status == "Failed")    stop("Batch failed: ",  status_url)
    if (Sys.time() > deadline) stop("Timed out: ",     status_url)

    Sys.sleep(poll_interval_s)
  }
}


#' Download one measure via the batch path
#'
#' Internal worker called by [download_hydrology()] for each measure when
#' `method = "batch"`. Submits the job, polls for completion, then either
#' streams the CSV to disk or reads it into a `data.table` in memory via
#' a temporary file. `data.table::fread()` is used for reading because it
#' is substantially faster than `read.csv()` for large files and returns a
#' `data.table` directly with correct column type inference.
#'
#' @param mid Character. Measure notation.
#' @param from_date Character. Start date in `"YYYY-MM-DD"` format.
#' @param to_date Character. End date in `"YYYY-MM-DD"` format.
#' @param output Character. `"memory"` or `"disk"`.
#' @param out_dir Character or `NULL`. Root output directory (disk only).
#' @param param Character. Parameter type.
#' @param max_wait_s Numeric. Max seconds to wait for the job. Default 600.
#'
#' @return A named list: `measure`, `data` (`data.table` or `NULL` in disk
#'   mode), `file` (`NA` in memory mode), `rows`, `ok`.
#'
#' @noRd
run_batch <- function(mid, from_date, to_date, output, out_dir, param,
                      max_wait_s = 600) {

  tryCatch({

    batch      <- submit_batch(mid, from_date, to_date)
    status_url <- batch$status_url

    # If the API returned a cached result the redirect lands directly on the
    # S3 CSV URL — no polling needed in that case
    if (grepl("\\.csv", status_url, ignore.case = TRUE)) {
      dl_url <- status_url
      message("  Cached — skipping poll")
    } else {
      dl_url <- poll_batch(status_url, max_wait_s = max_wait_s)
    }

    if (output == "disk") {

      # Stream directly to file — avoids loading the whole CSV into memory
      param_dir <- file.path(out_dir, param)
      dir.create(param_dir, showWarnings = FALSE, recursive = TRUE)
      safe_name <- gsub("[^A-Za-z0-9_-]", "_", mid)
      dest      <- file.path(param_dir, paste0(safe_name, ".csv"))
      httr::GET(dl_url, httr::write_disk(dest, overwrite = TRUE))
      message("  Saved -> ", dest)

      Sys.sleep(1)
      list(measure = mid, data = NULL, file = dest,
           rows = NA_integer_, ok = TRUE)

    } else {

      # Memory mode: download to a temp file and read back with fread().
      # fread() is much faster than read.csv() and returns a data.table
      # directly with correct column type inference
      tmp <- tempfile(fileext = ".csv")
      httr::GET(dl_url, httr::write_disk(tmp, overwrite = TRUE))
      dt  <- data.table::fread(tmp)

      # Parse date columns — fread reads them as character
      if ("date"     %in% names(dt)) dt[, date     := as.Date(date)]
      if ("dateTime" %in% names(dt)) dt[, dateTime := as.POSIXct(dateTime,
                                                        tz = "UTC")]
      dt[, measure_notation := mid]

      Sys.sleep(1)
      list(measure = mid, data = dt, file = NA_character_,
           rows = nrow(dt), ok = TRUE)
    }

  }, error = function(e) {
    message("  ERROR: ", conditionMessage(e))
    list(measure = mid, data = NULL, file = NA_character_,
         rows = NA_integer_, ok = FALSE)
  })
}
