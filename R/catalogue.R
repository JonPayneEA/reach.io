# ============================================================
# Tool:        Bronze Store Catalogue
# Description: Programmatic catalogue of what data exists in
#              the Bronze store. Answers three questions:
#
#                1. list_available_gauges() — what sites are
#                   held and for which data types / suppliers?
#
#                2. summarise_coverage() — what date range is
#                   held per gauge, how many records, how
#                   complete is the series?
#
#                3. check_gaps() — exactly which timesteps are
#                   missing for a given gauge and data type?
#
#              Primary data source: the governance register CSV.
#              Fallback (check_gaps only): reads Parquet files
#              directly where a fine-grained gap list is needed.
#
# Flode Module: flode.io
# Author:      JP
# Created:     2026-03-20
# Tier:        1 (Bronze / Read-only)
# Dependencies: data.table, arrow
# ============================================================


# =============================================================================
# Internal helpers
# =============================================================================

# Parse the data category from a Bronze file path.
# Path pattern: <root>/bronze/<category>/...
#' @noRd
.category_from_path <- function(paths, root) {
  bronze_prefix <- paste0(normalizePath(file.path(root, "bronze"),
                                        mustWork = FALSE), .Platform$file.sep)
  rel <- sub(paste0("^", gsub("([.+?^${}()|\\[\\]\\\\])", "\\\\\\1",
                               bronze_prefix)),
             "", normalizePath(paths, mustWork = FALSE))
  first <- vapply(strsplit(rel, "/|\\\\"), function(x) {
    if (length(x)) x[[1L]] else NA_character_
  }, character(1L))
  first
}

# Find all Bronze Parquet files for one site_id + data_type combination.
# Scans the filesystem rather than relying on the register so that files
# added outside the normal ingest pipeline are still found.
#' @noRd
.find_site_parquets <- function(root, site_id, data_type,
                                category = "hydrometric") {
  search_root <- file.path(root, "bronze", category)
  if (!dir.exists(search_root)) return(character(0))

  # Files are stored at bronze/<cat>/<supplier>/<data_type>/<year>/<id>.parquet
  # Glob by data_type subdir then filter filenames by site_id fragment.
  candidates <- list.files(search_root, pattern = "\\.parquet$",
                           recursive = TRUE, full.names = TRUE)
  # Keep only files under the correct data_type subdirectory
  dt_pat   <- paste0("/", data_type, "/")
  site_pat <- paste0("_", site_id, "_")
  candidates[grepl(dt_pat, candidates, fixed = TRUE) &
               grepl(site_pat, candidates, fixed = TRUE)]
}

# Infer the expected observation interval in seconds from a character label.
#' @noRd
.interval_seconds <- function(label) {
  label <- tolower(trimws(label))
  switch(label,
    "15min"  = 15L  * 60L,
    "hourly" = 60L  * 60L,
    "daily"  = 24L  * 60L * 60L,
    NA_integer_
  )
}


# =============================================================================
# list_available_gauges()
# =============================================================================

#' List all gauges available in the Bronze store
#'
#' Reads the governance register and returns a catalogue of every gauge (unique
#' `site_id` + `data_type` combination) for which Bronze data has been
#' ingested.  Results can be filtered to a specific category, supplier, or
#' data type.
#'
#' The register is the primary source of truth. If it does not yet exist, a
#' message is printed and an empty table is returned.
#'
#' @param root Character. Root of the data store (the directory containing
#'   `bronze/`, `register/`, etc.).
#' @param category Character vector or `NULL`. Filter to one or more data
#'   categories, e.g. `"hydrometric"`, `"radarH19"`, `"MOSES"`. `NULL`
#'   (default) returns all categories.
#' @param supplier Character vector or `NULL`. Filter to one or more supplier
#'   codes, e.g. `"EA"`, `"WISKI"`. `NULL` (default) returns all suppliers.
#' @param data_type Character vector or `NULL`. Filter to one or more
#'   framework data type codes, e.g. `"Q"`, `"H"`, `"P"`. `NULL` (default)
#'   returns all types.
#'
#' @return A `data.table` with one row per unique site + data-type
#'   combination, printed as a formatted catalogue. Returned invisibly.
#'   Columns: `site_id`, `category`, `supplier_code`, `data_type`,
#'   `temporal_resolution`, `start`, `end`, `n_datasets`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # All gauges
#' list_available_gauges("data/hydro")
#'
#' # Hydrometric flow gauges from the EA only
#' list_available_gauges("data/hydro",
#'                       category  = "hydrometric",
#'                       supplier  = "EA",
#'                       data_type = "Q")
#' }
list_available_gauges <- function(root, category = NULL, supplier = NULL,
                                  data_type = NULL) {
  reg_path <- file.path(root, "register", "hydrometric_data_register.csv")

  if (!file.exists(reg_path)) {
    message("No governance register found at: ", reg_path,
            "\nRun setup_hydro_store() and ingest data first.")
    return(invisible(data.table::data.table()))
  }

  reg <- data.table::fread(reg_path, showProgress = FALSE)
  # Keep Bronze rows only (register may include Silver/Gold in future)
  if ("tier" %in% names(reg)) reg <- reg[reg$tier == "Bronze"]

  if (nrow(reg) == 0L) {
    message("Governance register exists but contains no Bronze records.")
    return(invisible(reg))
  }

  # Derive category from file_path if the column is present
  if ("file_path" %in% names(reg) && !"category" %in% names(reg)) {
    reg[, category := .category_from_path(file_path, root)]
  }

  # Apply caller filters
  if (!is.null(category)  && "category"       %in% names(reg))
    reg <- reg[reg$category       %in% category ]
  if (!is.null(supplier)  && "supplier_code"   %in% names(reg))
    reg <- reg[reg$supplier_code  %in% supplier ]
  if (!is.null(data_type) && "data_type"       %in% names(reg))
    reg <- reg[reg$data_type      %in% data_type]

  if (nrow(reg) == 0L) {
    message("No Bronze records match the specified filters.")
    return(invisible(data.table::data.table()))
  }

  # Summarise: one row per site + data_type
  by_cols <- intersect(
    c("site_id", "category", "supplier_code", "data_type",
      "temporal_resolution"),
    names(reg)
  )
  date_cols <- intersect(c("time_period_start", "time_period_end"), names(reg))

  out <- reg[, {
    start_vals <- if ("time_period_start" %in% names(reg)) time_period_start else NA_character_
    end_vals   <- if ("time_period_end"   %in% names(reg)) time_period_end   else NA_character_
    list(
      start      = min(start_vals, na.rm = TRUE),
      end        = max(end_vals,   na.rm = TRUE),
      n_datasets = .N
    )
  }, by = by_cols]

  data.table::setorder(out, category, data_type, site_id)

  n_sites <- nrow(out)
  n_types <- length(unique(out$data_type))
  cat(sprintf("<Bronze Catalogue>  %d gauge(s) across %d data type(s)\n\n",
              n_sites, n_types))
  print(out)
  invisible(out)
}


# =============================================================================
# summarise_coverage()
# =============================================================================

#' Summarise date coverage for Bronze store gauges
#'
#' Reports the time span, record count, and completeness percentage for each
#' gauge in the Bronze store.  Completeness is estimated from the register
#' (`time_period_start` / `time_period_end` / `temporal_resolution`) without
#' reading the underlying Parquet files, so it reflects the *intended* coverage
#' per ingest batch rather than the literal row count.
#'
#' To inspect actual record counts or detect individual missing timesteps, use
#' [check_gaps()].
#'
#' @param root Character. Root of the data store.
#' @param site_id Character vector or `NULL`. Filter to one or more sites.
#'   `NULL` (default) shows all sites.
#' @param category Character vector or `NULL`. Filter to one or more
#'   categories. `NULL` shows all.
#' @param data_type Character vector or `NULL`. Filter to one or more data
#'   type codes. `NULL` shows all.
#'
#' @return A `data.table` with one row per site + data-type, printed as a
#'   coverage summary. Returned invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Coverage for all gauges
#' summarise_coverage("data/hydro")
#'
#' # Flow coverage for a specific site
#' summarise_coverage("data/hydro", site_id = "39001", data_type = "Q")
#' }
summarise_coverage <- function(root, site_id = NULL, category = NULL,
                               data_type = NULL) {
  reg_path <- file.path(root, "register", "hydrometric_data_register.csv")

  if (!file.exists(reg_path)) {
    message("No governance register found at: ", reg_path)
    return(invisible(data.table::data.table()))
  }

  reg <- data.table::fread(reg_path, showProgress = FALSE)
  if ("tier" %in% names(reg)) reg <- reg[reg$tier == "Bronze"]

  if ("file_path" %in% names(reg) && !"category" %in% names(reg))
    reg[, category := .category_from_path(file_path, root)]

  # Filters
  if (!is.null(site_id)   && "site_id"   %in% names(reg)) reg <- reg[reg$site_id   %in% site_id  ]
  if (!is.null(category)  && "category"  %in% names(reg)) reg <- reg[reg$category  %in% category ]
  if (!is.null(data_type) && "data_type" %in% names(reg)) reg <- reg[reg$data_type %in% data_type]

  if (nrow(reg) == 0L) {
    message("No records match the specified filters.")
    return(invisible(data.table::data.table()))
  }

  by_cols <- intersect(
    c("site_id", "category", "supplier_code", "data_type",
      "temporal_resolution"),
    names(reg)
  )

  out <- reg[, {
    start <- if ("time_period_start" %in% names(.SD)) min(time_period_start, na.rm = TRUE) else NA_character_
    end   <- if ("time_period_end"   %in% names(.SD)) max(time_period_end,   na.rm = TRUE) else NA_character_
    res   <- if ("temporal_resolution" %in% names(.SD)) temporal_resolution[1L] else NA_character_

    # Estimate expected observations from register metadata
    int_sec <- .interval_seconds(res)
    expected <- if (!is.na(int_sec) && !is.na(start) && !is.na(end)) {
      span <- as.numeric(difftime(as.POSIXct(end,   tz = "UTC"),
                                  as.POSIXct(start, tz = "UTC"),
                                  units = "secs"))
      as.integer(round(span / int_sec)) + 1L
    } else NA_integer_

    list(
      start       = start,
      end         = end,
      n_datasets  = .N,
      days_held   = as.integer(
        round(as.numeric(difftime(
          as.Date(end), as.Date(start), units = "days"
        )))
      ),
      expected_obs = expected
    )
  }, by = by_cols]

  data.table::setorder(out, category, data_type, site_id)

  cat(sprintf("<Bronze Coverage>  %d gauge(s)\n\n", nrow(out)))
  print(out)
  invisible(out)
}


# =============================================================================
# check_gaps()
# =============================================================================

#' Identify missing timesteps for a Bronze gauge series
#'
#' Reads all Bronze Parquet files for the specified `site_id` and `data_type`,
#' deduplicates timestamps (which may overlap across ingest batches), and
#' identifies any missing timesteps given the expected observation interval.
#'
#' Gaps are reported as a `data.table` of contiguous missing periods, each
#' described by its `gap_start`, `gap_end`, and `n_missing` (number of missing
#' timesteps within that period).
#'
#' @param root Character. Root of the data store.
#' @param site_id Character. Site identifier.
#' @param data_type Character. Framework data type code (`"Q"`, `"H"`,
#'   `"P"`, etc.).
#' @param category Character. Data category subdirectory. Default
#'   `"hydrometric"`.
#' @param expected_interval Character or `NULL`. Expected time step between
#'   observations: `"15min"`, `"hourly"`, or `"daily"`. When `NULL`
#'   (default), the interval is inferred from the register if available,
#'   otherwise estimated from the observed median gap between successive
#'   timestamps.
#'
#' @return A `data.table` with columns `gap_start` (POSIXct), `gap_end`
#'   (POSIXct), and `n_missing` (integer). An empty table means no gaps were
#'   found. Also prints a summary header. Returned invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Check for gaps in 15-min flow record
#' check_gaps("data/hydro", site_id = "39001", data_type = "Q")
#'
#' # Force daily interval check on a stage gauge
#' check_gaps("data/hydro", site_id = "2723TH", data_type = "H",
#'            expected_interval = "daily")
#' }
check_gaps <- function(root, site_id, data_type, category = "hydrometric",
                       expected_interval = NULL) {
  # --- Resolve interval ---
  int_sec <- if (!is.null(expected_interval)) {
    s <- .interval_seconds(expected_interval)
    if (is.na(s)) stop("Unrecognised expected_interval: ", expected_interval,
                       ". Use '15min', 'hourly', or 'daily'.")
    s
  } else {
    # Try to read from register
    reg_path <- file.path(root, "register", "hydrometric_data_register.csv")
    s <- NA_integer_
    if (file.exists(reg_path)) {
      reg <- data.table::fread(reg_path, showProgress = FALSE)
      if ("tier" %in% names(reg)) reg <- reg[reg$tier == "Bronze"]
      rows <- reg[reg$site_id == site_id & reg$data_type == data_type]
      if (nrow(rows) > 0L && "temporal_resolution" %in% names(rows)) {
        s <- .interval_seconds(rows$temporal_resolution[1L])
      }
    }
    s
  }

  # --- Read Parquet files ---
  pq_files <- .find_site_parquets(root, site_id, data_type, category)

  if (length(pq_files) == 0L) {
    message(sprintf("No Bronze Parquet files found for site '%s', data type '%s'.",
                    site_id, data_type))
    return(invisible(data.table::data.table()))
  }

  ts_list <- lapply(pq_files, function(f) {
    dt <- data.table::as.data.table(arrow::read_parquet(f,
                                                         col_select = "timestamp"))
    dt$timestamp
  })
  all_ts <- sort(unique(do.call(c, ts_list)))

  if (length(all_ts) < 2L) {
    message("Fewer than 2 observations found; cannot assess gaps.")
    return(invisible(data.table::data.table()))
  }

  # --- Infer interval from data if still unknown ---
  if (is.na(int_sec)) {
    diffs    <- as.numeric(diff(all_ts), units = "secs")
    int_sec  <- as.integer(stats::median(diffs, na.rm = TRUE))
    label    <- if (int_sec == 900L) "15min" else
                if (int_sec == 3600L) "hourly" else
                if (int_sec == 86400L) "daily" else
                sprintf("%ds", int_sec)
    message(sprintf("  Inferred interval: %s (%d s).", label, int_sec))
  }

  # --- Build expected sequence and find gaps ---
  t_start  <- min(all_ts)
  t_end    <- max(all_ts)
  n_obs    <- length(all_ts)

  # All expected timesteps
  expected <- seq(t_start, t_end, by = int_sec)
  missing  <- expected[!expected %in% all_ts]

  if (length(missing) == 0L) {
    n_exp <- length(expected)
    cat(sprintf(
      "<Gap check: %s / %s>  No gaps found.\n  %d / %d observations present (%s to %s).\n",
      site_id, data_type, n_obs, n_exp,
      format(t_start, "%Y-%m-%d"), format(t_end, "%Y-%m-%d")
    ))
    return(invisible(data.table::data.table()))
  }

  # Collapse contiguous missing timestamps into gap periods
  gaps   <- as.integer(diff(missing) / int_sec)
  breaks <- c(0L, which(gaps > 1L), length(missing))

  gap_dt <- data.table::rbindlist(lapply(seq_along(breaks[-1L]), function(i) {
    idx_start <- breaks[i]      + 1L
    idx_end   <- breaks[i + 1L]
    data.table::data.table(
      gap_start = missing[idx_start],
      gap_end   = missing[idx_end],
      n_missing = idx_end - idx_start + 1L
    )
  }))

  n_exp   <- length(expected)
  pct_gap <- round(100 * length(missing) / n_exp, 1)

  cat(sprintf(
    paste0("<Gap check: %s / %s>  %d gap period(s), %d missing",
           " timestep(s) (%.1f%% of series).\n",
           "  Period: %s to %s  |  Interval: %ds  |  ",
           "%d / %d observations present.\n\n"),
    site_id, data_type,
    nrow(gap_dt), length(missing), pct_gap,
    format(t_start, "%Y-%m-%d"), format(t_end, "%Y-%m-%d"),
    int_sec, n_obs, n_exp
  ))
  print(gap_dt)
  invisible(gap_dt)
}
