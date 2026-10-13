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
#              Data sources: list_available_gauges() and summarise_coverage()
#              read only the governance register CSV. check_gaps() always
#              scans the Bronze Parquet files for timestamps and uses the
#              register only to look up the expected interval. The Silver
#              functions at the end of the file (list_silver_gauges(),
#              summarise_silver_coverage(), check_silver_gaps()) scan
#              <root>/silver directly and do not use the register.
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
  # Both sides go through normalizePath() so relative roots, ".." and mixed
  # separators compare equal, and the prefix is regex-escaped so dots or
  # brackets in the root are matched literally. The register's file_path values
  # must sit under <root>/bronze; otherwise the prefix is not stripped and the
  # result is not a real category.
  bronze_prefix <- paste0(normalizePath(file.path(root, "bronze"),
                                        mustWork = FALSE), .Platform$file.sep)
  rel <- sub(paste0("^", gsub("([.+?^${}()|\\[\\]\\\\])", "\\\\\\1",
                               bronze_prefix)),
             "", normalizePath(paths, mustWork = FALSE))
  # Split on either separator (backslashes after normalizePath on Windows) and
  # keep the first directory, i.e. the category; NA for an empty path.
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
  # List every Parquet under the category, then filter on path fragments (there
  # is no directory-level globbing).
  candidates <- list.files(search_root, pattern = "\\.parquet$",
                           recursive = TRUE, full.names = TRUE)
  # Keep only files under the correct data_type subdirectory
  # "/<data_type>/" must match a whole directory name (so "Q" does not match
  # "QC"); "_<site_id>_" matches the site segment of the dataset_id
  # (EA_39001_Q_..., WISKI_3401TH_H_sg_ds_...). Matching is fixed-string, so
  # characters such as "." in an id are literal. Every supplier and WISKI trace
  # held for that site + type is returned, i.e. callers see them pooled.
  dt_pat   <- paste0("/", data_type, "/")
  site_pat <- paste0("_", site_id, "_")
  candidates[grepl(dt_pat, candidates, fixed = TRUE) &
               grepl(site_pat, candidates, fixed = TRUE)]
}

# min()/max() of a character vector that return NA instead of failing when
# every value is NA (min() of an all-NA character vector is an error, and a
# register can lack period dates).
#' @noRd
.min_chr <- function(x) { x <- x[!is.na(x)]; if (length(x)) min(x) else NA_character_ }
#' @noRd
.max_chr <- function(x) { x <- x[!is.na(x)]; if (length(x)) max(x) else NA_character_ }

# Infer the expected observation interval in seconds from a character label.
#' @noRd
.interval_seconds <- function(label) {
  # Labels are the register's temporal_resolution values; case and surrounding
  # blanks are ignored. Anything else, including the "unknown" the Bronze
  # writers record when no resolution is given, yields NA, which check_gaps()
  # treats as "infer the interval from the data".
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
#' @details
#' Only the register is consulted; the file system is not scanned. Files that
#' exist on disk but were never registered do not appear, and registered files
#' that have since been deleted still do. Use [check_gaps()] when the Parquet
#' files themselves matter.
#'
#' Only Bronze rows are kept (where the register has a `tier` column). The
#' register written by this package has no category column, so `category` is
#' derived from each `file_path` as the first directory below
#' `<root>/bronze/`; that works only if the recorded paths sit under `root`.
#' Filters are exact matches (any-of when a vector is given) and are applied
#' before summarising. A filter whose column is absent from the register is
#' ignored rather than raising an error.
#'
#' Rows are grouped by site, category, supplier, data type and temporal
#' resolution, so one site can appear several times (for example when two
#' suppliers hold it). `start` and `end` are the earliest and latest period
#' dates recorded (`"YYYY-MM-DD"` text). `n_datasets` counts register rows: the
#' Bronze writers record one row per year file and every ingest appends more
#' (a same-day re-ingest merges into the existing file but still adds a row), so it counts
#' registered files rather than distinct series.
#'
#' A header line and the table are printed to the console. When the register is
#' missing or nothing matches, a message (not an error) is issued and an empty
#' table is returned. Related: [summarise_coverage()], [check_gaps()],
#' [read_bronze()], [setup_hydro_store()].
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
  # -- Load register ---------------------------------------------------------
  # The register is the only source here; the file system is not scanned.
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

  # -- Filter ----------------------------------------------------------------
  # `category` and `data_type` are also column names. Inside reg[...] a bare
  # symbol resolves to the COLUMN, so reg[category %in% category] would compare
  # the column with itself and keep every row. .filter_rows() (package.R)
  # evaluates the match outside the table scope. A filter whose column is
  # absent from the register is skipped.
  if (!is.null(category)  && "category"       %in% names(reg))
    reg <- .filter_rows(reg, "category", category)
  if (!is.null(supplier)  && "supplier_code"   %in% names(reg))
    reg <- .filter_rows(reg, "supplier_code", supplier)
  if (!is.null(data_type) && "data_type"       %in% names(reg))
    reg <- .filter_rows(reg, "data_type", data_type)

  if (nrow(reg) == 0L) {
    message("No Bronze records match the specified filters.")
    return(invisible(data.table::data.table()))
  }

  # -- Summarise -------------------------------------------------------------
  # One row per site x category x supplier x data_type x resolution (not just
  # site + data_type), so a site with several suppliers or resolutions repeats.
  by_cols <- intersect(
    c("site_id", "category", "supplier_code", "data_type",
      "temporal_resolution"),
    names(reg)
  )

  # start/end are the min/max of ISO "YYYY-MM-DD" strings, which order
  # correctly as text. n_datasets counts register ROWS, one per year file.
  out <- reg[, {
    start_vals <- if ("time_period_start" %in% names(reg)) time_period_start else NA_character_
    end_vals   <- if ("time_period_end"   %in% names(reg)) time_period_end   else NA_character_
    list(
      start      = .min_chr(start_vals),
      end        = .max_chr(end_vals),
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
#' Reports, for each gauge in the Bronze store, the time span held and the number
#' of observations that span *should* contain at the register's resolution. Both
#' come from the register (`time_period_start` / `time_period_end` /
#' `temporal_resolution`) without reading the underlying Parquet files, so they
#' describe the *intended* coverage rather than the literal row count.
#'
#' To inspect actual record counts or detect individual missing timesteps, use
#' `check_gaps()`.
#'
#' @details
#' Like [list_available_gauges()], this reads only the register (Bronze rows;
#' `category` derived from `file_path`) and opens no Parquet files, so it cannot
#' report actual record counts or a completeness percentage (the table has
#' neither); use [check_gaps()] for that. Rows are grouped by site, category, supplier, data
#' type and temporal resolution, and the table adds:
#'
#' - `start`, `end`: earliest and latest period dates recorded, as
#'   `"YYYY-MM-DD"` text.
#' - `n_datasets`: number of register rows (one per year file; every ingest
#'   appends more).
#' - `days_held`: whole days between `start` and `end`. Gaps inside the span
#'   are not subtracted.
#' - `expected_obs`: the number of steps of the register's resolution in the
#'   whole days from `start` to `end` inclusive (for example 96 per day for
#'   15-minute data). It is a count for the span, not for the data held: gaps
#'   are not subtracted. It is `NA` where the resolution is missing or not one
#'   of `"15min"`, `"hourly"`, `"daily"`.
#'
#' Filters are exact any-of matches applied before summarising, and one whose
#' column is absent from the register is ignored. A message (not an error) is
#' issued, with an empty table returned, when the register is missing or
#' nothing matches. Related: [list_available_gauges()], [check_gaps()].
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
  # -- Load register ---------------------------------------------------------
  # Register only (Bronze rows); no Parquet files are opened here.
  reg_path <- file.path(root, "register", "hydrometric_data_register.csv")

  if (!file.exists(reg_path)) {
    message("No governance register found at: ", reg_path)
    return(invisible(data.table::data.table()))
  }

  reg <- data.table::fread(reg_path, showProgress = FALSE)
  if ("tier" %in% names(reg)) reg <- reg[reg$tier == "Bronze"]

  if ("file_path" %in% names(reg) && !"category" %in% names(reg))
    reg[, category := .category_from_path(file_path, root)]

  # -- Filter ----------------------------------------------------------------
  # `site_id`, `category` and `data_type` are also column names: inside reg[...]
  # a bare symbol would resolve to the COLUMN and the filter would keep every
  # row, so .filter_rows() does the match outside the table scope.
  if (!is.null(site_id)   && "site_id"   %in% names(reg)) reg <- .filter_rows(reg, "site_id", site_id)
  if (!is.null(category)  && "category"  %in% names(reg)) reg <- .filter_rows(reg, "category", category)
  if (!is.null(data_type) && "data_type" %in% names(reg)) reg <- .filter_rows(reg, "data_type", data_type)

  if (nrow(reg) == 0L) {
    message("No records match the specified filters.")
    return(invisible(data.table::data.table()))
  }

  # -- Summarise -------------------------------------------------------------
  by_cols <- intersect(
    c("site_id", "category", "supplier_code", "data_type",
      "temporal_resolution"),
    names(reg)
  )

  # Per group: span from the earliest start to the latest end across all
  # registered batches (overlapping re-ingests are not double counted in the
  # span), plus the number of register rows.
  out <- reg[, {
    start <- if ("time_period_start" %in% names(.SD)) .min_chr(time_period_start) else NA_character_
    end   <- if ("time_period_end"   %in% names(.SD)) .max_chr(time_period_end)   else NA_character_
    # Grouping columns are NOT part of .SD, but they are available by name in j.
    # temporal_resolution is normally a grouping column, so test by_cols first
    # (testing names(.SD) alone made `res`, and so expected_obs, always NA).
    res   <- if ("temporal_resolution" %in% by_cols) {
      temporal_resolution[1L]
    } else if ("temporal_resolution" %in% names(.SD)) {
      temporal_resolution[1L]
    } else NA_character_

    # Estimate expected observations from register metadata
    int_sec <- .interval_seconds(res)
    # The register dates are day-precision: `start` is the first day and `end`
    # the last day, both whole days. The covered span is therefore from the
    # midnight starting `start` to the midnight ENDING `end` (one extra day), and
    # the expected count is that span divided by the step. (Counting only up to
    # midnight at the start of the last day, as an earlier version did, left out
    # the whole last day: 95 steps for 15-minute data.)
    expected <- if (!is.na(int_sec) && !is.na(start) && !is.na(end)) {
      span <- as.numeric(difftime(as.POSIXct(end,   tz = "UTC"),
                                  as.POSIXct(start, tz = "UTC"),
                                  units = "secs")) + 86400
      as.integer(round(span / int_sec))
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

# Find all Silver Parquet files for one site_id + data_type combination.
#' @noRd
.find_silver_parquets <- function(root, site_id, data_type) {
  silver_root <- file.path(root, "silver")
  if (!dir.exists(silver_root)) return(character(0))

  # Same fragment matching as .find_site_parquets() ("/<data_type>/" directory,
  # "_<site_id>_" in the dataset_id). Silver has no supplier level, and every
  # category is searched.
  candidates <- list.files(silver_root, pattern = "\\.parquet$",
                           recursive = TRUE, full.names = TRUE)
  dt_pat   <- paste0("/", data_type, "/")
  site_pat <- paste0("_", site_id, "_")
  candidates[grepl(dt_pat,   candidates, fixed = TRUE) &
               grepl(site_pat, candidates, fixed = TRUE)]
}


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
#' @details
#' Order of work: the interval is resolved first (the `expected_interval`
#' argument, else the register's `temporal_resolution` for this site and data
#' type, else the median spacing of the timestamps found); the `timestamp`
#' column of every matching file is then read and pooled; and a regular grid
#' from the first to the last timestamp is compared with what was found. Each
#' grid step with no observation is a missing timestep, and runs of consecutive
#' missing steps are reported as one period.
#'
#' Files are found by scanning `<root>/bronze/<category>/` for paths containing
#' `/<data_type>/` and `_<site_id>_`, not through the register. All suppliers
#' and WISKI traces held for the site and type are therefore pooled, and
#' duplicate timestamps (Bronze is append-only, so re-ingests overlap) are
#' counted once. A gap in one supplier's record is hidden if another supplier
#' covers it.
#'
#' A grid step is present only if a timestamp equals it exactly. Off-grid
#' timestamps (for example 09:14:00 in a 15-minute series) do not fill any
#' step, so the steps around them are reported as missing; resolve them first
#' (see [snap_offgrid_timestamps()] for 15-minute data). Steps are fixed
#' lengths in seconds, which assumes timestamps stored in UTC. `gap_start` and
#' `gap_end` are the first and last missing steps of a period, not the
#' neighbouring observations.
#'
#' A message (not an error) is issued, with an empty table returned, when no
#' files match or fewer than two distinct timestamps are found; an
#' unrecognised `expected_interval` is an error. When the interval is inferred,
#' a message reports it. A summary and the gap table are printed to the console.
#' Related: [list_available_gauges()], [summarise_coverage()],
#' [check_silver_gaps()].
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
  # Precedence: explicit argument, then the register, then (further down) the
  # median spacing of the observed timestamps.
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
      # which() on explicit reg$ vectors: reg[site_id == site_id & ...] would
      # compare the column with itself (the column shadows the argument) and
      # match every row.
      # The register is not category-aware, and the first matching row wins.
      idx  <- which(reg$site_id == site_id & reg$data_type == data_type)
      rows <- reg[idx]
      if (nrow(rows) > 0L && "temporal_resolution" %in% names(rows)) {
        s <- .interval_seconds(rows$temporal_resolution[1L])
      }
    }
    s
  }

  # --- Read Parquet files ---
  # Scans the file system rather than the register, so files added outside
  # the normal ingest pipeline are found too.
  pq_files <- .find_site_parquets(root, site_id, data_type, category)

  if (length(pq_files) == 0L) {
    message(sprintf("No Bronze Parquet files found for site '%s', data type '%s'.",
                    site_id, data_type))
    return(invisible(data.table::data.table()))
  }

  # Only the timestamp column is read (column projection); values play no
  # part in gap detection.
  ts_list <- lapply(pq_files, function(f) {
    dt <- data.table::as.data.table(arrow::read_parquet(f,
                                                         col_select = "timestamp"))
    dt$timestamp
  })
  # Pool across files, suppliers, traces and years. unique() collapses the
  # duplicates that append-only re-ingests create, before gaps are assessed.
  all_ts <- sort(unique(do.call(c, ts_list)))

  if (length(all_ts) < 2L) {
    message("Fewer than 2 observations found; cannot assess gaps.")
    return(invisible(data.table::data.table()))
  }

  # --- Infer interval from data if still unknown ---
  if (is.na(int_sec)) {
    # Median successive spacing is robust to occasional gaps; diff() units are
    # forced to seconds so the result is comparable with .interval_seconds().
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
  # Regular grid from the first to the last observation. A grid step counts as
  # present only on an exact timestamp match.
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
  # Intent: start a new period wherever consecutive missing steps are more than
  # one interval apart. `breaks` holds the index of the last missing step of
  # each period, bracketed by 0 and length(missing); gap_start/gap_end are the
  # first and last MISSING steps of a period.
  # diff() of POSIXct is a difftime whose units R chooses (mins, hours, days)
  # from the data, so it must be converted to seconds explicitly before dividing
  # by int_sec; dividing the raw difftime gave ~0 for every pair and merged all
  # gaps into one period.
  gaps   <- as.integer(round(as.numeric(diff(missing), units = "secs") / int_sec))
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


# =============================================================================
# list_silver_gauges()
# =============================================================================

#' List all gauges available in the Silver store
#'
#' Scans the Silver store directory tree and returns a catalogue of every
#' gauge (unique `site_id` + `data_type` combination) for which Silver data
#' has been promoted from Bronze.
#'
#' @details
#' Only file names and directories are inspected: no Parquet file is opened and
#' the register is not used. Silver files are expected at
#' `<root>/silver/<category>/<data_type>/<YYYY>/<dataset_id>.parquet`, and the
#' year folder is the year the Bronze data was *received* (the end of the
#' `dataset_id`), not an observation year, so `year_min` and `year_max` are
#' ingest years. `site_id` is taken as the second underscore-separated field of
#' the file name (`EA_39001_Q_20240101` gives `39001`), so site ids that
#' themselves contain an underscore are mis-parsed. Paths of the wrong depth or
#' with too few name fields are skipped without comment.
#'
#' `file_count` is the number of Parquet files, not rows. Messages (not errors)
#' are issued, with an empty table returned, when there is no Silver store, no
#' files, or nothing matches. Related: [promote_to_silver()],
#' [summarise_silver_coverage()], [check_silver_gaps()], [read_silver()].
#'
#' @param root Character. Root of the data store.
#' @param data_type Character vector or `NULL`. Filter to one or more
#'   framework data type codes, e.g. `"Q"`, `"H"`, `"P"`. `NULL` (default)
#'   returns all types.
#'
#' @return A `data.table` with columns `site_id`, `category`, `data_type`,
#'   `year_min`, `year_max`, and `file_count`, printed as a formatted
#'   catalogue. Returned invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' list_silver_gauges("data/hydro")
#' list_silver_gauges("data/hydro", data_type = "Q")
#' }
list_silver_gauges <- function(root, data_type = NULL) {
  silver_root <- file.path(root, "silver")

  if (!dir.exists(silver_root)) {
    message("No Silver store found at: ", silver_root,
            "\nRun promote_to_silver() to populate the Silver tier.")
    return(invisible(data.table::data.table()))
  }

  files <- list.files(silver_root, pattern = "\\.parquet$",
                      recursive = TRUE, full.names = TRUE)

  if (length(files) == 0L) {
    message("No Silver Parquet files found in: ", silver_root)
    return(invisible(data.table::data.table()))
  }

  # (The root is regex-escaped below so dots or brackets in it match literally.)
  # Normalise paths for cross-platform splitting
  norm_root <- normalizePath(silver_root, mustWork = FALSE)
  rel <- sub(
    paste0("^", gsub("([.+?^${}()|\\[\\]\\\\])", "\\\\\\1", norm_root),
           .Platform$file.sep),
    "",
    normalizePath(files, mustWork = FALSE)
  )
  # No supplier level in Silver (see silver_path()); <year> is the received year.
  # Expected: <category>/<data_type>/<year>/<dataset_id>.parquet
  parts <- strsplit(rel, "/|\\\\")

  # site_id is the 2nd underscore-separated field of the dataset_id
  # (<SUPPLIER>_<site>_<type>_<date>); shallower or oddly named paths are
  # dropped here and NULL results disappear in rbindlist().
  parsed <- data.table::rbindlist(lapply(seq_along(parts), function(i) {
    p <- parts[[i]]
    if (length(p) < 4L) return(NULL)
    fname    <- tools::file_path_sans_ext(p[4L])
    segs     <- strsplit(fname, "_")[[1L]]
    if (length(segs) < 3L) return(NULL)
    data.table::data.table(
      category  = p[1L],
      data_type = p[2L],
      year      = p[3L],
      site_id   = segs[2L],
      file      = files[i]
    )
  }), fill = TRUE)

  if (nrow(parsed) == 0L) {
    message("Could not parse any Silver file paths.")
    return(invisible(data.table::data.table()))
  }

  # `data_type` is also a column of `parsed`: .filter_rows() avoids the bare
  # symbol resolving to the column inside parsed[...] (package.R).
  if (!is.null(data_type))
    parsed <- .filter_rows(parsed, "data_type", data_type)

  if (nrow(parsed) == 0L) {
    message("No Silver files match the specified data_type filter.")
    return(invisible(data.table::data.table()))
  }

  # year is the directory name (text); 4-digit years order correctly as text.
  out <- parsed[, .(
    year_min   = min(year),
    year_max   = max(year),
    file_count = .N
  ), by = c("category", "data_type", "site_id")]

  data.table::setorder(out, category, data_type, site_id)

  cat(sprintf("<Silver Catalogue>  %d gauge(s) across %d data type(s)\n\n",
              nrow(out), length(unique(out$data_type))))
  print(out)
  invisible(out)
}


# =============================================================================
# summarise_silver_coverage()
# =============================================================================

#' Summarise QC coverage for Silver store gauges
#'
#' Reads a subset of columns from every Silver Parquet file and returns
#' per-gauge statistics: date range, total row count, and the distribution
#' of QC flags (counts and percentage Good).
#'
#' @details
#' Files are selected by path fragment (`/<data_type>/` directory and
#' `_<site_id>_` in the file name; several values are OR-ed) before anything is
#' read, then only `timestamp`, `site_id`, `data_type` and `qc_flag` are loaded.
#' A file that cannot be read is skipped silently, so a corrupt file simply
#' drops out of the totals.
#'
#' Counts are over the rows of every matching file. If the same observations
#' were promoted more than once (Silver files are per received dataset) they
#' are counted each time. `pct_good` is the share of all rows with `qc_flag == 1`
#' (rows with a missing flag stay in the denominator), rounded to one decimal.
#' `start` and `end` are the first and last dates as `"YYYY-MM-DD"` text.
#' Messages (not errors) are issued, with an empty table returned, when the
#' store, files or matches are missing. Related: [list_silver_gauges()],
#' [check_silver_gaps()], [promote_to_silver()].
#'
#' @param root Character. Root of the data store.
#' @param data_type Character vector or `NULL`. Filter to one or more data
#'   type codes. `NULL` (default) shows all types.
#' @param site_id Character vector or `NULL`. Filter to one or more sites.
#'   `NULL` (default) shows all sites.
#'
#' @return A `data.table` with one row per site + data-type, printed as a
#'   coverage summary. Returned invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' summarise_silver_coverage("data/hydro")
#' summarise_silver_coverage("data/hydro", data_type = "Q", site_id = "39001")
#' }
summarise_silver_coverage <- function(root, data_type = NULL, site_id = NULL) {
  silver_root <- file.path(root, "silver")

  if (!dir.exists(silver_root)) {
    message("No Silver store found at: ", silver_root)
    return(invisible(data.table::data.table()))
  }

  files <- list.files(silver_root, pattern = "\\.parquet$",
                      recursive = TRUE, full.names = TRUE)

  if (length(files) == 0L) {
    message("No Silver Parquet files found.")
    return(invisible(data.table::data.table()))
  }

  # Apply path-level filters before reading
  # Filtering is on path fragments, not on a column, so the data_type and
  # site_id arguments never meet a same-named column in a [...] expression.
  if (!is.null(data_type)) {
    pats  <- paste0("/", data_type, "/")
    keep  <- Reduce(`|`, lapply(pats, function(p) grepl(p, files, fixed = TRUE)))
    files <- files[keep]
  }
  if (!is.null(site_id)) {
    pats  <- paste0("_", site_id, "_")
    keep  <- Reduce(`|`, lapply(pats, function(p) grepl(p, files, fixed = TRUE)))
    files <- files[keep]
  }

  if (length(files) == 0L) {
    message("No Silver files match the specified filters.")
    return(invisible(data.table::data.table()))
  }

  # Unreadable files become NULL and are dropped by Filter() below.
  records <- lapply(files, function(f) {
    tryCatch(
      data.table::as.data.table(
        arrow::read_parquet(f,
          col_select = c("timestamp", "site_id", "data_type", "qc_flag"))
      ),
      error = function(e) NULL
    )
  })
  combined <- data.table::rbindlist(Filter(Negate(is.null), records))

  if (nrow(combined) == 0L) {
    message("No data could be read from Silver files.")
    return(invisible(data.table::data.table()))
  }

  # n_good + n_estimated + n_suspect + n_rejected can be less than n_rows when
  # qc_flag is missing; pct_good uses all rows (.N) as the denominator.
  out <- combined[, .(
    start       = as.character(as.Date(min(timestamp))),
    end         = as.character(as.Date(max(timestamp))),
    n_rows      = .N,
    n_good      = sum(qc_flag == 1L, na.rm = TRUE),
    n_estimated = sum(qc_flag == 2L, na.rm = TRUE),
    n_suspect   = sum(qc_flag == 3L, na.rm = TRUE),
    n_rejected  = sum(qc_flag == 4L, na.rm = TRUE),
    pct_good    = round(100 * sum(qc_flag == 1L, na.rm = TRUE) / .N, 1)
  ), by = c("site_id", "data_type")]

  data.table::setorder(out, data_type, site_id)

  cat(sprintf("<Silver Coverage>  %d gauge(s)\n\n", nrow(out)))
  print(out)
  invisible(out)
}


# =============================================================================
# check_silver_gaps()
# =============================================================================

#' Identify missing timesteps for a Silver gauge series
#'
#' Reads timestamps from all Silver Parquet files for the specified `site_id`
#' and `data_type`, infers the expected observation interval, and reports any
#' contiguous periods of missing data.
#'
#' @details
#' Same method as [check_gaps()] but for the Silver tier: timestamps are pooled
#' from every file found under `<root>/silver` whose path contains
#' `/<data_type>/` and `_<site_id>_` (any category), duplicates are removed, and
#' a regular grid from the first to the last timestamp is compared with what
#' was found. Differences from the Bronze version: the register is never
#' consulted, so the interval comes from `expected_interval` or, failing that,
#' the median observed spacing (reported in a message); and files that cannot
#' be read are skipped silently.
#'
#' A grid step is present only on an exact timestamp match, so off-grid
#' timestamps leave the steps around them reported as missing
#' (see [snap_offgrid_timestamps()]). `gap_start` and `gap_end` are the first
#' and last missing steps of a period. Messages (not errors) are issued, with an
#' empty table returned, when no files match or fewer than two timestamps are
#' found; an unrecognised `expected_interval` is an error. A summary and the
#' gap table are printed. Related: [list_silver_gauges()],
#' [summarise_silver_coverage()].
#'
#' @param root Character. Root of the data store.
#' @param site_id Character. Site identifier.
#' @param data_type Character. Framework data type code (`"Q"`, `"H"`,
#'   `"P"`, etc.).
#' @param expected_interval Character or `NULL`. Expected time step:
#'   `"15min"`, `"hourly"`, or `"daily"`. When `NULL` (default), inferred
#'   from the median observed gap.
#'
#' @return A `data.table` with columns `gap_start` (POSIXct), `gap_end`
#'   (POSIXct), and `n_missing` (integer). An empty table means no gaps.
#'   Returned invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' check_silver_gaps("data/hydro", site_id = "39001", data_type = "Q")
#' }
check_silver_gaps <- function(root, site_id, data_type,
                              expected_interval = NULL) {
  pq_files <- .find_silver_parquets(root, site_id, data_type)

  if (length(pq_files) == 0L) {
    message(sprintf("No Silver files found for site '%s', data type '%s'.",
                    site_id, data_type))
    return(invisible(data.table::data.table()))
  }

  # Unreadable files are skipped silently (error -> NULL -> Filter()).
  ts_list <- lapply(pq_files, function(f) {
    tryCatch(
      data.table::as.data.table(
        arrow::read_parquet(f, col_select = "timestamp")
      )$timestamp,
      error = function(e) NULL
    )
  })
  all_ts <- sort(unique(do.call(c, Filter(Negate(is.null), ts_list))))

  if (length(all_ts) < 2L) {
    message("Fewer than 2 observations found; cannot assess gaps.")
    return(invisible(data.table::data.table()))
  }

  # Unlike check_gaps() there is no register lookup: explicit argument, else
  # the median spacing of the pooled timestamps.
  int_sec <- if (!is.null(expected_interval)) {
    s <- .interval_seconds(expected_interval)
    if (is.na(s))
      stop("Unrecognised expected_interval: ", expected_interval,
           ". Use '15min', 'hourly', or 'daily'.")
    s
  } else {
    diffs <- as.numeric(diff(all_ts), units = "secs")
    s     <- as.integer(stats::median(diffs, na.rm = TRUE))
    label <- if (s == 900L) "15min" else if (s == 3600L) "hourly" else
             if (s == 86400L) "daily" else sprintf("%ds", s)
    message(sprintf("  Inferred interval: %s (%d s).", label, s))
    s
  }

  # Regular grid from first to last observation; present only on an exact match.
  expected <- seq(min(all_ts), max(all_ts), by = int_sec)
  missing  <- expected[!expected %in% all_ts]
  n_obs    <- length(all_ts)

  if (length(missing) == 0L) {
    cat(sprintf(
      "<Silver gap check: %s / %s>  No gaps found.\n  %d / %d observations present (%s to %s).\n",
      site_id, data_type, n_obs, length(expected),
      format(min(all_ts), "%Y-%m-%d"), format(max(all_ts), "%Y-%m-%d")
    ))
    return(invisible(data.table::data.table()))
  }

  # Intent as in check_gaps(): a new period starts wherever consecutive missing
  # steps are more than one interval apart. Seconds are requested explicitly:
  # a bare difftime has auto-chosen units (see check_gaps()).
  gaps   <- as.integer(round(as.numeric(diff(missing), units = "secs") / int_sec))
  breaks <- c(0L, which(gaps > 1L), length(missing))

  gap_dt <- data.table::rbindlist(lapply(seq_along(breaks[-1L]), function(i) {
    idx_s <- breaks[i] + 1L
    idx_e <- breaks[i + 1L]
    data.table::data.table(
      gap_start = missing[idx_s],
      gap_end   = missing[idx_e],
      n_missing = idx_e - idx_s + 1L
    )
  }))

  pct_gap <- round(100 * length(missing) / length(expected), 1)
  cat(sprintf(
    paste0("<Silver gap check: %s / %s>  %d period(s), %d missing timestep(s)",
           " (%.1f%%).\n  %s to %s  |  %ds  |  %d / %d present.\n\n"),
    site_id, data_type, nrow(gap_dt), length(missing), pct_gap,
    format(min(all_ts), "%Y-%m-%d"), format(max(all_ts), "%Y-%m-%d"),
    int_sec, n_obs, length(expected)
  ))
  print(gap_dt)
  invisible(gap_dt)
}
