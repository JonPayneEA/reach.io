# ============================================================
# Tool:        Output Helper
# Description: Internal helper that either returns a
#              data.table in memory or writes it to a CSV,
#              used by the sync and batch download paths.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      data.table of readings, output mode
# Outputs:     data.table (memory) or CSV file path (disk)
# Dependencies: data.table
# ============================================================

# ── Output helper ─────────────────────────────────────────────────────────────

#' Handle output of a completed data.table of readings
#'
#' Internal helper that either returns a `data.table` of readings unchanged
#' (memory mode) or writes it as a Bronze-tier Parquet file following the
#' framework path convention:
#' `bronze/hydrometric/EA/<data_type>/<YYYY>/<dataset_id>.parquet`
#'
#' Applies [apply_bronze_schema()] to standardise columns before writing.
#'
#' @param dt A `data.table` of readings with at least `dateTime`, `value`,
#'   and optionally `quality` columns (as returned by [fetch_readings()]).
#' @param output Character. `"memory"` or `"disk"`.
#' @param out_dir Character or `NULL`. Root store directory (disk mode only).
#' @param parameter Character. Parameter name (`"rainfall"`, `"flow"`,
#'   `"level"`); mapped to a framework data type code via
#'   [param_to_data_type()].
#' @param measure_id Character. Measure notation; used as fallback `site_id`
#'   when `site_id` is `NA` or empty.
#' @param append Logical. Ignored — kept for signature compatibility.
#' @param site_id Character. Supplier site identifier (e.g. WISKI ID).
#'   Defaults to `measure_id` when `NA` or empty.
#' @param period Character. Temporal resolution stored in the Bronze schema
#'   `period` column, e.g. `"15min"`, `"daily"`.
#'
#' @return In `"memory"` mode, the `data.table` unchanged. In `"disk"` mode,
#'   the Bronze Parquet file path as a character string (invisibly).
#'
#' @noRd
handle_output <- function(dt, output, out_dir, parameter, measure_id,
                           append = FALSE, site_id = NA_character_,
                           period = NA_character_) {

  if (output == "memory") return(dt)

  data_type     <- param_to_data_type(parameter)
  supplier_code <- "EA"
  category      <- "hydrometric"

  sid        <- if (!is.na(site_id) && nzchar(site_id)) site_id else measure_id
  dataset_id <- make_dataset_id(supplier_code, sid, data_type)

  # Partition by data year — one Parquet file per calendar year of observations
  data_years <- sort(unique(data.table::year(dt[["dateTime"]])))
  paths      <- character(length(data_years))

  for (i in seq_along(data_years)) {
    yr      <- data_years[i]
    yr_dt   <- dt[data.table::year(dateTime) == yr]

    bronze_dt <- apply_bronze_schema(
      yr_dt,
      dataset_id        = dataset_id,
      site_id           = sid,
      data_type         = data_type,
      timestamp_col     = "dateTime",
      value_col         = "value",
      supplier_flag_col = "quality",
      period            = period
    )

    path <- bronze_path(out_dir, category, supplier_code, data_type, dataset_id,
                        year = yr)
    dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
    arrow::write_parquet(bronze_dt, path)
    paths[i] <- path
  }

  invisible(paths)
}


# ── PDM for PCs formatter ──────────────────────────────────────────────────────

#' Format a time series for PDM for PCs software
#'
#' Reshapes a `data.table` (or `data.frame`) containing a `dateTime` and
#' `value` column into the column layout expected by the PDM for PCs
#' rainfall-runoff modelling software:
#'
#' \code{year | month | day | hour | minute | second | <measure>}
#'
#' The measure column is named `"rainfall"`, `"flow"`, or `"level"` according
#' to the `measure` argument.
#'
#' @param dt A `data.table` or `data.frame` with at least two columns:
#'   `dateTime` (POSIXct) and `value` (numeric).
#' @param measure Character. One of `"rainfall"`, `"flow"`, or `"level"`.
#'   Names the data column in the output.
#' @param tz Character. Time zone used when decomposing `dateTime` into its
#'   components. Defaults to `"GMT"`.
#'
#' @return A plain `data.frame` (not a `data.table`) with columns
#'   `year`, `month`, `day`, `hour`, `minute`, `second`, and `<measure>`.
#'
#' @export
#'
#' @examples
#' library(data.table)
#' dt <- data.table(
#'   dateTime = as.POSIXct("2024-03-15 09:30:00", tz = "GMT"),
#'   value    = 2.5
#' )
#' format_for_pdm(dt, "rainfall")
format_for_pdm <- function(dt, measure = c("rainfall", "flow", "level"),
                           tz = "GMT") {
  measure <- match.arg(measure)

  if (!inherits(dt, "data.frame")) {
    stop("`dt` must be a data.frame or data.table.")
  }
  if (!"dateTime" %in% names(dt)) {
    stop("`dt` must contain a `dateTime` column.")
  }
  if (!"value" %in% names(dt)) {
    stop("`dt` must contain a `value` column.")
  }

  dtt <- as.POSIXlt(dt$dateTime, tz = tz)

  out <- data.frame(
    year   = dtt$year + 1900L,
    month  = dtt$mon  + 1L,
    day    = dtt$mday,
    hour   = dtt$hour,
    minute = dtt$min,
    second = as.integer(dtt$sec)
  )
  out[[measure]] <- dt$value

  out
}


# ── Flood Modeller Pro — Bespoke Format 1 exporter ────────────────────────────

#' Export flow time series to Flood Modeller Pro Bespoke Format 1
#'
#' Writes one or more [Flow_Daily] or [Flow_15min] objects to the four-header-row
#' CSV format used by Flood Modeller Pro (FMP) to assign flow boundary conditions
#' to named 1D IED (node) references at simulation runtime.
#'
#' **File layout:**
#' ```
#' <title>,,                     <- Row 1: file title (first cell only)
#' <comment>,,                   <- Row 2: free-text comment (first cell only)
#' ,<gauge_id_1>,<gauge_id_2>    <- Row 3: blank then WISKI gauge IDs
#' Time,<ied_ref_1>,<ied_ref_2>  <- Row 4: "Time" then IED references
#' 0.000,12.500,8.100            <- data: relative hours, flow in m³/s
#' 0.250,12.600,8.200
#' ```
#'
#' **IED reference construction (Row 4):**
#' The column headers in Row 4 are resolved in order of preference:
#' \enumerate{
#'   \item `ied_refs` supplied explicitly — used as-is.
#'   \item `names(flows)` and `gauge_ids` both present — concatenated as
#'     `"<name>_<gauge_id>"` (e.g. `"Bruton_405553"`).
#'   \item Only `gauge_ids` supplied — gauge IDs used directly.
#'   \item Only `names(flows)` present — list names used directly.
#'   \item No metadata available — fallback labels `"site_1"`, `"site_2"`, …
#' }
#'
#' **Multi-site merging:**
#' When `flows` contains more than one object, all time series are merged by a
#' full outer join on `dateTime`. Sites with different date ranges or missing
#' timesteps produce `NA` (or `na_fill`) in the merged output rather than
#' silently truncating any series to the shortest.
#'
#' **Reference time (`start_time`):**
#' The time column is hours elapsed since `start_time`. By default this is the
#' earliest `dateTime` value across all input series, giving `t = 0` at the
#' first observation. Override `start_time` when the FMP simulation clock must
#' start before the data begins — for example, when supplying a warm-up period
#' or aligning multiple boundary files to a common origin.
#'
#' **In-memory vs. file output:**
#' Omitting `out_file` returns the formatted lines as a character vector,
#' which is convenient for inspection (`cat(lines, sep = "\n")`) and unit
#' testing without touching the file system. Supply `out_file` to write
#' directly to disk; parent directories are created automatically.
#'
#' @param flows A named `list` of [Flow_Daily] or [Flow_15min] objects, one per
#'   gauge. A single (unnamed) object is also accepted and wrapped automatically.
#'   List names are used to construct IED references when `ied_refs` is `NULL`.
#' @param title Character scalar. File title placed in Row 1. Default
#'   `"flow_export"`.
#' @param comment Character scalar. Free-text comment placed in Row 2. Default
#'   `""` (blank row).
#' @param gauge_ids Character vector, one element per site. WISKI gauge IDs
#'   written to Row 3. If `NULL` (default), Row 3 is written with blank cells.
#' @param ied_refs Character vector, one element per site. IED reference labels
#'   for Row 4. If `NULL` (default), derived from `names(flows)` and
#'   `gauge_ids` according to the rules in the Details section.
#' @param start_time `POSIXct` scalar. Timestamp used as \eqn{t = 0} for the
#'   relative time column. If `NULL` (default), the earliest `dateTime` value
#'   across all `flows` is used.
#' @param na_fill Numeric scalar. Value substituted for `NA` readings in the
#'   output. Defaults to `NA_real_` (written as a blank CSV cell). Set to `0`
#'   if FMP requires non-missing values.
#' @param out_file Character scalar or `NULL`. If supplied, the formatted lines
#'   are written to this path (parent directories created as needed) and the
#'   path is returned invisibly. If `NULL` (default), a character vector of
#'   lines is returned invisibly — useful for inspection and testing.
#'
#' @return If `out_file` is `NULL`, a character vector of CSV lines (invisibly).
#'   If `out_file` is supplied, `out_file` as a character scalar (invisibly).
#'
#' @seealso [format_for_pdm()] for PDM for PCs output; [apply_rating()] to
#'   convert level data to flow before exporting.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Single site
#' lines <- format_for_fmp(
#'   list(Bruton = flow_obj),
#'   title     = "Somerset_boundaries",
#'   comment   = "EA operational export 2024-01-15",
#'   gauge_ids = "405553"
#' )
#' cat(paste(lines[1:6], collapse = "\n"))
#'
#' # Multiple sites — write directly to file
#' format_for_fmp(
#'   list(Bruton = flow1, Wincanton = flow2),
#'   title     = "Somerset_boundaries",
#'   gauge_ids = c("405553", "365943"),
#'   out_file  = "outputs/Somerset_boundaries.csv"
#' )
#' }
format_for_fmp <- function(flows,
                           title      = "flow_export",
                           comment    = "",
                           gauge_ids  = NULL,
                           ied_refs   = NULL,
                           start_time = NULL,
                           na_fill    = NA_real_,
                           out_file   = NULL) {

  # -- 1. Normalise input -------------------------------------------------------

  # Accept a single Flow object and wrap it
  if (S7::S7_inherits(flows, Flow_Daily) || S7::S7_inherits(flows, Flow_15min)) {
    flows <- list(flows)
  }

  if (!is.list(flows) || length(flows) == 0L) {
    stop("`flows` must be a non-empty list of Flow_Daily or Flow_15min objects.")
  }

  n <- length(flows)

  for (i in seq_len(n)) {
    if (!S7::S7_inherits(flows[[i]], Flow_Daily) &&
        !S7::S7_inherits(flows[[i]], Flow_15min)) {
      stop(sprintf(
        "`flows[[%d]]` is not a Flow_Daily or Flow_15min object.", i
      ))
    }
  }

  if (!is.null(gauge_ids) && length(gauge_ids) != n) {
    stop(sprintf(
      "`gauge_ids` has %d element(s) but `flows` has %d.", length(gauge_ids), n
    ))
  }

  if (!is.null(ied_refs) && length(ied_refs) != n) {
    stop(sprintf(
      "`ied_refs` has %d element(s) but `flows` has %d.", length(ied_refs), n
    ))
  }

  # Warn on mixed period_names
  periods <- vapply(flows, function(obj) obj@period_name, character(1L))
  if (length(unique(periods)) > 1L) {
    warning(
      "Mixed period_names detected (",
      paste(unique(periods), collapse = ", "),
      "). Merging by dateTime — gaps will appear where timesteps differ."
    )
  }

  # -- 2. Build IED references (Row 4 column headers) --------------------------

  nms <- names(flows)

  if (!is.null(ied_refs)) {
    col_refs <- ied_refs
  } else if (!is.null(gauge_ids) && !is.null(nms)) {
    col_refs <- paste0(nms, "_", gauge_ids)
  } else if (!is.null(gauge_ids)) {
    col_refs <- gauge_ids
  } else if (!is.null(nms)) {
    col_refs <- nms
  } else {
    col_refs <- paste0("site_", seq_len(n))
  }

  # -- 3. Merge all time series on dateTime ------------------------------------

  dts <- lapply(seq_len(n), function(i) {
    dt <- data.table::copy(flows[[i]]@readings)[, .(dateTime, value)]
    data.table::setnames(dt, "value", paste0("v", i))
    dt
  })

  merged <- Reduce(
    function(a, b) merge(a, b, by = "dateTime", all = TRUE),
    dts
  )
  data.table::setorder(merged, dateTime)

  # -- 4. Compute relative time in hours ---------------------------------------

  t0 <- if (!is.null(start_time)) {
    start_time
  } else {
    min(merged$dateTime, na.rm = TRUE)
  }

  rel_hours <- as.numeric(difftime(merged$dateTime, t0, units = "hours"))

  # -- 5. Apply na_fill --------------------------------------------------------

  val_cols <- paste0("v", seq_len(n))
  if (!is.na(na_fill)) {
    for (col in val_cols) {
      merged[is.na(get(col)), (col) := na_fill]
    }
  }

  # -- 6. Build CSV lines -------------------------------------------------------

  # Format NA as blank, numbers to 3 decimal places
  fmt_val <- function(x) {
    ifelse(is.na(x), "", sprintf("%.3f", x))
  }

  time_col  <- sprintf("%.3f", rel_hours)
  val_mat   <- as.matrix(merged[, ..val_cols])
  val_str   <- apply(val_mat, 2L, fmt_val)
  if (is.null(dim(val_str))) dim(val_str) <- c(length(val_str), 1L)

  row_strs <- vapply(
    seq_len(nrow(val_str)),
    function(i) paste(c(time_col[i], val_str[i, ]), collapse = ","),
    character(1L)
  )

  # Header rows — pad with n blank cells after the first entry
  blanks <- rep("", n)

  row1 <- paste(c(title,   blanks), collapse = ",")
  row2 <- paste(c(comment, blanks), collapse = ",")
  row3 <- paste(c("", if (!is.null(gauge_ids)) gauge_ids else blanks),
                collapse = ",")
  row4 <- paste(c("Time", col_refs), collapse = ",")

  lines <- c(row1, row2, row3, row4, row_strs)

  # -- 7. Output ----------------------------------------------------------------

  if (!is.null(out_file)) {
    dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
    writeLines(lines, out_file)
    return(invisible(out_file))
  }

  invisible(lines)
}

