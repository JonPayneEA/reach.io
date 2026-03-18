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
#' Internal helper that either writes a `data.table` of readings to a CSV
#' on disk or returns it unchanged for in-memory use, depending on the
#' `output` mode. When appending chunks to an existing file, the header is
#' suppressed so the CSV has exactly one header row.
#'
#' Uses `data.table::fwrite()` rather than `write.csv()` for substantially
#' faster writes on large time series files.
#'
#' @param dt A `data.table` of readings.
#' @param output Character. `"memory"` or `"disk"`.
#' @param out_dir Character or `NULL`. Root directory (disk mode only).
#' @param parameter Character. Parameter type; names the subdirectory.
#' @param measure_id Character. Measure notation; used to construct the
#'   filename. Non-alphanumeric characters are replaced with underscores.
#' @param append Logical. Append to an existing file? Default `FALSE`.
#'   Used when writing annual chunks so the full date range ends up in one
#'   file.
#'
#' @return In `"memory"` mode, the `data.table` unchanged. In `"disk"` mode,
#'   the file path as a character string.
#'
#' @noRd
handle_output <- function(dt, output, out_dir, parameter, measure_id,
                           append = FALSE) {

  if (output == "memory") return(dt)

  param_dir <- file.path(out_dir, parameter)
  dir.create(param_dir, showWarnings = FALSE, recursive = TRUE)

  # Sanitise measure ID for use as a filename — replace anything that isn't
  # alphanumeric, underscore, or hyphen with an underscore
  safe_name <- gsub("[^A-Za-z0-9_-]", "_", measure_id)
  dest      <- file.path(param_dir, paste0(safe_name, ".csv"))

  # fwrite() is substantially faster than write.csv() for large time series;
  # append = TRUE suppresses the header on subsequent chunk writes
  data.table::fwrite(dt, dest, append = append)

  dest
}
