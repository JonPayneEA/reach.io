# ============================================================
# Tool:        Gauge Registry Builder
# Description: One-off setup tool. Reads the raw EA gauge
#              list CSV, validates it, adds pipeline metadata
#              columns, and writes the registry Parquet.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      Raw gauge list CSV from EA
# Outputs:     gauge_registry.parquet
# Dependencies: data.table, arrow
# ============================================================

# -- Gauge Registry Builder ---------------------------------------------------

#' Build the master gauge registry Parquet table
#'
#' One-off setup tool that reads a raw gauge list CSV supplied by the EA,
#' validates it, adds pipeline metadata columns, and writes the result as a
#' Parquet file that every other pipeline tool reads from. Must be run before
#' any backfill starts.
#'
#' In a live Databricks environment the `write_parquet()` call can be replaced
#' with a Delta write via `sparklyr` or the Databricks REST API without
#' changing any other logic.
#'
#' @param input_csv Character. Path to the raw gauge list CSV. Must contain
#'   columns: `gauge_id`, `source_system`, `data_type`, `category`,
#'   `catchment`, `ea_site_ref`.
#'   `category` must be one of `"hydrometric"`, `"radarH19"`, `"MOSES"`.
#' @param output_path Character. Directory to write the registry Parquet file
#'   into. Created if it does not exist. The file is written as
#'   `gauge_registry.parquet` inside this directory.
#'
#' @return The registry `data.table` invisibly. The primary side-effect is
#'   writing `<output_path>/gauge_registry.parquet` to disk.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' registry_dt <- build_gauge_registry(
#'   input_csv   = "data/ea_gauge_list.csv",
#'   output_path = "data/fw_bronze/gauge_registry"
#' )
#' }
build_gauge_registry <- function(input_csv, output_path) {

  # -- Read raw gauge list supplied by EA -------------------------------------
  raw_dt <- data.table::fread(input_csv)

  # -- Validate required columns are present ----------------------------------
  required_cols <- c("gauge_id", "source_system", "data_type", "category",
                     "catchment", "ea_site_ref")
  missing <- setdiff(required_cols, names(raw_dt))
  if (length(missing) > 0) {
    stop("Missing required columns: ", paste(missing, collapse = ", "))
  }

  # -- Validate source_system values ------------------------------------------
  # Any value not in VALID_SOURCES will cause the router to fail later, so
  # catch bad values here at registry build time
  bad_sources <- unique(raw_dt$source_system[
    !raw_dt$source_system %in% VALID_SOURCES
  ])
  if (length(bad_sources) > 0) {
    stop(
      "Unknown source_system values: ", paste(bad_sources, collapse = ", "),
      "\nValid values: ",               paste(VALID_SOURCES, collapse = ", ")
    )
  }

  # -- Validate category values ------------------------------------------------
  bad_cats <- unique(raw_dt$category[!raw_dt$category %in% VALID_CATEGORIES])
  if (length(bad_cats) > 0) {
    stop(
      "Unknown category values: ", paste(bad_cats, collapse = ", "),
      "\nValid values: ",          paste(VALID_CATEGORIES, collapse = ", ")
    )
  }

  # -- Add registry metadata columns (modify in place) ------------------------
  raw_dt[, active        := TRUE]
  raw_dt[, date_added    := Sys.Date()]
  raw_dt[, backfill_done := FALSE]
  raw_dt[, notes         := NA_character_]

  # Select and order columns explicitly so the schema is stable regardless
  # of what extra columns the input CSV contains
  registry_dt <- raw_dt[, .(gauge_id, source_system, data_type, category,
                             catchment, ea_site_ref,
                             active, date_added, backfill_done, notes)]

  # -- Write as Parquet (Delta-compatible) ------------------------------------
  dir.create(output_path, recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(registry_dt,
                       file.path(output_path, "gauge_registry.parquet"))

  message(sprintf("Registry written: %d gauges -> %s",
                  nrow(registry_dt), output_path))
  invisible(registry_dt)
}
