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
#' Reads a raw gauge list CSV, validates it, adds pipeline metadata columns,
#' and writes the result as a Parquet file that every other pipeline tool reads
#' from. Must be run before any backfill starts.
#'
#' By default (`overwrite = TRUE`) the existing registry is replaced. Set
#' `overwrite = FALSE` to merge new gauges into an existing registry: gauges
#' already present (matched on `gauge_id`) are left untouched, and only new
#' rows are appended. This lets you update the gauge list CSV without losing
#' `backfill_done` status or other metadata already written for existing gauges.
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
#' @param overwrite Logical. If `TRUE` (default), replace any existing
#'   registry. If `FALSE`, merge new gauges into the existing registry,
#'   preserving metadata for gauges already registered.
#'
#' @return The registry `data.table` invisibly (full merged registry when
#'   `overwrite = FALSE`). The primary side-effect is writing
#'   `<output_path>/gauge_registry.parquet` to disk.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # First-time build
#' registry_dt <- build_gauge_registry(
#'   input_csv   = "data/ea_gauge_list.csv",
#'   output_path = "data/fw_bronze/gauge_registry"
#' )
#'
#' # Add new gauges without overwriting existing metadata
#' registry_dt <- build_gauge_registry(
#'   input_csv   = "data/ea_gauge_list_v2.csv",
#'   output_path = "data/fw_bronze/gauge_registry",
#'   overwrite   = FALSE
#' )
#' }
build_gauge_registry <- function(input_csv, output_path, overwrite = TRUE) {

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
  raw_dt[, live          := TRUE]
  raw_dt[, date_added    := Sys.Date()]
  raw_dt[, backfill_done := FALSE]
  raw_dt[, notes         := NA_character_]

  # Select and order columns explicitly so the schema is stable regardless
  # of what extra columns the input CSV contains
  new_dt <- raw_dt[, .(gauge_id, source_system, data_type, category,
                        catchment, ea_site_ref,
                        active, live, date_added, backfill_done, notes)]

  # -- Merge with existing registry if overwrite = FALSE ----------------------
  registry_path <- file.path(output_path, "gauge_registry.parquet")

  if (!overwrite && file.exists(registry_path)) {
    existing_dt <- data.table::as.data.table(arrow::read_parquet(registry_path))

    # Ensure existing registry has a `live` column (backward compatibility)
    if (!"live" %in% names(existing_dt)) {
      existing_dt[, live := TRUE]
    }

    # Add only gauges not already in the registry
    new_ids   <- new_dt$gauge_id[!new_dt$gauge_id %in% existing_dt$gauge_id]
    added_dt  <- new_dt[gauge_id %in% new_ids]
    registry_dt <- data.table::rbindlist(
      list(existing_dt, added_dt),
      use.names = TRUE, fill = TRUE
    )
    message(sprintf(
      "Merge mode: %d new gauge(s) added, %d existing preserved -> %s",
      nrow(added_dt), nrow(existing_dt), output_path
    ))
  } else {
    registry_dt <- new_dt
    message(sprintf("Registry written: %d gauges -> %s",
                    nrow(registry_dt), output_path))
  }

  # -- Write as Parquet (Delta-compatible) ------------------------------------
  # Write to a temp file then rename so that on Windows the read_parquet()
  # memory-map above is fully released before we touch the target path.
  # file.rename() is atomic on both Windows and POSIX.
  dir.create(output_path, recursive = TRUE, showWarnings = FALSE)
  tmp_path <- paste0(registry_path, ".tmp")
  arrow::write_parquet(registry_dt, tmp_path)
  file.rename(tmp_path, registry_path)

  invisible(registry_dt)
}
