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
#' A registry row is one *series*: the key is `gauge_id` + `data_type`, so a
#' gauge that records both flow and level has two rows. The input is validated
#' before anything is written: `gauge_id` must be non-empty, `data_type` must be
#' present for every source except `"WISKI_ALL"` (whose files hold several
#' parameters), and each key may appear only once.
#'
#' By default (`overwrite = TRUE`) the existing registry is replaced. Set
#' `overwrite = FALSE` to merge new series into an existing registry: series
#' already present (matched on `gauge_id` + `data_type`) are left untouched,
#' and only new rows are appended. This lets you update the gauge list CSV
#' without losing `backfill_done` status or other metadata already written for
#' existing series.
#'
#' @section Windows file-locking:
#' Arrow memory-maps Parquet files on Windows when reading, keeping a file
#' handle open. Writing back to the same path while that map is live causes
#' **Windows error 1224** ("The requested operation cannot be performed on a
#' file with a user-mapped section open"). This function writes to a temporary
#' `.tmp` file and then replaces the target (renaming, or copying if the rename
#' is refused). If the target still cannot be replaced the function **stops
#' with an error** rather than silently keeping the old registry — check that
#' no other process (e.g. an open R session or file explorer preview) is
#' holding the registry file open.
#'
#' @param input_csv Character. Path to the raw gauge list CSV. Must contain
#'   columns: `gauge_id`, `source_system`, `data_type`, `category`,
#'   `catchment`, `ea_site_ref`.
#'   `category` must be one of `"hydrometric"`, `"radarH19"`, `"MOSES"`.
#' @param output_path Character. Directory to write the registry Parquet file
#'   into. Created if it does not exist. The file is written as
#'   `gauge_registry.parquet` inside this directory.
#' @param overwrite Logical. If `TRUE` (default), replace any existing
#'   registry. If `FALSE`, merge new series into the existing registry,
#'   preserving metadata for series already registered.
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

  # gauge_id is an identifier, never a number: fread() reads "39001" as integer
  raw_dt[, gauge_id := trimws(as.character(gauge_id))]
  raw_dt[, data_type := trimws(as.character(data_type))]
  raw_dt[!nzchar(data_type), data_type := NA_character_]

  bad_id <- is.na(raw_dt$gauge_id) | !nzchar(raw_dt$gauge_id)
  if (any(bad_id)) {
    stop(sprintf("`gauge_id` is missing or empty in %d row(s) (rows: %s).",
                 sum(bad_id), paste(utils::head(which(bad_id), 10L), collapse = ", ")))
  }

  # -- Validate data_type: required except for WISKI_ALL ----------------------
  bad_type <- is.na(raw_dt$data_type) & raw_dt$source_system != "WISKI_ALL"
  if (any(bad_type)) {
    stop(sprintf("`data_type` is missing for %d non-WISKI_ALL row(s): %s",
                 sum(bad_type),
                 paste(utils::head(raw_dt$gauge_id[bad_type], 10L), collapse = ", ")))
  }

  # -- Validate uniqueness of (gauge_id, data_type) ----------------------------
  dup <- duplicated(.registry_key(raw_dt$gauge_id, raw_dt$data_type))
  if (any(dup)) {
    stop(sprintf("Duplicate gauge_id + data_type combination(s): %s",
                 paste(unique(.registry_key(raw_dt$gauge_id, raw_dt$data_type)[dup]),
                       collapse = ", ")))
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

    # Add only series (gauge_id + data_type) not already in the registry
    is_new    <- !.registry_key(new_dt$gauge_id, new_dt$data_type) %in%
                 .registry_key(existing_dt$gauge_id, existing_dt$data_type)
    added_dt  <- new_dt[is_new]
    registry_dt <- data.table::rbindlist(
      list(existing_dt, added_dt),
      use.names = TRUE, fill = TRUE
    )
    message(sprintf(
      "Merge mode: %d new series added, %d existing preserved -> %s",
      nrow(added_dt), nrow(existing_dt), output_path
    ))
  } else {
    registry_dt <- new_dt
    message(sprintf("Registry written: %d series -> %s",
                    nrow(registry_dt), output_path))
  }

  # -- Write as Parquet -------------------------------------------------------
  # Atomic write (temp file then replace); errors if the target is locked
  .write_parquet_atomic(registry_dt, registry_path)

  invisible(registry_dt)
}
