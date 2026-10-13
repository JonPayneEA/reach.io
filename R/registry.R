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
#' @details
#' ## Validation and defaults
#'
#' Checks run in this order and the first failure stops the function, before
#' anything is written, so a bad CSV can never replace a good registry:
#' required columns, empty `gauge_id`, missing `data_type`, duplicate keys,
#' unknown `source_system` (see `VALID_SOURCES`), unknown `category`. Both
#' `gauge_id` and `data_type` are read as character and trimmed, so numeric IDs
#' such as `39001` keep their identity; an empty `data_type` becomes `NA`.
#'
#' Pipeline columns are added with fixed defaults: `active = TRUE`,
#' `live = TRUE`, `backfill_done = FALSE`, `notes = NA` and `date_added` set
#' to today. Any other columns in the CSV are dropped, so the stored schema
#' does not depend on the input file. [run_backfill()] and [run_incremental()]
#' only process rows with `active == TRUE`, and [run_incremental()] also
#' skips `live == FALSE`. These flags cannot be set from the CSV (any such
#' columns are overwritten); change them in the Parquet file.
#'
#' A `WISKI_ALL` row may have no `data_type`, but the orchestrators need a
#' concrete type to fetch a series and log such a row as `FAILED`.
#'
#' ## Overwrite versus merge
#'
#' With `overwrite = TRUE` the registry is rebuilt from the CSV, so existing
#' `backfill_done`, `active`, `live` and `notes` values are lost. With
#' `overwrite = FALSE` only series not yet present are appended: attributes of
#' an existing series (for example a changed `catchment`) are *not* updated
#' from the CSV, and series missing from the CSV are not removed. If no
#' registry exists yet, `overwrite = FALSE` behaves like a first build. Rows
#' that [ingest_all_file()] adds (when given a `registry_path`) use the same
#' `gauge_id` + `data_type` key.
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
  # Normalise both key columns first so every later check, and the merge key,
  # sees the same strings. data_type is coerced too because a column that is
  # entirely empty (all WISKI_ALL rows) is read as logical NA, and empty
  # strings are made NA so "missing" has a single representation.
  raw_dt[, gauge_id := trimws(as.character(gauge_id))]
  raw_dt[, data_type := trimws(as.character(data_type))]
  raw_dt[!nzchar(data_type), data_type := NA_character_]

  # Report row numbers (first 10) so the offending CSV lines are easy to find
  bad_id <- is.na(raw_dt$gauge_id) | !nzchar(raw_dt$gauge_id)
  if (any(bad_id)) {
    stop(sprintf("`gauge_id` is missing or empty in %d row(s) (rows: %s).",
                 sum(bad_id), paste(utils::head(which(bad_id), 10L), collapse = ", ")))
  }

  # -- Validate data_type: required except for WISKI_ALL ----------------------
  # WISKI_ALL files hold several parameters, so the type is optional for that
  # source only; every other source is fetched per series and needs it.
  bad_type <- is.na(raw_dt$data_type) & raw_dt$source_system != "WISKI_ALL"
  if (any(bad_type)) {
    stop(sprintf("`data_type` is missing for %d non-WISKI_ALL row(s): %s",
                 sum(bad_type),
                 paste(utils::head(raw_dt$gauge_id[bad_type], 10L), collapse = ", ")))
  }

  # -- Validate uniqueness of (gauge_id, data_type) ----------------------------
  # Same gauge with different types is fine (flow + level); the same pair
  # twice is not. An NA type keys as "" (see .registry_key()), so a repeated
  # NA-type gauge also counts as a duplicate.
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
  # New rows always start active, live and not yet backfilled. In merge mode
  # these defaults only reach genuinely new series (see below).
  raw_dt[, active        := TRUE]
  raw_dt[, live          := TRUE]
  raw_dt[, date_added    := Sys.Date()]
  raw_dt[, backfill_done := FALSE]
  raw_dt[, notes         := NA_character_]

  # Select and order columns explicitly so the schema is stable regardless
  # of what extra columns the input CSV contains (extras are dropped here)
  new_dt <- raw_dt[, .(gauge_id, source_system, data_type, category,
                        catchment, ea_site_ref,
                        active, live, date_added, backfill_done, notes)]

  # -- Merge with existing registry if overwrite = FALSE ----------------------
  # Everything validated above is in memory only; nothing has touched the
  # existing registry file yet. When overwrite = FALSE but no file exists, the
  # else branch below makes this a plain first build.
  registry_path <- file.path(output_path, "gauge_registry.parquet")

  if (!overwrite && file.exists(registry_path)) {
    existing_dt <- data.table::as.data.table(arrow::read_parquet(registry_path))

    # Ensure existing registry has a `live` column (backward compatibility)
    if (!"live" %in% names(existing_dt)) {
      existing_dt[, live := TRUE]
    }

    # Add only series (gauge_id + data_type) not already in the registry.
    # Existing rows are kept exactly as stored, so backfill_done, active, live
    # and notes survive; their other fields are not refreshed from the CSV,
    # and series absent from the CSV are not removed.
    is_new    <- !.registry_key(new_dt$gauge_id, new_dt$data_type) %in%
                 .registry_key(existing_dt$gauge_id, existing_dt$data_type)
    added_dt  <- new_dt[is_new]
    # fill = TRUE: a column found on only one side (e.g. extra columns in an
    # older registry) becomes NA for the other rows instead of erroring
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
