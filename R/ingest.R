# ============================================================
# Tool:        Bulk File Ingestor
# Description: Reads EA bulk export files, standardises to
#              the Bronze Parquet schema, writes partitioned
#              Parquet and a provenance record.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: aligned to Bronze schema v1.3
# Tier:        1
# Inputs:      CSV/TSV/fixed-width bulk export file
# Outputs:     Bronze Parquet; provenance record row in register
# Dependencies: data.table, arrow, lubridate
# ============================================================

#' Ingest a historical bulk file to Bronze Parquet
#'
#' Reads a historical bulk export file, standardises column names, parses
#' datetimes, applies the Bronze Parquet schema (Section 7.2 of the
#' Hydrometric Data Framework), and writes to the framework folder structure.
#' A provenance record is appended to the Hydrometric Data Register.
#'
#' Output path follows Appendix C:
#' `<output_dir>/bronze/<SupplierCode>/<DataType>/<YYYY>/<DatasetID>.parquet`
#'
#' @param file_path Character. Path to the raw bulk file.
#' @param site_id Character. Gauge identifier (used in the dataset ID and
#'   the Bronze schema `site_id` column).
#' @param data_type Character. Parameter: `"flow"`, `"level"`, or
#'   `"rainfall"`.
#' @param output_dir Character. Root output directory (the `hydrometric/`
#'   root; subdirectories are created automatically).
#' @param register_path Character. Path to the Hydrometric Data Register CSV.
#' @param supplier Character. Supplier name for the provenance record,
#'   e.g. `"Environment Agency"`.
#' @param category Character. Data category from the gauge registry, e.g.
#'   `"hydrometric"`, `"radarH19"`, `"MOSES"`. Default `"hydrometric"`.
#' @param source_system Character. Registry source system value used to
#'   derive the supplier code. Default `"BULK_FILE"`.
#' @param file_format Character. One of `"csv"` (default), `"tsv"`, or
#'   `"fixed"`.
#' @param received_by Character. Custodian name for the provenance record.
#'   Defaults to `Sys.info()[["user"]]`.
#' @param notes Character or NULL. Any unusual aspects of this delivery.
#'
#' @return The Bronze `data.table` invisibly. Side effects: writes Parquet
#'   and appends a provenance record to the register.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' ingest_bulk_file(
#'   file_path     = "data/raw/EA_39001_flow_2000_2020.csv",
#'   site_id       = "39001",
#'   data_type     = "flow",
#'   output_dir    = "data/hydrometric",
#'   register_path = "data/hydrometric/register/register.csv",
#'   supplier      = "Environment Agency"
#' )
#' }
ingest_bulk_file <- function(file_path,
                             site_id,
                             data_type,
                             output_dir,
                             register_path,
                             supplier      = "Environment Agency",
                             category      = "hydrometric",
                             source_system = "BULK_FILE",
                             file_format   = c("csv", "tsv", "fixed"),
                             received_by   = Sys.info()[["user"]],
                             notes         = NA_character_) {

  file_format <- match.arg(file_format)
  message(sprintf("Ingesting bulk file: %s [%s]", file_path, file_format))

  # -- Read file ---------------------------------------------------------------
  raw_dt <- switch(file_format,
    "csv"   = data.table::fread(file_path),
    "tsv"   = data.table::fread(file_path, sep = "\t"),
    "fixed" = data.table::fread(file_path, sep = "")
  )

  # -- Standardise column names ------------------------------------------------
  name_map <- c(
    Date        = "datetime",
    date        = "datetime",
    timestamp   = "datetime",
    DateTime    = "datetime",
    Value       = "value",
    Measurement = "value",
    Quality     = "supplier_flag",
    QualityCode = "supplier_flag"
  )
  old_names <- intersect(names(name_map), names(raw_dt))
  data.table::setnames(raw_dt, old_names, name_map[old_names])

  if (!"datetime" %in% names(raw_dt)) {
    stop("Could not identify a datetime column. ",
         "Expected one of: Date, date, timestamp, DateTime.")
  }
  if (!"value" %in% names(raw_dt)) {
    stop("Could not identify a value column. ",
         "Expected one of: Value, Measurement.")
  }

  # -- Parse datetime ----------------------------------------------------------
  raw_dt[, datetime := lubridate::parse_date_time(
    datetime,
    orders = c("Ymd HMS", "dmY HM", "Ymd HM", "Ymd"),
    tz     = "UTC"
  )]
  raw_dt[, value := suppressWarnings(as.numeric(value))]

  # Drop unparseable rows
  n_in      <- nrow(raw_dt)
  raw_dt    <- raw_dt[!is.na(datetime) & !is.na(value)]
  n_dropped <- n_in - nrow(raw_dt)
  if (n_dropped > 0L) {
    message(sprintf("  Dropped %d rows with unparseable datetime or value.",
                    n_dropped))
  }

  # -- Build dataset ID and apply Bronze schema --------------------------------
  supplier_code <- source_to_supplier(source_system)
  dt_code       <- param_to_data_type(data_type)
  dataset_id    <- make_dataset_id(supplier_code, site_id, dt_code)

  bronze_dt <- apply_bronze_schema(
    dt                = raw_dt,
    dataset_id        = dataset_id,
    site_id           = site_id,
    data_type         = dt_code,
    timestamp_col     = "datetime",
    value_col         = "value",
    supplier_flag_col = if ("supplier_flag" %in% names(raw_dt))
                          "supplier_flag" else NULL
  )

  # -- Write to Bronze path ----------------------------------------------------
  out_file <- bronze_path(output_dir, category, supplier_code, dt_code, dataset_id)
  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(bronze_dt, out_file)
  message(sprintf("  Written %d rows -> %s", nrow(bronze_dt), out_file))

  # -- Provenance record -------------------------------------------------------
  write_provenance_record(
    register_path       = register_path,
    dataset_id          = dataset_id,
    supplier            = supplier,
    supplier_code       = supplier_code,
    site_id             = site_id,
    data_type           = dt_code,
    time_period_start   = as.character(min(as.Date(bronze_dt$timestamp))),
    time_period_end     = as.character(max(as.Date(bronze_dt$timestamp))),
    temporal_resolution = "unknown",
    method_of_receipt   = "bulk file",
    file_path           = out_file,
    received_by         = received_by,
    notes               = notes
  )

  invisible(bronze_dt)
}
