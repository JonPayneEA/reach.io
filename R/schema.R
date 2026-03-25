# ============================================================
# Tool:        Bronze Schema, Dataset IDs, and Provenance
# Description: Centralised Bronze Parquet schema definition,
#              dataset ID generation, supplier code mapping,
#              and provenance record writing for the
#              Hydrometric Data Register.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      Ingest metadata passed by calling functions
# Outputs:     dataset_id strings; provenance record CSV rows
# Dependencies: data.table, arrow
# ============================================================

# -- Supplier code lookup -----------------------------------------------------

# Maps source_system values in the registry to the EA-standard supplier codes
# used in dataset IDs and provenance records (Appendix A of the framework).
#' @noRd
SUPPLIER_CODES <- c(
  HDE       = "EA",
  WISKI     = "WISKI",
  WISKI_ALL = "WISKI",
  NRFA      = "NRFA",
  MO        = "MO",
  CEH       = "CEH"
)

# -- Data type code lookup ----------------------------------------------------

# Maps parameter names used internally to the framework data type codes:
# Q = flow, H = stage/water level, P = rainfall, SM = soil moisture,
# SWE = snow water equivalent
#' @noRd
DATA_TYPE_CODES <- c(
  flow          = "Q",
  level         = "H",
  rainfall      = "P",
  soil_moisture = "SM",
  snow          = "SWE",
  pe            = "PE"
)


# -- Data category lookup -----------------------------------------------------

# Valid data categories recognised by the pipeline. Category is a required
# column in the gauge registry and determines the second-level directory
# under each tier in the store structure.
#' @noRd
VALID_CATEGORIES <- c("hydrometric", "radarH19", "MOSES")


# -- Dataset ID generation ----------------------------------------------------

#' Generate a Bronze dataset ID
#'
#' Produces a dataset ID in the format specified by the Hydrometric Data
#' Framework (Appendix A): `[SupplierCode]_[SiteID]_[DataType]_[YYYYMMDD]`
#' e.g. `EA_39001_Q_20260115`.
#'
#' @param supplier_code Character. Supplier code, e.g. `"EA"`, `"WISKI"`,
#'   `"MO"`. Use [source_to_supplier()] to derive from `source_system`.
#' @param site_id Character. Gauge or site identifier.
#' @param data_type Character. Framework data type code: `"Q"`, `"H"`,
#'   `"P"`, `"SM"`, or `"SWE"`. Use [param_to_data_type()] to derive from
#'   parameter name.
#' @param received_date Date or character. Date the data was received or
#'   ingested. Defaults to today.
#'
#' @return A length-1 character string.
#'
#' @export
#'
#' @examples
#' make_dataset_id("EA", "39001", "Q")
#' make_dataset_id("WISKI", "2723TH", "H", received_date = "2026-01-15")
make_dataset_id <- function(supplier_code,
                            site_id,
                            data_type,
                            received_date = Sys.Date()) {
  sprintf("%s_%s_%s_%s",
          supplier_code,
          site_id,
          data_type,
          format(as.Date(received_date), "%Y%m%d"))
}


#' Map a source_system value to a supplier code
#'
#' Converts the `source_system` column values used in the gauge registry
#' to the EA-standard supplier codes used in dataset IDs.
#'
#' @param source_system Character vector. One or more `source_system` values
#'   from the gauge registry.
#'
#' @return Character vector of supplier codes, NA for unrecognised values.
#'
#' @export
#'
#' @examples
#' source_to_supplier("HDE")     # "EA"
#' source_to_supplier("WISKI")   # "WISKI"
source_to_supplier <- function(source_system) {
  unname(SUPPLIER_CODES[source_system])
}


#' Map a parameter name to a framework data type code
#'
#' Converts parameter names used internally (`"flow"`, `"level"`,
#' `"rainfall"`) to the framework data type codes (`"Q"`, `"H"`, `"P"`).
#'
#' @param parameter Character vector. One or more parameter names.
#'
#' @return Character vector of data type codes, NA for unrecognised values.
#'
#' @export
#'
#' @examples
#' param_to_data_type("flow")     # "Q"
#' param_to_data_type("rainfall") # "P"
param_to_data_type <- function(parameter) {
  unname(DATA_TYPE_CODES[parameter])
}


# -- Bronze Parquet schema ----------------------------------------------------

#' Apply the framework Bronze Parquet schema to a data.table
#'
#' Selects and coerces columns to match the mandatory Bronze schema defined
#' in Section 7.2 of the Hydrometric Data Framework. Input columns are
#' mapped from internal names to framework names. Any column not in the
#' schema is dropped.
#'
#' Mandatory output columns:
#' \describe{
#'   \item{`timestamp`}{POSIXct UTC. Full observation datetime, including
#'     sub-daily time for instantaneous readings (15-min, hourly). For daily
#'     aggregates (mean, max, min) the time component is midnight UTC — the
#'     time-of-occurrence (e.g. time of peak flow) is not available from the
#'     EA Hydrology API readings endpoint and must be derived from the
#'     sub-daily series separately.}
#'   \item{`value`}{float64. Observed value as received.}
#'   \item{`supplier_flag`}{character. Quality code from supplier, or NA.}
#'   \item{`dataset_id`}{character. Bronze dataset ID. Join key to register.}
#'   \item{`site_id`}{character. Supplier site identifier.}
#'   \item{`data_type`}{character. Framework code: Q, H, P, SM, SWE.}
#'   \item{`period`}{character. Temporal resolution of the series, e.g.
#'     `"15min"`, `"hourly"`, `"daily"`. `NA` when unknown.}
#' }
#'
#' @param dt A `data.table` of raw readings.
#' @param dataset_id Character. The Bronze dataset ID for this dataset.
#' @param site_id Character. Supplier site identifier.
#' @param data_type Character. Framework data type code.
#' @param timestamp_col Character. Name of the datetime column in `dt`.
#'   Default `"dateTime"`.
#' @param value_col Character. Name of the value column in `dt`.
#'   Default `"value"`.
#' @param supplier_flag_col Character or NULL. Name of the supplier quality
#'   flag column in `dt`. NULL produces NA for all rows.
#' @param period Character or NA. Temporal resolution of the series, e.g.
#'   `"15min"`, `"hourly"`, `"daily"`. Stored verbatim in the `period` column.
#'   Default `NA_character_`.
#'
#' @return A `data.table` conforming to the Bronze schema.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' bronze_dt <- apply_bronze_schema(
#'   dt            = raw_dt,
#'   dataset_id    = "EA_39001_Q_20260115",
#'   site_id       = "39001",
#'   data_type     = "Q",
#'   timestamp_col = "dateTime",
#'   value_col     = "value",
#'   supplier_flag_col = "quality",
#'   period        = "15min"
#' )
#' }
apply_bronze_schema <- function(dt,
                                dataset_id,
                                site_id,
                                data_type,
                                timestamp_col     = "dateTime",
                                value_col         = "value",
                                supplier_flag_col = NULL,
                                period            = NA_character_) {

  if (!timestamp_col %in% names(dt)) {
    stop(sprintf("timestamp column '%s' not found in dt.", timestamp_col))
  }
  if (!value_col %in% names(dt)) {
    stop(sprintf("value column '%s' not found in dt.", value_col))
  }

  ts <- dt[[timestamp_col]]
  if (!inherits(ts, "POSIXct")) {
    ts <- as.POSIXct(ts, tz = "UTC")
  }

  supplier_flag <- if (!is.null(supplier_flag_col) &&
                       supplier_flag_col %in% names(dt)) {
    as.character(dt[[supplier_flag_col]])
  } else {
    NA_character_
  }

  data.table::data.table(
    timestamp     = ts,
    value         = as.double(dt[[value_col]]),
    supplier_flag = supplier_flag,
    dataset_id    = dataset_id,
    site_id       = site_id,
    data_type     = data_type,
    period        = as.character(period)
  )
}


# -- Bronze file path ---------------------------------------------------------

#' Build the Bronze Parquet file path for a dataset
#'
#' Constructs the file path following the framework folder structure:
#' `bronze/<Category>/<SupplierCode>/<DataType>/<YYYY>/<DatasetID>.parquet`
#'
#' @param output_dir Character. Root store directory.
#' @param category Character. Data category from the gauge registry, e.g.
#'   `"hydrometric"`, `"radarH19"`, `"MOSES"`.
#' @param supplier_code Character. Supplier code, e.g. `"EA"`.
#' @param data_type Character. Framework data type code.
#' @param dataset_id Character. Full Bronze dataset ID.
#'
#' @return Character. Full file path including filename.
#'
#' @export
#'
#' @examples
#' bronze_path("data/hydrometric", "hydrometric", "EA", "Q", "EA_39001_Q_20260115")
bronze_path <- function(output_dir, category, supplier_code, data_type, dataset_id) {
  year <- substr(dataset_id, nchar(dataset_id) - 7L, nchar(dataset_id) - 4L)
  file.path(output_dir, "bronze", category, supplier_code, data_type, year,
            paste0(dataset_id, ".parquet"))
}


# -- Provenance record --------------------------------------------------------

#' Write a Bronze provenance record to the Hydrometric Data Register
#'
#' Appends a provenance record row to the register CSV file for the given
#' Bronze dataset. This satisfies the Section 4.2 requirement that every
#' Bronze dataset has a complete provenance record before storage.
#'
#' The register CSV is created if it does not exist. Each row represents
#' one Bronze dataset following the field list in Section 10.1 of the
#' framework.
#'
#' @param register_path Character. Path to the register CSV file. Typically
#'   `hydrometric/register/hydrometric_data_register.csv`.
#' @param dataset_id Character. Bronze dataset ID.
#' @param supplier Character. Supplier name, e.g. `"Environment Agency"`.
#' @param supplier_code Character. Supplier code, e.g. `"EA"`.
#' @param site_id Character. Gauge or site identifier.
#' @param data_type Character. Framework data type code.
#' @param time_period_start Character. Start date of the dataset (YYYY-MM-DD).
#' @param time_period_end Character. End date of the dataset (YYYY-MM-DD).
#' @param temporal_resolution Character. Time step, e.g. `"15min"`, `"daily"`.
#' @param method_of_receipt Character. How the data was obtained, e.g.
#'   `"API pull"`, `"FTP"`, `"bulk file"`, `"WISKI .all export"`.
#' @param file_path Character. Storage path of the Bronze Parquet file.
#' @param received_by Character. Name of the Custodian who ingested the data.
#'   Defaults to `Sys.info()[["user"]]`.
#' @param units Character or NULL. Units as stated by the supplier.
#' @param supplier_quality_flags Character or NULL. Description of any quality
#'   codes provided by the supplier.
#' @param known_limitations Character or NULL. Limitations stated by supplier.
#' @param notes Character or NULL. Any unusual aspects of the delivery.
#'
#' @return The provenance record as a one-row `data.table`, invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' write_provenance_record(
#'   register_path       = "data/hydrometric/register/register.csv",
#'   dataset_id          = "EA_39001_Q_20260115",
#'   supplier            = "Environment Agency",
#'   supplier_code       = "EA",
#'   site_id             = "39001",
#'   data_type           = "Q",
#'   time_period_start   = "2020-01-01",
#'   time_period_end     = "2024-12-31",
#'   temporal_resolution = "15min",
#'   method_of_receipt   = "API pull",
#'   file_path           = "data/hydrometric/bronze/EA/Q/2026/EA_39001_Q_20260115.parquet"
#' )
#' }
write_provenance_record <- function(register_path,
                                    dataset_id,
                                    supplier,
                                    supplier_code,
                                    site_id,
                                    data_type,
                                    time_period_start,
                                    time_period_end,
                                    temporal_resolution,
                                    method_of_receipt,
                                    file_path,
                                    received_by             = Sys.info()[["user"]],
                                    units                   = NA_character_,
                                    supplier_quality_flags  = NA_character_,
                                    known_limitations       = NA_character_,
                                    notes                   = NA_character_) {

  record <- data.table::data.table(
    dataset_id              = dataset_id,
    tier                    = "Bronze",
    status                  = "Active",
    supplier                = supplier,
    supplier_code           = supplier_code,
    site_id                 = site_id,
    data_type               = data_type,
    time_period_start       = time_period_start,
    time_period_end         = time_period_end,
    temporal_resolution     = temporal_resolution,
    units                   = units,
    file_format_received    = "Parquet",
    date_received           = as.character(Sys.Date()),
    received_by             = received_by,
    method_of_receipt       = method_of_receipt,
    supplier_quality_flags  = supplier_quality_flags,
    known_limitations       = known_limitations,
    file_path               = file_path,
    notes                   = notes
  )

  dir.create(dirname(register_path), recursive = TRUE, showWarnings = FALSE)

  # Append to register if it exists, otherwise create with header
  data.table::fwrite(record, register_path,
                     append = file.exists(register_path))

  message(sprintf("  Provenance record written: %s -> %s",
                  dataset_id, register_path))
  invisible(record)
}
