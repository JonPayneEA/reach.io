# -- Bulk File Ingestor -------------------------------------------------------

#' Ingest a historical bulk file to partitioned Bronze Parquet
#'
#' Reads a historical bulk export file from an FTP drop or local path,
#' standardises column names to the pipeline schema, parses and cleans the
#' data, and writes the result as a partitioned Parquet file in the Bronze
#' storage tier. Handles multiple file formats and the varying column naming
#' conventions used in EA bulk exports.
#'
#' Output is partitioned by `gauge_id` using Hive-style directory naming
#' (`gauge_id=<id>/`) for compatibility with both Arrow/DuckDB and Spark
#' partition discovery.
#'
#' @param file_path Character. Path to the raw bulk file on disk or a
#'   mounted FTP path.
#' @param gauge_id Character. Gauge identifier to tag all rows with. Used
#'   as the partition key in the output directory structure.
#' @param output_dir Character. Root Bronze output directory. A subdirectory
#'   `gauge_id=<gauge_id>/` is created inside this path.
#' @param file_format Character. One of `"csv"` (default), `"tsv"`, or
#'   `"fixed"` (fixed-width). `data.table::fread()` handles all three.
#'
#' @return The processed `data.table` invisibly. The primary side-effect is
#'   writing a Parquet file to
#'   `<output_dir>/gauge_id=<gauge_id>/bulk_<YYYYMMDD>.parquet`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' ingest_bulk_file(
#'   file_path   = "data/raw/EA_39001_flow_2000_2020.csv",
#'   gauge_id    = "39001",
#'   output_dir  = "data/fw_bronze/flow",
#'   file_format = "csv"
#' )
#' }
ingest_bulk_file <- function(file_path,
                             gauge_id,
                             output_dir,
                             file_format = c("csv", "tsv", "fixed")) {

  file_format <- match.arg(file_format)
  message(sprintf("Ingesting bulk file: %s [%s]", file_path, file_format))

  # -- Read file by format ----------------------------------------------------
  # fread() handles csv and tsv natively; fixed-width is handled via sep = ""
  # which causes fread to treat each character as a potential separator
  raw_dt <- switch(file_format,
    "csv"   = data.table::fread(file_path),
    "tsv"   = data.table::fread(file_path, sep = "\t"),
    "fixed" = data.table::fread(file_path, sep = "")
  )

  # -- Standardise column names to pipeline schema ----------------------------
  # EA bulk exports use varying names across data types and time periods;
  # map the most common variants to the canonical names used downstream
  name_map <- c(
    Date        = "datetime",
    date        = "datetime",
    timestamp   = "datetime",
    DateTime    = "datetime",
    Value       = "value",
    Measurement = "value",
    Quality     = "flag",
    QualityCode = "flag"
  )
  old_names <- intersect(names(name_map), names(raw_dt))
  data.table::setnames(raw_dt, old_names, name_map[old_names])

  # -- Enforce required columns -----------------------------------------------
  if (!"datetime" %in% names(raw_dt)) {
    stop("Could not identify a datetime column. ",
         "Expected one of: Date, date, timestamp, DateTime.")
  }
  if (!"value" %in% names(raw_dt)) {
    stop("Could not identify a value column. ",
         "Expected one of: Value, Measurement.")
  }

  # -- Parse and clean (modify in place) --------------------------------------
  raw_dt[, gauge_id    := gauge_id]

  # parse_date_time() tries multiple format strings in order, which handles
  # the mix of date formats found in EA historical exports
  raw_dt[, datetime    := lubridate::parse_date_time(
                            datetime,
                            orders = c("Ymd HMS", "dmY HM", "Ymd HM", "Ymd"),
                            tz     = "UTC")]

  raw_dt[, value       := suppressWarnings(as.numeric(value))]

  # Use flag column if present, otherwise default to 1 (unchecked/present)
  raw_dt[, flag        := if ("flag" %in% names(raw_dt)) {
                            as.integer(flag)
                          } else {
                            1L
                          }]

  raw_dt[, source      := "BULK_FILE"]
  raw_dt[, ingest_date := Sys.Date()]

  # Drop rows where parsing failed — these are typically header artefacts
  # or rows with placeholder missing-value codes
  processed_dt <- raw_dt[
    !is.na(datetime) & !is.na(value),
    .(gauge_id, datetime, value, flag, source, ingest_date)
  ]

  n_dropped <- nrow(raw_dt) - nrow(processed_dt)
  if (n_dropped > 0) {
    message(sprintf("  Dropped %d rows with unparseable datetime or value.",
                    n_dropped))
  }

  # -- Write to partitioned Bronze Parquet ------------------------------------
  # Hive-style partitioning (gauge_id=<id>/) is compatible with both
  # Arrow open_dataset() and Spark partition discovery
  part_dir <- file.path(output_dir, paste0("gauge_id=", gauge_id))
  dir.create(part_dir, recursive = TRUE, showWarnings = FALSE)

  out_file <- file.path(
    part_dir,
    paste0("bulk_", format(Sys.Date(), "%Y%m%d"), ".parquet")
  )
  arrow::write_parquet(processed_dt, out_file)

  message(sprintf("  Written %d rows -> %s", nrow(processed_dt), out_file))
  invisible(processed_dt)
}
