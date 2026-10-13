# Shared helpers for Bronze / orchestration tests (auto-sourced by testthat)

bronze_fixture <- function(ts, value = seq_along(ts), site = "S1", type = "P",
                           supplier = "EA", received = "2026-01-15",
                           trace = NULL, period = "15min") {
  ts <- as.POSIXct(ts, tz = "UTC")
  dataset_id <- make_dataset_id(supplier, site, type,
                                received_date = received, trace = trace)
  apply_bronze_schema(
    data.table::data.table(dateTime = ts, value = value),
    dataset_id = dataset_id, site_id = site, data_type = type, period = period
  )
}

write_fixture <- function(root, dt, supplier = "EA", type = "P") {
  .write_bronze_by_year(dt, root, "hydrometric", supplier, type,
                        dt$dataset_id[1L])
}


# Helper: write a minimal registry parquet under <root>/register/
make_test_registry <- function(root, gauges_dt) {
  reg_dir <- file.path(root, "register")
  dir.create(reg_dir, recursive = TRUE, showWarnings = FALSE)
  reg_path <- file.path(reg_dir, "gauge_registry.parquet")
  arrow::write_parquet(gauges_dt, reg_path)
  reg_path
}

reg_row <- function(id = "G001", data_type = "flow", source_system = "HDE",
                    active = TRUE, live = TRUE) {
  data.table::data.table(
    gauge_id = id, source_system = source_system, data_type = data_type,
    category = "hydrometric", catchment = "Exe", ea_site_ref = "X1",
    active = active, live = live, date_added = Sys.Date(),
    backfill_done = FALSE, notes = NA_character_
  )
}

# Helper: a Bronze-schema table, as route_gauge() returns it
bronze_rows <- function(id, ts, value = seq_along(ts) * 1.0, type = "Q",
                        supplier = "EA", received = Sys.Date()) {
  apply_bronze_schema(
    data.table::data.table(dateTime = as.POSIXct(ts, tz = "UTC"), value = value),
    dataset_id = make_dataset_id(supplier, id, type, received_date = received),
    site_id = id, data_type = type, period = "15min"
  )
}

# Helper: seed the Bronze store with existing data for a gauge
seed_bronze <- function(root, id, ts, type = "Q", supplier = "EA",
                        received = "2025-01-01") {
  dt <- bronze_rows(id, ts, type = type, supplier = supplier, received = received)
  .write_bronze_by_year(dt, root, "hydrometric", supplier, type, dt$dataset_id[1L])
  invisible(dt)
}

# Helper: mock route_gauge, recording the dates it was asked for
mock_route <- function(result, env = parent.frame()) {
  calls <- new.env()
  calls$n <- 0L
  testthat::local_mocked_bindings(
    route_gauge = function(gauge_row, start_date, end_date, on_error = "warn") {
      calls$n          <- calls$n + 1L
      calls$start_date <- start_date
      calls$end_date   <- end_date
      calls$on_error   <- on_error
      if (is.function(result)) result() else result
    },
    .package = "reach.io",
    .env = env
  )
  calls
}
