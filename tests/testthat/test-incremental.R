# -- Tests for run_incremental() and get_high_watermark() ---------------------

# Helper: write a minimal Bronze partition to a temp directory
make_test_partition <- function(output_dir, gauge_id, datetimes) {
  part_dir <- file.path(output_dir, paste0("gauge_id=", gauge_id))
  dir.create(part_dir, recursive = TRUE, showWarnings = FALSE)
  dt <- data.table::data.table(
    gauge_id = gauge_id,
    datetime = as.POSIXct(datetimes, tz = "UTC"),
    value    = seq_along(datetimes) * 1.0,
    unit     = "m3/s",
    flag     = 1L
  )
  arrow::write_parquet(dt, file.path(part_dir, "backfill_test.parquet"))
  invisible(dt)
}

# Helper: write a minimal registry parquet
make_test_registry <- function(output_dir, gauges_dt) {
  reg_dir <- file.path(output_dir, "registry")
  dir.create(reg_dir, recursive = TRUE, showWarnings = FALSE)
  reg_path <- file.path(reg_dir, "gauge_registry.parquet")
  arrow::write_parquet(gauges_dt, reg_path)
  reg_path
}


# -- get_high_watermark() -----------------------------------------------------

test_that("get_high_watermark returns NA when partition directory missing", {
  result <- get_high_watermark(tempdir(), "gauge_does_not_exist_xyz")
  expect_true(is.na(result))
})

test_that("get_high_watermark returns NA when partition has no parquet files", {
  tmp <- tempfile()
  part_dir <- file.path(tmp, "gauge_id=99999")
  dir.create(part_dir, recursive = TRUE)
  result <- get_high_watermark(tmp, "99999")
  expect_true(is.na(result))
})

test_that("get_high_watermark returns correct max datetime", {
  tmp <- tempfile()
  make_test_partition(tmp, "A001",
                      c("2020-01-01", "2020-06-15", "2021-03-01"))
  hwm <- get_high_watermark(tmp, "A001")
  expect_s3_class(hwm, "POSIXct")
  expect_equal(as.Date(hwm), as.Date("2021-03-01"))
})

test_that("get_high_watermark picks the max across multiple parquet files", {
  tmp <- tempfile()
  make_test_partition(tmp, "A002", c("2020-01-01", "2020-12-31"))
  # Write a second incremental file with a later date
  part_dir <- file.path(tmp, "gauge_id=A002")
  dt2 <- data.table::data.table(
    gauge_id = "A002",
    datetime = as.POSIXct("2022-06-01", tz = "UTC"),
    value    = 5.0,
    unit     = "m3/s",
    flag     = 1L
  )
  arrow::write_parquet(dt2, file.path(part_dir, "incremental_test.parquet"))
  hwm <- get_high_watermark(tmp, "A002")
  expect_equal(as.Date(hwm), as.Date("2022-06-01"))
})


# -- run_incremental() --------------------------------------------------------

test_that("run_incremental errors when registry has no active gauges for gauge_ids", {
  tmp     <- tempfile()
  reg_dt  <- data.table::data.table(
    gauge_id      = "G001",
    source_system = "HDE",
    data_type     = "flow",
    catchment     = "Exe",
    ea_site_ref   = "X1",
    active        = TRUE,
    date_added    = Sys.Date(),
    backfill_done = FALSE,
    notes         = NA_character_
  )
  reg_path <- make_test_registry(tmp, reg_dt)
  expect_error(
    run_incremental(reg_path, tmp, gauge_ids = "DOES_NOT_EXIST"),
    "No matching active gauges"
  )
})

test_that("run_incremental warns for unknown gauge_ids but continues with valid ones", {
  tmp    <- tempfile()
  reg_dt <- data.table::data.table(
    gauge_id      = "G001",
    source_system = "HDE",
    data_type     = "flow",
    catchment     = "Exe",
    ea_site_ref   = "X1",
    active        = TRUE,
    date_added    = Sys.Date(),
    backfill_done = FALSE,
    notes         = NA_character_
  )
  reg_path <- make_test_registry(tmp, reg_dt)

  # Mock route_gauge to return empty so the test doesn't hit the real API
  testthat::local_mocked_bindings(
    route_gauge = function(...) data.table::data.table(),
    .package = "reach.io"
  )
  expect_warning(
    run_incremental(reg_path, tmp,
                    gauge_ids = c("G001", "UNKNOWN"),
                    log_path  = tempfile()),
    "not found in registry"
  )
})

test_that("run_incremental returns NO_NEW_DATA when watermark is at end_date", {
  tmp    <- tempfile()
  reg_dt <- data.table::data.table(
    gauge_id      = "G002",
    source_system = "HDE",
    data_type     = "flow",
    catchment     = "Exe",
    ea_site_ref   = "X1",
    active        = TRUE,
    date_added    = Sys.Date(),
    backfill_done = FALSE,
    notes         = NA_character_
  )
  reg_path <- make_test_registry(tmp, reg_dt)

  # Partition with watermark already at or beyond end_date
  make_test_partition(tmp, "G002", "2023-12-31")

  log_path <- tempfile(fileext = ".csv")
  testthat::local_mocked_bindings(
    route_gauge = function(...) stop("should not be called"),
    .package = "reach.io"
  )
  log_dt <- run_incremental(reg_path, tmp,
                            end_date = "2023-12-31",
                            log_path = log_path,
                            n_workers = 1L)
  expect_equal(log_dt$status, "NO_NEW_DATA")
})

test_that("run_incremental logs SUCCESS and writes new parquet file", {
  tmp    <- tempfile()
  reg_dt <- data.table::data.table(
    gauge_id      = "G003",
    source_system = "HDE",
    data_type     = "flow",
    catchment     = "Exe",
    ea_site_ref   = "X1",
    active        = TRUE,
    date_added    = Sys.Date(),
    backfill_done = FALSE,
    notes         = NA_character_
  )
  reg_path <- make_test_registry(tmp, reg_dt)
  make_test_partition(tmp, "G003", "2022-12-31")

  new_data <- data.table::data.table(
    gauge_id = "G003",
    datetime = as.POSIXct("2023-06-01", tz = "UTC"),
    value    = 3.5,
    unit     = "m3/s",
    flag     = 1L
  )
  testthat::local_mocked_bindings(
    route_gauge = function(...) new_data,
    .package = "reach.io"
  )

  log_path <- tempfile(fileext = ".csv")
  log_dt   <- run_incremental(reg_path, tmp,
                              end_date  = "2023-12-31",
                              log_path  = log_path,
                              n_workers = 1L)

  expect_equal(log_dt$status, "SUCCESS")
  expect_equal(log_dt$rows,   1L)

  # A new incremental parquet file should have been written
  part_files <- list.files(file.path(tmp, "gauge_id=G003"),
                           pattern = "^incremental_")
  expect_true(length(part_files) >= 1L)
})

test_that("run_incremental uses default_start for gauges with no existing partition", {
  tmp    <- tempfile()
  reg_dt <- data.table::data.table(
    gauge_id      = "G004",
    source_system = "HDE",
    data_type     = "flow",
    catchment     = "Exe",
    ea_site_ref   = "X1",
    active        = TRUE,
    date_added    = Sys.Date(),
    backfill_done = FALSE,
    notes         = NA_character_
  )
  reg_path     <- make_test_registry(tmp, reg_dt)
  captured_args <- list()

  testthat::local_mocked_bindings(
    route_gauge = function(gauge_row, start_date, end_date) {
      captured_args <<- list(start_date = start_date, end_date = end_date)
      data.table::data.table()
    },
    .package = "reach.io"
  )

  run_incremental(reg_path, tmp,
                  end_date      = "2023-12-31",
                  default_start = "2005-01-01",
                  log_path      = tempfile(),
                  n_workers     = 1L)

  expect_equal(captured_args$start_date, "2005-01-01")
})

test_that("run_incremental appends to existing log file", {
  tmp    <- tempfile()
  reg_dt <- data.table::data.table(
    gauge_id      = "G005",
    source_system = "HDE",
    data_type     = "flow",
    catchment     = "Exe",
    ea_site_ref   = "X1",
    active        = TRUE,
    date_added    = Sys.Date(),
    backfill_done = FALSE,
    notes         = NA_character_
  )
  reg_path <- make_test_registry(tmp, reg_dt)
  log_path <- tempfile(fileext = ".csv")

  testthat::local_mocked_bindings(
    route_gauge = function(...) data.table::data.table(),
    .package = "reach.io"
  )

  # Run twice
  run_incremental(reg_path, tmp, log_path = log_path, n_workers = 1L)
  run_incremental(reg_path, tmp, log_path = log_path, n_workers = 1L)

  log_all <- data.table::fread(log_path)
  # Two runs, one gauge each = two rows
  expect_equal(nrow(log_all), 2L)
})

test_that("run_incremental log has expected columns", {
  tmp    <- tempfile()
  reg_dt <- data.table::data.table(
    gauge_id      = "G006",
    source_system = "HDE",
    data_type     = "flow",
    catchment     = "Exe",
    ea_site_ref   = "X1",
    active        = TRUE,
    date_added    = Sys.Date(),
    backfill_done = FALSE,
    notes         = NA_character_
  )
  reg_path <- make_test_registry(tmp, reg_dt)

  testthat::local_mocked_bindings(
    route_gauge = function(...) data.table::data.table(),
    .package = "reach.io"
  )

  log_dt <- run_incremental(reg_path, tmp,
                            log_path  = tempfile(),
                            n_workers = 1L)

  expected_cols <- c("run_at", "gauge_id", "watermark",
                     "end_date", "status", "rows", "elapsed_s", "error")
  expect_true(all(expected_cols %in% names(log_dt)))
})

test_that("run_incremental skips inactive gauges", {
  tmp    <- tempfile()
  reg_dt <- data.table::data.table(
    gauge_id      = c("G007", "G008"),
    source_system = c("HDE", "HDE"),
    data_type     = c("flow", "flow"),
    catchment     = c("Exe", "Exe"),
    ea_site_ref   = c("X1", "X2"),
    active        = c(TRUE, FALSE),
    date_added    = Sys.Date(),
    backfill_done = FALSE,
    notes         = NA_character_
  )
  reg_path <- make_test_registry(tmp, reg_dt)

  testthat::local_mocked_bindings(
    route_gauge = function(...) data.table::data.table(),
    .package = "reach.io"
  )

  log_dt <- run_incremental(reg_path, tmp,
                            log_path  = tempfile(),
                            n_workers = 1L)
  # Only G007 (active) should appear in the log
  expect_equal(nrow(log_dt), 1L)
  expect_equal(log_dt$gauge_id, "G007")
})
