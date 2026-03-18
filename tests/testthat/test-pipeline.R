test_that("build_gauge_registry errors on missing columns", {
  tmp <- tempfile(fileext = ".csv")
  data.table::fwrite(
    data.table::data.table(gauge_id = "A", source_system = "HDE"),
    # Missing category, data_type, catchment, ea_site_ref intentionally
    tmp
  )
  expect_error(build_gauge_registry(tmp, tempdir()),
               "Missing required columns")
})

test_that("build_gauge_registry errors on unknown source_system", {
  tmp <- tempfile(fileext = ".csv")
  data.table::fwrite(
    data.table::data.table(
      gauge_id      = "A",
      source_system = "UNKNOWN",
      data_type     = "flow",
      catchment     = "Test",
      ea_site_ref   = "X1"
    ),
    tmp
  )
  expect_error(build_gauge_registry(tmp, tempdir()),
               "Unknown source_system")
})

test_that("build_gauge_registry writes parquet and returns data.table", {
  tmp_csv <- tempfile(fileext = ".csv")
  tmp_out <- tempfile()
  data.table::fwrite(
    data.table::data.table(
      gauge_id      = c("A", "B"),
      source_system = c("HDE", "WISKI"),
      data_type     = c("flow", "level"),
      category      = c("hydrometric", "hydrometric"),
      catchment     = c("Exe", "Severn"),
      ea_site_ref   = c("X1", "X2")
    ),
    tmp_csv
  )
  result <- build_gauge_registry(tmp_csv, tmp_out)
  expect_s3_class(result, "data.table")
  expect_equal(nrow(result), 2L)
  expect_true(file.exists(file.path(tmp_out, "gauge_registry.parquet")))
  expect_true(all(result$active))
  expect_false(any(result$backfill_done))
})

test_that("route_gauge returns NULL and warns on unknown source_system", {
  gauge <- data.table::data.table(
    gauge_id      = "X",
    source_system = "BADVALUE",
    data_type     = "flow"
  )
  expect_warning(
    result <- route_gauge(gauge, "2020-01-01", "2020-12-31"),
    "Route failed"
  )
  expect_null(result)
})

test_that("route_gauge dispatches to HDE stub without error", {
  gauge <- data.table::data.table(
    gauge_id      = "39001",
    source_system = "HDE",
    data_type     = "flow"
  )
  result <- route_gauge(gauge, "2020-01-01", "2020-12-31")
  expect_s3_class(result, "data.table")
  expect_true("gauge_id" %in% names(result))
})

test_that("ingest_bulk_file errors when datetime column missing", {
  tmp <- tempfile(fileext = ".csv")
  data.table::fwrite(
    data.table::data.table(value = 1.5, flag = 1L),
    tmp
  )
  expect_error(
    ingest_bulk_file(tmp, "39001", tempfile()),
    "datetime column"
  )
})

test_that("ingest_bulk_file errors when value column missing", {
  tmp <- tempfile(fileext = ".csv")
  data.table::fwrite(
    data.table::data.table(Date = "2020-01-01"),
    tmp
  )
  expect_error(
    ingest_bulk_file(tmp, "39001", tempfile()),
    "value column"
  )
})

test_that("ingest_bulk_file writes parquet and returns data.table", {
  tmp_csv <- tempfile(fileext = ".csv")
  tmp_out <- tempfile()
  data.table::fwrite(
    data.table::data.table(
      Date  = c("2020-01-01", "2020-01-02"),
      Value = c(1.1, 2.2)
    ),
    tmp_csv
  )
  result <- ingest_bulk_file(tmp_csv, "39001", tmp_out)
  expect_s3_class(result, "data.table")
  expect_true("gauge_id"    %in% names(result))
  expect_true("datetime"    %in% names(result))
  expect_true("value"       %in% names(result))
  expect_true("source"      %in% names(result))
  expect_true("ingest_date" %in% names(result))
  parquet_path <- file.path(tmp_out, "gauge_id=39001")
  expect_true(dir.exists(parquet_path))
})

test_that("run_backfill errors when failed_only = TRUE and no log exists", {
  expect_error(
    run_backfill(
      registry_path = tempfile(),
      output_dir    = tempdir(),
      start_date    = "2020-01-01",
      end_date      = "2020-12-31",
      log_path      = tempfile(),   # does not exist
      failed_only   = TRUE
    ),
    "no log file found"
  )
})
