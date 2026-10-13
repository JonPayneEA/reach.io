# -- Tests for the registry builder, router and backfill ----------------------

write_registry_csv <- function(dt) {
  tmp <- tempfile(fileext = ".csv")
  data.table::fwrite(dt, tmp)
  tmp
}

valid_registry_dt <- function(...) {
  data.table::data.table(
    gauge_id      = c("A", "B"),
    source_system = c("HDE", "WISKI"),
    data_type     = c("flow", "level"),
    category      = c("hydrometric", "hydrometric"),
    catchment     = c("Exe", "Severn"),
    ea_site_ref   = c("X1", "X2"),
    ...
  )
}


# -- build_gauge_registry() ---------------------------------------------------

test_that("build_gauge_registry errors on missing columns", {
  tmp <- write_registry_csv(
    # Missing category, data_type, catchment, ea_site_ref intentionally
    data.table::data.table(gauge_id = "A", source_system = "HDE")
  )
  expect_error(build_gauge_registry(tmp, tempdir()), "Missing required columns")
})

test_that("build_gauge_registry errors on unknown source_system", {
  dt <- valid_registry_dt()[1L]
  dt[, source_system := "UNKNOWN"]
  expect_error(build_gauge_registry(write_registry_csv(dt), tempdir()),
               "Unknown source_system")
})

test_that("build_gauge_registry writes parquet and returns data.table", {
  tmp_out <- tempfile()
  result  <- build_gauge_registry(write_registry_csv(valid_registry_dt()), tmp_out)
  expect_s3_class(result, "data.table")
  expect_equal(nrow(result), 2L)
  expect_true(file.exists(file.path(tmp_out, "gauge_registry.parquet")))
  expect_true(all(result$active))
  expect_true(all(result$live))
  expect_false(any(result$backfill_done))
})

test_that("build_gauge_registry keeps numeric-looking gauge ids as character", {
  dt <- valid_registry_dt()
  dt[, gauge_id := c("39001", "39002")]
  result <- build_gauge_registry(write_registry_csv(dt), tempfile())
  expect_type(result$gauge_id, "character")
  expect_equal(result$gauge_id, c("39001", "39002"))
})

test_that("build_gauge_registry rejects empty ids, missing data_type and duplicates", {
  dt <- valid_registry_dt()
  dt[2L, gauge_id := ""]
  expect_error(build_gauge_registry(write_registry_csv(dt), tempfile()), "gauge_id")

  dt <- valid_registry_dt()
  dt[2L, data_type := NA_character_]
  expect_error(build_gauge_registry(write_registry_csv(dt), tempfile()), "data_type")

  dt <- valid_registry_dt()
  dt[2L, `:=`(gauge_id = "A", data_type = "flow")]
  expect_error(build_gauge_registry(write_registry_csv(dt), tempfile()), "Duplicate")
})

test_that("build_gauge_registry allows a missing data_type for WISKI_ALL", {
  dt <- valid_registry_dt()
  dt[2L, `:=`(source_system = "WISKI_ALL", data_type = NA_character_)]
  expect_no_error(build_gauge_registry(write_registry_csv(dt), tempfile()))
})

test_that("build_gauge_registry merge mode keys on gauge_id + data_type", {
  out <- tempfile()
  build_gauge_registry(write_registry_csv(valid_registry_dt()), out)

  # Same gauge A, new data type (level), plus the unchanged flow row
  more <- data.table::data.table(
    gauge_id = c("A", "A"), source_system = "HDE",
    data_type = c("flow", "level"), category = "hydrometric",
    catchment = "Exe", ea_site_ref = "X1"
  )
  reg <- build_gauge_registry(write_registry_csv(more), out, overwrite = FALSE)
  expect_equal(nrow(reg), 3L)
  expect_true(all(c("flow", "level") %in% reg[gauge_id == "A", data_type]))
})


# -- route_gauge() ------------------------------------------------------------

test_that("route_gauge returns NULL and warns on unknown source_system", {
  gauge <- data.table::data.table(gauge_id = "X", source_system = "BADVALUE",
                                  data_type = "flow")
  expect_warning(
    result <- route_gauge(gauge, "2020-01-01", "2020-12-31"),
    "Route failed"
  )
  expect_null(result)
})

test_that("route_gauge(on_error = 'stop') re-raises the error", {
  gauge <- data.table::data.table(gauge_id = "X", source_system = "BADVALUE",
                                  data_type = "flow")
  expect_error(route_gauge(gauge, "2020-01-01", "2020-12-31", on_error = "stop"),
               "Route failed for X")
})

test_that("route_gauge dispatches HDE gauges to fetch_from_hde", {
  expected <- bronze_rows("39001", "2020-01-01 00:00:00")
  testthat::local_mocked_bindings(
    fetch_from_hde = function(gauge_id, data_type, start_date, end_date, ...) expected
  )
  gauge  <- data.table::data.table(gauge_id = "39001", source_system = "HDE",
                                   data_type = "flow")
  result <- route_gauge(gauge, "2020-01-01", "2020-12-31")
  expect_s3_class(result, "data.table")
  expect_true(all(c("timestamp", "value", "site_id") %in% names(result)))
})


# -- run_backfill() -----------------------------------------------------------

backfill_setup <- function(ids = c("G1", "G2"), env = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = env)
  reg  <- data.table::rbindlist(lapply(ids, reg_row))
  list(root = root,
       reg_path = make_test_registry(root, reg),
       log_path = file.path(root, "logs", "backfill.csv"),
       register = file.path(root, "register", "hydrometric_data_register.csv"))
}

test_that("run_backfill errors when failed_only = TRUE and no log exists", {
  expect_error(
    run_backfill(registry_path = tempfile(), output_dir = tempdir(),
                 start_date = "2020-01-01", end_date = "2020-12-31",
                 log_path = tempfile(), failed_only = TRUE),
    "no log file found"
  )
})

test_that("run_backfill writes Bronze, provenance, log and backfill_done", {
  s <- backfill_setup("G1")
  mock_route(bronze_rows("G1", c("2022-12-31 23:45:00", "2023-01-01 00:00:00")))

  log_dt <- suppressMessages(
    run_backfill(s$reg_path, s$root, "2022-01-01", "2024-01-01",
                 log_path = s$log_path, n_workers = 1L)
  )
  expect_equal(log_dt$status, "SUCCESS")
  expect_equal(log_dt$rows, 2L)
  expect_true(file.exists(s$log_path))

  obj <- read_bronze(s$root, "G1", "Q")
  expect_equal(nrow(obj@readings), 2L)
  expect_equal(nrow(data.table::fread(s$register)), 2L)   # one per year file

  reg <- data.table::as.data.table(arrow::read_parquet(s$reg_path))
  expect_true(reg[gauge_id == "G1", backfill_done])
})

test_that("run_backfill distinguishes FAILED from EMPTY and alerts", {
  s <- backfill_setup(c("OK", "EMPTY", "BAD"))
  testthat::local_mocked_bindings(
    route_gauge = function(gauge_row, start_date, end_date, on_error = "warn") {
      switch(gauge_row$gauge_id,
             OK    = bronze_rows("OK", "2023-01-01 00:00:00"),
             EMPTY = data.table::data.table(),
             BAD   = stop("API down"))
    }
  )
  expect_warning(
    log_dt <- suppressMessages(
      run_backfill(s$reg_path, s$root, "2023-01-01", "2024-01-01",
                   log_path = s$log_path, n_workers = 1L)
    ),
    "STEWARD ALERT: 33\\.3% of gauges failed"
  )
  expect_equal(log_dt[match(c("OK", "EMPTY", "BAD"), gauge_id), status],
               c("SUCCESS", "EMPTY", "FAILED"))
  expect_match(log_dt[gauge_id == "BAD", error], "API down")

  reg <- data.table::as.data.table(arrow::read_parquet(s$reg_path))
  expect_equal(reg[match(c("OK", "EMPTY", "BAD"), gauge_id), backfill_done],
               c(TRUE, FALSE, FALSE))
})

test_that("run_backfill failed_only re-runs failures and merges into the log", {
  s <- backfill_setup(c("G1", "G2"))
  seen    <- character()
  flaky   <- TRUE
  testthat::local_mocked_bindings(
    route_gauge = function(gauge_row, start_date, end_date, on_error = "warn") {
      seen <<- c(seen, gauge_row$gauge_id)
      if (flaky && gauge_row$gauge_id == "G2") stop("flaky")
      bronze_rows(gauge_row$gauge_id, "2023-01-01 00:00:00")
    }
  )
  suppressWarnings(suppressMessages(
    run_backfill(s$reg_path, s$root, "2023-01-01", "2024-01-01",
                 log_path = s$log_path, n_workers = 1L)
  ))
  first <- data.table::fread(s$log_path)
  expect_equal(first[order(gauge_id), status], c("SUCCESS", "FAILED"))

  seen  <- character()
  flaky <- FALSE
  log_dt <- suppressMessages(
    run_backfill(s$reg_path, s$root, "2023-01-01", "2024-01-01",
                 log_path = s$log_path, n_workers = 1L, failed_only = TRUE)
  )
  expect_equal(seen, "G2")                                  # only the failure re-ran
  expect_equal(log_dt[order(gauge_id), status], c("SUCCESS", "SUCCESS"))
  expect_equal(nrow(data.table::fread(s$log_path)), 2L)     # history kept
})

test_that("run_backfill checkpoints the log between batches", {
  s <- backfill_setup(c("G1", "G2", "G3"))
  sizes <- integer()
  testthat::local_mocked_bindings(
    route_gauge = function(gauge_row, start_date, end_date, on_error = "warn") {
      sizes <<- c(sizes, if (file.exists(s$log_path)) nrow(data.table::fread(s$log_path)) else 0L)
      bronze_rows(gauge_row$gauge_id, "2023-01-01 00:00:00")
    }
  )
  suppressMessages(
    run_backfill(s$reg_path, s$root, "2023-01-01", "2024-01-01",
                 log_path = s$log_path, n_workers = 1L, batch_size = 1L)
  )
  expect_equal(sizes, c(0L, 1L, 2L))   # log grows after every batch
})

test_that("run_backfill restores the caller's future plan", {
  s <- backfill_setup("G1")
  mock_route(data.table::data.table())
  before <- class(future::plan())
  suppressMessages(run_backfill(s$reg_path, s$root, "2023-01-01", "2024-01-01",
                                log_path = s$log_path, n_workers = 1L))
  expect_equal(class(future::plan()), before)
})
