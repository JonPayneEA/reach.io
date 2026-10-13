# -- Tests for run_incremental() and get_high_watermark() ---------------------

# -- get_high_watermark() -----------------------------------------------------

test_that("get_high_watermark returns NA when the gauge has no Bronze data", {
  root   <- withr::local_tempdir()
  result <- get_high_watermark(root, "nope", "Q")
  expect_s3_class(result, "POSIXct")
  expect_true(is.na(result))
})

test_that("get_high_watermark returns the latest timestamp", {
  root <- withr::local_tempdir()
  seed_bronze(root, "A001", c("2020-05-01 00:00:00", "2021-03-01 06:15:00"))
  hwm <- get_high_watermark(root, "A001", "Q")
  expect_s3_class(hwm, "POSIXct")
  expect_equal(hwm, as.POSIXct("2021-03-01 06:15:00", tz = "UTC"))
})

test_that("get_high_watermark takes the max across files in the newest year", {
  root <- withr::local_tempdir()
  seed_bronze(root, "A002", "2022-06-01 00:00:00", received = "2025-01-01")
  seed_bronze(root, "A002", "2022-09-30 12:00:00", received = "2025-06-01")
  hwm <- get_high_watermark(root, "A002", "Q")
  expect_equal(hwm, as.POSIXct("2022-09-30 12:00:00", tz = "UTC"))
})

test_that("get_high_watermark does not confuse similarly named gauges", {
  root <- withr::local_tempdir()
  seed_bronze(root, "A1",  "2020-01-01")
  seed_bronze(root, "A10", "2024-01-01")
  expect_equal(as.Date(get_high_watermark(root, "A1", "Q")), as.Date("2020-01-01"))
})


# -- run_incremental(): argument handling -------------------------------------

test_that("run_incremental errors when no active gauges match gauge_ids", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G001"))
  expect_error(
    suppressWarnings(run_incremental(reg_path, root, gauge_ids = "DOES_NOT_EXIST",
                                     n_workers = 1L, log_path = tempfile())),
    "No matching active gauges"
  )
})

test_that("run_incremental warns for unknown gauge_ids but continues", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G001"))
  mock_route(data.table::data.table())
  expect_warning(
    run_incremental(reg_path, root, gauge_ids = c("G001", "GHOST"),
                    n_workers = 1L, log_path = tempfile()),
    "not found in registry"
  )
})

test_that("run_incremental skips inactive and non-live gauges", {
  root <- withr::local_tempdir()
  reg  <- data.table::rbindlist(list(
    reg_row("ACTIVE"), reg_row("OFF", active = FALSE), reg_row("HIST", live = FALSE)
  ))
  reg_path <- make_test_registry(root, reg)
  mock_route(data.table::data.table())
  log_dt <- run_incremental(reg_path, root, n_workers = 1L, log_path = tempfile())
  expect_equal(log_dt$gauge_id, "ACTIVE")
})


# -- run_incremental(): outcomes ----------------------------------------------

test_that("run_incremental reports NO_NEW_DATA without calling the router", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G002"))
  seed_bronze(root, "G002", "2023-12-31 23:45:00")
  calls <- mock_route(function() stop("router must not be called"))

  log_dt <- run_incremental(reg_path, root, end_date = "2023-12-31",
                            n_workers = 1L, log_path = tempfile())
  expect_equal(log_dt$status, "NO_NEW_DATA")
  expect_equal(calls$n, 0L)
})

test_that("run_incremental fetches from the watermark day and keeps only newer rows", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G003"))
  register <- file.path(root, "register", "hydrometric_data_register.csv")
  seed_bronze(root, "G003", "2022-12-31 12:00:00")

  fetched <- bronze_rows("G003", c("2022-12-31 11:45:00", "2022-12-31 12:00:00",
                                   "2022-12-31 12:15:00", "2023-01-01 00:00:00"))
  calls <- mock_route(fetched)

  log_dt <- run_incremental(reg_path, root, end_date = "2023-02-01",
                            n_workers = 1L, log_path = tempfile())

  # Same-day remainder is requested (not skipped to the next day)
  expect_equal(calls$start_date, "2022-12-31")
  expect_equal(calls$end_date, "2023-02-01")
  expect_equal(calls$on_error, "stop")

  expect_equal(log_dt$status, "SUCCESS")
  expect_equal(log_dt$rows, 2L)

  # 1 seeded row + 2 new rows, in two observation-year directories
  obj <- read_bronze(root, "G003", "Q")
  expect_equal(nrow(obj@readings), 3L)
  expect_setequal(basename(list.dirs(file.path(root, "bronze/hydrometric/EA/Q"),
                                     recursive = FALSE)), c("2022", "2023"))

  # Provenance written once per file, from the main process
  reg <- data.table::fread(register)
  expect_equal(nrow(reg), 2L)
  expect_true(all(reg$method_of_receipt == "HDE incremental"))
})

test_that("run_incremental uses default_start when the gauge has no Bronze data", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G004"))
  calls <- mock_route(data.table::data.table())
  log_dt <- suppressMessages(
    run_incremental(reg_path, root, default_start = "2005-01-01",
                    end_date = "2006-01-01", n_workers = 1L, log_path = tempfile())
  )
  expect_equal(calls$start_date, "2005-01-01")
  expect_equal(log_dt$status, "EMPTY")
})

test_that("run_incremental logs a fetch error as FAILED, not EMPTY", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G005"))
  mock_route(function() stop("API down"))
  expect_warning(
    log_dt <- run_incremental(reg_path, root, n_workers = 1L, log_path = tempfile()),
    "STEWARD ALERT"
  )
  expect_equal(log_dt$status, "FAILED")
  expect_match(log_dt$error, "API down")
})

test_that("run_incremental FAILED when the registry data_type is unusable", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G006", data_type = NA_character_))
  mock_route(data.table::data.table())
  suppressWarnings(
    log_dt <- run_incremental(reg_path, root, n_workers = 1L, log_path = tempfile())
  )
  expect_equal(log_dt$status, "FAILED")
  expect_match(log_dt$error, "data_type")
})

test_that("run_incremental NO_NEW_DATA when the fetch returns nothing newer", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G007"))
  seed_bronze(root, "G007", "2023-01-01 12:00:00")
  mock_route(bronze_rows("G007", "2023-01-01 11:00:00"))
  log_dt <- run_incremental(reg_path, root, end_date = "2023-06-01",
                            n_workers = 1L, log_path = tempfile())
  expect_equal(log_dt$status, "NO_NEW_DATA")
})


# -- run_incremental(): log ---------------------------------------------------

test_that("run_incremental appends to an existing log across runs", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G008"))
  log_path <- file.path(root, "logs", "inc.csv")
  mock_route(data.table::data.table())
  suppressMessages({
    run_incremental(reg_path, root, n_workers = 1L, log_path = log_path)
    run_incremental(reg_path, root, n_workers = 1L, log_path = log_path)
  })
  expect_equal(nrow(data.table::fread(log_path)), 2L)
})

test_that("run_incremental log has the expected columns", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G009"))
  mock_route(data.table::data.table())
  log_dt <- suppressMessages(
    run_incremental(reg_path, root, n_workers = 1L, log_path = tempfile())
  )
  expect_true(all(c("run_at", "gauge_id", "data_type", "watermark", "end_date",
                    "status", "rows", "elapsed_s", "error") %in% names(log_dt)))
})

test_that("run_incremental restores the caller's future plan", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G010"))
  mock_route(data.table::data.table())
  before <- class(future::plan())
  suppressMessages(run_incremental(reg_path, root, n_workers = 1L,
                                   log_path = tempfile()))
  expect_equal(class(future::plan()), before)
})


# -- regression: incremental after a same-day backfill must not lose data -------

test_that("run_incremental keeps earlier same-day Bronze rows (no overwrite)", {
  root     <- withr::local_tempdir()
  reg_path <- make_test_registry(root, reg_row("G020"))

  # Backfill earlier TODAY: all of 1 Jan 2023 (96 rows), filed under today's date
  ts <- format(as.POSIXct("2023-01-01", tz = "UTC") + (0:95) * 900, "%Y-%m-%d %H:%M:%S")
  seed_bronze(root, "G020", ts, received = Sys.Date())

  # Incremental run later the same day returns only newer rows
  mock_route(bronze_rows("G020", c("2023-01-02 00:00:00", "2023-01-02 00:15:00"),
                         received = Sys.Date()))
  log_dt <- run_incremental(reg_path, root, end_date = "2023-02-01",
                            n_workers = 1L, log_path = tempfile())
  expect_equal(log_dt$status, "SUCCESS")
  expect_equal(nrow(read_bronze(root, "G020", "Q")@readings), 98L)
})


# -- gauge_ids: unknown and excluded ids are reported separately ----------------

test_that("run_incremental distinguishes unknown ids from inactive / non-live ones", {
  root <- withr::local_tempdir()
  reg  <- data.table::rbindlist(list(
    reg_row("RUN"), reg_row("OFF", active = FALSE), reg_row("HIST", live = FALSE)
  ))
  reg_path <- make_test_registry(root, reg)
  mock_route(data.table::data.table())

  w <- character()
  withCallingHandlers(
    suppressMessages(run_incremental(
      reg_path, root, gauge_ids = c("RUN", "OFF", "HIST", "GHOST"),
      n_workers = 1L, log_path = tempfile())),
    warning = function(cnd) { w <<- c(w, conditionMessage(cnd)); invokeRestart("muffleWarning") }
  )
  expect_true(any(grepl("not found in registry: GHOST", w)))
  expect_true(any(grepl("inactive or not live: OFF, HIST", w)))
  expect_false(any(grepl("not found in registry:.*OFF", w)))
})
