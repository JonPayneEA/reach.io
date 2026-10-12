# -- Tests for the Bronze writer / reader ------------------------------------

# -- .write_bronze_by_year() --------------------------------------------------

test_that(".write_bronze_by_year writes one file per observation year", {
  root <- withr::local_tempdir()
  dt   <- bronze_fixture(c("2022-12-31 23:45:00", "2023-01-01 00:00:00",
                           "2023-06-01 12:00:00"))
  files <- write_fixture(root, dt)

  expect_equal(files$year, c("2022", "2023"))
  expect_equal(files$rows, c(1L, 2L))
  expect_true(all(file.exists(files$path)))
  expect_match(files$path[1], "bronze/hydrometric/EA/P/2022/EA_S1_P_20260115.parquet",
               fixed = TRUE)
  expect_equal(files$period_start, c("2022-12-31", "2023-01-01"))
  expect_equal(files$period_end,   c("2022-12-31", "2023-06-01"))
  # No temp files left behind
  expect_length(list.files(root, pattern = "\\.tmp$", recursive = TRUE), 0L)
})

test_that(".write_bronze_by_year drops (and warns about) missing timestamps", {
  root <- withr::local_tempdir()
  dt   <- bronze_fixture(c("2023-01-01 00:00:00", NA))
  expect_warning(files <- write_fixture(root, dt), "missing timestamp")
  expect_equal(sum(files$rows), 1L)
})

test_that(".write_bronze_by_year returns an empty table for empty input", {
  root <- withr::local_tempdir()
  dt   <- bronze_fixture(character(0))
  files <- write_fixture(root, dt)
  expect_equal(nrow(files), 0L)
})

test_that(".write_parquet_atomic overwrites an existing file", {
  root <- withr::local_tempdir()
  p <- file.path(root, "x", "a.parquet")
  .write_parquet_atomic(data.frame(v = 1), p)
  .write_parquet_atomic(data.frame(v = 2), p)
  expect_equal(arrow::read_parquet(p)$v, 2)
})


# -- file listing -------------------------------------------------------------

test_that(".re_escape escapes regex metacharacters", {
  expect_equal(.re_escape("S11512_FW.x(1)"), "S11512_FW\\.x\\(1\\)")
  expect_true(grepl(paste0("^", .re_escape("a.b")), "a.b"))
  expect_false(grepl(paste0("^", .re_escape("a.b")), "aXb"))
})

test_that(".list_bronze_files matches the exact gauge, oldest-received first", {
  root <- withr::local_tempdir()
  write_fixture(root, bronze_fixture("2023-01-01", site = "S1",  received = "2026-03-01"))
  write_fixture(root, bronze_fixture("2023-01-01", site = "S1",  received = "2026-01-01"))
  write_fixture(root, bronze_fixture("2023-01-01", site = "S10", received = "2026-02-01"))

  f <- .list_bronze_files(root, "S1", "P")
  expect_length(f, 2L)
  expect_equal(basename(f), c("EA_S1_P_20260101.parquet", "EA_S1_P_20260301.parquet"))
  expect_length(.list_bronze_files(root, "S1", "Q"), 0L)
  expect_length(.list_bronze_files(root, "nope", "P"), 0L)
})

test_that(".list_bronze_files narrows by year range", {
  root <- withr::local_tempdir()
  write_fixture(root, bronze_fixture(c("2020-06-01", "2021-06-01", "2022-06-01")))
  expect_length(.list_bronze_files(root, "S1", "P"), 3L)
  expect_length(.list_bronze_files(root, "S1", "P", start = "2021-01-01",
                                   end = "2021-12-31"), 1L)
})


# -- read_bronze() ------------------------------------------------------------

test_that("read_bronze returns the S7 class matching data type and period", {
  root <- withr::local_tempdir()
  write_fixture(root, bronze_fixture(c("2023-01-01 00:00:00", "2023-01-01 00:15:00")))
  obj <- read_bronze(root, "S1", "P")
  expect_true(S7::S7_inherits(obj, Rainfall_15min))
  expect_equal(nrow(obj@readings), 2L)
  expect_true(all(c("dateTime", "date", "value", "measure_notation",
                    "supplier_flag", "site_id", "data_type") %in% names(obj@readings)))
})

test_that("read_bronze reads across year partitions in time order", {
  root <- withr::local_tempdir()
  write_fixture(root, bronze_fixture(c("2023-01-01 00:00:00", "2022-12-31 23:45:00")))
  obj <- read_bronze(root, "S1", "P")
  expect_equal(nrow(obj@readings), 2L)
  expect_false(is.unsorted(obj@readings$dateTime))
})

test_that("read_bronze keeps the most recently received row for overlaps", {
  root <- withr::local_tempdir()
  ts <- c("2023-01-01 00:00:00", "2023-01-01 00:15:00")
  write_fixture(root, bronze_fixture(ts, value = c(1, 2), received = "2026-01-01"))
  write_fixture(root, bronze_fixture(c(ts, "2023-01-01 00:30:00"),
                                     value = c(10, 20, 30), received = "2026-02-01"))
  obj <- read_bronze(root, "S1", "P")
  expect_equal(nrow(obj@readings), 3L)
  expect_equal(obj@readings$value, c(10, 20, 30))
})

test_that("read_bronze start/end are applied (end day is inclusive)", {
  root <- withr::local_tempdir()
  write_fixture(root, bronze_fixture(c("2023-01-01 00:00:00", "2023-01-02 12:00:00",
                                       "2023-01-03 00:00:00")))
  obj <- read_bronze(root, "S1", "P", start = "2023-01-02", end = "2023-01-02")
  expect_equal(nrow(obj@readings), 1L)
})

test_that("read_bronze requires `trace` when a gauge holds several series", {
  root <- withr::local_tempdir()
  a <- bronze_fixture("2023-01-01", site = "3401TH", type = "H", supplier = "WISKI",
                      trace = "sg", value = 1)
  b <- bronze_fixture("2023-01-01", site = "3401TH", type = "H", supplier = "WISKI",
                      trace = "sg_ds", value = 2)
  write_fixture(root, a, "WISKI", "H")
  write_fixture(root, b, "WISKI", "H")

  expect_error(read_bronze(root, "3401TH", "H", supplier_code = "WISKI"),
               "several series")
  up <- read_bronze(root, "3401TH", "H", supplier_code = "WISKI", trace = "sg")
  ds <- read_bronze(root, "3401TH", "H", supplier_code = "WISKI", trace = "sg_ds")
  expect_equal(up@readings$value, 1)
  expect_equal(ds@readings$value, 2)
  expect_error(read_bronze(root, "3401TH", "H", supplier_code = "WISKI",
                           trace = "zzz"), "No Bronze series")
})

test_that("read_bronze gives clear errors for missing data", {
  root <- withr::local_tempdir()
  expect_error(read_bronze(root, "S1", "P"), "No Bronze store")
  dir.create(file.path(root, "bronze"))
  expect_error(read_bronze(root, "S1", "P"), "No Bronze data found")
})

test_that("read_bronze_multi skips gauges with no data", {
  root <- withr::local_tempdir()
  write_fixture(root, bronze_fixture("2023-01-01", site = "S1"))
  expect_warning(objs <- read_bronze_multi(root, c("S1", "NOPE"), "P"), "Skipping")
  expect_named(objs, "S1")
})


# -- handle_output() ----------------------------------------------------------

test_that("handle_output returns data unchanged in memory mode", {
  dt <- data.table::data.table(dateTime = Sys.time(), value = 1)
  expect_identical(handle_output(dt, "memory", NULL, "rainfall", "m"), dt)
})

test_that("handle_output writes year-partitioned Bronze Parquet in disk mode", {
  root <- withr::local_tempdir()
  dt <- data.table::data.table(
    dateTime = as.POSIXct(c("2022-12-31 23:45:00", "2023-01-01 00:00:00"), tz = "UTC"),
    value    = c(0.2, 0.4),
    quality  = c("Good", "Good")
  )
  paths <- handle_output(dt, "disk", root, "rainfall", "measure-x",
                         site_id = "SS92F014", period = "15min")
  expect_length(paths, 2L)
  expect_true(all(file.exists(paths)))
  expect_match(paths, "/bronze/hydrometric/EA/P/(2022|2023)/EA_SS92F014_P_", all = TRUE)

  back <- read_bronze(root, "SS92F014", "P")
  expect_equal(back@readings$value, c(0.2, 0.4))
  expect_equal(back@readings$supplier_flag, c("Good", "Good"))
})

test_that("handle_output honours supplier_code and category", {
  root <- withr::local_tempdir()
  dt <- data.table::data.table(dateTime = as.POSIXct("2023-01-01", tz = "UTC"), value = 1)
  paths <- handle_output(dt, "disk", root, "flow", "m", site_id = "X1",
                         supplier_code = "WISKI", category = "radarH19")
  expect_match(paths, "/bronze/radarH19/WISKI/Q/2023/", fixed = FALSE)
})
