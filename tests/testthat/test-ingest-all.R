# -- Tests for the WISKI .all ingest ------------------------------------------

# One station block in the .all layout: tab-separated metadata, then a
# "Time stamp,..." header and comma-separated rows.
all_block <- function(station = "2723TH", parameter = "Discharge",
                      ts_name = "THM/2723TH/Q/15m.Cmd.RelAbs.P",
                      rows, header = "Time stamp,Value,Quality Code") {
  c(sprintf("Station Site:\t%s site", station),
    "Station Name:\tTest station",
    sprintf("Station Number:\t%s", station),
    sprintf("Parameter Name:\t%s", parameter),
    sprintf("Time series Name:\t%s", ts_name),
    "Time series Unit:\tm3/s",
    "Longitude:\t-1.0",
    "Latitude:\t51.0",
    header, rows, "")
}

write_all_file <- function(..., path = tempfile(fileext = ".all")) {
  writeLines(unlist(list(...)), path)
  path
}

flow_rows <- c("01/01/2022 00:00:00,1.5,100",
               "01/01/2022 00:15:00,1.6,100",
               "01/01/2022 00:30:00,1.7,100")


# -- parameter mapping --------------------------------------------------------

test_that(".wiski_param_to_type maps known parameters and rejects the rest", {
  expect_equal(.wiski_param_to_type("Discharge"), "Q")
  expect_equal(.wiski_param_to_type("River Flow"), "Q")
  expect_equal(.wiski_param_to_type("Water Level"), "H")
  expect_equal(.wiski_param_to_type("Stage"), "H")
  expect_equal(.wiski_param_to_type("Rainfall"), "P")
  expect_true(is.na(.wiski_param_to_type("Groundwater Level")))
  expect_true(is.na(.wiski_param_to_type("Air Temperature")))
  expect_true(is.na(.wiski_param_to_type(NA_character_)))
})


# -- ingest_all_file() --------------------------------------------------------

test_that("ingest_all_file writes year-partitioned Bronze readable by read_bronze", {
  root <- withr::local_tempdir()
  f    <- write_all_file(all_block(rows = flow_rows))
  log_dt <- suppressMessages(ingest_all_file(f, root))

  expect_equal(log_dt$gauge_id, "2723TH")
  expect_equal(log_dt$data_type, "Q")
  expect_equal(log_dt$rows, 3L)
  expect_match(log_dt$out_file, "/bronze/hydrometric/WISKI/Q/2022/WISKI_2723TH_Q_q_",
               fixed = FALSE)

  obj <- read_bronze(root, "2723TH", "Q", supplier_code = "WISKI")
  expect_true(S7::S7_inherits(obj, FlodeFlow_15min))
  expect_equal(obj@readings$value, c(1.5, 1.6, 1.7))
  expect_equal(obj@readings$supplier_flag, rep("100", 3))

  # Provenance record per file; temp directory cleaned up
  expect_equal(nrow(data.table::fread(file.path(root, "register",
                                                "hydrometric_data_register.csv"))), 1L)
  expect_length(list.files(root, all.files = TRUE, pattern = "^\\.tmp_"), 0L)
})

test_that("ingest_all_file splits a multi-year series by observation year", {
  root <- withr::local_tempdir()
  f <- write_all_file(all_block(rows = c("31/12/2021 23:45:00,1,100",
                                         "01/01/2022 00:00:00,2,100")))
  log_dt <- suppressMessages(ingest_all_file(f, root))
  expect_match(log_dt$out_file, "2021/.*; .*2022/")
})

test_that("ingest_all_file skips unrecognised parameters instead of filing them as flow", {
  root <- withr::local_tempdir()
  f <- write_all_file(
    all_block(rows = flow_rows),
    all_block(parameter = "Groundwater Level", ts_name = "THM/2723TH/GW/15m.Cmd.P",
              rows = flow_rows)
  )
  expect_warning(
    log_dt <- suppressMessages(ingest_all_file(f, root)),
    "Unrecognised parameter 'Groundwater Level'"
  )
  expect_equal(nrow(log_dt), 1L)
  skipped <- attr(log_dt, "skipped")
  expect_equal(skipped$parameter, "Groundwater Level")
  expect_false(dir.exists(file.path(root, "bronze", "hydrometric", "WISKI", "H")))
})

test_that("ingest_all_file takes the Value column, not a numeric quality code", {
  root <- withr::local_tempdir()
  f <- write_all_file(all_block(
    header = "Time stamp,Quality Code,Value",
    rows   = c("01/01/2022 00:00:00,100,2.5", "01/01/2022 00:15:00,100,2.6")
  ))
  suppressMessages(ingest_all_file(f, root))
  obj <- read_bronze(root, "2723TH", "Q", supplier_code = "WISKI")
  expect_equal(obj@readings$value, c(2.5, 2.6))
})

test_that("ingest_all_file does not ingest quality codes when Value is all missing", {
  root <- withr::local_tempdir()
  f <- write_all_file(all_block(
    header = "Time stamp,Quality Code,Value",
    rows   = c("01/01/2022 00:00:00,100,---", "01/01/2022 00:15:00,100,---")
  ))
  log_dt <- suppressMessages(ingest_all_file(f, root))
  expect_equal(nrow(log_dt), 0L)
  expect_equal(attr(log_dt, "skipped")$reason, "no values")
})

test_that("ingest_all_file warns about unparseable timestamps and drops them", {
  root <- withr::local_tempdir()
  f <- write_all_file(all_block(rows = c(flow_rows, "not-a-date,9.9,100")))
  expect_warning(
    log_dt <- suppressMessages(ingest_all_file(f, root)),
    "1 row\\(s\\) had an unparseable timestamp"
  )
  expect_equal(log_dt$rows, 3L)
})

test_that("ingest_all_file converts timestamps from `tz` to UTC", {
  root <- withr::local_tempdir()
  f <- write_all_file(all_block(rows = "01/07/2022 12:00:00,1.5,100"))
  suppressMessages(ingest_all_file(f, root, tz = "Europe/London"))
  obj <- read_bronze(root, "2723TH", "Q", supplier_code = "WISKI")
  expect_equal(obj@readings$dateTime, as.POSIXct("2022-07-01 11:00:00", tz = "UTC"))
})

test_that("ingest_all_file: for duplicate timestamps the later block wins", {
  root <- withr::local_tempdir()
  f <- write_all_file(
    all_block(rows = c("01/01/2022 00:00:00,1,100", "01/01/2022 00:15:00,2,100")),
    all_block(rows = c("01/01/2022 00:15:00,99,100", "01/01/2022 00:30:00,3,100"))
  )
  suppressMessages(ingest_all_file(f, root))
  obj <- read_bronze(root, "2723TH", "Q", supplier_code = "WISKI")
  expect_equal(obj@readings$value, c(1, 99, 3))
})

test_that("ingest_all_file keeps separate series for different trace codes", {
  root <- withr::local_tempdir()
  f <- write_all_file(
    all_block(parameter = "Water Level", ts_name = "THM/3401TH/SG/15m.Cmd.RelAbs.P",
              station = "3401TH", rows = "01/01/2022 00:00:00,1,100"),
    all_block(parameter = "Water Level", ts_name = "THM/3401TH/SG_DS/15m.Cmd.RelAbs.P",
              station = "3401TH", rows = "01/01/2022 00:00:00,2,100")
  )
  suppressMessages(ingest_all_file(f, root))
  expect_error(read_bronze(root, "3401TH", "H", supplier_code = "WISKI"), "several series")
  ds <- read_bronze(root, "3401TH", "H", supplier_code = "WISKI", trace = "sg_ds")
  expect_equal(ds@readings$value, 2)
})

test_that("ingest_all_file warns about off-grid 15-min timestamps", {
  root <- withr::local_tempdir()
  f <- write_all_file(all_block(rows = c("01/01/2022 00:00:00,1,100",
                                         "01/01/2022 00:01:00,1,100",
                                         "01/01/2022 00:15:00,2,100")))
  expect_warning(suppressMessages(ingest_all_file(f, root)), "off-grid")
})


# -- registry auto-registration ----------------------------------------------

test_that("ingest_all_file registers each station + data type once", {
  root     <- withr::local_tempdir()
  reg_path <- file.path(root, "register", "gauge_registry.parquet")
  f1 <- write_all_file(all_block(rows = flow_rows))
  suppressMessages(ingest_all_file(f1, root, registry_path = reg_path))

  f2 <- write_all_file(
    all_block(rows = flow_rows),
    all_block(parameter = "Water Level", ts_name = "THM/2723TH/SG/15m.Cmd.RelAbs.P",
              rows = flow_rows)
  )
  suppressMessages(ingest_all_file(f2, root, registry_path = reg_path))

  reg <- data.table::as.data.table(arrow::read_parquet(reg_path))
  expect_equal(nrow(reg), 2L)
  expect_setequal(reg$data_type, c("flow", "level"))
  expect_true(all(reg$source_system == "WISKI_ALL"))
  expect_false(any(reg$live))
  expect_true(all(reg$backfill_done))
})


# -- fetch_from_wiski_all() ---------------------------------------------------

test_that("fetch_from_wiski_all returns Bronze-schema rows for the requested type and range", {
  root <- withr::local_tempdir()
  write_all_file(
    all_block(rows = c("01/01/2022 00:00:00,1,100", "01/01/2022 00:15:00,2,100",
                       "02/01/2022 00:00:00,3,100")),
    all_block(parameter = "Water Level", ts_name = "THM/2723TH/SG/15m.Cmd.RelAbs.P",
              rows = "01/01/2022 00:00:00,9,100"),
    path = file.path(root, "2723TH.all")
  )
  withr::local_envvar(WISKI_ALL_ROOT = root)

  dt <- suppressMessages(fetch_from_wiski_all("2723TH", "flow", "2022-01-01", "2022-01-02"))
  expect_true(all(c("timestamp", "value", "supplier_flag", "dataset_id", "site_id",
                    "data_type", "period") %in% names(dt)))
  expect_equal(dt$value, c(1, 2))            # end date exclusive, level excluded
  expect_equal(unique(dt$data_type), "Q")

  expect_equal(nrow(suppressMessages(
    fetch_from_wiski_all("2723TH", "rainfall", "2022-01-01", "2022-02-01"))), 0L)
})

test_that("fetch_from_wiski_all handles a missing file and unset root", {
  withr::local_envvar(WISKI_ALL_ROOT = withr::local_tempdir())
  expect_warning(
    res <- suppressMessages(fetch_from_wiski_all("ABSENT", "flow", "2022-01-01", "2022-02-01")),
    "No .all file"
  )
  expect_equal(nrow(res), 0L)

  withr::local_envvar(WISKI_ALL_ROOT = NA)
  expect_error(suppressMessages(fetch_from_wiski_all("X", "flow", "2022-01-01", "2022-02-01")),
               "WISKI_ALL_ROOT")
})


# -- leftover temp blocks must not leak into a new ingest ------------------------
#
# Regression: pass 2 merges every block file in the temp directory, and an
# existing directory was reused without being emptied.

test_that("ingest_all_file ignores stale block files left in the temp directory", {
  root <- withr::local_tempdir()
  f    <- write_all_file(all_block(rows = flow_rows), path = file.path(root, "export.all"))

  # First run keeps its intermediate files (as a debugging run would)
  suppressMessages(ingest_all_file(f, root, keep_tmp = TRUE))
  tmp <- file.path(root, ".tmp_export")
  expect_true(dir.exists(tmp))

  # Plant a stale block for a station that is NOT in the file we ingest next
  stale <- data.table::data.table(
    timestamp = as.POSIXct("2020-01-01", tz = "UTC"), Value = 9, Quality.Code = 100L,
    station_number = "STALE1", station_name = "x", parameter = "Discharge",
    unit = "m3/s", ts_name = "THM/STALE1/Q/15m.Cmd.RelAbs.P", lon = 0, lat = 0
  )
  arrow::write_parquet(stale, file.path(tmp, "STALE1__q__000099.parquet"))

  log_dt <- suppressMessages(ingest_all_file(f, root))
  expect_equal(log_dt$gauge_id, "2723TH")                       # only the real station
  expect_false(dir.exists(tmp))                                 # cleaned up this time
  expect_length(.list_bronze_files(root, "STALE1", "Q", "WISKI"), 0L)
})


# -- on_error: missing source data is a failure when asked to stop ---------------

test_that("fetch_from_wiski_all(on_error = 'stop') fails for a missing file or series", {
  root <- withr::local_tempdir()
  write_all_file(all_block(rows = flow_rows), path = file.path(root, "2723TH.all"))
  withr::local_envvar(WISKI_ALL_ROOT = root)

  # no file at all
  expect_error(
    suppressMessages(fetch_from_wiski_all("ABSENT", "flow", "2022-01-01", "2022-02-01",
                                          on_error = "stop")),
    "No .all file"
  )
  # file exists but has no level series
  expect_error(
    suppressMessages(fetch_from_wiski_all("2723TH", "level", "2022-01-01", "2022-02-01",
                                          on_error = "stop")),
    "No level series for station '2723TH'"
  )
  # default still warns / returns empty
  expect_equal(nrow(suppressMessages(
    fetch_from_wiski_all("2723TH", "level", "2022-01-01", "2022-02-01"))), 0L)
})

test_that("route_gauge(on_error = 'stop') makes a missing .all file a failure", {
  withr::local_envvar(WISKI_ALL_ROOT = withr::local_tempdir())
  gauge <- data.table::data.table(gauge_id = "ABSENT", source_system = "WISKI_ALL",
                                  data_type = "flow")
  expect_error(
    suppressMessages(route_gauge(gauge, "2022-01-01", "2022-02-01", on_error = "stop")),
    "Route failed for ABSENT.*No .all file"
  )
})


test_that("ingest_all_file warns when different time series share one series key", {
  root <- withr::local_tempdir()
  f <- write_all_file(
    all_block(ts_name = "THM/2723TH/Q/15m.Cmd.RelAbs.P", rows = flow_rows),
    all_block(ts_name = "THM/2723TH/Q/1d.Cmd.RelAbs.P",
              rows = "05/01/2022 00:00:00,9,100")
  )
  expect_warning(suppressMessages(ingest_all_file(f, root)),
                 "2 different time series names share the series key '2723TH__q'")
})
