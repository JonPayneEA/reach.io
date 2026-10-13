# -- Tests for the download paths: parsing, lookup, sync + batch to disk -------

uuid <- "c46d1245-e34a-4ea9-8c4c-410357e80e15"

# -- .parse_reading_datetimes() -----------------------------------------------

test_that(".parse_reading_datetimes handles ISO strings with T, Z and offsets", {
  dt <- data.table::data.table(
    date     = c("2022-01-01", "2022-01-01", "2022-01-01"),
    dateTime = c("2022-01-01T00:15:00", "2022-01-01T00:30:00Z", "2022-01-01T00:45:00+00:00"),
    value    = 1:3
  )
  .parse_reading_datetimes(dt)
  expect_s3_class(dt$date, "Date")
  expect_s3_class(dt$dateTime, "POSIXct")
  expect_equal(attr(dt$dateTime, "tzone"), "UTC")
  expect_equal(format(dt$dateTime, "%H:%M"), c("00:15", "00:30", "00:45"))
})

test_that(".parse_reading_datetimes forces UTC on POSIXct without shifting the instant", {
  ts <- as.POSIXct("2022-06-01 12:00:00", tz = "Europe/London")  # 11:00 UTC
  dt <- data.table::data.table(dateTime = ts, value = 1)
  .parse_reading_datetimes(dt)
  expect_equal(attr(dt$dateTime, "tzone"), "UTC")
  expect_equal(as.numeric(dt$dateTime), as.numeric(ts))
})


# -- find_stations() ----------------------------------------------------------

mock_stations <- function(items) {
  testthat::local_mocked_bindings(
    GET = function(...) structure(list(status_code = 200L), class = "response"),
    stop_for_status = function(x, ...) invisible(x),
    content = function(...) list(items = items),
    .package = "httr",
    .env = parent.frame()
  )
}

test_that("find_stations returns one row per measure with parsed metadata", {
  items <- data.frame(
    label = "Test stn", notation = uuid, wiskiID = "SS92F014",
    lat = 51, long = -1, easting = 1, northing = 2, riverName = "Exe",
    stringsAsFactors = FALSE
  )
  items$measures <- list(data.frame(
    `@id` = c(sprintf("http://x/%s-flow-i-900-m3s-qualified", uuid),
              sprintf("http://x/%s-rainfall-t-86400-mm-qualified", uuid),
              sprintf("http://x/%s-temperature-i-900-c-qualified", uuid)),
    check.names = FALSE
  ))
  mock_stations(items)

  res <- find_stations(wiski_ids = "SS92F014")
  expect_equal(nrow(res), 2L)                      # temperature is filtered out
  expect_equal(res$parameter, c("flow", "rainfall"))
  expect_equal(res$value_type, c("instantaneous", "total"))
  expect_equal(res$period, c("15min", "daily"))
  expect_equal(res$measure_notation[1], sprintf("%s-flow-i-900-m3s-qualified", uuid))
  expect_identical(res$station.notation, res$measure_notation)   # deprecated alias
})

test_that("find_stations copes with a station that has no measures", {
  items <- data.frame(
    label = c("A", "B"), notation = c(uuid, "other-station"),
    wiskiID = c("A1", "B1"), lat = 1, long = 1, easting = 1, northing = 1,
    riverName = "R", stringsAsFactors = FALSE
  )
  items$measures <- list(
    data.frame(`@id` = sprintf("http://x/%s-flow-i-900-m3s-qualified", uuid),
               check.names = FALSE),
    data.frame()
  )
  mock_stations(items)
  res <- find_stations(wiski_ids = "A1")
  expect_equal(nrow(res), 1L)
  expect_equal(res$wiskiID, "A1")
})

test_that(".parse_measure_notation gives NA (with a warning) for unexpected IDs", {
  expect_warning(
    out <- .parse_measure_notation(c(sprintf("%s-flow-m-86400-m3s-qualified", uuid), "weird")),
    "did not match"
  )
  expect_equal(out$parameter, c("flow", NA))
  expect_equal(out$period, c("daily", NA))
})


# -- download_hydrology(): disk output ---------------------------------------

fake_station_table <- function() {
  data.table::data.table(
    label = "Test", notation = "stn-suid", wiskiID = "SS92F014",
    measure_notation = sprintf("%s-rainfall-t-900-mm-qualified", uuid),
    station.notation = sprintf("%s-rainfall-t-900-mm-qualified", uuid),
    parameter = "rainfall", value_type = "total", period = "15min"
  )
}

fake_readings <- function(measure_notation, from_date, to_date, ...) {
  d <- data.table::data.table(
    dateTime = c("2022-12-31T23:45:00", "2023-01-01T00:00:00"),
    date = c("2022-12-31", "2023-01-01"),
    value = c(0.2, 0.4), quality = "Good", completeness = "Complete"
  )
  d[, measure_notation := measure_notation]
  .parse_reading_datetimes(d)
  d
}

test_that("download_hydrology (sync, disk) writes Bronze Parquet by observation year", {
  root <- withr::local_tempdir()
  testthat::local_mocked_bindings(
    find_stations = function(...) fake_station_table(),
    fetch_readings = fake_readings
  )
  res <- suppressMessages(download_hydrology(
    parameters = "rainfall", from_date = "2022-06-01", to_date = "2023-06-01",
    method = "sync", output = "disk", out_dir = root,
    period_name = "15min", wiski_ids = "SS92F014"
  ))

  expect_equal(nrow(res), 1L)                       # one summary row per measure
  expect_true(res$ok)
  expect_match(res$file, "/2022/.*; .*/2023/")
  expect_true(file.exists(file.path(root, "download_summary.csv")))

  obj <- read_bronze(root, "SS92F014", "P")
  expect_true(S7::S7_inherits(obj, FlodeRainfall_15min))
  expect_equal(obj@readings$value, c(0.2, 0.4))
  # No stray CSV tree from the old behaviour
  expect_false(dir.exists(file.path(root, "rainfall")))

  # One provenance record per year file, in the default register location
  reg <- data.table::fread(file.path(root, "register", "hydrometric_data_register.csv"))
  expect_equal(nrow(reg), 2L)
  expect_equal(unique(reg$site_id), "SS92F014")
  expect_equal(unique(reg$method_of_receipt), "API pull (sync)")
  expect_equal(unique(reg$temporal_resolution), "15min")
})

test_that("download_hydrology rejects an invalid category", {
  expect_error(
    download_hydrology(parameters = "rainfall", output = "disk", out_dir = tempdir(),
                       category = "nonsense", wiski_ids = "x"),
    "category"
  )
})


# -- run_batch() --------------------------------------------------------------

batch_csv <- c("dateTime,date,value,quality,completeness",
               "2022-12-31T23:45:00,2022-12-31,0.2,Good,Complete",
               "2023-01-01T00:00:00,2023-01-01,0.4,Good,Complete")

mock_batch <- function(csv = batch_csv, status = 200L, env = parent.frame()) {
  testthat::local_mocked_bindings(
    submit_batch = function(...) list(status_url = "https://x/results/job.csv", measure = "m"),
    .package = "reach.io", .env = env
  )
  testthat::local_mocked_bindings(
    GET = function(url, config = NULL, ...) {
      if (!is.null(config$output$path)) writeLines(csv, config$output$path)
      structure(list(status_code = status, url = url), class = "response")
    },
    stop_for_status = function(x, ...) {
      if (x$status_code >= 400) stop(sprintf("HTTP %d", x$status_code), call. = FALSE)
      invisible(x)
    },
    .package = "httr", .env = env
  )
}

test_that("run_batch (memory) parses dates like the sync path", {
  mock_batch()
  res <- suppressMessages(run_batch("m", "2022-01-01", "2023-01-01", "memory",
                                    NULL, "rainfall"))
  expect_true(res$ok)
  expect_equal(res$rows, 2L)
  expect_s3_class(res$data$dateTime, "POSIXct")
  expect_s3_class(res$data$date, "Date")
  expect_equal(res$data$measure_notation, rep("m", 2))
})

test_that("run_batch (disk) writes Bronze Parquet, not CSV", {
  root <- withr::local_tempdir()
  mock_batch()
  register <- file.path(root, "register.csv")
  res <- suppressMessages(run_batch("m", "2022-01-01", "2023-01-01", "disk", root,
                                    "rainfall", site_id = "SS92F014", period = "15min",
                                    register_path = register))
  expect_true(res$ok)
  expect_equal(nrow(data.table::fread(register)), 2L)
  expect_match(res$file, "/bronze/hydrometric/EA/P/2022/.*; .*/2023/")
  # The only CSV is the provenance register - no per-measure CSVs any more
  expect_equal(list.files(root, pattern = "\\.csv$", recursive = TRUE), "register.csv")
  expect_equal(nrow(read_bronze(root, "SS92F014", "P")@readings), 2L)
})

test_that("run_batch reports an HTTP error instead of saving it as data", {
  root <- withr::local_tempdir()
  mock_batch(csv = "<html>Forbidden</html>", status = 403L)
  res <- suppressMessages(run_batch("m", "2022-01-01", "2023-01-01", "disk", root,
                                    "rainfall", site_id = "S"))
  expect_false(res$ok)
  expect_true(is.na(res$file))
  expect_length(list.files(root, recursive = TRUE), 0L)
})


# -- fetch_from_hde(): a failed chunk must not look like a complete series -------
#
# Regression: a failed annual chunk was only a warning, so route_gauge(on_error =
# "stop") never saw it and the orchestrators logged SUCCESS for a partial series.

flaky_readings <- function(fail_call) {
  state <- new.env(parent = emptyenv())
  state$n <- 0L
  function(measure_notation, from_date, to_date, ...) {
    state$n <- state$n + 1L
    if (state$n == fail_call) stop("HTTP 500")
    d <- data.table::data.table(
      dateTime = paste0(from_date, "T00:00:00"), date = from_date,
      value = 1, quality = "Good", completeness = "Complete"
    )
    d[, measure_notation := measure_notation]
    .parse_reading_datetimes(d)
    d
  }
}

test_that("fetch_from_hde(on_error = 'warn') returns the partial series with a warning", {
  testthat::local_mocked_bindings(
    find_stations  = function(...) fake_station_table(),
    fetch_readings = flaky_readings(2L)
  )
  expect_warning(
    res <- suppressMessages(fetch_from_hde("SS92F014", "rainfall",
                                           "2020-01-01", "2023-01-01")),
    "Chunk 2 failed"
  )
  expect_equal(nrow(res), 2L)              # chunks 1 and 3 only
})

test_that("fetch_from_hde(on_error = 'stop') raises after trying every chunk", {
  testthat::local_mocked_bindings(
    find_stations  = function(...) fake_station_table(),
    fetch_readings = flaky_readings(2L)
  )
  expect_error(
    suppressMessages(fetch_from_hde("SS92F014", "rainfall", "2020-01-01",
                                    "2023-01-01", on_error = "stop")),
    "1 of 3 chunk\\(s\\) failed for SS92F014.*HTTP 500"
  )
})

test_that("route_gauge(on_error = 'stop') surfaces a failed HDE chunk", {
  testthat::local_mocked_bindings(
    find_stations  = function(...) fake_station_table(),
    fetch_readings = flaky_readings(1L)
  )
  gauge <- data.table::data.table(gauge_id = "SS92F014", source_system = "HDE",
                                  data_type = "rainfall")
  expect_error(
    suppressMessages(route_gauge(gauge, "2020-01-01", "2022-01-01", on_error = "stop")),
    "Route failed for SS92F014.*chunk"
  )
})


test_that("fetch_from_hde(on_error = 'stop') fails for an unknown gauge or missing measure", {
  testthat::local_mocked_bindings(find_stations = function(...) data.table::data.table())
  expect_error(suppressMessages(fetch_from_hde("NOPE", "rainfall", "2020-01-01",
                                               "2021-01-01", on_error = "stop")),
               "No station found for gauge_id: NOPE")
  expect_warning(res <- suppressMessages(fetch_from_hde("NOPE", "rainfall", "2020-01-01",
                                                        "2021-01-01")),
                 "No station found")
  expect_equal(nrow(res), 0L)

  stn <- fake_station_table()
  stn[, period := "daily"]                       # only a daily measure exists
  testthat::local_mocked_bindings(find_stations = function(...) stn)
  expect_error(suppressMessages(fetch_from_hde("SS92F014", "rainfall", "2020-01-01",
                                               "2021-01-01", on_error = "stop")),
               "No 15min .*rainfall measure found.*Available: daily")
})


# -- run_sync(): write errors are contained; every chunk is paced ----------------

test_that("run_sync records a Bronze write error as ok = FALSE instead of aborting", {
  testthat::local_mocked_bindings(
    fetch_readings = fake_readings,
    handle_output  = function(...) stop("disk full")
  )
  chunks <- data.table::data.table(chunk_from = "2022-06-01", chunk_to = "2023-06-01")
  res <- suppressMessages(run_sync("m", chunks, "disk", tempfile(), "rainfall", NULL,
                                   site_id = "S1", period = "15min"))
  expect_false(res$ok)
  expect_true(is.na(res$file))
  expect_equal(res$rows, 2L)                 # the rows were fetched
})

test_that("run_sync pauses after every chunk, including empty and failed ones", {
  sleeps <- 0L
  calls  <- 0L
  testthat::local_mocked_bindings(
    fetch_readings = function(...) {
      calls <<- calls + 1L
      if (calls == 2L) stop("HTTP 500")
      if (calls == 3L) return(data.table::data.table())
      fake_readings(...)
    },
    .pace = function(...) {
      sleeps <<- sleeps + 1L
      invisible()
    }
  )
  chunks <- data.table::data.table(chunk_from = c("2020-01-01", "2021-01-01", "2022-01-01"),
                                   chunk_to   = c("2021-01-01", "2022-01-01", "2023-01-01"))
  res <- suppressMessages(run_sync("m", chunks, "memory", NULL, "rainfall", NULL))
  expect_equal(sleeps, 3L)                   # success, failure and empty all paced
  expect_false(res$ok)
})


# -- argument / column name clashes in the API clients ---------------------------

test_that("fetch_readings tags rows with the requested measure even if the response has one", {
  items <- data.frame(dateTime = "2022-01-01T00:00:00", date = "2022-01-01",
                      value = 1, measure_notation = "WRONG", stringsAsFactors = FALSE)
  testthat::local_mocked_bindings(
    GET = function(...) structure(list(status_code = 200L), class = "response"),
    stop_for_status = function(x, ...) invisible(x),
    content = function(...) list(items = items),
    .package = "httr"
  )
  res <- fetch_readings("REQUESTED-ID", "2022-01-01", "2022-02-01")
  expect_equal(res$measure_notation, "REQUESTED-ID")
})

test_that("get_measures sets `parameter` to the requested one even if the response has one", {
  items <- data.frame(notation = "m1", parameter = "WRONG", stringsAsFactors = FALSE)
  testthat::local_mocked_bindings(
    GET = function(...) structure(list(status_code = 200L), class = "response"),
    stop_for_status = function(x, ...) invisible(x),
    content = function(...) list(items = items),
    .package = "httr"
  )
  res <- get_measures("rainfall")
  expect_equal(res$parameter, "rainfall")
})


# -- several measures at one station are filed separately in disk mode -----------
#
# Regression: a dataset_id has no period / value-type component, so e.g. an
# instantaneous and a daily-maximum measure at one station shared one file.

test_that(".measure_traces builds unique, file-safe traces", {
  m <- data.table::data.table(period = c("15min", "15min", "daily"),
                              value_type = c("instantaneous", "instantaneous", "max"))
  tr <- .measure_traces(m)
  expect_equal(tr, c("15min_instantaneous", "15min_instantaneous_1", "daily_max"))
  expect_false(anyDuplicated(tr) > 0L)
  # get_measures() column names are understood too
  expect_equal(.measure_traces(data.table::data.table(periodName = "900", valueType = "mean")),
               "900_mean")
  expect_equal(.measure_traces(data.table::data.table(x = 1)), "measure")
})

test_that("download_hydrology (disk) files several measures at one station separately", {
  root <- withr::local_tempdir()
  stn <- rbind(fake_station_table(), fake_station_table())
  stn[, measure_notation := c(sprintf("%s-rainfall-t-900-mm-qualified", uuid),
                              sprintf("%s-rainfall-t-86400-mm-qualified", uuid))]
  stn[, station.notation := measure_notation]
  stn[, period := c("15min", "daily")]
  testthat::local_mocked_bindings(
    find_stations  = function(...) stn,
    fetch_readings = function(measure_notation, from_date, to_date, ...) {
      d <- fake_readings(measure_notation, from_date, to_date)
      # decided outside d[...]: inside it, `measure_notation` is d's column
      vals <- if (grepl("-86400-", measure_notation)) c(10, 20) else c(0.2, 0.4)
      data.table::set(d, j = "value", value = vals)
      d
    }
  )
  res <- suppressMessages(download_hydrology(
    parameters = "rainfall", from_date = "2022-06-01", to_date = "2023-06-01",
    method = "sync", output = "disk", out_dir = root,
    period_name = "all", wiski_ids = "SS92F014"
  ))
  expect_equal(nrow(res), 2L)
  expect_false(anyDuplicated(res$file) > 0L)

  fine  <- read_bronze(root, "SS92F014", "P", trace = "15min_total")
  daily <- read_bronze(root, "SS92F014", "P", trace = "daily_total")
  expect_equal(fine@readings$value,  c(0.2, 0.4))
  expect_equal(daily@readings$value, c(10, 20))
  # Each series records its OWN resolution, not the "all" filter setting, so the
  # right S7 class comes back
  expect_true(S7::S7_inherits(fine,  FlodeRainfall_15min))
  expect_true(S7::S7_inherits(daily, FlodeRainfall_Daily))
  # reading without a trace is ambiguous and says so
  expect_error(read_bronze(root, "SS92F014", "P"), "several series")
})


# -- handle_output(): provenance supplier follows supplier_code ------------------

test_that("handle_output records the source system, not a hard-coded HDE", {
  root <- withr::local_tempdir()
  reg  <- file.path(root, "register.csv")
  dt <- data.table::data.table(dateTime = as.POSIXct("2023-01-01", tz = "UTC"), value = 1)
  handle_output(dt, "disk", root, "flow", "m", site_id = "X1", register_path = reg)
  handle_output(dt, "disk", root, "flow", "m", site_id = "X2", register_path = reg,
                supplier_code = "WISKI")
  r <- data.table::fread(reg)
  expect_equal(r[site_id == "X1", supplier], "HDE")
  expect_equal(r[site_id == "X2", supplier], "WISKI")
})


# -- format_for_fmp(): one timestamp, several sites -------------------------------

test_that("format_for_fmp lays out several sites correctly when there is one timestamp", {
  mk <- function(v) {
    FlodeFlow_15min(readings = data.table::data.table(
      dateTime = as.POSIXct("2023-01-01 00:00:00", tz = "UTC"),
      date = as.Date("2023-01-01"),
      value = v,
      measure_notation = "m"
    ))
  }
  lines <- format_for_fmp(list(A = mk(1.5), B = mk(2.5), C = mk(3.5)))
  # 4 header rows then one data row: time, then one value per site
  expect_equal(length(lines), 5L)
  expect_equal(lines[5L], "0.000,1.500,2.500,3.500")
})
