# ============================================================
# Tests: Hydrological utilities (cumsum_na, add_hydro_year,
#        apply_inverse_rating, format_for_pdm)
# ============================================================

library(data.table)


# =============================================================================
# cumsum_na()
# =============================================================================

test_that("cumsum_na treats NA as zero contribution", {
  expect_equal(cumsum_na(c(1, NA, 3)), c(1, 1, 4))
  expect_equal(cumsum_na(c(NA, NA)),   c(0, 0))
  expect_equal(cumsum_na(c(1, 2, 3)),  c(1, 3, 6))
})

test_that("cumsum_na returns numeric vector same length as input", {
  x <- c(2.5, NA, 1.5)
  out <- cumsum_na(x)
  expect_length(out, 3L)
  expect_type(out, "double")
})


# =============================================================================
# add_hydro_year()
# =============================================================================

test_that("add_hydro_year: Sep 30 / Oct 1 boundary is correct", {
  dt <- data.table(
    dateTime = as.POSIXct(c("2023-09-30", "2023-10-01"), tz = "UTC")
  )
  add_hydro_year(dt)
  expect_equal(dt$hydro_year,     c(2023L, 2024L))
  expect_equal(dt$hydro_year_day, c(365L,  1L))
})

test_that("add_hydro_year: Jan 1 mid-year is correct", {
  dt <- data.table(
    dateTime = as.POSIXct("2024-01-01", tz = "UTC")
  )
  add_hydro_year(dt)
  expect_equal(dt$hydro_year,     2024L)
  # Days from Oct 1 2023 to Jan 1 2024 = 92 days (Oct 31 + Nov 30 + Dec 31 + 1)
  expect_equal(dt$hydro_year_day, 93L)
})

test_that("add_hydro_year: leap hydro year (2019-10-01 to 2020-09-30) has 366 days", {
  dt <- data.table(
    dateTime = as.POSIXct(c("2020-09-29", "2020-09-30"), tz = "UTC")
  )
  add_hydro_year(dt)
  expect_equal(dt$hydro_year,     c(2020L, 2020L))
  expect_equal(dt$hydro_year_day, c(365L,  366L))
})

test_that("add_hydro_year: Oct 1 is always day 1", {
  years <- 2018:2024
  oct1 <- as.POSIXct(paste0(years, "-10-01"), tz = "UTC")
  dt   <- data.table(dateTime = oct1)
  add_hydro_year(dt)
  expect_true(all(dt$hydro_year_day == 1L))
  expect_equal(dt$hydro_year, years + 1L)
})

test_that("add_hydro_year modifies dt in place and returns invisibly", {
  dt  <- data.table(dateTime = as.POSIXct("2024-06-01", tz = "UTC"))
  ret <- add_hydro_year(dt)
  expect_true("hydro_year" %in% names(dt))      # modified in place
  expect_identical(ret, dt)                      # same object returned
})

test_that("add_hydro_year errors on non-data.table input", {
  df <- data.frame(dateTime = as.POSIXct("2024-01-01", tz = "UTC"))
  expect_error(add_hydro_year(df), "`dt` must be a data.table")
})

test_that("add_hydro_year errors when date_col is absent", {
  dt <- data.table(x = 1)
  expect_error(add_hydro_year(dt), "no column named")
})


# =============================================================================
# apply_inverse_rating()  — round-trip test
# =============================================================================

# Reuse helpers from test-rating.R (they share the same test environment)

make_limbs_inv <- function() {
  data.table(
    lower    = c(0.000, 0.095, 2.000),
    upper    = c(0.095, 2.000,   Inf),
    C        = c(12.2495, 23.252, 23.252),
    a        = c(-0.003, -0.098, -0.098),
    b        = c( 1.641,  2.706,  2.706),
    doubtful = c(FALSE,   FALSE,  TRUE)
  )
}

make_rc_inv <- function() {
  RatingCurve(limbs = make_limbs_inv(), station_id = "510310", source = "WISKI")
}

make_level_15min_inv <- function(stages) {
  dt <- data.table(
    dateTime         = as.POSIXct("2020-01-01", tz = "UTC") + seq_along(stages) * 900,
    date             = as.Date("2020-01-01"),
    value            = stages,
    measure_notation = "level"
  )
  Level_15min(readings = dt, from_date = "2020-01-01", to_date = "2020-01-02")
}

test_that("apply_inverse_rating round-trips with apply_rating (< 0.001 m error)", {
  stages <- c(0.05, 0.10, 0.50, 1.00, 1.80)
  rc     <- make_rc_inv()
  lvl    <- make_level_15min_inv(stages)
  flow   <- apply_rating(lvl, rc)
  lvl2   <- apply_inverse_rating(flow, rc)
  orig   <- as_data_table(lvl)$value
  back   <- as_data_table(lvl2)$value
  expect_true(max(abs(orig - back), na.rm = TRUE) < 0.001)
})

test_that("apply_inverse_rating returns Level_15min from Flow_15min", {
  rc   <- make_rc_inv()
  lvl  <- make_level_15min_inv(c(0.5, 1.0))
  flow <- apply_rating(lvl, rc)
  out  <- apply_inverse_rating(flow, rc)
  expect_true(S7::S7_inherits(out, Level_15min))
})

test_that("apply_inverse_rating propagates NA", {
  rc  <- make_rc_inv()
  lvl <- make_level_15min_inv(c(0.5, NA, 1.0))
  flow <- apply_rating(lvl, rc)
  out  <- apply_inverse_rating(flow, rc)
  expect_true(is.na(as_data_table(out)$value[2L]))
})

test_that("apply_inverse_rating errors on non-Flow input", {
  rc  <- make_rc_inv()
  lvl <- make_level_15min_inv(c(0.5))
  expect_error(apply_inverse_rating(lvl, rc), "Flow_Daily or Flow_15min")
})

test_that("apply_inverse_rating errors on non-rating input", {
  rc   <- make_rc_inv()
  lvl  <- make_level_15min_inv(c(0.5))
  flow <- apply_rating(lvl, rc)
  expect_error(apply_inverse_rating(flow, "not_a_rating"), "RatingCurve or RatingSet")
})


# =============================================================================
# format_for_pdm()
# =============================================================================

test_that("format_for_pdm returns correct columns for rainfall", {
  dt  <- data.table(
    dateTime = as.POSIXct("2024-03-15 09:30:00", tz = "GMT"),
    value    = 2.5
  )
  out <- format_for_pdm(dt, "rainfall")
  expect_equal(names(out), c("year", "month", "day", "hour", "minute", "second", "rainfall"))
  expect_equal(out$year,     2024L)
  expect_equal(out$month,    3L)
  expect_equal(out$day,      15L)
  expect_equal(out$hour,     9L)
  expect_equal(out$minute,   30L)
  expect_equal(out$second,   0L)
  expect_equal(out$rainfall, 2.5)
})

test_that("format_for_pdm returns plain data.frame, not data.table", {
  dt  <- data.table(dateTime = as.POSIXct("2024-01-01", tz = "GMT"), value = 1)
  out <- format_for_pdm(dt, "flow")
  expect_true(is.data.frame(out))
  expect_false(inherits(out, "data.table"))
})

test_that("format_for_pdm names measure column correctly for all three types", {
  dt <- data.table(dateTime = as.POSIXct("2024-01-01", tz = "GMT"), value = 1)
  expect_true("rainfall" %in% names(format_for_pdm(dt, "rainfall")))
  expect_true("flow"     %in% names(format_for_pdm(dt, "flow")))
  expect_true("level"    %in% names(format_for_pdm(dt, "level")))
})

test_that("format_for_pdm errors on missing dateTime column", {
  dt <- data.table(time = as.POSIXct("2024-01-01", tz = "GMT"), value = 1)
  expect_error(format_for_pdm(dt, "flow"), "`dateTime`")
})

test_that("format_for_pdm errors on missing value column", {
  dt <- data.table(dateTime = as.POSIXct("2024-01-01", tz = "GMT"), v = 1)
  expect_error(format_for_pdm(dt, "flow"), "`value`")
})

test_that("format_for_pdm errors on non-data.frame input", {
  expect_error(format_for_pdm(list(dateTime = 1, value = 1), "flow"), "data.frame")
})


# =============================================================================
# format_for_fmp()
# =============================================================================

# Helper: make a small Flow_15min object with known values
make_flow_15min_fmp <- function(values, start = as.POSIXct("2024-01-01 09:00:00", tz = "UTC")) {
  n  <- length(values)
  dt <- data.table(
    dateTime         = start + (seq_len(n) - 1L) * 900L,
    date             = as.Date(start),
    value            = values,
    measure_notation = "test_flow"
  )
  Flow_15min(readings = dt, from_date = "2024-01-01", to_date = "2024-01-02")
}

test_that("format_for_fmp: single site produces 4 header rows + n data rows", {
  flow  <- make_flow_15min_fmp(c(12.5, 12.6, 12.4))
  lines <- format_for_fmp(list(Bruton = flow))
  expect_length(lines, 4L + 3L)
})

test_that("format_for_fmp: Row 4 is 'Time,<ied_ref>'", {
  flow  <- make_flow_15min_fmp(c(10, 11))
  lines <- format_for_fmp(
    list(Bruton = flow),
    gauge_ids = "405553",
    ied_refs  = "Bruton_405553"
  )
  expect_equal(lines[4L], "Time,Bruton_405553")
})

test_that("format_for_fmp: Row 4 IED refs auto-constructed from names + gauge_ids", {
  flow  <- make_flow_15min_fmp(c(10, 11))
  lines <- format_for_fmp(list(Bruton = flow), gauge_ids = "405553")
  expect_equal(lines[4L], "Time,Bruton_405553")
})

test_that("format_for_fmp: Row 4 falls back to list names when gauge_ids absent", {
  flow  <- make_flow_15min_fmp(c(10, 11))
  lines <- format_for_fmp(list(MyGauge = flow))
  expect_equal(lines[4L], "Time,MyGauge")
})

test_that("format_for_fmp: Row 3 blank when gauge_ids not supplied", {
  flow  <- make_flow_15min_fmp(c(10, 11))
  lines <- format_for_fmp(list(Bruton = flow))
  expect_equal(lines[3L], ",")   # blank first cell, blank site cell
})

test_that("format_for_fmp: Row 3 contains gauge_ids when supplied", {
  flow  <- make_flow_15min_fmp(c(10, 11))
  lines <- format_for_fmp(list(Bruton = flow), gauge_ids = "405553")
  expect_equal(lines[3L], ",405553")
})

test_that("format_for_fmp: Row 1 contains title, Row 2 contains comment", {
  flow  <- make_flow_15min_fmp(c(10))
  lines <- format_for_fmp(list(s = flow), title = "MyTitle", comment = "QA check")
  expect_true(startsWith(lines[1L], "MyTitle"))
  expect_true(startsWith(lines[2L], "QA check"))
})

test_that("format_for_fmp: 15-min data produces relative hours 0.000, 0.250, 0.500", {
  flow  <- make_flow_15min_fmp(c(10, 11, 12))
  lines <- format_for_fmp(list(s = flow))
  data_lines <- lines[5:7]
  times <- vapply(strsplit(data_lines, ","), `[[`, character(1L), 1L)
  expect_equal(times, c("0.000", "0.250", "0.500"))
})

test_that("format_for_fmp: start_time shifts relative hours correctly", {
  flow  <- make_flow_15min_fmp(c(10, 11),
             start = as.POSIXct("2024-01-01 10:00:00", tz = "UTC"))
  t0    <- as.POSIXct("2024-01-01 09:00:00", tz = "UTC")
  lines <- format_for_fmp(list(s = flow), start_time = t0)
  first_time <- strsplit(lines[5L], ",")[[1L]][1L]
  expect_equal(first_time, "1.000")   # 10:00 is 1 hour after 09:00
})

test_that("format_for_fmp: na_fill = 0 replaces NA in output", {
  flow  <- make_flow_15min_fmp(c(10, NA, 12))
  lines <- format_for_fmp(list(s = flow), na_fill = 0)
  row6  <- strsplit(lines[6L], ",")[[1L]]
  expect_equal(row6[2L], "0.000")
})

test_that("format_for_fmp: NA written as blank by default", {
  flow  <- make_flow_15min_fmp(c(10, NA, 12))
  lines <- format_for_fmp(list(s = flow))
  row6  <- strsplit(lines[6L], ",")[[1L]]
  expect_equal(row6[2L], "")
})

test_that("format_for_fmp: multi-site produces correct column count in Row 4", {
  f1    <- make_flow_15min_fmp(c(10, 11))
  f2    <- make_flow_15min_fmp(c(8, 9))
  lines <- format_for_fmp(
    list(Bruton = f1, Wincanton = f2),
    gauge_ids = c("405553", "365943")
  )
  row4_cells <- strsplit(lines[4L], ",")[[1L]]
  expect_length(row4_cells, 3L)   # Time + 2 sites
  expect_equal(row4_cells[1L], "Time")
  expect_equal(row4_cells[2L], "Bruton_405553")
  expect_equal(row4_cells[3L], "Wincanton_365943")
})

test_that("format_for_fmp: single unnamed Flow object accepted without error", {
  flow  <- make_flow_15min_fmp(c(10, 11))
  expect_no_error(format_for_fmp(flow))
})

test_that("format_for_fmp: writes to file when out_file is provided", {
  flow <- make_flow_15min_fmp(c(10, 11))
  tmp  <- tempfile(fileext = ".csv")
  ret  <- format_for_fmp(list(s = flow), out_file = tmp)
  expect_true(file.exists(tmp))
  expect_equal(ret, tmp)
  written <- readLines(tmp)
  in_mem  <- format_for_fmp(list(s = flow))
  expect_equal(written, in_mem)
})

test_that("format_for_fmp: errors on non-Flow input", {
  expect_error(format_for_fmp(list(1, 2, 3)), "Flow_Daily or Flow_15min")
})

test_that("format_for_fmp: errors when gauge_ids length mismatches flows", {
  flow <- make_flow_15min_fmp(c(10))
  expect_error(
    format_for_fmp(list(s = flow), gauge_ids = c("A", "B")),
    "gauge_ids.*has 2"
  )
})

test_that("format_for_fmp: errors when ied_refs length mismatches flows", {
  flow <- make_flow_15min_fmp(c(10))
  expect_error(
    format_for_fmp(list(s = flow), ied_refs = c("A", "B")),
    "ied_refs.*has 2"
  )
})
