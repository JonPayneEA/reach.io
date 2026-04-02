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
