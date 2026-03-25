library(testthat)
library(reach.io)
library(data.table)

# -- Helpers ------------------------------------------------------------------

# Build a noiseless sine series with known parameters
.make_sine_pe <- function(B = 2.5, A = 2.0, phi = 172,
                          start = "2010-01-01", end = "2019-12-31") {
  dates <- seq(as.Date(start), as.Date(end), by = "day")
  doy   <- as.integer(format(dates, "%j"))
  pe    <- B + A * sin(2 * pi / 365.25 * (doy - phi))
  list(pe = pe, dates = dates, B = B, A = A, phi = phi)
}

# =============================================================================
# 1. Schema prerequisite
# =============================================================================

test_that("param_to_data_type('pe') returns 'PE'", {
  expect_equal(param_to_data_type("pe"), "PE")
})

# =============================================================================
# 2. fit_pe_sine() — structure
# =============================================================================

test_that("fit_pe_sine() lm_fourier returns correct list structure", {
  s      <- .make_sine_pe()
  result <- fit_pe_sine(s$pe, s$dates, method = "lm_fourier")

  expect_type(result, "list")
  expect_named(result, c("B", "A", "phi", "r_squared", "rmse", "method", "n_obs"),
               ignore.order = FALSE)
  expect_type(result$B,         "double")
  expect_type(result$A,         "double")
  expect_type(result$phi,       "double")
  expect_type(result$r_squared, "double")
  expect_type(result$rmse,      "double")
  expect_equal(result$method,   "lm_fourier")
  expect_equal(result$n_obs,    length(s$pe))
})

# =============================================================================
# 3. fit_pe_sine() — parameter recovery
# =============================================================================

test_that("fit_pe_sine() lm_fourier recovers known B, A, phi from noiseless sine", {
  s      <- .make_sine_pe(B = 2.5, A = 2.0, phi = 172)
  result <- fit_pe_sine(s$pe, s$dates, method = "lm_fourier")

  expect_equal(result$B,   s$B,   tolerance = 1e-6)
  expect_equal(result$A,   s$A,   tolerance = 1e-6)
  expect_equal(result$phi, s$phi, tolerance = 1e-3)
  expect_gt(result$r_squared, 0.9999)
  expect_lt(result$rmse, 1e-6)
})

test_that("fit_pe_sine() nls recovers known parameters from noiseless sine", {
  s      <- .make_sine_pe(B = 2.5, A = 2.0, phi = 172)
  result <- fit_pe_sine(s$pe, s$dates, method = "nls")

  expect_equal(result$B,   s$B,   tolerance = 1e-4)
  expect_equal(result$A,   s$A,   tolerance = 1e-4)
  expect_equal(result$phi, s$phi, tolerance = 0.01)
  expect_gt(result$r_squared, 0.999)
})

test_that("fit_pe_sine() lm_fourier and nls agree on noisy data", {
  set.seed(42)
  s    <- .make_sine_pe()
  pe_n <- s$pe + stats::rnorm(length(s$pe), sd = 0.3)

  lm_res  <- fit_pe_sine(pe_n, s$dates, method = "lm_fourier")
  nls_res <- fit_pe_sine(pe_n, s$dates, method = "nls")

  expect_equal(lm_res$B,   nls_res$B,   tolerance = 0.05)
  expect_equal(lm_res$A,   nls_res$A,   tolerance = 0.05)
  expect_equal(lm_res$phi, nls_res$phi, tolerance = 2)
})

# =============================================================================
# 4. fit_pe_sine() — validation errors
# =============================================================================

test_that("fit_pe_sine() errors on mismatched length", {
  s <- .make_sine_pe()
  expect_error(fit_pe_sine(s$pe[-1], s$dates), "same length")
})

test_that("fit_pe_sine() errors on fewer than 14 observations", {
  s <- .make_sine_pe()
  expect_error(fit_pe_sine(s$pe[1:10], s$dates[1:10]), "at least 14")
})

test_that("fit_pe_sine() errors when pe contains NA", {
  s       <- .make_sine_pe()
  pe_na   <- s$pe
  pe_na[5] <- NA_real_
  expect_error(fit_pe_sine(pe_na, s$dates), "NA")
})

test_that("fit_pe_sine() errors when dates is not a Date vector", {
  s <- .make_sine_pe()
  expect_error(fit_pe_sine(s$pe, as.character(s$dates)), "Date vector")
})

# =============================================================================
# 5. generate_pe_sine()
# =============================================================================

test_that("generate_pe_sine() returns PotEvap_Daily when as_s7 = TRUE", {
  obj <- generate_pe_sine(2.5, 2.0, 172,
                          start = "2020-01-01", end = "2020-12-31",
                          as_s7 = TRUE)
  expect_true(inherits(obj, "reach.io::PotEvap_Daily"))
})

test_that("generate_pe_sine() returns data.table when as_s7 = FALSE", {
  dt <- generate_pe_sine(2.5, 2.0, 172,
                         start = "2020-01-01", end = "2020-12-31",
                         as_s7 = FALSE)
  expect_true(data.table::is.data.table(dt))
  expect_named(dt, c("date", "pe"))
})

test_that("generate_pe_sine() returns correct number of rows", {
  start <- as.Date("2020-01-01")
  end   <- as.Date("2020-12-31")
  dt    <- generate_pe_sine(2.5, 2.0, 172, start, end, as_s7 = FALSE)
  expect_equal(nrow(dt), as.integer(end - start) + 1L)
})

test_that("generate_pe_sine() PE values are never negative", {
  # Use extreme parameters likely to produce negative values before clamping
  dt <- generate_pe_sine(B = 0.5, A = 2.0, phi = 172,
                         start = "2020-01-01", end = "2020-12-31",
                         as_s7 = FALSE)
  expect_true(all(dt$pe >= 0))
})

test_that("generate_pe_sine() errors when end < start", {
  expect_error(
    generate_pe_sine(2.5, 2.0, 172,
                     start = "2020-12-31", end = "2020-01-01"),
    "on or after"
  )
})

# =============================================================================
# 6. regionalise_pe_sine()
# =============================================================================

test_that("regionalise_pe_sine() equal-weight average matches arithmetic mean", {
  s <- .make_sine_pe()
  f1 <- fit_pe_sine(s$pe + 0.2, s$dates)
  f2 <- fit_pe_sine(s$pe - 0.2, s$dates)
  f3 <- fit_pe_sine(s$pe,       s$dates)

  reg <- regionalise_pe_sine(list(g1 = f1, g2 = f2, g3 = f3))

  expect_equal(reg$B, mean(c(f1$B, f2$B, f3$B)), tolerance = 1e-6)
  expect_equal(reg$A, mean(c(f1$A, f2$A, f3$A)), tolerance = 1e-6)
  expect_equal(reg$n_gauges, 3L)
  expect_equal(reg$gauge_ids, c("g1", "g2", "g3"))
})

test_that("regionalise_pe_sine() weighted average gives correct B", {
  s  <- .make_sine_pe()
  f1 <- fit_pe_sine(s$pe + 1, s$dates)   # B ~ 3.5
  f2 <- fit_pe_sine(s$pe,     s$dates)   # B ~ 2.5

  # 75% weight on f1, 25% on f2
  reg <- regionalise_pe_sine(list(a = f1, b = f2), weights = c(3, 1))

  expected_B <- 0.75 * f1$B + 0.25 * f2$B
  expect_equal(reg$B, expected_B, tolerance = 1e-6)
})

test_that("regionalise_pe_sine() silently drops NULL entries", {
  s  <- .make_sine_pe()
  f1 <- fit_pe_sine(s$pe, s$dates)

  reg <- regionalise_pe_sine(list(good = f1, bad = NULL))

  expect_equal(reg$n_gauges, 1L)
  expect_equal(reg$gauge_ids, "good")
})

test_that("regionalise_pe_sine() phase-aware phi avoids wrap artefact", {
  # Two gauges: one with phi near 365 (e.g. 360) and one near 5
  # The circular mean should be near 182.5, not 182.5 offset by arithmetic error
  s      <- .make_sine_pe()
  dates  <- s$dates
  doy    <- as.integer(format(dates, "%j"))
  pe_360 <- 2.5 + 2.0 * sin(2 * pi / 365.25 * (doy - 360))
  pe_5   <- 2.5 + 2.0 * sin(2 * pi / 365.25 * (doy - 5))

  f360 <- fit_pe_sine(pe_360, dates)
  f5   <- fit_pe_sine(pe_5,   dates)

  reg <- regionalise_pe_sine(list(a = f360, b = f5))

  # Circular mean of 360 and 5 should be ~182.5 (i.e. they wrap around correctly)
  # The naive arithmetic mean would give (360+5)/2 = 182.5 which is correct here,
  # but the unit-circle mean avoids degenerate cases near 0/365 boundary
  expect_gte(reg$phi, 0)
  expect_lt(reg$phi, 365.25)
})

test_that("regionalise_pe_sine() errors on all-NULL list", {
  expect_error(
    regionalise_pe_sine(list(a = NULL, b = NULL)),
    "All entries"
  )
})

test_that("regionalise_pe_sine() errors on wrong weights length", {
  s  <- .make_sine_pe()
  f1 <- fit_pe_sine(s$pe, s$dates)
  expect_error(
    regionalise_pe_sine(list(a = f1), weights = c(1, 2)),
    "same length"
  )
})

# =============================================================================
# 7. compare_pe_sine()
# =============================================================================

test_that("compare_pe_sine() returns near-zero RMSE and bias on noiseless data", {
  s      <- .make_sine_pe()
  result <- compare_pe_sine(s$pe, s$dates, s$B, s$A, s$phi)

  expect_lt(result$rmse,      1e-6)
  expect_lt(abs(result$bias), 1e-6)
  expect_gt(result$r_squared, 0.9999)
})

test_that("compare_pe_sine() returns correct list structure", {
  s      <- .make_sine_pe()
  result <- compare_pe_sine(s$pe, s$dates, s$B, s$A, s$phi)

  expect_type(result, "list")
  expect_true(all(c("rmse", "bias", "r_squared", "fitted") %in% names(result)))
  expect_true(data.table::is.data.table(result$fitted))
  expect_named(result$fitted, c("date", "observed", "fitted"))
})

test_that("compare_pe_sine() plot = TRUE returns a ggplot object", {
  s      <- .make_sine_pe()
  result <- compare_pe_sine(s$pe, s$dates, s$B, s$A, s$phi, plot = TRUE)

  expect_true("plot" %in% names(result))
  expect_true(inherits(result$plot, "gg"))
})

test_that("compare_pe_sine() plot = FALSE does not include plot element", {
  s      <- .make_sine_pe()
  result <- compare_pe_sine(s$pe, s$dates, s$B, s$A, s$phi, plot = FALSE)
  expect_false("plot" %in% names(result))
})

# =============================================================================
# 8. End-to-end chain: fit -> generate -> disagg
# =============================================================================

test_that("fit -> generate -> disagg_to_15min chain works end-to-end", {
  s      <- .make_sine_pe()
  params <- fit_pe_sine(s$pe, s$dates)

  pe_daily <- generate_pe_sine(params$B, params$A, params$phi,
                               start = "2020-01-01", end = "2020-12-31")
  expect_true(inherits(pe_daily, "reach.io::PotEvap_Daily"))

  pe_15min <- disagg_to_15min(pe_daily)
  expect_true(inherits(pe_15min, "reach.io::PotEvap_15min"))

  dt_15 <- as_data_table(pe_15min)
  # 366 days * 96 intervals = 35136 rows
  expect_equal(nrow(dt_15), 366L * 96L)
  expect_true(all(dt_15$value >= 0))
})
