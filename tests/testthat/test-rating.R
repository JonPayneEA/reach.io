# ============================================================
# Tests: Rating curves and rated flow
# ============================================================

library(data.table)

# -- helpers -------------------------------------------------------------------

make_limbs <- function() {
  data.table(
    lower    = c(0.000, 0.095, 2.000),
    upper    = c(0.095, 2.000,   Inf),
    C        = c(12.2495, 23.252, 23.252),
    a        = c(-0.003, -0.098, -0.098),
    b        = c( 1.641,  2.706,  2.706),
    doubtful = c(FALSE,   FALSE,  TRUE)
  )
}

make_rc <- function() {
  RatingCurve(
    limbs      = make_limbs(),
    valid_from = as.Date("2019-10-01"),
    station_id = "510310",
    source     = "WISKI"
  )
}

make_level_daily <- function(stages) {
  dt <- data.table(
    dateTime          = as.POSIXct("2020-01-01", tz = "UTC") + seq_along(stages) * 86400,
    date              = as.Date("2020-01-01") + seq_along(stages),
    value             = stages,
    measure_notation  = "level"
  )
  Level_Daily(readings = dt, from_date = "2020-01-01", to_date = "2020-12-31")
}

make_level_15min <- function(stages) {
  dt <- data.table(
    dateTime          = as.POSIXct("2020-01-01", tz = "UTC") + seq_along(stages) * 900,
    date              = as.Date("2020-01-01"),
    value             = stages,
    measure_notation  = "level"
  )
  Level_15min(readings = dt, from_date = "2020-01-01", to_date = "2020-01-02")
}

# ==============================================================================
# RatingCurve construction
# ==============================================================================

test_that("RatingCurve constructs from valid limbs", {
  rc <- make_rc()
  expect_true(S7::S7_inherits(rc, RatingCurve))
  expect_equal(nrow(rc@limbs), 3L)
  expect_equal(rc@station_id, "510310")
  expect_equal(rc@source, "WISKI")
})

test_that("RatingCurve rejects missing columns", {
  bad <- data.table(lower = 0, upper = 1, C = 1, a = 0, b = 1)
  expect_error(RatingCurve(limbs = bad), "missing column")
})

test_that("RatingCurve rejects non-contiguous boundaries", {
  bad <- data.table(
    lower = c(0, 1), upper = c(0.5, 2),
    C = c(1, 1), a = c(0, 0), b = c(1, 1),
    doubtful = c(FALSE, FALSE)
  )
  expect_error(RatingCurve(limbs = bad), "not contiguous")
})

test_that("RatingCurve rejects lower >= upper", {
  bad <- data.table(
    lower = 1, upper = 0.5,
    C = 1, a = 0, b = 1, doubtful = FALSE
  )
  expect_error(RatingCurve(limbs = bad), "lower.*less than")
})

test_that("RatingCurve rejects C <= 0", {
  bad <- data.table(
    lower = 0, upper = Inf,
    C = -1, a = 0, b = 1, doubtful = FALSE
  )
  expect_error(RatingCurve(limbs = bad), "positive")
})

test_that("RatingCurve rejects b <= 0", {
  bad <- data.table(
    lower = 0, upper = Inf,
    C = 1, a = 0, b = 0, doubtful = FALSE
  )
  expect_error(RatingCurve(limbs = bad), "positive")
})

test_that("RatingCurve rejects NaN in parameters", {
  bad <- data.table(
    lower = 0, upper = Inf,
    C = NaN, a = 0, b = 1, doubtful = FALSE
  )
  expect_error(RatingCurve(limbs = bad), "finite")
})

test_that("RatingCurve normalises common column name variants", {
  limbs <- data.table(
    lower_level = 0, upper_level = Inf,
    multiplier  = 10, offset = 0, exponent = 2,
    flag        = FALSE
  )
  rc <- RatingCurve(limbs = limbs)
  expect_true(all(c("lower", "upper", "C", "a", "b", "doubtful") %in% names(rc@limbs)))
})

# ==============================================================================
# RatingSet construction
# ==============================================================================

test_that("RatingSet wraps a single RatingCurve", {
  rc <- make_rc()
  rs <- RatingSet(curves = rc, station_id = "510310")
  expect_true(S7::S7_inherits(rs, RatingSet))
  expect_equal(length(rs@curves), 1L)
})

test_that("RatingSet wraps a list of RatingCurves", {
  limbs <- make_limbs()
  rc1 <- RatingCurve(limbs = limbs, valid_from = as.Date("2019-01-01"),
                     valid_to = as.Date("2020-12-31"), station_id = "X")
  rc2 <- RatingCurve(limbs = limbs, valid_from = as.Date("2021-01-01"),
                     station_id = "X")
  rs <- RatingSet(curves = list(rc1, rc2), station_id = "X")
  expect_equal(length(rs@curves), 2L)
})

test_that("RatingSet rejects overlapping validity periods", {
  limbs <- make_limbs()
  rc1 <- RatingCurve(limbs = limbs, valid_from = as.Date("2019-01-01"),
                     valid_to = as.Date("2021-12-31"), station_id = "X")
  rc2 <- RatingCurve(limbs = limbs, valid_from = as.Date("2021-01-01"),
                     station_id = "X")
  expect_error(RatingSet(curves = list(rc1, rc2)), "overlapping")
})

# ==============================================================================
# .rate_stage correctness
# ==============================================================================

test_that("rating equation gives correct Q for known inputs", {
  # Single limb: Q = C * (h - a)^b
  limbs <- data.table(
    lower = 0, upper = Inf,
    C = 10, a = 0, b = 2, doubtful = FALSE
  )
  # h = 3 → Q = 10 * (3 - 0)^2 = 90
  rated <- reach.io:::.rate_stage(3, limbs)
  expect_equal(rated$value, 90)
  expect_false(rated$doubtful)
})

test_that("stage below lowest limb returns NA", {
  limbs <- data.table(
    lower = 1, upper = Inf,
    C = 10, a = 0, b = 2, doubtful = FALSE
  )
  rated <- reach.io:::.rate_stage(0.5, limbs)
  expect_true(is.na(rated$value))
})

test_that("stage at datum (h == a) returns Q = 0", {
  limbs <- data.table(
    lower = 0, upper = Inf,
    C = 10, a = 0.5, b = 2, doubtful = FALSE
  )
  rated <- reach.io:::.rate_stage(0.5, limbs)
  expect_equal(rated$value, 0)
})

test_that("multi-limb rating selects correct limb", {
  limbs <- make_limbs()
  # h = 1.0 falls in limb 2 (0.095 to 2.000)
  rated <- reach.io:::.rate_stage(1.0, limbs)
  expected <- 23.252 * max(1.0 - (-0.098), 0)^2.706
  expect_equal(rated$value, expected, tolerance = 1e-6)
  expect_false(rated$doubtful)
})

test_that("stage in doubtful limb is flagged", {
  limbs <- make_limbs()
  # h = 3.0 falls in limb 3 (doubtful = TRUE)
  rated <- reach.io:::.rate_stage(3.0, limbs)
  expect_true(rated$doubtful)
})

test_that("NA stage returns NA", {
  limbs <- make_limbs()
  rated <- reach.io:::.rate_stage(NA_real_, limbs)
  expect_true(is.na(rated$value))
})

# ==============================================================================
# apply_rating
# ==============================================================================

test_that("apply_rating with Level_Daily returns Flow_Daily", {
  rc    <- make_rc()
  level <- make_level_daily(c(0.5, 1.0, 1.5))
  flow  <- apply_rating(level, rc)
  expect_true(S7::S7_inherits(flow, Flow_Daily))
  expect_equal(nrow(flow@readings), 3L)
  expect_true("doubtful" %in% names(flow@readings))
})

test_that("apply_rating with Level_15min returns Flow_15min", {
  rc    <- make_rc()
  level <- make_level_15min(c(0.5, 1.0, 1.5))
  flow  <- apply_rating(level, rc)
  expect_true(S7::S7_inherits(flow, Flow_15min))
  expect_equal(nrow(flow@readings), 3L)
})

test_that("apply_rating with RatingSet dispatches correctly", {
  limbs <- make_limbs()
  rc1 <- RatingCurve(limbs = limbs, valid_from = as.Date("2019-01-01"),
                     valid_to = as.Date("2020-06-30"), station_id = "X")
  rc2 <- RatingCurve(limbs = limbs, valid_from = as.Date("2020-07-01"),
                     station_id = "X")
  rs    <- RatingSet(curves = list(rc1, rc2), station_id = "X")
  level <- make_level_daily(c(0.5, 1.0, 1.5))
  flow  <- apply_rating(level, rs)
  expect_true(S7::S7_inherits(flow, Flow_Daily))
})

test_that("apply_rating warns about below-limb stages", {
  limbs <- data.table(
    lower = 0.5, upper = Inf,
    C = 10, a = 0, b = 2, doubtful = FALSE
  )
  rc    <- RatingCurve(limbs = limbs)
  level <- make_level_daily(c(0.1, 1.0))
  expect_warning(apply_rating(level, rc), "below the lowest limb")
})

# ==============================================================================
# check_limb_continuity
# ==============================================================================

test_that("check_limb_continuity reports discontinuous junctions", {
  rc  <- make_rc()
  out <- expect_message(check_limb_continuity(rc), "exceed")
  expect_true(data.table::is.data.table(out))
  expect_equal(nrow(out), 2L)
})

test_that("check_limb_continuity reports all connected for perfect limbs", {
  # Build limbs where junction Q matches exactly
  limbs <- data.table(
    lower = c(0, 1), upper = c(1, Inf),
    C = c(10, 10), a = c(0, 0), b = c(2, 2),
    doubtful = c(FALSE, FALSE)
  )
  rc  <- RatingCurve(limbs = limbs)
  out <- expect_message(check_limb_continuity(rc), "All.*connect")
  expect_true(all(out$connected))
})

test_that("check_limb_continuity handles single limb", {
  limbs <- data.table(
    lower = 0, upper = Inf,
    C = 10, a = 0, b = 2, doubtful = FALSE
  )
  rc  <- RatingCurve(limbs = limbs)
  out <- expect_message(check_limb_continuity(rc), "Only one limb")
  expect_equal(nrow(out), 0L)
})

# ==============================================================================
# fix_limb_continuity
# ==============================================================================

test_that("fix_limb_continuity produces continuous junctions", {
  rc      <- make_rc()
  rc_fix  <- expect_message(fix_limb_continuity(rc), "Adjusting")
  cont    <- expect_message(check_limb_continuity(rc_fix, tol_pct = 0.01),
                            "All.*connect")
  expect_true(all(cont$connected))
})

test_that("fix_limb_continuity preserves a and b parameters", {
  rc     <- make_rc()
  rc_fix <- expect_message(fix_limb_continuity(rc), "Adjusting")
  expect_equal(rc_fix@limbs$a, rc@limbs$a)
  expect_equal(rc_fix@limbs$b, rc@limbs$b)
})

test_that("fix_limb_continuity preserves anchor limb C", {
  rc     <- make_rc()
  rc_fix <- expect_message(fix_limb_continuity(rc), "Adjusting")
  expect_equal(rc_fix@limbs$C[1L], rc@limbs$C[1L])
})

# ==============================================================================
# Print methods
# ==============================================================================

test_that("RatingCurve prints formatted output", {
  rc  <- make_rc()
  out <- capture.output(print(rc))
  expect_true(any(grepl("RatingCurve", out)))
  expect_true(any(grepl("510310", out)))
  expect_true(any(grepl("WISKI", out)))
  expect_true(any(grepl("Lower", out)))
})

test_that("RatingSet prints formatted output", {
  rc <- make_rc()
  rs <- RatingSet(curves = rc, station_id = "510310")
  out <- capture.output(print(rs))
  expect_true(any(grepl("RatingSet", out)))
  expect_true(any(grepl("510310", out)))
})
