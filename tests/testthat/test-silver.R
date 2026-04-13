# ============================================================
# Tests: Silver Tier QC Promotion
# ============================================================

library(data.table)

# ---- qc_negative ------------------------------------------------------------

test_that("qc_negative flags values below zero", {
  expect_equal(qc_negative(c(-1, 0, 1)), c(TRUE, FALSE, FALSE))
})

test_that("qc_negative does not flag zero", {
  expect_false(qc_negative(0))
})

test_that("qc_negative returns FALSE for NA", {
  expect_false(qc_negative(NA_real_))
})

# ---- qc_relative_spike ------------------------------------------------------

test_that("qc_relative_spike flags a ten-fold increase", {
  v    <- c(1, 11)
  flag <- qc_relative_spike(v, relative_spike_ratio = 10, min_baseline = 0.1)
  expect_equal(flag, c(FALSE, TRUE))
})

test_that("qc_relative_spike does not flag a moderate increase", {
  v    <- c(1, 5)
  flag <- qc_relative_spike(v, relative_spike_ratio = 10, min_baseline = 0.1)
  expect_equal(flag, c(FALSE, FALSE))
})

test_that("qc_relative_spike suppresses check below min_baseline", {
  # Preceding value is below the baseline — check should not fire
  v    <- c(0.05, 1.0)
  flag <- qc_relative_spike(v, relative_spike_ratio = 10, min_baseline = 0.1)
  expect_equal(flag, c(FALSE, FALSE))
})

test_that("qc_relative_spike returns all FALSE for length-1 input", {
  expect_false(qc_relative_spike(42))
})

# ---- qc_absolute_spike ------------------------------------------------------

test_that("qc_absolute_spike flags a large isolated jump", {
  # Stable series with one large spike
  set.seed(1L)
  v       <- c(rep(5, 50), 500, rep(5, 49))
  flag    <- qc_absolute_spike(v, absolute_spike_k = 5, min_spike_flow = 1)
  expect_true(flag[51L])   # spike position flagged
  expect_true(sum(flag) <= 3L)  # only the spike region flagged
})

test_that("qc_absolute_spike does not flag a flat series", {
  v    <- rep(10, 20)
  flag <- qc_absolute_spike(v)
  expect_true(all(!flag))
})

test_that("qc_absolute_spike returns all FALSE for length-1 input", {
  expect_false(qc_absolute_spike(10))
})

test_that("qc_absolute_spike suppresses check below min_spike_flow", {
  # Spike occurs at a very low flow level
  v    <- c(rep(0.01, 10), 0.5, rep(0.01, 10))
  flag <- qc_absolute_spike(v, min_spike_flow = 1.0)
  expect_true(all(!flag))
})

# ---- qc_drop ----------------------------------------------------------------

test_that("qc_drop flags a near-complete collapse from high flow", {
  v    <- c(10, 0.1)
  flag <- qc_drop(v, drop_ratio = 0.9, min_flow_for_drop = 1.0)
  expect_equal(flag, c(FALSE, TRUE))
})

test_that("qc_drop does not flag a gradual decrease", {
  v    <- c(10, 8)
  flag <- qc_drop(v, drop_ratio = 0.9, min_flow_for_drop = 1.0)
  expect_equal(flag, c(FALSE, FALSE))
})

test_that("qc_drop suppresses check when preceding value is below threshold", {
  v    <- c(0.5, 0.0)
  flag <- qc_drop(v, drop_ratio = 0.9, min_flow_for_drop = 1.0)
  expect_equal(flag, c(FALSE, FALSE))
})

test_that("qc_drop returns all FALSE for length-1 input", {
  expect_false(qc_drop(10))
})

# ---- qc_fluctuation ---------------------------------------------------------

test_that("qc_fluctuation flags rapid alternating series", {
  # Perfectly alternating series: 1, 5, 1, 5, ... triggers fluctuation
  v    <- rep(c(1, 5), 10)
  flag <- qc_fluctuation(v, fluctuation_window = 8L, fluctuation_min_reversals = 4L)
  expect_true(any(flag))
})

test_that("qc_fluctuation does not flag a monotone series", {
  v    <- seq(1, 20)
  flag <- qc_fluctuation(v)
  expect_true(all(!flag))
})

test_that("qc_fluctuation returns all FALSE for short series", {
  v    <- c(1, 2, 1)
  flag <- qc_fluctuation(v, fluctuation_window = 8L, fluctuation_min_reversals = 4L)
  expect_true(all(!flag))
})

# ---- qc_truncation_low ------------------------------------------------------

test_that("qc_truncation_low flags long runs at low flow", {
  # 10 identical low-flow values at the 5th percentile of the series
  v    <- c(rep(0.1, 10), seq(1, 90))
  flag <- qc_truncation_low(v, truncation_min_run = 4L,
                             truncation_low_quantile = 0.1)
  expect_true(any(flag[1:10]))
})

test_that("qc_truncation_low does not flag short runs", {
  v    <- c(0.1, 0.1, 0.1, seq(1, 97))
  flag <- qc_truncation_low(v, truncation_min_run = 4L,
                             truncation_low_quantile = 0.1)
  expect_true(all(!flag[1:3]))
})

test_that("qc_truncation_low does not flag identical values at mid range", {
  v    <- c(seq(1, 40), rep(50, 10), seq(60, 99))
  flag <- qc_truncation_low(v, truncation_min_run = 4L,
                             truncation_low_quantile = 0.1)
  expect_true(all(!flag[41:50]))
})

# ---- qc_truncation_high -----------------------------------------------------

test_that("qc_truncation_high flags long runs at high flow", {
  v    <- c(seq(1, 90), rep(100, 10))
  flag <- qc_truncation_high(v, truncation_min_run = 4L,
                              truncation_high_quantile = 0.9)
  expect_true(any(flag[91:100]))
})

test_that("qc_truncation_high does not flag short runs", {
  v    <- c(seq(1, 97), 100, 100, 100)
  flag <- qc_truncation_high(v, truncation_min_run = 4L,
                              truncation_high_quantile = 0.9)
  expect_true(all(!flag[98:100]))
})

# ---- apply_y_digit ----------------------------------------------------------

test_that("apply_y_digit returns 0 for a clean series", {
  v <- seq(1, 10, by = 0.5)
  y <- apply_y_digit(v)
  expect_true(all(y == 0L))
})

test_that("apply_y_digit assigns code 1 for negative values when allow_negative = FALSE", {
  v <- c(5, -1, 5)
  y <- apply_y_digit(v, allow_negative = FALSE)
  expect_equal(y[2L], 1L)
})

test_that("apply_y_digit does not flag negative values when allow_negative = TRUE", {
  v <- c(5, -1, 5)
  y <- apply_y_digit(v, allow_negative = TRUE)
  expect_false(y[2L] == 1L)
})

test_that("apply_y_digit assigns code 8 when relative and absolute spike coincide", {
  # A large single-step jump should trigger both relative and absolute spike
  v <- c(rep(2, 50), 500, rep(2, 49))
  y <- apply_y_digit(v, relative_spike_ratio = 10, min_baseline = 0.1,
                      absolute_spike_k = 5, min_spike_flow = 1.0)
  expect_equal(y[51L], 8L)
})

test_that("apply_y_digit returns integer vector", {
  y <- apply_y_digit(c(1, 2, 3))
  expect_type(y, "integer")
})

test_that("apply_y_digit output length matches input", {
  v <- runif(200)
  y <- apply_y_digit(v)
  expect_equal(length(y), 200L)
})

# ---- y_to_qc_flag -----------------------------------------------------------

test_that("y_to_qc_flag maps 0 to Good (1)", {
  expect_equal(y_to_qc_flag(0L), 1L)
})

test_that("y_to_qc_flag maps suspect codes to 3", {
  expect_true(all(y_to_qc_flag(2L:7L) == 3L))
})

test_that("y_to_qc_flag maps rejected codes to 4", {
  expect_true(all(y_to_qc_flag(c(1L, 8L, 9L)) == 4L))
})

test_that("y_to_qc_flag never returns 2 (Estimated is manual-only)", {
  all_y <- 0L:9L
  expect_true(all(y_to_qc_flag(all_y) != 2L))
})

# ---- silver_path ------------------------------------------------------------

test_that("silver_path builds correct path", {
  p <- silver_path("data/hydro", "hydrometric", "Q", "EA_39001_Q_20260115")
  expect_equal(p,
    "data/hydro/silver/hydrometric/Q/2026/EA_39001_Q_20260115.parquet")
})

# ---- promote_to_silver ------------------------------------------------------

# Helper: minimal Bronze data.table for two sites
make_bronze_dt <- function() {
  data.table::data.table(
    timestamp     = as.POSIXct(
      rep(seq(as.Date("2022-01-01"), by = "15 min",
              length.out = 100), 2),
      tz = "UTC"
    ),
    value         = c(runif(100, 1, 10), runif(100, 1, 10)),
    supplier_flag = NA_character_,
    dataset_id    = rep(c("EA_39001_Q_20220101",
                          "EA_39002_Q_20220101"), each = 100L),
    site_id       = rep(c("39001", "39002"), each = 100L),
    data_type     = "Q"
  )
}

test_that("promote_to_silver adds all required Silver columns", {
  dt  <- make_bronze_dt()
  out <- promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE)
  expect_true(all(c("qc_flag", "qc_value", "qc_y_code",
                    "qc_flagged_at") %in% names(out)))
})

test_that("promote_to_silver qc_flag is never NA", {
  dt  <- make_bronze_dt()
  out <- promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE)
  expect_false(anyNA(out$qc_flag))
})

test_that("promote_to_silver qc_value equals value where qc_flag is Good (1)", {
  dt  <- make_bronze_dt()
  out <- promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE)
  good <- out[qc_flag == 1L]
  expect_equal(good$qc_value, good$value)
})

test_that("promote_to_silver qc_value is NA where qc_flag is Rejected (4)", {
  dt  <- make_bronze_dt()
  out <- promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE)
  rejected <- out[qc_flag == 4L]
  if (nrow(rejected) > 0L) expect_true(all(is.na(rejected$qc_value)))
})

test_that("promote_to_silver flags negative flow as Rejected when allow_negative = FALSE", {
  dt          <- make_bronze_dt()
  dt$value[5] <- -1.0
  out <- promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE,
                            allow_negative = FALSE)
  expect_equal(out$qc_flag[5L], 4L)
})

test_that("promote_to_silver accepts negative flow when allow_negative = TRUE", {
  dt          <- make_bronze_dt()
  dt$value[5] <- -1.0
  out <- promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE,
                            allow_negative = TRUE)
  expect_false(out$qc_flag[5L] == 4L)
})

test_that("promote_to_silver errors on missing Bronze columns", {
  dt <- data.table::data.table(timestamp = Sys.time(), value = 1)
  expect_error(promote_to_silver(dt, tempdir(), write_output = FALSE),
               "missing column")
})

test_that("promote_to_silver errors on non-existent file path", {
  expect_error(
    promote_to_silver("does/not/exist.parquet", tempdir()),
    "Bronze file not found"
  )
})

test_that("promote_to_silver errors on invalid bronze_data type", {
  expect_error(promote_to_silver(42L, tempdir()), "file path.*data.table")
})

test_that("promote_to_silver writes Parquet when write_output = TRUE", {
  tmp <- tempfile()
  dir.create(tmp)
  dt  <- make_bronze_dt()
  promote_to_silver(dt, output_dir = tmp, write_output = TRUE)
  silver_files <- list.files(tmp, pattern = "\\.parquet$", recursive = TRUE)
  expect_true(length(silver_files) > 0L)
})

test_that("promote_to_silver returns data.table invisibly when writing", {
  tmp <- tempfile()
  dir.create(tmp)
  dt  <- make_bronze_dt()
  out <- promote_to_silver(dt, output_dir = tmp, write_output = TRUE)
  expect_true(data.table::is.data.table(out))
})

test_that("promote_to_silver applies checks independently per site_id", {
  # A spike in site 39001 should not affect site 39002
  dt          <- make_bronze_dt()
  dt$value[50] <- 999  # spike in site 39001 only
  out <- promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE)
  site2_flags <- out[site_id == "39002", qc_flag]
  expect_true(all(site2_flags == 1L))
})

# ---- deduplication ----------------------------------------------------------

test_that("promote_to_silver removes duplicate (site_id, timestamp) rows", {
  dt       <- make_bronze_dt()
  dt_duped <- rbind(dt, dt[1:10])
  expect_warning(
    out <- promote_to_silver(dt_duped, output_dir = tempdir(),
                              write_output = FALSE, dedup = TRUE),
    "duplicate"
  )
  expect_equal(nrow(out), nrow(dt))
})

test_that("promote_to_silver preserves all rows when dedup = FALSE", {
  dt       <- make_bronze_dt()
  dt_duped <- rbind(dt, dt[1:5])
  out <- suppressWarnings(
    promote_to_silver(dt_duped, output_dir = tempdir(),
                      write_output = FALSE, dedup = FALSE)
  )
  expect_equal(nrow(out), nrow(dt_duped))
})

# ---- gap annotation ---------------------------------------------------------

test_that("promote_to_silver attaches gap_counts attribute when annotate_gaps = TRUE", {
  dt  <- make_bronze_dt()
  out <- suppressWarnings(
    promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE,
                      annotate_gaps = TRUE)
  )
  gc <- attr(out, "gap_counts")
  expect_false(is.null(gc))
  expect_true(is.integer(gc))
  expect_named(gc)
})

test_that("gap_counts has one entry per site_id", {
  dt  <- make_bronze_dt()
  out <- suppressWarnings(
    promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE,
                      annotate_gaps = TRUE)
  )
  expect_setequal(names(attr(out, "gap_counts")), dt[, unique(site_id)])
})

test_that("promote_to_silver does not attach gap_counts when annotate_gaps = FALSE", {
  dt  <- make_bronze_dt()
  out <- suppressWarnings(
    promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE,
                      annotate_gaps = FALSE)
  )
  expect_null(attr(out, "gap_counts"))
})


# ---- Stage (H) QC checks — planned ------------------------------------------
# These stubs document the intended Option A checks for H data.
# Implementation is deferred until UK-Flow15 (Fileni et al., 2026) is
# peer-reviewed and the check logic is finalised.

test_that("qc_h: stage outside plausible datum range is flagged as Suspect", {
  skip("H QC not yet implemented — awaiting UK-Flow15 peer review")
})

test_that("qc_h: sudden spike then return (debris/ice/sensor contact) is flagged as Suspect", {
  skip("H QC not yet implemented — awaiting UK-Flow15 peer review")
})

test_that("qc_h: rate of change exceeding physical bound is flagged as Suspect", {
  skip("H QC not yet implemented — awaiting UK-Flow15 peer review")
})

test_that("qc_h: flat-lined stage over extended period is flagged as Suspect", {
  skip("H QC not yet implemented — awaiting UK-Flow15 peer review")
})

test_that("qc_h: stage rising while flow drops (sustained) is flagged as Suspect", {
  skip("H QC not yet implemented — awaiting UK-Flow15 peer review (requires paired Q+H series)")
})

test_that("qc_h: persistent step-change offset (datum shift) is flagged", {
  skip("H QC not yet implemented — awaiting UK-Flow15 peer review")
})


# ---- Rainfall (P) QC checks — planned ----------------------------------------
# These stubs document the intended Option A checks for P data.
# Implementation is deferred until UK-Flow15 (Fileni et al., 2026) is
# peer-reviewed and the check logic is finalised.

test_that("qc_p: negative rainfall value is flagged as Rejected", {
  skip("P QC not yet implemented — awaiting UK-Flow15 peer review")
})

test_that("qc_p: 15-min intensity exceeding credible cap (>50 mm/15 min) is flagged as Suspect", {
  skip("P QC not yet implemented — awaiting UK-Flow15 peer review")
})

test_that("qc_p: rolling 24-h accumulation exceeding UK cap (~300 mm) is flagged as Suspect", {
  skip("P QC not yet implemented — awaiting UK-Flow15 peer review")
})

test_that("qc_p: extended dry-run during known wet period is flagged as Suspect", {
  skip("P QC not yet implemented — awaiting UK-Flow15 peer review (requires wet-period mask)")
})

test_that("qc_p: tipping bucket counter overflow (negative increment) is flagged or corrected", {
  skip("P QC not yet implemented — awaiting UK-Flow15 peer review")
})

test_that("qc_p: rain at site while all neighbouring gauges show zero is flagged as Suspect", {
  skip("P QC not yet implemented — awaiting UK-Flow15 peer review (requires neighbouring gauges)")
})


# ---- Rating extrapolation flag — planned -------------------------------------

test_that("stage outside valid rating curve range sets extrapolated = TRUE", {
  skip("Rating extrapolation flag not yet implemented — requires operational rating curve store")
})
