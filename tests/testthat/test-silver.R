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


# ---- Off-grid timestamp resolution ------------------------------------------

test_that(".snap_offgrid_timestamps returns dt unchanged when all timestamps are on-grid", {
  dt <- data.table::data.table(
    site_id   = "A",
    timestamp = seq(as.POSIXct("2022-01-01 00:00", tz = "UTC"),
                    by = "15 min", length.out = 8L),
    value     = 1:8
  )
  out <- .snap_offgrid_timestamps(data.table::copy(dt))
  expect_equal(nrow(out), 8L)
  expect_true(all(!out$.snapped))
})

test_that(".snap_offgrid_timestamps drops off-grid row when on-grid slot is occupied", {
  ts_base <- as.POSIXct("2022-01-01 00:00", tz = "UTC")
  dt <- data.table::data.table(
    site_id   = "A",
    timestamp = ts_base + c(0L, 60L, 900L),   # :00, :01, :15
    value     = c(1.0, 1.1, 2.0)
  )
  out <- suppressWarnings(.snap_offgrid_timestamps(data.table::copy(dt)))
  expect_equal(nrow(out), 2L)
  expect_equal(sort(out$timestamp), ts_base + c(0L, 900L))
  expect_true(all(!out$.snapped))
})

test_that(".snap_offgrid_timestamps snaps off-grid row when target slot is empty", {
  ts_base <- as.POSIXct("2022-01-01 00:00", tz = "UTC")
  dt <- data.table::data.table(
    site_id   = "A",
    timestamp = ts_base + c(60L, 900L),   # :01, :15 — no :00
    value     = c(1.0, 2.0)
  )
  out <- suppressWarnings(.snap_offgrid_timestamps(data.table::copy(dt)))
  expect_equal(nrow(out), 2L)
  expect_equal(nrow(out[.snapped == TRUE]), 1L)
  expect_equal(out[.snapped == TRUE, timestamp], ts_base)   # snapped :01 → :00
})

test_that(".snap_offgrid_timestamps handles the mixed case: 00, 01, 15, 30, 43, 45", {
  ts_base <- as.POSIXct("2022-01-01 00:00", tz = "UTC")
  # :01 snaps to :00 (occupied) → dropped; :43 snaps to :45 (occupied) → dropped
  dt <- data.table::data.table(
    site_id   = "A",
    timestamp = ts_base + c(0L, 60L, 900L, 1800L, 2580L, 2700L),
    value     = seq_len(6L) * 1.0
  )
  out <- suppressWarnings(.snap_offgrid_timestamps(data.table::copy(dt)))
  expect_equal(nrow(out), 4L)
  expect_true(all(!out$.snapped))
  expect_equal(sort(out$timestamp), ts_base + c(0L, 900L, 1800L, 2700L))
})

test_that("promote_to_silver sets qc_flag = 2 for a snapped off-grid timestamp", {
  ts_base <- as.POSIXct("2022-01-01 00:00", tz = "UTC")
  # :01 snaps to :00 (empty) and should emerge with qc_flag = 2
  dt <- data.table::data.table(
    timestamp     = ts_base + c(60L, 900L, 1800L, 2700L),
    value         = c(1.5, 1.6, 1.7, 1.8),
    supplier_flag = NA_character_,
    dataset_id    = "EA_39001_Q_20220101",
    site_id       = "39001",
    data_type     = "Q"
  )
  out <- suppressWarnings(
    promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE,
                      annotate_gaps = FALSE)
  )
  snapped_row <- out[timestamp == ts_base]
  expect_equal(nrow(snapped_row), 1L)
  expect_equal(snapped_row$qc_flag, 2L)
})

test_that("promote_to_silver drops off-grid row when on-grid slot is occupied", {
  n       <- 100L
  ts_base <- as.POSIXct("2022-01-01 00:00", tz = "UTC")
  on_grid <- seq(ts_base, by = "15 min", length.out = n)
  dt <- data.table::data.table(
    timestamp     = sort(c(on_grid, ts_base + 60L)),   # inject :01 off-grid
    value         = runif(n + 1L),
    supplier_flag = NA_character_,
    dataset_id    = "EA_39001_Q_20220101",
    site_id       = "39001",
    data_type     = "Q"
  )
  out <- suppressWarnings(
    promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE,
                      annotate_gaps = FALSE)
  )
  expect_equal(nrow(out), n)
})


# ---- Stage / Level (H) QC checks --------------------------------------------

# Helper: 200-row Bronze data.table for stage data
make_h_bronze_dt <- function(n = 200L) {
  data.table::data.table(
    timestamp     = seq(as.POSIXct("2022-01-01", tz = "UTC"),
                        by = "15 min", length.out = n),
    value         = runif(n, 0.5, 3.0),
    supplier_flag = NA_character_,
    dataset_id    = "EA_39001_H_20220101",
    site_id       = "39001",
    data_type     = "H"
  )
}

test_that("qc_h_range flags values outside plausible datum bounds", {
  v    <- c(-0.5, 0.0, 1.0, 5.0, 10.5)
  flag <- qc_h_range(v, min_datum = 0.0, max_credible = 10.0)
  expect_equal(flag, c(TRUE, FALSE, FALSE, FALSE, TRUE))
})

test_that("qc_h_range returns all FALSE when no bounds are set", {
  expect_true(all(!qc_h_range(c(-99, 0, 99))))
})

test_that("qc_h_range returns FALSE for NA", {
  expect_false(qc_h_range(NA_real_, min_datum = 0, max_credible = 5))
})

test_that("qc_h_spike flags a sudden spike-and-return", {
  v    <- c(rep(1.0, 20), 8.0, 1.1, rep(1.0, 20))
  flag <- qc_h_spike(v, spike_k = 3, spike_return_window = 3L)
  expect_true(any(flag[21:22]))
  expect_true(all(!flag[1:20]))
})

test_that("qc_h_spike does not flag a sustained high level", {
  v    <- c(rep(1.0, 10), rep(5.0, 40), rep(1.0, 10))
  flag <- qc_h_spike(v, spike_k = 3, spike_return_window = 3L)
  expect_true(all(!flag[12:49]))
})

test_that("qc_h_rate_of_change flags an implausible single-step rise", {
  v    <- c(rep(1.0, 10), 3.0, rep(3.0, 10))
  flag <- qc_h_rate_of_change(v, max_rise_per_step = 1.0)
  expect_true(flag[11L])
  expect_true(all(!flag[1:10]))
})

test_that("qc_h_rate_of_change flags an implausible single-step fall", {
  v    <- c(rep(3.0, 10), 1.0, rep(1.0, 10))
  flag <- qc_h_rate_of_change(v, max_fall_per_step = 1.0)
  expect_true(flag[11L])
})

test_that("qc_h_rate_of_change does not flag gradual changes", {
  v    <- seq(1.0, 2.0, length.out = 20)
  flag <- qc_h_rate_of_change(v, max_rise_per_step = 0.5)
  expect_true(all(!flag))
})

test_that("qc_h_flatline flags runs of identical stage values", {
  v    <- c(runif(20, 0.5, 1.5), rep(1.234, 10), runif(20, 0.5, 1.5))
  flag <- qc_h_flatline(v, flatline_min_run = 4L)
  expect_true(all(flag[21:30]))
})

test_that("qc_h_flatline does not flag short runs", {
  v    <- c(1.0, 1.0, 1.0, runif(20, 0.5, 1.5))
  flag <- qc_h_flatline(v, flatline_min_run = 4L)
  expect_true(all(!flag[1:3]))
})

test_that("qc_h_stage_flow_consistency flags sustained stage-up / flow-down", {
  stage <- c(seq(1.0, 1.5, length.out = 10L),
             seq(1.5, 1.0, length.out = 10L),
             rep(1.0, 10L))
  flow  <- c(seq(5.0, 3.0, length.out = 10L),
             seq(3.0, 5.0, length.out = 10L),
             rep(5.0, 10L))
  flag  <- qc_h_stage_flow_consistency(stage, flow, consistency_window = 4L)
  expect_true(any(flag[1:10]))
  expect_true(all(!flag[11:30]))
})

test_that("qc_h_stage_flow_consistency errors when lengths differ", {
  expect_error(qc_h_stage_flow_consistency(1:10, 1:9), "same length")
})

test_that("qc_h_datum_shift flags a persistent step-change in stage", {
  v    <- c(rep(1.0, 50), rep(1.8, 50))
  flag <- qc_h_datum_shift(v, shift_window = 20L, shift_threshold = 0.3)
  expect_true(any(flag[51:70]))
  expect_true(all(!flag[1:30]))
})

test_that("promote_to_silver dispatches H checks for data_type = 'H'", {
  dt  <- make_h_bronze_dt()
  out <- suppressWarnings(
    promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE)
  )
  expect_true("qc_h_code" %in% names(out))
  expect_false("qc_y_code" %in% names(out))
  expect_false(anyNA(out$qc_flag))
})


# ---- Rainfall (P) QC checks -------------------------------------------------

# Helper: 200-row Bronze data.table for rainfall data
make_p_bronze_dt <- function(n = 200L) {
  data.table::data.table(
    timestamp     = seq(as.POSIXct("2022-01-01", tz = "UTC"),
                        by = "15 min", length.out = n),
    value         = runif(n, 0, 2),
    supplier_flag = NA_character_,
    dataset_id    = "EA_39001_P_20220101",
    site_id       = "39001",
    data_type     = "P"
  )
}

test_that("qc_p_negative flags values below zero", {
  expect_equal(qc_p_negative(c(-0.5, 0.0, 1.2)), c(TRUE, FALSE, FALSE))
})

test_that("qc_p_negative returns FALSE for NA", {
  expect_false(qc_p_negative(NA_real_))
})

test_that("qc_p_intensity flags values exceeding the cap", {
  flag <- qc_p_intensity(c(1.0, 25.0, 75.0), max_intensity_15min = 50)
  expect_equal(flag, c(FALSE, FALSE, TRUE))
})

test_that("qc_p_intensity does not flag values at or below the cap", {
  expect_false(qc_p_intensity(50.0, max_intensity_15min = 50))
})

test_that("qc_p_daily_cap flags windows exceeding the 24-h total", {
  v    <- rep(4.0, 200L)   # 4 × 96 = 384 mm — exceeds 300 mm cap
  flag <- qc_p_daily_cap(v, max_daily_mm = 300, steps_per_day = 96L)
  expect_true(any(flag))
})

test_that("qc_p_daily_cap does not flag a series well below the cap", {
  v    <- rep(0.5, 200L)   # 0.5 × 96 = 48 mm/day
  flag <- qc_p_daily_cap(v, max_daily_mm = 300, steps_per_day = 96L)
  expect_true(all(!flag))
})

test_that("qc_p_dry_run flags extended zeros during wet periods", {
  v    <- c(rep(0.0, 20L), rep(0.5, 30L))
  mask <- rep(TRUE, 50L)
  flag <- qc_p_dry_run(v, wet_period_mask = mask, dry_run_min_steps = 10L)
  expect_true(all(flag[1:20]))
  expect_true(all(!flag[21:50]))
})

test_that("qc_p_dry_run does not flag zeros outside wet periods", {
  v    <- rep(0.0, 30L)
  mask <- rep(FALSE, 30L)
  expect_true(all(!qc_p_dry_run(v, wet_period_mask = mask, dry_run_min_steps = 5L)))
})

test_that("qc_p_dry_run errors when mask length differs from value", {
  expect_error(qc_p_dry_run(1:10, wet_period_mask = rep(TRUE, 5)), "same length")
})

test_that("qc_p_reset flags a large isolated spike followed by return to normal", {
  v    <- c(rep(0.2, 30L), 55.0, 0.2, rep(0.2, 30L))
  flag <- qc_p_reset(v, reset_spike_k = 5, reset_return_window = 2L)
  expect_true(flag[31L])
  expect_true(all(!flag[1:30]))
})

test_that("qc_p_reset does not flag all of a sustained high-rainfall block", {
  v    <- c(rep(0.2, 10L), rep(5.0, 20L), rep(0.2, 10L))
  flag <- qc_p_reset(v, reset_spike_k = 5, reset_return_window = 2L)
  expect_true(sum(flag) < 5L)
})

test_that("qc_p_temporal_consistency flags rain when all neighbours show zero", {
  n    <- 50L
  v    <- c(rep(0.0, 20L), rep(2.0, 10L), rep(0.0, 20L))
  nbrs <- matrix(0.0, nrow = n, ncol = 3L)
  flag <- qc_p_temporal_consistency(v, neighbours = nbrs,
                                     isolation_threshold = 0.1,
                                     isolation_fraction  = 0.8)
  expect_true(all(flag[21:30]))
  expect_true(all(!flag[1:20]))
})

test_that("qc_p_temporal_consistency does not flag rain when neighbours also show rain", {
  n    <- 30L
  v    <- rep(2.0, n)
  nbrs <- matrix(1.5, nrow = n, ncol = 3L)
  expect_true(all(!qc_p_temporal_consistency(v, neighbours = nbrs,
                                              isolation_threshold = 0.1,
                                              isolation_fraction  = 0.8)))
})

test_that("qc_p_temporal_consistency errors when neighbour rows differ from value length", {
  expect_error(
    qc_p_temporal_consistency(1:10, neighbours = matrix(0, nrow = 5, ncol = 2)),
    "same number of rows"
  )
})

test_that("promote_to_silver dispatches P checks for data_type = 'P'", {
  dt  <- make_p_bronze_dt()
  out <- suppressWarnings(
    promote_to_silver(dt, output_dir = tempdir(), write_output = FALSE)
  )
  expect_true("qc_p_code" %in% names(out))
  expect_false("qc_y_code" %in% names(out))
  expect_false(anyNA(out$qc_flag))
})


# ---- Rating extrapolation flag — planned -------------------------------------

test_that("stage outside valid rating curve range sets extrapolated = TRUE", {
  skip("Rating extrapolation flag not yet implemented — requires operational rating curve store")
})
