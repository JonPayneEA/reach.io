# -- Tests for snap_offgrid_timestamps() gating and purity -------------------

snap_input <- function(period = NULL) {
  dt <- data.table::data.table(
    site_id   = "A",
    timestamp = as.POSIXct(c("2022-01-01 00:00", "2022-01-01 00:01", "2022-01-01 00:15",
                             "2022-01-01 00:43"), tz = "UTC"),
    value     = 1:4
  )
  if (!is.null(period)) dt[, period := period]
  dt
}

test_that("snap_offgrid_timestamps drops redundant and snaps isolated off-grid rows", {
  expect_warning(expect_warning(out <- snap_offgrid_timestamps(snap_input())))
  expect_equal(format(out$timestamp, "%H:%M"), c("00:00", "00:15", "00:45"))
  expect_equal(out$.snapped, c(FALSE, FALSE, TRUE))
})

test_that("snap_offgrid_timestamps leaves non-15min periods alone", {
  for (p in c("hourly", "daily", "5min")) {
    out <- snap_offgrid_timestamps(snap_input(p))
    expect_equal(nrow(out), 4L)
    expect_false(any(out$.snapped))
    expect_equal(format(out$timestamp, "%H:%M"), c("00:00", "00:01", "00:15", "00:43"))
  }
})

test_that("snap_offgrid_timestamps treats 15min and NA periods as 15-minute data", {
  expect_warning(expect_warning(out <- snap_offgrid_timestamps(snap_input("15min"))))
  expect_equal(nrow(out), 3L)
  expect_warning(expect_warning(out <- snap_offgrid_timestamps(snap_input(NA_character_))))
  expect_equal(nrow(out), 3L)
})

test_that("snap_offgrid_timestamps does not modify its input", {
  dt <- snap_input()
  before <- data.table::copy(dt)
  suppressWarnings(snap_offgrid_timestamps(dt))
  expect_equal(dt, before)
  expect_false(".snapped" %in% names(dt))
})
