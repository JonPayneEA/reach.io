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


# -- two off-grid rows competing for one empty slot ----------------------------
#
# Regression: occupancy was judged against on-grid rows only, so 00:13 and 00:14
# (both rounding to the empty 00:15 slot) were both kept at 00:15.

compete <- function(times, site = "A") {
  data.table::data.table(
    site_id   = site,
    timestamp = as.POSIXct(times, tz = "UTC"),
    value     = seq_along(times)
  )
}

test_that("only the nearest of several off-grid rows is snapped onto an empty slot", {
  dt <- compete(c("2022-01-01 00:00", "2022-01-01 00:13", "2022-01-01 00:14"))
  expect_warning(expect_warning(out <- snap_offgrid_timestamps(dt)))
  expect_equal(format(out$timestamp, "%H:%M"), c("00:00", "00:15"))
  expect_equal(out[.snapped == TRUE, value], 3L)          # the 00:14 reading (nearer)
  expect_false(anyDuplicated(out[, .(site_id, timestamp)]) > 0L)
})

test_that("on an equal distance the earlier off-grid row is kept", {
  # 00:07:30 and 00:22:30 are not competing; use symmetric rows around 00:15
  dt <- compete(c("2022-01-01 00:14", "2022-01-01 00:16"))
  expect_warning(out <- snap_offgrid_timestamps(dt))
  expect_equal(nrow(out), 1L)
  expect_equal(out$value, 1L)
})

test_that("competition is judged per site", {
  dt <- data.table::rbindlist(list(
    compete(c("2022-01-01 00:00", "2022-01-01 00:14"), site = "A"),
    compete(c("2022-01-01 00:00", "2022-01-01 00:14"), site = "B")
  ))
  expect_warning(out <- snap_offgrid_timestamps(dt))
  expect_equal(nrow(out), 4L)                              # one snapped row per site
  expect_equal(out[.snapped == TRUE, site_id], c("A", "B"))
})
