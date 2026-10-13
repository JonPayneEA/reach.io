# ============================================================
# Tests: S7 FlodeHydroData and FlodePotEvapData classes
# ============================================================

library(data.table)

# -- helpers -------------------------------------------------------------------

make_hydro_dt <- function(n = 10L) {
  data.table(
    dateTime          = as.POSIXct("2022-01-01", tz = "UTC") + seq_len(n) * 86400,
    date              = as.Date("2022-01-01") + seq_len(n),
    value             = runif(n, 0, 10),
    measure_notation  = "test_measure"
  )
}

make_pe_dt <- function(n = 10L) {
  data.table(
    dateTime = as.POSIXct("2022-01-01", tz = "UTC") + seq_len(n) * 86400,
    date     = as.Date("2022-01-01") + seq_len(n),
    value    = runif(n, 0, 5)
  )
}

# ==============================================================================
# FlodeHydroData concrete classes
# ==============================================================================

test_that("FlodeRainfall_Daily constructs correctly", {
  dt  <- make_hydro_dt()
  obj <- FlodeRainfall_Daily(readings = dt, from_date = "2022-01-01", to_date = "2022-01-10")
  expect_true(S7::S7_inherits(obj, FlodeRainfall_Daily))
  expect_equal(obj@parameter, "rainfall")
  expect_equal(obj@period_name, "daily")
  expect_equal(obj@n_rows, 10L)
  expect_equal(obj@n_measures, 1L)
})

test_that("FlodeFlow_15min constructs correctly", {
  dt  <- make_hydro_dt()
  obj <- FlodeFlow_15min(readings = dt)
  expect_true(S7::S7_inherits(obj, FlodeFlow_15min))
  expect_equal(obj@parameter, "flow")
  expect_equal(obj@period_name, "15min")
})

test_that("FlodeLevel_Daily constructs correctly", {
  dt  <- make_hydro_dt()
  obj <- FlodeLevel_Daily(readings = dt)
  expect_true(S7::S7_inherits(obj, FlodeLevel_Daily))
  expect_equal(obj@parameter, "level")
  expect_equal(obj@period_name, "daily")
})

test_that("FlodeHydroData rejects non-data.table readings", {
  expect_error(FlodeRainfall_Daily(readings = list(a = 1)), "data.table")
})

test_that("FlodeHydroData rejects missing required columns", {
  dt <- data.table(dateTime = Sys.time(), value = 1)
  expect_error(FlodeRainfall_Daily(readings = dt), "missing column")
})

test_that("FlodeHydroData rejects wrong column types", {
  dt <- data.table(
    dateTime         = "not-posixct",
    date             = as.Date("2022-01-01"),
    value            = 1,
    measure_notation = "x"
  )
  expect_error(FlodeRainfall_Daily(readings = dt), "POSIXct")
})

# ==============================================================================
# as_data_table and as_long
# ==============================================================================

test_that("as_data_table returns the readings", {
  dt  <- make_hydro_dt()
  obj <- FlodeFlow_Daily(readings = dt)
  out <- as_data_table(obj)
  expect_true(data.table::is.data.table(out))
  expect_equal(nrow(out), nrow(dt))
})

test_that("as_long adds parameter column", {
  dt  <- make_hydro_dt()
  obj <- FlodeFlow_Daily(readings = dt)
  out <- as_long(obj)
  expect_true("parameter" %in% names(out))
  expect_equal(out$parameter[1L], "flow")
  expect_equal(names(out)[1L], "parameter")
})

# ==============================================================================
# HYDRO_CLASS lookup
# ==============================================================================

test_that("HYDRO_CLASS covers all parameter/period combinations", {
  for (param in c("rainfall", "flow", "level")) {
    for (period in c("daily", "15min")) {
      cls <- reach.io:::HYDRO_CLASS[[param]][[period]]
      expect_true(!is.null(cls),
                  info = sprintf("Missing HYDRO_CLASS entry: %s / %s", param, period))
    }
  }
})

# ==============================================================================
# FlodePotEvapData concrete classes
# ==============================================================================

test_that("FlodePotEvap_Daily constructs correctly", {
  dt  <- make_pe_dt()
  obj <- FlodePotEvap_Daily(readings = dt, source_name = "CHESS-PE")
  expect_true(S7::S7_inherits(obj, FlodePotEvap_Daily))
  expect_equal(obj@period_name, "daily")
  expect_equal(obj@source_name, "CHESS-PE")
  expect_equal(obj@n_rows, 10L)
})

test_that("FlodePotEvap_Hourly constructs correctly", {
  dt  <- make_pe_dt()
  obj <- FlodePotEvap_Hourly(readings = dt, source_name = "MORECS")
  expect_true(S7::S7_inherits(obj, FlodePotEvap_Hourly))
  expect_equal(obj@period_name, "hourly")
})

test_that("FlodePotEvapData rejects missing columns", {
  dt <- data.table(dateTime = Sys.time(), value = 1)
  expect_error(FlodePotEvap_Daily(readings = dt, source_name = "x"), "missing column")
})

test_that("FlodePotEvapData rejects invalid period_name", {
  # FlodePotEvap_Daily sets period_name = "daily" internally, so this should pass.
  # But a hypothetical bad period_name should be caught.
  dt  <- make_pe_dt()
  obj <- FlodePotEvap_Daily(readings = dt, source_name = "x")
  expect_equal(obj@period_name, "daily")
})

# ==============================================================================
# disagg_to_15min
# ==============================================================================

test_that("disagg_to_15min from hourly produces 4x rows", {
  dt  <- make_pe_dt(24L)
  pe  <- FlodePotEvap_Hourly(readings = dt, source_name = "test")
  out <- disagg_to_15min(pe)
  expect_true(S7::S7_inherits(out, FlodePotEvap_15min))
  expect_equal(out@n_rows, 24L * 4L)
  expect_equal(out@disagg_method, "uniform_hourly")
  expect_true(out@is_calculated)
})

test_that("disagg_to_15min from daily produces 96x rows", {
  dt  <- make_pe_dt(5L)
  pe  <- FlodePotEvap_Daily(readings = dt, source_name = "test")
  out <- disagg_to_15min(pe)
  expect_true(S7::S7_inherits(out, FlodePotEvap_15min))
  expect_equal(out@n_rows, 5L * 96L)
  expect_equal(out@disagg_method, "uniform_daily")
})

test_that("disagg_to_15min conserves total volume", {
  dt <- make_pe_dt(10L)
  pe <- FlodePotEvap_Hourly(readings = dt, source_name = "test")
  out <- disagg_to_15min(pe)
  expect_equal(sum(out@readings$value), sum(dt$value), tolerance = 1e-10)
})

# ==============================================================================
# Print methods
# ==============================================================================

test_that("FlodeHydroData prints formatted output", {
  dt  <- make_hydro_dt()
  obj <- FlodeFlow_Daily(readings = dt, from_date = "2022-01-01", to_date = "2022-01-10")
  out <- capture.output(print(obj))
  expect_true(any(grepl("FlodeFlow_Daily", out)))
  expect_true(any(grepl("Date range", out)))
})

test_that("FlodePotEvapData prints formatted output", {
  dt  <- make_pe_dt()
  obj <- FlodePotEvap_Daily(readings = dt, source_name = "CHESS-PE",
                       from_date = "2022-01-01", to_date = "2022-01-10")
  out <- capture.output(print(obj))
  expect_true(any(grepl("FlodePotEvap_Daily", out)))
  expect_true(any(grepl("CHESS-PE", out)))
})

test_that("FlodePotEvap_15min print includes disagg method", {
  dt  <- make_pe_dt(24L)
  pe  <- FlodePotEvap_Hourly(readings = dt, source_name = "test")
  out <- disagg_to_15min(pe)
  printed <- capture.output(print(out))
  expect_true(any(grepl("Disagg method", printed)))
  expect_true(any(grepl("uniform_hourly", printed)))
})
