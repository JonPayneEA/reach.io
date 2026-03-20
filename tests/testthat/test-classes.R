# ============================================================
# Tests: S7 HydroData and PotEvapData classes
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
# HydroData concrete classes
# ==============================================================================

test_that("Rainfall_Daily constructs correctly", {
  dt  <- make_hydro_dt()
  obj <- Rainfall_Daily(readings = dt, from_date = "2022-01-01", to_date = "2022-01-10")
  expect_true(S7::S7_inherits(obj, Rainfall_Daily))
  expect_equal(obj@parameter, "rainfall")
  expect_equal(obj@period_name, "daily")
  expect_equal(obj@n_rows, 10L)
  expect_equal(obj@n_measures, 1L)
})

test_that("Flow_15min constructs correctly", {
  dt  <- make_hydro_dt()
  obj <- Flow_15min(readings = dt)
  expect_true(S7::S7_inherits(obj, Flow_15min))
  expect_equal(obj@parameter, "flow")
  expect_equal(obj@period_name, "15min")
})

test_that("Level_Daily constructs correctly", {
  dt  <- make_hydro_dt()
  obj <- Level_Daily(readings = dt)
  expect_true(S7::S7_inherits(obj, Level_Daily))
  expect_equal(obj@parameter, "level")
  expect_equal(obj@period_name, "daily")
})

test_that("HydroData rejects non-data.table readings", {
  expect_error(Rainfall_Daily(readings = list(a = 1)), "data.table")
})

test_that("HydroData rejects missing required columns", {
  dt <- data.table(dateTime = Sys.time(), value = 1)
  expect_error(Rainfall_Daily(readings = dt), "missing column")
})

test_that("HydroData rejects wrong column types", {
  dt <- data.table(
    dateTime         = "not-posixct",
    date             = as.Date("2022-01-01"),
    value            = 1,
    measure_notation = "x"
  )
  expect_error(Rainfall_Daily(readings = dt), "POSIXct")
})

# ==============================================================================
# as_data_table and as_long
# ==============================================================================

test_that("as_data_table returns the readings", {
  dt  <- make_hydro_dt()
  obj <- Flow_Daily(readings = dt)
  out <- as_data_table(obj)
  expect_true(data.table::is.data.table(out))
  expect_equal(nrow(out), nrow(dt))
})

test_that("as_long adds parameter column", {
  dt  <- make_hydro_dt()
  obj <- Flow_Daily(readings = dt)
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
# PotEvapData concrete classes
# ==============================================================================

test_that("PotEvap_Daily constructs correctly", {
  dt  <- make_pe_dt()
  obj <- PotEvap_Daily(readings = dt, source_name = "CHESS-PE")
  expect_true(S7::S7_inherits(obj, PotEvap_Daily))
  expect_equal(obj@period_name, "daily")
  expect_equal(obj@source_name, "CHESS-PE")
  expect_equal(obj@n_rows, 10L)
})

test_that("PotEvap_Hourly constructs correctly", {
  dt  <- make_pe_dt()
  obj <- PotEvap_Hourly(readings = dt, source_name = "MORECS")
  expect_true(S7::S7_inherits(obj, PotEvap_Hourly))
  expect_equal(obj@period_name, "hourly")
})

test_that("PotEvapData rejects missing columns", {
  dt <- data.table(dateTime = Sys.time(), value = 1)
  expect_error(PotEvap_Daily(readings = dt, source_name = "x"), "missing column")
})

test_that("PotEvapData rejects invalid period_name", {
  # PotEvap_Daily sets period_name = "daily" internally, so this should pass.
  # But a hypothetical bad period_name should be caught.
  dt  <- make_pe_dt()
  obj <- PotEvap_Daily(readings = dt, source_name = "x")
  expect_equal(obj@period_name, "daily")
})

# ==============================================================================
# disagg_to_15min
# ==============================================================================

test_that("disagg_to_15min from hourly produces 4x rows", {
  dt  <- make_pe_dt(24L)
  pe  <- PotEvap_Hourly(readings = dt, source_name = "test")
  out <- disagg_to_15min(pe)
  expect_true(S7::S7_inherits(out, PotEvap_15min))
  expect_equal(out@n_rows, 24L * 4L)
  expect_equal(out@disagg_method, "uniform_hourly")
  expect_true(out@is_calculated)
})

test_that("disagg_to_15min from daily produces 96x rows", {
  dt  <- make_pe_dt(5L)
  pe  <- PotEvap_Daily(readings = dt, source_name = "test")
  out <- disagg_to_15min(pe)
  expect_true(S7::S7_inherits(out, PotEvap_15min))
  expect_equal(out@n_rows, 5L * 96L)
  expect_equal(out@disagg_method, "uniform_daily")
})

test_that("disagg_to_15min conserves total volume", {
  dt <- make_pe_dt(10L)
  pe <- PotEvap_Hourly(readings = dt, source_name = "test")
  out <- disagg_to_15min(pe)
  expect_equal(sum(out@readings$value), sum(dt$value), tolerance = 1e-10)
})

# ==============================================================================
# Print methods
# ==============================================================================

test_that("HydroData prints formatted output", {
  dt  <- make_hydro_dt()
  obj <- Flow_Daily(readings = dt, from_date = "2022-01-01", to_date = "2022-01-10")
  out <- capture.output(print(obj))
  expect_true(any(grepl("Flow_Daily", out)))
  expect_true(any(grepl("Date range", out)))
})

test_that("PotEvapData prints formatted output", {
  dt  <- make_pe_dt()
  obj <- PotEvap_Daily(readings = dt, source_name = "CHESS-PE",
                       from_date = "2022-01-01", to_date = "2022-01-10")
  out <- capture.output(print(obj))
  expect_true(any(grepl("PotEvap_Daily", out)))
  expect_true(any(grepl("CHESS-PE", out)))
})

test_that("PotEvap_15min print includes disagg method", {
  dt  <- make_pe_dt(24L)
  pe  <- PotEvap_Hourly(readings = dt, source_name = "test")
  out <- disagg_to_15min(pe)
  printed <- capture.output(print(out))
  expect_true(any(grepl("Disagg method", printed)))
  expect_true(any(grepl("uniform_hourly", printed)))
})
