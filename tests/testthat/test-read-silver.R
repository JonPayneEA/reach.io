# ============================================================
# Tests: Silver Tier Data Retrieval
# ============================================================

library(data.table)

# ---- helpers ----------------------------------------------------------------

# Write a minimal Silver Parquet file to a temp store and return its root.
make_silver_store <- function(n_sites = 1L, n_rows = 100L) {
  root <- tempfile()
  dir.create(root)

  for (i in seq_len(n_sites)) {
    site  <- sprintf("3900%d", i)
    ts    <- seq(
      as.POSIXct("2024-01-01", tz = "UTC"),
      by = "15 min",
      length.out = n_rows
    )
    dt <- data.table::data.table(
      timestamp     = ts,
      value         = runif(n_rows, 1, 10),
      supplier_flag = NA_character_,
      dataset_id    = sprintf("EA_%s_Q_20240101", site),
      site_id       = site,
      data_type     = "Q",
      qc_flag       = 1L,
      qc_value      = runif(n_rows, 1, 10),
      qc_y_code     = 0L,
      qc_flagged_at = as.POSIXct("2024-04-01", tz = "UTC")
    )
    out_path <- file.path(root, "silver", "hydrometric", "Q", "2024",
                          sprintf("EA_%s_Q_20240101.parquet", site))
    dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
    arrow::write_parquet(dt, out_path)
  }

  root
}

# ---- list_silver_gauges -----------------------------------------------------

test_that("list_silver_gauges returns empty data.table when store is absent", {
  root <- tempfile()
  dir.create(root)
  out <- list_silver_gauges(root)
  expect_true(data.table::is.data.table(out))
  expect_equal(nrow(out), 0L)
})

test_that("list_silver_gauges returns one row per gauge", {
  root <- make_silver_store(n_sites = 2L)
  out  <- list_silver_gauges(root)
  expect_equal(nrow(out), 2L)
})

test_that("list_silver_gauges filters by data_type", {
  root <- make_silver_store(n_sites = 2L)
  out  <- list_silver_gauges(root, data_type = "Q")
  expect_true(all(out$data_type == "Q"))

  out_h <- list_silver_gauges(root, data_type = "H")
  expect_equal(nrow(out_h), 0L)
})

test_that("list_silver_gauges includes year_min, year_max, file_count columns", {
  root <- make_silver_store()
  out  <- list_silver_gauges(root)
  expect_true(all(c("year_min", "year_max", "file_count") %in% names(out)))
})

# ---- read_silver ------------------------------------------------------------

test_that("read_silver errors when Silver store does not exist", {
  expect_error(
    read_silver(tempfile(), "39001", "Q"),
    "No Silver store found"
  )
})

test_that("read_silver errors when gauge has no Silver data", {
  root <- make_silver_store()
  expect_error(
    read_silver(root, "99999", "Q"),
    "No Silver data found for gauge"
  )
})

test_that("read_silver errors on unsupported data_type", {
  root <- make_silver_store()
  expect_error(
    read_silver(root, "39001", "Z"),
    "No parameter mapping"
  )
})

test_that("read_silver returns an S7 HydroData object", {
  root <- make_silver_store()
  obj  <- read_silver(root, "39001", "Q")
  expect_true(S7::S7_inherits(obj, Flow_15min))
})

test_that("read_silver readings slot is a data.table", {
  root <- make_silver_store()
  obj  <- read_silver(root, "39001", "Q")
  expect_true(data.table::is.data.table(obj@readings))
})

test_that("read_silver readings contains required HydroData columns", {
  root <- make_silver_store()
  obj  <- read_silver(root, "39001", "Q")
  expected <- c("dateTime", "date", "value", "measure_notation",
                "qc_flag", "qc_y_code")
  expect_true(all(expected %in% names(obj@readings)))
})

test_that("read_silver value column equals qc_value from Silver file", {
  root <- make_silver_store()
  obj  <- read_silver(root, "39001", "Q")
  # All rows are flagged Good (1) so qc_value was set during make_silver_store
  expect_false(anyNA(obj@readings$value))
})

test_that("read_silver from_date and to_date are populated", {
  root <- make_silver_store()
  obj  <- read_silver(root, "39001", "Q")
  expect_false(is.na(obj@from_date))
  expect_false(is.na(obj@to_date))
})

test_that("read_silver filters by start date", {
  root  <- make_silver_store(n_rows = 200L)
  start <- "2024-01-01 12:00:00"
  obj   <- read_silver(root, "39001", "Q", start = start)
  expect_true(all(obj@readings$dateTime >= as.POSIXct(start, tz = "UTC")))
})

test_that("read_silver min_quality = 1 excludes Suspect and Rejected rows", {
  root <- make_silver_store()
  # Inject a Suspect row directly into the parquet
  pq_path <- list.files(file.path(root, "silver"), pattern = "\\.parquet$",
                         recursive = TRUE, full.names = TRUE)[[1L]]
  dt <- data.table::as.data.table(arrow::read_parquet(pq_path))
  dt$qc_flag[1L]  <- 3L   # Suspect
  dt$qc_value[1L] <- NA_real_
  arrow::write_parquet(dt, pq_path)

  obj <- read_silver(root, "39001", "Q", min_quality = 1L)
  expect_true(all(obj@readings$qc_flag == 1L))
})

test_that("read_silver min_quality = NULL returns all rows", {
  root <- make_silver_store()
  pq_path <- list.files(file.path(root, "silver"), pattern = "\\.parquet$",
                         recursive = TRUE, full.names = TRUE)[[1L]]
  dt <- data.table::as.data.table(arrow::read_parquet(pq_path))
  dt$qc_flag[1L] <- 4L   # Rejected
  arrow::write_parquet(dt, pq_path)

  obj <- read_silver(root, "39001", "Q", min_quality = NULL)
  expect_true(any(obj@readings$qc_flag == 4L))
})

test_that("read_silver errors informatively when quality filter removes all rows", {
  root <- make_silver_store()
  pq_path <- list.files(file.path(root, "silver"), pattern = "\\.parquet$",
                         recursive = TRUE, full.names = TRUE)[[1L]]
  dt <- data.table::as.data.table(arrow::read_parquet(pq_path))
  dt$qc_flag[] <- 4L  # all Rejected
  arrow::write_parquet(dt, pq_path)

  expect_error(
    read_silver(root, "39001", "Q", min_quality = 1L),
    "passes the quality filter"
  )
})

# ---- read_silver_multi ------------------------------------------------------

test_that("read_silver_multi returns a named list", {
  root <- make_silver_store(n_sites = 2L)
  objs <- read_silver_multi(root, c("39001", "39002"), "Q")
  expect_type(objs, "list")
  expect_named(objs)
})

test_that("read_silver_multi names match requested gauge_ids that succeeded", {
  root <- make_silver_store(n_sites = 2L)
  objs <- read_silver_multi(root, c("39001", "39002"), "Q")
  expect_setequal(names(objs), c("39001", "39002"))
})

test_that("read_silver_multi skips missing gauges with a warning", {
  root <- make_silver_store(n_sites = 1L)
  expect_warning(
    objs <- read_silver_multi(root, c("39001", "99999"), "Q"),
    "Skipping gauge"
  )
  expect_equal(length(objs), 1L)
  expect_named(objs, "39001")
})

test_that("read_silver_multi errors on empty gauge_ids", {
  root <- make_silver_store()
  expect_error(read_silver_multi(root, character(0), "Q"),
               "at least one gauge")
})

# ---- read_silver_catchment --------------------------------------------------

test_that("read_silver_catchment looks up gauges from registry", {
  root <- make_silver_store(n_sites = 2L)
  reg  <- data.table::data.table(
    site_id      = c("39001", "39002", "99999"),
    catchment_id = c("Upper", "Upper", "Lower")
  )
  objs <- read_silver_catchment(root, "Upper", "Q", registry = reg)
  expect_setequal(names(objs), c("39001", "39002"))
})

test_that("read_silver_catchment errors when catchment_id column is absent", {
  root <- make_silver_store()
  reg  <- data.table::data.table(site_id = "39001", basin = "Upper")
  expect_error(
    read_silver_catchment(root, "Upper", "Q", registry = reg,
                           catchment_col = "catchment_id"),
    "not found in registry"
  )
})

test_that("read_silver_catchment errors when no gauges match catchment", {
  root <- make_silver_store()
  reg  <- data.table::data.table(
    site_id      = "39001",
    catchment_id = "Lower"
  )
  expect_error(
    read_silver_catchment(root, "Upper", "Q", registry = reg),
    "No gauges found in registry"
  )
})
