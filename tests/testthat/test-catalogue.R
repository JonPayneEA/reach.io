# -- Tests for catalogue / rating-store filters --------------------------------
#
# Regression tests for a data.table scoping bug: inside `dt[...]` a bare symbol
# resolves to the column before the function argument, so
# `reg[reg$category %in% category]` compared the column with itself and kept
# every row. These tests use fixtures where the filter must exclude something.

# Register one Bronze series (files + provenance) in a store
register_series <- function(root, id, type, supplier, ts, resolution = "15min") {
  reg <- file.path(root, "register", "hydrometric_data_register.csv")
  dt  <- bronze_rows(id, ts, type = type, supplier = supplier)
  files <- .write_bronze_by_year(dt, root, "hydrometric", supplier, type,
                                 dt$dataset_id[1L])
  .write_provenance_rows(
    .bronze_prov_rows(files, dt$dataset_id[1L], supplier, supplier, id, type,
                      resolution, "test"),
    reg
  )
  invisible(dt)
}

quarter_hours <- function(start, n, skip = integer()) {
  ts <- as.POSIXct(start, tz = "UTC") + (seq_len(n) - 1) * 900
  if (length(skip) > 0L) ts <- ts[-skip]    # NB: ts[-integer(0)] would drop everything
  format(ts, "%Y-%m-%d %H:%M:%S")
}

two_series_store <- function(env = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = env)
  # A daily series is registered FIRST so a filter that ignores site_id/data_type
  # (and so reads row 1 of the register) would pick the wrong interval.
  register_series(root, "S2", "P", "WISKI",
                  format(as.POSIXct("2023-01-01", tz = "UTC") + (0:3) * 86400,
                         "%Y-%m-%d %H:%M:%S"),
                  resolution = "daily")
  register_series(root, "S1", "Q", "EA", quarter_hours("2023-01-01 00:00:00", 10))
  root
}


# -- list_available_gauges() / summarise_coverage() ---------------------------

test_that("list_available_gauges filters by data_type, supplier and category", {
  root <- two_series_store()
  all  <- suppressMessages(capture.output(out_all <- list_available_gauges(root)))
  expect_setequal(out_all$site_id, c("S1", "S2"))

  capture.output(q <- list_available_gauges(root, data_type = "Q"))
  expect_equal(q$site_id, "S1")

  capture.output(w <- list_available_gauges(root, supplier = "WISKI"))
  expect_equal(w$site_id, "S2")

  capture.output(h <- list_available_gauges(root, category = "hydrometric"))
  expect_equal(nrow(h), 2L)
  expect_message(none <- list_available_gauges(root, category = "radarH19"),
                 "No Bronze records match")
  expect_equal(nrow(none), 0L)
})

test_that("summarise_coverage filters by site_id and data_type", {
  root <- two_series_store()
  capture.output(s <- summarise_coverage(root, site_id = "S2"))
  expect_equal(s$site_id, "S2")
  capture.output(d <- summarise_coverage(root, data_type = "Q"))
  expect_equal(d$site_id, "S1")
})


# -- check_gaps() -------------------------------------------------------------

test_that("check_gaps uses the requested series, not the first register row", {
  root <- withr::local_tempdir()
  register_series(root, "S2", "P", "WISKI",
                  format(as.POSIXct("2023-01-01", tz = "UTC") + (0:3) * 86400,
                         "%Y-%m-%d %H:%M:%S"), resolution = "daily")
  # S1: 15-min with two missing steps (indices 4 and 5)
  register_series(root, "S1", "Q", "EA", quarter_hours("2023-01-01 00:00:00", 10, skip = c(4L, 5L)))

  capture.output(gaps <- check_gaps(root, "S1", "Q"))
  expect_equal(nrow(gaps), 1L)
  expect_equal(gaps$n_missing, 2L)
})


# -- list_silver_gauges() -----------------------------------------------------

test_that("list_silver_gauges data_type filter actually filters", {
  root <- withr::local_tempdir()
  for (type in c("Q", "H")) {
    p <- file.path(root, "silver", "hydrometric", type, "2024",
                   sprintf("EA_39001_%s_20240101.parquet", type))
    dir.create(dirname(p), recursive = TRUE)
    arrow::write_parquet(data.frame(x = 1), p)
  }
  capture.output(q <- list_silver_gauges(root, data_type = "Q"))
  expect_equal(q$data_type, "Q")
  expect_message(none <- list_silver_gauges(root, data_type = "SM"), "No Silver files match")
})


# -- catchment lookups --------------------------------------------------------

test_that("read_bronze_catchment selects gauges for the requested catchment only", {
  root <- withr::local_tempdir()
  register_series(root, "UP1", "Q", "EA", quarter_hours("2023-01-01 00:00:00", 4))
  register_series(root, "LOW1", "Q", "EA", quarter_hours("2023-01-01 00:00:00", 4))
  reg <- data.table::data.table(site_id = c("UP1", "LOW1"),
                                catchment_id = c("Upper", "Lower"))

  objs <- read_bronze_catchment(root, "Upper", "Q", registry = reg)
  expect_named(objs, "UP1")
  expect_error(read_bronze_catchment(root, "Nowhere", "Q", registry = reg),
               "No gauges found in registry")
})


# -- rating store -------------------------------------------------------------

rating_limbs <- function() {
  data.table::data.table(
    lower = c(0, 1), upper = c(1, Inf), C = c(10, 12), a = c(0, 0),
    b = c(2, 2), doubtful = c(FALSE, FALSE)
  )
}

test_that("rating versions and lookups are scoped to the requested site and tier", {
  root <- withr::local_tempdir()
  n_saved <- 0L
  save_for <- function(site, tier = "bronze") {
    n_saved <<- n_saved + 1L           # distinct validity start per version
    suppressMessages(save_rating(
      FlodeRatingCurve(limbs = rating_limbs(), station_id = site,
                  valid_from = as.Date("2015-01-01") + 365L * n_saved,
                  valid_to   = as.Date("2015-01-01") + 365L * n_saved + 364L),
      root = root, tier = tier, saved_by = "tester",
      method_of_receipt = "test", derivation_method = "spot gaugings"
    ))
  }
  save_for("A"); save_for("A"); save_for("A")
  save_for("B")                                   # first version for site B
  save_for("B", tier = "silver")

  reg <- suppressMessages(capture.output(r <- list_ratings(root)))
  expect_equal(nrow(r), 5L)

  capture.output(b <- list_ratings(root, site_id = "B"))
  expect_setequal(b$site_id, "B")
  expect_equal(nrow(b), 2L)
  capture.output(bs <- list_ratings(root, site_id = "B", tier = "silver"))
  expect_equal(nrow(bs), 1L)

  # Version numbering is per site + tier (the bug counted every site together)
  reg_path <- file.path(root, "register", "rating_register.csv")
  expect_equal(.next_rating_version("B", reg_path, "bronze"), 2L)
  expect_equal(.next_rating_version("B", reg_path, "silver"), 2L)
  expect_equal(.next_rating_version("A", reg_path, "bronze"), 4L)
  expect_equal(.next_rating_version("NEW", reg_path, "bronze"), 1L)

  # load_rating returns only the requested site
  rc <- load_rating("B", root, tier = "bronze", version = 1)
  expect_equal(rc@station_id, "B")
  rs <- load_rating("A", root, tier = "bronze")
  expect_equal(rs@station_id, "A")
  expect_length(rs@curves, 3L)
})
