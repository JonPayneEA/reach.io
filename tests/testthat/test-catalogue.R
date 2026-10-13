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


# -- save_rating(): superseding is per site AND tier -----------------------------
#
# Regression: `tier == tier` inside `reg[...]` compared the column with itself,
# so saving a silver rating marked the same site's bronze rating Superseded.

save_one <- function(root, site, tier, from, to = NA, ...) {
  suppressMessages(save_rating(
    FlodeRatingCurve(limbs = rating_limbs(), station_id = site,
                     valid_from = as.Date(from),
                     valid_to   = if (is.na(to)) as.Date(NA) else as.Date(to)),
    root = root, tier = tier, saved_by = "tester",
    method_of_receipt = "test", derivation_method = "spot gaugings", ...
  ))
}

read_register <- function(root) {
  data.table::fread(file.path(root, "register", "rating_register.csv"))
}

test_that("saving a rating in one tier does not supersede the same site in another tier", {
  root <- withr::local_tempdir()
  save_one(root, "A", "bronze", "2020-01-01")
  save_one(root, "A", "silver", "2020-01-01")
  reg <- read_register(root)
  expect_equal(reg[tier == "bronze", status], "Active")
  expect_equal(reg[tier == "silver", status], "Active")
})

test_that("a new version supersedes the old one within the same site and tier only", {
  root <- withr::local_tempdir()
  save_one(root, "A", "bronze", "2020-01-01", "2020-12-31")
  save_one(root, "B", "bronze", "2020-01-01", "2020-12-31")
  save_one(root, "A", "bronze", "2021-01-01", "2021-12-31")
  reg <- read_register(root)
  expect_equal(reg[site_id == "A" & version == 1, status], "Superseded")
  expect_equal(reg[site_id == "A" & version == 2, status], "Active")
  expect_equal(reg[site_id == "B", status], "Active")      # other site untouched
})

test_that("saving a RatingSet leaves every member Active and supersedes the old set", {
  root <- withr::local_tempdir()
  save_one(root, "A", "gold", "2019-01-01", "2019-12-31")          # an earlier version
  set <- FlodeRatingSet(
    curves = lapply(c("2020", "2021", "2022"), function(y) {
      FlodeRatingCurve(limbs = rating_limbs(), station_id = "A",
                       valid_from = as.Date(paste0(y, "-01-01")),
                       valid_to   = as.Date(paste0(y, "-12-31")))
    }),
    station_id = "A"
  )
  suppressMessages(save_rating(set, root = root, tier = "gold", saved_by = "tester",
                               method_of_receipt = "test",
                               derivation_method = "spot gaugings"))
  reg <- read_register(root)
  expect_equal(nrow(reg), 4L)
  expect_equal(reg[version == 1, status], "Superseded")          # the earlier one
  expect_equal(reg[version %in% 2:4, status], rep("Active", 3))  # whole new set current
})


# -- check_gaps(): separate gaps stay separate ------------------------------------
#
# Regression: diff() of POSIXct is a difftime with auto-chosen units, and dividing
# it by seconds gave ~0, so every missing step was merged into one gap period.

three_gap_times <- function() {
  ts <- as.POSIXct("2023-01-01", tz = "UTC") + (0:99) * 900
  ts[-c(11:13, 40, 70:71)]            # gaps of 3, 1 and 2 steps
}

expect_three_gaps <- function(g, ts_all) {
  expect_equal(nrow(g), 3L)
  expect_equal(g$n_missing, c(3L, 1L, 2L))
  step <- function(i) as.POSIXct("2023-01-01", tz = "UTC") + (i - 1) * 900
  expect_equal(g$gap_start, step(c(11, 40, 70)))
  expect_equal(g$gap_end,   step(c(13, 40, 71)))
}

test_that("check_gaps reports separate gap periods separately", {
  root <- withr::local_tempdir()
  x <- bronze_rows("S1", format(three_gap_times(), "%Y-%m-%d %H:%M:%S"))
  .write_bronze_by_year(x, root, "hydrometric", "EA", "Q", x$dataset_id[1L])
  capture.output(g <- check_gaps(root, "S1", "Q", expected_interval = "15min"))
  expect_three_gaps(g)
})

test_that("check_gaps handles hourly and daily steps too", {
  root <- withr::local_tempdir()
  hrs <- as.POSIXct("2023-01-01", tz = "UTC") + (0:47) * 3600
  x <- bronze_rows("H1", format(hrs[-c(5, 6, 30)], "%Y-%m-%d %H:%M:%S"), type = "H")
  .write_bronze_by_year(x, root, "hydrometric", "EA", "H", x$dataset_id[1L])
  capture.output(g <- check_gaps(root, "H1", "H", expected_interval = "hourly"))
  expect_equal(g$n_missing, c(2L, 1L))
})

test_that("check_silver_gaps reports separate gap periods separately", {
  root <- withr::local_tempdir()
  p <- file.path(root, "silver", "hydrometric", "Q", "2023", "EA_S1_Q_20230101.parquet")
  dir.create(dirname(p), recursive = TRUE)
  ts <- three_gap_times()
  storage.mode(ts) <- "double"
  arrow::write_parquet(
    data.frame(timestamp = ts, site_id = "S1", qc_flag = 1L, value = 1), p
  )
  capture.output(g <- check_silver_gaps(root, "S1", "Q", expected_interval = "15min"))
  expect_three_gaps(g)
})
