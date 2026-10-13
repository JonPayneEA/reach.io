# -- Tests for the Silver file layout (the I/O side of promote_to_silver) -------
#
# These deliberately avoid asserting any QC outcome; they check where Silver
# files are written and that read_silver() can find them.

silver_input <- function() {
  ts <- format(as.POSIXct("2022-12-31 22:00:00", tz = "UTC") + (0:15) * 900,
               "%Y-%m-%d %H:%M:%S")                       # crosses new year
  # bronze_rows() lives in helper-bronze.R, which lintr cannot see
  a <- bronze_rows("S1", ts, value = 5 + (0:15) / 10)  # nolint: object_usage_linter
  b <- bronze_rows("S2", ts, value = 8 + (0:15) / 10)  # nolint: object_usage_linter
  data.table::rbindlist(list(a, b))
}

promote_quietly <- function(x, root) {
  suppressMessages(suppressWarnings(
    promote_to_silver(x, output_dir = root, write_output = TRUE)
  ))
}

test_that("silver_path uses the received year by default and an explicit year when given", {
  expect_match(silver_path("r", "hydrometric", "Q", "EA_S1_Q_20260115"),
               "/Q/2026/EA_S1_Q_20260115.parquet")
  expect_match(silver_path("r", "hydrometric", "Q", "EA_S1_Q_20260115", year = 2020),
               "/Q/2020/EA_S1_Q_20260115.parquet")
})

test_that("promote_to_silver writes one file per dataset and observation year", {
  root <- withr::local_tempdir()
  promote_quietly(silver_input(), root)

  files <- list.files(file.path(root, "silver"), pattern = "\\.parquet$",
                      recursive = TRUE)
  expect_equal(length(files), 4L)                           # 2 sites x 2 years
  expect_setequal(basename(dirname(files)), c("2022", "2023"))
  expect_equal(sum(grepl("EA_S1_Q_", files)), 2L)
  expect_equal(sum(grepl("EA_S2_Q_", files)), 2L)

  # each file holds only its own year's rows for its own site
  f <- list.files(file.path(root, "silver"), pattern = "EA_S1_Q_.*parquet$",
                  recursive = TRUE, full.names = TRUE)
  for (p in f) {
    d <- arrow::read_parquet(p)
    expect_equal(unique(d$site_id), "S1")
    expect_equal(length(unique(format(d$timestamp, "%Y", tz = "UTC"))), 1L)
    expect_equal(unique(format(d$timestamp, "%Y", tz = "UTC")), basename(dirname(p)))
  }
})

test_that("read_silver finds data by observation year", {
  root <- withr::local_tempdir()
  promote_quietly(silver_input(), root)

  all_rows <- read_silver(root, "S1", "Q", min_quality = NULL)
  expect_equal(nrow(all_rows@readings), 16L)

  only_2023 <- read_silver(root, "S1", "Q", start = "2023-01-01", end = "2023-12-31",
                           min_quality = NULL)
  expect_true(all(format(only_2023@readings$dateTime, "%Y") == "2023"))
  # 22:00-23:45 is 2022 (8 rows); 00:00-01:45 is 2023 (8)
  expect_equal(nrow(only_2023@readings), 8L)
})
