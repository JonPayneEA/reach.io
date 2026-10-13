# -- Guards for Windows-specific failures (these run on every platform) ----------
#
# Windows cannot be exercised on the Linux CI/dev boxes, so the two causes of the
# Windows-only failures are guarded here as pure logic and as source checks.

# -- .relative_path(): separators and case ---------------------------------------
#
# .Platform$file.sep is "/" even on Windows, so a prefix built with it never
# matched Windows' backslash paths and nothing under the store was recognised.

test_that(".relative_path strips a base given with forward slashes from backslash paths", {
  p <- "C:\\Users\\jp\\Temp\\store\\silver\\hydrometric\\Q\\2024\\EA_39001_Q_20240101.parquet"
  expect_equal(.relative_path(p, "C:/Users/jp/Temp/store/silver", ignore_case = TRUE),
               "hydrometric/Q/2024/EA_39001_Q_20240101.parquet")
})

test_that(".relative_path handles a backslash base and mixed separators", {
  p <- "C:\\Users\\jp\\Temp/store/bronze/hydrometric/EA/Q/2023/x.parquet"   # mixed, as file.path() gives
  expect_equal(.relative_path(p, "C:\\Users\\jp\\Temp\\store\\bronze", ignore_case = TRUE),
               "hydrometric/EA/Q/2023/x.parquet")
  # trailing separator on the base makes no difference
  expect_equal(.relative_path(p, "C:/Users/jp/Temp/store/bronze/", ignore_case = TRUE),
               "hydrometric/EA/Q/2023/x.parquet")
})

test_that(".relative_path ignores drive-letter / folder case only when asked to", {
  p <- "c:/users/JP/store/bronze/hydrometric/x.parquet"
  expect_equal(.relative_path(p, "C:/Users/jp/store/bronze", ignore_case = TRUE),
               "hydrometric/x.parquet")
  expect_equal(.relative_path(p, "C:/Users/jp/store/bronze", ignore_case = FALSE), p)
})

test_that(".relative_path leaves a path outside the base unchanged", {
  expect_equal(.relative_path("D:/other/x.parquet", "C:/store/bronze", ignore_case = TRUE),
               "D:/other/x.parquet")
})

test_that(".relative_path treats regex metacharacters in the base literally", {
  expect_equal(.relative_path("/tmp/a.b[1]/(x)/bronze/h/f.parquet", "/tmp/a.b[1]/(x)/bronze",
                              ignore_case = FALSE),
               "h/f.parquet")
})

test_that(".category_from_path returns the category for backslash register paths", {
  root <- withr::local_tempdir()
  p <- file.path(root, "bronze", "hydrometric", "EA", "Q", "2023", "x.parquet")
  expect_equal(.category_from_path(p, root), "hydrometric")
  expect_equal(.category_from_path(gsub("/", "\\\\", p), root), "hydrometric")
})


# -- source guards ----------------------------------------------------------------

package_sources <- function() {
  dir <- testthat::test_path("..", "..", "R")
  if (!dir.exists(dir)) dir <- system.file("R", package = "reach.io")
  list.files(dir, pattern = "\\.R$", full.names = TRUE)
}

code_lines <- function(file) {
  x <- readLines(file, warn = FALSE)
  x[!grepl("^\\s*#", x)]                      # ignore comments and roxygen
}

test_that("Parquet is only read through .read_parquet() (no default memory-mapping)", {
  skip_if(length(package_sources()) == 0L, "package sources not available")
  offenders <- unlist(lapply(package_sources(), function(f) {
    bad <- grep("arrow::read_parquet\\(", code_lines(f), value = TRUE)
    # the wrapper itself is the one allowed call, and it sets mmap = FALSE
    bad <- bad[!grepl("mmap *= *FALSE", bad)]
    if (length(bad)) paste0(basename(f), ": ", trimws(bad))
  }))
  expect_length(offenders, 0L)
})

test_that(".read_parquet() disables memory-mapping", {
  expect_true(grepl("mmap *= *FALSE", paste(deparse(.read_parquet), collapse = " ")))
})

test_that(".Platform$file.sep is never used in package code (it is '/' on Windows too)", {
  skip_if(length(package_sources()) == 0L, "package sources not available")
  offenders <- unlist(lapply(package_sources(), function(f) {
    bad <- grep("Platform\\$file\\.sep", code_lines(f), value = TRUE)
    if (length(bad)) paste0(basename(f), ": ", trimws(bad))
  }))
  expect_length(offenders, 0L)
})


# -- read then rewrite the same file ------------------------------------------------

test_that("a Parquet file can be read and then rewritten in the same session", {
  p <- file.path(withr::local_tempdir(), "x.parquet")
  .write_parquet_atomic(data.frame(v = 1), p)
  x <- .read_parquet(p)                        # kept alive on purpose
  .write_parquet_atomic(data.frame(v = 2), p)  # fails on Windows if x memory-maps p
  expect_equal(.read_parquet(p)$v, 2)
  expect_equal(x$v, 1)
})
