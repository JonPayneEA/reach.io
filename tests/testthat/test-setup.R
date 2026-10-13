# -- Tests for setup_hydro_store() ---------------------------------------------

# Directories under `root`, relative to it, with "/" separators on every platform.
# (An earlier version built a regex from `root`, which breaks on Windows paths:
# backslashes and metacharacters in the temp directory name.)
store_dirs <- function(root) {
  sort(.relative_path(list.dirs(root, recursive = TRUE)[-1L], root))
}

test_that("setup_hydro_store builds the tier / category / supplier / data_type tree", {
  root <- withr::local_tempdir()
  suppressMessages(setup_hydro_store(root, categories = list(
    hydrometric = list(suppliers = c("EA", "NRFA"), data_types = c("Q", "H"))
  ), gold_purposes = NULL, verbose = FALSE))
  d <- store_dirs(root)
  expect_true(all(c("bronze/hydrometric/EA/Q", "bronze/hydrometric/EA/H",
                    "bronze/hydrometric/NRFA/Q",           # NRFA is flow only
                    "silver/hydrometric/Q", "silver/hydrometric/H",
                    "gold/hydrometric/Q", "register") %in% d))
  expect_false("bronze/hydrometric/NRFA/H" %in% d)
})

test_that("rating_curves gets ratings folders only, not data_type folders", {
  root <- withr::local_tempdir()
  suppressMessages(setup_hydro_store(root, categories = list(
    hydrometric = list(suppliers = c("EA", "rating_curves"), data_types = c("Q", "H", "P"))
  ), gold_purposes = NULL, verbose = FALSE))
  d <- store_dirs(root)
  expect_true(all(sprintf("%s/hydrometric/rating_curves/ratings", c("bronze", "silver", "gold")) %in% d))
  # regression: these empty folders used to be created too
  expect_false(any(grepl("rating_curves/(Q|H|P)$", d)))
})

test_that("a minimal store has no ratings folders", {
  root <- withr::local_tempdir()
  suppressMessages(setup_hydro_store(root, categories = list(
    hydrometric = list(suppliers = "EA", data_types = "P")
  ), gold_purposes = NULL, verbose = FALSE))
  expect_false(any(grepl("rating", store_dirs(root))))
})

test_that("setup_hydro_store is safe to re-run and reports what it created", {
  root <- withr::local_tempdir()
  cats <- list(hydrometric = list(suppliers = "EA", data_types = "Q"))
  m1 <- suppressMessages(setup_hydro_store(root, cats, gold_purposes = NULL, verbose = FALSE))
  m2 <- suppressMessages(setup_hydro_store(root, cats, gold_purposes = NULL, verbose = FALSE))
  expect_true(all(m1$created))
  expect_false(any(m2$created))
})
