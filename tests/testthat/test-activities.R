test_that("reach.io registers run_backfill and run_incremental on load", {
  activities <- reach.utils::list_activities()
  expect_true("reach.io::backfill" %in% activities)
  expect_true("reach.io::incremental" %in% activities)
})

test_that("the registered backfill activity resolves to run_backfill", {
  fn <- reach.utils:::.get_activity("reach.io", "backfill")
  expect_identical(fn, run_backfill)
})

test_that("the registered incremental activity resolves to run_incremental", {
  fn <- reach.utils:::.get_activity("reach.io", "incremental")
  expect_identical(fn, run_incremental)
})
