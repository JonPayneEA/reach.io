test_that("make_date_chunks splits correctly", {
  chunks <- make_date_chunks("2020-01-01", "2022-01-01")
  expect_equal(nrow(chunks), 2L)
  expect_equal(chunks$chunk_from[1], "2020-01-01")
  expect_equal(chunks$chunk_to[2],   "2022-01-01")
})

test_that("make_date_chunks handles sub-year range", {
  chunks <- make_date_chunks("2021-03-01", "2021-09-01")
  expect_equal(nrow(chunks), 1L)
  expect_equal(chunks$chunk_from[1], "2021-03-01")
  expect_equal(chunks$chunk_to[1],   "2021-09-01")
})

test_that("make_date_chunks returns a data.table", {
  chunks <- make_date_chunks("2020-01-01", "2021-01-01")
  expect_s3_class(chunks, "data.table")
})

test_that("%||% returns lhs when not NULL", {
  expect_equal("hello" %||% "default", "hello")
})

test_that("%||% returns rhs when lhs is NULL", {
  expect_equal(NULL %||% "default", "default")
})

test_that("get_measures errors on bad parameter", {
  expect_error(get_measures("wind"), "should be one of")
})

test_that("download_hydrology errors without out_dir in disk mode", {
  expect_error(
    download_hydrology(output = "disk"),
    "`out_dir` must be supplied"
  )
})

test_that("download_hydrology errors when no stations resolved", {
  # Stub find_stations to return empty table
  local_mocked_bindings(
    find_stations = function(...) data.table::data.table(),
    .package = "eahydrologyr"
  )
  expect_error(
    download_hydrology(wiski_ids = "DOESNOTEXIST"),
    "No stations found"
  )
})
