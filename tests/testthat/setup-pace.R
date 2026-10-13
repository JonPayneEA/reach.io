# Replace the EA fair-use pause with a no-op for the whole test run. The pause
# exists only to be polite to the real API; the tests never touch it.
testthat::local_mocked_bindings(.pace = function(...) invisible(NULL),
                                .package = "reach.io", .env = testthat::teardown_env())
