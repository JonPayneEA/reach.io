# ============================================================
# Tool:        Project Directory Setup
# Description: Creates the hydrometric data store directory
#              structure. Tier / Category / Supplier / DataType
#              following the Hydrometric Data Framework v1.3.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: added category layer
# Tier:        2
# Inputs:      Root directory path; optional category, supplier,
#              and data type selections
# Outputs:     Directory tree; directory manifest data.table
# Dependencies: data.table
# ============================================================

#' Set up the hydrometric data store directory structure
#'
#' Creates the full directory tree for Bronze, Silver, Gold, and Register
#' storage. The structure follows the convention:
#'
#' `<tier>/<category>/<supplier>/<data_type>/`
#'
#' where category is one of `"hydrometric"`, `"radarH19"`, or `"MOSES"`.
#'
#' ```
#' <root>/
#' ├── bronze/
#' │   ├── hydrometric/
#' │   │   ├── EA/Q/  EA/H/  EA/P/
#' │   │   ├── WISKI/Q/  WISKI/H/
#' │   │   └── NRFA/Q/
#' │   ├── radarH19/
#' │   │   └── MO/P/
#' │   └── MOSES/
#' │       └── MO/SM/
#' ├── silver/
#' │   ├── hydrometric/Q/  hydrometric/H/  hydrometric/P/
#' │   ├── radarH19/P/
#' │   └── MOSES/SM/
#' ├── bronze/
#' │   └── hydrometric/
#' │       └── ratings/
#' ├── silver/
#' │   └── hydrometric/
#' │       └── ratings/
#' ├── gold/
#' │   ├── hydrometric/
#' │   │   ├── ratings/
#' │   │   ├── Q/FFA/
#' │   │   └── P/catchment_average/
#' │   ├── radarH19/P/
#' │   └── MOSES/SM/
#' └── register/
#' ```
#'
#' Safe to re-run on an existing store — existing directories are untouched.
#'
#' @param root Character. Root directory for the data store,
#'   e.g. `"data/hydrometric"`.
#' @param categories Named list. Each name is a category (e.g.
#'   `"hydrometric"`, `"radarH19"`, `"MOSES"`). Each element is a named list
#'   with `suppliers` (character vector of supplier codes) and `data_types`
#'   (character vector of framework data type codes). Defaults to the
#'   standard set for the three default categories.
#' @param gold_purposes Named list. Top-level names are categories; each
#'   element is a named list mapping data type codes to a character vector
#'   of Gold purpose subdirectory names. Set to `NULL` to skip Gold
#'   purpose subdirectories.
#' @param verbose Logical. Print each directory as it is created. Default
#'   `TRUE`.
#'
#' @return A `data.table` manifest with columns `path` and `created`
#'   (logical). Returned invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Standard setup — all three default categories
#' setup_hydro_store("data/hydrometric")
#'
#' # Custom: hydrometric only, EA and WISKI sources, flow and level only
#' setup_hydro_store(
#'   "data/hydrometric",
#'   categories = list(
#'     hydrometric = list(
#'       suppliers  = c("EA", "WISKI"),
#'       data_types = c("Q", "H")
#'     )
#'   )
#' )
#' }
setup_hydro_store <- function(
    root,
    categories = list(
      hydrometric = list(
        suppliers  = c("EA", "WISKI", "NRFA", "rating_curves"),
        data_types = c("Q", "H", "P")
      ),
      radarH19 = list(
        suppliers  = "MO",
        data_types = "P"
      ),
      MOSES = list(
        suppliers  = "MO",
        data_types = c("SM", "PE")
      )
    ),
    gold_purposes = list(
      hydrometric = list(
        Q = c("FFA"),
        P = c("catchment_average")
      )
    ),
    verbose = TRUE) {

  dirs <- character(0)

  for (cat in names(categories)) {
    cfg        <- categories[[cat]]
    suppliers  <- cfg$suppliers
    data_types <- cfg$data_types

    # Bronze: <tier>/<category>/<supplier>/<data_type>/
    for (supplier in suppliers) {
      # NRFA carries flow only; MO carries rainfall and soil moisture only
      supplier_types <- switch(supplier,
        NRFA = intersect(data_types, "Q"),
        data_types
      )
      for (dt in supplier_types) {
        dirs <- c(dirs, file.path(root, "bronze", cat, supplier, dt))
      }
    }

    # Silver: <tier>/<category>/<data_type>/
    for (dt in data_types) {
      dirs <- c(dirs, file.path(root, "silver", cat, dt))
    }

    # Gold: <tier>/<category>/<data_type>/[<purpose>/]
    for (dt in data_types) {
      dirs <- c(dirs, file.path(root, "gold", cat, dt))
    }

    # Gold purposes for this category
    if (!is.null(gold_purposes) && cat %in% names(gold_purposes)) {
      for (dt in names(gold_purposes[[cat]])) {
        if (!dt %in% data_types) next
        for (purpose in gold_purposes[[cat]][[dt]]) {
          dirs <- c(dirs, file.path(root, "gold", cat, dt, purpose))
        }
      }
    }
  }

  # Ratings — only created when "rating_curves" is explicitly listed as a
  # hydrometric supplier, so minimal setups don't get unwanted folders.
  if ("hydrometric" %in% names(categories) &&
      "rating_curves" %in% categories$hydrometric$suppliers) {
    for (tier in c("bronze", "silver", "gold")) {
      dirs <- c(dirs, file.path(root, tier, "hydrometric", "rating_curves", "ratings"))
    }
  }

  # Register — shared across all categories
  dirs <- c(dirs, file.path(root, "register"))

  # Create directories and build manifest
  manifest <- data.table::rbindlist(lapply(dirs, function(d) {
    already_exists <- dir.exists(d)
    dir.create(d, recursive = TRUE, showWarnings = FALSE)
    if (verbose && !already_exists) message("  Created: ", d)
    data.table::data.table(path = d, created = !already_exists)
  }))

  n_new      <- sum(manifest$created)
  n_existing <- nrow(manifest) - n_new

  message(sprintf(
    "\nStore setup complete under '%s':\n  %d directories created, %d already existed.",
    root, n_new, n_existing
  ))

  invisible(manifest)
}
