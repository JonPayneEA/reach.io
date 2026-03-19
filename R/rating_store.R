# ============================================================
# Tool:        Rating curve storage
# Description: Read/write helpers for persisting RatingCurve
#              and RatingSet objects under the governance
#              directory structure.
#
#              Storage location (Gold tier, calibration):
#                <root>/gold/hydrometric/Q/calibration/
#                  <site_id>_ratings.parquet
#
#              Governance manifest:
#                <root>/register/rating_register.csv
#                  (one row per saved version; mirrors the
#                   hydrometric_data_register pattern)
#
#              Parquet schema (one row per limb):
#                site_id    chr   Station identifier
#                version    int   Auto-incremented per site
#                valid_from chr   ISO-8601 date or NA
#                valid_to   chr   ISO-8601 date or NA (= current)
#                limb_no    int   Limb sequence within version
#                lower      dbl   Lower stage boundary (m)
#                upper      dbl   Upper stage boundary (m / Inf)
#                C          dbl   Multiplier
#                a          dbl   Datum offset
#                b          dbl   Exponent
#                doubtful   lgl   Extrapolation flag
#                source     chr   Origin label
#
# Flode Module: flode.io
# Author:      JP
# Created:     2026-03-19
# Tier:        3 (Gold / Register)
# Dependencies: S7, data.table, arrow
# ============================================================

#' @import S7
NULL


# =============================================================================
# Internal helpers
# =============================================================================

#' @noRd
.rating_paths <- function(root, site_id) {
  list(
    calib_dir = file.path(root, "gold", "hydrometric", "Q", "calibration"),
    pq_file   = file.path(root, "gold", "hydrometric", "Q", "calibration",
                          paste0(site_id, "_ratings.parquet")),
    register  = file.path(root, "register", "rating_register.csv")
  )
}

# Return the next version integer for a site_id given the register CSV.
#' @noRd
.next_rating_version <- function(site_id, reg_path) {
  if (!file.exists(reg_path)) return(1L)
  reg <- data.table::fread(reg_path, colClasses = "character", showProgress = FALSE)
  if (nrow(reg) == 0L || !"site_id" %in% names(reg)) return(1L)
  rows <- reg[reg$site_id == site_id]
  if (nrow(rows) == 0L) return(1L)
  max(as.integer(rows$version), na.rm = TRUE) + 1L
}

# Flatten one RatingCurve to a data.table row-per-limb ready for Parquet.
#' @noRd
.curve_to_dt <- function(x, version) {
  dt <- data.table::copy(x@limbs)
  dt[, `:=`(
    site_id    = x@station_id,
    version    = as.integer(version),
    valid_from = if (inherits(x@valid_from, "Date")) format(x@valid_from) else NA_character_,
    valid_to   = if (inherits(x@valid_to,   "Date")) format(x@valid_to)   else NA_character_,
    limb_no    = seq_len(.N),
    source     = x@source
  )]
  data.table::setcolorder(dt, c(
    "site_id", "version", "valid_from", "valid_to",
    "limb_no", "lower", "upper", "C", "a", "b", "doubtful", "source"
  ))
  dt
}

# Reconstruct one RatingCurve from a data.table slice (one version only).
#' @noRd
.dt_to_curve <- function(dt) {
  data.table::setorder(dt, limb_no)
  limbs <- dt[, .(lower, upper, C, a, b, doubtful)]

  vf <- dt$valid_from[1L]
  vt <- dt$valid_to[1L]

  RatingCurve(
    limbs      = limbs,
    valid_from = if (!is.na(vf) && nzchar(vf)) as.Date(vf) else NA,
    valid_to   = if (!is.na(vt) && nzchar(vt)) as.Date(vt) else NA,
    station_id = dt$site_id[1L],
    source     = if ("source" %in% names(dt)) dt$source[1L] else NA_character_
  )
}


# =============================================================================
# save_rating()
# =============================================================================

#' Save a rating curve to the Gold calibration store
#'
#' Writes a [RatingCurve] (or every curve in a [RatingSet]) to the
#' site's Parquet file under `<root>/gold/hydrometric/Q/calibration/`
#' and appends a governance row to `<root>/register/rating_register.csv`.
#'
#' Each call to `save_rating()` with a [RatingCurve] is assigned the next
#' available version number for that `station_id`. Existing versions are
#' never overwritten — the Parquet accumulates all versions for the site,
#' differentiated by `version` and `valid_from` / `valid_to`.
#'
#' The calibration directory and register are created automatically if they
#' do not yet exist (no need to call [setup_hydro_store()] first).
#'
#' @param x A [RatingCurve] or [RatingSet]. The curve's `station_id` slot
#'   must be set to a non-empty string.
#' @param root Character. Root of the data store
#'   (the directory that contains `bronze/`, `silver/`, `gold/`,
#'   `register/`).
#' @param saved_by Character. Name or username of the person saving the
#'   curve. Recorded in the register. Defaults to `Sys.info()[["user"]]`.
#' @param notes Character. Optional free-text note appended to the register
#'   row (e.g. `"Post-survey 2023 refit — upper limb revised"`).
#'
#' @return The path(s) to the written Parquet file(s), invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' save_rating(rc_fixed, root = "data/hydro",
#'             saved_by = "J.Payne",
#'             notes = "Upper limb continuity corrected with fix_limb_continuity()")
#'
#' # Save an entire time-varying set in one call
#' save_rating(rating_set, root = "data/hydro", saved_by = "J.Payne")
#' }
save_rating <- S7::new_generic("save_rating", "x")

S7::method(save_rating, RatingCurve) <- function(x, root,
                                                  saved_by = Sys.info()[["user"]],
                                                  notes    = NA_character_) {
  if (is.na(x@station_id) || !nzchar(x@station_id)) {
    stop("`station_id` must be set on the RatingCurve before saving.")
  }

  paths <- .rating_paths(root, x@station_id)
  dir.create(paths$calib_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(dirname(paths$register), recursive = TRUE, showWarnings = FALSE)

  version    <- .next_rating_version(x@station_id, paths$register)
  limbs_flat <- .curve_to_dt(x, version)

  # Append to or create Parquet
  if (file.exists(paths$pq_file)) {
    existing <- data.table::as.data.table(
      arrow::read_parquet(paths$pq_file)
    )
    combined <- data.table::rbindlist(list(existing, limbs_flat), fill = TRUE)
  } else {
    combined <- limbs_flat
  }
  arrow::write_parquet(combined, paths$pq_file)

  # Append governance row to register
  reg_row <- data.table::data.table(
    site_id    = x@station_id,
    version    = as.integer(version),
    valid_from = if (inherits(x@valid_from, "Date")) format(x@valid_from) else NA_character_,
    valid_to   = if (inherits(x@valid_to,   "Date")) format(x@valid_to)   else NA_character_,
    n_limbs    = nrow(x@limbs),
    source     = if (is.na(x@source)) NA_character_ else x@source,
    saved_by   = as.character(saved_by),
    saved_at   = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    file_path  = paths$pq_file,
    notes      = as.character(notes)
  )

  if (file.exists(paths$register)) {
    existing_reg <- data.table::fread(paths$register, colClasses = "character",
                                      showProgress = FALSE)
    updated_reg  <- data.table::rbindlist(list(existing_reg, reg_row), fill = TRUE)
  } else {
    updated_reg <- reg_row
  }
  data.table::fwrite(updated_reg, paths$register)

  message(sprintf(
    "Saved RatingCurve v%d for site '%s'  ->  %s",
    version, x@station_id, paths$pq_file
  ))
  invisible(paths$pq_file)
}

S7::method(save_rating, RatingSet) <- function(x, root,
                                                saved_by = Sys.info()[["user"]],
                                                notes    = NA_character_) {
  paths <- vapply(x@curves, function(cv) {
    save_rating(cv, root = root, saved_by = saved_by, notes = notes)
  }, character(1L))
  invisible(paths)
}


# =============================================================================
# load_rating()
# =============================================================================

#' Load rating curve(s) from the Gold calibration store
#'
#' Reads the site's Parquet file and reconstructs either one [RatingCurve]
#' (when `version` is specified) or a [RatingSet] containing every stored
#' version (default).
#'
#' @param site_id Character. Station identifier (must match the `station_id`
#'   used when the curve was saved).
#' @param root Character. Root of the data store.
#' @param version Integer or `NULL`. When `NULL` (default), all versions are
#'   loaded and returned as a [RatingSet] ordered by version number.
#'   When a specific integer is given, only that version is returned as a
#'   [RatingCurve].
#'
#' @return A [RatingCurve] (single version) or [RatingSet] (all versions).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # All versions as a RatingSet — pass directly to apply_rating()
#' rs   <- load_rating("510310", root = "data/hydro")
#' flow <- apply_rating(level_obj, rs)
#'
#' # Inspect one specific version
#' rc3 <- load_rating("510310", root = "data/hydro", version = 3)
#' check_limb_continuity(rc3)
#' }
load_rating <- function(site_id, root, version = NULL) {
  paths <- .rating_paths(root, site_id)

  if (!file.exists(paths$pq_file)) {
    stop(sprintf(
      "No rating file found for site '%s'.\n  Expected: %s",
      site_id, paths$pq_file
    ))
  }

  dt <- data.table::as.data.table(arrow::read_parquet(paths$pq_file))

  if (!is.null(version)) {
    v  <- as.integer(version)
    dt <- dt[dt$version == v]
    if (nrow(dt) == 0L) {
      stop(sprintf("Version %d not found for site '%s'.", v, site_id))
    }
    return(.dt_to_curve(dt))
  }

  # Return all versions as a RatingSet
  versions <- sort(unique(dt$version))
  curves   <- lapply(versions, function(v) .dt_to_curve(dt[version == v]))

  RatingSet(curves = curves, station_id = site_id)
}


# =============================================================================
# list_ratings()
# =============================================================================

#' List saved rating curve versions from the governance register
#'
#' Reads `<root>/register/rating_register.csv` and prints a summary of
#' stored versions. Optionally filtered to one or more sites.
#'
#' @param root Character. Root of the data store.
#' @param site_id Character vector or `NULL`. Filter to specific site(s).
#'   `NULL` (default) shows all sites.
#'
#' @return The register `data.table`, invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' list_ratings("data/hydro")
#' list_ratings("data/hydro", site_id = "510310")
#' }
list_ratings <- function(root, site_id = NULL) {
  reg_path <- file.path(root, "register", "rating_register.csv")

  if (!file.exists(reg_path)) {
    message("No rating register found at: ", reg_path)
    return(invisible(data.table::data.table()))
  }

  reg <- data.table::fread(reg_path, showProgress = FALSE)

  if (!is.null(site_id)) {
    reg <- reg[reg$site_id %in% site_id]
  }

  if (nrow(reg) == 0L) {
    msg <- if (!is.null(site_id)) {
      paste0("No ratings found for site(s): ", paste(site_id, collapse = ", "))
    } else {
      "Rating register is empty."
    }
    message(msg)
    return(invisible(reg))
  }

  # Print condensed summary
  display <- reg[, .(site_id, version, valid_from, valid_to,
                     n_limbs, source, saved_by, saved_at, notes)]

  cat(sprintf("<Rating register>  %d version(s)\n\n", nrow(display)))
  print(display)
  invisible(reg)
}
