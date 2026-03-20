# ============================================================
# Tool:        Rating curve storage
# Description: Read/write helpers for persisting RatingCurve
#              and RatingSet objects under the governance
#              directory structure.
#
#              Storage location (all three tiers):
#                <root>/bronze/hydrometric/ratings/ratings.parquet
#                <root>/silver/hydrometric/ratings/ratings.parquet
#                <root>/gold/hydrometric/ratings/ratings.parquet
#                  (one file per tier; all sites and versions stacked)
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
#              Register-only provenance fields:
#                derivation_method chr  How the rating was developed:
#                                        "spot gaugings", "hydraulic model",
#                                        "statistical model", "mixed", or
#                                        free text
#                model_or_project  chr  Model name, project, or team (free text)
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
.rating_paths <- function(root, tier = "gold") {
  tier        <- match.arg(tier, c("bronze", "silver", "gold"))
  ratings_dir <- file.path(root, tier, "hydrometric", "ratings")
  list(
    ratings_dir = ratings_dir,
    pq_file     = file.path(ratings_dir, "ratings.parquet"),
    register    = file.path(root, "register", "rating_register.csv")
  )
}

# Return the next version integer for a site_id + tier given the register CSV.
# Versions are scoped per tier so bronze/silver/gold each start at 1.
#' @noRd
.next_rating_version <- function(site_id, reg_path, tier) {
  if (!file.exists(reg_path)) return(1L)
  reg <- data.table::fread(reg_path, colClasses = "character", showProgress = FALSE)
  if (nrow(reg) == 0L || !"site_id" %in% names(reg)) return(1L)
  rows <- reg[reg$site_id == site_id & reg$tier == tier]
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

#' Save a rating curve to the hydrometric ratings store
#'
#' Writes a [RatingCurve] (or every curve in a [RatingSet]) into the
#' shared `<root>/<tier>/hydrometric/ratings/ratings.parquet` file and
#' appends a governance row to `<root>/register/rating_register.csv`.
#'
#' Three tiers are supported, matching the observation data lifecycle:
#' \describe{
#'   \item{`"bronze"`}{As-received from the source system (WISKI export,
#'     consultant report, etc.). Immutable once saved.}
#'   \item{`"silver"`}{QC-reviewed: continuity checked, limb boundaries
#'     verified, but not yet formally signed off.}
#'   \item{`"gold"`}{Approved for production use. Default tier.}
#' }
#'
#' Each call is assigned the next available version number for that
#' `station_id` **within** the target tier. Existing versions are never
#' overwritten — the Parquet accumulates all sites and versions,
#' differentiated by `site_id` and `version`. The register records `tier`
#' so the full lineage (bronze → silver → gold) is traceable.
#'
#' The ratings directory and register are created automatically if they
#' do not yet exist (no need to call [setup_hydro_store()] first).
#'
#' @param x A [RatingCurve] or [RatingSet]. The curve's `station_id` slot
#'   must be set to a non-empty string.
#' @param root Character. Root of the data store
#'   (the directory that contains `bronze/`, `silver/`, `gold/`,
#'   `register/`).
#' @param tier Character. Storage tier: `"bronze"`, `"silver"`, or
#'   `"gold"` (default).
#' @param saved_by Character. Name or username of the person saving the
#'   curve. Recorded in the register. Defaults to `Sys.info()[["user"]]`.
#' @param method_of_receipt Character. How the rating was obtained, e.g.
#'   `"WISKI export"`, `"consultant report"`, `"manual entry"`. Recorded
#'   in the register. Defaults to `NA`.
#' @param derivation_method Character. How the rating was developed.
#'   Suggested values: `"spot gaugings"` (field measurements only),
#'   `"hydraulic model"` (e.g. HEC-RAS, ISIS, TUFLOW), `"statistical model"`,
#'   or `"mixed"` (gaugings calibrated to a model). Free text is also
#'   accepted. Defaults to `NA`.
#' @param model_or_project Character. Name of the hydraulic model, statistical
#'   framework, project, or team that produced the rating (free text).
#'   Useful when the same site has ratings from different sources over time.
#'   Defaults to `NA`.
#' @param notes Character. Optional free-text note appended to the register
#'   row (e.g. `"Post-survey 2023 refit — upper limb revised"`).
#'
#' @section Provenance register:
#' Every saved curve gets a unique `rating_id` of the form
#' `{site_id}_RATING_{tier}_v{version}` (e.g. `510310_RATING_gold_v2`).
#' The register also records a `status` column. When a new version is saved,
#' any previously `"Active"` version for the same site and tier is
#' automatically updated to `"Superseded"`, so `list_ratings()` always
#' shows which version is currently in use.
#'
#' @return The path(s) to the written Parquet file(s), invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Bronze — as received from WISKI
#' save_rating(rc_raw, root = "data/hydro", tier = "bronze",
#'             saved_by = "J.Payne", method_of_receipt = "WISKI export",
#'             derivation_method = "spot gaugings",
#'             notes = "WISKI export 2023-11-01")
#'
#' # Silver — after continuity check and correction
#' rc_fixed <- fix_limb_continuity(rc_raw)
#' save_rating(rc_fixed, root = "data/hydro", tier = "silver",
#'             saved_by = "J.Payne", method_of_receipt = "derived from bronze v1",
#'             derivation_method = "spot gaugings",
#'             notes = "Continuity corrected")
#'
#' # Gold — modelled rating from a hydraulic model project
#' save_rating(rc_model, root = "data/hydro", tier = "gold",
#'             saved_by = "J.Payne", method_of_receipt = "ISIS 2D model output",
#'             derivation_method = "hydraulic model",
#'             model_or_project  = "TUFLOW_Phase2_2023",
#'             notes = "Approved by lead hydrologist")
#'
#' # Gold — mixed: field gaugings calibrated against hydraulic model
#' save_rating(rc_fixed, root = "data/hydro", tier = "gold",
#'             saved_by = "J.Payne", method_of_receipt = "approved from silver v1",
#'             derivation_method = "mixed",
#'             model_or_project  = "Flood Risk Assessment 2023, Hydrology Team",
#'             notes = "Approved by lead hydrologist")
#'
#' # Save an entire time-varying set in one call
#' save_rating(rating_set, root = "data/hydro", tier = "bronze",
#'             saved_by = "J.Payne", method_of_receipt = "WISKI export",
#'             derivation_method = "spot gaugings")
#' }
save_rating <- S7::new_generic("save_rating", "x")

S7::method(save_rating, RatingCurve) <- function(x, root,
                                                  tier               = "gold",
                                                  saved_by           = Sys.info()[["user"]],
                                                  method_of_receipt  = NA_character_,
                                                  derivation_method  = NA_character_,
                                                  model_or_project   = NA_character_,
                                                  notes              = NA_character_) {
  if (is.na(x@station_id) || !nzchar(x@station_id)) {
    stop("`station_id` must be set on the RatingCurve before saving.")
  }
  tier <- match.arg(tier, c("bronze", "silver", "gold"))

  paths <- .rating_paths(root, tier)
  dir.create(paths$ratings_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(dirname(paths$register), recursive = TRUE, showWarnings = FALSE)

  version    <- .next_rating_version(x@station_id, paths$register, tier)
  rating_id  <- sprintf("%s_RATING_%s_v%d", x@station_id, tier, version)
  limbs_flat <- .curve_to_dt(x, version)

  # Append to or create Parquet
  if (file.exists(paths$pq_file)) {
    existing <- data.table::as.data.table(arrow::read_parquet(paths$pq_file))
    combined <- data.table::rbindlist(list(existing, limbs_flat), fill = TRUE)
  } else {
    combined <- limbs_flat
  }
  arrow::write_parquet(combined, paths$pq_file)

  # Build new register row
  reg_row <- data.table::data.table(
    rating_id         = rating_id,
    site_id           = x@station_id,
    tier              = tier,
    status            = "Active",
    version           = as.integer(version),
    valid_from        = if (inherits(x@valid_from, "Date")) format(x@valid_from) else NA_character_,
    valid_to          = if (inherits(x@valid_to,   "Date")) format(x@valid_to)   else NA_character_,
    n_limbs           = nrow(x@limbs),
    source            = if (is.na(x@source)) NA_character_ else x@source,
    method_of_receipt = as.character(method_of_receipt),
    derivation_method = as.character(derivation_method),
    model_or_project  = as.character(model_or_project),
    saved_by          = as.character(saved_by),
    saved_at          = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    file_path         = paths$pq_file,
    notes             = as.character(notes)
  )

  if (file.exists(paths$register)) {
    existing_reg <- data.table::fread(paths$register, colClasses = "character",
                                      showProgress = FALSE)
    # Supersede any previously Active version for this site + tier
    if ("status" %in% names(existing_reg)) {
      n_superseded <- sum(existing_reg$site_id == x@station_id &
                          existing_reg$tier    == tier          &
                          existing_reg$status  == "Active")
      existing_reg[site_id == x@station_id & tier == tier & status == "Active",
                   status := "Superseded"]
      if (n_superseded > 0L) {
        message(sprintf("  Superseded %d previously Active version(s) at [%s] for site '%s'.",
                        n_superseded, tier, x@station_id))
      }
    }
    updated_reg <- data.table::rbindlist(list(existing_reg, reg_row), fill = TRUE)
  } else {
    updated_reg <- reg_row
  }
  data.table::fwrite(updated_reg, paths$register)

  message(sprintf(
    "Saved RatingCurve [%s] v%d for site '%s'  ->  %s",
    tier, version, x@station_id, paths$pq_file
  ))
  invisible(paths$pq_file)
}

S7::method(save_rating, RatingSet) <- function(x, root,
                                                tier              = "gold",
                                                saved_by          = Sys.info()[["user"]],
                                                method_of_receipt = NA_character_,
                                                derivation_method = NA_character_,
                                                model_or_project  = NA_character_,
                                                notes             = NA_character_) {
  paths <- vapply(x@curves, function(cv) {
    save_rating(cv, root = root, tier = tier, saved_by = saved_by,
                method_of_receipt = method_of_receipt,
                derivation_method = derivation_method,
                model_or_project  = model_or_project,
                notes             = notes)
  }, character(1L))
  invisible(paths)
}


# =============================================================================
# load_rating()
# =============================================================================

#' Load rating curve(s) from the Gold calibration store
#'
#' Reads the shared ratings Parquet, filters to `site_id`, and reconstructs
#' either one [RatingCurve] (when `version` is specified) or a [RatingSet]
#' containing every stored version for that site (default).
#'
#' @param site_id Character. Station identifier (must match the `station_id`
#'   used when the curve was saved).
#' @param root Character. Root of the data store.
#' @param tier Character. Tier to load from: `"bronze"`, `"silver"`, or
#'   `"gold"` (default).
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
#' # Gold (default) — all versions as a RatingSet for apply_rating()
#' rs   <- load_rating("510310", root = "data/hydro")
#' flow <- apply_rating(level_obj, rs)
#'
#' # Load from bronze to compare against gold
#' rc_bronze <- load_rating("510310", root = "data/hydro", tier = "bronze",
#'                          version = 1)
#' rc_gold   <- load_rating("510310", root = "data/hydro", version = 1)
#' }
load_rating <- function(site_id, root, tier = "gold", version = NULL) {
  tier  <- match.arg(tier, c("bronze", "silver", "gold"))
  paths <- .rating_paths(root, tier)

  if (!file.exists(paths$pq_file)) {
    stop(sprintf(
      "No ratings Parquet found.\n  Expected: %s",
      paths$pq_file
    ))
  }

  dt <- data.table::as.data.table(arrow::read_parquet(paths$pq_file))
  dt <- dt[dt$site_id == site_id]

  if (nrow(dt) == 0L) {
    stop(sprintf("No ratings found for site '%s'.", site_id))
  }

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
#' @param tier Character vector or `NULL`. Filter to one or more tiers
#'   (`"bronze"`, `"silver"`, `"gold"`). `NULL` (default) shows all tiers.
#'
#' @return The register `data.table`, invisibly.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' list_ratings("data/hydro")
#' list_ratings("data/hydro", site_id = "510310")
#' list_ratings("data/hydro", tier = "gold")
#' list_ratings("data/hydro", site_id = "510310", tier = c("silver", "gold"))
#' }
list_ratings <- function(root, site_id = NULL, tier = NULL) {
  reg_path <- file.path(root, "register", "rating_register.csv")

  if (!file.exists(reg_path)) {
    message("No rating register found at: ", reg_path)
    return(invisible(data.table::data.table()))
  }

  reg <- data.table::fread(reg_path, showProgress = FALSE)

  if (!is.null(site_id)) reg <- reg[reg$site_id %in% site_id]
  if (!is.null(tier))    reg <- reg[reg$tier    %in% tier]

  if (nrow(reg) == 0L) {
    parts <- c(
      if (!is.null(site_id)) paste0("site(s): ", paste(site_id, collapse = ", ")),
      if (!is.null(tier))    paste0("tier(s): ", paste(tier,    collapse = ", "))
    )
    msg <- if (length(parts)) {
      paste0("No ratings found for ", paste(parts, collapse = "; "))
    } else {
      "Rating register is empty."
    }
    message(msg)
    return(invisible(reg))
  }

  # Print condensed summary — lead with identity and status columns
  core_cols    <- c("rating_id", "site_id", "tier", "status", "version",
                    "valid_from", "valid_to", "n_limbs", "source",
                    "method_of_receipt", "derivation_method", "model_or_project",
                    "saved_by", "saved_at", "notes")
  present_cols <- intersect(core_cols, names(reg))
  display      <- reg[, ..present_cols]

  cat(sprintf("<Rating register>  %d version(s)\n\n", nrow(display)))
  print(display)
  invisible(reg)
}
