# ============================================================
# Tool:        Rating curve storage
# Description: Read/write helpers for persisting FlodeRatingCurve
#              and FlodeRatingSet objects under the governance
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

# Layout: <root>/<tier>/hydrometric/ratings/ratings.parquet, one Parquet per
# tier holding all sites and versions. The register is shared by all tiers. The
# category directory is fixed at "hydrometric".
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
# Numbers come from the REGISTER, not the Parquet, and are never reused
# (Superseded rows still count), so the next version is max + 1. If the
# register is lost while the Parquet remains, numbering restarts at 1 and would
# duplicate (site_id, version) pairs in the Parquet.
#' @noRd
.next_rating_version <- function(site_id, reg_path, tier) {
  if (!file.exists(reg_path)) return(1L)
  # Read as text so ids and version numbers keep their written form.
  reg <- data.table::fread(reg_path, colClasses = "character", showProgress = FALSE)
  if (nrow(reg) == 0L || !"site_id" %in% names(reg)) return(1L)
  # which() on whole vectors: the arguments site_id and tier are also column
  # names, so reg[site_id == site_id & ...] would match every row.
  idx  <- which(reg$site_id == site_id & reg$tier == tier)
  rows <- reg[idx]
  if (nrow(rows) == 0L) return(1L)
  max(as.integer(rows$version), na.rm = TRUE) + 1L
}

# Flatten one FlodeRatingCurve to a data.table row-per-limb ready for Parquet.
# Dates are stored as ISO-8601 text with NA for open-ended; limb_no records the
# limb order that .dt_to_curve() restores.
#' @noRd
.curve_to_dt <- function(x, version) {
  # Copy: := must not add columns to the curve's own limbs.
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

# Reconstruct one FlodeRatingCurve from a data.table slice (one version only).
# Rebuilding through FlodeRatingCurve() re-runs coercion and validation, so a
# damaged or hand-edited Parquet fails loudly on load. valid_from, valid_to and
# source are repeated on every limb row of a version; row 1 is used.
#' @noRd
.dt_to_curve <- function(dt) {
  # Restore limb order (the curve validator requires ascending `lower`). Sorts
  # the slice by reference; callers pass a subset, not the full table.
  data.table::setorder(dt, limb_no)
  limbs <- dt[, .(lower, upper, C, a, b, doubtful)]

  vf <- dt$valid_from[1L]
  vt <- dt$valid_to[1L]

  FlodeRatingCurve(
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
#' Writes a [FlodeRatingCurve] (or every curve in a [FlodeRatingSet]) into the
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
#' @details
#' Order of work: the curve's `station_id` is checked (a missing or empty one is
#' an error); the next version number is read from the register; the limbs are
#' appended to the tier's Parquet (the whole file is read, the new rows stacked
#' on, and the file rewritten); then earlier `"Active"` rows for the same site
#' and tier are marked `"Superseded"` and the new register row is appended. The
#' register, not the Parquet, is
#' the source of version numbers, and numbers are never reused.
#'
#' Each file is replaced atomically (written to a temporary file, then moved
#' into place, and read without memory-mapping so the replace also works on
#' Windows), but the two writes together are not atomic. If the process stops
#' after the Parquet is rewritten but before the register is, the Parquet holds a version the
#' register does not know about, and the next save for that site and tier takes
#' the same number again. Writers are also not safe to run concurrently against
#' one store, since each does a read-modify-write of the whole Parquet and
#' register.
#'
#' Validity windows are stored as given and are not compared with those of
#' existing versions at save time. Overlaps only surface when the versions are
#' loaded together (see [load_rating()]).
#'
#' For a [FlodeRatingSet], each curve is saved in list order through the same
#' method, using the curve's own `station_id` (the set's is ignored). Each gets
#' its own version and register row. The members of a set are the rating for
#' different periods, so they are all current together: the site's previous
#' `"Active"` versions are marked `"Superseded"` once, before the set is saved,
#' and every member of the new set is left `"Active"`. The return value is one
#' path per curve, all the same file.
#'
#' Directories are created as needed. A message reports any superseding and the
#' file written. Related: [list_ratings()], [load_rating()],
#' [check_limb_continuity()], [fix_limb_continuity()].
#'
#' @param x A [FlodeRatingCurve] or [FlodeRatingSet]. The curve's `station_id` slot
#'   must be set to a non-empty string.
#' @param ... Arguments passed to the method. For a [FlodeRatingCurve] or
#'   [FlodeRatingSet] these are:
#'   - `root`: Character. Root of the data store (the directory that contains
#'     `bronze/`, `silver/`, `gold/`, `register/`).
#'   - `tier`: Character. Storage tier: `"bronze"`, `"silver"`, or `"gold"`
#'     (default).
#'   - `saved_by`: Character. Name or username of the person saving the curve.
#'     Recorded in the register. Defaults to `Sys.info()[["user"]]`.
#'   - `method_of_receipt`: Character. How the rating was obtained, e.g.
#'     `"WISKI export"`, `"consultant report"`, `"manual entry"`. Recorded in
#'     the register. Defaults to `NA`.
#'   - `derivation_method`: Character. How the rating was developed. Suggested
#'     values: `"spot gaugings"` (field measurements only), `"hydraulic model"`
#'     (e.g. HEC-RAS, ISIS, TUFLOW), `"statistical model"`, or `"mixed"`
#'     (gaugings calibrated to a model). Free text is also accepted. Defaults
#'     to `NA`.
#'   - `model_or_project`: Character. Name of the hydraulic model, statistical
#'     framework, project, or team that produced the rating (free text). Useful
#'     when the same site has ratings from different sources over time.
#'     Defaults to `NA`.
#'   - `notes`: Character. Optional free-text note appended to the register row
#'     (e.g. `"Post-survey 2023 refit - upper limb revised"`).
#'
#' @section Provenance register:
#' Every saved curve gets a unique `rating_id` of the form
#' `{site_id}_RATING_{tier}_v{version}` (e.g. `510310_RATING_gold_v2`).
#' The register also records a `status` column. When a new version is saved,
#' any previously `"Active"` version for the same site and tier (other tiers
#' are not affected) is automatically updated to `"Superseded"`, so `list_ratings()` always
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

# Mark every "Active" register row for one site AND tier as "Superseded".
# Rows are selected with a logical vector built outside data.table scope: an
# earlier version wrote `tier == tier` inside `reg[...]`, where the bare `tier`
# is the COLUMN, so every tier's Active rows for the site were superseded.
# Returns the number of rows changed. Read-modify-write of the whole register.
#' @noRd
.supersede_active <- function(register_path, site_id, tier) {
  if (!file.exists(register_path)) return(0L)
  reg <- data.table::fread(register_path, colClasses = "character",
                           showProgress = FALSE)
  if (!"status" %in% names(reg)) return(0L)
  hit <- which(reg$site_id == site_id & reg$tier == tier & reg$status == "Active")
  if (length(hit) == 0L) return(0L)
  data.table::set(reg, i = hit, j = "status", value = "Superseded")
  data.table::fwrite(reg, register_path)
  length(hit)
}

S7::method(save_rating, FlodeRatingCurve) <- function(x, root,
                                                  tier               = "gold",
                                                  saved_by           = Sys.info()[["user"]],
                                                  method_of_receipt  = NA_character_,
                                                  derivation_method  = NA_character_,
                                                  model_or_project   = NA_character_,
                                                  notes              = NA_character_,
                                                  .supersede         = TRUE) {
  # `.supersede` is internal: the FlodeRatingSet method turns it off for the
  # members of a set, which are all current together
  # -- Validate and resolve paths --------------------------------------------
  if (is.na(x@station_id) || !nzchar(x@station_id)) {
    stop("`station_id` must be set on the FlodeRatingCurve before saving.")
  }
  tier <- match.arg(tier, c("bronze", "silver", "gold"))

  paths <- .rating_paths(root, tier)
  dir.create(paths$ratings_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(dirname(paths$register), recursive = TRUE, showWarnings = FALSE)

  # -- Version and limbs -----------------------------------------------------
  # Version comes from the register (see .next_rating_version()); the curve is
  # flattened to one row per limb.
  version    <- .next_rating_version(x@station_id, paths$register, tier)
  rating_id  <- sprintf("%s_RATING_%s_v%d", x@station_id, tier, version)
  limbs_flat <- .curve_to_dt(x, version)

  # -- Append limbs to Parquet -----------------------------------------------
  # Read-modify-write of the whole shared file. fill = TRUE tolerates an
  # existing file that lacks columns added later. The read is not memory-mapped
  # and the write goes through the temp-file-and-rename writer used for Bronze
  # (see .read_parquet() / .write_parquet_atomic()), so replacing a file that was
  # just read works on Windows too. The Parquet write is atomic, but it is
  # separate from the register write below.
  if (file.exists(paths$pq_file)) {
    existing <- data.table::as.data.table(.read_parquet(paths$pq_file))
    combined <- data.table::rbindlist(list(existing, limbs_flat), fill = TRUE)
  } else {
    combined <- limbs_flat
  }
  .write_parquet_atomic(combined, paths$pq_file)

  # -- Register --------------------------------------------------------------
  # New versions start "Active". derivation_method and model_or_project live
  # only in the register, not in the Parquet. file_path is the shared Parquet,
  # the same for every version.
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

  # Earlier "Active" versions of this site + tier are superseded by the new one
  # (not other tiers, and not the site's versions in other tiers). Done before
  # the new row is appended so the new row itself stays "Active".
  n_superseded <- if (.supersede) {
    .supersede_active(paths$register, x@station_id, tier)
  } else {
    0L
  }
  if (n_superseded > 0L) {
    message(sprintf("  Superseded %d previously Active version(s) at [%s] for site '%s'.",
                    n_superseded, tier, x@station_id))
  }

  if (file.exists(paths$register)) {
    existing_reg <- data.table::fread(paths$register, colClasses = "character",
                                      showProgress = FALSE)
    updated_reg <- data.table::rbindlist(list(existing_reg, reg_row), fill = TRUE)
  } else {
    updated_reg <- reg_row
  }
  # The whole register is rewritten, not appended to.
  data.table::fwrite(updated_reg, paths$register)

  message(sprintf(
    "Saved FlodeRatingCurve [%s] v%d for site '%s'  ->  %s",
    tier, version, x@station_id, paths$pq_file
  ))
  invisible(paths$pq_file)
}

S7::method(save_rating, FlodeRatingSet) <- function(x, root,
                                                tier              = "gold",
                                                saved_by          = Sys.info()[["user"]],
                                                method_of_receipt = NA_character_,
                                                derivation_method = NA_character_,
                                                model_or_project  = NA_character_,
                                                notes             = NA_character_) {
  # The members of a set are the rating for different periods, so they are all
  # current together. Supersede the previous Active versions ONCE (per distinct
  # site and tier), then save every member without superseding the others.
  # Curves are saved one at a time in list order, so versions are numbered in
  # that order. The result is one path per curve.
  reg_path <- .rating_paths(root, tier)$register
  for (site in unique(vapply(x@curves, function(cv) cv@station_id, character(1L)))) {
    n_old <- .supersede_active(reg_path, site, tier)
    if (n_old > 0L) {
      message(sprintf("  Superseded %d previously Active version(s) at [%s] for site '%s'.",
                      n_old, tier, site))
    }
  }
  paths <- vapply(x@curves, function(cv) {
    save_rating(cv, root = root, tier = tier, saved_by = saved_by,
                method_of_receipt = method_of_receipt,
                derivation_method = derivation_method,
                model_or_project  = model_or_project,
                notes             = notes,
                .supersede        = FALSE)
  }, character(1L))
  invisible(paths)
}


# =============================================================================
# load_rating()
# =============================================================================

#' Load rating curve(s) from the Gold calibration store
#'
#' Reads the shared ratings Parquet, filters to `site_id`, and reconstructs
#' either one [FlodeRatingCurve] (when `version` is specified) or a [FlodeRatingSet]
#' containing every stored version for that site (default).
#'
#' @details
#' The shared Parquet of the chosen tier is read in full and filtered to
#' `site_id`. The register is not consulted, so a version's `status`
#' (`"Active"` or `"Superseded"`) has no effect on what is loaded. Each stored
#' version is rebuilt through the [FlodeRatingCurve] constructor, which
#' re-validates it. A missing Parquet, an unknown site and an unknown `version`
#' are errors; nothing is printed.
#'
#' With `version = NULL` all versions are returned as a [FlodeRatingSet], whose
#' validator requires non-overlapping validity windows. A window with
#' `valid_to = NA` is open-ended, so an earlier version saved without a
#' `valid_to` overlaps any later version that has a `valid_from`, and loading
#' fails with the set's overlap error. Give superseded versions a closed window
#' when saving them, or load them one at a time with `version`. A version saved
#' without `valid_from` loads, but cannot be selected by [apply_rating()] inside
#' a set.
#'
#' Related: [save_rating()], [list_ratings()], [apply_rating()].
#'
#' @param site_id Character. Station identifier (must match the `station_id`
#'   used when the curve was saved).
#' @param root Character. Root of the data store.
#' @param tier Character. Tier to load from: `"bronze"`, `"silver"`, or
#'   `"gold"` (default).
#' @param version Integer or `NULL`. When `NULL` (default), all versions are
#'   loaded and returned as a [FlodeRatingSet] ordered by version number.
#'   When a specific integer is given, only that version is returned as a
#'   [FlodeRatingCurve].
#'
#' @return A [FlodeRatingCurve] (single version) or [FlodeRatingSet] (all versions).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Gold (default) — all versions as a FlodeRatingSet for apply_rating()
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

  # The whole shared Parquet is read, then filtered. `site_id` is also a column
  # name, hence which() on dt$site_id rather than dt[site_id == site_id].
  dt <- data.table::as.data.table(.read_parquet(paths$pq_file))
  idx <- which(dt$site_id == site_id)
  dt  <- dt[idx]

  if (nrow(dt) == 0L) {
    stop(sprintf("No ratings found for site '%s'.", site_id))
  }

  # Explicit dt$version below: a bare `version` inside dt[...] is the column,
  # not this function's argument.
  if (!is.null(version)) {
    v  <- as.integer(version)
    dt <- dt[dt$version == v]
    if (nrow(dt) == 0L) {
      stop(sprintf("Version %d not found for site '%s'.", v, site_id))
    }
    return(.dt_to_curve(dt))
  }

  # Return all versions as a FlodeRatingSet
  versions <- sort(unique(dt$version))
  # Inside dt[...] `version` is the COLUMN (intended) and `v` the loop value;
  # the function argument `version` is NULL on this path. The set's validator
  # rejects overlapping validity windows.
  curves   <- lapply(versions, function(v) .dt_to_curve(dt[version == v]))

  FlodeRatingSet(curves = curves, station_id = site_id)
}


# =============================================================================
# list_ratings()
# =============================================================================

#' List saved rating curve versions from the governance register
#'
#' Reads `<root>/register/rating_register.csv` and prints a summary of
#' stored versions. Optionally filtered to one or more sites.
#'
#' @details
#' Only the register is read (not the Parquet), so the listing shows what was
#' recorded at save time, including each version's `status`: `"Superseded"`
#' rows stay listed. Filters are exact any-of matches. If the register is
#' missing, or nothing matches, a message (not an error) is issued and an empty
#' table is returned.
#'
#' The printed table is a condensed view (identity, validity, provenance and
#' note columns; `file_path` and any other columns are left out), but the
#' invisibly returned value is the complete filtered register. Column types here
#' come from `fread()` autodetection (`version` is integer), unlike
#' [save_rating()], which reads the register as text. Related: [save_rating()],
#' [load_rating()].
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

  # `site_id` and `tier` are also column names: a bare symbol inside reg[...]
  # would resolve to the column and keep every row, so .filter_rows() matches
  # outside the table scope.
  if (!is.null(site_id)) reg <- .filter_rows(reg, "site_id", site_id)
  if (!is.null(tier))    reg <- .filter_rows(reg, "tier", tier)

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
  # `..` looks present_cols up in the calling scope, not as a column.
  display      <- reg[, ..present_cols]

  cat(sprintf("<Rating register>  %d version(s)\n\n", nrow(display)))
  print(display)
  invisible(reg)
}
