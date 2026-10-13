# ============================================================
# Tool:        Rating curves and rated flow
# Description: S7 classes for multi-limb stage-discharge
#              rating curves (Q = C(h-a)^b) with optional
#              time-varying support, limb-continuity checks,
#              and continuity correction.
# Flode Module: flode.io
# Author:      JP
# Created:     2026-03-19
# Tier:        2
# Inputs:      data.table of rating limbs; FlodeLevel_Daily or
#              FlodeLevel_15min S7 objects
# Outputs:     FlodeRatingCurve / FlodeRatingSet S7 objects;
#              FlodeFlow_Daily / FlodeFlow_15min rated output
# Dependencies: S7, data.table
# Notes:       Equation form: Q = C * (h - a)^b
#              Limbs must be ordered and contiguous — upper[i]
#              must equal lower[i+1].  The "doubtful" flag on
#              a limb marks out-of-bank extrapolation; rated
#              flow is still computed but flagged in the output.
# ============================================================

#' @import S7
NULL


# =============================================================================
# Internal helpers
# =============================================================================

# Normalise column names supplied in the limbs data.frame so users can pass
# common variants (e.g. "Lower.level", "upper_level", "multiplier") and still
# get a valid data.table.
#' @noRd
.normalise_limb_names <- function(dt) {
  # Renames by reference, one alias per target (first match in column order).
  # Only the spellings listed are recognised; any other name is left alone and
  # the validator then reports the column as missing. The constructor passes
  # its own copy of the caller's table.
  nm  <- names(dt)
  map <- list(
    lower    = c("lower", "lower_level", "lower.level", "Lower", "Lower_level", "Lower.level"),
    upper    = c("upper", "upper_level", "upper.level", "Upper", "Upper_level", "Upper.level"),
    C        = c("C", "c_param", "multiplier", "Multiplier"),
    a        = c("a", "A", "a_param", "offset",     "Offset",     "datum"),
    b        = c("b", "B", "b_param", "exponent",   "Exponent"),
    doubtful = c("doubtful", "Doubtful", "doubtful_flag", "flag")
  )
  for (target in names(map)) {
    hit <- intersect(nm, map[[target]])[1L]
    if (!is.na(hit) && hit != target) {
      data.table::setnames(dt, hit, target)
      nm <- names(dt)
    }
  }
  dt
}

# Vectorised rating: given a numeric vector of stage values h and a limbs
# data.table, returns list(value, doubtful).
#   - h below the lowest lower boundary  → NA (no applicable limb)
#   - (h - a) <= 0 within a limb         → Q = 0  (stage at/below datum)
#   - NaN from 0^0 edge case             → 0
#' @noRd
.rate_stage <- function(h, limbs) {
  # findInterval() looks at the LOWER bounds only: limb i covers
  # [lower[i], lower[i+1]), so a stage exactly on a junction is rated by the
  # upper limb, and the last limb's `upper` is never consulted (higher stages
  # are extrapolated with that limb and take its doubtful flag).
  idx    <- findInterval(h, limbs$lower)   # 0 = below all limbs
  n      <- length(h)
  q      <- rep(NA_real_, n)
  dbt    <- rep(NA, n)

  valid  <- !is.na(h) & idx >= 1L
  if (any(valid)) {
    li         <- idx[valid]
    hv         <- h[valid]
    # (h - a) is floored at 0 so a stage at or below the datum gives Q = 0
    # rather than a negative base raised to a fractional power (NaN).
    q[valid]   <- pmax(0,
                       limbs$C[li] * pmax(hv - limbs$a[li], 0)^limbs$b[li])
    q[valid][is.nan(q[valid])] <- 0
    dbt[valid] <- limbs$doubtful[li]
  }

  list(value = q, doubtful = dbt)
}

# For each Date in `dates`, return the integer index of the applicable
# FlodeRatingCurve in rating_set@curves (NA_integer_ if none applies).
#' @noRd
.find_valid_curve <- function(dates, rating_set) {
  curves <- rating_set@curves
  n_obs  <- length(dates)
  idx    <- rep(NA_integer_, n_obs)

  # Later curves overwrite earlier ones where windows overlap (the set
  # validator normally rules that out). Matching is on calendar date, both ends
  # inclusive, with an NA valid_to meaning open-ended. A curve whose valid_from
  # is NA matches nothing: the comparison is NA, and assigning through an NA
  # index is skipped.
  for (i in seq_along(curves)) {
    cv   <- curves[[i]]
    from <- cv@valid_from
    to   <- cv@valid_to
    in_range <- if (is.na(to)) {
      !is.na(dates) & dates >= from
    } else {
      !is.na(dates) & dates >= from & dates <= to
    }
    idx[in_range] <- i
  }
  idx
}

# Compute Q at the junction between limb i and limb i+1 from each side.
# Each limb is evaluated with its own C, a and b at the shared stage upper[i]
# (equal to lower[i+1] within the 1e-9 the validator allows), with (h - a)
# floored at 0 as in .rate_stage().
#' @noRd
.junction_Q <- function(limbs, i) {
  h <- limbs$upper[i]  # == limbs$lower[i + 1]
  list(
    Q_lower = limbs$C[i]     * max(h - limbs$a[i],     0)^limbs$b[i],
    Q_upper = limbs$C[i + 1] * max(h - limbs$a[i + 1], 0)^limbs$b[i + 1],
    h       = h
  )
}


# =============================================================================
# FlodeRatingCurve
# =============================================================================

#' S7 class for a multi-limb stage-discharge rating curve
#'
#' Holds the limb table for a single rating and an optional validity window.
#' The rating equation applied within each limb is:
#'
#' \deqn{Q = C \cdot (h - a)^b}
#'
#' where \eqn{h} is gauge stage (m), \eqn{Q} is discharge (m³/s), and
#' \eqn{C}, \eqn{a}, \eqn{b} are per-limb parameters.
#'
#' @details
#' The constructor accepts any `data.frame`. Common column spellings (for
#' example `Lower.level`, `multiplier`, `datum`, `exponent`, `flag`) are renamed
#' to `lower`, `upper`, `C`, `a`, `b` and `doubtful`, those columns are coerced
#' to numeric (`doubtful` to logical), and the result is validated. The table
#' you pass in is not modified.
#'
#' Validation rejects a curve unless:
#'
#' - all six columns are present and there is at least one limb;
#' - `C`, `a` and `b` are finite, with `C > 0` and `b > 0`;
#' - each limb has `lower < upper`, limbs are sorted by `lower`, and
#'   `upper[i]` equals `lower[i + 1]` to within 1e-9 m;
#' - `valid_from` and `valid_to` are each `NA` or a `Date`.
#'
#' Contiguity is checked on **stage** only. Whether adjacent limbs give the same
#' *discharge* at their shared stage is a separate question: see
#' [check_limb_continuity()] and [fix_limb_continuity()]. The order of
#' `valid_from` and `valid_to`, and the content of `station_id`, are not
#' checked.
#'
#' How a curve is applied by [apply_rating()]: limb `i` covers
#' `[lower[i], upper[i])`, so a stage exactly on a junction is rated by the
#' upper limb; a stage below the first `lower` has no rating (`NA`); a stage
#' above the last `upper` is extrapolated with the last limb; and a stage at or
#' below `a` gives zero discharge.
#'
#' @param limbs A `data.table` with one row per rating limb and columns:
#'   \describe{
#'     \item{`lower`}{Lower stage boundary (m). The first limb's lower bound
#'       is typically 0 or the channel invert.}
#'     \item{`upper`}{Upper stage boundary (m). The final limb should have
#'       `upper = Inf` to handle all stages above the highest measured point.}
#'     \item{`C`}{Multiplier (positive numeric).}
#'     \item{`a`}{Stage offset / datum correction (numeric). Flow is zero when
#'       \eqn{h \le a}.}
#'     \item{`b`}{Exponent (positive numeric, typically 1.5–3).}
#'     \item{`doubtful`}{Logical flag. `TRUE` marks extrapolated (usually
#'       out-of-bank) limbs whose rated flows should not be plotted or used
#'       uncritically.}
#'   }
#'   Rows must be sorted by `lower` (ascending) and boundaries must be
#'   contiguous (`upper[i]` == `lower[i+1]`).
#' @param valid_from Date (or `NA`). Start of the period this curve applies.
#' @param valid_to Date (or `NA`). End of the period (`NA` = open-ended /
#'   still current).
#' @param station_id Character. Station identifier (free-text, for labelling).
#' @param source Character. Origin of the curve (e.g. `"WISKI"`, `"manual"`).
#'
#' @export
FlodeRatingCurve <- S7::new_class(
  "FlodeRatingCurve",
  package    = "reach.io",
  properties = list(
    limbs      = S7::new_property(class = S7::class_any),
    valid_from = S7::new_property(class = S7::class_any),
    valid_to   = S7::new_property(class = S7::class_any),
    station_id = S7::new_property(class = S7::class_character),
    source     = S7::new_property(class = S7::class_character)
  ),
  constructor = function(limbs,
                         valid_from = NA,
                         valid_to   = NA,
                         station_id = NA_character_,
                         source     = NA_character_) {
    # as.data.table() copies a data.table input, so the in-place renaming and
    # coercion below never touch the caller's table.
    dt <- data.table::as.data.table(limbs)
    dt <- .normalise_limb_names(dt)

    # Coerce types where safely possible
    # as.numeric() on non-numeric text gives NA with a warning; the validator
    # then rejects NA in C, a and b.
    for (col in c("lower", "upper", "C", "a", "b")) {
      if (col %in% names(dt)) {
        data.table::set(dt, j = col, value = as.numeric(dt[[col]]))
      }
    }
    if ("doubtful" %in% names(dt)) {
      data.table::set(dt, j = "doubtful", value = as.logical(dt[["doubtful"]]))
    }

    S7::new_object(S7::S7_object(),
                   limbs      = dt,
                   valid_from = valid_from,
                   valid_to   = valid_to,
                   station_id = station_id,
                   source     = source)
  },
  validator = function(self) {
    dt <- self@limbs

    # -- Structure and types -------------------------------------------------
    if (!inherits(dt, "data.table")) {
      return("`limbs` must be a data.table.")
    }

    required <- c("lower", "upper", "C", "a", "b", "doubtful")
    missing  <- setdiff(required, names(dt))
    if (length(missing) > 0L) {
      return(sprintf("`limbs` is missing column(s): %s.",
                     paste(missing, collapse = ", ")))
    }

    if (nrow(dt) < 1L) {
      return("`limbs` must have at least one row.")
    }

    for (col in c("lower", "upper", "C", "a", "b")) {
      if (!is.numeric(dt[[col]])) {
        return(sprintf("`limbs$%s` must be numeric.", col))
      }
    }
    if (!is.logical(dt$doubtful)) {
      return("`limbs$doubtful` must be logical.")
    }

    # -- Parameter values ----------------------------------------------------
    # NA/NaN/Inf would propagate silently into rated flows. C > 0 and b > 0 keep
    # Q non-negative and increasing in h, and keep the inverse (Q/C)^(1/b)
    # defined.
    if (any(!is.finite(dt$C) | !is.finite(dt$a) | !is.finite(dt$b))) {
      return("`limbs` columns C, a, and b must all be finite (no NA, NaN, or Inf).")
    }
    if (any(dt$C <= 0)) {
      return("`limbs$C` must be positive (> 0).")
    }
    if (any(dt$b <= 0)) {
      return("`limbs$b` must be positive (> 0).")
    }

    # -- Limb geometry -------------------------------------------------------
    # na.rm = TRUE: an NA bound is caught by neither this check nor the
    # contiguity check below.
    if (any(dt$lower >= dt$upper, na.rm = TRUE)) {
      return("Each limb's `lower` must be strictly less than its `upper`.")
    }

    if (nrow(dt) > 1L) {
      # .rate_stage() runs findInterval() on `lower`, which needs ascending
      # breakpoints.
      if (!all(dt$lower == sort(dt$lower))) {
        return("`limbs` must be sorted by `lower` in ascending order.")
      }
      # Contiguity is on stage, with 1e-9 m absolute tolerance for floating-point
      # noise. Whether Q agrees at the junction is checked elsewhere
      # (check_limb_continuity()).
      gaps <- abs(dt$upper[-nrow(dt)] - dt$lower[-1L])
      if (any(gaps > 1e-9, na.rm = TRUE)) {
        bad <- which(gaps > 1e-9)[1L]
        return(sprintf(
          "Limb boundaries are not contiguous: `upper[%d]` (%.6g) != `lower[%d]` (%.6g). ",
          bad, dt$upper[bad], bad + 1L, dt$lower[bad + 1L]
        ))
      }
    }

    # -- Validity dates ------------------------------------------------------
    # NA means open on that side. The two dates are not compared with each other.
    if (!is.na(self@valid_from) && !inherits(self@valid_from, "Date")) {
      return("`valid_from` must be a Date or NA.")
    }
    if (!is.na(self@valid_to) && !inherits(self@valid_to, "Date")) {
      return("`valid_to` must be a Date or NA.")
    }

    NULL
  }
)


# =============================================================================
# FlodeRatingSet
# =============================================================================

#' S7 class for a time-varying collection of rating curves
#'
#' Groups multiple [FlodeRatingCurve] objects that each apply over a distinct
#' calendar period — for instance to account for channel changes, re-surveys,
#' or shift corrections. [apply_rating()] uses `valid_from` / `valid_to` on
#' each curve to select the correct rating for every observation.
#'
#' @details
#' The constructor accepts a single [FlodeRatingCurve] and wraps it in a list.
#' Validation checks that the list is non-empty, every element is a
#' [FlodeRatingCurve], and no two validity windows overlap. Windows are closed
#' on both ends (a curve ending on 2020-03-31 and one starting on the same day
#' overlap) and `valid_to = NA` is open-ended. Only pairs of curves that both
#' have a `valid_from` are compared, so a curve without one escapes the check;
#' such a curve is also never selected by [apply_rating()], which matches rows
#' on the window.
#'
#' The order of curves in the list does not matter, and the set's `station_id`
#' is not compared with the `station_id` of its curves.
#'
#' @param curves A `list` of [FlodeRatingCurve] objects with non-overlapping
#'   validity periods.
#' @param station_id Character. Station identifier shared across all curves.
#'
#' @export
FlodeRatingSet <- S7::new_class(
  "FlodeRatingSet",
  package    = "reach.io",
  properties = list(
    curves     = S7::new_property(class = S7::class_list),
    station_id = S7::new_property(class = S7::class_character)
  ),
  constructor = function(curves, station_id = NA_character_) {
    if (S7::S7_inherits(curves, FlodeRatingCurve)) curves <- list(curves)
    S7::new_object(S7::S7_object(),
                   curves     = curves,
                   station_id = station_id)
  },
  validator = function(self) {
    if (length(self@curves) == 0L) {
      return("`curves` must contain at least one FlodeRatingCurve.")
    }
    for (i in seq_along(self@curves)) {
      if (!S7::S7_inherits(self@curves[[i]], FlodeRatingCurve)) {
        return(sprintf("Element %d of `curves` is not a FlodeRatingCurve.", i))
      }
    }

    # -- Validity windows ----------------------------------------------------
    # Pairwise check that windows [valid_from, valid_to] do not overlap. Both
    # ends are inclusive and an NA valid_to is open-ended (Inf). Pairs in which
    # either curve lacks a valid_from are skipped; non-Date values count as NA.
    froms <- vapply(self@curves, function(cv) {
      if (inherits(cv@valid_from, "Date")) as.numeric(cv@valid_from) else NA_real_
    }, numeric(1L))
    tos   <- vapply(self@curves, function(cv) {
      if (inherits(cv@valid_to, "Date")) as.numeric(cv@valid_to) else NA_real_
    }, numeric(1L))

    n <- length(self@curves)
    for (i in seq_len(n - 1L)) {
      for (j in seq(i + 1L, n)) {
        fi <- froms[i]; ti <- if (is.na(tos[i])) Inf else tos[i]
        fj <- froms[j]; tj <- if (is.na(tos[j])) Inf else tos[j]
        if (!is.na(fi) && !is.na(fj)) {
          if (fi <= tj && fj <= ti) {
            return(sprintf(
              "FlodeRatingCurve %d and %d have overlapping validity periods.", i, j
            ))
          }
        }
      }
    }

    NULL
  }
)


# =============================================================================
# apply_rating()
# =============================================================================

#' Convert stage readings to rated discharge using a rating curve
#'
#' Applies the stage-discharge rating equation \eqn{Q = C(h - a)^b} to each
#' observation in a [FlodeLevel_Daily] or [FlodeLevel_15min] object and returns a
#' [FlodeFlow_Daily] or [FlodeFlow_15min] object of the same period.
#'
#' When `rating` is a [FlodeRatingSet], each observation is matched to whichever
#' [FlodeRatingCurve] has a validity window that spans that observation's date.
#' Observations that fall outside all validity windows are set to `NA`.
#'
#' The output `readings` data.table carries an extra `doubtful` column
#' (`TRUE` where the applicable rating limb has its doubtful flag set).
#'
#' @details
#' Each stage is rated as follows:
#'
#' - An `NA` stage stays `NA`.
#' - A stage below the lowest limb's `lower` has no rating and gives `NA`, so a
#'   negative stage on a curve whose first limb starts at 0 is `NA`, not zero
#'   flow.
#' - Otherwise the limb with the greatest `lower` not above the stage is used.
#'   A stage exactly on a junction therefore uses the upper limb, and stages
#'   above the last limb's `upper` are extrapolated with the last limb.
#' - A stage at or below the limb's datum offset `a` gives `Q = 0`, not `NA`.
#'
#' With a single [FlodeRatingCurve], `valid_from` / `valid_to` are **not**
#' consulted: the curve is applied to every row. Validity windows take effect
#' only through a [FlodeRatingSet], where each row is matched on its `date`
#' column (calendar date, both ends inclusive, `valid_to = NA` open-ended; the
#' time of day plays no part). A curve whose `valid_from` is `NA` never matches
#' inside a set.
#'
#' Counts of rows set to `NA` are reported as warnings, not errors: one for
#' stages below the lowest limb and, for a set, one for rows outside every
#' validity window. The input object is not modified. The output keeps the
#' input columns (such as `quality`), replaces `value` with discharge, sets
#' `measure_notation` to the single label given (so the result has one measure
#' whatever the input had), and adds `doubtful`, which is `NA` where no limb
#' applied. `from_date` and `to_date` are copied from `level`. See also
#' [apply_inverse_rating()] for the reverse conversion and
#' [check_limb_continuity()] to inspect a curve before using it.
#'
#' @param level A [FlodeLevel_Daily] or [FlodeLevel_15min] object.
#' @param rating A [FlodeRatingCurve] or [FlodeRatingSet].
#' @param measure_notation Character scalar placed in the `measure_notation`
#'   column of the output. Defaults to `"rated_flow"`.
#'
#' @return A [FlodeFlow_Daily] or [FlodeFlow_15min] object matching the timestep of
#'   `level`. Rated flows are in m³/s (consistent with the rating equation).
#'   Stages below the lowest limb boundary, or outside the validity window of
#'   any curve in a [FlodeRatingSet], are returned as `NA`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' limbs <- data.table::data.table(
#'   lower    = c(0.00, 0.50),
#'   upper    = c(0.50,  Inf),
#'   C        = c(12.25, 23.25),
#'   a        = c(-0.003, -0.098),
#'   b        = c(1.641, 2.706),
#'   doubtful = c(FALSE, TRUE)
#' )
#' rc    <- FlodeRatingCurve(limbs, station_id = "510310", source = "WISKI")
#' flow  <- apply_rating(level_obj, rc)
#' }
#' @export

apply_rating <- function(level, rating, measure_notation = "rated_flow") {
  if (S7::S7_inherits(rating, FlodeRatingCurve)) {
    .apply_rating_curve(level, rating, measure_notation)
  } else if (S7::S7_inherits(rating, FlodeRatingSet)) {
    .apply_rating_set(level, rating, measure_notation)
  } else {
    stop("`rating` must be a FlodeRatingCurve or FlodeRatingSet.")
  }
}

#' @noRd
.apply_rating_curve <- function(level, rating, measure_notation = "rated_flow") {
  if (!S7::S7_inherits(level, FlodeLevel_Daily) &&
      !S7::S7_inherits(level, FlodeLevel_15min)) {
    stop("`level` must be a FlodeLevel_Daily or FlodeLevel_15min object.")
  }

  # -- Rate stage ------------------------------------------------------------
  # Work on a copy: := below must not alter the Level object's own table
  # (as_data_table() hands out the stored table by reference).
  dt    <- data.table::copy(level@readings)
  stage <- dt$value

  rated <- .rate_stage(stage, rating@limbs)

  # A non-NA stage with NA discharge can only be one below the lowest limb's
  # lower bound, the single case .rate_stage() leaves unrated.
  n_below <- sum(is.na(rated$value) & !is.na(stage))
  if (n_below > 0L) {
    warning(sprintf(
      "%d observation(s) had stage below the lowest limb boundary (%.4g m) and were rated as NA.",
      n_below, rating@limbs$lower[1L]
    ))
  }

  # -- Build output ----------------------------------------------------------
  # The result is a Flow object of the same period; its constructor recomputes
  # n_rows and n_measures (now a single measure_notation).
  dt[, value            := rated$value]
  dt[, doubtful         := rated$doubtful]
  dt[, measure_notation := measure_notation]

  if (S7::S7_inherits(level, FlodeLevel_Daily)) {
    FlodeFlow_Daily(readings  = dt,
               from_date = level@from_date,
               to_date   = level@to_date)
  } else {
    FlodeFlow_15min(readings  = dt,
               from_date = level@from_date,
               to_date   = level@to_date)
  }
}

#' @noRd
.apply_rating_set <- function(level, rating, measure_notation = "rated_flow") {
  if (!S7::S7_inherits(level, FlodeLevel_Daily) &&
      !S7::S7_inherits(level, FlodeLevel_15min)) {
    stop("`level` must be a FlodeLevel_Daily or FlodeLevel_15min object.")
  }

  # -- Match rows to curves --------------------------------------------------
  # Copy for the same reason as in .apply_rating_curve().
  dt    <- data.table::copy(level@readings)
  stage <- dt$value
  n     <- nrow(dt)

  q_out  <- rep(NA_real_, n)
  dbt_out <- rep(NA, n)

  # Per row, the index of the curve whose window contains the row's `date`
  # (NA if none).
  curve_idx <- .find_valid_curve(dt$date, rating)

  # Rows with no matching curve stay NA. They are counted here, separately from
  # rows below a curve's limb range (counted further down).
  n_unmatched <- sum(is.na(curve_idx))
  if (n_unmatched > 0L) {
    warning(sprintf(
      "%d observation(s) fall outside all FlodeRatingCurve validity windows and were rated as NA.",
      n_unmatched
    ))
  }

  # -- Rate per curve --------------------------------------------------------
  # One vectorised .rate_stage() call per curve, on only that curve's rows.
  for (i in seq_along(rating@curves)) {
    rows <- which(curve_idx == i)
    if (length(rows) == 0L) next

    rated          <- .rate_stage(stage[rows], rating@curves[[i]]@limbs)
    q_out[rows]    <- rated$value
    dbt_out[rows]  <- rated$doubtful
  }

  # Excludes unmatched rows (curve_idx NA) so the two warnings do not double
  # count.
  n_below <- sum(is.na(q_out) & !is.na(stage) & !is.na(curve_idx))
  if (n_below > 0L) {
    warning(sprintf(
      "%d observation(s) had stage below the lowest limb boundary of their applicable curve and were rated as NA.",
      n_below
    ))
  }

  # -- Build output ----------------------------------------------------------
  dt[, value            := q_out]
  dt[, doubtful         := dbt_out]
  dt[, measure_notation := measure_notation]

  if (S7::S7_inherits(level, FlodeLevel_Daily)) {
    FlodeFlow_Daily(readings  = dt,
               from_date = level@from_date,
               to_date   = level@to_date)
  } else {
    FlodeFlow_15min(readings  = dt,
               from_date = level@from_date,
               to_date   = level@to_date)
  }
}


# =============================================================================
# check_limb_continuity()
# =============================================================================

#' Check that rating limbs connect at their shared boundaries
#'
#' At each junction between limb \eqn{i} and limb \eqn{i+1}, evaluates the
#' rating equation from both limbs at the shared boundary stage and reports
#' the absolute and relative Q difference. A junction is considered connected
#' when the relative gap is within `tol_pct`.
#'
#' Use [fix_limb_continuity()] to correct any discontinuities by adjusting
#' the `C` parameters.
#'
#' @details
#' Each junction is evaluated with the two limbs' own `C`, `a` and `b` at the
#' shared stage `upper[i]`, with `(h - a)` floored at zero. `gap_m3s` keeps its
#' sign (upper minus lower limb), but `gap_pct` is unsigned and is taken
#' relative to the lower limb's discharge. When that discharge is zero (the
#' junction is at or below the lower limb's `a`) the percentage is undefined, so
#' `gap_pct` is `NA` and the junction counts as **not** connected, even if both
#' limbs give zero.
#'
#' A curve with one limb has no junctions: a message is printed and an empty
#' table returned. Otherwise a message says how many junctions exceed
#' `tol_pct`; when any do, the failing rows are also printed to the console.
#' The full table is returned invisibly in either case. The curve is not
#' modified; see [fix_limb_continuity()] to correct it.
#'
#' @param x A [FlodeRatingCurve].
#' @param ... Arguments passed to the method. For a [FlodeRatingCurve]:
#'   `tol_pct`, a numeric scalar giving the percentage tolerance for declaring
#'   a junction connected (default `1`, i.e. 1 per cent).
#'
#' @return A `data.table` (returned invisibly) with one row per junction and
#'   columns:
#'   \describe{
#'     \item{`junction`}{Label, e.g. `"1-2"`.}
#'     \item{`h_junction`}{Stage at the boundary (m).}
#'     \item{`Q_lower_limb`}{Q from the lower limb at the boundary (m³/s).}
#'     \item{`Q_upper_limb`}{Q from the upper limb at the boundary (m³/s).}
#'     \item{`gap_m3s`}{Signed difference `Q_upper - Q_lower` (m³/s).}
#'     \item{`gap_pct`}{Relative gap as a percentage of `Q_lower_limb`.}
#'     \item{`connected`}{Logical. `TRUE` when `abs(gap_pct)` <= `tol_pct`.}
#'   }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' check_limb_continuity(rc)
#' check_limb_continuity(rc, tol_pct = 0.5)
#' }
check_limb_continuity <- S7::new_generic("check_limb_continuity", "x")

S7::method(check_limb_continuity, FlodeRatingCurve) <- function(x, tol_pct = 1) {
  limbs <- x@limbs
  n     <- nrow(limbs)

  if (n < 2L) {
    message("Only one limb - no junctions to check.")
    return(invisible(data.table::data.table()))
  }

  # One row per junction (limb i / i + 1); .junction_Q() evaluates both sides.
  rows <- lapply(seq_len(n - 1L), function(i) {
    jq      <- .junction_Q(limbs, i)
    gap     <- jq$Q_upper - jq$Q_lower
    # Unsigned and relative to the LOWER limb's Q. Undefined when that Q is 0;
    # the NA then fails the `connected` test below, so it counts as not connected.
    gap_pct <- if (jq$Q_lower > 0) abs(gap) / jq$Q_lower * 100 else NA_real_

    data.table::data.table(
      junction     = paste0(i, "-", i + 1L),
      h_junction   = jq$h,
      Q_lower_limb = jq$Q_lower,
      Q_upper_limb = jq$Q_upper,
      gap_m3s      = gap,
      gap_pct      = gap_pct,
      connected    = !is.na(gap_pct) && gap_pct <= tol_pct
    )
  })

  out <- data.table::rbindlist(rows)

  # Report: always a message; failing junctions are also printed. The full
  # table is returned invisibly.
  n_bad <- sum(!out$connected)
  if (n_bad == 0L) {
    message(sprintf("All %d junction(s) connect within %.4g%%.", nrow(out), tol_pct))
  } else {
    message(sprintf(
      "%d of %d junction(s) exceed the %.4g%% continuity tolerance.",
      n_bad, nrow(out), tol_pct
    ))
    print(out[(!connected)][, .(junction, h_junction,
                                Q_lower_limb, Q_upper_limb,
                                gap_m3s, gap_pct)])
  }

  invisible(out)
}


# =============================================================================
# fix_limb_continuity()
# =============================================================================

#' Adjust rating limb C parameters to enforce continuity at junctions
#'
#' For each junction between limb \eqn{i} and limb \eqn{i+1}, computes the
#' discharge produced by the lower limb at the boundary stage and sets
#' \eqn{C_{i+1}} of the upper limb so that it produces the same discharge at
#' that stage. The shape of each limb (parameters \eqn{a} and \eqn{b}) is
#' preserved; only \eqn{C} is rescaled.
#'
#' Adjustment propagates upward from limb 1 (the anchor), so a change to
#' \eqn{C_2} feeds into the computation of \eqn{C_3}, and so on. The anchor
#' limb's \eqn{C} is never modified.
#'
#' @details
#' Junctions are processed from the lowest upward and each is forced to match
#' exactly; there is no tolerance, so a junction that already agreed within
#' [check_limb_continuity()]'s tolerance still has its `C` nudged (usually by a
#' tiny percentage). Each new `C` is computed from the limb below as already
#' corrected, so a change at one junction carries through to all later limbs.
#' Only `C` of limbs 2 and up is rescaled, so every rated flow within such a
#' limb moves by the same factor as its `C`; flows in limb 1 are unchanged.
#' `lower`, `upper`, `a`, `b` and `doubtful` are untouched.
#'
#' A junction is skipped with a warning (the limb keeps its `C`, and later
#' junctions continue from that unchanged value) when the junction stage is at
#' or below the upper limb's `a` (that limb gives zero flow whatever `C` is, so
#' `C` cannot be solved) or when the lower limb gives zero or negative flow
#' there (the new `C` would be zero, which a [FlodeRatingCurve] forbids).
#'
#' The original object is not changed; the result keeps the validity dates,
#' `station_id` and `source`, and is re-validated by the constructor. A
#' single-limb curve is returned as is, with a message. Nothing is written to
#' the store: use [save_rating()] to keep the corrected curve.
#'
#' @param x A [FlodeRatingCurve].
#' @param ... Additional arguments for methods. The [FlodeRatingCurve] method
#'   takes none.
#'
#' @return A new [FlodeRatingCurve] with updated `C` values. A message is printed
#'   for each limb showing the old and new `C` and the percentage change.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' check_limb_continuity(rc)      # inspect before
#' rc_fixed <- fix_limb_continuity(rc)
#' check_limb_continuity(rc_fixed) # verify after
#' }
fix_limb_continuity <- S7::new_generic("fix_limb_continuity", "x")

S7::method(fix_limb_continuity, FlodeRatingCurve) <- function(x) {
  limbs <- data.table::copy(x@limbs)
  n     <- nrow(limbs)

  if (n < 2L) {
    message("Only one limb - nothing to fix.")
    return(x)
  }

  message("Adjusting C parameters (anchor = limb 1, propagating upward):")

  # Walk the junctions from the bottom. `limbs` is updated inside the loop, so
  # .junction_Q() at junction i sees limb i's C as already corrected at the
  # previous step - this is how a change propagates upward from the anchor.
  for (i in seq_len(n - 1L)) {
    jq    <- .junction_Q(limbs, i)   # uses current (possibly already adjusted) C[i]
    # Limb i + 1 must give Q_lower at the junction stage h:
    #   C_new * (h - a)^b = Q_lower   =>   C_new = Q_lower / (h - a)^b
    # denom is that power term.
    denom <- max(jq$h - limbs$a[i + 1L], 0)^limbs$b[i + 1L]

    # denom is 0 when h is at or below the upper limb's datum offset a: the limb
    # then gives Q = 0 whatever C is, so C cannot be solved. A skipped limb
    # keeps its C and the loop carries on from that unchanged value.
    if (denom <= 0 || !is.finite(denom)) {
      warning(sprintf(
        "  Limb %d: junction stage (%.4g m) is at or below datum offset a = %.4g. ",
        i + 1L, jq$h, limbs$a[i + 1L]
      ), "Cannot compute new C - skipping this junction.", call. = FALSE)
      next
    }

    # A zero (or negative) lower-limb Q would force C_new to 0, which
    # FlodeRatingCurve rejects (C must be > 0).
    if (jq$Q_lower <= 0) {
      warning(sprintf(
        "  Limb %d -> %d: Q at junction is zero or negative (%.4g m\u00b3/s). ",
        i, i + 1L, jq$Q_lower
      ), "Skipping - check datum offsets.", call. = FALSE)
      next
    }

    C_old <- limbs$C[i + 1L]
    C_new <- jq$Q_lower / denom
    limbs$C[i + 1L] <- C_new

    pct_change <- (C_new - C_old) / C_old * 100
    message(sprintf(
      "  Limb %d -> %d  h = %.4g m:  C  %.6g  ->  %.6g  (%+.3f%%)",
      i, i + 1L, jq$h, C_old, C_new, pct_change
    ))
  }

  # Rebuilt through the constructor so the result is re-validated; the validity
  # window, station and source are carried over unchanged.
  FlodeRatingCurve(
    limbs      = limbs,
    valid_from = x@valid_from,
    valid_to   = x@valid_to,
    station_id = x@station_id,
    source     = x@source
  )
}


# =============================================================================
# Print methods
# =============================================================================

# Registered as the S3 print method for "reach.io::FlodeRatingCurve" in
# .onLoad() (package.R): S7's print.S7_object calls str() directly and ignores
# S7::method(print, ...) assignments.
.print_RatingCurve <- function(x, ...) {
  vf <- if (inherits(x@valid_from, "Date")) format(x@valid_from) else "-"
  vt <- if (inherits(x@valid_to,   "Date")) format(x@valid_to)   else "open"

  cat(sprintf(
    "<FlodeRatingCurve>\n  Station:  %s\n  Source:   %s\n  Valid:    %s to %s\n  Limbs:    %d\n\n",
    if (is.na(x@station_id)) "-" else x@station_id,
    if (is.na(x@source))     "-" else x@source,
    vf, vt,
    nrow(x@limbs)
  ))

  # Pretty-print limbs table
  # Format a COPY: the := calls below turn columns into text and must not
  # touch x@limbs.
  lt <- data.table::copy(x@limbs)
  lt[, upper := ifelse(is.infinite(upper), "Inf", sprintf("%.4g", upper))]
  lt[, lower := sprintf("%.4g", lower)]
  lt[, C     := sprintf("%.6g", C)]
  lt[, a     := sprintf("%.6g", a)]
  lt[, b     := sprintf("%.6g", b)]

  header <- sprintf("  %3s  %7s  %7s  %9s  %9s  %9s  %s",
                    "#", "Lower", "Upper", "C", "a", "b", "Doubtful")
  cat(header, "\n")
  cat(strrep("-", nchar(header)), "\n")
  for (i in seq_len(nrow(lt))) {
    cat(sprintf("  %3d  %7s  %7s  %9s  %9s  %9s  %s\n",
                i,
                lt$lower[i], lt$upper[i],
                lt$C[i], lt$a[i], lt$b[i],
                lt$doubtful[i]))
  }
  invisible(x)
}

# =============================================================================
# apply_inverse_rating()
# =============================================================================

# Vectorised inverse rating: given a numeric vector of flow values q and a
# limbs data.table, returns a numeric vector of stage values h.
#
#   - NA or negative q         -> NA
#   - q within a limb          -> h = (q / C)^(1/b) + a
#   - q in a discontinuity gap -> junction stage (upper boundary of lower limb)
#   - q above highest limb     -> extrapolate using the last limb
#' @noRd
.invert_stage <- function(q, limbs) {
  nl <- nrow(limbs)
  n  <- length(q)
  h  <- rep(NA_real_, n)

  # Q at the lower and upper boundary of each limb, evaluated from that limb's
  # own equation.  Upper boundary of the last limb is Inf.
  # (With a discontinuous curve q_upper[i - 1] differs from q_lower[i]; the
  # difference is the "gap" handled below.)
  q_lower <- pmax(limbs$C * pmax(limbs$lower - limbs$a, 0)^limbs$b, 0)
  q_upper <- ifelse(
    is.finite(limbs$upper),
    limbs$C * pmax(limbs$upper - limbs$a, 0)^limbs$b,
    Inf
  )
  q_lower[is.nan(q_lower)] <- 0
  q_upper[is.nan(q_upper)] <- 0

  # NA and negative flows give NA; zero flow is allowed.
  valid <- !is.na(q) & q >= 0

  for (idx in which(valid)) {
    qi <- q[idx]

    # First limb whose upper-boundary Q >= qi
    # (If the curve steps down at a junction, limbs overlap in Q and the lower
    # limb wins here.)
    i <- which(q_upper >= qi)[1L]
    if (is.na(i)) i <- nl  # above all limbs: extrapolate using last limb

    # No gap test for limb 1: a flow below the discharge limb 1 gives at its
    # own `lower` is inverted by extending limb 1 below that stage, whereas the
    # forward rating returns NA for stages there.
    if (i > 1L && qi < q_lower[i]) {
      # Falls in a discontinuity gap between limbs i-1 and i
      h[idx] <- limbs$upper[i - 1L]
    } else if (qi <= 0) {
      # Zero flow maps to the lowest limb bound, not to the datum a.
      h[idx] <- limbs$lower[1L]
    } else {
      h[idx] <- (qi / limbs$C[i])^(1 / limbs$b[i]) + limbs$a[i]
    }
  }

  h
}

#' @noRd
.apply_inverse_rating_curve <- function(flow, rating,
                                        measure_notation = "rated_level") {
  if (!S7::S7_inherits(flow, FlodeFlow_Daily) &&
      !S7::S7_inherits(flow, FlodeFlow_15min)) {
    stop("`flow` must be a FlodeFlow_Daily or FlodeFlow_15min object.")
  }

  # Copy: := below must not alter the Flow object's own table.
  dt <- data.table::copy(flow@readings)

  h <- .invert_stage(dt$value, rating@limbs)

  dt[, value            := h]
  dt[, measure_notation := measure_notation]
  # Stage has no doubtful flag, so the column from apply_rating() is dropped.
  if ("doubtful" %in% names(dt)) dt[, doubtful := NULL]

  if (S7::S7_inherits(flow, FlodeFlow_Daily)) {
    FlodeLevel_Daily(readings  = dt,
                from_date = flow@from_date,
                to_date   = flow@to_date)
  } else {
    FlodeLevel_15min(readings  = dt,
                from_date = flow@from_date,
                to_date   = flow@to_date)
  }
}

#' @noRd
.apply_inverse_rating_set <- function(flow, rating,
                                      measure_notation = "rated_level") {
  if (!S7::S7_inherits(flow, FlodeFlow_Daily) &&
      !S7::S7_inherits(flow, FlodeFlow_15min)) {
    stop("`flow` must be a FlodeFlow_Daily or FlodeFlow_15min object.")
  }

  # -- Match rows to curves --------------------------------------------------
  # Copy: := below must not alter the Flow object's own table.
  dt  <- data.table::copy(flow@readings)
  q   <- dt$value
  n   <- nrow(dt)
  h   <- rep(NA_real_, n)

  curve_idx <- .find_valid_curve(dt$date, rating)

  n_unmatched <- sum(is.na(curve_idx))
  if (n_unmatched > 0L) {
    warning(sprintf(
      "%d observation(s) fall outside all FlodeRatingCurve validity windows and were inverted as NA.",
      n_unmatched
    ))
  }

  # -- Invert per curve ------------------------------------------------------
  for (i in seq_along(rating@curves)) {
    rows <- which(curve_idx == i)
    if (length(rows) == 0L) next
    h[rows] <- .invert_stage(q[rows], rating@curves[[i]]@limbs)
  }

  # -- Build output ----------------------------------------------------------
  # The doubtful column from apply_rating() is dropped: stage has no such flag.
  dt[, value            := h]
  dt[, measure_notation := measure_notation]
  if ("doubtful" %in% names(dt)) dt[, doubtful := NULL]

  if (S7::S7_inherits(flow, FlodeFlow_Daily)) {
    FlodeLevel_Daily(readings  = dt,
                from_date = flow@from_date,
                to_date   = flow@to_date)
  } else {
    FlodeLevel_15min(readings  = dt,
                from_date = flow@from_date,
                to_date   = flow@to_date)
  }
}

#' Convert rated discharge back to stage using an inverse rating curve
#'
#' Applies the inverse of the stage-discharge equation
#' \eqn{Q = C(h - a)^b}, solving for \eqn{h}:
#'
#' \deqn{h = \left(\frac{Q}{C}\right)^{1/b} + a}
#'
#' to each observation in a [FlodeFlow_Daily] or [FlodeFlow_15min] object and returns a
#' [FlodeLevel_Daily] or [FlodeLevel_15min] object of the same period.
#'
#' When `rating` is a [FlodeRatingSet], each observation is matched to the
#' applicable [FlodeRatingCurve] by date, exactly as [apply_rating()] does for the
#' forward direction.
#'
#' **Discontinuity gaps:** Where consecutive limbs do not meet at their shared
#' boundary (i.e. `check_limb_continuity()` reports a non-zero gap), a flow
#' value that falls inside the gap is mapped to the junction stage
#' (the upper boundary of the lower limb). Use [fix_limb_continuity()] to
#' remove gaps before inverting if this behaviour is undesirable.
#'
#' @details
#' Each flow is inverted as follows:
#'
#' - An `NA` or negative flow gives `NA`. A zero flow gives the first limb's
#'   `lower`, not the datum `a`.
#' - A positive flow is assigned to the first limb whose discharge at its upper
#'   boundary (from that limb's own equation) reaches the flow, then inverted
#'   with the equation above. If limbs overlap in discharge because the curve
#'   steps down at a junction, the lower limb wins. Flows above the last limb
#'   are extrapolated with the last limb.
#' - A flow inside a gap between limbs gives the junction stage (see above).
#' - A flow below the discharge the first limb gives at its own `lower` is
#'   inverted by extending the first limb below that stage, whereas
#'   [apply_rating()] returns `NA` for such stages, so a round trip is not exact
#'   there.
#'
#' As in [apply_rating()], a single [FlodeRatingCurve] is applied to every row
#' regardless of its validity dates, while a [FlodeRatingSet] matches each row
#' on its `date` column and sets rows outside every window to `NA` with a
#' warning. No other warnings are issued. The `doubtful` column of the flow
#' object, if present, is dropped because stage has no such flag, and
#' `measure_notation` is replaced by the single label given. The input is not
#' modified.
#'
#' @param flow A [FlodeFlow_Daily] or [FlodeFlow_15min] object.
#' @param rating A [FlodeRatingCurve] or [FlodeRatingSet].
#' @param measure_notation Character scalar placed in the `measure_notation`
#'   column of the output. Defaults to `"rated_level"`.
#'
#' @return A [FlodeLevel_Daily] or [FlodeLevel_15min] object matching the timestep of
#'   `flow`. Stage values are in metres. Flow values of `NA` or below zero
#'   are returned as `NA`.
#'
#' @seealso [apply_rating()] for the forward direction;
#'   [check_limb_continuity()] to inspect gaps before inverting.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' limbs <- data.table::data.table(
#'   lower    = c(0.00, 0.50),
#'   upper    = c(0.50,  Inf),
#'   C        = c(12.25, 23.25),
#'   a        = c(-0.003, -0.098),
#'   b        = c(1.641, 2.706),
#'   doubtful = c(FALSE, TRUE)
#' )
#' rc    <- FlodeRatingCurve(limbs, station_id = "510310", source = "WISKI")
#' flow  <- apply_rating(level_obj, rc)
#' level <- apply_inverse_rating(flow, rc)
#' }
apply_inverse_rating <- function(flow, rating,
                                 measure_notation = "rated_level") {
  if (S7::S7_inherits(rating, FlodeRatingCurve)) {
    .apply_inverse_rating_curve(flow, rating, measure_notation)
  } else if (S7::S7_inherits(rating, FlodeRatingSet)) {
    .apply_inverse_rating_set(flow, rating, measure_notation)
  } else {
    stop("`rating` must be a FlodeRatingCurve or FlodeRatingSet.")
  }
}


# =============================================================================
# Print methods
# =============================================================================

# Registered as the S3 print method for "reach.io::FlodeRatingSet" in .onLoad()
# (package.R), for the same reason as .print_RatingCurve().
.print_RatingSet <- function(x, ...) {
  fmt_date <- function(d) if (inherits(d, "Date")) format(d) else "-"

  cat(sprintf(
    "<FlodeRatingSet>\n  Station:  %s\n  Curves:   %d\n\n",
    if (is.na(x@station_id)) "-" else x@station_id,
    length(x@curves)
  ))

  header <- sprintf("  %3s  %12s  %12s  %6s  %s",
                    "#", "Valid from", "Valid to", "Limbs", "Source")
  cat(header, "\n")
  cat(strrep("-", nchar(header)), "\n")
  for (i in seq_along(x@curves)) {
    cv <- x@curves[[i]]
    cat(sprintf("  %3d  %12s  %12s  %6d  %s\n",
                i,
                fmt_date(cv@valid_from),
                fmt_date(cv@valid_to),
                nrow(cv@limbs),
                if (is.na(cv@source)) "-" else cv@source))
  }
  invisible(x)
}
