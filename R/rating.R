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
# Inputs:      data.table of rating limbs; Level_Daily or
#              Level_15min S7 objects
# Outputs:     RatingCurve / RatingSet S7 objects;
#              Flow_Daily / Flow_15min rated output
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
  idx    <- findInterval(h, limbs$lower)   # 0 = below all limbs
  n      <- length(h)
  q      <- rep(NA_real_, n)
  dbt    <- rep(NA, n)

  valid  <- !is.na(h) & idx >= 1L
  if (any(valid)) {
    li         <- idx[valid]
    hv         <- h[valid]
    q[valid]   <- pmax(0,
                       limbs$C[li] * pmax(hv - limbs$a[li], 0)^limbs$b[li])
    q[valid][is.nan(q[valid])] <- 0
    dbt[valid] <- limbs$doubtful[li]
  }

  list(value = q, doubtful = dbt)
}

# For each Date in `dates`, return the integer index of the applicable
# RatingCurve in rating_set@curves (NA_integer_ if none applies).
#' @noRd
.find_valid_curve <- function(dates, rating_set) {
  curves <- rating_set@curves
  n_obs  <- length(dates)
  idx    <- rep(NA_integer_, n_obs)

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
# RatingCurve
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
#' @slot limbs A `data.table` with one row per rating limb and columns:
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
#' @slot valid_from Date (or `NA`). Start of the period this curve applies.
#' @slot valid_to Date (or `NA`). End of the period (`NA` = open-ended /
#'   still current).
#' @slot station_id Character. Station identifier (free-text, for labelling).
#' @slot source Character. Origin of the curve (e.g. `"WISKI"`, `"manual"`).
#'
#' @export
RatingCurve <- S7::new_class(
  "RatingCurve",
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
    dt <- data.table::as.data.table(limbs)
    dt <- .normalise_limb_names(dt)

    # Coerce types where safely possible
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

    if (any(dt$lower >= dt$upper, na.rm = TRUE)) {
      return("Each limb's `lower` must be strictly less than its `upper`.")
    }

    if (nrow(dt) > 1L) {
      if (!all(dt$lower == sort(dt$lower))) {
        return("`limbs` must be sorted by `lower` in ascending order.")
      }
      gaps <- abs(dt$upper[-nrow(dt)] - dt$lower[-1L])
      if (any(gaps > 1e-9, na.rm = TRUE)) {
        bad <- which(gaps > 1e-9)[1L]
        return(sprintf(
          "Limb boundaries are not contiguous: `upper[%d]` (%.6g) != `lower[%d]` (%.6g). ",
          bad, dt$upper[bad], bad + 1L, dt$lower[bad + 1L]
        ))
      }
    }

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
# RatingSet
# =============================================================================

#' S7 class for a time-varying collection of rating curves
#'
#' Groups multiple [RatingCurve] objects that each apply over a distinct
#' calendar period — for instance to account for channel changes, re-surveys,
#' or shift corrections. [apply_rating()] uses `valid_from` / `valid_to` on
#' each curve to select the correct rating for every observation.
#'
#' @slot curves A `list` of [RatingCurve] objects with non-overlapping
#'   validity periods.
#' @slot station_id Character. Station identifier shared across all curves.
#'
#' @export
RatingSet <- S7::new_class(
  "RatingSet",
  package    = "reach.io",
  properties = list(
    curves     = S7::new_property(class = S7::class_list),
    station_id = S7::new_property(class = S7::class_character)
  ),
  constructor = function(curves, station_id = NA_character_) {
    if (S7::S7_inherits(curves, RatingCurve)) curves <- list(curves)
    S7::new_object(S7::S7_object(),
                   curves     = curves,
                   station_id = station_id)
  },
  validator = function(self) {
    if (length(self@curves) == 0L) {
      return("`curves` must contain at least one RatingCurve.")
    }
    for (i in seq_along(self@curves)) {
      if (!S7::S7_inherits(self@curves[[i]], RatingCurve)) {
        return(sprintf("Element %d of `curves` is not a RatingCurve.", i))
      }
    }

    # Check validity periods are non-overlapping (where both bounds are known)
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
              "RatingCurve %d and %d have overlapping validity periods.", i, j
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
#' observation in a [Level_Daily] or [Level_15min] object and returns a
#' [Flow_Daily] or [Flow_15min] object of the same period.
#'
#' When `rating` is a [RatingSet], each observation is matched to whichever
#' [RatingCurve] has a validity window that spans that observation's date.
#' Observations that fall outside all validity windows are set to `NA`.
#'
#' The output `readings` data.table carries an extra `doubtful` column
#' (`TRUE` where the applicable rating limb has its doubtful flag set).
#'
#' @param level A [Level_Daily] or [Level_15min] object.
#' @param rating A [RatingCurve] or [RatingSet].
#' @param measure_notation Character scalar placed in the `measure_notation`
#'   column of the output. Defaults to `"rated_flow"`.
#'
#' @return A [Flow_Daily] or [Flow_15min] object matching the timestep of
#'   `level`. Rated flows are in m³/s (consistent with the rating equation).
#'   Stages below the lowest limb boundary, or outside the validity window of
#'   any curve in a [RatingSet], are returned as `NA`.
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
#' rc    <- RatingCurve(limbs, station_id = "510310", source = "WISKI")
#' flow  <- apply_rating(level_obj, rc)
#' }
#' @export

apply_rating <- function(level, rating, measure_notation = "rated_flow") {
  if (S7::S7_inherits(rating, RatingCurve)) {
    .apply_rating_curve(level, rating, measure_notation)
  } else if (S7::S7_inherits(rating, RatingSet)) {
    .apply_rating_set(level, rating, measure_notation)
  } else {
    stop("`rating` must be a RatingCurve or RatingSet.")
  }
}

#' @noRd
.apply_rating_curve <- function(level, rating, measure_notation = "rated_flow") {
  if (!S7::S7_inherits(level, Level_Daily) &&
      !S7::S7_inherits(level, Level_15min)) {
    stop("`level` must be a Level_Daily or Level_15min object.")
  }

  dt    <- data.table::copy(level@readings)
  stage <- dt$value

  rated <- .rate_stage(stage, rating@limbs)

  n_below <- sum(is.na(rated$value) & !is.na(stage))
  if (n_below > 0L) {
    warning(sprintf(
      "%d observation(s) had stage below the lowest limb boundary (%.4g m) and were rated as NA.",
      n_below, rating@limbs$lower[1L]
    ))
  }

  dt[, value            := rated$value]
  dt[, doubtful         := rated$doubtful]
  dt[, measure_notation := measure_notation]

  if (S7::S7_inherits(level, Level_Daily)) {
    Flow_Daily(readings  = dt,
               from_date = level@from_date,
               to_date   = level@to_date)
  } else {
    stop("`rating` must be a RatingCurve or RatingSet.")
  }
}

#' @noRd
.apply_rating_set <- function(level, rating, measure_notation = "rated_flow") {
  if (!S7::S7_inherits(level, Level_Daily) &&
      !S7::S7_inherits(level, Level_15min)) {
    stop("`level` must be a Level_Daily or Level_15min object.")
  }

  dt    <- data.table::copy(level@readings)
  stage <- dt$value
  n     <- nrow(dt)

  q_out  <- rep(NA_real_, n)
  dbt_out <- rep(NA, n)

  curve_idx <- .find_valid_curve(dt$date, rating)

  n_unmatched <- sum(is.na(curve_idx))
  if (n_unmatched > 0L) {
    warning(sprintf(
      "%d observation(s) fall outside all RatingCurve validity windows and were rated as NA.",
      n_unmatched
    ))
  }

  for (i in seq_along(rating@curves)) {
    rows <- which(curve_idx == i)
    if (length(rows) == 0L) next

    rated          <- .rate_stage(stage[rows], rating@curves[[i]]@limbs)
    q_out[rows]    <- rated$value
    dbt_out[rows]  <- rated$doubtful
  }

  n_below <- sum(is.na(q_out) & !is.na(stage) & !is.na(curve_idx))
  if (n_below > 0L) {
    warning(sprintf(
      "%d observation(s) had stage below the lowest limb boundary of their applicable curve and were rated as NA.",
      n_below
    ))
  }

  dt[, value            := q_out]
  dt[, doubtful         := dbt_out]
  dt[, measure_notation := measure_notation]

  if (S7::S7_inherits(level, Level_Daily)) {
    Flow_Daily(readings  = dt,
               from_date = level@from_date,
               to_date   = level@to_date)
  } else {
    Flow_15min(readings  = dt,
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
#' @param x A [RatingCurve].
#' @param tol_pct Numeric scalar. Percentage tolerance for declaring a junction
#'   connected. Default `1` (i.e. 1 %).
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

S7::method(check_limb_continuity, RatingCurve) <- function(x, tol_pct = 1) {
  limbs <- x@limbs
  n     <- nrow(limbs)

  if (n < 2L) {
    message("Only one limb — no junctions to check.")
    return(invisible(data.table::data.table()))
  }

  rows <- lapply(seq_len(n - 1L), function(i) {
    jq      <- .junction_Q(limbs, i)
    gap     <- jq$Q_upper - jq$Q_lower
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
#' @param x A [RatingCurve].
#'
#' @return A new [RatingCurve] with updated `C` values. A message is printed
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

S7::method(fix_limb_continuity, RatingCurve) <- function(x) {
  limbs <- data.table::copy(x@limbs)
  n     <- nrow(limbs)

  if (n < 2L) {
    message("Only one limb — nothing to fix.")
    return(x)
  }

  message("Adjusting C parameters (anchor = limb 1, propagating upward):")

  for (i in seq_len(n - 1L)) {
    jq    <- .junction_Q(limbs, i)   # uses current (possibly already adjusted) C[i]
    denom <- max(jq$h - limbs$a[i + 1L], 0)^limbs$b[i + 1L]

    if (denom <= 0 || !is.finite(denom)) {
      warning(sprintf(
        "  Limb %d: junction stage (%.4g m) is at or below datum offset a = %.4g. ",
        i + 1L, jq$h, limbs$a[i + 1L]
      ), "Cannot compute new C — skipping this junction.", call. = FALSE)
      next
    }

    if (jq$Q_lower <= 0) {
      warning(sprintf(
        "  Limb %d -> %d: Q at junction is zero or negative (%.4g m³/s). ",
        i, i + 1L, jq$Q_lower
      ), "Skipping — check datum offsets.", call. = FALSE)
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

  RatingCurve(
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

S7::method(print, RatingCurve) <- function(x, ...) {
  vf <- if (inherits(x@valid_from, "Date")) format(x@valid_from) else "—"
  vt <- if (inherits(x@valid_to,   "Date")) format(x@valid_to)   else "open"

  cat(sprintf(
    "<RatingCurve>\n  Station:  %s\n  Source:   %s\n  Valid:    %s to %s\n  Limbs:    %d\n\n",
    if (is.na(x@station_id)) "—" else x@station_id,
    if (is.na(x@source))     "—" else x@source,
    vf, vt,
    nrow(x@limbs)
  ))

  # Pretty-print limbs table
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

S7::method(print, RatingSet) <- function(x, ...) {
  fmt_date <- function(d) if (inherits(d, "Date")) format(d) else "—"

  cat(sprintf(
    "<RatingSet>\n  Station:  %s\n  Curves:   %d\n\n",
    if (is.na(x@station_id)) "—" else x@station_id,
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
                if (is.na(cv@source)) "—" else cv@source))
  }
  invisible(x)
}
