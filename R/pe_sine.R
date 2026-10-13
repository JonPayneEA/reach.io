# ============================================================
# Tool:        PE Sine Curve Tools
# Description: Fit, generate, regionalise, and compare annual
#              sine curve approximations to potential
#              evapotranspiration (PE) time series.
#              Standard form: PE(t) = B + A * sin(2pi/365.25 * (doy - phi))
#              B   = annual mean (mm/day)
#              A   = amplitude, in mm/day
#              phi = phase offset (days). As coded, PE rises through B at
#                    doy = phi and peaks at doy = phi + 365.25/4 (about 91
#                    days later). Older wording called phi the day of the
#                    peak (~172 for UK); the formula does not behave that way.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-03-25
# Modified:    2026-03-25 - JP: initial implementation
# Tier:        1
# Inputs:      Observed PE numeric vectors with Date vectors
# Outputs:     Fitted parameters; FlodePotEvap_Daily S7 objects; diagnostics
# Dependencies: data.table, ggplot2, stats
# ============================================================


# -- fit_pe_sine --------------------------------------------------------------

#' Fit a sine curve to an observed PE series
#'
#' Estimates the three parameters of the annual PE sine model:
#' \deqn{PE(t) = B + A \sin\!\left(\frac{2\pi}{365.25}(doy - \phi)\right)}
#' where \eqn{B} is the annual mean (mm day\eqn{^{-1}}), \eqn{A} is the
#' amplitude (mm day\eqn{^{-1}}), and \eqn{\phi} is the phase offset: the
#' day of year at which the curve rises through the annual mean \eqn{B}. PE
#' peaks about 91 days later (\eqn{\phi + 365.25/4}), so a UK-like mid-June
#' peak (day of year about 172) corresponds to \eqn{\phi} of roughly 81, not
#' 172.
#'
#' Two fitting methods are available. `"lm_fourier"` (the default) rewrites
#' the model as a linear regression via a Fourier identity, making it
#' numerically stable and guaranteed to converge. `"nls"` fits the model
#' directly using nonlinear least squares; if NLS fails to converge it falls
#' back to `"lm_fourier"` with a warning.
#'
#' The sine approximation is reliable for UK lowland catchments. In upland or
#' coastal sites where PE timing is shifted by elevation or sea surface
#' temperature, inspect `r_squared` before using the fitted curve
#' operationally.
#'
#' @details
#' **Day of year.** `doy` is the day of year (1 to 366) of each date and is the
#' only time variable: all years supplied are pooled into one mean annual
#' cycle. The period is fixed at 365.25 days, so day 366 of a leap year lies
#' just past one full cycle rather than wrapping to day 1.
#'
#' **Meaning of `phi`.** As coded, the curve rises through `B` at
#' `doy = phi` and reaches its maximum about 91 days later, at
#' `phi + 365.25 / 4`. `phi` is therefore the day the curve crosses the annual
#' mean going upward, not the day of peak PE. Parameters from this function are
#' consistent with [generate_pe_sine()] and [compare_pe_sine()], which use the
#' same formula; the date of maximum PE is `phi + 91.3` (wrapped at 365.25).
#'
#' **Methods.** `"lm_fourier"` expands the sine into
#' `B + C1 sin(w doy) + C2 cos(w doy)` (with `w = 2 pi / 365.25`), fits it by
#' ordinary least squares, and recovers `A = sqrt(C1^2 + C2^2)` and
#' `phi = atan2(-C2, C1) / w`, wrapped into `[0, 365.25)`. `A` is a norm, so it
#' is never negative. `"nls"` fits `B`, `A` and `phi` directly, starting from
#' the `"lm_fourier"` solution (the exact least-squares answer for this model,
#' so NLS refines rather than searches), without bounds, so
#' `A` can come back negative (equivalent to shifting `phi` by half a year);
#' `phi` is wrapped but the sign of `A` is not normalised. If NLS fails, a
#' warning is issued and the `"lm_fourier"` result is returned instead; check
#' the `method` element to see which was used.
#'
#' **Fit statistics.** `r_squared` and `rmse` are computed on the fitted sine
#' without the zero floor that [generate_pe_sine()] and [compare_pe_sine()]
#' apply, so they can differ slightly from `compare_pe_sine()` where the curve
#' dips below zero. `r_squared` is `NA` for a constant series.
#'
#' **Data requirements.** At least 14 non-missing observations are required,
#' but that is only a guard on the count: nothing checks that the data span a
#' full year, and a short window leaves `A` and `phi` poorly determined.
#' Observations need not be sorted. Apart from the NLS warning the function
#' has no side effects.
#'
#' @param pe Numeric vector. Daily PE values (mm day\eqn{^{-1}}).
#'   Must not contain NA.
#' @param dates Date vector. Dates corresponding to `pe`. Must be the same
#'   length as `pe` and must not contain NA.
#' @param method Character. Fitting method: `"lm_fourier"` (default, stable)
#'   or `"nls"` (direct nonlinear fit, may not converge for noisy series).
#'
#' @return A named list with elements:
#' \describe{
#'   \item{`B`}{Annual mean PE (mm day\eqn{^{-1}}).}
#'   \item{`A`}{Amplitude (mm day\eqn{^{-1}}).}
#'   \item{`phi`}{Phase offset: day of year at which the curve rises through
#'     `B`. Peak PE falls about 91 days later (`phi + 91.3`).}
#'   \item{`r_squared`}{Coefficient of determination of the sine fit.}
#'   \item{`rmse`}{Root mean squared error (mm day\eqn{^{-1}}).}
#'   \item{`method`}{The method actually used (may differ from `method` if
#'     NLS fell back to `"lm_fourier"`).}
#'   \item{`n_obs`}{Number of observations used in the fit.}
#' }
#'
#' @export
#'
#' @examples
#' # Synthetic noiseless series with known parameters
#' dates  <- seq(as.Date("2010-01-01"), as.Date("2014-12-31"), by = "day")
#' doy    <- as.integer(format(dates, "%j"))
#' pe_obs <- 2.5 + 2.0 * sin(2 * pi / 365.25 * (doy - 172))
#'
#' params <- fit_pe_sine(pe_obs, dates)
#' params$B    # ~2.5
#' params$A    # ~2.0
#' params$phi  # ~172
#'
#' # NLS method
#' params_nls <- fit_pe_sine(pe_obs, dates, method = "nls")
fit_pe_sine <- function(pe, dates, method = c("lm_fourier", "nls")) {

  method <- match.arg(method)

  # -- Validation -------------------------------------------------------------
  if (!is.numeric(pe))
    stop("`pe` must be a numeric vector.")
  if (!inherits(dates, "Date"))
    stop("`dates` must be a Date vector.")
  if (length(pe) != length(dates))
    stop("`pe` and `dates` must be the same length.")
  if (length(pe) < 14L)
    stop("`pe` must have at least 14 observations to fit a sine curve.")
  if (anyNA(pe))
    stop("`pe` contains NA values. Remove or impute before fitting.")
  if (anyNA(dates))
    stop("`dates` contains NA values.")

  # Day of year (1-366) is the only time variable: all years are pooled into
  # one mean annual cycle, and omega fixes the period at 365.25 days.
  doy   <- as.integer(format(dates, "%j"))
  omega <- 2 * pi / 365.25

  # -- lm_fourier method ------------------------------------------------------
  # Linearise: fit PE as B plus C1 times sin(omega*doy) plus C2 times cos(omega*doy)
  # Recover:   A = sqrt(C1^2 + C2^2),  phi = atan2(-C2, C1) / omega
  # (Why atan2(-C2, C1): sin(omega*(doy - phi)) expands to
  #  sin(omega*doy)*cos(omega*phi) - cos(omega*doy)*sin(omega*phi), so
  #  C1 = A*cos(omega*phi) and C2 = -A*sin(omega*phi). A is a norm, hence >= 0.)
  .fit_lm_fourier <- function() {
    # The regressors go in a data frame so lm() names the coefficients below
    terms_df <- data.frame(pe = pe, sin_term = sin(omega * doy),
                           cos_term = cos(omega * doy))
    fit      <- stats::lm(pe ~ sin_term + cos_term, data = terms_df)
    coefs    <- stats::coef(fit)
    B        <- unname(coefs["(Intercept)"])
    C1       <- unname(coefs["sin_term"])
    C2       <- unname(coefs["cos_term"])
    A        <- sqrt(C1^2 + C2^2)
    phi      <- atan2(-C2, C1) / omega
    # Normalise phi into [0, 365.25)
    phi      <- phi %% 365.25
    # Fit statistics use the unclamped sine that was regressed; compare_pe_sine()
    # and generate_pe_sine() floor the curve at zero, so numbers can differ.
    fitted   <- B + A * sin(omega * (doy - phi))
    resid    <- pe - fitted
    ss_res   <- sum(resid^2)
    ss_tot   <- sum((pe - mean(pe))^2)
    # R^2 is undefined for a constant series (zero total variance).
    r_sq     <- if (ss_tot > 0) 1 - ss_res / ss_tot else NA_real_
    rmse     <- sqrt(mean(resid^2))
    list(B = B, A = A, phi = phi, r_squared = r_sq, rmse = rmse,
         method = "lm_fourier", n_obs = length(pe))
  }

  # -- nls method -------------------------------------------------------------
  # Direct fit of the three parameters. There are no bounds, so unlike
  # lm_fourier the optimiser can return a negative A (the same curve as a
  # positive A with phi shifted by half a year): phi is wrapped into [0, 365.25)
  # but the sign of A is left as found.
  # Starting values come from the linear Fourier fit, which is the exact
  # least-squares solution for this model, so nls() starts at the answer and
  # only polishes it. (A fixed phi = 172 was used before; that start described
  # a September peak in this parameterisation and is meaningless as a guess.)
  .fit_nls <- function() {
    lin <- .fit_lm_fourier()
    result <- tryCatch(
      stats::nls(
        pe ~ B + A * sin(omega * (doy - phi)),
        data  = list(pe = pe, doy = doy, omega = omega),
        start = list(B = lin$B, A = lin$A, phi = lin$phi),
        # Starting at the exact linear solution, a noiseless (zero-residual)
        # series makes nls()'s relative-offset convergence test fail with
        # "number of iterations exceeded maximum". scaleOffset adds a constant to
        # the denominator of that test (the remedy documented in ?nls for
        # artificial zero-residual data); it does not change the fitted values.
        control = stats::nls.control(scaleOffset = 1)
      ),
      # Any nls() failure (non-convergence, singular gradient, ...) becomes a
      # warning; the NULL it returns triggers the lm_fourier fallback below.
      error = function(e) {
        warning(sprintf(
          "[fit_pe_sine] NLS failed to converge (%s). Falling back to lm_fourier.",
          e$message
        ))
        NULL
      }
    )
    # The fallback result reports method = "lm_fourier", which is how callers
    # can tell the NLS fit was not used.
    if (is.null(result)) return(.fit_lm_fourier())
    B      <- unname(stats::coef(result)["B"])
    A      <- unname(stats::coef(result)["A"])
    phi    <- unname(stats::coef(result)["phi"]) %% 365.25
    fitted <- stats::fitted(result)
    resid  <- pe - fitted
    ss_res <- sum(resid^2)
    ss_tot <- sum((pe - mean(pe))^2)
    r_sq   <- if (ss_tot > 0) 1 - ss_res / ss_tot else NA_real_
    rmse   <- sqrt(mean(resid^2))
    list(B = B, A = A, phi = phi, r_squared = r_sq, rmse = rmse,
         method = "nls", n_obs = length(pe))
  }

  if (method == "lm_fourier") .fit_lm_fourier() else .fit_nls()
}


# -- generate_pe_sine ---------------------------------------------------------

#' Generate a synthetic daily PE series from sine curve parameters
#'
#' Produces a daily PE time series over a specified date range using the
#' standard annual sine model:
#' \deqn{PE(t) = \max\bigl(0,\; B + A \sin\!\left(\frac{2\pi}{365.25}(doy - \phi)\right)\bigr)}
#' Values are clamped to zero so physically impossible negative PE is never
#' returned.
#'
#' @details
#' The series is daily from `start` to `end` inclusive, using the same
#' day-of-year (1 to 366) and 365.25-day period as [fit_pe_sine()], so
#' parameters fitted there reproduce the same curve here. Because negative
#' values are clamped to zero, a series with `A > B` has a mean above `B` and
#' does not round-trip exactly through `fit_pe_sine()`.
#'
#' With `as_s7 = TRUE` the readings table has columns `dateTime` (midnight UTC
#' of each date), `date` and `value`; with `as_s7 = FALSE` the same numbers
#' come back as `date` and `pe`. `from_date` / `to_date` on the S7 object are
#' the requested range. Nothing is written to disk. Use [disagg_to_15min()] on
#' the S7 result for a finer timestep.
#'
#' @param B Numeric. Annual mean PE (mm day\eqn{^{-1}}).
#' @param A Numeric. Amplitude (mm day\eqn{^{-1}}).
#' @param phi Numeric. Phase offset: the day of year at which the curve rises
#'   through `B` (peak PE is about 91 days later, so about 81 for a UK-like
#'   mid-June peak).
#' @param start Date or character (`"YYYY-MM-DD"`). Start of the output series
#'   (inclusive).
#' @param end Date or character (`"YYYY-MM-DD"`). End of the output series
#'   (inclusive).
#' @param as_s7 Logical. If `TRUE` (default), wrap the result in a
#'   [FlodePotEvap_Daily] S7 object with `source_name = "sine_curve"`.
#'   If `FALSE`, return a plain `data.table` with columns `date` and `pe`.
#'
#' @return A [FlodePotEvap_Daily] S7 object (when `as_s7 = TRUE`) or a
#'   `data.table` with columns `date` (Date) and `pe` (numeric,
#'   mm day\eqn{^{-1}}) (when `as_s7 = FALSE`).
#'
#' @export
#'
#' @examples
#' # Generate 5 years of synthetic PE from fitted parameters
#' params <- list(B = 2.5, A = 2.0, phi = 172)
#' pe_obj <- generate_pe_sine(params$B, params$A, params$phi,
#'                            start = "2010-01-01", end = "2014-12-31")
#' pe_obj  # FlodePotEvap_Daily S7 object
#'
#' # Plain data.table output
#' dt <- generate_pe_sine(2.5, 2.0, 172,
#'                        start = "2020-01-01", end = "2020-12-31",
#'                        as_s7 = FALSE)
#' head(dt)
generate_pe_sine <- function(B, A, phi, start, end, as_s7 = TRUE) {

  if (!is.numeric(B) || length(B) != 1L || is.na(B))
    stop("`B` must be a length-1 numeric scalar.")
  if (!is.numeric(A) || length(A) != 1L || is.na(A))
    stop("`A` must be a length-1 numeric scalar.")
  if (!is.numeric(phi) || length(phi) != 1L || is.na(phi))
    stop("`phi` must be a length-1 numeric scalar.")

  start <- as.Date(start)
  end   <- as.Date(end)
  if (end < start)
    stop("`end` must be on or after `start`.")

  # Inclusive daily sequence. Day of year (1-366) and the 365.25-day period
  # match fit_pe_sine(), so a fitted curve is reproduced here.
  dates     <- seq(start, end, by = "day")
  doy       <- as.integer(format(dates, "%j"))
  # Floor at zero: with A > B the winter trough would be negative, which PE
  # cannot be. This lifts the series mean above B in that case.
  pe_values <- pmax(0, B + A * sin(2 * pi / 365.25 * (doy - phi)))

  # The plain table names the column `pe`; the S7 object uses the schema name
  # `value` and also needs `dateTime` (midnight UTC) and `date`, both required
  # by the FlodePotEvapData validator.
  if (!as_s7) {
    return(data.table::data.table(date = dates, pe = pe_values))
  }

  FlodePotEvap_Daily(
    readings    = data.table::data.table(
      dateTime = as.POSIXct(dates, tz = "UTC"),
      date     = dates,
      value    = pe_values
    ),
    source_name = "sine_curve",
    from_date   = as.character(start),
    to_date     = as.character(end)
  )
}


# -- regionalise_pe_sine ------------------------------------------------------

#' Regionalise PE sine parameters by pooling fitted gauges
#'
#' Combines `fit_pe_sine()` results from multiple gauges into a single set
#' of regional parameters, using weighted or equal averaging. Useful for
#' estimating PE at ungauged catchments by pooling neighbouring gauges.
#'
#' Phase (\eqn{\phi}) averaging is performed on the unit circle to avoid
#' wrap-around artefacts when gauges span the day 0/365 boundary (e.g.
#' mixing gauges with \eqn{\phi = 5} and \eqn{\phi = 360}).
#'
#' @details
#' Order of operations: `NULL` entries are dropped first. `weights`, if given,
#' are checked against the length of the original `params_list` (so they line
#' up with the `NULL` entries too), reduced to the kept fits, and rescaled to
#' sum to 1. A weight attached to a failed fit is therefore discarded, and
#' weights that sum to zero over the kept fits are an error.
#'
#' `B` and `A` are weighted arithmetic means. `phi` is treated as an angle on a
#' circle of 365.25 days and averaged as a weighted mean of unit vectors, so
#' phases either side of the year boundary combine sensibly. The length of the
#' mean vector is discarded; if the phases cancel exactly (for example two
#' equally weighted gauges half a year apart) the result is arbitrary.
#'
#' Only the `B`, `A` and `phi` elements of each entry are read, so any list
#' with those elements works. A fit from `fit_pe_sine(method = "nls")` can carry
#' a negative `A`; averaging it with positive amplitudes mixes sign
#' conventions, so the default `"lm_fourier"` fits (always `A >= 0`) are the
#' safer input. The result can be passed to [generate_pe_sine()].
#'
#' @param params_list Named list of `fit_pe_sine()` results, one per gauge.
#'   `NULL` entries (failed fits) are silently dropped. Must contain at least
#'   one non-NULL entry.
#' @param weights Numeric vector or `NULL`. Importance weights, one per entry
#'   in `params_list` (including NULL entries, which are dropped before
#'   weighting). Weights are normalised to sum to 1. If `NULL`, equal weighting
#'   is used.
#'
#' @return A named list with elements:
#' \describe{
#'   \item{`B`}{Weighted mean annual PE (mm day\eqn{^{-1}}).}
#'   \item{`A`}{Weighted mean amplitude (mm day\eqn{^{-1}}).}
#'   \item{`phi`}{Phase-aware weighted mean phase (day of year).}
#'   \item{`n_gauges`}{Number of gauges used after dropping NULLs.}
#'   \item{`gauge_ids`}{Names of the gauges used, from `names(params_list)`.}
#' }
#'
#' @export
#'
#' @examples
#' dates  <- seq(as.Date("2010-01-01"), as.Date("2019-12-31"), by = "day")
#' doy    <- as.integer(format(dates, "%j"))
#'
#' # Three gauges with slightly different parameters
#' make_pe <- function(B, A, phi)
#'   B + A * sin(2 * pi / 365.25 * (doy - phi))
#'
#' fits <- list(
#'   gauge_A = fit_pe_sine(make_pe(2.4, 1.9, 170), dates),
#'   gauge_B = fit_pe_sine(make_pe(2.6, 2.1, 174), dates),
#'   gauge_C = fit_pe_sine(make_pe(2.5, 2.0, 172), dates)
#' )
#'
#' # Equal weighting
#' regional <- regionalise_pe_sine(fits)
#'
#' # Area weighting (catchment areas in km2)
#' regional_w <- regionalise_pe_sine(fits, weights = c(120, 85, 200))
regionalise_pe_sine <- function(params_list, weights = NULL) {

  if (!is.list(params_list) || length(params_list) == 0L)
    stop("`params_list` must be a non-empty list of fit_pe_sine() results.")

  gauge_ids <- names(params_list)

  # Drop NULL entries (failed fits) — keep weights aligned
  keep      <- !vapply(params_list, is.null, logical(1L))
  valid     <- params_list[keep]
  gauge_ids <- gauge_ids[keep]

  if (length(valid) == 0L)
    stop("All entries in `params_list` are NULL. No fits available to regionalise.")

  # Validate or build weights
  # Weights are validated against the ORIGINAL list length (NULL entries
  # included) so they line up positionally, then subset to the kept fits and
  # renormalised: weight given to a failed fit is simply discarded.
  if (!is.null(weights)) {
    if (length(weights) != length(params_list))
      stop("`weights` must be the same length as `params_list`.")
    weights <- weights[keep]
    if (any(weights < 0))
      stop("`weights` must all be non-negative.")
    if (sum(weights) == 0)
      stop("Sum of `weights` is zero.")
    weights <- weights / sum(weights)
  } else {
    weights <- rep(1 / length(valid), length(valid))
  }

  # Only B, A and phi are read; other fit_pe_sine() fields are ignored.
  b_vec   <- vapply(valid, `[[`, numeric(1L), "B")
  a_vec   <- vapply(valid, `[[`, numeric(1L), "A")
  phi_vec <- vapply(valid, `[[`, numeric(1L), "phi")

  # B and A: plain weighted means.
  b_reg <- sum(weights * b_vec)
  a_reg <- sum(weights * a_vec)

  # Phase-aware averaging on the unit circle
  # phi is an angle on a circle of 365.25 days, so average unit vectors rather
  # than the numbers (phases of 5 and 360 average to about day 0, not 182).
  # The length of the mean vector is dropped; if the phases cancel exactly the
  # result is arbitrary (atan2(0, 0) is 0).
  omega        <- 2 * pi / 365.25
  mean_sin_phi <- sum(weights * sin(omega * phi_vec))
  mean_cos_phi <- sum(weights * cos(omega * phi_vec))
  phi_reg      <- atan2(mean_sin_phi, mean_cos_phi) / omega
  phi_reg      <- phi_reg %% 365.25

  list(B        = b_reg,
       A        = a_reg,
       phi      = phi_reg,
       n_gauges = length(valid),
       gauge_ids = gauge_ids)
}


# -- compare_pe_sine ----------------------------------------------------------

#' Compare an observed PE series against a fitted sine curve
#'
#' Evaluates the goodness-of-fit of a sine curve (defined by parameters
#' `B`, `A`, `phi`) against an observed PE series. Returns RMSE, bias,
#' and \eqn{R^2}, and optionally a `ggplot2` plot overlaying the observed
#' and fitted series.
#'
#' @details
#' The fitted series is `max(0, B + A sin(2 pi / 365.25 (doy - phi)))`: the
#' same curve and zero floor as [generate_pe_sine()]. [fit_pe_sine()] does not
#' apply the floor, so its own `rmse` and `r_squared` can differ slightly from
#' the values here for series that dip below zero.
#'
#' `bias` is fitted minus observed, so a positive value means the model
#' over-estimates. `r_squared` is `1 - SS_res / SS_tot` against the mean of
#' `pe`: it is negative when the curve fits worse than a constant at the
#' observed mean, and `NA` when `pe` is constant. Dates need not be contiguous
#' or sorted, and `B`, `A` and `phi` are not validated beyond being used in the
#' formula. The `fitted` element holds `date`, `observed` and the (floored)
#' curve for every input row.
#'
#' @param pe Numeric vector. Observed daily PE (mm day\eqn{^{-1}}).
#' @param dates Date vector. Dates corresponding to `pe`.
#' @param B Numeric. Annual mean parameter from `fit_pe_sine()`.
#' @param A Numeric. Amplitude parameter from `fit_pe_sine()`.
#' @param phi Numeric. Phase offset parameter from `fit_pe_sine()`.
#' @param plot Logical. If `TRUE`, return a `ggplot2` plot instead of (in
#'   addition to) the numeric diagnostics. The plot is attached to the return
#'   list as element `$plot`. Default `FALSE`.
#'
#' @return A named list with elements:
#' \describe{
#'   \item{`rmse`}{Root mean squared error (mm day\eqn{^{-1}}).}
#'   \item{`bias`}{Mean signed error — fitted minus observed (mm day\eqn{^{-1}}).
#'     Positive values indicate the model overestimates PE.}
#'   \item{`r_squared`}{Coefficient of determination.}
#'   \item{`fitted`}{`data.table` with columns `date`, `observed`, `fitted`.}
#'   \item{`plot`}{A `ggplot` object (only present when `plot = TRUE`).}
#' }
#'
#' @note The `plot = TRUE` functionality will be moved to the `reach.viz`
#'   package in a future release. The diagnostic return values (`rmse`,
#'   `bias`, `r_squared`, `fitted`) will remain in `reach.io`.
#'
#' @export
#'
#' @examples
#' dates  <- seq(as.Date("2010-01-01"), as.Date("2019-12-31"), by = "day")
#' doy    <- as.integer(format(dates, "%j"))
#' pe_obs <- 2.5 + 2.0 * sin(2 * pi / 365.25 * (doy - 172)) +
#'           stats::rnorm(length(dates), sd = 0.3)
#'
#' params <- fit_pe_sine(pe_obs, dates)
#' diag   <- compare_pe_sine(pe_obs, dates, params$B, params$A, params$phi,
#'                           plot = TRUE)
#' diag$rmse
#' diag$bias
#' diag$plot
compare_pe_sine <- function(pe, dates, B, A, phi, plot = FALSE) {

  if (!is.numeric(pe) || anyNA(pe))
    stop("`pe` must be a numeric vector with no NAs.")
  if (!inherits(dates, "Date") || anyNA(dates))
    stop("`dates` must be a Date vector with no NAs.")
  if (length(pe) != length(dates))
    stop("`pe` and `dates` must be the same length.")

  doy    <- as.integer(format(dates, "%j"))
  # Same curve, including the zero floor, as generate_pe_sine().
  fitted <- pmax(0, B + A * sin(2 * pi / 365.25 * (doy - phi)))

  # Signed fitted - observed: bias > 0 means the model over-estimates.
  resid  <- fitted - pe
  rmse   <- sqrt(mean(resid^2))
  bias   <- mean(resid)
  ss_res <- sum((pe - fitted)^2)
  ss_tot <- sum((pe - mean(pe))^2)
  # R^2 is relative to the observed mean, so it goes negative when the curve is
  # worse than a constant; NA for a constant series.
  r_sq   <- if (ss_tot > 0) 1 - ss_res / ss_tot else NA_real_

  fitted_dt <- data.table::data.table(
    date     = dates,
    observed = pe,
    fitted   = fitted
  )

  out <- list(rmse = rmse, bias = bias, r_squared = r_sq, fitted = fitted_dt)

  if (plot) {
    if (!requireNamespace("ggplot2", quietly = TRUE))
      stop("`plot = TRUE` requires the 'ggplot2' package. ",
           "Install it with install.packages(\"ggplot2\").", call. = FALSE)
    # Thin observed points to at most 1000 for readability on long series
    n    <- nrow(fitted_dt)
    idx  <- if (n > 1000L) seq(1L, n, length.out = 1000L) else seq_len(n)
    thin <- fitted_dt[idx]

    p <- ggplot2::ggplot() +
      ggplot2::geom_point(
        data    = thin,
        mapping = ggplot2::aes(x = date, y = observed),
        colour  = "grey60", size = 0.6, alpha = 0.6
      ) +
      ggplot2::geom_line(
        data    = fitted_dt,
        mapping = ggplot2::aes(x = date, y = fitted),
        colour  = "#2166AC", linewidth = 0.8
      ) +
      ggplot2::labs(
        title    = "PE: observed vs. sine curve",
        subtitle = sprintf("RMSE = %.3f mm/day  |  Bias = %+.3f mm/day  |  R\u00b2 = %.3f",
                           rmse, bias, r_sq),
        x = NULL,
        y = "PE (mm/day)"
      ) +
      ggplot2::theme_bw()

    out$plot <- p
  }

  out
}
