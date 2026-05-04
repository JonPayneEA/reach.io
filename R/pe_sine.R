# ============================================================
# Tool:        PE Sine Curve Tools
# Description: Fit, generate, regionalise, and compare annual
#              sine curve approximations to potential
#              evapotranspiration (PE) time series.
#              Standard form: PE(t) = B + A * sin(2pi/365.25 * (doy - phi))
#              B   = annual mean (mm/day)
#              A   = amplitude (mm/day)
#              phi = phase offset (day of year of peak; ~172 for UK)
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-03-25
# Modified:    2026-03-25 - JP: initial implementation
# Tier:        1
# Inputs:      Observed PE numeric vectors with Date vectors
# Outputs:     Fitted parameters; PotEvap_Daily S7 objects; diagnostics
# Dependencies: data.table, ggplot2, stats
# ============================================================


# -- fit_pe_sine --------------------------------------------------------------

#' Fit a sine curve to an observed PE series
#'
#' Estimates the three parameters of the annual PE sine model:
#' \deqn{PE(t) = B + A \sin\!\left(\frac{2\pi}{365.25}(doy - \phi)\right)}
#' where \eqn{B} is the annual mean (mm day\eqn{^{-1}}), \eqn{A} is the
#' amplitude (mm day\eqn{^{-1}}), and \eqn{\phi} is the phase offset
#' (day of year of peak PE; approximately 172 for UK lowland catchments,
#' i.e. around 21 June).
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
#'   \item{`phi`}{Phase offset (day of year of peak PE).}
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

  doy   <- as.integer(format(dates, "%j"))
  omega <- 2 * pi / 365.25

  # -- lm_fourier method ------------------------------------------------------
  # Linearise: PE = B + C1*sin(omega*doy) + C2*cos(omega*doy)
  # Recover:   A = sqrt(C1^2 + C2^2),  phi = atan2(-C2, C1) / omega
  .fit_lm_fourier <- function() {
    sin_term <- sin(omega * doy)
    cos_term <- cos(omega * doy)
    fit      <- stats::lm(pe ~ sin_term + cos_term)
    coefs    <- stats::coef(fit)
    B        <- unname(coefs["(Intercept)"])
    C1       <- unname(coefs["sin_term"])
    C2       <- unname(coefs["cos_term"])
    A        <- sqrt(C1^2 + C2^2)
    phi      <- atan2(-C2, C1) / omega
    # Normalise phi into [0, 365.25)
    phi      <- phi %% 365.25
    fitted   <- B + A * sin(omega * (doy - phi))
    resid    <- pe - fitted
    ss_res   <- sum(resid^2)
    ss_tot   <- sum((pe - mean(pe))^2)
    r_sq     <- if (ss_tot > 0) 1 - ss_res / ss_tot else NA_real_
    rmse     <- sqrt(mean(resid^2))
    list(B = B, A = A, phi = phi, r_squared = r_sq, rmse = rmse,
         method = "lm_fourier", n_obs = length(pe))
  }

  # -- nls method -------------------------------------------------------------
  .fit_nls <- function() {
    amp_guess <- (max(pe) - min(pe)) / 2
    result <- tryCatch(
      stats::nls(
        pe ~ B + A * sin(omega * (doy - phi)),
        data  = list(pe = pe, doy = doy, omega = omega),
        start = list(B = mean(pe), A = amp_guess, phi = 172)
      ),
      error = function(e) {
        warning(sprintf(
          "[fit_pe_sine] NLS failed to converge (%s). Falling back to lm_fourier.",
          e$message
        ))
        NULL
      }
    )
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
#' @param B Numeric. Annual mean PE (mm day\eqn{^{-1}}).
#' @param A Numeric. Amplitude (mm day\eqn{^{-1}}).
#' @param phi Numeric. Phase offset — day of year of peak PE (~172 for UK).
#' @param start Date or character (`"YYYY-MM-DD"`). Start of the output series
#'   (inclusive).
#' @param end Date or character (`"YYYY-MM-DD"`). End of the output series
#'   (inclusive).
#' @param as_s7 Logical. If `TRUE` (default), wrap the result in a
#'   [PotEvap_Daily] S7 object with `source_name = "sine_curve"`.
#'   If `FALSE`, return a plain `data.table` with columns `date` and `pe`.
#'
#' @return A [PotEvap_Daily] S7 object (when `as_s7 = TRUE`) or a
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
#' pe_obj  # PotEvap_Daily S7 object
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

  dates     <- seq(start, end, by = "day")
  doy       <- as.integer(format(dates, "%j"))
  pe_values <- pmax(0, B + A * sin(2 * pi / 365.25 * (doy - phi)))

  if (!as_s7) {
    return(data.table::data.table(date = dates, pe = pe_values))
  }

  PotEvap_Daily(
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

  B_vec   <- vapply(valid, `[[`, numeric(1L), "B")
  A_vec   <- vapply(valid, `[[`, numeric(1L), "A")
  phi_vec <- vapply(valid, `[[`, numeric(1L), "phi")

  B_reg <- sum(weights * B_vec)
  A_reg <- sum(weights * A_vec)

  # Phase-aware averaging on the unit circle
  omega        <- 2 * pi / 365.25
  mean_sin_phi <- sum(weights * sin(omega * phi_vec))
  mean_cos_phi <- sum(weights * cos(omega * phi_vec))
  phi_reg      <- atan2(mean_sin_phi, mean_cos_phi) / omega
  phi_reg      <- phi_reg %% 365.25

  list(B        = B_reg,
       A        = A_reg,
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
  fitted <- pmax(0, B + A * sin(2 * pi / 365.25 * (doy - phi)))

  resid  <- fitted - pe
  rmse   <- sqrt(mean(resid^2))
  bias   <- mean(resid)
  ss_res <- sum((pe - fitted)^2)
  ss_tot <- sum((pe - mean(pe))^2)
  r_sq   <- if (ss_tot > 0) 1 - ss_res / ss_tot else NA_real_

  fitted_dt <- data.table::data.table(
    date     = dates,
    observed = pe,
    fitted   = fitted
  )

  out <- list(rmse = rmse, bias = bias, r_squared = r_sq, fitted = fitted_dt)

  if (plot) {
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
