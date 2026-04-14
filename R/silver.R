# ============================================================
# Tool:        Silver Tier QC Promotion — Flow Data
# Description: Traditional quality-control checks for 15-min
#              river flow data, implementing the Y-digit anomaly
#              coding scheme from the UK-Flow15 QC framework
#              (Fileni et al., 2026). Promotes Bronze Parquet
#              records to Silver tier with QC flags attached.
#
# !! TRIAL / SKELETON CODE — DO NOT USE OPERATIONALLY !!
# This file is a structural prototype intended to demonstrate
# the intended approach and way of working for Silver tier QC.
# Thresholds, check logic, and schema decisions have NOT been
# validated against EA operational data and WILL change.
# Do not apply promote_to_silver() to production datasets until
# this notice is removed following formal review and sign-off.
#
# Reference:   Fileni, F. et al. (2026). UK-Flow15 Part 1:
#              Development of a coherent national-scale 15-min
#              flow dataset. Earth Syst. Sci. Data (preprint).
#              https://doi.org/10.5194/essd-2026-152
#
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-03-19
# Modified:    2026-03-19 - JP: initial skeleton version
# Tier:        2 (Silver)
# Inputs:      Bronze Parquet (data_type = "Q")
# Outputs:     Silver Parquet with qc_flag, qc_value, qc_y_code
# Dependencies: data.table, arrow
# ============================================================

# -- Silver schema overview ---------------------------------------------------
#
# The Silver schema extends the six Bronze columns with four QC columns:
#
# Inherited from Bronze:
#   timestamp      POSIXct UTC   Observation datetime
#   value          float64       Raw observed value (m3/s), as received
#   supplier_flag  character     Supplier quality code, or NA
#   dataset_id     character     Bronze dataset ID; join key to register
#   site_id        character     Supplier site identifier
#   data_type      character     Framework code: Q, H, P, SM, SWE
#
# Added by Silver promotion:
#   qc_flag        integer       1 = Good, 2 = Estimated,
#                                3 = Suspect, 4 = Rejected
#   qc_value       float64       Accepted value; NA where Rejected
#   qc_y_code      integer       UK-Flow15 Y-digit anomaly code (0-9):
#                                0  No issues
#                                1  Negative value
#                                2  Relative spike
#                                3  Absolute spike
#                                4  Drop
#                                5  Fluctuation
#                                6  Truncated low flows
#                                7  Truncated high flows
#                                8  Combination of 2 and 3 (rel + abs spike)
#                                9  Combination of 4 with 1 or 6
#                                   (drop + negative / truncated low)
#   qc_flagged_at  POSIXct UTC   Timestamp of QC promotion run
#
# Y-digit → qc_flag mapping:
#   Y = 0            → 1 (Good)
#   Y = 2,3,4,5,6,7  → 3 (Suspect)
#   Y = 1,8,9        → 4 (Rejected)
#
# Priority rules (highest priority overwrites lower, per the paper):
#   Code 8 > Code 9 > Code 1 > Code 2 > Code 3 > Code 4 >
#   Code 6 = Code 7 > Code 5 > Code 0


# -- Individual QC check functions --------------------------------------------
#
# Each returns a logical vector the same length as `value`.
# TRUE means the check fired for that observation.
# NA values in `value` are always returned as FALSE (no flag).

#' Detect negative flow values (Y = 1)
#'
#' Flags observations where recorded flow is strictly below zero. Negative
#' values are physically unrealistic for standard fluvial gauges. They may
#' occur due to sensor drift, datum errors, or tidal backwater effects.
#'
#' Set `allow_negative = TRUE` in `apply_y_digit()` to suppress this check
#' for ultrasonic gauges or tidal reaches where bidirectional flow is expected.
#'
#' @param value Numeric vector of flow values (m3/s).
#' @return Logical vector; `TRUE` where `value < 0`.
#' @noRd
qc_negative <- function(value) {
  !is.na(value) & value < 0
}


#' Detect relative spikes between consecutive timesteps (Y = 2)
#'
#' A relative spike is a large positive jump in flow relative to the
#' immediately preceding value. Only fired where the preceding value exceeds
#' `min_baseline`, to avoid false positives during near-zero base flows where
#' any absolute change appears large in relative terms.
#'
#' @param value Numeric vector of flow values, ordered by time.
#' @param relative_spike_ratio Numeric. Minimum ratio of increase to previous
#'   value to be flagged. Default 10 (i.e. flow must increase ten-fold in a
#'   single 15-min step).
#' @param min_baseline Numeric. Minimum preceding flow (m3/s) below which
#'   the relative check is not applied. Default 0.1.
#' @return Logical vector; `TRUE` where a relative spike is detected.
#' @noRd
qc_relative_spike <- function(value,
                               relative_spike_ratio = 10,
                               min_baseline         = 0.1) {
  n       <- length(value)
  flag    <- logical(n)
  if (n < 2L) return(flag)

  lag_val <- c(NA_real_, value[-n])
  dv      <- value - lag_val

  tested  <- !is.na(lag_val) & !is.na(value) & lag_val > min_baseline
  flag[tested] <- (dv / lag_val)[tested] > relative_spike_ratio
  flag
}


#' Detect absolute spikes (Y = 3)
#'
#' An absolute spike is a single-step change in flow that is anomalously
#' large relative to the typical variability of the series. Variability is
#' estimated as the median absolute deviation (MAD) of the step-change series
#' (i.e. `abs(diff(value))`), providing a robust, outlier-resistant scale
#' estimate across the full record.
#'
#' Only fired where the current flow exceeds `min_spike_flow`, to avoid
#' flagging noise on very low baseflows.
#'
#' @param value Numeric vector of flow values, ordered by time.
#' @param absolute_spike_k Numeric. Number of MADs above the median step
#'   change required to flag. Default 5.
#' @param min_spike_flow Numeric. Minimum flow (m3/s) at the flagged
#'   timestep below which the absolute check is not applied. Default 1.0.
#' @return Logical vector; `TRUE` where an absolute spike is detected.
#' @noRd
qc_absolute_spike <- function(value,
                               absolute_spike_k = 5,
                               min_spike_flow   = 1.0) {
  n    <- length(value)
  flag <- logical(n)
  if (n < 2L) return(flag)

  dv      <- abs(diff(value))  # step-change magnitudes (length n-1)
  dv_mad  <- mad(dv, constant = 1, na.rm = TRUE)

  if (is.na(dv_mad) || dv_mad == 0) return(flag)

  dv_med  <- median(dv, na.rm = TRUE)
  threshold <- dv_med + absolute_spike_k * dv_mad

  # dv[i] is the change from position i to i+1; flag position i+1
  tested        <- !is.na(value[-1L]) & value[-1L] > min_spike_flow
  flag[-1L][tested] <- dv[tested] > threshold
  flag
}


#' Detect sudden drops in flow (Y = 4)
#'
#' A drop is a single-step decrease in flow that is disproportionately large
#' relative to the preceding value. Only applied where the preceding flow
#' exceeds `min_flow_for_drop` so the check is not triggered during low-flow
#' periods where small absolute changes produce large relative drops.
#'
#' Drops receive lower QC priority than spikes per the UK-Flow15 paper,
#' as abrupt decreases are less likely to represent spurious data.
#'
#' @param value Numeric vector of flow values, ordered by time.
#' @param drop_ratio Numeric. Minimum fractional decrease (0–1) to flag.
#'   Default 0.9 (90% drop in a single step).
#' @param min_flow_for_drop Numeric. Minimum preceding flow (m3/s).
#'   Default 1.0.
#' @return Logical vector; `TRUE` where a sudden drop is detected.
#' @noRd
qc_drop <- function(value,
                     drop_ratio        = 0.9,
                     min_flow_for_drop = 1.0) {
  n       <- length(value)
  flag    <- logical(n)
  if (n < 2L) return(flag)

  lag_val <- c(NA_real_, value[-n])
  tested  <- !is.na(lag_val) & !is.na(value) & lag_val > min_flow_for_drop
  flag[tested] <- ((lag_val - value) / lag_val)[tested] > drop_ratio
  flag
}


#' Detect recurrent short-period fluctuations (Y = 5)
#'
#' Flags observations embedded in a sequence of rapid alternating
#' increases and decreases. These oscillating patterns are physically
#' implausible at 15-min resolution and typically indicate sensor
#' interference, pump cycling, or data transmission artefacts.
#'
#' Detection: within a rolling window of `fluctuation_window` consecutive
#' step-changes, count direction reversals (where consecutive diffs have
#' opposite non-zero signs). If the count meets or exceeds
#' `fluctuation_min_reversals`, all observations in that window are flagged.
#'
#' @param value Numeric vector of flow values, ordered by time.
#' @param fluctuation_window Integer. Window size in timesteps. Default 8
#'   (two hours at 15-min resolution).
#' @param fluctuation_min_reversals Integer. Minimum direction reversals in
#'   the window to trigger the flag. Default 4.
#' @return Logical vector; `TRUE` where fluctuation is detected.
#' @noRd
qc_fluctuation <- function(value,
                            fluctuation_window        = 8L,
                            fluctuation_min_reversals = 4L) {
  n <- length(value)
  flag <- logical(n)
  if (n < fluctuation_window + 1L) return(flag)

  dv      <- diff(value)                     # length n-1
  sgn     <- sign(dv)
  # Reversal: adjacent non-zero signs differ
  reversal <- c(FALSE,
                sgn[-1L] != 0L &
                  sgn[-length(sgn)] != 0L &
                  sgn[-1L] != sgn[-length(sgn)])  # length n-1

  # Slide window over the reversal vector; flag all obs in window when hit
  w <- as.integer(fluctuation_window)
  for (i in seq(w, n - 1L)) {
    if (sum(reversal[(i - w + 1L):i], na.rm = TRUE) >= fluctuation_min_reversals) {
      flag[(i - w + 2L):(i + 1L)] <- TRUE
    }
  }
  flag
}


#' Detect truncated low flows (Y = 6)
#'
#' A low-flow truncation occurs when a recording instrument reaches its
#' minimum sensitivity floor, producing long runs of identical values at the
#' low end of the flow distribution. Detected via run-length encoding: runs
#' of at least `truncation_min_run` consecutive identical values are flagged
#' where the run value falls at or below the `truncation_low_quantile`
#' percentile of the full record.
#'
#' @param value Numeric vector of flow values, ordered by time.
#' @param truncation_min_run Integer. Minimum run length to flag. Default 4
#'   (one hour of identical 15-min values).
#' @param truncation_low_quantile Numeric (0–1). Percentile ceiling for a
#'   run to be considered a low-flow truncation. Default 0.1.
#' @return Logical vector; `TRUE` where low-flow truncation is detected.
#' @noRd
qc_truncation_low <- function(value,
                               truncation_min_run      = 4L,
                               truncation_low_quantile = 0.1) {
  n    <- length(value)
  flag <- logical(n)
  if (n < truncation_min_run) return(flag)

  threshold <- quantile(value, truncation_low_quantile, na.rm = TRUE)
  r         <- rle(round(value, digits = 4L))
  ends      <- cumsum(r$lengths)
  starts    <- ends - r$lengths + 1L

  for (i in seq_along(r$lengths)) {
    if (!is.na(r$values[i]) &&
        r$lengths[i] >= truncation_min_run &&
        r$values[i] <= threshold) {
      flag[starts[i]:ends[i]] <- TRUE
    }
  }
  flag
}


#' Detect truncated high flows (Y = 7)
#'
#' A high-flow truncation occurs when the recording instrument reaches its
#' maximum capacity, producing long runs of identical values at the high end
#' of the flow distribution. Detection mirrors `qc_truncation_low()`: runs
#' of at least `truncation_min_run` consecutive identical values at or above
#' the `truncation_high_quantile` percentile are flagged.
#'
#' @param value Numeric vector of flow values, ordered by time.
#' @param truncation_min_run Integer. Minimum run length to flag. Default 4.
#' @param truncation_high_quantile Numeric (0–1). Percentile floor for a
#'   run to be considered a high-flow truncation. Default 0.9.
#' @return Logical vector; `TRUE` where high-flow truncation is detected.
#' @noRd
qc_truncation_high <- function(value,
                                truncation_min_run       = 4L,
                                truncation_high_quantile = 0.9) {
  n    <- length(value)
  flag <- logical(n)
  if (n < truncation_min_run) return(flag)

  threshold <- quantile(value, truncation_high_quantile, na.rm = TRUE)
  r         <- rle(round(value, digits = 4L))
  ends      <- cumsum(r$lengths)
  starts    <- ends - r$lengths + 1L

  for (i in seq_along(r$lengths)) {
    if (!is.na(r$values[i]) &&
        r$lengths[i] >= truncation_min_run &&
        r$values[i] >= threshold) {
      flag[starts[i]:ends[i]] <- TRUE
    }
  }
  flag
}


# -- Stage / Level (H) QC check functions -------------------------------------
#
# H-digit codes mirror the Y-digit approach but are tuned for stage/level
# series. Stage data has different physical bounds and failure modes to flow:
# spikes tend to be localised (debris, ice, sensor contact), flat-lining
# indicates a frozen or stuck sensor, and datum shifts reflect recalibration
# or sensor movement rather than the physical event dynamics seen in flow.
#
# H-digit code table:
#   0  No issues
#   1  Outside plausible datum range
#   2  Spike (large step up with return within short window)
#   3  Rate of change exceeded physical bound (single step)
#   4  Flat-line (identical value over extended period)
#   5  Datum shift (persistent step-change offset)
#
# Stage-flow consistency (H-code 6 in the original plan) requires a paired
# flow series and is exposed as a standalone function rather than being
# applied automatically inside promote_to_silver().
#
# Priority: code 1 > code 2 > code 3 > code 4 > code 5 > code 0
# All H codes map to qc_flag 3 (Suspect); none auto-Reject.


#' Detect stage values outside plausible datum bounds (H = 1)
#'
#' Flags readings that fall outside the physically credible operating range
#' for the instrument installation. Bounds are station-specific and must be
#' supplied by the caller; defaults are \code{-Inf} / \code{Inf} (no check).
#'
#' @param value Numeric vector of stage/level values (mAOD or mASD).
#' @param min_datum Numeric. Minimum credible stage. Default \code{-Inf}.
#' @param max_credible Numeric. Maximum credible stage. Default \code{Inf}.
#' @return Logical vector; \code{TRUE} where value is outside bounds.
#' @noRd
qc_h_range <- function(value, min_datum = -Inf, max_credible = Inf) {
  !is.na(value) & (value < min_datum | value > max_credible)
}


#' Detect sudden spike-and-return in stage (H = 2)
#'
#' A stage spike is characterised by a single large step-change upward
#' followed by a return to near-original levels within a short window
#' (debris, ice, sensor contact). Detection uses the MAD of the step-change
#' series to set a robust threshold.
#'
#' @param value Numeric vector of stage values, ordered by time.
#' @param spike_k Numeric. MAD multiplier for the threshold. Default 5.
#' @param spike_return_window Integer. Number of subsequent steps within
#'   which a return must be observed. Default 3.
#' @return Logical vector; \code{TRUE} at spike and return positions.
#' @noRd
qc_h_spike <- function(value,
                        spike_k             = 5,
                        spike_return_window = 3L) {
  n    <- length(value)
  flag <- logical(n)
  if (n < 3L) return(flag)

  dv      <- diff(value)
  dv_mad  <- mad(dv, constant = 1, na.rm = TRUE)
  if (is.na(dv_mad) || dv_mad == 0) return(flag)

  threshold <- median(abs(dv), na.rm = TRUE) + spike_k * dv_mad
  up_spikes <- which(!is.na(dv) & dv > threshold)

  for (s in up_spikes) {
    window_end <- min(s + as.integer(spike_return_window), n - 1L)
    if (window_end >= s + 1L &&
        any(dv[(s + 1L):window_end] < -threshold * 0.5, na.rm = TRUE)) {
      flag[(s + 1L):(window_end + 1L)] <- TRUE
    }
  }
  flag
}


#' Detect physically implausible rates of change in stage (H = 3)
#'
#' Rivers and reservoirs can only rise or fall at physically bounded rates.
#' Single-step changes larger than \code{max_rise_per_step} or
#' \code{max_fall_per_step} are almost certainly instrument artefacts.
#' Thresholds should be set in the same units as \code{value} per timestep.
#'
#' @param value Numeric vector of stage values, ordered by time.
#' @param max_rise_per_step Numeric. Maximum credible rise per timestep.
#'   Default 0.5 (suitable for 15-min data in mAOD).
#' @param max_fall_per_step Numeric. Maximum credible fall per timestep.
#'   Default 0.5.
#' @return Logical vector; \code{TRUE} at the timestep where the rate was
#'   exceeded (the destination of the step, not the origin).
#' @noRd
qc_h_rate_of_change <- function(value,
                                 max_rise_per_step = 0.5,
                                 max_fall_per_step = 0.5) {
  n    <- length(value)
  flag <- logical(n)
  if (n < 2L) return(flag)

  dv          <- diff(value)
  flag[-1L]   <- !is.na(dv) & (dv > max_rise_per_step | dv < -max_fall_per_step)
  flag
}


#' Detect flat-lined stage (H = 4)
#'
#' A frozen or stuck sensor produces long runs of identical values across its
#' full operating range — not restricted to low or high quantiles as in the
#' flow truncation checks. Any run of \code{flatline_min_run} or more
#' identical values is flagged.
#'
#' @param value Numeric vector of stage values, ordered by time.
#' @param flatline_min_run Integer. Minimum run length to flag. Default 4
#'   (one hour of identical 15-min values).
#' @return Logical vector; \code{TRUE} throughout each flagged run.
#' @noRd
qc_h_flatline <- function(value, flatline_min_run = 4L) {
  n    <- length(value)
  flag <- logical(n)
  if (n < flatline_min_run) return(flag)

  r      <- rle(round(value, digits = 4L))
  ends   <- cumsum(r$lengths)
  starts <- ends - r$lengths + 1L

  for (i in seq_along(r$lengths)) {
    if (!is.na(r$values[i]) && r$lengths[i] >= flatline_min_run) {
      flag[starts[i]:ends[i]] <- TRUE
    }
  }
  flag
}


#' Detect stage–flow consistency failures (standalone utility)
#'
#' Flags timesteps where stage and flow move in opposite directions
#' consistently over a rolling window — physically implausible for a single
#' cross-section under normal conditions. This check requires a paired flow
#' series and is therefore not called automatically inside
#' \code{promote_to_silver()}; it is exposed as a standalone utility for use
#' in multi-type workflows.
#'
#' @param stage Numeric vector of stage values, ordered by time.
#' @param flow Numeric vector of flow values the same length as \code{stage}.
#' @param consistency_window Integer. Rolling window size in timesteps.
#'   Default 4.
#' @return Logical vector; \code{TRUE} where sustained inconsistency is
#'   detected.
#' @export
qc_h_stage_flow_consistency <- function(stage, flow,
                                         consistency_window = 4L) {
  n <- length(stage)
  flag <- logical(n)
  if (length(flow) != n)
    stop("`stage` and `flow` must be the same length.")
  if (n < consistency_window + 1L) return(flag)

  d_stage <- diff(stage)
  d_flow  <- diff(flow)

  nonzero <- !is.na(d_stage) & !is.na(d_flow)
  incon   <- nonzero &
             sign(d_stage) != 0L & sign(d_flow) != 0L &
             sign(d_stage) != sign(d_flow)

  w <- as.integer(consistency_window)
  for (i in seq(w, n - 1L)) {
    if (sum(incon[(i - w + 1L):i], na.rm = TRUE) >= w) {
      flag[(i - w + 2L):(i + 1L)] <- TRUE
    }
  }
  flag
}


#' Detect persistent datum shifts in stage (H = 5)
#'
#' Compares the median stage in two adjacent rolling windows of width
#' \code{shift_window}. A sustained offset exceeding \code{shift_threshold}
#' between successive windows indicates a likely sensor recalibration,
#' datum reset, or physical movement of the gauge. The post-shift window is
#' flagged.
#'
#' @param value Numeric vector of stage values, ordered by time.
#' @param shift_window Integer. Width (timesteps) of each comparison window.
#'   Default 24 (six hours at 15-min resolution).
#' @param shift_threshold Numeric. Minimum median difference (same units as
#'   \code{value}) required to flag a shift. Default 0.1.
#' @return Logical vector; \code{TRUE} in the post-shift window where a
#'   datum shift is detected.
#' @noRd
qc_h_datum_shift <- function(value,
                              shift_window    = 24L,
                              shift_threshold = 0.1) {
  n    <- length(value)
  flag <- logical(n)
  w    <- as.integer(shift_window)
  if (n < 2L * w) return(flag)

  for (i in seq(w + 1L, n - w + 1L)) {
    before <- value[(i - w):(i - 1L)]
    after  <- value[i:min(i + w - 1L, n)]
    shift  <- abs(median(after, na.rm = TRUE) - median(before, na.rm = TRUE))
    if (!is.na(shift) && shift > shift_threshold) {
      flag[i:min(i + w - 1L, n)] <- TRUE
    }
  }
  flag
}


# -- H-digit assembly ---------------------------------------------------------

#' Apply all Stage/Level QC checks and return H-digit codes
#'
#' Runs all five automated H checks against an ordered stage series and
#' combines results into a single integer code per observation. Stage-flow
#' consistency is not applied here as it requires a paired flow series; use
#' \code{\link{qc_h_stage_flow_consistency}()} directly for that check.
#'
#' Priority order (highest overwrites lower):
#' code 1 (range) > code 2 (spike) > code 3 (rate of change) >
#' code 4 (flatline) > code 5 (datum shift) > code 0 (clean)
#'
#' @param value Numeric vector of stage values, ordered by time.
#' @param min_datum,max_credible Forwarded to \code{qc_h_range()}.
#' @param spike_k,spike_return_window Forwarded to \code{qc_h_spike()}.
#' @param max_rise_per_step,max_fall_per_step Forwarded to
#'   \code{qc_h_rate_of_change()}.
#' @param flatline_min_run Forwarded to \code{qc_h_flatline()}.
#' @param shift_window,shift_threshold Forwarded to \code{qc_h_datum_shift()}.
#' @return Integer vector of H-digit codes (0–5), one per observation.
#' @noRd
apply_h_code <- function(value,
                          min_datum           = -Inf,
                          max_credible        =  Inf,
                          spike_k             = 5,
                          spike_return_window = 3L,
                          max_rise_per_step   = 0.5,
                          max_fall_per_step   = 0.5,
                          flatline_min_run    = 4L,
                          shift_window        = 24L,
                          shift_threshold     = 0.1) {
  rng <- qc_h_range(value, min_datum, max_credible)
  spk <- qc_h_spike(value, spike_k, spike_return_window)
  roc <- qc_h_rate_of_change(value, max_rise_per_step, max_fall_per_step)
  flt <- qc_h_flatline(value, flatline_min_run)
  dst <- qc_h_datum_shift(value, shift_window, shift_threshold)

  h        <- integer(length(value))
  h[dst]   <- 5L
  h[flt]   <- 4L
  h[roc]   <- 3L
  h[spk]   <- 2L
  h[rng]   <- 1L
  h
}


#' Map H-digit codes to framework qc_flag values
#'
#' All non-zero H codes map to 3 (Suspect). Automatic Rejection is not
#' applied to stage data — decisions about Rejected status require manual
#' review or downstream rule application.
#'
#' @param h Integer vector of H-digit codes (0–5).
#' @return Integer vector of qc_flag values (1 or 3).
#' @noRd
h_to_qc_flag <- function(h) {
  flag           <- integer(length(h))
  flag[h == 0L]  <- 1L   # Good
  flag[h  > 0L]  <- 3L   # Suspect
  flag
}


# -- Rainfall (P) QC check functions ------------------------------------------
#
# P-digit codes cover the common failure modes of tipping-bucket and
# catchment-average rainfall gauges at 15-min resolution. Unlike flow and
# stage, rainfall is non-negative and physically bounded by climatological
# maxima. Two checks (dry run during wet periods, temporal isolation) require
# external reference data and are exposed as standalone functions rather than
# being called automatically inside promote_to_silver().
#
# P-digit code table:
#   0  No issues
#   1  Negative value            → Rejected
#   2  Intensity cap exceeded    → Suspect
#   3  24-h accumulation cap     → Suspect
#   4  Flat-line / dry run       → Suspect  (standalone; needs wet-period mask)
#   5  Counter reset / overflow  → Rejected
#   6  Temporal isolation        → Suspect  (standalone; needs neighbours)
#
# Codes 1 and 5 map to Rejected; codes 2, 3, 4, 6 map to Suspect.


#' Detect negative rainfall values (P = 1)
#'
#' Negative increments in 15-min rainfall data are physically impossible and
#' indicate a sensor reset, overflow artefact, or data transmission error.
#'
#' @param value Numeric vector of 15-min rainfall increments (mm).
#' @return Logical vector; \code{TRUE} where \code{value < 0}.
#' @noRd
qc_p_negative <- function(value) {
  !is.na(value) & value < 0
}


#' Detect 15-min intensities exceeding a credible maximum (P = 2)
#'
#' Flags individual 15-min values that exceed the physically credible
#' maximum rainfall intensity for the UK climate. The default of 50 mm per
#' 15 minutes is a conservative upper bound; adjust for specific regional
#' contexts.
#'
#' @param value Numeric vector of 15-min rainfall increments (mm).
#' @param max_intensity_15min Numeric. Credible maximum (mm/15 min).
#'   Default 50.
#' @return Logical vector; \code{TRUE} where intensity exceeds the cap.
#' @noRd
qc_p_intensity <- function(value, max_intensity_15min = 50) {
  !is.na(value) & value > max_intensity_15min
}


#' Detect 24-hour accumulations exceeding a credible maximum (P = 3)
#'
#' Computes a rolling sum over \code{steps_per_day} consecutive timesteps
#' and flags all observations in windows whose total exceeds
#' \code{max_daily_mm}. The default of 300 mm is a conservative upper bound
#' for UK conditions.
#'
#' @param value Numeric vector of 15-min rainfall increments (mm),
#'   ordered by time.
#' @param max_daily_mm Numeric. Maximum credible 24-hour total (mm).
#'   Default 300.
#' @param steps_per_day Integer. Number of timesteps per 24 hours.
#'   Default 96 (15-min data).
#' @return Logical vector; \code{TRUE} for all observations in windows
#'   that exceed the daily cap.
#' @noRd
qc_p_daily_cap <- function(value,
                            max_daily_mm  = 300,
                            steps_per_day = 96L) {
  n    <- length(value)
  flag <- logical(n)
  s    <- as.integer(steps_per_day)
  if (n < s) return(flag)

  for (i in seq(s, n)) {
    idx <- (i - s + 1L):i
    if (sum(value[idx], na.rm = TRUE) > max_daily_mm) {
      flag[idx] <- TRUE
    }
  }
  flag
}


#' Detect extended dry runs during known wet periods (standalone utility)
#'
#' Long sequences of zero rainfall during periods when neighbouring gauges
#' are recording rain suggest a blocked or frozen gauge. This check requires
#' an external \code{wet_period_mask} (a logical vector of the same length
#' as \code{value}, \code{TRUE} where the period is known to be wet) and is
#' therefore not applied automatically inside \code{promote_to_silver()}.
#'
#' @param value Numeric vector of 15-min rainfall increments (mm),
#'   ordered by time.
#' @param wet_period_mask Logical vector, same length as \code{value}.
#'   \code{TRUE} indicates the timestep falls within a known wet period.
#' @param dry_run_min_steps Integer. Minimum consecutive zero-rainfall steps
#'   during a wet period to trigger the flag. Default 12 (three hours).
#' @return Logical vector; \code{TRUE} throughout each flagged dry run.
#' @export
qc_p_dry_run <- function(value, wet_period_mask, dry_run_min_steps = 12L) {
  n <- length(value)
  flag <- logical(n)
  if (length(wet_period_mask) != n)
    stop("`value` and `wet_period_mask` must be the same length.")
  if (n < dry_run_min_steps) return(flag)

  is_dry_in_wet <- !is.na(value) & value == 0 & wet_period_mask
  r      <- rle(is_dry_in_wet)
  ends   <- cumsum(r$lengths)
  starts <- ends - r$lengths + 1L

  for (i in seq_along(r$lengths)) {
    if (isTRUE(r$values[i]) && r$lengths[i] >= dry_run_min_steps) {
      flag[starts[i]:ends[i]] <- TRUE
    }
  }
  flag
}


#' Detect tipping-bucket counter resets and overflows (P = 5)
#'
#' A counter overflow causes a sudden isolated spike — a large positive
#' increment immediately followed by a return to typical low values. This is
#' distinct from a genuine rain event in that the spike is not preceded by
#' rising increments and is followed by an abrupt return. Detection mirrors
#' the absolute spike approach: a value that exceeds the median by
#' \code{reset_spike_k} MADs and is followed within
#' \code{reset_return_window} steps by a value at or below the median is
#' flagged.
#'
#' @param value Numeric vector of 15-min rainfall increments (mm),
#'   ordered by time.
#' @param reset_spike_k Numeric. MAD multiplier for the spike threshold.
#'   Default 10.
#' @param reset_return_window Integer. Steps within which a return to
#'   normal must be observed. Default 2.
#' @return Logical vector; \code{TRUE} at the overflow timestep.
#' @noRd
qc_p_reset <- function(value,
                        reset_spike_k       = 10,
                        reset_return_window = 2L) {
  n    <- length(value)
  flag <- logical(n)
  if (n < 3L) return(flag)

  dv_mad <- mad(value, constant = 1, na.rm = TRUE)
  if (is.na(dv_mad) || dv_mad == 0) return(flag)

  dv_med    <- median(value, na.rm = TRUE)
  threshold <- dv_med + reset_spike_k * dv_mad

  spikes <- which(!is.na(value) & value > threshold)
  for (s in spikes) {
    idx_end <- min(s + as.integer(reset_return_window), n)
    if (idx_end > s &&
        any(value[(s + 1L):idx_end] <= dv_med + dv_mad, na.rm = TRUE)) {
      flag[s] <- TRUE
    }
  }
  flag
}


#' Detect rainfall isolated from neighbouring gauges (standalone utility)
#'
#' Rain recorded at a site while all (or most) neighbouring gauges report
#' zero suggests a localised instrument artefact rather than a genuine event.
#' This check requires neighbour readings and is therefore not applied
#' automatically inside \code{promote_to_silver()}.
#'
#' @param value Numeric vector of 15-min rainfall increments (mm).
#' @param neighbours Matrix or data.frame with one row per timestep and one
#'   column per neighbouring gauge. Values are 15-min increments (mm).
#' @param isolation_threshold Numeric. Values at or below this level are
#'   treated as "zero rain" when assessing neighbours. Default 0.2 mm.
#' @param isolation_fraction Numeric (0–1). Fraction of neighbours that must
#'   report zero for the isolation check to fire. Default 0.8.
#' @return Logical vector; \code{TRUE} where isolated rainfall is detected.
#' @export
qc_p_temporal_consistency <- function(value, neighbours,
                                       isolation_threshold = 0.2,
                                       isolation_fraction  = 0.8) {
  n <- length(value)
  flag <- logical(n)

  if (!is.matrix(neighbours) && !is.data.frame(neighbours))
    stop("`neighbours` must be a matrix or data.frame of neighbour readings.")
  if (nrow(neighbours) != n)
    stop("`neighbours` must have the same number of rows as `value`.")

  nbr_mat <- as.matrix(neighbours)
  n_nbrs  <- ncol(nbr_mat)
  if (n_nbrs == 0L) return(flag)

  nbr_zero_frac <- apply(nbr_mat, 1, function(row) {
    sum(!is.na(row) & row <= isolation_threshold) / n_nbrs
  })

  flag <- !is.na(value) & value > isolation_threshold &
          !is.na(nbr_zero_frac) & nbr_zero_frac >= isolation_fraction
  flag
}


# -- P-digit assembly ---------------------------------------------------------

#' Apply automated Rainfall QC checks and return P-digit codes
#'
#' Runs the four self-contained P checks (negative, intensity cap, daily cap,
#' counter reset) against an ordered 15-min rainfall series. The dry-run and
#' temporal-isolation checks require external reference data and must be
#' applied separately via \code{\link{qc_p_dry_run}()} and
#' \code{\link{qc_p_temporal_consistency}()}.
#'
#' Priority order:
#' code 1 (negative) = code 5 (reset) > code 2 (intensity) >
#' code 3 (daily cap) > code 0 (clean)
#'
#' @param value Numeric vector of 15-min rainfall increments (mm).
#' @param max_intensity_15min Forwarded to \code{qc_p_intensity()}.
#' @param max_daily_mm Forwarded to \code{qc_p_daily_cap()}.
#' @param steps_per_day Forwarded to \code{qc_p_daily_cap()}.
#' @param reset_spike_k,reset_return_window Forwarded to \code{qc_p_reset()}.
#' @return Integer vector of P-digit codes (0–5), one per observation.
#' @noRd
apply_p_code <- function(value,
                          max_intensity_15min = 50,
                          max_daily_mm        = 300,
                          steps_per_day       = 96L,
                          reset_spike_k       = 10,
                          reset_return_window = 2L) {
  neg <- qc_p_negative(value)
  int <- qc_p_intensity(value, max_intensity_15min)
  cap <- qc_p_daily_cap(value, max_daily_mm, steps_per_day)
  rst <- qc_p_reset(value, reset_spike_k, reset_return_window)

  p        <- integer(length(value))
  p[cap]   <- 3L
  p[int]   <- 2L
  p[rst]   <- 5L
  p[neg]   <- 1L   # highest priority
  p
}


#' Map P-digit codes to framework qc_flag values
#'
#' @param p Integer vector of P-digit codes (0–5).
#' @return Integer vector of qc_flag values (1, 3, or 4).
#' @noRd
p_to_qc_flag <- function(p) {
  flag                    <- integer(length(p))
  flag[p == 0L]           <- 1L   # Good
  flag[p %in% c(2L, 3L)] <- 3L   # Suspect
  flag[p %in% c(1L, 5L)] <- 4L   # Rejected
  flag
}


# -- Y-digit assembly ---------------------------------------------------------

#' Apply all traditional QC checks and return the UK-Flow15 Y-digit code
#'
#' Runs all seven individual QC checks against an ordered flow series and
#' combines their results into a single integer code per observation,
#' following the UK-Flow15 Y-digit scheme (Table 2, Fileni et al., 2026).
#'
#' Priority rules applied (highest priority overwrites lower):
#' \enumerate{
#'   \item Code 8 — relative + absolute spike (combined, highest priority)
#'   \item Code 9 — drop + negative or truncated low (combined)
#'   \item Code 1 — negative value
#'   \item Code 2 — relative spike
#'   \item Code 3 — absolute spike
#'   \item Code 4 — drop
#'   \item Code 6/7 — truncated low / high
#'   \item Code 5 — fluctuation (lowest priority; recurrent and lower severity)
#' }
#'
#' All parameters prefixed with a check name are forwarded to the
#' corresponding check function. See individual `qc_*()` function
#' documentation for details.
#'
#' @param value Numeric vector of flow values, ordered by time.
#' @param allow_negative Logical. If `TRUE`, negative values are not flagged
#'   (appropriate for ultrasonic gauges or tidal reaches). Default `FALSE`.
#' @param relative_spike_ratio Numeric. Forwarded to `qc_relative_spike()`.
#' @param min_baseline Numeric. Forwarded to `qc_relative_spike()`.
#' @param absolute_spike_k Numeric. Forwarded to `qc_absolute_spike()`.
#' @param min_spike_flow Numeric. Forwarded to `qc_absolute_spike()`.
#' @param drop_ratio Numeric. Forwarded to `qc_drop()`.
#' @param min_flow_for_drop Numeric. Forwarded to `qc_drop()`.
#' @param fluctuation_window Integer. Forwarded to `qc_fluctuation()`.
#' @param fluctuation_min_reversals Integer. Forwarded to `qc_fluctuation()`.
#' @param truncation_min_run Integer. Forwarded to truncation checks.
#' @param truncation_low_quantile Numeric. Forwarded to `qc_truncation_low()`.
#' @param truncation_high_quantile Numeric. Forwarded to `qc_truncation_high()`.
#'
#' @return Integer vector of Y-digit codes (0–9), one per observation.
#' @noRd
apply_y_digit <- function(value,
                           allow_negative            = FALSE,
                           relative_spike_ratio      = 10,
                           min_baseline              = 0.1,
                           absolute_spike_k          = 5,
                           min_spike_flow            = 1.0,
                           drop_ratio                = 0.9,
                           min_flow_for_drop         = 1.0,
                           fluctuation_window        = 8L,
                           fluctuation_min_reversals = 4L,
                           truncation_min_run        = 4L,
                           truncation_low_quantile   = 0.1,
                           truncation_high_quantile  = 0.9) {

  neg <- qc_negative(value)
  rsp <- qc_relative_spike(value, relative_spike_ratio, min_baseline)
  asp <- qc_absolute_spike(value, absolute_spike_k, min_spike_flow)
  drp <- qc_drop(value, drop_ratio, min_flow_for_drop)
  flu <- qc_fluctuation(value, fluctuation_window, fluctuation_min_reversals)
  trl <- qc_truncation_low(value, truncation_min_run, truncation_low_quantile)
  trh <- qc_truncation_high(value, truncation_min_run, truncation_high_quantile)

  # Combined codes
  code8 <- rsp & asp           # relative AND absolute spike
  code9 <- drp & (neg | trl)  # drop AND (negative OR truncated low)

  # Assign in ascending priority order; later assignments overwrite earlier
  y           <- integer(length(value))
  y[flu]      <- 5L
  y[trh]      <- 7L
  y[trl]      <- 6L
  y[drp]      <- 4L
  y[asp]      <- 3L
  y[rsp]      <- 2L
  if (!allow_negative) y[neg] <- 1L
  y[code9]    <- 9L
  y[code8]    <- 8L
  y
}


#' Map UK-Flow15 Y-digit codes to framework qc_flag values
#'
#' Converts the Y-digit anomaly codes produced by `apply_y_digit()` into
#' the integer QC flag used in the Silver schema:
#'
#' \describe{
#'   \item{1 — Good}{Y = 0 (no anomaly detected)}
#'   \item{3 — Suspect}{Y = 2, 3, 4, 5, 6, 7}
#'   \item{4 — Rejected}{Y = 1, 8, 9}
#' }
#'
#' Estimated (flag 2) is not assigned by the automated checks; it is
#' reserved for manually corrected or gap-filled values added downstream.
#'
#' @param y Integer vector of Y-digit codes (0–9).
#' @return Integer vector of qc_flag values (1, 3, or 4).
#' @noRd
y_to_qc_flag <- function(y) {
  flag <- integer(length(y))
  flag[y == 0L]              <- 1L  # Good
  flag[y %in% c(2L:7L)]     <- 3L  # Suspect
  flag[y %in% c(1L, 8L, 9L)] <- 4L  # Rejected
  flag
}


# -- Silver file path ---------------------------------------------------------

#' Build the Silver Parquet file path for a dataset
#'
#' Constructs the Silver file path following the framework folder structure:
#' `silver/<category>/<data_type>/<YYYY>/<dataset_id>.parquet`
#'
#' The Silver tier drops the supplier code level present in Bronze, as
#' promoted data is normalised to a common schema regardless of origin.
#'
#' @param output_dir Character. Root store directory.
#' @param category Character. Data category, e.g. `"hydrometric"`.
#' @param data_type Character. Framework data type code, e.g. `"Q"`.
#' @param dataset_id Character. Bronze dataset ID. Year is parsed from the
#'   trailing eight-digit date component.
#'
#' @return Character. Full file path including filename.
#'
#' @export
#'
#' @examples
#' silver_path("data/hydrometric", "hydrometric", "Q", "EA_39001_Q_20260115")
silver_path <- function(output_dir, category, data_type, dataset_id) {
  year <- substr(dataset_id, nchar(dataset_id) - 7L, nchar(dataset_id) - 4L)
  file.path(output_dir, "silver", category, data_type, year,
            paste0(dataset_id, ".parquet"))
}


# -- Main promotion function --------------------------------------------------

#' Promote a Bronze flow dataset to Silver tier with QC flags
#'
#' Reads a Bronze Parquet file (or accepts a `data.table` directly), applies
#' the traditional quality-control checks from the UK-Flow15 framework
#' (Fileni et al., 2026), and writes a Silver Parquet file. The output
#' extends the Bronze schema with four QC columns: `qc_flag`, `qc_value`,
#' `qc_y_code`, and `qc_flagged_at`.
#'
#' QC checks are applied independently per `site_id`. The series for each
#' gauge is sorted by `timestamp` before checks are run.
#'
#' Only the **traditional QC checks** (Y-digit of the UK-Flow15 three-digit
#' code) are implemented here. The consistency checks against NRFA daily /
#' AMAX / POT data (X-digit) and the high-flow hydrological plausibility
#' checks (Z-digit) require external reference datasets and are not applied
#' automatically; they can be added to the output manually via the
#' `qc_x_code` and `qc_z_code` columns after this step.
#'
#' @section QC flag definitions:
#' \describe{
#'   \item{1 — Good}{No anomaly detected.}
#'   \item{2 — Estimated}{Reserved for manually corrected or gap-filled
#'     values; not assigned by this function.}
#'   \item{3 — Suspect}{Anomaly detected but value may still be physically
#'     plausible. Y-digit codes 2–7.}
#'   \item{4 — Rejected}{Value is almost certainly erroneous. Y-digit codes
#'     1 (negative), 8 (rel + abs spike), or 9 (drop + neg/trunc_low).}
#' }
#'
#' @param bronze_data Character path to a Bronze Parquet file, or a
#'   `data.table` conforming to the Bronze schema.
#' @param output_dir Character. Root store directory. Silver file is written
#'   to `silver/<category>/<data_type>/<year>/<dataset_id>.parquet`.
#' @param category Character. Data category for the output path, e.g.
#'   `"hydrometric"`. Default `"hydrometric"`.
#' @param allow_negative Logical. If `TRUE`, negative flow values are not
#'   flagged as Rejected. Appropriate for ultrasonic or tidal gauges.
#'   Default `FALSE`.
#' @param relative_spike_ratio Numeric. Ten-fold increase threshold for
#'   relative spike detection. Default 10.
#' @param min_baseline Numeric (m3/s). Minimum preceding value required for
#'   the relative spike check to fire. Default 0.1.
#' @param absolute_spike_k Numeric. MAD multiplier for absolute spike
#'   detection. Default 5.
#' @param min_spike_flow Numeric (m3/s). Minimum current value for absolute
#'   spike check to fire. Default 1.0.
#' @param drop_ratio Numeric (0–1). Fractional single-step decrease required
#'   to flag a drop. Default 0.9.
#' @param min_flow_for_drop Numeric (m3/s). Minimum preceding value for drop
#'   check to fire. Default 1.0.
#' @param fluctuation_window Integer. Rolling window size (timesteps) for
#'   fluctuation detection. Default 8 (two hours).
#' @param fluctuation_min_reversals Integer. Minimum direction reversals in
#'   the window to flag fluctuation. Default 4.
#' @param truncation_min_run Integer. Minimum consecutive identical values
#'   to flag a truncation. Default 4 (one hour).
#' @param truncation_low_quantile Numeric (0–1). Quantile ceiling for
#'   low-flow truncation. Default 0.1.
#' @param truncation_high_quantile Numeric (0–1). Quantile floor for
#'   high-flow truncation. Default 0.9.
#' @param write_output Logical. If `TRUE` (default), write Silver Parquet to
#'   disk. Set `FALSE` to return the result without writing.
#' @param dedup Logical. If `TRUE` (default), duplicate `(site_id, timestamp)`
#'   rows are removed before QC, keeping the last row per pair. A warning is
#'   issued if any duplicates are found. Set `FALSE` to skip deduplication
#'   (e.g. when the caller has already deduplicated).
#' @param annotate_gaps Logical. If `TRUE` (default), the number of missing
#'   timesteps per site is estimated from the median observed interval and
#'   attached as a named integer vector via `attr(result, "gap_counts")`. A
#'   warning is issued if any site has gaps. Set `FALSE` to skip annotation.
#'
#' @return A `data.table` conforming to the Silver schema, invisibly when
#'   `write_output = TRUE`. When `annotate_gaps = TRUE`, the result carries an
#'   attribute `"gap_counts"`: a named integer vector of missing-timestep
#'   counts keyed by `site_id`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Promote a single Bronze file
#' promote_to_silver(
#'   bronze_data = "data/hydrometric/bronze/hydrometric/EA/Q/2024/EA_39001_Q_20240101.parquet",
#'   output_dir  = "data/hydrometric"
#' )
#'
#' # From a data.table (e.g. after run_backfill())
#' silver_dt <- promote_to_silver(
#'   bronze_data  = bronze_dt,
#'   output_dir   = "data/hydrometric",
#'   write_output = FALSE
#' )
#'
#' # Allow negative values for a tidal gauge
#' promote_to_silver(
#'   bronze_data    = "data/hydrometric/bronze/.../EA_TG001_Q_20240101.parquet",
#'   output_dir     = "data/hydrometric",
#'   allow_negative = TRUE
#' )
#' }
promote_to_silver <- function(bronze_data,
                               output_dir,
                               category                  = "hydrometric",
                               # --- Flow (Q) parameters ---
                               allow_negative            = FALSE,
                               relative_spike_ratio      = 10,
                               min_baseline              = 0.1,
                               absolute_spike_k          = 5,
                               min_spike_flow            = 1.0,
                               drop_ratio                = 0.9,
                               min_flow_for_drop         = 1.0,
                               fluctuation_window        = 8L,
                               fluctuation_min_reversals = 4L,
                               truncation_min_run        = 4L,
                               truncation_low_quantile   = 0.1,
                               truncation_high_quantile  = 0.9,
                               # --- Stage / Level (H) parameters ---
                               min_datum                 = -Inf,
                               max_credible              =  Inf,
                               spike_k                   = 5,
                               spike_return_window       = 3L,
                               max_rise_per_step         = 0.5,
                               max_fall_per_step         = 0.5,
                               flatline_min_run          = 4L,
                               shift_window              = 24L,
                               shift_threshold           = 0.1,
                               # --- Rainfall (P) parameters ---
                               max_intensity_15min       = 50,
                               max_daily_mm              = 300,
                               steps_per_day             = 96L,
                               reset_spike_k             = 10,
                               reset_return_window       = 2L,
                               # --- Pipeline options ---
                               write_output              = TRUE,
                               dedup                     = TRUE,
                               annotate_gaps             = TRUE) {

  # -- Trial warning -----------------------------------------------------------
  warning(
    "promote_to_silver() is a skeleton implementation for development purposes ",
    "only. Thresholds and check logic have not been validated against ",
    "operational data and will change. Do not apply to production datasets.",
    call. = FALSE
  )

  # -- Read input --------------------------------------------------------------
  if (is.character(bronze_data)) {
    if (!file.exists(bronze_data)) {
      stop(sprintf("Bronze file not found: %s", bronze_data))
    }
    dt <- data.table::as.data.table(arrow::read_parquet(bronze_data))
  } else if (data.table::is.data.table(bronze_data)) {
    dt <- data.table::copy(bronze_data)
  } else {
    stop("`bronze_data` must be a file path (character) or a data.table.")
  }

  # -- Deduplication -----------------------------------------------------------
  if (dedup) {
    n_before <- nrow(dt)
    dt       <- unique(dt, by = c("site_id", "timestamp"), fromLast = TRUE)
    n_dup    <- n_before - nrow(dt)
    if (n_dup > 0L) {
      warning(
        sprintf("%d duplicate (site_id, timestamp) row(s) removed before QC.",
                n_dup),
        call. = FALSE
      )
    }
  }

  # -- Validate Bronze schema --------------------------------------------------
  required <- c("timestamp", "value", "supplier_flag",
                "dataset_id", "site_id", "data_type")
  missing  <- setdiff(required, names(dt))
  if (length(missing) > 0L) {
    stop(sprintf("Bronze data is missing column(s): %s.",
                 paste(missing, collapse = ", ")))
  }

  # -- Apply QC per site (dispatch by data_type) --------------------------------
  data.table::setorder(dt, site_id, timestamp)

  flagged_at     <- as.POSIXct(Sys.time(), tz = "UTC")
  data_type_used <- dt$data_type[[1L]]

  if (data_type_used == "Q") {
    dt[, qc_y_code := apply_y_digit(
      value,
      allow_negative            = allow_negative,
      relative_spike_ratio      = relative_spike_ratio,
      min_baseline              = min_baseline,
      absolute_spike_k          = absolute_spike_k,
      min_spike_flow            = min_spike_flow,
      drop_ratio                = drop_ratio,
      min_flow_for_drop         = min_flow_for_drop,
      fluctuation_window        = fluctuation_window,
      fluctuation_min_reversals = fluctuation_min_reversals,
      truncation_min_run        = truncation_min_run,
      truncation_low_quantile   = truncation_low_quantile,
      truncation_high_quantile  = truncation_high_quantile
    ), by = site_id]
    dt[, qc_flag := y_to_qc_flag(qc_y_code)]
    code_col <- "qc_y_code"

  } else if (data_type_used == "H") {
    dt[, qc_h_code := apply_h_code(
      value,
      min_datum           = min_datum,
      max_credible        = max_credible,
      spike_k             = spike_k,
      spike_return_window = spike_return_window,
      max_rise_per_step   = max_rise_per_step,
      max_fall_per_step   = max_fall_per_step,
      flatline_min_run    = flatline_min_run,
      shift_window        = shift_window,
      shift_threshold     = shift_threshold
    ), by = site_id]
    dt[, qc_flag := h_to_qc_flag(qc_h_code)]
    code_col <- "qc_h_code"

  } else if (data_type_used == "P") {
    dt[, qc_p_code := apply_p_code(
      value,
      max_intensity_15min = max_intensity_15min,
      max_daily_mm        = max_daily_mm,
      steps_per_day       = steps_per_day,
      reset_spike_k       = reset_spike_k,
      reset_return_window = reset_return_window
    ), by = site_id]
    dt[, qc_flag := p_to_qc_flag(qc_p_code)]
    code_col <- "qc_p_code"

  } else {
    warning(
      sprintf(
        "No QC checks defined for data_type '%s'; all values assigned Good (flag 1).",
        data_type_used
      ),
      call. = FALSE
    )
    dt[, qc_flag := 1L]
    code_col <- NULL
  }

  dt[, qc_value      := data.table::fifelse(qc_flag < 4L, value, NA_real_)]
  dt[, qc_flagged_at := flagged_at]

  # Reorder to Silver schema column order (code column is data_type-specific)
  base_cols  <- c("timestamp", "value", "supplier_flag", "dataset_id",
                  "site_id", "data_type", "qc_flag", "qc_value", "qc_flagged_at")
  col_order  <- if (!is.null(code_col))
    append(base_cols, code_col, after = match("qc_value", base_cols))
  else
    base_cols
  data.table::setcolorder(dt, intersect(col_order, names(dt)))

  # -- Gap annotation ----------------------------------------------------------
  if (annotate_gaps) {
    sites   <- dt[, unique(site_id)]
    gap_vec <- vapply(sites, function(sid) {
      ts <- sort(dt[site_id == sid, timestamp])
      if (length(ts) < 2L) return(0L)
      diffs   <- as.numeric(diff(ts), units = "secs")
      int_sec <- as.integer(stats::median(diffs, na.rm = TRUE))
      if (is.na(int_sec) || int_sec <= 0L) return(0L)
      expected_n <- as.integer(round(
        as.numeric(difftime(max(ts), min(ts), units = "secs")) / int_sec
      )) + 1L
      max(0L, expected_n - length(ts))
    }, integer(1L))
    names(gap_vec) <- sites

    if (any(gap_vec > 0L)) {
      warning(
        sprintf("Gaps detected in %d site(s); see attr(result, \"gap_counts\").",
                sum(gap_vec > 0L)),
        call. = FALSE
      )
    }
    data.table::setattr(dt, "gap_counts", gap_vec)
  }

  # -- Write Silver Parquet ----------------------------------------------------
  if (write_output) {
    dataset_id  <- dt$dataset_id[[1L]]
    data_type   <- dt$data_type[[1L]]
    out_path    <- silver_path(output_dir, category, data_type, dataset_id)

    dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
    arrow::write_parquet(dt, out_path)
    message(sprintf("  Silver file written: %s", out_path))
    invisible(dt)
  } else {
    dt
  }
}
