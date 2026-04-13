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

  # -- Apply QC per site -------------------------------------------------------
  data.table::setorder(dt, site_id, timestamp)

  flagged_at <- as.POSIXct(Sys.time(), tz = "UTC")

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

  dt[, qc_flag     := y_to_qc_flag(qc_y_code)]
  dt[, qc_value    := data.table::fifelse(qc_flag < 4L, value, NA_real_)]
  dt[, qc_flagged_at := flagged_at]

  # Reorder to Silver schema column order
  data.table::setcolorder(dt, c("timestamp", "value", "supplier_flag",
                                "dataset_id", "site_id", "data_type",
                                "qc_flag", "qc_value",
                                "qc_y_code", "qc_flagged_at"))

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
