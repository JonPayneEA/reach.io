# ============================================================
# Tool:        Double-mass plot (trial)
# Description: Compares two cumulative volume series built
#              from Flow, Rainfall, and/or PotEvap S7 objects.
#              Intended as a trial utility for water balance
#              checks; longer term this belongs in reach.viz /
#              reach.hydro.
# Author:      JP
# Created:     2026-03-19
# Tier:        2
# Inputs:      Lists of HydroData / PotEvapData S7 objects
# Outputs:     ggplot2 double-mass plot
# Dependencies: S7, data.table, ggplot2
# Notes:       Level gauge data is not yet supported (requires
#              rating equations). All sources on the same axis
#              should share the same timestep; mixed timesteps
#              will produce NA gaps in the combined series.
# ============================================================

#' @import S7
#' @import ggplot2
NULL


# -- Internal helpers ---------------------------------------------------------

#' @noRd
.dt_seconds <- function(period_name) {
  switch(period_name,
    daily  = 86400L,
    hourly = 3600L,
    `15min` = 900L,
    stop(sprintf("double_mass_plot: unknown period_name '%s'.", period_name))
  )
}

#' @noRd
.unit_divisor <- function(output_units) {
  switch(output_units, m3 = 1, Ml = 1e3, Gl = 1e6,
    stop(sprintf("Unknown output_units '%s'. Must be one of: m3, Ml, Gl.", output_units))
  )
}

#' Cumulative sum treating NA as zero contribution
#'
#' Replacement for \code{cumsum()} that treats \code{NA} values as contributing
#' zero to the running total rather than propagating \code{NA} through all
#' subsequent values. Useful for accumulating rainfall or volume series where
#' missing timesteps should not reset the cumulative total.
#'
#' @param x A numeric vector.
#' @return A numeric vector of the same length as \code{x}.
#' @export
#' @examples
#' cumsum_na(c(1, NA, 3))  # returns c(1, 1, 4)
#' cumsum_na(c(NA, NA))    # returns c(0, 0)
cumsum_na <- function(x) cumsum(data.table::fifelse(is.na(x), 0, x))

# Auto-generate a readable axis label from a list of objects
#' @noRd
.axis_label <- function(data_list, output_units) {
  params <- vapply(data_list, function(obj) {
    if (S7::S7_inherits(obj, HydroData))    obj@parameter
    else if (S7::S7_inherits(obj, PotEvapData)) "pot. evaporation"
    else "unknown"
  }, character(1L))

  unique_params <- unique(params)
  param_str <- paste(unique_params, collapse = " + ")
  n <- length(data_list)
  prefix <- if (n > 1L) "Cumulative weighted" else "Cumulative"
  sprintf("%s %s (%s)", prefix, param_str, output_units)
}

# Scale one S7 object's readings to volume in output_units, optionally
# subtracting PE from rainfall.
#
# Returns data.table(dateTime, value) where value is in output_units.
#' @noRd
.scale_to_volume <- function(obj, weight, catchment_area_km2,
                             output_units, pe_daily = NULL) {

  if (S7::S7_inherits(obj, HydroData) && obj@parameter == "level") {
    stop(
      "double_mass_plot: level gauge data is not yet supported. ",
      "Convert to flow via a rating equation before passing to this function."
    )
  }

  dt <- data.table::copy(obj@readings)[, .(dateTime, date, value)]

  is_rainfall <- S7::S7_inherits(obj, HydroData) && obj@parameter == "rainfall"
  is_pe_obj   <- S7::S7_inherits(obj, PotEvapData)
  is_flow     <- S7::S7_inherits(obj, HydroData) && obj@parameter == "flow"

  period_nm <- obj@period_name
  dt_sec    <- .dt_seconds(period_nm)
  unit_div  <- .unit_divisor(output_units)

  # -- Subtract daily PE from rainfall (distributed evenly across sub-day slots)
  if (is_rainfall && !is.null(pe_daily)) {
    slots_per_day <- 86400L / dt_sec
    dt <- merge(dt, pe_daily[, .(date, pe_mm = value)], by = "date", all.x = TRUE)
    dt[, value := value - data.table::fifelse(is.na(pe_mm), 0, pe_mm / slots_per_day)]
    dt[, pe_mm := NULL]
  }

  # -- Apply volume scale factor
  # Rainfall / PE (mm):   1 mm over 1 km² = 1e-3 m × 1e6 m² = 1 000 m³
  # Flow      (m³/s):     volume = mean_rate × timestep_seconds
  if (is_rainfall || is_pe_obj) {
    if (is.null(catchment_area_km2)) {
      stop(
        "double_mass_plot: `catchment_area_km2` must be provided for ",
        "rainfall and potential evaporation sources."
      )
    }
    factor <- weight * catchment_area_km2 * 1e3 / unit_div
  } else if (is_flow) {
    factor <- weight * dt_sec / unit_div
  } else {
    stop("double_mass_plot: unrecognised object class '", class(obj)[1L], "'.")
  }

  dt[, value := value * factor]
  dt[, .(dateTime, value)]
}

# Merge a list of scaled data.tables into one combined series (outer join,
# summing across sources so that missing sources contribute 0).
#' @noRd
.combine_series <- function(data_list, weights, catchment_area_km2,
                            output_units, pe_daily) {

  scaled <- Map(
    function(obj, w, idx) {
      dt <- .scale_to_volume(obj, w, catchment_area_km2, output_units, pe_daily)
      data.table::setnames(dt, "value", paste0("v", idx))
      dt
    },
    data_list, weights, seq_along(data_list)
  )

  merged <- Reduce(
    function(a, b) merge(a, b, by = "dateTime", all = TRUE),
    scaled
  )

  vcols <- grep("^v", names(merged), value = TRUE)
  merged[, .(dateTime,
             value = rowSums(.SD, na.rm = TRUE)),
         .SDcols = vcols]
}


# -- Exported function --------------------------------------------------------

#' Double-mass plot for hydrological volume comparison (trial)
#'
#' Builds two cumulative volume series — one for each axis — from any
#' combination of [Rainfall_Daily], [Rainfall_15min], [Flow_Daily],
#' [Flow_15min], [PotEvap_Daily], [PotEvap_Hourly], or [PotEvap_15min]
#' objects, then plots the cumulative X series against the cumulative Y
#' series. A perfectly consistent volume ratio plots as a straight line.
#'
#' Dashed reference lines are drawn at 1:1 and at ±(final Y − final X)
#' to give a visual sense of overall balance and drift.
#'
#' @section PE subtraction:
#' When `pe` is supplied, the daily PE signal is subtracted from every
#' rainfall source on both axes before volume conversion. The PE value is
#' distributed evenly across sub-daily rainfall intervals (e.g. ÷96 for
#' 15-minute data).
#'
#' @section Mixed timesteps:
#' All sources on the same axis should ideally share the same timestep.
#' Sources with differing timesteps are outer-joined by `dateTime`; rows
#' present in only one source contribute 0 via `na.rm = TRUE` in the
#' column sum, which may not be physically correct.
#'
#' @section Not yet supported:
#' Level gauge data requires rating equations and is not currently
#' handled. Convert level to flow before passing data to this function.
#'
#' @param x_data A `HydroData` or `PotEvapData` object, **or** a list of
#'   such objects, contributing to the X axis.
#' @param y_data A `HydroData` or `PotEvapData` object, **or** a list of
#'   such objects, contributing to the Y axis.
#' @param catchment_area_km2 Numeric scalar. Catchment area in km².
#'   Required when any source is rainfall or potential evaporation (both
#'   measured in mm and must be multiplied by area to give volume).
#' @param weights_x Numeric vector, length `length(x_data)`. Multiplicative
#'   weights for each X source. Defaults to `1` for all sources.
#' @param weights_y Numeric vector, length `length(y_data)`. Multiplicative
#'   weights for each Y source. Defaults to `1` for all sources.
#' @param pe An optional [PotEvap_Daily] object. When provided, the PE
#'   signal is subtracted from every rainfall source on both axes before
#'   any volume scaling or weighting is applied.
#' @param output_units Character scalar. Volume units for both axes.
#'   `"m3"` (cubic metres), `"Ml"` (megalitres, default), or `"Gl"`
#'   (gigalitres).
#' @param title Character scalar. Plot title.
#' @param subtitle Character scalar. Defaults to the overlapping date
#'   range of the aligned data.
#' @param label_x,label_y Axis labels. Auto-generated from the source
#'   parameter types and `output_units` when `NULL`.
#'
#' @return A `ggplot` object. The aligned, cumulative `data.table` used
#'   to build the plot is attached as `attr(p, "data")` for inspection or
#'   further processing.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Daily flow vs weighted rainfall (no PE correction)
#' p <- double_mass_plot(
#'   x_data             = flow_obj,
#'   y_data             = list(rain_a, rain_b),
#'   catchment_area_km2 = 120.5,
#'   weights_y          = c(0.6, 0.4),
#'   output_units       = "Ml",
#'   title              = "Flow vs weighted rainfall"
#' )
#' p
#'
#' # With PE correction applied to rainfall
#' p2 <- double_mass_plot(
#'   x_data             = flow_obj,
#'   y_data             = rain_obj,
#'   catchment_area_km2 = 120.5,
#'   pe                 = pe_daily_obj,
#'   output_units       = "Gl"
#' )
#' p2
#'
#' # Inspect the underlying data
#' attr(p, "data")
#' }
double_mass_plot <- function(x_data,
                             y_data,
                             catchment_area_km2 = NULL,
                             weights_x          = NULL,
                             weights_y          = NULL,
                             pe                 = NULL,
                             output_units       = "Ml",
                             title              = "Double-mass plot",
                             subtitle           = NULL,
                             label_x            = NULL,
                             label_y            = NULL) {

  # -- Coerce single objects to lists
  if (!is.list(x_data) || S7::S7_inherits(x_data, S7::S7_object)) {
    x_data <- list(x_data)
  }
  if (!is.list(y_data) || S7::S7_inherits(y_data, S7::S7_object)) {
    y_data <- list(y_data)
  }

  output_units <- match.arg(output_units, c("m3", "Ml", "Gl"))

  if (is.null(weights_x)) weights_x <- rep(1, length(x_data))
  if (is.null(weights_y)) weights_y <- rep(1, length(y_data))

  if (length(weights_x) != length(x_data)) {
    stop("`weights_x` must have the same length as `x_data`.")
  }
  if (length(weights_y) != length(y_data)) {
    stop("`weights_y` must have the same length as `y_data`.")
  }

  # Validate optional PE object
  pe_daily <- NULL
  if (!is.null(pe)) {
    if (!S7::S7_inherits(pe, PotEvapData)) {
      stop("`pe` must be a PotEvapData object (e.g. PotEvap_Daily).")
    }
    pe_daily <- data.table::copy(pe@readings)
  }

  # -- Build combined volume series for each axis
  x_series <- .combine_series(x_data, weights_x, catchment_area_km2,
                              output_units, pe_daily)
  y_series <- .combine_series(y_data, weights_y, catchment_area_km2,
                              output_units, pe_daily)

  # -- Align to the overlapping time window (inner join)
  aligned <- merge(x_series, y_series, by = "dateTime")
  data.table::setnames(aligned, c("value.x", "value.y"), c("x_val", "y_val"))
  aligned <- aligned[!is.na(x_val) & !is.na(y_val)]
  data.table::setorder(aligned, dateTime)

  if (nrow(aligned) == 0L) {
    stop("No overlapping timestamps between X and Y data sources.")
  }

  aligned[, cum_x := cumsum_na(x_val)]
  aligned[, cum_y := cumsum_na(y_val)]

  # -- Auto labels and subtitle
  if (is.null(label_x)) label_x <- .axis_label(x_data, output_units)
  if (is.null(label_y)) label_y <- .axis_label(y_data, output_units)

  if (is.null(subtitle)) {
    dr      <- range(aligned$dateTime)
    subtitle <- sprintf("Data from %s to %s",
                        format(dr[1L], "%Y-%m-%d"),
                        format(dr[2L], "%Y-%m-%d"))
  }

  # Reference lines: 1:1 plus ±(final cumulative difference)
  final_diff <- aligned$cum_y[nrow(aligned)] - aligned$cum_x[nrow(aligned)]

  # -- Build plot
  p <- ggplot2::ggplot(data    = aligned,
                       mapping = ggplot2::aes(x = cum_x, y = cum_y)) +
    ggplot2::theme_bw() +
    ggplot2::geom_abline(
      intercept = c(0, final_diff, -final_diff),
      slope     = 1,
      colour    = c("#555555", "#AAAAAA", "#AAAAAA"),
      linetype  = "dashed",
      linewidth = 0.8
    ) +
    ggplot2::geom_line(colour = "#008531", linewidth = 1.25) +
    ggplot2::labs(
      title    = title,
      subtitle = subtitle,
      x        = label_x,
      y        = label_y
    ) +
    ggplot2::theme(
      plot.title    = ggplot2::element_text(face   = "bold",
                                            colour = "#008531",
                                            size   = 14),
      plot.subtitle = ggplot2::element_text(size   = 10,
                                            colour = "#555555")
    )

  attr(p, "data") <- aligned
  p
}
