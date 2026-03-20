# ============================================================
# Tool:        S7 Hydrometric & Potential Evaporation Classes
# Description: S7 class hierarchy for typed hydrometric
#              reading objects and potential evaporation (PE)
#              objects. Six concrete HydroData classes encode
#              parameter type and timestep in the class name;
#              three concrete PotEvapData classes cover daily,
#              hourly, and 15-minute PE readings.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-03-19 - JP: merged PotEvapData classes
# Tier:        2
# Inputs:      data.table of readings from download_hydrology()
#              or an external PE source (e.g. CEH CHESS-PE)
# Outputs:     Typed S7 objects (Rainfall_Daily, PotEvap_Daily
#              etc.)
# Dependencies: S7, data.table
# ============================================================

# -- S7 classes for hydrometric data ------------------------------------------
#
# Six concrete classes cover every parameter/timestep combination:
#   Rainfall_Daily, Rainfall_15min
#   Flow_Daily,     Flow_15min
#   Level_Daily,    Level_15min
#
# All inherit from the internal abstract parent HydroData, which holds the
# shared properties and validator. The timestep is encoded in the class name
# so downstream code can dispatch on it directly rather than inspecting the
# period_name slot.
#
# All six can be coerced back to a plain data.table via as_data_table(), and
# a combined format with a parameter column is available via as_long().
#
# -- S7 classes for potential evaporation data --------------------------------
#
# Three concrete classes cover every supported timestep:
#   PotEvap_Daily   - daily   PE sourced directly
#   PotEvap_Hourly  - hourly  PE sourced directly
#   PotEvap_15min   - 15-min  PE calculated via disagg_to_15min()
#
# All inherit from the internal abstract parent PotEvapData, which holds
# shared properties and validation logic.  The 15-min class adds two extra
# slots that record how it was derived: `is_calculated` (always TRUE) and
# `disagg_method` ("uniform_hourly" or "uniform_daily").
#
# Helper:
#   disagg_to_15min(x)  - converts PotEvap_Daily or PotEvap_Hourly to
#                         PotEvap_15min by uniform disaggregation.

#' @import S7
NULL


# =============================================================================
# HydroData
# =============================================================================

# -- Abstract parent ----------------------------------------------------------

#' Abstract base class for hydrometric reading data
#'
#' Not exported. Holds the properties and validator shared by all six
#' concrete classes. Every concrete class inherits from this.
#'
#' @slot readings A `data.table` of readings. Must contain columns
#'   `dateTime` (POSIXct), `date` (Date), `value` (numeric), and
#'   `measure_notation` (character).
#' @slot parameter Character scalar. One of `"rainfall"`, `"flow"`,
#'   `"level"`.
#' @slot period_name Character scalar. `"daily"` or `"15min"`.
#' @slot from_date Character scalar. Start of the requested date range
#'   (YYYY-MM-DD).
#' @slot to_date Character scalar. End of the requested date range
#'   (YYYY-MM-DD).
#' @slot n_measures Integer. Number of distinct measure notations present.
#' @slot n_rows Integer. Total reading rows.
#' @slot downloaded_at POSIXct. When the object was constructed.
#'
#' @noRd
HydroData <- S7::new_class(
  "HydroData",
  abstract   = TRUE,
  package    = "reach.io",
  properties = list(
    readings      = S7::new_property(class = S7::class_any),
    parameter     = S7::new_property(class = S7::class_character),
    period_name   = S7::new_property(class = S7::class_character),
    from_date     = S7::new_property(class = S7::class_character),
    to_date       = S7::new_property(class = S7::class_character),
    n_measures    = S7::new_property(class = S7::class_integer),
    n_rows        = S7::new_property(class = S7::class_integer),
    downloaded_at = S7::new_property(class = S7::class_any)
  ),
  validator = function(self) {
    dt <- self@readings

    if (!inherits(dt, "data.table")) {
      return("`readings` must be a data.table.")
    }

    required <- c("dateTime", "date", "value", "measure_notation")
    missing  <- setdiff(required, names(dt))
    if (length(missing) > 0) {
      return(sprintf("`readings` is missing column(s): %s.",
                     paste(missing, collapse = ", ")))
    }

    if (!inherits(dt$dateTime, "POSIXct")) {
      return("`readings$dateTime` must be POSIXct.")
    }
    if (!inherits(dt$date, "Date")) {
      return("`readings$date` must be Date.")
    }
    if (!is.numeric(dt$value)) {
      return("`readings$value` must be numeric.")
    }
    if (!is.character(dt$measure_notation)) {
      return("`readings$measure_notation` must be character.")
    }
    if (length(self@parameter) != 1L) {
      return("`parameter` must be length 1.")
    }
    if (length(self@period_name) != 1L) {
      return("`period_name` must be length 1.")
    }

    NULL
  }
)


# -- Internal constructor helper ----------------------------------------------
#
# All six classes share identical constructor logic. This helper avoids
# repeating it six times.

#' @noRd
.new_hydro_class <- function(class_name, parameter, period_name_fixed) {
  S7::new_class(
    class_name,
    package = "reach.io",
    parent  = HydroData,
    constructor = function(readings,
                           from_date = NA_character_,
                           to_date   = NA_character_) {
      S7::new_object(
        S7::S7_object(),
        readings      = data.table::as.data.table(readings),
        parameter     = parameter,
        period_name   = period_name_fixed,
        from_date     = from_date,
        to_date       = to_date,
        n_measures    = data.table::uniqueN(readings$measure_notation),
        n_rows        = nrow(readings),
        downloaded_at = Sys.time()
      )
    }
  )
}


# -- Concrete classes ---------------------------------------------------------

#' S7 class for daily rainfall readings
#'
#' Wraps daily cumulative rainfall readings from [download_hydrology()].
#' Typical units: mm. One row per day per measure.
#'
#' @slot readings `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, mm), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @slot parameter Always `"rainfall"`.
#' @slot period_name Always `"daily"`.
#' @slot from_date,to_date Requested date range (YYYY-MM-DD).
#' @slot n_measures Number of distinct measure notations.
#' @slot n_rows Total reading rows.
#' @slot downloaded_at POSIXct timestamp of object construction.
#'
#' @export
Rainfall_Daily <- .new_hydro_class("Rainfall_Daily", "rainfall", "daily")

#' S7 class for 15-minute rainfall readings
#'
#' Wraps 15-minute cumulative rainfall readings from [download_hydrology()].
#' Typical units: mm. One row per 15-minute interval per measure.
#'
#' @slot readings `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, mm), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @slot parameter Always `"rainfall"`.
#' @slot period_name Always `"15min"`.
#' @slot from_date,to_date Requested date range (YYYY-MM-DD).
#' @slot n_measures Number of distinct measure notations.
#' @slot n_rows Total reading rows.
#' @slot downloaded_at POSIXct timestamp of object construction.
#'
#' @export
Rainfall_15min <- .new_hydro_class("Rainfall_15min", "rainfall", "15min")

#' S7 class for daily river flow readings
#'
#' Wraps daily mean flow readings from [download_hydrology()].
#' Typical units: m3/s. One row per day per measure.
#'
#' @slot readings `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, m3/s), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @slot parameter Always `"flow"`.
#' @slot period_name Always `"daily"`.
#' @slot from_date,to_date Requested date range (YYYY-MM-DD).
#' @slot n_measures Number of distinct measure notations.
#' @slot n_rows Total reading rows.
#' @slot downloaded_at POSIXct timestamp of object construction.
#'
#' @export
Flow_Daily <- .new_hydro_class("Flow_Daily", "flow", "daily")

#' S7 class for 15-minute river flow readings
#'
#' Wraps 15-minute flow readings from [download_hydrology()].
#' Typical units: m3/s. One row per 15-minute interval per measure.
#'
#' @slot readings `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, m3/s), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @slot parameter Always `"flow"`.
#' @slot period_name Always `"15min"`.
#' @slot from_date,to_date Requested date range (YYYY-MM-DD).
#' @slot n_measures Number of distinct measure notations.
#' @slot n_rows Total reading rows.
#' @slot downloaded_at POSIXct timestamp of object construction.
#'
#' @export
Flow_15min <- .new_hydro_class("Flow_15min", "flow", "15min")

#' S7 class for daily water level readings
#'
#' Wraps daily water level readings from [download_hydrology()].
#' Typical units: mAOD or mASD. Multiple value types (min, max,
#' instantaneous) may be present depending on the `value_type` argument
#' used when downloading.
#'
#' @slot readings `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, m), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @slot parameter Always `"level"`.
#' @slot period_name Always `"daily"`.
#' @slot from_date,to_date Requested date range (YYYY-MM-DD).
#' @slot n_measures Number of distinct measure notations.
#' @slot n_rows Total reading rows.
#' @slot downloaded_at POSIXct timestamp of object construction.
#'
#' @export
Level_Daily <- .new_hydro_class("Level_Daily", "level", "daily")

#' S7 class for 15-minute water level readings
#'
#' Wraps 15-minute water level readings from [download_hydrology()].
#' Typical units: mAOD or mASD. One row per 15-minute interval per measure.
#'
#' @slot readings `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, m), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @slot parameter Always `"level"`.
#' @slot period_name Always `"15min"`.
#' @slot from_date,to_date Requested date range (YYYY-MM-DD).
#' @slot n_measures Number of distinct measure notations.
#' @slot n_rows Total reading rows.
#' @slot downloaded_at POSIXct timestamp of object construction.
#'
#' @export
Level_15min <- .new_hydro_class("Level_15min", "level", "15min")


# -- Constructor lookup -------------------------------------------------------

# Internal: map parameter + period_name to the correct constructor.
# Used by download_hydrology() to wrap output without a switch() block.
#' @noRd
HYDRO_CLASS <- list(
  rainfall = list(daily  = Rainfall_Daily,
                  `15min` = Rainfall_15min),
  flow     = list(daily  = Flow_Daily,
                  `15min` = Flow_15min),
  level    = list(daily  = Level_Daily,
                  `15min` = Level_15min)
)


# -- Generics and methods -----------------------------------------------------

#' Extract the readings data.table from a HydroData object
#'
#' Returns the inner `data.table` from any `HydroData`-derived object for
#' direct use with `data.table` or `arrow` without touching `@readings`.
#'
#' @param x A [Rainfall_Daily], [Rainfall_15min], [Flow_Daily], [Flow_15min],
#'   [Level_Daily], or [Level_15min] object.
#'
#' @return A `data.table` of readings.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' result <- download_hydrology("flow", "2022-01-01", "2022-12-31",
#'                              wiski_ids = "SS92F014")
#' dt <- as_data_table(result$flow)
#' }
as_data_table <- S7::new_generic("as_data_table", "x")

S7::method(as_data_table, HydroData) <- function(x) x@readings


#' Reshape a HydroData object to long format
#'
#' Adds a `parameter` column and reorders so results from multiple
#' parameter objects can be combined with `rbind()`.
#'
#' @param x A [Rainfall_Daily], [Rainfall_15min], [Flow_Daily], [Flow_15min],
#'   [Level_Daily], or [Level_15min] object.
#'
#' @return A `data.table` with columns `parameter`, `measure_notation`,
#'   `date`, `dateTime`, `value`, and any other columns present in the
#'   readings (e.g. `quality`, `completeness`).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' result <- download_hydrology(c("flow", "level"), "2022-01-01", "2022-12-31",
#'                              wiski_ids = "SS92F014")
#' combined <- rbind(as_long(result$flow), as_long(result$level))
#' }
as_long <- S7::new_generic("as_long", "x")

S7::method(as_long, HydroData) <- function(x) {
  dt <- data.table::copy(x@readings)
  dt[, parameter := x@parameter]
  data.table::setcolorder(dt, c("parameter", "measure_notation",
                                "date", "dateTime", "value"))
  dt
}


# -- Print method -------------------------------------------------------------
#
# One method on the abstract parent covers all six classes since the print
# output is identical in structure — the class name already carries the
# parameter and timestep information.

.print_HydroData <- function(x, ...) {
  cat(sprintf(
    "<%s>\n  Date range:  %s to %s\n  Measures:    %d\n  Rows:        %s\n  Downloaded:  %s\n",
    class(x)[1L],
    x@from_date, x@to_date,
    x@n_measures,
    format(x@n_rows, big.mark = ","),
    format(x@downloaded_at, "%Y-%m-%d %H:%M:%S %Z")
  ))
  invisible(x)
}


# =============================================================================
# PotEvapData
# =============================================================================

# -- Abstract parent ----------------------------------------------------------

#' Abstract base class for potential evaporation data
#'
#' Not exported. Holds the properties and validator shared by all three
#' concrete PE classes. Every concrete class inherits from this.
#'
#' @slot readings A `data.table` of readings. Must contain columns
#'   `dateTime` (POSIXct), `date` (Date), and `value` (numeric).
#' @slot period_name Character scalar. `"daily"`, `"hourly"`, or `"15min"`.
#' @slot source_name Character scalar. Identifies the data origin (e.g.
#'   `"CHESS-PE"`, `"MORECS"`, `"bespoke"`). Free-text; used for
#'   provenance tracking and printing.
#' @slot from_date Character scalar. Start of the data range (YYYY-MM-DD).
#' @slot to_date Character scalar. End of the data range (YYYY-MM-DD).
#' @slot n_rows Integer. Total reading rows.
#' @slot created_at POSIXct. When the object was constructed.
#'
#' @noRd
PotEvapData <- S7::new_class(
  "PotEvapData",
  abstract   = TRUE,
  package    = "reach.io",
  properties = list(
    readings    = S7::new_property(class = S7::class_any),
    period_name = S7::new_property(class = S7::class_character),
    source_name = S7::new_property(class = S7::class_character),
    from_date   = S7::new_property(class = S7::class_character),
    to_date     = S7::new_property(class = S7::class_character),
    n_rows      = S7::new_property(class = S7::class_integer),
    created_at  = S7::new_property(class = S7::class_any)
  ),
  validator = function(self) {
    dt <- self@readings

    if (!inherits(dt, "data.table")) {
      return("`readings` must be a data.table.")
    }

    required <- c("dateTime", "date", "value")
    missing  <- setdiff(required, names(dt))
    if (length(missing) > 0L) {
      return(sprintf("`readings` is missing column(s): %s.",
                     paste(missing, collapse = ", ")))
    }

    if (!inherits(dt$dateTime, "POSIXct")) {
      return("`readings$dateTime` must be POSIXct.")
    }
    if (!inherits(dt$date, "Date")) {
      return("`readings$date` must be Date.")
    }
    if (!is.numeric(dt$value)) {
      return("`readings$value` must be numeric.")
    }
    if (length(self@period_name) != 1L ||
        !self@period_name %in% c("daily", "hourly", "15min")) {
      return('`period_name` must be one of "daily", "hourly", "15min".')
    }
    if (length(self@source_name) != 1L) {
      return("`source_name` must be a length-1 character scalar.")
    }

    NULL
  }
)


# -- Internal constructor helper ----------------------------------------------

#' @noRd
.new_pe_class <- function(class_name, period_name_fixed, extra_props = list()) {
  S7::new_class(
    class_name,
    package    = "reach.io",
    parent     = PotEvapData,
    properties = extra_props,
    constructor = function(readings,
                           source_name = NA_character_,
                           from_date   = NA_character_,
                           to_date     = NA_character_) {
      dt <- data.table::as.data.table(readings)
      S7::new_object(
        S7::S7_object(),
        readings    = dt,
        period_name = period_name_fixed,
        source_name = source_name,
        from_date   = from_date,
        to_date     = to_date,
        n_rows      = nrow(dt),
        created_at  = Sys.time()
      )
    }
  )
}


# -- Concrete classes ---------------------------------------------------------

#' S7 class for daily potential evaporation readings
#'
#' Wraps daily PE values sourced directly from an external PE dataset
#' (e.g. CEH CHESS-PE, MORECS). Units are typically mm/day.
#'
#' @slot readings `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, mm/day), and optionally any
#'   additional columns present in the source (e.g. `quality`).
#' @slot period_name Always `"daily"`.
#' @slot source_name Character. Origin of the PE data.
#' @slot from_date,to_date Data range (YYYY-MM-DD).
#' @slot n_rows Total reading rows.
#' @slot created_at POSIXct timestamp of object construction.
#'
#' @export
PotEvap_Daily <- .new_pe_class("PotEvap_Daily", "daily")

#' S7 class for hourly potential evaporation readings
#'
#' Wraps hourly PE values sourced directly from an external PE dataset.
#' Units are typically mm/hr. One row per hour.
#'
#' @slot readings `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, mm/hr), and optionally any
#'   additional columns present in the source.
#' @slot period_name Always `"hourly"`.
#' @slot source_name Character. Origin of the PE data.
#' @slot from_date,to_date Data range (YYYY-MM-DD).
#' @slot n_rows Total reading rows.
#' @slot created_at POSIXct timestamp of object construction.
#'
#' @export
PotEvap_Hourly <- .new_pe_class("PotEvap_Hourly", "hourly")

#' S7 class for 15-minute potential evaporation (calculated)
#'
#' Wraps 15-minute PE values that have been **disaggregated** from a
#' coarser timestep via [disagg_to_15min()]. This class is not intended
#' to be constructed directly; use `disagg_to_15min()` instead.
#'
#' @slot readings `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, mm/15min).
#' @slot period_name Always `"15min"`.
#' @slot source_name Character. Inherited from the parent object.
#' @slot from_date,to_date Data range (YYYY-MM-DD).
#' @slot n_rows Total reading rows.
#' @slot created_at POSIXct timestamp of object construction.
#' @slot is_calculated Logical. Always `TRUE` — 15-min PE is never
#'   sourced directly; it is always derived.
#' @slot disagg_method Character. How disaggregation was performed.
#'   One of `"uniform_hourly"` (each hourly value split into four equal
#'   15-min intervals) or `"uniform_daily"` (each daily value split into
#'   96 equal 15-min intervals).
#'
#' @export
PotEvap_15min <- S7::new_class(
  "PotEvap_15min",
  package    = "reach.io",
  parent     = PotEvapData,
  properties = list(
    is_calculated = S7::new_property(class = S7::class_logical),
    disagg_method = S7::new_property(class = S7::class_character)
  ),
  constructor = function(readings,
                         source_name   = NA_character_,
                         from_date     = NA_character_,
                         to_date       = NA_character_,
                         disagg_method = NA_character_) {
    dt <- data.table::as.data.table(readings)
    S7::new_object(
      S7::S7_object(),
      readings      = dt,
      period_name   = "15min",
      source_name   = source_name,
      from_date     = from_date,
      to_date       = to_date,
      n_rows        = nrow(dt),
      created_at    = Sys.time(),
      is_calculated = TRUE,
      disagg_method = disagg_method
    )
  },
  validator = function(self) {
    if (!self@is_calculated) {
      return("`is_calculated` must be TRUE for PotEvap_15min.")
    }
    valid_methods <- c("uniform_hourly", "uniform_daily")
    if (!self@disagg_method %in% valid_methods) {
      return(sprintf('`disagg_method` must be one of: %s.',
                     paste(valid_methods, collapse = ", ")))
    }
    NULL
  }
)


# -- Disaggregation helper ----------------------------------------------------

#' Disaggregate potential evaporation to 15-minute intervals
#'
#' Converts a [PotEvap_Daily] or [PotEvap_Hourly] object to a [PotEvap_15min]
#' object using uniform disaggregation:
#'
#' * **From hourly**: each hourly value is divided equally across four
#'   consecutive 15-minute intervals (method `"uniform_hourly"`).
#' * **From daily**: each daily value is divided equally across 96
#'   consecutive 15-minute intervals (method `"uniform_daily"`).
#'
#' Values are assumed to represent totals (e.g. mm) and are split
#' proportionally. If your values are rates (mm/hr), convert to totals
#' first.
#'
#' @param x A [PotEvap_Daily] or [PotEvap_Hourly] object.
#'
#' @return A [PotEvap_15min] object whose readings contain columns
#'   `dateTime` (POSIXct), `date` (Date), and `value` (numeric).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' pe_hr  <- PotEvap_Hourly(readings = hourly_dt, source_name = "CHESS-PE")
#' pe_15m <- disagg_to_15min(pe_hr)
#' }
disagg_to_15min <- S7::new_generic("disagg_to_15min", "x")

S7::method(disagg_to_15min, PotEvap_Hourly) <- function(x) {
  dt <- data.table::copy(x@readings)

  offsets_sec <- c(0L, 900L, 1800L, 2700L)
  idx  <- rep(seq_len(nrow(dt)), each = 4L)
  rows <- dt[idx]
  rows[, dateTime := dateTime + offsets_sec]
  rows[, date     := as.Date(dateTime)]
  rows[, value    := value / 4]

  PotEvap_15min(
    readings      = rows,
    source_name   = x@source_name,
    from_date     = x@from_date,
    to_date       = x@to_date,
    disagg_method = "uniform_hourly"
  )
}

S7::method(disagg_to_15min, PotEvap_Daily) <- function(x) {
  dt  <- data.table::copy(x@readings)
  tz  <- attr(dt$dateTime, "tzone")
  tz  <- if (is.null(tz) || !nzchar(tz)) "UTC" else tz

  offsets_sec <- as.integer(seq(0L, by = 900L, length.out = 96L))
  idx  <- rep(seq_len(nrow(dt)), each = 96L)
  rows <- dt[idx]

  rows[, dateTime := as.POSIXct(date, tz = tz) + offsets_sec]
  rows[, date     := as.Date(dateTime)]
  rows[, value    := value / 96]

  PotEvap_15min(
    readings      = rows,
    source_name   = x@source_name,
    from_date     = x@from_date,
    to_date       = x@to_date,
    disagg_method = "uniform_daily"
  )
}


# -- Generics and methods -----------------------------------------------------

#' @noRd
S7::method(as_data_table, PotEvapData) <- function(x) x@readings


# -- Print method -------------------------------------------------------------

.print_PotEvapData <- function(x, ...) {
  calc_tag <- if (S7::S7_inherits(x, PotEvap_15min)) {
    sprintf("  Disagg method: %s\n", x@disagg_method)
  } else {
    ""
  }

  cat(sprintf(
    "<%s>\n  Source:      %s\n  Date range:  %s to %s\n  Rows:        %s\n%s  Created:     %s\n",
    class(x)[1L],
    x@source_name,
    x@from_date, x@to_date,
    format(x@n_rows, big.mark = ","),
    calc_tag,
    format(x@created_at, "%Y-%m-%d %H:%M:%S %Z")
  ))
  invisible(x)
}
