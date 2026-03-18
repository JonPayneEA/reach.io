# ============================================================
# Tool:        S7 Hydrometric Data Classes
# Description: S7 class hierarchy for typed hydrometric
#              reading objects. Six concrete classes encode
#              parameter type and timestep in the class name.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        2
# Inputs:      data.table of readings from download_hydrology()
# Outputs:     Typed S7 objects (Rainfall_Daily etc.)
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

#' @import S7
NULL


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

S7::method(print, HydroData) <- function(x, ...) {
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
