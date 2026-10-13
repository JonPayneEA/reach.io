# ============================================================
# Tool:        S7 Hydrometric & Potential Evaporation Classes
# Description: S7 class hierarchy for typed hydrometric
#              reading objects and potential evaporation (PE)
#              objects. Six concrete FlodeHydroData classes encode
#              parameter type and timestep in the class name;
#              three concrete FlodePotEvapData classes cover daily,
#              hourly, and 15-minute PE readings.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-03-19 - JP: merged FlodePotEvapData classes
# Tier:        2
# Inputs:      data.table of readings from download_hydrology()
#              or an external PE source (e.g. CEH CHESS-PE)
# Outputs:     Typed S7 objects (FlodeRainfall_Daily, FlodePotEvap_Daily
#              etc.)
# Dependencies: S7, data.table
# ============================================================

# -- S7 classes for hydrometric data ------------------------------------------
#
# Six concrete classes cover every parameter/timestep combination:
#   FlodeRainfall_Daily, FlodeRainfall_15min
#   FlodeFlow_Daily,     FlodeFlow_15min
#   FlodeLevel_Daily,    FlodeLevel_15min
#
# All inherit from the internal abstract parent FlodeHydroData, which holds the
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
#   FlodePotEvap_Daily   - daily   PE sourced directly
#   FlodePotEvap_Hourly  - hourly  PE sourced directly
#   FlodePotEvap_15min   - 15-min  PE calculated via disagg_to_15min()
#
# All inherit from the internal abstract parent FlodePotEvapData, which holds
# shared properties and validation logic.  The 15-min class adds two extra
# slots that record how it was derived: `is_calculated` (always TRUE) and
# `disagg_method` ("uniform_hourly" or "uniform_daily").
#
# Helper:
#   disagg_to_15min(x)  - converts FlodePotEvap_Daily or FlodePotEvap_Hourly to
#                         FlodePotEvap_15min by uniform disaggregation.

#' @import S7
NULL


# =============================================================================
# FlodeHydroData
# =============================================================================

# -- Abstract parent ----------------------------------------------------------

#' Abstract base class for hydrometric reading data
#'
#' Not exported. Holds the properties and validator shared by all six
#' concrete classes. Every concrete class inherits from this.
#'
#' @prop readings A `data.table` of readings. Must contain columns
#'   `dateTime` (POSIXct), `date` (Date), `value` (numeric), and
#'   `measure_notation` (character).
#' @prop parameter Character scalar. One of `"rainfall"`, `"flow"`,
#'   `"level"`.
#' @prop period_name Character scalar. `"daily"` or `"15min"`.
#' @prop from_date Character scalar. Start of the requested date range
#'   (YYYY-MM-DD).
#' @prop to_date Character scalar. End of the requested date range
#'   (YYYY-MM-DD).
#' @prop n_measures Integer. Number of distinct measure notations present.
#' @prop n_rows Integer. Total reading rows.
#' @prop downloaded_at POSIXct. When the object was constructed.
#'
#' @noRd
FlodeHydroData <- S7::new_class(
  "FlodeHydroData",
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

    # -- Structure -----------------------------------------------------------
    # Normally .as_readings_dt() has already rejected a non-table; this also
    # guards a value assigned later with `@<-`.
    if (!inherits(dt, "data.table")) {
      return("`readings` must be a data.table.")
    }

    required <- c("dateTime", "date", "value", "measure_notation")
    missing  <- setdiff(required, names(dt))
    if (length(missing) > 0) {
      return(sprintf("`readings` is missing column(s): %s.",
                     paste(missing, collapse = ", ")))
    }

    # -- Column types --------------------------------------------------------
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
    # -- Scalar metadata -----------------------------------------------------
    # Only the lengths are checked, not the values (the concrete class fixes
    # them). n_rows and n_measures are computed once at construction and are not
    # recomputed or checked if `readings` is replaced later.
    if (length(self@parameter) != 1L) {
      return("`parameter` must be length 1.")
    }
    if (length(self@period_name) != 1L) {
      return("`period_name` must be length 1.")
    }

    NULL
  }
)


# Coerce constructor input to a data.table, failing early (and clearly) for
# anything that is not tabular. Doing this before any metadata is computed
# means a bad `readings` produces this message rather than an unrelated
# "n_rows must be <integer>" error from the S7 property checks.
#' @noRd
.as_readings_dt <- function(readings) {
  if (!is.data.frame(readings)) {
    stop("`readings` must be a data.table (or data.frame).", call. = FALSE)
  }
  data.table::as.data.table(readings)
}


# -- Internal constructor helper ----------------------------------------------
#
# All six classes share identical constructor logic. This helper avoids
# repeating it six times.

#' @noRd
.new_hydro_class <- function(class_name, parameter, period_name_fixed) {
  S7::new_class(
    class_name,
    package = "reach.io",
    parent  = FlodeHydroData,
    constructor = function(readings,
                           from_date = NA_character_,
                           to_date   = NA_character_) {
      readings <- .as_readings_dt(readings)
      # parameter and period_name are fixed by the class, not arguments.
      # n_rows, n_measures and downloaded_at are derived here from the coerced
      # table; downloaded_at is the construction time, not the original
      # download time. S7 validates the object when new_object() returns.
      S7::new_object(
        S7::S7_object(),
        readings      = readings,
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
#' @details
#' The constructor first checks that `readings` is a table (anything else is an
#' immediate error), then fills in the properties: `parameter` and
#' `period_name` are fixed by the class (they are not arguments), and `n_rows`,
#' `n_measures` and `downloaded_at` are computed at construction.
#' `downloaded_at` is the time the object was built, not the original download
#' time, so an object read back with [read_bronze()] carries the read time.
#'
#' Validation requires columns `dateTime` (`POSIXct`), `date` (`Date`), `value`
#' (numeric) and `measure_notation` (character); extra columns such as
#' `quality` are kept untouched. It does not check that `date` agrees with
#' `dateTime`, that rows are sorted or unique, that the spacing matches the
#' period, or the units. `n_rows` and `n_measures` are not recomputed if
#' `@readings` is replaced later. All six hydrometric classes share these
#' rules.
#'
#' @param readings A `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, mm), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @param from_date,to_date Requested date range (YYYY-MM-DD). Default `NA`.
#' @prop parameter Always `"rainfall"`.
#' @prop period_name Always `"daily"`.
#' @prop n_measures Number of distinct measure notations.
#' @prop n_rows Total reading rows.
#' @prop downloaded_at POSIXct timestamp of object construction.
#'
#' @returns A `FlodeRainfall_Daily` S7 object.
#' @export
FlodeRainfall_Daily <- .new_hydro_class("FlodeRainfall_Daily", "rainfall", "daily")

#' S7 class for 15-minute rainfall readings
#'
#' Wraps 15-minute cumulative rainfall readings from [download_hydrology()].
#' Typical units: mm. One row per 15-minute interval per measure.
#'
#' @details
#' Construction and validation are as for [FlodeRainfall_Daily]. Nothing checks
#' that the rows really are 15 minutes apart: the class only labels
#' `period_name`.
#'
#' @param readings A `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, mm), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @param from_date,to_date Requested date range (YYYY-MM-DD). Default `NA`.
#' @prop parameter Always `"rainfall"`.
#' @prop period_name Always `"15min"`.
#' @prop n_measures Number of distinct measure notations.
#' @prop n_rows Total reading rows.
#' @prop downloaded_at POSIXct timestamp of object construction.
#'
#' @returns A `FlodeRainfall_15min` S7 object.
#' @export
FlodeRainfall_15min <- .new_hydro_class("FlodeRainfall_15min", "rainfall", "15min")

#' S7 class for daily river flow readings
#'
#' Wraps daily mean flow readings from [download_hydrology()].
#' Typical units: m3/s. One row per day per measure.
#'
#' @details
#' Construction and validation are as for [FlodeRainfall_Daily]. Rated flow from
#' [apply_rating()] also carries a `doubtful` column, which is kept (extra
#' columns are not validated); [apply_inverse_rating()] converts flow back to
#' level.
#'
#' @param readings A `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, m3/s), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @param from_date,to_date Requested date range (YYYY-MM-DD). Default `NA`.
#' @prop parameter Always `"flow"`.
#' @prop period_name Always `"daily"`.
#' @prop n_measures Number of distinct measure notations.
#' @prop n_rows Total reading rows.
#' @prop downloaded_at POSIXct timestamp of object construction.
#'
#' @returns A `FlodeFlow_Daily` S7 object.
#' @export
FlodeFlow_Daily <- .new_hydro_class("FlodeFlow_Daily", "flow", "daily")

#' S7 class for 15-minute river flow readings
#'
#' Wraps 15-minute flow readings from [download_hydrology()].
#' Typical units: m3/s. One row per 15-minute interval per measure.
#'
#' @details
#' Construction and validation are as for [FlodeRainfall_Daily]. Nothing checks
#' that the rows really are 15 minutes apart. Rated flow from [apply_rating()]
#' also carries a `doubtful` column, which is kept.
#'
#' @param readings A `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, m3/s), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @param from_date,to_date Requested date range (YYYY-MM-DD). Default `NA`.
#' @prop parameter Always `"flow"`.
#' @prop period_name Always `"15min"`.
#' @prop n_measures Number of distinct measure notations.
#' @prop n_rows Total reading rows.
#' @prop downloaded_at POSIXct timestamp of object construction.
#'
#' @returns A `FlodeFlow_15min` S7 object.
#' @export
FlodeFlow_15min <- .new_hydro_class("FlodeFlow_15min", "flow", "15min")

#' S7 class for daily water level readings
#'
#' Wraps daily water level readings from [download_hydrology()].
#' Typical units: mAOD or mASD. Multiple value types (min, max,
#' instantaneous) may be present depending on the `value_type` argument
#' used when downloading.
#'
#' @details
#' Construction and validation are as for [FlodeRainfall_Daily]. Stage is
#' stored as supplied: the vertical datum is neither recorded nor checked. Use
#' [apply_rating()] to convert to flow.
#'
#' @param readings A `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, m), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @param from_date,to_date Requested date range (YYYY-MM-DD). Default `NA`.
#' @prop parameter Always `"level"`.
#' @prop period_name Always `"daily"`.
#' @prop n_measures Number of distinct measure notations.
#' @prop n_rows Total reading rows.
#' @prop downloaded_at POSIXct timestamp of object construction.
#'
#' @returns A `FlodeLevel_Daily` S7 object.
#' @export
FlodeLevel_Daily <- .new_hydro_class("FlodeLevel_Daily", "level", "daily")

#' S7 class for 15-minute water level readings
#'
#' Wraps 15-minute water level readings from [download_hydrology()].
#' Typical units: mAOD or mASD. One row per 15-minute interval per measure.
#'
#' @details
#' Construction and validation are as for [FlodeRainfall_Daily]. Nothing checks
#' that the rows really are 15 minutes apart, and the vertical datum is neither
#' recorded nor checked. Use [apply_rating()] to convert to flow.
#'
#' @param readings A `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, m), `measure_notation` (character),
#'   and optionally `quality` and `completeness`.
#' @param from_date,to_date Requested date range (YYYY-MM-DD). Default `NA`.
#' @prop parameter Always `"level"`.
#' @prop period_name Always `"15min"`.
#' @prop n_measures Number of distinct measure notations.
#' @prop n_rows Total reading rows.
#' @prop downloaded_at POSIXct timestamp of object construction.
#'
#' @returns A `FlodeLevel_15min` S7 object.
#' @export
FlodeLevel_15min <- .new_hydro_class("FlodeLevel_15min", "level", "15min")


# -- Constructor lookup -------------------------------------------------------

# Internal: map parameter + period_name to the correct constructor.
# Used by download_hydrology() to wrap output without a switch() block.
# Keys are the parameter ("rainfall", "flow", "level") and the period_name
# ("daily", "15min") of the class. Also used by read_bronze() and read_silver()
# to wrap stored data.
#' @noRd
HYDRO_CLASS <- list(
  rainfall = list(daily  = FlodeRainfall_Daily,
                  `15min` = FlodeRainfall_15min),
  flow     = list(daily  = FlodeFlow_Daily,
                  `15min` = FlodeFlow_15min),
  level    = list(daily  = FlodeLevel_Daily,
                  `15min` = FlodeLevel_15min)
)


# -- Generics and methods -----------------------------------------------------

#' Extract the readings data.table from a FlodeHydroData object
#'
#' `as_data_table()` is an S7 generic. Returns the inner `data.table` from
#' any `FlodeHydroData`- or `FlodePotEvapData`-derived object for direct use with
#' `data.table` or `arrow` without touching `@readings`.
#'
#' @details
#' The table returned is the one stored in the object, not a copy, so changing
#' it by reference (`:=`, `set()`, `setorder()`) also changes the object. Take
#' `data.table::copy()` first if the object must stay intact; [as_long()]
#' returns a copy. Methods exist for the hydrometric and potential evaporation
#' classes only.
#'
#' @param x A [FlodeRainfall_Daily], [FlodeRainfall_15min], [FlodeFlow_Daily], [FlodeFlow_15min],
#'   [FlodeLevel_Daily], [FlodeLevel_15min], [FlodePotEvap_Daily], [FlodePotEvap_Hourly], or
#'   [FlodePotEvap_15min] object.
#' @param ... Additional arguments for methods (currently unused).
#'
#' @returns A `data.table` of readings.
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

S7::method(as_data_table, FlodeHydroData) <- function(x) x@readings


#' Reshape a FlodeHydroData object to long format
#'
#' `as_long()` is an S7 generic. Adds a `parameter` column and reorders so
#' results from multiple parameter objects can be combined with `rbind()`.
#'
#' @details
#' Works on a copy, so the object is unchanged. The `parameter` column holds
#' `"rainfall"`, `"flow"` or `"level"`. The first five columns are `parameter`,
#' `measure_notation`, `date`, `dateTime` and `value`; any other columns follow
#' in their original order. Binding results for different parameters with
#' `rbind()` needs identical columns, so use `data.table::rbindlist()` with
#' `fill = TRUE` if some carry extras (for example `doubtful` from
#' [apply_rating()]). No method exists for potential evaporation objects.
#'
#' @param x A [FlodeRainfall_Daily], [FlodeRainfall_15min], [FlodeFlow_Daily], [FlodeFlow_15min],
#'   [FlodeLevel_Daily], or [FlodeLevel_15min] object.
#' @param ... Additional arguments for methods (currently unused).
#'
#' @returns A `data.table` with columns `parameter`, `measure_notation`,
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

S7::method(as_long, FlodeHydroData) <- function(x) {
  # Copy: := must not add a `parameter` column to the object's own table.
  dt <- data.table::copy(x@readings)
  dt[, parameter := x@parameter]
  # Leading columns are fixed; setcolorder() leaves the rest in place after them.
  data.table::setcolorder(dt, c("parameter", "measure_notation",
                                "date", "dateTime", "value"))
  dt
}


# -- Print method -------------------------------------------------------------
#
# One method on the abstract parent covers all six classes. Slot names are
# printed explicitly so users can see what is available with @ without
# relying on IDE autocomplete (which has limited S7 support).

.print_HydroData <- function(x, ...) {
  dt        <- x@readings
  col_types <- vapply(dt, function(col) class(col)[1L], character(1L))
  col_lines <- paste(
    sprintf("      %-24s %s", paste0("$", names(col_types)), col_types),
    collapse = "\n"
  )
  cat(
    # class(x)[1L] already carries the package prefix ("reach.io::FlodeFlow_Daily"),
    # so strip it before adding it back, or the header reads reach.io::reach.io::
    sprintf("<reach.io::%s>\n", sub("^reach\\.io::", "", class(x)[1L])),
    "Slots (@):\n",
    sprintf("  @parameter      %s\n",  x@parameter),
    sprintf("  @period_name    %s\n",  x@period_name),
    sprintf("  @from_date      %s\n",  x@from_date),
    sprintf("  @to_date        %s\n",  x@to_date),
    sprintf("  @n_measures     %d\n",  x@n_measures),
    sprintf("  @n_rows         %s\n",  format(x@n_rows, big.mark = ",")),
    sprintf("  @downloaded_at  %s\n",  format(x@downloaded_at,
                                              "%Y-%m-%d %H:%M:%S %Z")),
    sprintf("  @readings       data.table [%s \u00d7 %d]\n%s\n",
            format(nrow(dt), big.mark = ","), ncol(dt), col_lines),
    sep = ""
  )
  invisible(x)
}

# NB: S7's print.S7_object calls str() and does not consult this method table,
# so this assignment alone does not change auto-printing. .onLoad() (package.R)
# also registers .print_HydroData as an S3 method for "reach.io::FlodeHydroData",
# which is the registration that takes effect.
S7::method(print, FlodeHydroData) <- .print_HydroData


# =============================================================================
# FlodePotEvapData
# =============================================================================

# -- Abstract parent ----------------------------------------------------------

#' Abstract base class for potential evaporation data
#'
#' Not exported. Holds the properties and validator shared by all three
#' concrete PE classes. Every concrete class inherits from this.
#'
#' @prop readings A `data.table` of readings. Must contain columns
#'   `dateTime` (POSIXct), `date` (Date), and `value` (numeric).
#' @prop period_name Character scalar. `"daily"`, `"hourly"`, or `"15min"`.
#' @prop source_name Character scalar. Identifies the data origin (e.g.
#'   `"CHESS-PE"`, `"MORECS"`, `"bespoke"`). Free-text; used for
#'   provenance tracking and printing.
#' @prop from_date Character scalar. Start of the data range (YYYY-MM-DD).
#' @prop to_date Character scalar. End of the data range (YYYY-MM-DD).
#' @prop n_rows Integer. Total reading rows.
#' @prop created_at POSIXct. When the object was constructed.
#'
#' @noRd
FlodePotEvapData <- S7::new_class(
  "FlodePotEvapData",
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

    # -- Column types --------------------------------------------------------
    if (!inherits(dt$dateTime, "POSIXct")) {
      return("`readings$dateTime` must be POSIXct.")
    }
    if (!inherits(dt$date, "Date")) {
      return("`readings$date` must be Date.")
    }
    if (!is.numeric(dt$value)) {
      return("`readings$value` must be numeric.")
    }
    # -- Scalar metadata -----------------------------------------------------
    # The concrete constructors fix period_name, so this guards later
    # assignment. source_name is free text; NA is accepted but it must be length 1.
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
    parent     = FlodePotEvapData,
    properties = extra_props,
    constructor = function(readings,
                           source_name = NA_character_,
                           from_date   = NA_character_,
                           to_date     = NA_character_) {
      dt <- .as_readings_dt(readings)
      # period_name is fixed by the class; n_rows and created_at are derived at
      # construction (created_at is the build time). S7 validates on return.
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
#' @details
#' The constructor checks that `readings` is a table, fixes `period_name`, and
#' computes `n_rows` and `created_at` (the construction time). Validation
#' requires `dateTime` (`POSIXct`), `date` (`Date`) and `value` (numeric), and a
#' length-one `source_name` (`NA` is accepted). Extra columns are kept; units,
#' sorting, spacing and agreement between `date` and `dateTime` are not
#' checked, and `from_date` / `to_date` are labels only. [generate_pe_sine()]
#' returns this class, and [disagg_to_15min()] converts it to
#' [FlodePotEvap_15min].
#'
#' @param readings A `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, mm/day), and optionally any
#'   additional columns present in the source (e.g. `quality`).
#' @param source_name Character. Origin of the PE data. Default `NA`.
#' @param from_date,to_date Data range (YYYY-MM-DD). Default `NA`.
#' @prop period_name Always `"daily"`.
#' @prop n_rows Total reading rows.
#' @prop created_at POSIXct timestamp of object construction.
#'
#' @returns A `FlodePotEvap_Daily` S7 object.
#' @export
FlodePotEvap_Daily <- .new_pe_class("FlodePotEvap_Daily", "daily")

#' S7 class for hourly potential evaporation readings
#'
#' Wraps hourly PE values sourced directly from an external PE dataset.
#' Units are typically mm/hr. One row per hour.
#'
#' @details
#' Construction and validation are as for [FlodePotEvap_Daily]. Nothing checks
#' that the rows are one hour apart. [disagg_to_15min()] converts this class to
#' [FlodePotEvap_15min], treating each value as an hourly total.
#'
#' @param readings A `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, mm/hr), and optionally any
#'   additional columns present in the source.
#' @param source_name Character. Origin of the PE data. Default `NA`.
#' @param from_date,to_date Data range (YYYY-MM-DD). Default `NA`.
#' @prop period_name Always `"hourly"`.
#' @prop n_rows Total reading rows.
#' @prop created_at POSIXct timestamp of object construction.
#'
#' @returns A `FlodePotEvap_Hourly` S7 object.
#' @export
FlodePotEvap_Hourly <- .new_pe_class("FlodePotEvap_Hourly", "hourly")

#' S7 class for 15-minute potential evaporation (calculated)
#'
#' Wraps 15-minute PE values that have been **disaggregated** from a
#' coarser timestep via [disagg_to_15min()]. This class is not intended
#' to be constructed directly; use `disagg_to_15min()` instead.
#'
#' @details
#' Validation runs the parent checks (as for [FlodePotEvap_Daily]) and then
#' requires `disagg_method` to be `"uniform_hourly"` or `"uniform_daily"`. The
#' constructor's default of `NA` fails that check, so building one directly
#' means naming a method; [disagg_to_15min()] does this for you. `is_calculated`
#' is set to `TRUE` by the constructor and is not an argument, so the check on
#' it only matters if the slot is overwritten later with `@<-`.
#'
#' @param readings A `data.table` with columns `dateTime` (POSIXct),
#'   `date` (Date), `value` (numeric, mm/15min).
#' @param source_name Character. Inherited from the parent object. Default `NA`.
#' @param from_date,to_date Data range (YYYY-MM-DD). Default `NA`.
#' @param disagg_method Character. How disaggregation was performed.
#'   One of `"uniform_hourly"` or `"uniform_daily"`. Default `NA`.
#' @prop period_name Always `"15min"`.
#' @prop n_rows Total reading rows.
#' @prop created_at POSIXct timestamp of object construction.
#' @prop is_calculated Logical. Always `TRUE` — 15-min PE is never
#'   sourced directly; it is always derived.
#'
#' @returns A `FlodePotEvap_15min` S7 object.
#' @export
FlodePotEvap_15min <- S7::new_class(
  "FlodePotEvap_15min",
  package    = "reach.io",
  parent     = FlodePotEvapData,
  properties = list(
    is_calculated = S7::new_property(class = S7::class_logical),
    disagg_method = S7::new_property(class = S7::class_character)
  ),
  constructor = function(readings,
                         source_name   = NA_character_,
                         from_date     = NA_character_,
                         to_date       = NA_character_,
                         disagg_method = NA_character_) {
    # is_calculated is always TRUE and not an argument. disagg_method defaults to
    # NA, which the validator rejects on purpose: objects are meant to come from
    # disagg_to_15min(), or to name their method explicitly.
    dt <- .as_readings_dt(readings)
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
    # Runs after FlodePotEvapData's validator. The constructor always sets
    # is_calculated = TRUE, so this check only fires if the slot is overwritten
    # with `@<-`; the check below rejects the NA default.
    if (!self@is_calculated) {
      return("`is_calculated` must be TRUE for FlodePotEvap_15min.")
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
#' `disagg_to_15min()` is an S7 generic. Converts a [FlodePotEvap_Daily] or
#' [FlodePotEvap_Hourly] object to a [FlodePotEvap_15min] object using uniform
#' disaggregation:
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
#' @details
#' The hourly method treats each `dateTime` as the start of its hour and adds
#' offsets of 0, 15, 30 and 45 minutes. The daily method builds 96 steps of 15
#' minutes from the start of each `date` (not from `dateTime`), in the time zone
#' of the `dateTime` column (UTC if none is recorded); the fixed 96 steps assume
#' 24-hour days, so a time zone with daylight saving would not line up on
#' changeover days. In both cases `date` is recomputed from the new `dateTime`,
#' other columns are repeated on every sub-interval, `NA` values stay `NA`, and
#' the sub-interval values sum to the original value. `source_name`,
#' `from_date` and `to_date` are carried over, `created_at` is the time of
#' conversion, and the input object is not modified. Each input row expands in
#' place, so sorted input stays sorted.
#'
#' @param x A [FlodePotEvap_Daily] or [FlodePotEvap_Hourly] object.
#' @param ... Additional arguments for methods (currently unused).
#'
#' @returns A [FlodePotEvap_15min] object whose readings contain columns
#'   `dateTime` (POSIXct), `date` (Date), and `value` (numeric).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' pe_hr  <- FlodePotEvap_Hourly(readings = hourly_dt, source_name = "CHESS-PE")
#' pe_15m <- disagg_to_15min(pe_hr)
#' }
disagg_to_15min <- S7::new_generic("disagg_to_15min", "x")

S7::method(disagg_to_15min, FlodePotEvap_Hourly) <- function(x) {
  dt <- data.table::copy(x@readings)

  # Each hourly row becomes four rows at +0, +15, +30 and +45 minutes from its
  # dateTime, which is therefore taken as the START of the hour. value / 4 splits
  # a TOTAL (e.g. mm) evenly so sums are preserved; a rate would be wrong here.
  offsets_sec <- c(0L, 900L, 1800L, 2700L)
  idx  <- rep(seq_len(nrow(dt)), each = 4L)
  rows <- dt[idx]
  rows[, dateTime := dateTime + offsets_sec]
  # date follows the new dateTime.
  rows[, date     := as.Date(dateTime)]
  rows[, value    := value / 4]

  FlodePotEvap_15min(
    readings      = rows,
    source_name   = x@source_name,
    from_date     = x@from_date,
    to_date       = x@to_date,
    disagg_method = "uniform_hourly"
  )
}

S7::method(disagg_to_15min, FlodePotEvap_Daily) <- function(x) {
  dt  <- data.table::copy(x@readings)
  # Time zone of the input dateTime; UTC when none is recorded.
  tz  <- attr(dt$dateTime, "tzone")
  tz  <- if (is.null(tz) || !nzchar(tz)) "UTC" else tz

  # 96 fixed 15-minute offsets from the start of each day (24-hour days; no
  # daylight-saving handling).
  offsets_sec <- as.integer(seq(0L, by = 900L, length.out = 96L))
  idx  <- rep(seq_len(nrow(dt)), each = 96L)
  rows <- dt[idx]

  # Built from the `date` column (the start of that day in tz), not from the
  # input dateTime. value / 96 splits a daily TOTAL evenly, preserving sums.
  rows[, dateTime := as.POSIXct(date, tz = tz) + offsets_sec]
  rows[, date     := as.Date(dateTime)]
  rows[, value    := value / 96]

  FlodePotEvap_15min(
    readings      = rows,
    source_name   = x@source_name,
    from_date     = x@from_date,
    to_date       = x@to_date,
    disagg_method = "uniform_daily"
  )
}


# -- Generics and methods -----------------------------------------------------

#' @noRd
S7::method(as_data_table, FlodePotEvapData) <- function(x) x@readings


# -- Print method -------------------------------------------------------------

.print_PotEvapData <- function(x, ...) {
  dt        <- x@readings
  col_types <- vapply(dt, function(col) class(col)[1L], character(1L))
  col_lines <- paste(
    sprintf("      %-24s %s", paste0("$", names(col_types)), col_types),
    collapse = "\n"
  )
  disagg_line <- if (S7::S7_inherits(x, FlodePotEvap_15min))
    sprintf("  @disagg_method  %s\n  @is_calculated  %s\n",
            x@disagg_method, x@is_calculated)
  else ""

  cat(
    # class(x)[1L] already carries the package prefix ("reach.io::FlodeFlow_Daily"),
    # so strip it before adding it back, or the header reads reach.io::reach.io::
    sprintf("<reach.io::%s>\n", sub("^reach\\.io::", "", class(x)[1L])),
    "Slots (@):\n",
    sprintf("  @source_name    %s\n",  x@source_name),
    sprintf("  @period_name    %s\n",  x@period_name),
    sprintf("  @from_date      %s\n",  x@from_date),
    sprintf("  @to_date        %s\n",  x@to_date),
    sprintf("  @n_rows         %s\n",  format(x@n_rows, big.mark = ",")),
    disagg_line,
    sprintf("  @created_at     %s\n",  format(x@created_at,
                                              "%Y-%m-%d %H:%M:%S %Z")),
    sprintf("  @readings       data.table [%s \u00d7 %d]\n%s\n",
            format(nrow(dt), big.mark = ","), ncol(dt), col_lines),
    sep = ""
  )
  invisible(x)
}

# NB: as for FlodeHydroData above, .onLoad() (package.R) registers
# .print_PotEvapData as the S3 print method that actually takes effect.
S7::method(print, FlodePotEvapData) <- .print_PotEvapData
