# ============================================================
# Tool:        S7 Potential Evaporation Classes
# Description: S7 class hierarchy for typed potential
#              evaporation (PE) objects. Three concrete classes
#              cover daily, hourly (both sourced directly), and
#              15-minute (always disaggregated from a coarser
#              timestep). PE data is sourced independently of
#              the Hydrology API and may differ in extent,
#              coverage and structure from HydroData objects.
# Flode Module: flode.io
# Author:      JP
# Created:     2026-03-19
# Tier:        2
# Inputs:      data.table of PE readings from an external PE
#              source (e.g. CEH CHESS-PE, MORECS, bespoke).
# Outputs:     Typed S7 objects (PotEvap_Daily etc.)
# Dependencies: S7, data.table
# ============================================================

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
.new_pe_class <- function(class_name, period_name_fixed,
                          extra_props = list(),
                          extra_constructor_args = NULL,
                          extra_object_args = NULL) {
  S7::new_class(
    class_name,
    package    = "reach.io",
    parent     = PotEvapData,
    properties = extra_props,
    constructor = function(readings,
                           source_name = NA_character_,
                           from_date   = NA_character_,
                           to_date     = NA_character_,
                           ...) {
      dt <- data.table::as.data.table(readings)
      base_args <- list(
        S7::S7_object(),
        readings    = dt,
        period_name = period_name_fixed,
        source_name = source_name,
        from_date   = from_date,
        to_date     = to_date,
        n_rows      = nrow(dt),
        created_at  = Sys.time()
      )
      dots <- list(...)
      do.call(S7::new_object, c(base_args, dots))
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

  # Repeat each row 4 times, then shift dateTime by 0/15/30/45 minutes
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

  # Repeat each row 96 times (one per 15-min slot in a day)
  offsets_sec <- as.integer(seq(0L, by = 900L, length.out = 96L))
  idx  <- rep(seq_len(nrow(dt)), each = 96L)
  rows <- dt[idx]

  # Anchor to midnight of each date, then add 15-min offsets
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

#' Extract the readings data.table from a PotEvapData object
#'
#' Returns the inner `data.table` from any `PotEvapData`-derived object.
#'
#' @param x A [PotEvap_Daily], [PotEvap_Hourly], or [PotEvap_15min] object.
#'
#' @return A `data.table` of readings.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' pe   <- PotEvap_Daily(readings = daily_dt, source_name = "MORECS")
#' dt   <- as_data_table(pe)
#' }
S7::method(as_data_table, PotEvapData) <- function(x) x@readings


# -- Print method -------------------------------------------------------------

S7::method(print, PotEvapData) <- function(x, ...) {
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
