# ============================================================
# Tool:        Hydrological Utilities
# Description: General-purpose utilities for working with
#              hydrometric time series data. Currently
#              provides hydro-year (Oct-Sep) labelling for
#              data.table objects.
# Flode Module: flode.io
# Author:      JP
# Created:     2026-04-02
# Tier:        2
# Inputs:      data.table with POSIXct or Date column
# Outputs:     data.table with hydro_year / hydro_year_day
#              columns added in place
# Dependencies: data.table
# ============================================================


#' Add hydrological year columns to a data.table
#'
#' Adds two integer columns to a `data.table` in place:
#' \describe{
#'   \item{`hydro_year`}{The hydrological year (Oct–Sep). Observations on or
#'     after 1 October are assigned to the following calendar year — e.g. a
#'     date of 2023-10-15 gets `hydro_year = 2024`.}
#'   \item{`hydro_year_day`}{Day number within the hydrological year, where
#'     1 October = day 1. Leap years are handled correctly via \code{Date}
#'     arithmetic, so a leap hydrological year (one that contains 29 February)
#'     has 366 days.}
#' }
#'
#' The function modifies `dt` by reference (data.table semantics) and returns
#' it invisibly, so it can be chained:
#' \code{add_hydro_year(dt)[]}.
#'
#' @details
#' Only the calendar date is used: the time of day is discarded, so every
#' reading on one date gets the same `hydro_year` and `hydro_year_day`.
#' `POSIXct` input is converted with `as.Date()`, which reads it in UTC unless
#' told otherwise, so a timestamp is labelled by its UTC date. If the
#' hydrological day should follow local time, convert to `Date` in the required
#' time zone before calling.
#'
#' Missing dates are not supported: remove or fill them first (depending on
#' where it sits, a missing date can make `as.Date()` fail inside the function).
#'
#' Existing `hydro_year` / `hydro_year_day` columns are replaced without
#' warning. Because `dt` is modified by reference, the caller's table changes
#' even when the return value is not assigned; take `data.table::copy()` first
#' to keep the original intact.
#'
#' @param dt A `data.table`.
#' @param date_col Character scalar. Name of the column containing date or
#'   datetime values. Defaults to `"dateTime"`. The column may be `POSIXct`,
#'   `POSIXlt`, or `Date`; it is coerced to `Date` internally.
#'
#' @return `dt`, invisibly, with `hydro_year` and `hydro_year_day` columns
#'   added (or overwritten if they already exist).
#'
#' @export
#'
#' @examples
#' library(data.table)
#' dt <- data.table(
#'   dateTime = as.POSIXct(c("2023-09-30", "2023-10-01", "2024-09-30"), tz = "UTC")
#' )
#' add_hydro_year(dt)
#' # hydro_year:     2023, 2024, 2024
#' # hydro_year_day:  365,    1,  366  (2024 is a leap hydro year)
add_hydro_year <- function(dt, date_col = "dateTime") {
  if (!data.table::is.data.table(dt)) {
    stop("`dt` must be a data.table.")
  }
  if (!date_col %in% names(dt)) {
    stop(sprintf("`dt` has no column named '%s'.", date_col))
  }

  # Work on calendar dates: the time of day plays no part in the Oct-Sep label.
  dates    <- as.Date(dt[[date_col]])
  m        <- data.table::month(dates)
  y        <- data.table::year(dates)
  # The hydro year is named for the calendar year in which it ENDS, so Oct-Dec
  # belong to the next one (2023-10-15 -> 2024). hy_start is the 1 Oct that
  # opened that year, i.e. in calendar year hy - 1.
  hy       <- y + as.integer(m >= 10L)
  hy_start <- as.Date(paste0(hy - 1L, "-10-01"))

  # Day 1 is 1 Oct. Date subtraction counts real days, so 29 Feb is included
  # automatically (a hydro year containing it has 366 days).
  # NB: inside dt[...] the symbols `hy`, `dates` and `hy_start` resolve to
  # columns of `dt` before these local variables, so a table that already has a
  # column with one of those names would be labelled from that column instead.
  dt[, hydro_year     := hy]
  dt[, hydro_year_day := as.integer(dates - hy_start) + 1L]

  invisible(dt)
}
