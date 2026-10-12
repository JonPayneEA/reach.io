# ============================================================
# Tool:        Station and Measure Lookup
# Description: Queries the EA Hydrology API stations and
#              measures endpoints. find_stations() returns one
#              row per measure with parsed metadata columns.
# Flode Module: flode.io
# Author:      [Hydrometric Data Lead]
# Created:     2026-02-01
# Modified:    2026-02-01 - JP: initial version
# Tier:        1
# Inputs:      Station identifiers or location
# Outputs:     data.table of station/measure metadata
# Dependencies: httr, data.table
# ============================================================

# -- Station and measure lookup -----------------------------------------------

#' Parse EA measure notations into parameter / value type / period
#'
#' A measure notation has the form
#' `<station-uuid>-<parameter>-<valuetype>-<period>-<unit>-<quality>`, e.g.
#' `c46d1245-e34a-4ea9-8c4c-410357e80e15-flow-i-900-m3s-qualified`. The
#' station UUID is matched explicitly so the parse does not depend on the
#' position of hyphens; notations that do not match give `NA` (with a warning)
#' rather than silently wrong values.
#'
#' @param x Character vector of measure notations.
#' @return A `list` of three character vectors: `parameter`, `value_type`,
#'   `period`.
#' @noRd
.parse_measure_notation <- function(x) {
  uuid  <- "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}-"
  ok    <- grepl(uuid, x)
  if (any(!ok)) {
    warning(sprintf(
      "%d measure notation(s) did not match the expected '<uuid>-<parameter>-<type>-<period>-...' form; parameter/value_type/period set to NA.",
      sum(!ok)), call. = FALSE)
  }
  rest  <- sub(uuid, "", x)
  parts <- strsplit(rest, "-", fixed = TRUE)
  pick  <- function(i) {
    out <- vapply(parts, function(p) if (length(p) >= i) p[i] else NA_character_,
                  character(1L))
    out[!ok] <- NA_character_
    out
  }
  parameter <- pick(1L)
  vt_raw    <- pick(2L)
  pd_raw    <- pick(3L)

  # fcase() default must be length-1; fall back to the raw segment afterwards
  vt_mapped <- data.table::fcase(
    vt_raw == "m",   "mean",
    vt_raw == "min", "min",
    vt_raw == "max", "max",
    vt_raw == "i",   "instantaneous",
    vt_raw == "t",   "total",
    default = NA_character_
  )
  pd_mapped <- data.table::fcase(
    pd_raw == "900",   "15min",
    pd_raw == "86400", "daily",
    default = NA_character_
  )

  list(
    parameter  = parameter,
    value_type = data.table::fifelse(is.na(vt_mapped), vt_raw, vt_mapped),
    period     = data.table::fifelse(is.na(pd_mapped), pd_raw, pd_mapped)
  )
}


#' Find stations matching a set of identifiers
#'
#' Looks up stations from the EA Hydrology API by one or more identifier
#' types and returns a `data.table` with **one row per measure** (time series)
#' available at each matched station. Multiple identifier types can be supplied
#' simultaneously — results are pooled and de-duplicated by station.
#'
#' The measure notation is parsed into `parameter`, `value_type` and `period`
#' columns, so a second call to [get_measures()] is not needed for the common
#' case where you already know which stations you want. Only rainfall, flow
#' and level measures are returned.
#'
#' @param names Character vector or `NULL`. Fuzzy station name search using
#'   the API's `search` parameter.
#' @param wiski_ids Character vector or `NULL`. WISKI identifiers, e.g.
#'   `"SS92F014"`.
#' @param rloi_ids Character vector or `NULL`. River Levels on the Internet
#'   identifiers, e.g. `"5022"`.
#' @param notations Character vector or `NULL`. Station SUID/notation values,
#'   e.g. `"c46d1245-e34a-4ea9-8c4c-410357e80e15"`.
#' @param lat Numeric or `NULL`. Latitude for proximity search. Must be
#'   supplied with `long` and `dist`.
#' @param long Numeric or `NULL`. Longitude for proximity search.
#' @param dist Numeric or `NULL`. Search radius in km.
#'
#' @return A `data.table`, one row per measure, with columns:
#'   \describe{
#'     \item{`label`}{Station name.}
#'     \item{`notation`}{Station SUID.}
#'     \item{`wiskiID`}{WISKI identifier (where available).}
#'     \item{`RLOIid`}{River Levels on the Internet identifier (where
#'       available).}
#'     \item{`lat`, `long`, `easting`, `northing`}{Coordinates.}
#'     \item{`riverName`}{Associated river name (where available).}
#'     \item{`measure_notation`}{Measure notation — the ID passed to the
#'       readings endpoint, e.g. `"<uuid>-flow-i-900-m3s-qualified"`.}
#'     \item{`station.notation`}{Deprecated alias of `measure_notation`
#'       (misleading name kept for backwards compatibility).}
#'     \item{`parameter`}{`"rainfall"`, `"flow"` or `"level"`.}
#'     \item{`value_type`}{e.g. `"mean"`, `"instantaneous"`, `"total"`.}
#'     \item{`period`}{`"15min"` or `"daily"`.}
#'   }
#'   Returns an empty `data.table` if no stations are found.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' stns <- find_stations(wiski_ids = "SS92F014")
#'
#' # One row per measure available at the station
#' stns[, .(label, wiskiID, parameter, period, value_type, measure_notation)]
#'
#' # Other lookup methods
#' find_stations(names = "Thames")
#' find_stations(rloi_ids = c("5022", "7001"))
#' find_stations(notations = "c46d1245-e34a-4ea9-8c4c-410357e80e15")
#' find_stations(lat = 51.5, long = -1.8, dist = 10)
#'
#' # Combine methods — results are pooled and de-duplicated
#' find_stations(wiski_ids = "SS92F014", lat = 51.5, long = -1.8, dist = 10)
#' }
find_stations <- function(names     = NULL,
                          wiski_ids = NULL,
                          rloi_ids  = NULL,
                          notations = NULL,
                          lat       = NULL,
                          long      = NULL,
                          dist      = NULL) {

  keep_cols <- c("label", "notation", "wiskiID", "RLOIid",
                 "lat", "long", "easting", "northing", "riverName", "measures")

  results <- list()

  fetch_stations <- function(query) {
    resp  <- httr::GET(paste0(BASE_URL, "/id/stations.json"), query = query)
    httr::stop_for_status(resp)
    items <- httr::content(resp, as = "parsed", simplifyVector = TRUE)$items
    if (length(items) == 0) return(data.table::data.table())
    dt <- data.table::as.data.table(items)
    dt <- dt[, intersect(keep_cols, names(dt)), with = FALSE]
    dt
  }

  if (!is.null(names)) {
    for (nm in names) {
      message(sprintf("  Searching by name: '%s'", nm))
      dt <- fetch_stations(list(search = nm, `_limit` = 1000))
      if (nrow(dt) > 0) {
        message(sprintf("    Found %d station(s).", nrow(dt)))
        results[[length(results) + 1]] <- dt
      } else {
        message("    No stations found.")
      }
    }
  }

  if (!is.null(wiski_ids)) {
    for (wid in wiski_ids) {
      message(sprintf("  Looking up WISKI ID: %s", wid))
      dt <- fetch_stations(list(wiskiID = wid))
      if (nrow(dt) > 0) {
        results[[length(results) + 1]] <- dt
      } else {
        warning(sprintf("No station found for WISKI ID: %s", wid))
      }
    }
  }

  if (!is.null(rloi_ids)) {
    for (rid in rloi_ids) {
      message(sprintf("  Looking up RLOIid: %s", rid))
      dt <- fetch_stations(list(RLOIid = rid))
      if (nrow(dt) > 0) {
        results[[length(results) + 1]] <- dt
      } else {
        warning(sprintf("No station found for RLOIid: %s", rid))
      }
    }
  }

  if (!is.null(notations)) {
    for (nt in notations) {
      message(sprintf("  Looking up notation: %s", nt))
      dt <- fetch_stations(list(notation = nt))
      if (nrow(dt) > 0) {
        results[[length(results) + 1]] <- dt
      } else {
        warning(sprintf("No station found for notation: %s", nt))
      }
    }
  }

  if (!is.null(lat) && !is.null(long) && !is.null(dist)) {
    message(sprintf("  Searching within %g km of (%.4f, %.4f)", dist, lat, long))
    dt <- fetch_stations(list(lat = lat, long = long, dist = dist,
                              `_limit` = 1000))
    if (nrow(dt) > 0) {
      message(sprintf("    Found %d station(s).", nrow(dt)))
      results[[length(results) + 1]] <- dt
    } else {
      message("    No stations within radius.")
    }
  }

  if (length(results) == 0) return(data.table::data.table())

  dt <- unique(data.table::rbindlist(results, fill = TRUE), by = "notation")

  # Guarantee all expected columns exist. Some are absent when no looked-up
  # station returned that field (e.g. RLOIid is not present for WISKI-only
  # sites). Without this, the by = .(... RLOIid ...) grouping below errors.
  scalar_keep <- setdiff(keep_cols, "measures")
  for (col in scalar_keep) {
    if (!col %in% names(dt)) dt[, (col) := NA_character_]
  }

  # Resolve any remaining list columns except measures
  list_cols <- names(dt)[sapply(dt, is.list)]
  list_cols <- list_cols[list_cols != "measures"]
  dt[, (list_cols) := lapply(.SD, function(x) sapply(x, toString)),
     .SDcols = list_cols]

  # Unnest measures to one row per measure, keeping all station scalar columns.
  # Stations with no measures drop out (zero-row result for that group).
  first_id <- function(m) {
    m <- data.table::rbindlist(m, fill = TRUE)
    if (nrow(m) == 0L || !"@id" %in% names(m)) {
      return(data.table::data.table(`@id` = character()))
    }
    m[, "@id", with = FALSE]
  }
  dt <- dt[, first_id(measures),
             by = .(label, notation, wiskiID, RLOIid,
                    lat, long, easting, northing, riverName)]
  if (nrow(dt) == 0L) return(data.table::data.table())
  data.table::setnames(dt, "@id", "measure_notation")
  dt[, measure_notation := basename(measure_notation)]

  # Deprecated alias: this column was called `station.notation` although it
  # holds the *measure* notation. Kept so existing scripts keep working.
  dt[, station.notation := measure_notation]

  # Parse the measure notation into useful metadata columns
  dt[, c("parameter", "value_type", "period") :=
       .parse_measure_notation(measure_notation)]

  return(dt[parameter %in% c("rainfall", "flow", "level")])
}


#' Get measure metadata from the EA Hydrology API
#'
#' Fetches the full list of available measurement time series for a given
#' parameter type, filtered by temporal resolution and optionally by value
#' statistic. Returns a `data.table` that includes `station.wiskiID` and
#' `station` (the full station URI), which can be used to filter to specific
#' stations after fetching rather than making multiple per-station API calls.
#'
#' For the common case where you already know which stations you want,
#' `find_stations()` returns measure notations directly in its `measures`
#' list column, avoiding a second API call entirely.
#'
#' @param parameter Character. One of `"rainfall"`, `"flow"`, or `"level"`.
#' @param period_name Character or `NULL`. `"daily"` or `"15min"`. Defaults
#'   to the parameter's built-in default (daily for all three).
#' @param value_type Character or `NULL`. Value statistic filter, e.g.
#'   `"mean"`, `"min"`, `"max"`, `"instantaneous"`, `"total"`. `NULL` uses
#'   the parameter default. `"all"` skips the filter entirely.
#' @param limit Integer. Maximum number of measures to return. Default 2000.
#'
#' @return A `data.table` of measure metadata. Key columns include
#'   `notation` (the measure ID used in readings requests),
#'   `station` (full station URI), `station.label`, `station.wiskiID`,
#'   `periodName`, `unitName`, `valueType`, and `parameter`.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # All daily mean flow measures
#' all_flow <- get_measures("flow")
#'
#' # Filter to specific stations by WISKI ID after fetching
#' all_flow[`station.wiskiID` %in% c("SS92F014", "S11512_FW")]
#'
#' # All 15-minute rainfall measures
#' get_measures("rainfall", period_name = "15min")
#'
#' # All level statistics (min, max, instantaneous)
#' get_measures("level", value_type = "all")
#' }
get_measures <- function(parameter   = c("rainfall", "flow", "level"),
                         period_name = NULL,
                         value_type  = NULL,
                         limit       = 5000) {

  parameter <- match.arg(parameter)
  config    <- PARAMETER_CONFIG[[parameter]]

  if (is.null(period_name)) period_name <- config$default_period
  if (is.null(value_type))  value_type  <- config$value_type

  query <- list(
    observedProperty = config$observed_property,
    periodName       = period_name,
    `_limit`         = limit
  )

  # "all" is a sentinel meaning skip the valueType filter entirely
  if (!is.null(value_type) && value_type != "all") {
    query$valueType <- value_type
  }

  url  <- paste0(BASE_URL, "/id/measures.json")
  resp <- httr::GET(url, query = query)
  httr::stop_for_status(resp)

  items <- httr::content(resp, as = "parsed", simplifyVector = TRUE)$items

  if (length(items) == 0) {
    warning(sprintf(
      "No measures found for parameter='%s', period='%s', valueType='%s'.",
      parameter, period_name, value_type %||% "any"
    ))
    return(data.table::data.table())
  }

  dt <- data.table::as.data.table(items)
  dt[, parameter := parameter][]
  return(dt)
}
