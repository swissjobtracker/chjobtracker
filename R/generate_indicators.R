library(data.table)
library(magrittr)

#' Generate the indicator series
#'
#' @param con RPostgres connection
#'
#' @return a tslist with all indicator series
#' @export
generate_indicators <- function(con, verbose = FALSE, drop_lichtenstein = TRUE) {
  prnt <- function(x) {
    if (verbose) message(x)
  }

  prnt("Loading data...")
  ads <- get_ad_data(con)

  if (drop_lichtenstein) {
    prnt("Dropping Lichtenstein...")
    prnt("Amending data with geo information...")
    ads_w_geo <- get_ads_w_canton(con, ads)
    prnt("Dropping ads from Lichtenstein...")
    ads_from_li_only <- ads_w_geo[, .(.N == 1 && canton == "li"), by = "id"][V1 == TRUE, id]
    ads <- ads[!(id %in% ads_from_li_only)]
  }

  ads_prepared <- prepare_ads(ads, verbose)
  ads_prepared[, data.table::uniqueN(domain), by = from_portal]


  # (source, date) pairs to include
  # date will only be fridays. do further restrict to only "clean" portals
  # use only rows with include == TRUE
  week_inclusion <- portal_filter_within_portal_hampler(
    ads_prepared,
    weeks_not_flagged = 5, mad_factor = 4
  )

  prnt("Calculating total...")
  weeks <- get_stocks(ads_prepared)
  srs_tot <- make_series(ads_prepared, week_inclusion)

  prnt("Calculating by noga...")
  ads_noga <- ads_merge_noga(con, ads_prepared)
  weeks_noga <- get_stocks(
    ads_noga,
    by.cols = "noga_letter"
  )
  srs_noga <- make_series(ads_noga, week_inclusion, by.cols = "noga_letter")
  setnames(srs_noga, "noga_letter", "noga")

  prnt("Calculating by isco...")
  ads_isco <- ads_merge_isco(con, ads_prepared)
  weeks_isco <- get_stocks(
    ads_isco,
    by.cols = "isco_1d"
  )
  srs_isco <- make_series(ads_isco, week_inclusion, by.cols = "isco_1d")
  setnames(srs_isco, "isco_1d", "isco")

  prnt("Calculating by canton...")
  ads_canton <- ads_merge_canton(con, ads_prepared)
  weeks_canton <- get_stocks(
    ads_canton,
    by.cols = "canton"
  )
  srs_canton <- make_series(ads_canton, week_inclusion, by.cols = "canton")

  affected <- save_stocks(con, weeks, weeks_noga, weeks_isco, weeks_canton)
  prnt(sprintf("Added %d rows", affected))

  prnt(
    paste(
      "Manual changes to indices (removing values that don't have",
      "publishable quality)"
    )
  )
  srs_canton <- manual_changes_to_canton_indices(srs_canton)

  prnt("Converting to ts...")
  tsl_tot <- output_to_tslist(srs_tot)

  tsl_noga <- output_to_tslist(srs_noga, "noga")

  tsl_isco <- output_to_tslist(srs_isco, "isco")

  tsl_canton <- output_to_tslist(srs_canton, "canton")


  c(
    tsl_tot,
    tsl_noga,
    tsl_isco,
    tsl_canton
  )
}


#' Convert data.table output of make_series to a ts list
#'
#' @param dt output from make_series
#' @param by name of category column (e.g. "noga_letter"). NULL for total
#'
#' @importFrom tsbox ts_xts
#' @import data.table
#' @export
output_to_tslist <- function(dt, by = NULL) {
  if (is.null(by)) {
    dt_long <- melt(dt, "date", c("raw", "clean", "companies", "sum_clean"))
    dt_long[, id := sprintf("total.total.%s", variable)]
  } else {
    dt_long <- melt(dt, c(by, "date"), c("raw", "clean", "companies", "sum_clean"))
    dt_long[, id := sprintf("%s.%s", get(by), variable)]
  }

  dt_long <- dt_long[, .(date, value, id)]

  # ts_xts returns a multi-column xts. as.list makes it into a list
  # of separate xts
  tsl <- as.list(tsbox::ts_xts(dt_long[, .(time = date, value, id)]))

  if (!is.null(by)) {
    names(tsl) <- tolower(sprintf("ch.kof.jobtracker.%s.%s.idx", by, names(tsl)))
  } else {
    names(tsl) <- tolower(sprintf("ch.kof.jobtracker.%s.idx", names(tsl)))
  }

  # share_clean is independent (i think)
  names(tsl) <- gsub("share_clean.idx$", "share_clean", names(tsl))
  tsl
}

get_stocks <- function(ads,
                       by.cols = c(),
                       start_date = "2018-01-01") {
  # Some calculations need to be done by by.cols plus source
  by.cols.source <- union(by.cols, "source")
  # calculate actual daily stock with capped duration
  vacancies <- vacancies_by_day(ads, by.cols = by.cols.source)[, N := job_postings][, c("source", "date", by.cols, "N"), with = FALSE]
  # calculate needed rolling average on actual stock
  setorderv(vacancies, c(by.cols.source, "date"))
  # Get lag from previous week
  vacancies <- vacancies[, wlag_N := shift(N, 7), by = by.cols.source]
  # Only keep Fridays in data. Note that Fridays that fall on Feb 29 are missing for now
  weeks <- vacancies[!is.na(wlag_N) &
    # strftime is locale dependent i.e. on a german system it would yield "Freitag"
    # therefore wday is preferred
    wday(date) == 6 &
    date >= as.Date(start_date)]
  return(weeks)
}

save_indicators <- function(con, indicators, date = Sys.Date()) {
  affected <- DBI::dbSendStatement(
    con, paste(
      "INSERT INTO x28.indicator_history VALUES ($1, $2)",
      "ON CONFLICT (date) DO UPDATE SET values = excluded.values"
    ),
    params = list(date, indicators)
  )

  rows_affected <- DBI::dbGetRowsAffected(affected)
  DBI::dbClearResult(affected)
  rows_affected
}

save_stocks <- function(con, weeks, weeks_noga, weeks_isco, weeks_canton) {
  # Keep latest date only
  weeks <- weeks[date == max(date)]
  weeks_noga <- weeks_noga[date == max(date)]
  weeks_isco <- weeks_isco[date == max(date)]
  weeks_canton <- weeks_canton[date == max(date)]

  # Convert to list
  weeks_vec <- weeks[, N]
  names(weeks_vec) <- weeks[, source]

  weeks_vec %>% as.list()

  to_list <- function(x) {
    vec <- x[, N]
    names(vec) <- x[, source]
    as.list(vec)
  }

  indicators <- list(
    total = list(
      weeks_vec %>% as.list()
    ),
    noga = split(weeks_noga, by = "noga_letter") %>% lapply(to_list),
    isco = split(weeks_isco, by = "isco_1d") %>% lapply(to_list),
    canton = split(weeks_canton, by = "canton") %>% lapply(to_list)
  )
  # to JSON
  json_indicators <- jsonlite::toJSON(indicators)
  save_indicators(con, json_indicators)
}
