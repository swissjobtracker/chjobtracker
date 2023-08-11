#' This function computes the stock of vacancies by week, source and (if specified) by other columns such as canton, occupation or industry
get_stocks <- function(ads,
                       by.cols = c(),
                       start_date = "2018-01-01") {
  # Some calculations need to be done by by.cols plus source
  by.cols.source <- union(by.cols, "source")
  # calculate actual daily stock of ads
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
