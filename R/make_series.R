
#' Title
#'
#' @param ads_data
#' @param by.cols
#' @param aggregate
#' @param start_date
#'
#' @return
#' @import data.table
#' @export
#'
#' @examples
#'

make_series <- function(ads,
                        week_inclusion,
                        by.cols = c(),
                        start_date = "2018-01-01", index_date="2020-01-01", average_over_x_days=31){
  # Some calculations need to be done by by.cols plus source
  by.cols.source <- union(by.cols, "source")

  # DON'T cap vacancy duration (this is why it is commented out)
  # ads[(is.na(deleted) & difftime(max(created), created, units = "days") > 180) |
  #       (difftime(deleted, created, units = "days") > 180),
  #              deleted := created + 180]

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

  # wow growth rate
  weeks[, wow := (N - wlag_N) / wlag_N]
  weeks[wlag_N==0, wow:=NA]
  weeks<-weeks[!is.na(wow) & is.finite(wow)]

  # include portal?
  weeks<-merge(weeks, week_inclusion, all.x=T, by=c("date", "source"))
  weeks[source=="company", include:=1 ]
  weeks[is.na(include), include:=0]
  weeks[, include:=as.numeric(include)]


  # compute weights for index calculation
  weeks[, weights_raw:=wlag_N/sum(wlag_N), by=c("date",by.cols)]
  weeks[, weights_company:=as.numeric(source=="company")]
  weeks[, weights_portal:=wlag_N*(source!="company")/sum(wlag_N*(source!="company")),  by=c("date",by.cols) ]
  weeks[, weights_clean:=(wlag_N*include)/sum(wlag_N*(include)),
        by=c("date",by.cols)]
  weeks[, weights_clean_portals:=wlag_N*include*(source!="company")/
          sum(wlag_N*include*(source!="company")),
        by=c("date",by.cols)]

 # compute index and also the number of ads it is based on
  index<-weeks[, .(raw=sum(weights_raw*(1+wow)),
                   companies=sum(weights_company*(1+wow)),
                   portals_raw=sum(weights_portal*(1+wow)),
                   clean=sum(weights_clean*(1+wow)),
                   clean_portals=sum(weights_clean_portals*(1+wow)),
                   sum_raw=sum(wlag_N),
                   sum_company=sum(wlag_N*(source=="company")),
                   sum_portal=sum(wlag_N*(source!="company")),
                   sum_clean=sum(wlag_N*(include)),
                   sum_clean_portals= sum(wlag_N*include*(source!="company"))),
               by=c("date", by.cols)]

  cols<-c("raw","companies","portals_raw", "clean", "clean_portals")
  index[, (cols) := lapply(.SD, function(x)cumprod(x)), by=by.cols, .SDcols=cols]

  # index to the average of the index start month
  for(c in cols){
    index[, (c):=get(c)/.SD[date %between% c(as.Date(index_date), as.Date(index_date)+average_over_x_days), mean(get(c))]*100, by=by.cols]
  }

  return(index)
}

