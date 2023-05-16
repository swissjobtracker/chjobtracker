library(data.table)
library(magrittr)

#' Generate the indicator series
#'
#' @param con RPostgres connection
#'
#' @return a tslist with all indicator series
#' @export
generate_indicators <- function(con, con_main, verbose = FALSE, drop_lichtenstein = TRUE) {
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

  prnt("Adding canton, occupation and indutry to the ads")
  ads_noga <- ads_merge_noga(con, ads_prepared)
  ads_isco <- ads_merge_isco(con, ads_prepared)
  ads_canton <- ads_merge_canton(con, ads_prepared)


  prnt("Compute this week's stock of ads by source")
  stocks <- get_stocks(ads_prepared)
  stocks_noga <- get_stocks(ads_noga,by.cols = "noga_letter")
  stocks_isco <- get_stocks(ads_isco,by.cols = "isco_1d")[, isco_1d:=as.character(isco_1d)]
  stocks_canton <- get_stocks(ads_canton, by.cols = "canton")

  prnt("Save the stocks to the DB, they will be next week's vintage lagged stock")
  affected <- save_stocks(con, stocks, stocks_noga, stocks_isco, stocks_canton)
  prnt(sprintf("Added %d rows", affected))



  prnt("Get the vintage stocks from last week")
  newest_date<-weeks[, max(date)] # which is the date of last week?
  prnt(paste("We compute the index for", newest_date, "using the vintage stock from", newest_date-7))
  vintage_stocks<-get_vintage_stocks(con, newest_date-7)


  prnt("Run the filter algorithm, which portals are stable and can be included")
  week_inclusion <- portal_filter_within_portal_hampler(
    ads_prepared,
    weeks_not_flagged = 5, mad_factor = 4
  )


  prnt("Compute the growth rate between today's and last weeks stock using the included portals")
  wow_tot<-growth_rate_from_stocks(stocks[date==newest_date], vintage_stocks$total, week_inclusion)
  wow_noga<-growth_rate_from_stocks(stocks_noga[date==newest_date], vintage_stocks$noga, week_inclusion, by.cols = c("noga_letter"))
  setnames(wow_noga, "noga_letter", "noga")
  wow_isco<-growth_rate_from_stocks(stocks_isco[date==newest_date], vintage_stocks$isco, week_inclusion, by.cols = c("isco_1d"))
  setnames(wow_isco, "isco_1d", "isco")
  wow_canton<-growth_rate_from_stocks(stocks_canton[date==newest_date],  vintage_stocks$canton, week_inclusion, by.cols = c("canton"))

  # convert them to the format of the vintage stocks
  wow_with_keys<-c(output_to_tslist(wow_tot), output_to_tslist(wow_noga, by="noga"), output_to_tslist(wow_canton, by="canton"), output_to_tslist(wow_isco, by="isco"))
  wow_with_keys <- lapply(wow_with_keys, function(x){
    a<-as.data.table(x, keep.rownames = "date")
    names(a)<-c("date", "value")
    a
  }
  ) %>% rbindlist(idcol="key")



  prnt("Get the index values for the weeks up to last week")
  index_until_now<-get_indices_from_db(con_main)
  index_last_week<-index_until_now[date==newest_date-7,.(date_wlag=date, index_wlag=index, key)]

  prnt("Compute today's index")
  # by multiplying the latest value with the growth rate
  update<-merge(index_last_week,
                wow_with_keys, by="key")
  # for the sum we just take the new value
  update[key %like% "sum_clean", index:=value]
  # for the indices we multiply the last value with the growth rate
  update[!(key %like%  "sum_clean"), index:=value*index_wlag]



  # prnt(paste(
  #     "Manual changes to indices (removing values that don't have",
  #     "publishable quality)"))
  # srs_canton <- manual_changes_to_canton_indices(srs_canton)
  # TODO: We have to implement that when we compute the one-off index



  prnt("Converting to ts...")
  as.list(tsbox::ts_xts(update[, .(time=date, value=index, id=key)]))
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


get_vintage_stocks <- function(con, date=Sys.Date()-7){

  db_result <- DBI::dbGetQuery(
    con, "SELECT values FROM x28.indicator_history WHERE date = $1",
    params = list(as.Date(date))
  )

 # format the jsons as a list of data.tables
 list<-jsonlite::fromJSON(db_result[1,1])
 out<-list()
 out[["total"]]<-list$total %>% t() %>% as.data.table(keep.rownames = "source") %>% cbind(list(vintage_date=date))
 names(out[["total"]])[2]<-"N_vintage"
 names(out[["total"]])[3]<-"vintage_date"
 out[["total"]][, N_vintage:=as.numeric(N_vintage)]

 out[["canton"]]<-list$canton %>% lapply(function(x){
   rbindlist(lapply(x, function(N_vintage) cbind(as.data.table(N_vintage), as.data.table(list("vintage_date"=date)))), idcol = "source")
 }) %>% rbindlist(idcol = "canton")
 out[["canton"]][, N_vintage:=as.numeric(N_vintage)]

 out[["noga"]]<-list$noga %>% lapply(function(x){
   rbindlist(lapply(x, function(N_vintage) cbind(as.data.table(N_vintage), as.data.table(list("vintage_date"=date)))), idcol = "source")
 }) %>% rbindlist(idcol = "noga_letter")
 out[["noga"]][, N_vintage:=as.numeric(N_vintage)]


 out[["isco"]]<-list$isco %>% lapply(function(x){
   rbindlist(lapply(x, function(N_vintage) cbind(as.data.table(N_vintage), as.data.table(list("vintage_date"=date)))), idcol = "source")
 }) %>% rbindlist(idcol = "isco_1d")
 out[["noga"]][, N_vintage:=as.numeric(N_vintage)]


 return(out)
}
