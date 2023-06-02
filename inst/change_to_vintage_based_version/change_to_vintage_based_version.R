#'
#' We want to switch the indices to a version wehere the stock is always at the
#'  real-time state and is not affected by potential revisions in the data that come later
#'  This is to avoid a temporary downwards bias due to vacancies that get found,
#'  then get lost but then found again by the crawler
#'
#'  Usually it takes 4-6 weeks for the stock to be constant again
#'
#'
library(devtools)
library(timeseriesdb)
library(jsonlite)
library(ggplot2)
#library(chjobtracker)

devtools::load_all()

verbose=T
drop_lichtenstein=F


prnt <- function(x) {
  if (verbose) message(x)
}



# Connections to db -------------------------------------------------------



con <- RPostgres::dbConnect(
  drv = RPostgres::Postgres(),
  host = "archivedb.kof.ethz.ch",
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASSWORD"),
  db = "nrp77"
)


con_main <- db_connection_create("kofdb",
                                 user = Sys.getenv("PG_USER"),
                                 "archivedb.kof.ethz.ch",
                                 passwd = Sys.getenv("PG_PASSWORD"))









# Download ads and run filter on today's version of vintages --------------


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

prnt("Run the filter algorithm, which portals are stable and can be included")
week_inclusion <- portal_filter_within_portal_hampler(
  ads_prepared,
  weeks_not_flagged = 5, mad_factor = 4
)



prnt("Define transition period")

dates_with_vintage_stocks <- DBI::dbGetQuery(
  con, "SELECT date FROM x28.indicator_history"
) %>% as.data.table()

fridays_w_vintages<-dates_with_vintage_stocks[wday(date)==6]
# cut the first one because there is a missing week in between

TRANSITION_PERIOD_START<-"2023-05-05"

fridays_w_vintages<-fridays_w_vintages[date>=TRANSITION_PERIOD_START]




prnt("Get vintage stocks for the transition period between the two versions")
vintage_stock_list<-lapply(fridays_w_vintages$date, function(x) get_vintage_stocks(con, x))


prnt("Compute the wow growth rates")

# Total
wow_tot<-lapply(2:length(vintage_stock_list), function(i){
  growth_rate_from_two_vintage_series(copy(vintage_stock_list[[i]]$total),
                                      copy(vintage_stock_list[[i-1]]$total),
                                      week_inclusion)
}) %>% rbindlist()

# Noga
wow_noga<-lapply(2:length(vintage_stock_list), function(i){
  growth_rate_from_two_vintage_series(copy(vintage_stock_list[[i]]$noga),
                                      copy(vintage_stock_list[[i-1]]$noga),
                                      week_inclusion, by.cols = "noga_letter")
}) %>% rbindlist()

# Isco
wow_isco<-lapply(2:length(vintage_stock_list), function(i){
  growth_rate_from_two_vintage_series(copy(vintage_stock_list[[i]]$isco),
                                      copy(vintage_stock_list[[i-1]]$isco),
                                      week_inclusion, by.cols = "isco_1d")
}) %>% rbindlist()

# Canton
wow_canton<-lapply(2:length(vintage_stock_list), function(i){
  growth_rate_from_two_vintage_series(copy(vintage_stock_list[[i]]$canton),
                                      copy(vintage_stock_list[[i-1]]$canton),
                                      week_inclusion, by.cols = "canton")
}) %>% rbindlist()



# convert them to the format of the vintage stocks
setnames(wow_noga, "noga_letter", "noga")
setnames(wow_isco, "isco_1d", "isco")

wow_with_keys<-c(output_to_tslist(wow_tot), output_to_tslist(wow_noga, by="noga"), output_to_tslist(wow_canton, by="canton"), output_to_tslist(wow_isco, by="isco"))
wow_with_keys <- lapply(wow_with_keys, function(x){
  a<-as.data.table(x, keep.rownames = "date")
  names(a)<-c("date", "value")
  a
}
) %>% rbindlist(idcol="key")





prnt("Get the index values for the weeks up to the transition period")
index_until_now<-get_indices_from_db(con_main)
index_trans_period_start<-index_until_now[date==TRANSITION_PERIOD_START,.(date_wlag=date, index_wlag=index, key)]

prnt("Update the index")
# by multiplying the latest value with the first growth rate
update<-merge(index_trans_period_start,
              wow_with_keys, by="key")

setorder(update, key, date)


# for the growth rate in the transistion period we take the cumulated product of the weekly growth rates
update[!(key %like%  "sum_clean"), wow:=cumprod(value), by=key]
# and then multiply with the intial value the week before the first wow in the new data
update[!(key %like%  "sum_clean") , index:=value*index_wlag]

# for the sums we just take the new value
update[key %like% "sum_clean", index:=value]



# This is the output ------------------------------------------------------

as.list(tsbox::ts_xts(update[, .(time=date, value=index, id=key)]))


# data table with all to plot ---------------------------

all_new<-rbind(index_until_now[date<=TRANSITION_PERIOD_START],
           update[, .(key, date, index)])

all_new<-rbind(all_new[, series:="New"],
               index_until_now[, series:="Old"])


keys_clean_idx<-all_new[key %like% "\\.clean\\.idx$", unique(key)]

# graph labels on fridays
fridays <- unique(all_new$date)

lapply(keys_clean_idx, function(key_x){
 ggplot(all_new[key==key_x & year(date)>=2023], aes(x=date, y=index, color=series))+
    geom_line(linewidth=1)+
    scale_x_date(breaks=fridays)+
    xlab(key_x)+
    theme(axis.text.x = element_text(angle=45,hjust=1))
  ggsave(paste0("inst/change_to_vintage_based_version/plots/",key_x,".png"), width=10, height=5)
})
