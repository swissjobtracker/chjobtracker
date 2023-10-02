#library(chjobtracker)
library(devtools)
library(timeseriesdb)
library(tsbox)
library(RPostgres)
devtools::load_all() # load the local version of the package

# Connect to the database where the vacancy data is stored
con <- dbConnect(Postgres(),
  dbname = "nrp77",
  host = "archivedb.kof.ethz.ch",
  user = Sys.getenv("PG_USER"), # store the username and password in the system environment
  password = Sys.getenv("PG_PASSWORD"),
  timezone = "Europe/Berlin"
)

# Connect to the main database where the index is stored
con_main <- db_connection_create("kofdb",
  Sys.getenv("PG_USER"), # store the username and password in the system environment
  "archivedb.kof.ethz.ch",
  passwd = Sys.getenv("PG_PASSWORD")
)



# some settings
START_DATE="2010-01-01"
date=NULL # compute the index for the last week
verbose = FALSE #  print some information about the progress
drop_lichtenstein = TRUE # drop ads from Lichtenstein from the computation

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
    weeks_not_flagged = 5, mad_factor = 4,
    start_date = START_DATE
  )

  prnt("Calculating total...")
  weeks <- get_stocks(ads_prepared, start_date = START_DATE)
  srs_tot <- make_series(ads_prepared, week_inclusion,start_date = START_DATE)

  prnt("Calculating by noga...")
  ads_noga <- ads_merge_noga(con, ads_prepared)
  weeks_noga <- get_stocks(
    ads_noga,
    by.cols = "noga_letter",
    start_date = START_DATE)

  srs_noga <- make_series(ads_noga, week_inclusion, by.cols = "noga_letter",start_date = START_DATE)
  setnames(srs_noga, "noga_letter", "noga")

  prnt("Calculating by isco...")
  ads_isco <- ads_merge_isco(con, ads_prepared)
  weeks_isco <- get_stocks(
    ads_isco,
    by.cols = "isco_1d",
    start_date = START_DATE
  )
  srs_isco <- make_series(ads_isco, week_inclusion, by.cols = "isco_1d", start_date = START_DATE)
  setnames(srs_isco, "isco_1d", "isco")

  prnt("Calculating by canton...")
  ads_canton <- ads_merge_canton(con, ads_prepared)
  weeks_canton <- get_stocks(
    ads_canton,
    by.cols = "canton",
    start_date = START_DATE
  )
  srs_canton <- make_series(ads_canton, week_inclusion, by.cols = "canton", start_date = START_DATE)

  
library(ggplot2)
ggplot(srs_tot, aes(x=date, y=clean)) + geom_line() +
scale_x_continuous(breaks = seq(as.Date("2010-01-01"), as.Date("2023-01-01"), by = "1 year")) 

out<-list(srs_tot=srs_tot,srs_noga=srs_noga,srs_isco=srs_isco,srs_canton=srs_canton)
saveRDS(out,"~/Downloads/export_zuchuat.rds")
