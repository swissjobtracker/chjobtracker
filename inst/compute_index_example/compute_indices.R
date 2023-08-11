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
date=NULL # compute the index for the last week
verbose = FALSE #  print some information about the progress
drop_lichtenstein = TRUE # drop ads from Lichtenstein from the computation

# this is the main function, see comments there
tsl <- generate_indicators(con, con_main, verbose = TRUE)

cat("generated!\n")

pblc_series <- tsl[grepl("clean\\.idx$", names(tsl))]
rest_series <- tsl[!(names(tsl) %in% names(pblc_series))]

message("Storing...")
db_ts_store(con_main, pblc_series, "timeseries_access_public", release_date = as.POSIXct(paste0(Sys.Date(), " 12:00:00")))
db_ts_store(con_main, rest_series, "timeseries_access_restricted")
db_ts_assign_dataset(con_main, names(tsl), "ch.kof.jobtracker_all")

# Recreate the collection in case keys get added/removed
message("Resetting collection...")
db_collection_delete(con_main, "ch.kof.jobtracker", "datenservice_public")
db_collection_add_ts(con_main, "ch.kof.jobtracker", names(pblc_series), user = "datenservice_public")

message("done!")

db_connection_close(con_nrp)
db_connection_close(con_main)
