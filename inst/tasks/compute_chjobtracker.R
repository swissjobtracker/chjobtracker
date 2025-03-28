library(chjobtracker)
library(tsbox)
library(koftsdb)
library(RPostgres)

con_nrp <- dbConnect(Postgres(),
  dbname = "nrp77",
  host = "archivedb.kof.ethz.ch",
  user = "kofdocker",
  password = Sys.getenv("PG_PASSWORD"),
  timezone = "Europe/Berlin"
)

con_main <- db_connection_create("kofdb",
  "kofdocker",
  "archivedb.kof.ethz.ch",
  passwd = Sys.getenv("PG_PASSWORD")
)

execution_date <- Sys.getenv("EXECUTION_DATE") |> as.Date()
if (is.na(execution_date)) {
  execution_date <- Sys.Date()
}

tsl <- generate_indicators(con_nrp, con_main, execution_date, verbose = TRUE)

cat("generated!\n")

pblc_series <- tsl[grepl("clean\\.idx$", names(tsl))]
rest_series <- tsl[!(names(tsl) %in% names(pblc_series))]

message("Storing...")
db_ts_store(con_main, pblc_series, "timeseries_access_public", release_date = as.POSIXct(paste0(execution_date, " 12:00:00")), valid_from = execution_date)
db_ts_store(con_main, rest_series, "timeseries_access_restricted", valid_from = execution_date)
db_ts_assign_dataset(con_main, names(tsl), "ch.kof.jobtracker_all")

# Recreate the collection in case keys get added/removed
message("Resetting collection...")
db_collection_delete(con_main, "ch.kof.jobtracker", "datenservice_public")
db_collection_add_ts(con_main, "ch.kof.jobtracker", names(pblc_series), user = "datenservice_public")

message("done!")

db_connection_close(con_nrp)
db_connection_close(con_main)
