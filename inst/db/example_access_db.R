library(devtools)
library(timeseriesdb)
#library(chjobtracker)

devtools::load_all()

verbose=T
drop_lichtenstein=F


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

