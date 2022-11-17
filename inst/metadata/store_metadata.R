md <- tstools::read_swissdata_meta("inst/ch.kof.jobtracker.yaml", "en", as_list = TRUE)

# straight out multiplying all combinations results in too many keys
# as total only exists in the total.total variety
md <- md[(!grepl("total", ts_key)) | grepl("total\\.total", ts_key)]

# remove the improper data
mdd <- lapply(md, function(x) {
  x[setdiff(names(x), c("Details", "Last update"))]
})




con <- kofutils::kof_dbconnect()
timeseriesdb::db_metadata_store(con, mdd, "1900-01-01", on_conflict = "overwrite")
timeseriesdb::db_metadata_store(con, mdd, "1900-01-01", "en", on_conflict = "overwrite")
