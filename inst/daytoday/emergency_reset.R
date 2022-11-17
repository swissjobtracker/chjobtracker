library(data.table)
library(timeseriesdb)

con <- kofutils::kof_dbconnect()

# Get all keys in the public facing collection
keys <- db_collection_get_keys(con, "ch.kof.jobtracker", "datenservice_public")

# Alternatively to get all keys in the whole dataset:
# keys <- db_dataset_get_keys(con, "ch.kof.jobtracker_all")

# Get times those series were last stored
updates <- db_ts_get_last_update(con, keys)

# Filter out the keys that were stored today
# This is generally all of them but we do it this way to reduce the risk of deleting
# older vintages if this code is ran multiple times on the same day.
# CARE SHOULD STILL BE TAKEN to not run this script more often than necessary. ;)
to_trim <- updates[as.Date(updated) == Sys.Date(), ts_key]

# Remove the latest vintage of the given series
{
  db_ts_delete_latest_version(con, to_trim)
  # and remove the variables, again for safety
  rm(keys, updates, to_trim)
}
