# Since the yearly dumps we god are partitioned by creation date, many ads
# created near the end of the year but only deleted in the next year have their
# deletion date missing.
# to fix this, Marco extracted the creation and deletion dates for all ads in the
# archive for us and this script transfers these into the populated database.

library(data.table)

# Paths "relative to" MTEC-KOF-S07
json <- readLines("D:/x28/dumps_raw/id_created_deleted_2022-08-25.json.gz")
json_parsed <- data.table(jsonlite::fromJSON(paste0("[", paste(json, collapse = ", "), "]")))

# the timestamps are stored without TZ on the db, so we need to manually adjust
# the ones in UTC to central europe
json_parsed[, is_utc := grepl("Z$", created)]

# Pass the data 'round through data.table's write/read functions
# because that is much faster and more accurate in parsing the timestamps
fwrite(json_parsed, "D:/x28/dumps_csv/id_created_deleted_2022-08-25.csv")

dt <- fread("D:/x28/dumps_csv/id_created_deleted_2022-08-25.csv")

# rejigger the times for UTC timestamps
dt[is_utc == TRUE, created := created + 3600]
dt[is_utc == TRUE, deleted := deleted + 3600]
dt[is_utc & created >= as.POSIXct("2022-03-27 02:00:00"), created := created + 3600]
dt[is_utc & deleted >= as.POSIXct("2022-03-27 02:00:00"), deleted := deleted + 3600]


ht <- rbind(head(dt), tail(dt))
con <- kofutils::kof_dbconnect(db_name = "nrp77")

library(RPostgres)
dbWriteTable(con,
             "deletefix",
             dt[, .(id = as.character(id), created, deleted)],
             temporary = TRUE,
             overwrite = TRUE,
             field.types = c(
              id = "text",
              created = "timestamp",
              deleted = "timestamp"
            ))

bb <- dbGetQuery(con, "SELECT * from deletefix limit 10")


cc <- dbGetQuery(con, "select deletefix.*,
                  advertisements.created as adcreated,
                  advertisements.deleted as addeleted
                from deletefix
                join x28.advertisements
                using(id)")
cc <- data.table(cc)
setcolorder(cc, c("id", "created", "adcreated", "deleted", "addeleted"))
cc

# note: though some ads have differing creation/deletion times than we
#       got from the full year dumps, Severin and Michael decided 1-2 hours
#       shift forward or backward is not worth the time.
#       thus id_created_deleted_2022-08-25 is taken to be the base truff
dbBegin(con)
dbExecute(con, "UPDATE x28.advertisements
          SET created = deletefix.created, deleted = deletefix.deleted
          FROM deletefix
          WHERE advertisements.id = deletefix.id")
dbGetQuery(con, "SELECT id, created, deleted FROM x28.advertisements limit 199")
dbCommit(con)
