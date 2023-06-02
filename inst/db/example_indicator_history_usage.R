# These are a series of example queris that demonstrate the use of the
# indicator_history table.


con <- RPostgres::dbConnect(
        drv = RPostgres::Postgres(),
        host = "archivedb.kof.ethz.ch",
        user = Sys.getenv("PG_USER"),
        password = Sys.getenv("PG_PASSWORD"),
        db = "nrp77"
)

# Create the indicator with the appropriate structure. For example:

indicators <- list(
        noga = list(
                "J" = list(
                        "jobroom.ch" = 123,
                        "jobs.ch" = 456,
                        "total" = 579
                )
        ),
        isco = list(
                "1" = list(
                        "jobroom.ch" = 123,
                        "jobs.ch" = 456,
                        "total" = 579
                )
        ),
        canton = list(
                "fr" = list(
                        "jobroom.ch" = 123,
                        "jobs.ch" = 456,
                        "total" = 579
                )
        ),
        total = list(
                "jobroom.ch" = 123,
                "jobs.ch" = 456,
                "total" = 579
        )
)

json_indicators <- jsonlite::toJSON(indicators)

# **INSERT**
# Create a new row for the date:

affected <- DBI::dbSendStatement(
        con, "INSERT INTO x28.indicator_history VALUES ($1, $2)",
        params = list(Sys.Date(), json_indicators)
)

message(sprintf("Added %d rows", DBI::dbGetRowsAffected(affected)))
DBI::dbClearResult(affected)


# **UPDATE**
# Update an existing row - overwrite:

affected <- DBI::dbSendStatement(
        con, "UPDATE x28.indicator_history SET values = $2 WHERE date = $1",
        params = list(Sys.Date(), json_indicators)
)

message(sprintf("Updated %d rows", DBI::dbGetRowsAffected(affected)))
DBI::dbClearResult(affected)

# **DELETE**
# Remove values for a day

affected <- DBI::dbSendStatement(
        con, "DELETE FROM x28.indicator_history WHERE date = $1",
        params = list(Sys.Date())
)

message(sprintf("Deleted %d rows", DBI::dbGetRowsAffected(affected)))
DBI::dbClearResult(affected)


# Read values for a specific date

db_result <- DBI::dbGetQuery(
        con, "SELECT values FROM x28.indicator_history WHERE date = $1",
        params = list(as.Date("2023-05-05"))
)

previous_indicators <- jsonlite::fromJSON(db_result$values)




# Read all values

db_result <- DBI::dbGetQuery(
  con, "SELECT * FROM x28.indicator_history"
)

previous_indicators <- jsonlite::fromJSON(db_result$values)
