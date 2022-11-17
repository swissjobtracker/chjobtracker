#' Update or insert entries into a x28 table
#'
#' @param con RPostgres connection
#' @param x data.table of entries to add
#' @param table name of the table
#' @param field.types if necessary, field types to override automatically detected ones (see dbWriteTable)
#'
#' @importFrom RPostgres dbWriteTable dbExecute
x28_upsert_table <- function(con,
                            x,
                            table,
                            field.types = c()) {

  message("transferring data...")
  dbWriteTable(con,
               paste0(table, "_temp"),
               x,
               temporary = TRUE,
               overwrite = TRUE,
               field.types = field.types)

  constraint <- sprintf("%s_pkey", table)

  # quote the column names because one is called 'primary' which would cause a syntax error
  conflict <- paste('"', names(x), '"', " = EXCLUDED.", names(x), sep = "", collapse = ",\n")

  message("upserting rows...")
  dbExecute(con, sprintf("
                         INSERT INTO x28.%s
                         (SELECT *
                          FROM %s_temp)
                         ON CONFLICT ON CONSTRAINT %s
                         DO UPDATE
                         SET %s",
                         table, table, constraint, conflict))
}


#' Get IDs of advertisements for which we have newer info
#'
#' Since only the advertisements table has info on when an ad was updated we
#' need to get the ids for which we have newer info to filter the other tables.
#' An ad ID is considered newer if either the updated column in the passed table
#' is newer or if the id does not yet exist on the database.
#'
#' @param con RPostgres connection
#' @param advertisements advertisements table
#'
#' @return data.table(id = character) of ids to update
#' @export
#'
#' @examples
x28_get_ids_to_upsert <- function(con, advertisements) {
  dbExecute(con, "SET TIMEZONE = 'Europe/Berlin'")

  dbWriteTable(con,
               "x28_new_ids",
               advertisements[, .(id, updated)],
               temporary = TRUE,
               overwrite = TRUE,
               field.types = c(
                 id = "text",
                 updated = "timestamptz"
               ))

  dbGetQuery(con,
             "SELECT id
             FROM x28_new_ids
             LEFT OUTER JOIN x28.advertisements
             USING(id)
             WHERE x28_new_ids.updated > advertisements.updated
             OR advertisements.updated IS NULL")$id
}

#' Store a x28 object to db
#'
#' inserts any new entries in the tables of data and updates the rows
#' which already exist with records in data
#'
#' @param con RPostgres connection
#' @param data list of tables ("x28 object" returned by parse_x28_chunk)
#' @param tables which tables to store (default names(data) i.e. all of them)
#'
#' @export
#' @importFrom RPostgres dbBegin dbCommit dbRollback
x28_to_db <- function(con,
                      data,
                      tables = names(data)) {

  field.type_overrides <- list(
    advertisements = c(
      duplicategroup = "uuid",
      qualityscore = "integer")
  )

  # Column order to ensure they do not get mixed up when inserting if something
  # changes in the future
  columns <- list(
    advertisements = c(
      "id",
      "duplicategroup",
      "company_id",
      "origin",
      "created",
      "updated",
      "deleted",
      "language",
      "qualityscore",
      "workquota_minimum",
      "workquota_maximum",
      "temporary",
      "homeoffice",
      "url",
      "domain",
      "company_url",
      "company_domain",
      "company_name",
      "company_uid",
      "company_crn",
      "company_recruitment_agency",
      "company_size_id",
      "company_size_name",
      "company_size_min",
      "company_size_max"
    ),
    advertisement_details = c("advertisement_id", "title", "raw_text"),
    advertisement_metadata = c(
      "advertisement_id",
      "metadata_id",
      "type",
      "source",
      "name",
      "level",
      "phrase"
    ),
    advertisement_positions = c("advertisement_id", "position"),
    advertisement_education_levels = c("advertisement_id", "education"),
    advertisement_country = c("advertisement_id", "country"),
    advertisement_canton = c("advertisement_id", "canton"),
    advertisement_postalcode = c("advertisement_id", "postalcode"),
    company_metadata = c(
      "advertisement_id",
      "company_id",
      "metadata_id",
      "type",
      "name"
    ),
    company_addresses = c(
      "advertisement_id",
      "company_id",
      "country",
      "postalcode",
      "city",
      "primary"
    )
  )

  # Get ids for which we have new info
  # this is only really relevant when reading in new dumps
  # api data will always be newer
  newer_ids <- x28_get_ids_to_upsert(con, data$advertisements)

  tryCatch({
    dbBegin(con)
    for(t in tables) {
      message(t)
      dta <- data[[t]]
      if(t == "advertisements") {
        dta <- dta[id %in% newer_ids]
      } else {
        dta <- dta[advertisement_id %in% newer_ids]
      }
      x28_upsert_table(con, dta[, columns[[t]], with = FALSE], t, field.type_overrides[[t]])
    }
    dbCommit(con)
  },
  error = function(e) {
    message("Error! Rolling back.")
    dbRollback(con)
    stop(e)
  })
}

#' Append a record to x28.event_log
#'
#' @param con RPostgres connection
#' @param t POSIXct timestamp of the event
#' @param event name of the event
#' @param details details of the event e.g. 'automatic update'
#'
#' @export
#' @import data.table
#' @importFrom RPostgres dbAppendTable
#' @importFrom DBI Id
x28_db_log_event <- function(con,
                              t,
                              event,
                              details) {
  dbAppendTable(con, Id(schema = "x28", table = "event_log"), data.table(
    t = t,
    event = event,
    details = details
  ))
}
