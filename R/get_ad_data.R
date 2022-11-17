#' Title
#'
#' @param con RPostgres connection object
#' @param start character or Date object of start of observations to get (created >= start)
#' @param end character or date object of end of observations to get (created <= end)
#' @param columns which columns to select
#' @param schema character schema name
#' More params TBD, don't use too widely yet.
#'
#' @details
#' You should always strive to select the least number of columns possible to save on memory.
#'
#' @return
#' @export
#'
#' @import data.table
#' @importFrom RPostgres dbGetQuery dbQuoteIdentifier
get_ad_data <- function(con,
                        start = "2016-01-01",
                        end = Sys.Date(),
                        columns = c("id",
                                    "duplicategroup",
                                    "company_id",
                                    "company_domain",
                                    "company_recruitment_agency",
                                    "domain",
                                    "created",
                                    "updated",
                                    "deleted"
                                    ),
                        schema = "x28") {
  dbExecute(con, "SET TIMEZONE = 'Europe/Berlin'")

  cols_quoted <- sapply(columns, dbQuoteIdentifier, conn = con)
  date_cols <- c('"created"', '"updated"', '"deleted"')
  cols_quoted[cols_quoted %in% date_cols] <- sprintf("%s::DATE", cols_quoted[cols_quoted %in% date_cols])

  query <- sprintf("SELECT %s FROM %s.advertisements
                   WHERE (deleted >= $1  OR deleted IS NULL)
                   AND created <= $2",
                   paste(cols_quoted, collapse = ", "),
                   dbQuoteIdentifier(con, schema))
  as.data.table(dbGetQuery(con,
                           query,
                           params = list(
                             start,
                             end
  )))
}
