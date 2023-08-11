# THis function saves the indicators produced by save_stocks() to the database
save_indicators <- function(con, indicators, date = Sys.Date()) {
  affected <- DBI::dbSendStatement(
    con, paste(
      "INSERT INTO x28.indicator_history VALUES ($1, $2)",
      "ON CONFLICT (date) DO UPDATE SET values = excluded.values"
    ),
    params = list(date, indicators)
  )

  rows_affected <- DBI::dbGetRowsAffected(affected)
  DBI::dbClearResult(affected)
  rows_affected
}
