#' Saves the latest stock of ad ("indicator") to the database
#' They are saved by three dimensions: source (portal or company domain), week and total or the industry, isco_1d or canton
#' The indicators are converted to a json string and saved to the database, where one row is one date
#' @param con A database connection
#' @param weeks A data.table with the number of ads per source and week
#' @param weeks_noga A data.table with the number of ads per source, week and noga letter
#' @param weeks_isco A data.table with the number of ads per source, week and isco_1d
#' @param weeks_canton A data.table with the number of ads per source, week and canton
#' @return the answer to the database query that stored the indicators
#'

save_stocks <- function(con, weeks, weeks_noga, weeks_isco, weeks_canton) {
  # Keep latest date only
  weeks <- weeks[date == max(date)]
  weeks_noga <- weeks_noga[date == max(date)]
  weeks_isco <- weeks_isco[date == max(date)]
  weeks_canton <- weeks_canton[date == max(date)]

  # Convert to list
  weeks_vec <- weeks[, N]
  names(weeks_vec) <- weeks[, source]

  weeks_vec %>% as.list()

  to_list <- function(x) {
    vec <- x[, N]
    names(vec) <- x[, source]
    as.list(vec)
  }

  indicators <- list(
    total = list(
      weeks_vec %>% as.list()
    ),
    noga = split(weeks_noga, by = "noga_letter") %>% lapply(to_list),
    isco = split(weeks_isco, by = "isco_1d") %>% lapply(to_list),
    canton = split(weeks_canton, by = "canton") %>% lapply(to_list)
  )
  # to JSON
  json_indicators <- jsonlite::toJSON(indicators)
  save_indicators(con, json_indicators) # this function saves the indicators to the database
}
