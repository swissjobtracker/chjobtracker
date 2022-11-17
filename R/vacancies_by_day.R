#' Vacancies by day
#'
#' TODO: Flesh out documentation
#'
#' @param ads_data
#' @param by
#'
#' @import data.table
#' @export
vacancies_by_day <- function(ads_data,
                             by.cols = NULL){
  if(!all(by.cols %in% names(ads_data))) {
    stop("Cannot calculate vacancies by day by ", by.cols, ". One or more of by.cols is missing.")
  }

  created_per_day <- ads_data[, .N, by = c("created", by.cols)]
  names(created_per_day)[1] <- "date"
  deleted_per_day <- ads_data[!is.na(deleted), .N, by = c("deleted", by.cols)]
  names(deleted_per_day)[1] <- "date"

  panel <- merge(created_per_day,
                 deleted_per_day,
                 by = c("date", by.cols),
                 all = TRUE,
                 suffixes = c("_created", "_deleted"))
  setorderv(panel, "date")

  all_the_dates <- panel[, {
    r <- range(date)
    seq(r[1], r[2], by = "day")
  }]

  grps <- lapply(panel[, by.cols, with = FALSE], unique)
  fullspine <- do.call(
    expand.grid,
    c(
      as.list(grps),
      list(
        date = all_the_dates,
        stringsAsFactors = FALSE
      )
    )
  )

  balanced <- merge(panel, fullspine, all = TRUE)
  balanced[is.na(balanced)] <- 0

  balanced[, job_postings := cumsum(N_created) - cumsum(N_deleted), by = by.cols]

  setcolorder(balanced, c(by.cols, "date"))
  setorderv(balanced, c(by.cols, "date"))
  balanced
}
