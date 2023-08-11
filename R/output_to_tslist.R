


#' Convert data.table output of make_series to a ts list
#'
#' @param dt output from make_series
#' @param by name of category column (e.g. "noga_letter"). NULL for total
#'
#' @importFrom tsbox ts_xts
#' @import data.table
#' @export
output_to_tslist <- function(dt, by = NULL) {
  if (is.null(by)) {
    dt_long <- melt(dt, "date", c("raw", "clean", "companies", "sum_clean"))
    dt_long[, id := sprintf("total.total.%s", variable)]
  } else {
    dt_long <- melt(dt, c(by, "date"), c("raw", "clean", "companies", "sum_clean"))
    dt_long[, id := sprintf("%s.%s", get(by), variable)]
  }

  dt_long <- dt_long[, .(date, value, id)]

  # ts_xts returns a multi-column xts. as.list makes it into a list
  # of separate xts
  tsl <- as.list(tsbox::ts_xts(dt_long[, .(time = date, value, id)]))

  if (!is.null(by)) {
    names(tsl) <- tolower(sprintf("ch.kof.jobtracker.%s.%s.idx", by, names(tsl)))
  } else {
    names(tsl) <- tolower(sprintf("ch.kof.jobtracker.%s.idx", names(tsl)))
  }

  # share_clean is independent (i think)
  names(tsl) <- gsub("share_clean.idx$", "share_clean", names(tsl))
  tsl
}



