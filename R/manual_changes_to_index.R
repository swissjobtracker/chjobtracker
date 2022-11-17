
#' Manually change some values (to NA)
#' in cases where we have some theoretical grounds to not publish them
#'
#' @param srs
#'
#' @return srs
#'

manual_changes_to_canton_indices<-function(srs){

  cols<-names(srs[, -c("date", "canton")])

  #' BL before Mid 2018
  #' We remove the values because there was a break in how ads that were
  #' attributable to both BL and BS were handled at the beginning of 2018
  #' From Mid 2018 on the merge is consistent

  srs[canton=="bl" & date<"2018-07-01", (cols):=NA]

  return(srs)
}
