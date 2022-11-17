#' Add Source col
#'
#' Adds an additional column "source" to the panel.
#' We consider all companies that are not recruitement platforms as
#' a single source which is indicated by this variable.
#'
#' @param ads Advertisement panel
#'
#' @return ads with an additional column "source"
add_source <- function(ads) {
  ads[, source := ifelse(from_portal, domain, "company")]
}
