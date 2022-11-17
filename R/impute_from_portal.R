#' Add column from_portal to ad data
#'
#' from_portal indicates whether an ad was (likely) posted on a job portal
#' rather than a company website.
#' This is assumed if an ad was posted on a domain hosting ads for less
#' than 100 different company_ids
#'
#' @param ads data.table Panel of job ads
#'
#' @import data.table
#' @export
impute_from_portal <- function(ads) {
  if(!all(c("company_id", "domain") %in% names(ads))) {
    stop("ads must have columns 'company_id' and 'domain'")
  }
  ads[, from_portal := uniqueN(company_id) > 100, by = domain]
}
