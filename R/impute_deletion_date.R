#' Impute Deletion Date where it is Missing
#'
#' @param ads Advertisement panel
#'
#' @details
#' Note: This functon modifies the original object passed to it.
#'
#' There is a small number of missings,
#' we just assign them the median duration of a posting: 28 days
#'
#' @export
impute_deletion_date <- function(ads) {
  ads[is.na(deleted) & created < (max(created) - 365),
      deleted :=created + 28]

  ads
}
