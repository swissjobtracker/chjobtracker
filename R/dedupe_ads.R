#' Remove Duplicates from an Advertisement Panel
#'
#' @param ads data.table of advertisement data from get_ad_data
#' @param verbose whether or not to print progress messages
#'
#' @details
#' In order for deduping to work, ads must have the following columns:
#' id
#' duplicategroup
#' domain
#' created
#' deleted
#' company_domain
#' company_recruitement_agency
#'
#' Additional columns may be included, they do not affect the outcome.
#'
#' @return a data.table of the same form of ads with duplicates removed
#' @export
#'
dedupe_ads <- function(ads, verbose = FALSE) {
  ads_names <- names(ads)
  columns_required <- c("id", "duplicategroup", "domain", "created",
                        "deleted", "company_domain", "company_recruitment_agency")
  missing_cols <- setdiff(columns_required, ads_names)
  if(length(missing_cols) > 0) {
    stop(sprintf("Some required columns are missing from ads:\n%s", paste(missing_cols, collapse = ", ")))
  }


  say <- function(x) {
    if(verbose) {
      message(x)
    }
  }

  say("Calculating number of ads per duplicategroup...")
  ads[, nDup := .N, by = duplicategroup]

  duplicates <- ads[nDup > 1]

  # If an ad has an entry directly from the company website, keep that
  # otherwise keep the whole bunch and unify the time range it was online
  # across that
  say("Removing duplicates that don't come from company site but one is available...")

  # jiggerypokery to keep the rows that are not from a portal in groups that have such or
  # otherwise if there are no such rows, mark all as viable
  duplicates[, group_has_ad_from_company := any(!from_portal), by = duplicategroup]
  duplicates <- duplicates[(!from_portal) == group_has_ad_from_company, ]
  duplicates[, group_has_ad_from_company := NULL]

  # Use the earliest time an ad was seen and the earliest time it was registered as removed
  # a bit counterintuitive but it works better that way cf. Jeremias Kläui
  say("Adjusting dates...")
  duplicates[, created := min(created, na.rm = TRUE), by = duplicategroup]
  duplicates[, deleted := suppressWarnings(min(deleted, na.rm = TRUE)), by = duplicategroup]

  # reconstruct NA deleted
  # min(c(NA, NA), na.rm = TRUE) is Inf, thus !is.finite
  duplicates[!is.finite(deleted), deleted := NA]

  say("Constructing output...")
  rbind(ads[nDup == 1], unique(duplicates, by= "duplicategroup"))[, nDup := NULL]
}
