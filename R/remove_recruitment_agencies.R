remove_recruitment_agencies <- function(ads, verbose = FALSE) {
  if(verbose) {
    message("Removing recruitment agencies")
  }

  ads[company_recruitment_agency == FALSE]
}
