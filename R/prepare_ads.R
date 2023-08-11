# This function prepares the ads for analysis
# It does the following:
# 1. Infer whether ad is from a job portal or from a company domain (from_portal indicates whether an ad was (likely) posted on a job portal
#' rather than a company website. This is assumed if an ad was posted on a domain hosting ads for less than 100 different company_ids)
# 2. deduplicates ads
# 3. removes ads from recruitment agencies (according to the flag provided by X28)
# 4. adds a source column (wheter the ad is from a company domain or from a job portal, if from a job portal, which one)

prepare_ads <- function(ads, verbose = FALSE) {
  # TODO: use a piper here: |>

  ################################
  # from_portal
  ################################
  ads <- impute_from_portal(ads)

  ################################
  # duplicates
  ################################
  ads <- dedupe_ads(ads, verbose)

  ################################
  # recruitement_agencies
  ################################
  ads <- remove_recruitment_agencies(ads, verbose)

  ################################
  # add source col bunching up all company sources
  ################################
  ads <- add_source(ads)

  ads
}
