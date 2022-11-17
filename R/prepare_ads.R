prepare_ads <- function(ads, verbose = FALSE) {
  # TODO: use a piper here: |>

  ################################
  # part III: from_portal
  ################################
  ads <- impute_from_portal(ads)

  ################################
  # part I: duplicates
  ################################
  ads <- dedupe_ads(ads, verbose)

  ################################
  # part IV: recruitement_agencies
  ################################
  ads <- remove_recruitment_agencies(ads, verbose)

  ################################
  # part V: add source col bunching up all company sources
  ################################
  ads <- add_source(ads)

  ads
}
