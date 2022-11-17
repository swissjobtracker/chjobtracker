csv_to_db <- function(path_in, con) {
  tables <- c(
    "advertisements",
    "advertisement_country",
    "advertisement_canton",
    "advertisement_postalcode",
    "advertisement_details",
    "advertisement_education_levels",
    "advertisement_details",
    "advertisement_positions",
    "company_addresses",
    "company_metadata"
  )

  fls <- file.path(path_in, sprintf("%s.csv", tables))

  message("reading csvs...")
  dta <- lapply(setNames(fls, nm = tables), fread)

  message("storing to db...")
  x28_to_db(con, dta)
}
