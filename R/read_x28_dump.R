#' Read an x28 elastic dump file
#'
#' Reads a ndjson dump file and converts it into a list of data.tables
#'
#' @param f The file to read
#' @param chunksize How many lines (records) to read at a time.
#'
#' @return A list of relational tables. See (README, vignette, w/e) for details
#' @export
#'
#' @details
#' chunksize can be used to limit how much memory the function needs at any given time.
#' Reading and parsing too many entries at once needs lots of memory, too few can be slower.
#'
#' @import data.table
#' @importFrom jsonlite fromJSON
read_x28_dump <- function(f, chunksize = 10000, n = Inf) {
  fid <- file(f, open = "r")

  cnt <- 0
  advertisements <- NULL
  advertisement_details <- NULL
  advertisement_metadata <- NULL
  advertisement_positions <- NULL
  advertisement_educations <- NULL
  advertisement_country <- NULL
  advertisement_canton <- NULL
  advertisement_postalcode <- NULL
  company_metadata <- NULL
  company_addresses <- NULL
  lines_processed <- 0
  while(lines_processed < n) {
    message(cnt <- cnt + 1)
    lines_processed <- lines_processed + chunksize
    message("reading")
    block <- readLines(fid, chunksize, encoding = "UTF-8")

    if(length(block) == 0) {
      message("aaand done")
      close(fid)
      break;
    }

    message("parsing json")
    l <- parse_x28_chunk(text = block)


    # todo: find a way to to rbindlist all at once
    message("collecting")
    advertisements <- rbindlist(list(advertisements, l$advertisements), use.names=TRUE)
    advertisement_details <- rbindlist(list(advertisement_details, l$advertisement_details), use.names=TRUE)
    advertisement_metadata <- rbindlist(list(advertisement_metadata, l$advertisement_metadata), use.names=TRUE)
    advertisement_positions <- rbindlist(list(advertisement_positions, l$advertisement_positions), use.names=TRUE)
    advertisement_educations <- rbindlist(list(advertisement_educations, l$advertisement_education_levels), use.names=TRUE)
    advertisement_country <- rbindlist(list(advertisement_country, l$advertisement_country), use.names=TRUE)
    advertisement_canton <- rbindlist(list(advertisement_canton, l$advertisement_canton), use.names=TRUE)
    advertisement_postalcode <- rbindlist(list(advertisement_postalcode, l$advertisement_postalcode), use.names=TRUE)
    company_metadata <- rbindlist(list(company_metadata, l$company_metadata), use.names=TRUE)
    company_addresses <- rbindlist(list(company_addresses, l$company_addresses), use.names=TRUE)
  }

  list(
    advertisements = advertisements,
    advertisement_details = advertisement_details,
    advertisement_metadata = advertisement_metadata,
    advertisement_positions = advertisement_positions,
    advertisement_education_levels = advertisement_educations,
    advertisement_country = advertisement_country,
    advertisement_canton = advertisement_canton,
    advertisement_postalcode = advertisement_postalcode,
    company_metadata = company_metadata,
    company_addresses = company_addresses
  )
}
