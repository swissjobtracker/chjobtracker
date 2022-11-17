#' Create Numerical Date
#'
#' Creates a fully numeric date notation based on a YYYY-MM-DD POSIXct date.
#'
#' @importFrom data.table year month
#' @export
# TODO: If at all possible I would avoid such a "data structure"
create_num_date <- function(d){
  10000*year(d) + 100*month(d) + mday(d)
}

#' Extraction Helper for Root Domain
#'
#' @param url
#' some sites hop subdomains over time which makes them harder to identify
#' -> use bare root domain for that purpose
#'
#' @export
#' @importFrom urltools domain
get_root_domain <- function(url) {
  d <- domain(url)
  dd <- strsplit(d, ".", fixed = TRUE)
  sapply(dd, function(x) {
    l <- length(x)
    paste0(x[(l-1):l], collapse = ".")
  })
}


#' Helper to parse the timestamp strings from x28 api
#'
#' parse ISO8601 is probly a bit ambitious of a name
#'
#' @param x character vector
#'
#' @export
#'
#' @examples
#' parse_ISO8601("2022-03-01T14:57:13.839+01:00")
parse_ISO8601 <- function(x) {
  as.POSIXct(
    paste0(gsub("(T|\\.\\d+|:00$)", "", x),
           "00"),
    format = "%Y-%m-%d%H:%M:%S%z")
}

#' Helper to consistently parse content from api responses
#'
#' @param response
#'
#' @export
#' @importFrom httr content
get_api_response_content <- function(response) {
  content(response, as = "parsed", simplifyVector = TRUE, encoding = "UTF-8")
}

#' Helper to combine a list of x28 data sets into one
#'
#' @param x list of lists of tables from parse_x28_chunk and family
#'
#' @export
#' @import data.table
combine_x28_data <- function(x) {
  tables <- names(x[[1]])

  lapply(setNames(nm = tables), function(t) {
    unique(rbindlist(lapply(x, '[[', t), fill = TRUE, use.names = TRUE))
  })
}

#' Helper to quickly write x28 data to disk
#'
#'
#' @param x a list of tables from parse_x28_chunk and family
#' @param path_out folder to put the files in
#'
#' @export
#' @import data.table
write_x28_tables <- function(x, path_out) {
  tables <- names(x)

  for(tab in tables) {
    message(sprintf("writing %s.csv", tab))
    fwrite(x[[tab]], file.path(path_out, sprintf("%s.csv", tab)))
  }
}

#' Counterpart to write_x28_tables
#'
#' @param path_in path to csvs
#' @export
#' @import data.table
read_x28_csv <- function(path_in) {
    tables <- c("advertisements",
                "advertisement_details",
                "advertisement_metadata",
                "advertisement_positions",
                "advertisement_education_levels",
                "advertisement_country",
                "advertisement_canton",
                "advertisement_postalcode",
                "company_metadata",
                "company_addresses")

    out <- lapply(setNames(nm = tables), function(t) {
      message(sprintf("reading %s.csv", t))
      fread(file.path(path_in, paste0(t, ".csv")))
    })

    # When reading backlogs where no ads have a deleted set, the column
    # comes out as NA/logical but it needs to be POSIXct
    if(mode(out$advertisements$deleted) == "logical") {
      out$advertisements$deleted <- as.POSIXct(out$advertisements$deleted, origin = "1970-01-01")
    }

    out
}

#' Helper for figuring out what the latest observation in the given data is
#'
#' @param data list of x28 tables
#'
#' @export
get_x28_latest_timestamp <- function(data) {
  if(is.null(data$advertisements)) {
    stop("data does not contain an element 'advertisements'. Don't know what to do with that.")
  }

  # Suppress "no non-missing arguments" warning message
  suppressWarnings(
    data$advertisements[, max(
      max(created),
      max(updated, na.rm = TRUE),
      max(deleted, na.rm = TRUE)
    )]
  )
}
