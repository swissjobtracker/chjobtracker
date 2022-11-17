library(data.table)
library(jsonlite)

dta <- read_json("1000_lines_from_archive.json")

library(xml2)

read_xml("API-Response-Example.xml")

dta <- read_json("API-Response-Example.json")

ddta <- as.data.table(dta$jobs)

xx <- XML::xmlParse("API-Response-Example.xml")
xxx <- XML::xmlToDataFrame(xx, stringsAsFactors = FALSE)

x28pw <- .rs.askForPassword("PASSWORD")
library(httr)
ptr <- content(GET(
  "https://api.x28.ch/export/api/jobs/updates/json?date=2022-02-28T14%3A43%3A37.133%2B0200",
  authenticate(
    "kofethzch",
    x28pw
  )
), as = "parsed", simplifyVector = TRUE) # <= thems the tickit

ptrj <- content(GET(
  "https://api.x28.ch/export/api/jobs/json",
  authenticate(
    "kofethzch",
    x28pw
  )
), as = "parsed", simplifyVector = TRUE)

writeLines(jsonlite::toJSON(ptr$jobs[[1]], pretty = TRUE, auto_unbox = TRUE), "inst/api_example/api_single_job.json")

qq <- parse_x28_chunk(ptr$jobs)
qqq <- parse_x28_chunk(ptrj$jobs)

ptr2 <- content(GET(
  "https://api.x28.ch/export/api/jobs/json?token=QWHAPoK6GrCQqLRbl87yAuDtnhlQBvvzmyp3PhUR0jJiJu9E",
  authenticate(
    "kofethzch",
    "PASSWORD"
  )
))

# qvestions:
# * which fields do we need
# * how much info do we need about companies
# * which metadata do we need
# * one time pull? periodic update? if so: frequency?
