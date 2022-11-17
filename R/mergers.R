#' Merge x28 Job IDs onto an Ad Panel
#'
#' If no ads is supplied, reads the last filtered panel from x28.filtered_advertisements
#'
#' @param con RPostgres connection
#' @param ads data.table Ad panel with at least id
#'
#' @importFrom RPostgres dbGetQuery
#' @return
#' data.table with the same columns as ads plus job_id
get_job_ids <- function(con) {
    job_ids <- data.table(dbGetQuery(con,
                                     "SELECT advertisement_id as id, metadata_id::INTEGER as job_id
                                     FROM x28.advertisement_metadata
                                     WHERE type = 'JOB'"))
}

#' Merge x28 Company Industry IDs onto an Ad Panel
#'
#' If no ads is supplied, reads the last filtered panel from x28.filtered_advertisements
#'
#' @param con RPostgres connection
#' @param ads data.table Ad panel with at least id
#'
#' @importFrom RPostgres dbGetQuery
#'
#' @return
#' data.table with the same columns as ads plus industry_id
get_industry_ids <- function(con) {
    industries <- data.table(dbGetQuery(con,
                                        "SELECT advertisement_id as id, metadata_id::INTEGER as industry_id
                                        FROM x28.company_metadata
                                        WHERE type = 'INDUSTRY'"))
}

#' Merge x28 Cantons onto Ad Panel
#'
#' If no ads is supplied, reads the last filtered panel from x28.filtered_advertisements
#'
#' @param con RPostgres connection
#' @param ads data.table Ad panel with at least id
#'
#' @return
#' @importFrom RPostgres dbGetQuery
get_ads_w_canton <- function(con, ads = NULL) {
  if(!is.null(ads)) {
    cantons <- data.table(dbGetQuery(con,
                                     "SELECT advertisement_id as id, LOWER(canton) as canton
                                     FROM x28.advertisement_canton"))
    merge(ads, cantons, by = "id")
  } else {
    dbGetQuery(con, "
                    SELECT * FROM x28.filtered_advertisements
                    JOIN (
                      SELECT advertisement_id as id, LOWER(canton) as canton
                      FROM x28.advertisement_canton
                    ) cantons
                    USING(id)")
  }
}

#' Merge SBN IDs onto an Ad Panel
#'
#' If no ads is supplied, reads the last filtered panel from x28.filtered_advertisements
#'
#' @param con RPostgres connection
#' @param ads data.table Ad panel with at least id
#'
#' @return data.table with the same columns as ads plus namex28, codesbn, namesbn
#'
#' @import data.table
#' @export
ads_merge_sbn <- function(con, ads = NULL) {
  merge(get_ads_w_job_ids(con, ads), x28_sbn, by.x = "job_id", by.y = "idx28")
}

#' Merge ISCO Job IDs onto an Ad Panel
#'
#'
#' @param con RPostgres connection
#' @param ads data.table Ad panel with at least id
#'
#' @return data.table with the same columns as ads plus namex28, isco_1d, isco_long, nameisco
#'
#' @import data.table
#' @export
ads_merge_isco <- function(con, ads = NULL) {
  jobids<-get_job_ids(con)



  mapping<-merge(jobids,
                 unique(x28_isco[, .(job_id=idx28, isco_1d)]),
                 by="job_id",
                 all.x=T,
                 allow.cartesian = TRUE)
  mapping<-unique(mapping[, .(id, isco_1d)])
  # mapping[, .N, by=id][, summary(N)]
  # Min. 1st Qu.  Median    Mean 3rd Qu.    Max.
  # 1.000   1.000   1.000   1.111   1.000  10.000
  merge(ads, mapping)
}

#' Merge NOGA codes onto an Ad Panel
#'
#'
#' @param con RPostgres connection
#' @param ads data.table Ad panel with at least id
#'
#' @return data.table with the same columns as ads plus namex28, noga_letter, noga_long, namenoga
#'
#' @import data.table
#' @export
ads_merge_noga <- function(con, ads) {
  mapping<-merge(get_industry_ids(con),
                 unique(x28_noga[, .(industry_id=idx28, noga_letter)]),
                 by="industry_id",
                 all.x=T,
                 allow.cartesian = TRUE)
  mapping<-unique(mapping[, .(id, noga_letter)])
  #mapping[, .N, by=id][, summary(N)]
  # Min. 1st Qu.  Median    Mean 3rd Qu.    Max.
  # 1.00    1.00    1.00    1.34    1.00    9.00

  merge(ads, mapping)
}

#' Merge 2 Letter canton codes onto an Ad Panel
#'
#' @param con RPostgres connection
#' @param ads data.table Ad panel with at least id
#'
#' @return data.table with the same columns as ads plus canton
#' @export
ads_merge_canton <- function(con, ads = NULL) {
  get_ads_w_canton(con, ads)
}
