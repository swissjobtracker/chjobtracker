#' Generate the indicator series
#'
#' @param con RPostgres connection
#'
#' @return a tslist with all indicator series
#' @export
generate_indicators <- function(con, verbose = FALSE, drop_lichtenstein = TRUE) {
  prnt <- function(x) {
    if(verbose) message(x)
  }

  prnt("Loading data...")
  ads <- get_ad_data(con)

  if(drop_lichtenstein) {
    prnt("Dropping Lichtenstein...")
    prnt("Amending data with geo information...")
    ads_w_geo <- get_ads_w_canton(con, ads)
    prnt("Dropping ads from Lichtenstein...")
    ads_from_li_only <- ads_w_geo[, .(.N == 1 && canton == "li"), by = "id"][V1 == TRUE, id]
    ads <- ads[!(id %in% ads_from_li_only)]
  }

  ads_prepared <- prepare_ads(ads, verbose)

  # (source, date) pairs to include
  # date will only be fridays. do further restrict to only "clean" portals
  # use only rows with include == TRUE
  week_inclusion<-portal_filter_within_portal_hampler(ads_prepared, weeks_not_flagged = 5, mad_factor = 4)

  prnt("Calculating total...")
  srs_tot <- make_series(ads_prepared, week_inclusion)

  prnt("Calculating by noga...")
  srs_noga <- make_series(ads_merge_noga(con, ads_prepared), week_inclusion, by.cols = "noga_letter")
  setnames(srs_noga, "noga_letter", "noga")

  prnt("Calculating by isco...")
  srs_isco <- make_series(ads_merge_isco(con, ads_prepared), week_inclusion, by.cols = "isco_1d")
  setnames(srs_isco, "isco_1d", "isco")

  prnt("Calculating by canton...")
  srs_canton <- make_series(ads_merge_canton(con, ads_prepared), week_inclusion, by.cols = "canton")


  prnt("Manual changes to indices (removing values that don't have publishable quality)")
  srs_canton<-manual_changes_to_canton_indices(srs_canton)

  prnt("Converting to ts...")
  tsl_tot <- output_to_tslist(srs_tot)

  tsl_noga <- output_to_tslist(srs_noga, "noga")

  tsl_isco <- output_to_tslist(srs_isco, "isco")

  tsl_canton <- output_to_tslist(srs_canton, "canton")


  c(
    tsl_tot,
    tsl_noga,
    tsl_isco,
    tsl_canton
  )
}


#' Convert data.table output of make_series to a ts list
#'
#' @param dt output from make_series
#' @param by name of category column (e.g. "noga_letter"). NULL for total
#'
#' @importFrom tsbox ts_xts
#' @import data.table
#' @export
output_to_tslist <- function(dt, by = NULL) {
  if(is.null(by)) {
    dt_long <- melt(dt, "date", c("raw", "clean", "companies", "sum_clean"))
    dt_long[, id := sprintf("total.total.%s", variable)]
  } else {
    dt_long <- melt(dt, c(by, "date"), c("raw", "clean", "companies", "sum_clean"))
    dt_long[, id := sprintf("%s.%s", get(by), variable)]
  }

  dt_long <- dt_long[, .(date, value, id)]

  # ts_xts returns a multi-column xts. as.list makes it into a list
  # of separate xts
  tsl <- as.list(ts_xts(dt_long[, .(time = date, value, id)]))

  if(!is.null(by)) {
    names(tsl) <- tolower(sprintf("ch.kof.jobtracker.%s.%s.idx", by, names(tsl)))
  } else {
    names(tsl) <- tolower(sprintf("ch.kof.jobtracker.%s.idx", names(tsl)))
  }

  # share_clean is independent (i think)
  names(tsl) <- gsub("share_clean.idx$", "share_clean", names(tsl))
  tsl
}
