library(data.table)
library(magrittr)

#' Describe the generation of the jobtracker index
#'
#' This function is used to generate the jobtracker index. It is used in the
#' \code{compute_indices.R} script.
#'
#'
#' @export
#' @param con A database connection to the NRP77 database
#'  (archivedb.kof.ethz.ch)
#' @param con_main A database connection to the main database
#' (archivedb.kof.ethz.ch)
#' @param date The date for which the index should be computed.
#' If \code{NULL} the index is computed for the last week.
#' @param verbose If \code{TRUE} the function prints some information about
#' the progress.
#' @param drop_lichtenstein If \code{TRUE} ads from Lichtenstein are dropped
#' from the computation.
generate_indicators <- function(con, con_main, date = NULL, verbose = FALSE, drop_lichtenstein = TRUE) {
  # Function to print messages if verbose is TRUE
  prnt <- function(x) {
    if (verbose) message(x)
  }
  if(is.null(date)) {
    date <- Sys.Date()
  }
  date <- as.Date(date)
  if(is.na(date)) {
    date <- Sys.Date()
  }


  # Load the vacancy data from the database
  prnt("Loading data...")
  ads <- get_ad_data(con, end=date)

  # Drop ads from Lichtenstein
  if (drop_lichtenstein) {
    prnt("Dropping Lichtenstein...")
    prnt("Amending data with geo information...")
    ads_w_geo <- get_ads_w_canton(con, ads)
    prnt("Dropping ads from Lichtenstein...")
    ads_from_li_only <- ads_w_geo[, .(.N == 1 && canton == "li"), by = "id"][V1 == TRUE, id]
    ads <- ads[!(id %in% ads_from_li_only)]
  }

  # The ads data.table has the following columns:
  # id, duplicategroup, company_id, company_domain, company_recruitment_agency,domain, created, updated, deleted



  # Prepare the data
  # This function prepares the ads for analysis
  # It does the following:
  # 1. Infer whether ad is from a job portal or from a company domain (from_portal indicates whether an ad was (likely) posted on a job portal
  #' rather than a company website. This is assumed if an ad was posted on a domain hosting ads for less than 100 different company_ids)
  # 2. deduplicates ads
  # 3. removes ads from recruitment agencies (according to the flag provided by X28)
  # 4. adds a source column (wheter the ad is from a company domain or from a job portal, if from a job portal, which one)
  ads_prepared <- prepare_ads(ads, verbose)

  prnt("Adding canton, occupation and industry to the ads")
  # Merge the ads with information about the canton, occupation and industry
  # We have separate data.tables for each of these three dimensions cause an
  # ad might correspond to more than one industry or occupation
  ads_canton <- ads_merge_canton(con, ads_prepared)
  ads_isco <- ads_merge_isco(con, ads_prepared)
  ads_noga <- ads_merge_noga(con, ads_prepared)


  print(max(ads_prepared$created))
  print(max(ads_prepared$updated))
  prnt("Compute this week's stock of ads by source")
  #' Compute the stock of ads for each source (all company domains together and by job portal)
  #' Also compute them by the three dimensions canton, occupation and industry
  stocks <- get_stocks(ads_prepared)
  stocks_noga <- get_stocks(ads_noga, by.cols = "noga_letter")
  stocks_isco <- get_stocks(ads_isco, by.cols = "isco_1d")[, isco_1d := as.character(isco_1d)]
  stocks_canton <- get_stocks(ads_canton, by.cols = "canton")


  #' Handle the date: which is the date of last week?
  #' If date is NULL (i.e if we haven't specified a date for the index computation = the default),
  #' we compute the index for the last week
  if (is.null(date)) {
    newest_date <- stocks[, max(date)]
  } else {
    newest_date <- date
  }
  #' If there are no ads for the index computation date, we stop the computation
  if (min(
    stocks[date == newest_date, .N],
    stocks_noga[date == newest_date, .N],
    stocks_canton[date == newest_date, .N],
    stocks_isco[date == newest_date, .N]
  ) == 0) {
    stop("No ads found for the index computation date")
  }

  # tell the user the newest date and what we do
  prnt(paste0("We will compute the index growth rate between ", newest_date, " and ", newest_date - 7, " using the saved (vintage) stocks from ", newest_date - 7))



  # First step: save the stock for each source from this week to the database,
  # they will be used for computation next week (cause next week this week's stock is the vintage stock)
  # See save_stocks
  prnt("Save the stocks to the DB, they will be next week's vintage lagged stock")
  query <- save_stocks(con, stocks, stocks_noga, stocks_isco, stocks_canton)
  prnt(sprintf("Added %d rows", query))




  #' Second step, now we get the vintage stocks from last week
  prnt("Get the vintage stocks from last week")
  vintage_stocks <- get_vintage_stocks(con, newest_date - 7)

  #' Third step: The filter algorithm to determine which portals are stable and can be included
  #' For now all the stocks of ads are saved separately by portal
  #' We use these series of values to determine which portals are stable and can be included
  #' We use a modified version of the Hampel filter, see our documentation for details
  #' The parameters where chosen to match the stock of vacancies computed by the BFS
  #' We use the median absolute deviation (MAD) as a measure of dispersion
  #'
  #' The filter is run using the aggregated stock of ads by portal (the canton, occupation and industry dimensions are ignored here)
  #' See also the comments in the function portal_filter_within_portal_hampler
  #'
  #' The function returns a data.table with the following columns:
  #' data, source (the portal),
  #' include (whether the portal is stable and can be included in the index computation, NA is treated as FALSE),
  #' filter_deleted (whether the deletion of ads was the reason for the portal not being included, this is not needed but can be nice to know),
  #' filter_created (whether the creation of ads was the reason for the portal not being included, this is not needed but can be nice to know)
  #'
  prnt("Run the filter algorithm, which portals are stable and can be included")
  week_inclusion <- portal_filter_within_portal_hampler(
    ads_prepared,
    weeks_not_flagged = 5, # if a portal is flagged as unstable it remains excluded from the index computation for 5 weeks
    mad_factor = 4 # the factor on the median absolute deviation for the Hampel filter
  )

  # compute the growth rate between this week's stock and last week's stock using the portals that are stable and can be included
  # the function aggregates the stock of ads over (stable) sources and then computes the growth rate
  # see the function for details
  prnt("Compute the growth rate between today's and last weeks stock using the included portals only")
  wow_tot <- growth_rate_from_stocks(stocks[date == newest_date], vintage_stocks$total, week_inclusion)
  wow_noga <- growth_rate_from_stocks(stocks_noga[date == newest_date], vintage_stocks$noga, week_inclusion, by.cols = c("noga_letter"))
  setnames(wow_noga, "noga_letter", "noga") # rename the column
  wow_isco <- growth_rate_from_stocks(stocks_isco[date == newest_date], vintage_stocks$isco, week_inclusion, by.cols = c("isco_1d"))
  setnames(wow_isco, "isco_1d", "isco") # rename the column
  wow_canton <- growth_rate_from_stocks(stocks_canton[date == newest_date], vintage_stocks$canton, week_inclusion, by.cols = c("canton"))

  # convert the growth rates to the format of the vintage stocks
  wow_with_keys <- c(output_to_tslist(wow_tot), output_to_tslist(wow_noga, by = "noga"), output_to_tslist(wow_canton, by = "canton"), output_to_tslist(wow_isco, by = "isco"))
  wow_with_keys <- lapply(wow_with_keys, function(x) {
    a <- as.data.table(x, keep.rownames = "date")
    names(a) <- c("date", "value")
    a
  }) %>% rbindlist(idcol = "key")

  prnt("Get the index values for the weeks up to last week")
  #' Get the index values for the weeks up to last week
  #' (the KOF time series DB doesnt allow for just one entry to be updated, so we have to get the whole series and rewrrite it as a whole)
  index_until_now <- get_indices_from_db(con_main)

  # append the new growth factor to last week's index value
  prnt("Compute today's index")
  index_last_week <- index_until_now[!is.na(index) & date == newest_date - 7, .(date_wlag = date, index_wlag = index, key)]
  if (nrow(index_last_week) == 0) {
    message(
      "Value for last week is missing. Using the last available value instead."
    )
    index_last_week <- index_until_now[
      !is.na(index) & date <= newest_date - 7,
    ][
      date == max(date),
      .(date_wlag = date, index_wlag = index, key)
    ]
  }


  update <- merge(index_last_week,
    wow_with_keys,
    by = "key"
  )

  # for the stock of ads (ie the time series with "sum_clean" in their name) we just take the new value
  update[key %like% "sum_clean", index := value]

  #' for the new index value (ie the time series withOUT "sum_clean" in their name) ,
  #' we multiply the new growth factor to last week's index value to get today's index value
  update[!(key %like% "sum_clean"), index := value * index_wlag]

  index_new <- rbind(index_until_now[date <= newest_date - 7], update, fill = TRUE)


  # prnt(paste(
  #     "Manual changes to indices (removing values that don't have",
  #     "publishable quality)"))
  # srs_canton <- manual_changes_to_canton_indices(srs_canton)
  # TODO: We have to implement that when we compute the one-off index

  # Convert the index values to the format of the KOF time series DB
  # write the new index values to the DB
  prnt("Converting to ts...")
  index_new[, .(time = date, value = index, id = key)] |>
    tsbox::ts_xts() |>
    fill_missing_dates(newest_date) |>
    as.list()
}

fill_missing_dates <- function(series, current_date) {
  dates <- seq(start(series), end(series), by = 7)
  missing <- dates[!dates %in% time(series)]

  if (length(missing) == 0) {
    return(series)
  }

  rbind(
    series,
    xts::xts(
      matrix(
        rep_len(as.numeric(NA), length(missing) * ncol(series)),
        ncol = ncol(series)
      ),
      order.by = missing
    )
  )
}
