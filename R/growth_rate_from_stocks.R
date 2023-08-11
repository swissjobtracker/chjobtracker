#' Compute the aggregated growth rate from dissgregated stocks of ads by source
#' We do so using a weighted growth rate, where the weights are the number of ads in the vintage stock by source
#' If a portal is not included (cause it was filtered out) we set the weight to 0

growth_rate_from_stocks <- function(stocks,
                            vintage_stocks,
                            week_inclusion,
                            by.cols = c()){
  # define a vector of the columns to group by and the source
  by.cols.source <- union(by.cols, "source")   # e.g by.cols.source <- c("canton", "source")

  # merge stocks form this week to the  vintage stocks from last week
  weeks<-merge(stocks, vintage_stocks, by=by.cols.source, all.x=T)


  # wow growth rate of stocks by source (and whatever else we group by, such as canton)
  weeks[, wow := (N - N_vintage) / N_vintage]
  weeks[wlag_N==0, wow:=NA] # if there is no vintage stock, we cannot compute the growth rate
  weeks<-weeks[!is.na(wow) & is.finite(wow)] # remove NAs and infinities


  # merge in the info whether to include a source or not for a given week
  weeks<-merge(weeks, week_inclusion, all.x=T, by=c("date", "source"))
  weeks[source=="company", include:=1 ] # always include ads that come directly from the company website
  weeks[is.na(include), include:=0] # if there is no info on whether to include a source, we don't include it
  weeks[, include:=as.numeric(include)] # 0 = no, 1 = yes


  # compute weights for index calculation
  #' do different versions, the one shown on the tracker is 'clean'
  

  #' raw: no filter applied, 
  #'      weights are the share of ads in the vintage stock by source
  weeks[, weights_raw:=N_vintage/sum(N_vintage), by=c("date",by.cols)]

  #' company: only count ads the come directly from the company website, 
  #'          we don't need to weight different sources here, since it is just one source: the aggreate of all company websites
  weeks[, weights_company:=as.numeric(source=="company")]

  #' portal: only count ads that come from a portal, 
  #'         weights are the share of ads in the vintage stock by source times a dummy that is 1 if the source is a portal
  weeks[, weights_portal:=N_vintage*(source!="company")/sum(N_vintage*(source!="company")),  by=c("date",by.cols) ]

  #' THIS IS THE VERSION WE USE ON THE TRACKER
  #' clean: only count ads that are not filtered out, 
  #'        weights are the share of ads in the vintage stock by source times a dummy that is 1 if the source is not filtered out
  weeks[, weights_clean:=(N_vintage*include)/sum(N_vintage*(include)),
        by=c("date",by.cols)]

  #' clean, no company domains: only count ads from portals that are not filtered out       
  weeks[, weights_clean_portals:=N_vintage*include*(source!="company")/
          sum(N_vintage*include*(source!="company")),
        by=c("date",by.cols)]

  #' compute aggregate the growth factor (1+growth rate) between this week and last week (for each group defined by by.cols, if any)
  #' Aggregate growth rate: For every source we multiply the growth rate with the weight and sum over all sources
  index<-weeks[, .(raw=sum(weights_raw*(1+wow)),
                   companies=sum(weights_company*(1+wow)),
                   portals_raw=sum(weights_portal*(1+wow)),
                   clean=sum(weights_clean*(1+wow)),
                   clean_portals=sum(weights_clean_portals*(1+wow)),

                   # also save the stock of ads this week, in case we use it for diagnostics
                   sum_raw=sum(N_vintage),
                   sum_company=sum(N_vintage*(source=="company")),
                   sum_portal=sum(N_vintage*(source!="company")),
                   sum_clean=sum(N_vintage*(include)),
                   sum_clean_portals= sum(N_vintage*include*(source!="company"))),
               by=c("date", by.cols)]
 # return the different versions of the aggregate growth rate between this week and last week
  return(index)
}




#'
#' For the one-off change to the new system (that we did in Summer 2023) we also needed to be able to
#' compute the growth rate if the inputs for both weeks are vintage series
#'
growth_rate_from_two_vintage_series <- function(vintage_stocks,
                                    vintage_stocks_wlag,
                                    week_inclusion,
                                    by.cols = c()){
  # Some calculations need to be done by by.cols plus source
  by.cols.source <- union(by.cols, "source")

  # make the two dataset have clear col names
  setnames(vintage_stocks, "N_vintage", "N")
  setnames(vintage_stocks, "vintage_date", "date")

  vintage_stocks[, date:=as.Date(unlist(date))]
  vintage_stocks_wlag[, vintage_date:=as.Date(unlist(vintage_date))]


  # merge stocks to vintage stocks
  weeks<-merge(vintage_stocks, vintage_stocks_wlag, by=by.cols.source, all.x=T)

  #weeks[, summary((wlag_N-N_vintage)/N_vintage)*100] # check difference to stock

  # wow growth rate
  weeks[, wow := (N - N_vintage) / N_vintage]
  weeks[N_vintage==0, wow:=NA]
  weeks<-weeks[!is.na(wow) & is.finite(wow)]


  # include portal?
  weeks<-merge(weeks, week_inclusion, all.x=T, by=c("date", "source"))
  weeks[source=="company", include:=1 ]
  weeks[is.na(include), include:=0]
  weeks[, include:=as.numeric(include)]


  # compute weights for index calculation
  weeks[, weights_raw:=N_vintage/sum(N_vintage), by=c("date",by.cols)]
  weeks[, weights_company:=as.numeric(source=="company")]
  weeks[, weights_portal:=N_vintage*(source!="company")/sum(N_vintage*(source!="company")),  by=c("date",by.cols) ]
  weeks[, weights_clean:=(N_vintage*include)/sum(N_vintage*(include)),
        by=c("date",by.cols)]
  weeks[, weights_clean_portals:=N_vintage*include*(source!="company")/
          sum(N_vintage*include*(source!="company")),
        by=c("date",by.cols)]

  # compute index and also the number of ads it is based on
  index<-weeks[, .(raw=sum(weights_raw*(1+wow)),
                   companies=sum(weights_company*(1+wow)),
                   portals_raw=sum(weights_portal*(1+wow)),
                   clean=sum(weights_clean*(1+wow)),
                   clean_portals=sum(weights_clean_portals*(1+wow)),
                   sum_raw=sum(N_vintage),
                   sum_company=sum(N_vintage*(source=="company")),
                   sum_portal=sum(N_vintage*(source!="company")),
                   sum_clean=sum(N_vintage*(include)),
                   sum_clean_portals= sum(N_vintage*include*(source!="company"))),
               by=c("date", by.cols)]
  index
}

