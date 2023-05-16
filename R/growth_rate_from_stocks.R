
growth_rate_from_stocks <- function(stocks,
                            vintage_stocks,
                            week_inclusion,
                            by.cols = c()){
  # Some calculations need to be done by by.cols plus source
  by.cols.source <- union(by.cols, "source")

  # merge stocks to vintage stocks
  weeks<-merge(stocks, vintage_stocks, by=by.cols.source, all.x=T)

  #weeks[, summary((wlag_N-N_vintage)/N_vintage)*100] # check difference to stock

  # wow growth rate
  weeks[, wow := (N - N_vintage) / N_vintage]
  weeks[wlag_N==0, wow:=NA]
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

