#' Flag (date, source) pairs to not be included
#'
#' @param ads
#' @param by.cols
#' @param start_date
#'
#' @return
#' @export
#'
#' @examples
portal_filter_within_portal_hampler <- function(ads,
                                           weeks_not_flagged=5, mad_factor=4,
                                           start_date="2018-01-01") {
  # get daily number of created and deleted ads
  vacancies <- vacancies_by_day(ads, by.cols = "source")
  # only keep the created and deleted ads up to a year ago
  vacancies<-vacancies[date>=as.Date(start_date)-365-7*(weeks_not_flagged+1)]

  setorderv(vacancies, c("source", "date"))

# gives us the upper bound threshold based on the vector of values in x
# threshold = whether a value is bigger than the median plus a multiple of the median absolute deviation
# Note that we don't care about the lower bound since 0 is fine and we are only interested in spikes in created or deleted ads

# we exclude days with a stock of zero from the calibration of the filter
# Intuitevely, we want to exclude days with an excessive amount of creation or deletion of ads conditional on any activity on the portal
  ub_hampler<-function(x){
    xx<-x[x>0] # we exclude days with a stock of zero from the calibration of the filter
    median(xx)+mad_factor*mad(xx)
  }

#' for each source and day, compute the upper bound threshold for the number of created and deleted ads 
#' based on all the 365 past days
  vacancies[, ub_created:=frollapply(N_created, 365, ub_hampler), by=source]
  vacancies[, ub_deleted:=frollapply(N_deleted, 365, ub_hampler), by=source]

#' for each source and week, compute the maximum number of daily created and deleted ads, 
#' as this will be the number we compare to the upper bound
#' Ie if there was one day with an annormal amount of created or deleted ads, we will flag the whole week
  vacancies[, week_max_created:=frollapply(N_created, 7, max), by=source]
  vacancies[, week_max_deleted:=frollapply(N_deleted, 7, max), by=source]

  # Get lag of stock from previous week
  vacancies[, wlag_N := shift(job_postings, 7), by = "source"]

  # Only keep Fridays in data. Note that Fridays that fall on Feb 29 are missing, a minor problem which we ignore for now
  weeks <- vacancies[!is.na(wlag_N) &
                       # wday == 6 stands for friday
                       wday(date) == 6]

  # week-on-week growth rate of the stock by source
  weeks[, wow := (job_postings - wlag_N) / wlag_N]
  weeks[wlag_N==0, wow:=NA] # if the stock in the past week was zero, we can't compute the growth rate

  # just look at the portals, since the source "company" refers to the aggregate of all company domains which we take as stable
  portals<-weeks[source!="company"]
  portals<-portals[!is.na(wow)]
  setorder(portals, date, wow)

# Apply the hampel filter
# include or not? if the number of created or deleted ads is above the upper bound threshold, we flag the whole week
  portals[, filter_created:=week_max_created>ub_created ]
  portals[, filter_deleted:=week_max_deleted>ub_deleted ]


  #' Second exclusion criteria: If a portal is inactive for too long 
  #' (more than two weeks in a row with no created or deleted ads) 
  #' we flag it as well
  portals[, two_weeks_max_activity:=
            frollapply(week_max_created,2,max) + frollapply(week_max_deleted,2,max)] # get the maximum number of created or deleted ads in the past two weeks


  # apply all the three filters together (hampel on created, hampel on deleted, and two weeks of inactivity)
  portals[, extreme:=filter_deleted | filter_created | two_weeks_max_activity==0]
  portals[is.na(extreme), extreme:=T]


  # Some stats, not relevant for comupation
  # just to see what happens
  # ggplot(portals[wow %between% c(-.66, 3)], aes(x=1+wow, fill=extreme))+
  #   geom_histogram(bins=100, position="identity", alpha=0.5)+
  #   scale_x_continuous(trans="log")
  # lm(log(1+wow)~extreme, data=portals[!is.na(wow) & is.finite(log(1+wow))]) %>% summary()

  setorderv(portals, c("source", "date"))

  # for each source and week, check if any of the past 5 weeks (or whatever weeks_not_flagged is) was flagged
  portals[, include:=frollapply(extreme,weeks_not_flagged, max )==0, by=source]

  # return a dataset with all the week-portal combinations and a flag whether to include or not
  return(portals[, c("date", "source", "include", "filter_deleted", "filter_created")])
}
