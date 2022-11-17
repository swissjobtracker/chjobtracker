#' Flag (date, source) pairs to not be included
#'
#' @param ads
#' @param by.cols
#' @param exclusion_override a data.table(date, source) identifying records to always EXclude
#' @param start_date
#'
#' @return
#' @export
#'
#' @examples
portal_filter_within_portal_hampler <- function(ads,
                                           exclusion_override = NULL,
                                           weeks_not_flagged=5, mad_factor=4,
                                           start_date="2018-01-01") {
  # get daily stock
  vacancies <- vacancies_by_day(ads, by.cols = "source")
  vacancies<-vacancies[date>=as.Date(start_date)-365-7*(weeks_not_flagged+1)]

  setorderv(vacancies, c("source", "date"))

  ub_hampler<-function(x){
    xx<-x[x>0]
    median(xx)+mad_factor*mad(xx)
  }

  vacancies[, ub_created:=frollapply(N_created, 365, ub_hampler), by=source]
  vacancies[, ub_deleted:=frollapply(N_deleted, 365, ub_hampler), by=source]

  vacancies[, week_max_created:=frollapply(N_created, 7, max), by=source]
  vacancies[, week_max_deleted:=frollapply(N_deleted, 7, max), by=source]


  vacancies <- vacancies[, wlag_N := shift(job_postings, 7), by = "source"]

  # Only keep Fridays in data. Note that Fridays that fall on Feb 29 are missing for now
  weeks <- vacancies[!is.na(wlag_N) &
                       # strftime is locale dependent i.e. on a german system it would yield "Freitag"
                       # therefore wday is preferred
                       wday(date) == 6]

  # wow growth rate
  weeks[, wow := (job_postings - wlag_N) / wlag_N]
  weeks[wlag_N==0, wow:=NA]

  # just look at the portals
  portals<-weeks[source!="company"]
  portals<-portals[!is.na(wow)]
  setorder(portals, date, wow)

  # include or not?
  portals[, filter_created:=week_max_created>ub_created ]
  portals[, filter_deleted:=week_max_deleted>ub_deleted ]



  # Second exclusion criteria: If a portal is inactive for too long
  portals[, two_weeks_max_activity:=
            frollapply(week_max_created,2,max) + frollapply(week_max_deleted,2,max)]


  # both filters together
  portals[, extreme:=filter_deleted | filter_created | two_weeks_max_activity==0]
  portals[is.na(extreme), extreme:=T]


  # Some stats, not relevant for comupation
  # just to see what happens
  # ggplot(portals[wow %between% c(-.66, 3)], aes(x=1+wow, fill=extreme))+
  #   geom_histogram(bins=100, position="identity", alpha=0.5)+
  #   scale_x_continuous(trans="log")
  # lm(log(1+wow)~extreme, data=portals[!is.na(wow) & is.finite(log(1+wow))]) %>% summary()

  # return a dataset with all the week-portal combinations which will be inlcuded
  setorderv(portals, c("source", "date"))

  portals[, include:=frollapply(extreme,weeks_not_flagged, max )==0, by=source]

  return(portals[, c("date", "source", "include", "filter_deleted", "filter_created")])
}
