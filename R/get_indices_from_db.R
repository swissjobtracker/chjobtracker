
#' Get the indices up until now from the db
#'
#' @param ads_data
#' @param by.cols
#' @param aggregate
#' @param start_date
#'
#' @return
#' @import data.table
#' @export
#'
#' @examples
#'

get_indices_from_db<-function(con_main){

  key<-db_dataset_get_keys(con_main, "ch.kof.jobtracker_all")

  series<-lapply(key, function(x){
    a<-as.data.table(db_ts_read(con_main, x, valid_on = Sys.Date()), keep.rownames = "date")
    if(nrow(a)==0){
      a<-as.data.table(list(date=as.Date(NA), index=NA))
    }else {
        names(a)<-c("date", "index")
      }
    a[, key:=..x]
    a
  })
  rbindlist(series)
}

