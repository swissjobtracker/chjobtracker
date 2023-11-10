#' Get the stocks from last week that was saved last week (so if it was updated in the meantime it will be the original version)
#' This function gets the stock as a json from the database and converts it back to a list of data.tables
#' @param con A database connection
#' @param date The date for which the vintage stocks should be retrieved
#' @return A list of data.tables with the vintage stocks
#' 
get_vintage_stocks <- function(con, date=Sys.Date()-7){

  db_result <- DBI::dbGetQuery(
    con, "SELECT values FROM x28.indicator_history WHERE date = $1",
    params = list(as.Date(date))
  )
  if(nrow(db_result)==0){
    stop("No vintage stocks for a week before the index computation date")
  }

 # format the jsons as a list of data.tables
 list<-jsonlite::fromJSON(db_result[1,1])
 out<-list()
 out[["total"]]<-list$total %>% t() %>% as.data.table(keep.rownames = "source") %>% cbind(list(vintage_date=date))
 names(out[["total"]])[2]<-"N_vintage"
 names(out[["total"]])[3]<-"vintage_date"
 out[["total"]][, N_vintage:=as.numeric(N_vintage)]

 out[["canton"]]<-list$canton %>% lapply(function(x){
   rbindlist(lapply(x, function(N_vintage) cbind(as.data.table(N_vintage), as.data.table(list("vintage_date"=date)))), idcol = "source")
 }) %>% rbindlist(idcol = "canton")
 out[["canton"]][, N_vintage:=as.numeric(N_vintage)]

 out[["noga"]]<-list$noga %>% lapply(function(x){
   rbindlist(lapply(x, function(N_vintage) cbind(as.data.table(N_vintage), as.data.table(list("vintage_date"=date)))), idcol = "source")
 }) %>% rbindlist(idcol = "noga_letter")
 out[["noga"]][, N_vintage:=as.numeric(N_vintage)]


 out[["isco"]]<-list$isco %>% lapply(function(x){
   rbindlist(lapply(x, function(N_vintage) cbind(as.data.table(N_vintage), as.data.table(list("vintage_date"=date)))), idcol = "source")
 }) %>% rbindlist(idcol = "isco_1d")
 out[["noga"]][, N_vintage:=as.numeric(N_vintage)]


 return(out)
}
