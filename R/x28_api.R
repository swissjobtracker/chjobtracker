#' Get job ad data from x28 API
#'
#' @param updates boolean indicating whether to get all jobs currently listed or just ones modified since modified_since
#' @param modified_since POSIXct timestamp indicating from which point in time we deliver the modified jobs
#' @param user character username
#' @param password character password
#'
#' @return
#' @export
#' @importFrom httr RETRY authenticate content stop_for_status
get_x28_api_data <- function(user,
                             password,
                             updates = TRUE,
                             modified_since = Sys.time() - 86400) {
  route <- sprintf("https://api.x28.ch/export/api/jobs/%sjson",
                   ifelse(updates, "updates/", ""))

  # Get initial request
  # all followind requests are the same for updates and not, only route and
  # prev_response$token are needed

  message("Initial request...")
  if(updates) {
    res <- RETRY("GET",
                 route,
                 query = list(
                   date = format(modified_since, format = "%Y-%m-%dT%H:%M:%S%z")
                 ),
                 authenticate(user, password))
  } else {
    res <- RETRY("GET",
                 route,
                 authenticate(user, password))
  }

  stop_for_status(res)

  cont <- get_api_response_content(res)
  token <- cont$token
  n_pages <- cont$pages
  page0 <- parse_x28_chunk(cont$jobs)

  pages <- list(
    page0
  )

  if(n_pages > 1) {
    pages <- c(
      pages,
      # paralellize here ;)
      # not quite sure if this could lead to racing conditions on the api tho
      # since all requests use the same token
      lapply(2:n_pages, function(pagenr) {
        message(sprintf("Getting page %d/%d", pagenr, n_pages))

        res <- RETRY("GET",
                     route,
                     query = list(token = token),
                     authenticate(user, password))

        stop_for_status(res)

        parse_x28_chunk(get_api_response_content(res)$jobs)
      })
    )
  }

  combine_x28_data(pages)
}


#' Get updates to job ads into the DB
#'
#' @param con RPostgres connection
#' @param api_user x28 API username
#' @param api_pw x28 API password
#'
#' @export
#' @import data.table
x28_update_db <- function(con,
                          api_user,
                          api_pw,
                          modified_since,
                          reason = "") {
  up <- get_x28_api_data(api_user, api_pw, modified_since = modified_since)

  x28_to_db(con, up)
  x28_db_log_event(con,
                   t = get_x28_latest_timestamp(up),
                   event = "x28_update_db",
                   details = reason)
}


#' Get all currently listed jobs and put them in the DB
#'
#' In detail, this calls export/api/jobs which lists all currently
#' active ads. n.b. this does not include any ads marked as deleted and is therefore
#' not complete.
#'
#' @param con RPostgres connection
#' @param api_user x28 API username
#' @param api_pw x28 API password
#' @param reason comment on why this was (manually) run
#'
#' @export
x28_backlog_db <- function(con,
                           api_user,
                           api_pw,
                           reason = "") {
  bl <- get_x28_api_data(api_user, api_pw, updates = FALSE)

  x28_to_db(con, bl)
  x28_db_log_event(con,
                   t = get_x28_latest_timestamp(bl),
                   event = "x28_backlog_db",
                   details = reason)
}
