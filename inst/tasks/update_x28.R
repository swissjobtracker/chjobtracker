library(RPostgres)
library(nrp77)

con <- kofutils::kof_dbconnect(
  user_name = "kofdocker",
  db_name = "nrp77",
  env_pass_name = "PG_PASSWORD"
)

api_pw <- Sys.getenv("X28_API_PASSWORD")
api_user <- "kofethzch"

# We can at most ask for 2 days of updates
default_update <- Sys.time() - 2*24*60*60 + 120

# TODO: Something goes awry with time zones. Since Zurich is ahead of UTC nothing gets lost though.
last_update <- dbGetQuery(con, "select max(t) from x28.event_log where event = 'x28_update_db'")$max

if(is.na(last_update)) {
  message("This seems to be the first run, asking for 2 days of stuff (starting ", default_update, ").")
  last_update <- default_update
}

if(last_update < default_update) {
  warning("Last update (", last_update, ") is more than 2 days old (", default_update, ")!\nDoing what I can...")
  last_update <- default_update
}

message("Updating x28 ads starting ", last_update)

x28_update_db(con,
              api_user,
              api_pw,
              last_update,
              "automatic update")

dbDisconnect(con)
