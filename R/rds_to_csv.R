rds_to_csv <- function(path_in, path_out) {
  ll <- list()
  fls <- list.files(path_in, pattern = ".rds")
  for(f in fls) {
    message(f)
    ll[[f]] <- readRDS(file.path(path_in, f))
  }

  write_x28_tables(combine_x28_data(ll), path_out)
}
