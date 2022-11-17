#' Convert json dumps into rds with relational tables
#'
#' This is mainly for development use. For production use dumps_to_db instead.
#'
#' @param path_in path where the dumps lie
#' @param path_out path to store the resulting rds files
#'
#' @export
dumps_to_rds <- function(path_in, path_out) {
  fls <- list.files(path_in, pattern = "json.gz")

  for(f in fls) {
    message(f)
    ll <- read_x28_dump(file.path(path_in, f))
    saveRDS(ll, file.path(path_out, paste0(f, ".rds")))
  }
}
