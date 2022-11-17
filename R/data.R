#' Mapping from x28 job ids to SBN
#'
#' @format A 2413x4 data.frame:
#' \describe{
#'   \item{idx28}{numeric job id used by x28}
#'   \item{namex28}{Job label used by x28}
#'   \item{codesbn}{SBN code}
#'   \item{namesbn}{SBN Label}
#' }
"x28_sbn"

#' Mapping from x28 industry ids to NOGA
#'
#' @format A 1811x5 data.frame:
#' \describe{
#'   \item{idx28}{numeric industry id used by x28}
#'   \item{namex28}{Industry label used by x28}
#'   \item{noga_letter}{1 digit NOGA letter code}
#'   \item{noga_full}{full NOGA code}
#'   \item{namenoga}{NOGA label}
#' }
"x28_noga"

#' Mapping from x28 job ids to ISCO
#'
#' @format A 2544x5 data.frame:
#' \describe{
#'   \item{idx28}{numeric job id used by x28}
#'   \item{namex28}{Job label used by x28}
#'   \item{isco_1d}{1 digit ISCO code}
#'   \item{isco_2d}{2 digit ISCO code}
#'   \item{isco_5d}{full ISCO code}
#'   \item{nameisco}{ISCO label}
#' }
"x28_isco"
