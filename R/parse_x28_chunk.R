#' Parse a chunk of x28 ndjson
#'
#' Parse a number of ndjson entries into relational tables
#' Can be applied to API return value or dumps. Contains code to adjust older
#' formats from dumps to the same form we use.
#'
#' @param chunk a list of job ad objects (e.g. content(GET("api.x28...")))
#' @param text alternative to chunk, a vector of unparsed JSON objects
#'
#' @import data.table
#' @importFrom urltools domain
#' @export
parse_x28_chunk <- function(chunk = data.frame(), text = NULL) {
  if (!is.null(text)) {
    # From dump
    chunk_parsed <- as.data.table(fromJSON(paste0("[", paste(text, collapse = ","), "]")))
  } else {
    # From api with as = "parsed", simplifyVector = TRUE
    chunk_parsed <- as.data.table(chunk)
  }

  # Over time the capitalization of some fields varies
  # best to just use all lower case
  names(chunk_parsed) <- tolower(names(chunk_parsed))

  # 2022-03-02: Typo in API responses
  names(chunk_parsed) <- gsub("minumum", "minimum", names(chunk_parsed))
  names(chunk_parsed) <- gsub("company.adresses", "company.addresses", names(chunk_parsed))
  # content is called rawcontent in api returns (23.03.2022)
  names(chunk_parsed) <- gsub("plaincontent", "content", names(chunk_parsed))

  chunk_names <- names(chunk_parsed)

  # introduced starting 2019
  if (!"origin" %in% chunk_names) {
    message("inserting origin column")
    chunk_parsed[, origin := NA]
  }

  # switches beginning 2019
  if ("quotamax" %in% chunk_names && "workquota.maximum" %in% chunk_names) {
    chunk_parsed[is.na(workquota.maximum), workquota.maximum := quotamax]
    chunk_parsed[is.na(workquota.minimum), workquota.minimum := quotamin]
  } else if ("quotamax" %in% chunk_names) {
    chunk_parsed[, workquota.maximum := quotamax]
    chunk_parsed[, workquota.minimum := quotamin]
  }

  # 2019
  if ("firstseen" %in% chunk_names && "created" %in% chunk_names) {
    chunk_parsed[is.na(created), created := firstseen]
    chunk_parsed[is.na(updated), updated := lastseen]
  } else if ("firstseen" %in% chunk_names) {
    chunk_parsed[, created := firstseen]
    chunk_parsed[, updated := lastseen]
  }

  if (!"company.uid" %in% chunk_names) {
    chunk_parsed[, company.uid := NA]
  }
  if (!"company.crn" %in% chunk_names) {
    chunk_parsed[, company.crn := NA]
  }

  # Either pre nested data (as above) or the occasional page from the api
  # lack company size information
  if (!any(grepl("company.size", chunk_names))) {
    chunk_parsed[, company.size.id := NA]
    chunk_parsed[, company.size.name := NA]
    chunk_parsed[, company.size.employees.minimum := NA]
    chunk_parsed[, company.size.employees.maximum := NA]
  }

  if (mode(chunk_parsed$created) == "character") {
    chunk_parsed[, created_parsed := parse_ISO8601(created)]
    chunk_parsed[, updated_parsed := parse_ISO8601(updated)]

    # on the API, deleted is a boolean flag indicating if an ad is marked
    # as deleted. in the dumps it is a timestamp
    if (mode(chunk_parsed$deleted) == "logical") {
      chunk_parsed[deleted == TRUE, deleted_parsed := updated_parsed[deleted]]
    } else {
      # this case should probably never occur (neither the is.logical one below)
      chunk_parsed[, deleted_parsed := parse_ISO8601(deleted)]
    }
  } else {
    chunk_parsed[, created_parsed := as.POSIXct(created / 1000, origin = "1970-01-01")]
    chunk_parsed[, updated_parsed := as.POSIXct(updated / 1000, origin = "1970-01-01")]

    if (mode(chunk_parsed$deleted) == "logical") {
      chunk_parsed[deleted == TRU, deleted_parsed := updated_parsed[deleted]]
    } else {
      chunk_parsed[, deleted_parsed := as.POSIXct(deleted / 1000, origin = "1970-01-01")]
    }
  }

  # Main advertisment data
  ads <- chunk_parsed[, .(
    id,
    # TODO: ensure all rows have a duplicategroup here?
    duplicategroup,
    company_id = company.id,
    origin,
    created = created_parsed,
    updated = updated_parsed,
    deleted = deleted_parsed,
    language,
    qualityscore = if ("qualityscore" %in% chunk_names) qualityscore else NA, # TODO: this is not in the dumps?
    workquota_minimum = workquota.minimum,
    workquota_maximum = workquota.maximum,
    temporary,
    homeoffice = if ("homeoffice" %in% chunk_names) homeoffice else NA,
    url,
    domain = get_root_domain(url),
    company_url = company.url,
    company_domain = get_root_domain(company.url),
    company_name = company.name,
    company_uid = company.uid,
    company_crn = company.crn,
    company_recruitment_agency = company.recruitmentagency,
    company_size_id = company.size.id,
    company_size_name = company.size.name,
    company_size_min = company.size.employees.minimum,
    company_size_max = company.size.employees.maximum
  )]


  # Additional Advertisment data (title, raw text)
  # TODO: raw text is not in all dumps. we may even have requested this
  ads_details <- chunk_parsed[, .(
    advertisement_id = id,
    title,
    raw_text = if ("content" %in% chunk_names) content else NA_character_
  )]

  # Positions to occupy
  ads_positions <- chunk_parsed[, .(
    position = if (is.null(ncol(positions[[1]]))) {
      unlist(positions)
    } else {
      as.character(positions[[1]]$value)
    }
  ), by = .(advertisement_id = id)]

  # Education being asked for
  ads_educations <- chunk_parsed[, .(
    education = if (is.null(ncol(educations[[1]]))) {
      if (length(educations[[1]]) > 0) {
        unlist(educations[[1]])
      } else {
        character()
      }
    } else {
      as.character(educations[[1]]$value)
    }
  ), by = .(advertisement_id = id)]

  # Bind all metadata together
  # Some entries appear to be duplicated (e.g. if skill "informatik" is detected
  # multiple times in content). We only need one of each.
  ads_metadata <- chunk_parsed[, rbindlist(metadata, idcol = TRUE, fill = TRUE, use.names = TRUE)]

  # Pre-2019 there was a separate id that was not the id of the datum which was stored in value
  # 2019 and onward the value is in id so we follow this format
  if ("value" %in% names(ads_metadata)) {
    ads_metadata[, id := value][, value := NULL]
  }

  # In the raw data column id is the meta data object id. we want it to be the ad id
  ads_metadata <- ads_metadata[, metadata_id := id][, advertisement_id := chunk_parsed[.id, id]][, .id := NULL][, id := NULL][
    , .(
      advertisement_id,
      metadata_id,
      type,
      source,
      name,
      level = ifelse("level" %in% names(ads_metadata), level, NA_character_),
      phrase = ifelse("phrase" %in% names(ads_metadata), phrase, NA_character_)
    )
  ]

  ads_metadata <- unique(ads_metadata)

  if (all(lapply(chunk_parsed$locations, is.data.frame) == FALSE)) {
    ads_country <- data.table(advertisement_id = c(), country = c())
    ads_canton <- data.table(advertisement_id = c(), canton = c())
    ads_postalcode <- data.table(advertisement_id = c(), postalcode = c())
  } else {
    ads_locations <- chunk_parsed[, rbindlist(
      lapply(locations, as.data.table),
      idcol = TRUE,
      use.names = TRUE,
      fill = TRUE
    )][, id := chunk_parsed[.id, id]][, .id := NULL]
    names(ads_locations) <- tolower(names(ads_locations))

    if (ncol(ads_locations) == 3) {
      # Old "type - value" pair type
      ads_country <- ads_locations[type == "COUNTRY", .(advertisement_id = id, country = value)]
      ads_canton <- ads_locations[type == "PROVINCE", .(advertisement_id = id, canton = value)]
      ads_postalcode <- ads_locations[type == "POSTALCODE", .(advertisement_id = id, postalcode = value)]
    } else {
      ads_country <- ads_locations[, .(country = unique(country)), by = .(advertisement_id = id)][!is.na(country)]
      ads_canton <- ads_locations[, .(canton = unique(province)), by = .(advertisement_id = id)][!is.na(canton)]

      # newer / API format bundles them
      if ("postalcodes" %in% names(ads_locations)) {
        # Ensure (at least the first) postalcodes is typed
        # otherwise data.table can't infer the type of the new column
        ads_locations[sapply(postalcodes, length) == 0, postalcodes := list(numeric())]
        ads_postalcode <- ads_locations[,
          .(postalcode = as.numeric(unique(unlist(postalcodes)))),
          by = .(advertisement_id = id)
        ][
          !is.na(postalcode)
        ]
      } else {
        ads_postalcode <- ads_locations[,
          .(postalcode = unique(postalcode)),
          by = .(advertisement_id = id)
        ][
          !is.na(postalcode)
        ]
      }
    }
    ads_postalcode <- ads_postalcode[postalcode != "null"]
  }

  comps_metadata <- rbindlist(chunk_parsed$company.metadata, idcol = "md_id", use.names = TRUE)

  # cf. ads_metadata a few lines up there ^^^
  if ("value" %in% names(comps_metadata)) {
    comps_metadata[, id := value][, value := NULL]
  }
  comps_metadata <- comps_metadata[, advertisement_id := chunk_parsed[md_id, id]][
    ,
    company_id := chunk_parsed[md_id, company.id]
  ][
    ,
    md_id := NULL
  ][, .(
    advertisement_id,
    company_id,
    metadata_id = as.numeric(id),
    type,
    name
  )]

  # the earlier dumps don't contain company addresses
  comps_addresses <- data.table()
  if ("company.addresses" %in% chunk_names) {
    # The newer dump (~ Jun 2021) has an additional street for (very few) company.addresses
    # get rid of that
    addrs <- lapply(chunk_parsed$company.addresses, function(x) {
      if (ncol(x) > 0) {
        x$street <- NULL
      }
      x
    })

    comps_addresses <- rbindlist(addrs, idcol = "adr_id", use.names = TRUE, fill = TRUE)
    names(comps_addresses) <- tolower(names(comps_addresses))
    comps_addresses[, company.id := chunk_parsed[adr_id, company.id]][, advertisement_id := chunk_parsed[adr_id, id]][, adr_id := NULL]
    comps_addresses <- comps_addresses[, .(
      advertisement_id,
      company_id = company.id,
      country,
      postalcode,
      city,
      primary
    )]

    # Some records have primary both TRUE and FALSE (multiple locations in same zipcode?)
    # Only keep the primary one (note FALSE < TRUE)
    setorder(comps_addresses, advertisement_id, company_id, primary)
    comps_addresses <- unique(comps_addresses, fromLast = TRUE, by = c("advertisement_id", "company_id", "postalcode"))
  }

  list(
    advertisements = ads,
    advertisement_details = ads_details,
    advertisement_metadata = ads_metadata,
    advertisement_positions = ads_positions,
    advertisement_education_levels = ads_educations,
    advertisement_country = ads_country,
    advertisement_canton = ads_canton,
    advertisement_postalcode = ads_postalcode,
    company_metadata = comps_metadata,
    company_addresses = comps_addresses
  )
}
