## Process:
## 1. Download and compile a list of all county names along with associated geographic codes from Census
## 2. Downlaod and compile a list of all New England municipality names along with
## associated _county_ geographic code where the municipality resides
## 3. Downlaod and compile a list of all place names published in the OpenElex election results file
## 4. Process OpenElex text, i.e., remove whitespace, non prinatable characters, "total", replace "St.", "&", "N.", etc.
## 5. Create unique key in the OpenElex data set that's a combination of the lowercased county name and lowercased state abbreviation
## 6. Create unique key in the Census data set that's a combination of the lowercased county name and lowercased state abbreviation
## 7. Merge OpenElex and Census data sets on the unique key
## 8. Filter combined data set on null values (counties found in one table but not in the other)
## 9. Run local alignment on remaining counties

library(Biostrings)
library(tidyr)
library(dplyr)
library(readr)
library(stringr)
library(stringdist)
library(RCurl)

CreateKey <- function(tbl, names = NULL)  {
  if (is.null(names)) {
    column_names <- tbl$name
  } else {
    column_names <- names
  }
  results <- tbl %>%
    mutate(key = paste(tolower(column_names),
                       tolower(state_abbr), sep = "")) %>%
    mutate(key = str_replace(key, "&", "and")) %>%
    mutate(key = str_replace(key, "ñ", "n")) %>%
    mutate(key = str_replace(key, "n\\. ", "north")) %>%
    mutate(key = str_replace(key, "s\\. ", "south")) %>%
    mutate(key = str_replace(key, "e\\. ", "east")) %>%
    mutate(key = str_replace(key, "w\\. ", "west")) %>%
    mutate(key = str_replace(key, " twp", "township")) %>%
    mutate(key = str_replace(key, " county", "")) %>%
    mutate(key = str_replace(key, " parish", "")) %>%
    mutate(key = str_replace(key, " place", "")) %>%
    mutate(key = str_replace(key, " ccd", "")) %>%
    mutate(key = str_replace(key, " cdp", "")) %>%
    mutate(key = str_replace(key, "\\.", "")) %>%
    mutate(key = str_replace(key, "\\'", "")) %>%
    mutate(key = str_replace(key, "\\-", "")) %>%
    mutate(key = str_replace_all(key, "\\ ", ""))
  
  return(results)
}

FilterNESubdisivions <- function(tbl) {
  results <- tbl %>%
    filter(
      !grepl("VT", state_abbr) &
        !grepl("ME", state_abbr) &
        !grepl("NH", state_abbr) &
        !grepl("MA", state_abbr) &
        !grepl("RI", state_abbr) &
        !grepl("CT", state_abbr)
    )
  return(results)
}

RemoveUndefinedSubdivisions <- function(tbl) {
  results <- tbl %>%
    filter(!grepl("00000", subdivision_fips))
  
  return(results)
}

SplitGEOID <- function(tbl) {
  results <- tbl %>%
    mutate(state_fips = str_sub(geoid, 1, 2)) %>%
    mutate(county_fips = str_sub(geoid, 3, 5)) %>%
    mutate(subdivision_fips = na_if(str_sub(geoid, 6, 10), "")) %>%
    mutate(geography_type = ifelse(is.na(subdivision_fips), "county", "subdivision"))
  
  return(results)
}

UpdateFIPS <- function(tbl) {
  results <- tbl %>%
    mutate(county_fips = replace(
      county_fips,
      which(state_fips == '46' & county_fips == '113'),
      '102'
    )) %>%
    mutate(county_fips = replace(
      county_fips,
      which(state_fips == '02' & county_fips == '270'),
      '158'
    ))
  
  return(results)
}

UpdateCountyNames <- function(tbl) {
  results <- tbl %>%
    mutate(name = replace(
      name,
      which(name == 'Wade Hampton Census Area'),
      'Kusilvak Census Area'
    )) %>%
    mutate(name = replace(name, which(name == 'La Salle Parish'), 'LaSalle Parish')) %>%
    mutate(name = replace(
      name,
      which(name == 'Shannon County' &
              state_abbr == 'SD'),
      'Oglala Lakota County'
    ))
  
  return(results)
}

## Process Census data
## Process Census Gazetteer counties & county subdivisions

gazetteer_temp <- tempfile()
download.file(
  "http://www2.census.gov/geo/docs/maps-data/data/gazetteer/2017_Gazetteer/2017_Gaz_counties_national.zip",
  gazetteer_temp
)
gazetteer_zip <- unzip(gazetteer_temp)

counties_raw <- read_tsv(
  gazetteer_zip,
  skip = 1,
  locale = locale(encoding = "latin1"),
  col_names = c(
    "state_abbr",
    "geoid",
    "ansi_code",
    "name",
    "land_area_meters",
    "water_area_meters",
    "land_area_miles",
    "water_area_miles",
    "latitude",
    "longitude"
  ),
  col_types = cols(
    state_abbr = col_factor(levels = NULL),
    geoid = col_character(),
    ansi_code = col_character(),
    name = col_character(),
    land_area_meters = col_double(),
    water_area_meters = col_double(),
    land_area_miles = col_double(),
    water_area_miles = col_double(),
    latitude = col_character(),
    longitude = col_character()
  )
)

rm(list = c("gazetteer_zip", "gazetteer_temp"))

gazetteer_temp <- tempfile()
download.file(
  "http://www2.census.gov/geo/docs/maps-data/data/gazetteer/2017_Gazetteer/2017_Gaz_cousubs_national.zip",
  gazetteer_temp
)
gazetteer_zip <- unzip(gazetteer_temp)

subdivisions_raw <- read_tsv(
  gazetteer_zip,
  skip = 1,
  locale = locale(encoding = "latin1"),
  col_names = c(
    "state_abbr",
    "geoid",
    "ansi_code",
    "name",
    "functional_status",
    "land_area_meters",
    "water_area_meters",
    "land_area_miles",
    "water_area_miles",
    "latitude",
    "longitude"
  ),
  col_types = cols(
    state_abbr = col_factor(levels = NULL),
    geoid = col_character(),
    ansi_code = col_character(),
    name = col_character(),
    functional_status = col_character(),
    land_area_meters = col_double(),
    water_area_meters = col_double(),
    land_area_miles = col_double(),
    water_area_miles = col_double(),
    latitude = col_character(),
    longitude = col_character()
  )
)

rm(list = c("gazetteer_zip", "gazetteer_temp"))

counties_clean <- counties_raw %>%
  select(c("geoid", "state_abbr", "name")) %>%
  UpdateCountyNames() %>%
  SplitGEOID() %>%
  UpdateFIPS() %>%
  CreateKey()

subdivisions_clean <- subdivisions_raw %>%
  select(c("geoid", "state_abbr", "name")) %>%
  SplitGEOID() %>%
  RemoveUndefinedSubdivisions() %>%
  UpdateFIPS() %>%
  CreateKey()

counties_merge <- counties_clean %>%
  select(c("geoid", "state_abbr", "name", "key")) %>%
  distinct()

subdivisions_merge <- subdivisions_clean %>%
  select(c("geoid", "state_abbr", "name", "key")) %>%
  distinct()

## Create a union of county and county subdivisions
places_merge = dplyr::union(x = counties_merge,
                            y = subdivisions_merge)

## Process OpenElex data
openelex_raw <-
  read_csv(
    getURL(
      "https://raw.githubusercontent.com/openelections/openelections-data-us/master/2016/20161108__us__general__president__county.csv"
    ),
    skip = 1,
    locale = locale(encoding = "latin1"),
    col_names = c(
      "state_abbr",
      "name",
      "office",
      "district",
      "party",
      "candidate",
      "votes"
    ),
    col_types = cols(
      state_abbr = col_factor(levels = NULL),
      name = col_character(),
      office = col_character(),
      district = col_character(),
      party = col_character(),
      candidate = col_character(),
      votes = col_double()
    )
  )

openelex_clean <- openelex_raw %>%
  mutate(name = str_trim(name)) %>%
  mutate(name = replace(name, which(is.na(name) &
                                      state_abbr == "DC"), "District of Columbia")) %>%
  mutate(name = replace(name, which(is.na(name)), "total")) %>%
  filter(!grepl("total", tolower(name))) %>%
  CreateKey()

openelex_merge <- openelex_clean %>%
  select(c("state_abbr", "name", "key")) %>%
  distinct()

rm(list = ls()[!ls() %in% c("openelex_merge",
                            "counties_merge",
                            "subdivisions_merge",
                            "places_merge",
                            "CreateKey")])


##################################################

ne_states <- c("RI", "VT", "NH", "MA", "ME", "CT")

## Join Census and OpenElex counties on unique key
openelex_counties_merge <- full_join(
  x = counties_merge,
  y = openelex_merge,
  by = "key",
  suffix = c(".census", ".openelex")
)

openelex_counties_matched <- openelex_counties_merge %>%
  filter(grepl("", name.census) & grepl("", name.openelex))

openelex_counties_unmatched <- openelex_counties_merge %>%
  filter(!grepl("", name.census) | !grepl("", name.openelex))

openelex_unmatched <- openelex_counties_unmatched %>%
  filter(!grepl("", name.census)) %>%
  select(c("state_abbr.openelex", "name.openelex")) %>%
  rename("state_abbr" = state_abbr.openelex,
         "name" = name.openelex)

counties_unmatched <- openelex_counties_unmatched %>%
  filter(!grepl("", name.openelex)) %>%
  select(c("state_abbr.census", "name.census")) %>%
  rename("state_abbr" = state_abbr.census,
         "name" = name.census)

unmatched_states_all <- unique(unlist(openelex_unmatched %>%
                                        .$state_abbr))


unmatched_states_all_ne <- unique(unlist(
  openelex_unmatched %>%
    filter(state_abbr %in% ne_states) %>%
    .$state_abbr
))

unmatched_states_not_ne <- unique(unlist(
  openelex_unmatched %>%
    filter(!state_abbr %in% ne_states) %>%
    .$state_abbr
))


alignment_matches <- data.frame(
  missing_name = character(),
  state_abbr = character(),
  matched_name = character(),
  stringsAsFactors = FALSE
)

for (state in unmatched_states_not_ne) {
  print(state)
  
  ## Find closest matches of missing counties using local alignment
  alignment_unmatched <- openelex_unmatched %>%
    filter(grepl(state, state_abbr)) %>%
    rename("missing_name" = name)
  
  unmatched_names <- counties_unmatched %>%
    filter(grepl(state, state_abbr)) %>%
    .$name
  
  if (nrow(counties_unmatched %>%
           filter(grepl(state, state_abbr))) == 0) {
    next
  } else {
    alignment_matched <- alignment_unmatched %>%
      mutate(
        matched_name = missing_name %>%
          sapply(
            function(n)
              pairwiseAlignment(
                tolower(unmatched_names),
                tolower(n),
                type = "local",
                gapOpening = 0,
                gapExtension = 1,
                scoreOnly = TRUE
              ) %>%
              which.max
          ) %>%
          unmatched_names[.]
      )
    alignment_matches <- bind_rows(alignment_matches,
                                   alignment_matched)
    
  }
  
  rm(list = c(
    "alignment_unmatched",
    "unmatched_names",
    "alignment_matched"
  ))
  
}

alignment_results <- alignment_matches %>%
  rename(name = "matched_name") %>%
  CreateKey() %>%
  select(c("state_abbr","missing_name","key")) %>%
  rename(name = "missing_name")

alignment_results_merge <- left_join(x = openelex_counties_unmatched,
                             y = alignment_results,
                             by = "key",
                             suffix = c("", ".results")
                             )
                             
