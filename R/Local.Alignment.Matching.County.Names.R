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

####################################################################################################

## Load data
## Load Census Gazetteer counties

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

## Load Census Gazetteer county subdivisions
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

## Load OpenElex data
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

## Process data

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

counties <- counties_clean %>%
  select(c("geoid", "state_abbr", "name", "key", "geography_type")) %>%
  distinct()

subdivisions <- subdivisions_clean %>%
  select(c("geoid", "state_abbr", "name", "key", "geography_type")) %>%
  distinct()

## Create a union of county and county subdivisions
places = dplyr::union(
  x = counties,
  y = subdivisions
  )

openelex_clean <- openelex_raw %>%
  mutate(name = str_trim(name)) %>%
  mutate(name = replace(name, which(is.na(name) &
                                      state_abbr == "DC"), "District of Columbia")) %>%
  mutate(name = replace(name, which(is.na(name)), "total")) %>%
  filter(!grepl("total", tolower(name))) %>%
  CreateKey()

openelex <- openelex_clean %>%
  select(c("state_abbr", "name", "key")) %>%
  distinct()

rm(list = ls()[!ls() %in% c("openelex","places","CreateKey","openelex_clean")])

####################################################################################################

ne_states <- c("RI", "VT", "NH", "MA", "ME", "CT")

## Merge openelex places with Census counties
## First filter places to show those place WE KNOW are county geographies types
## This means no New England states
places_counties <- places %>% 
  filter(geography_type == 'county') %>%
  filter(!state_abbr %in% ne_states)

openelex_counties <- openelex %>%
  filter(!state_abbr %in% ne_states)
  
## Full join Census and OpenElex counties on unique key
openelex_counties_join <- full_join(
  x = openelex_counties,
  y = places_counties,
  by = "key",
  suffix = c(".openelex", ".census")
)

openelex_counties_matched <- inner_join(
  x = openelex_counties,
  y = places_counties,
  by = "key",
  suffix = c(".openelex", ".census")
)

openelex_counties_unmatched <- anti_join(
  x = openelex_counties,
  y = places_counties,
  by = "key"
) 

places_counties_unmatched <- anti_join(
  x = places_counties,
  y = openelex_counties,
  by = "key"
) 

setdiff(openelex_counties_unmatched$key, openelex_counties_matched$key)

## Now do it again with county subdivisions
places_subdivisions <- places %>% 
  filter(geography_type == 'subdivision') %>%
  filter(state_abbr %in% ne_states)

openelex_subdivisions <- openelex %>%
  filter(state_abbr %in% ne_states)

## Full join Census and OpenElex county subdivisions on unique key
openelex_subdivisions_join <- full_join(
  x = openelex_subdivisions,
  y = places_subdivisions,
  by = "key",
  suffix = c(".openelex", ".census")
)

openelex_subdivisions_matched <- inner_join(
  x = openelex_subdivisions,
  y = places_subdivisions,
  by = "key",
  suffix = c(".openelex", ".census")
)

openelex_subdivisions_unmatched <- anti_join(
  x = openelex_subdivisions,
  y = places_subdivisions,
  by = "key"
) 

places_subdivisions_unmatched <- anti_join(
  x = places_subdivisions,
  y = openelex_subdivisions,
  by = "key"
) %>%
  filter(state_abbr %in% ne_states)

## Create a union of matched counties and county subdivisions
openelex_matched <- dplyr::union(x = openelex_counties_matched,
                                  y = openelex_subdivisions_matched) %>%
  select(c("key","geoid"))

## Create a union of unmatched counties and county subdivisions
openelex_unmatched <- dplyr::union(x = openelex_counties_unmatched,
                                 y = openelex_subdivisions_unmatched) %>%
  select(c("state_abbr","name")) # drop key since we were unable to match on this variable

## Create a union of unmatched counties and county subdivisions to match against
places_to_match <- dplyr::union(x = places_counties_unmatched,
                                   y = places_subdivisions_unmatched)

rm(list = ls()[!ls() %in% c("openelex","places","openelex_matched","openelex_unmatched","places_to_match","ne_states","CreateKey","openelex_clean")])

####################################################################################################
# Local Alignment process

unmatched_states <- unique(unlist(openelex_unmatched %>% .$state_abbr))

alignment_result <- data.frame(
  missing_name = character(),
  state_abbr = character(),
  matched_name = character(),
  stringsAsFactors = FALSE
)

for (state in unmatched_states) {
  print(state)
  
  ## Find closest matches of missing counties using local alignment
  alignment_unmatched <- openelex_unmatched %>%
    filter(state_abbr == state) %>%
    rename("missing_name" = name)
  
  alignment_to_match <- places_to_match %>% filter(state_abbr == state)
    
  if (nrow(alignment_to_match) == 0) {
    next
  } else {
    if (!state %in% ne_states) {
      alignment_to_match <- alignment_to_match %>% .$name
    } else {
      alignment_to_match <- alignment_to_match %>% filter(geography_type == 'subdivision') %>% .$name
    }
    
    print(alignment_to_match)
    
    alignment_matches <- alignment_unmatched %>%
      mutate(
        matched_name = missing_name %>%
          sapply(
            function(n)
              pairwiseAlignment(
                tolower(alignment_to_match),
                tolower(n),
                type = "local",
                gapOpening = 0,
                gapExtension = 1,
                scoreOnly = TRUE
              ) %>%
              which.max
          ) %>%
          alignment_to_match[.]
      )
    
    alignment_result <- dplyr::union(x = alignment_result,
                                     y = alignment_matches)

  }
  
  rm(list = c(
    "alignment_unmatched",
    "alignment_to_match",
    "alignment_matches"
  ))
  
}


rm(list = ls()[!ls() %in% c("alignment_result","places_to_match","openelex_matched","CreateKey","openelex_clean")])

####################################################################################################
## Final Results

alignment_matched <- alignment_result %>%
  select(c("matched_name","state_abbr","missing_name")) %>%
  rename(name = "matched_name") %>%
  CreateKey()

alignment_join <- inner_join(x = places_to_match,
                             y = alignment_matched,
                             by ="key",
                             suffix = c(".census",".alignment")) %>%
  select(c("geoid","missing_name","state_abbr.alignment")) %>%
  rename(name = "missing_name",
         state_abbr = "state_abbr.alignment") %>%
  CreateKey() %>%
  select("geoid","key")

matched_results <- dplyr::union(x = openelex_matched,
                                y = alignment_join
                                )
                             
## Merge results back to original OpenElex file
openelex_results <- full_join(x = openelex_clean,
                              y = matched_results,
                              by = "key"
                              ) %>%
  select(-c(key))

openelex_distinct <- openelex_clean %>% select("state_abbr", "name") %>% distinct()

write_csv(openelex_results, paste(getwd(),"data/results.csv",sep="/"))

