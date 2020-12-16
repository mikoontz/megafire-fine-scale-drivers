
#### Dependencies ####
dependencies <- c("data.table",
                  "dplyr", 
                  "furrr", "future",
                  "glue", 
                  "here",
                  "lubridate",
                  "ncdf4",
                  "pbapply",
                  "purrr",
                  "readr", 
                  "sf", 
                  "stringr", 
                  "terra", 
                  "USAboundaries")

needs_install <- !sapply(dependencies, FUN = require, character.only = TRUE)

install.packages(dependencies[needs_install])

sapply(dependencies[needs_install], FUN = require, character.only = TRUE)

if(!require(USAboundariesData)) {
  install.packages("USAboundariesData", repos = "http://packages.ropensci.org", type = "source")
}

require(USAboundariesData)
#### end dependencies ####

#### Create directories
dir.create(here::here("data/out/"), recursive = TRUE, showWarnings = FALSE)
dir.create(here::here("data/raw/"), recursive = TRUE, showWarnings = FALSE)

#### Functions ####

#### Sync goes detections from AWS

sync_goes <- function(target_goes, year) {
  (start <- Sys.time())
  system2(command = "aws", args = glue::glue("s3 sync s3://noaa-{target_goes}/ABI-L2-FDCC/{year} data/raw/{target_goes}/ABI-L2-FDCC/{year} --no-sign-request"), stdout = FALSE)
  
  return(NULL)
}

#### End sync function

goes_year_buckets <-
  lapply(c("goes16", "goes17"), FUN = function(target_goes) {
    years <- 
      system2(command = "aws", args = glue::glue("s3 ls s3://noaa-{target_goes}/ABI-L2-FDCC/  --no-sign-request"), stdout = TRUE) %>% 
      stringr::str_extract_all(pattern = "[0-9]+") %>% 
      unlist()
    
    return(dplyr::tibble(target_goes = target_goes, year = years))
  }) %>% 
  dplyr::bind_rows() %>% 
  dplyr::filter(year == "2020")

for (i in 1:nrow(goes_year_buckets)) {
  target_goes <- goes_year_buckets$target_goes[i]
  year <- goes_year_buckets$year[i]
  
  dir.create(glue::glue("{here::here()}/data/out/california_goes/{target_goes}_{year}/"), recursive = TRUE, showWarnings = FALSE)
  
  sync_goes(target_goes, year)
  
}