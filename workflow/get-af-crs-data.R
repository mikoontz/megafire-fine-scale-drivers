# retrieve just the CRS data from each GOES detection

library(tidyverse)
library(glue)
library(terra)
library(sf)
library(slider)
library(purrr)
library(furrr)
library(future)

target_goes <- "goes16"
get_latest_goes <- TRUE

if(get_latest_goes | !file.exists(glue::glue("data/data_output/{target_goes}-filenames.csv"))) {
  source("R/get-af-metadata.R")
}  

# Read in the GOES metadata acquired from Amazon Earth using get-af-metadata.R script
goes_af <- readr::read_csv(file = glue::glue("data/data_output/{target_goes}-filenames.csv"), col_types = "ciiiiinicTTTccccc")

get_crs_info <- function(aws_path, filename, scan_center, local_path) {
  
  # Round the image datetime to the nearest hour
  rounded_datetime <- 
    scan_center %>% 
    lubridate::parse_date_time2(orders = "%Y%m%d%H%M%S") %>% # lubridate::ymd_hms() is failing me here for e.g., "2020052200050.9"
    lubridate::round_date(scan_center, unit = "hour")
  
  rounded_datetime_txt <- 
    paste0(lubridate::year(rounded_datetime),
           stringr::str_pad(lubridate::month(rounded_datetime), width = 2, side = "left", pad = "0"),
           stringr::str_pad(lubridate::day(rounded_datetime), width = 2, side = "left", pad = "0"),
           stringr::str_pad(lubridate::hour(rounded_datetime), width = 2, side = "left", pad = "0"),
           "00")
  
  # Read in the .nc file using the {terra} package in order to preserve CRS data and values properly (and its fast!)
  goes <- terra::rast(local_path)
  
  # Get the crs of the .nc file; This will be important later because the satellite moved in 2017
  # from its initial testing position to its operational position, and so the raster cells
  # are representing different areas on the Earth when that happened (encoded in the CRS though)
  goes_crs <- terra::crs(goes)
  
  # Record CRS data for this particular goes image
  out <-
    tibble::tibble(scan_center = scan_center,
                   filename = glue::glue("{rounded_datetime_txt}_{scan_center}_{filename}.csv"),
                   local_path = local_path,
                   aws_path = aws_path,
                   goes_crs = goes_crs)
  return(crs_table)
  
}

# Get the file names of the data that have already been processed
processed_goes <- 
  tibble::tibble(aws_files_raw = system2(command = "aws", args = glue::glue("s3 ls s3://earthlab-mkoontz/{target_goes}/ --recursive"), stdout = TRUE)) %>% 
  dplyr::filter(nchar(aws_files_raw) == 139) %>% 
  dplyr::mutate(filename_full = stringr::str_sub(string = aws_files_raw, start = 39),
                filename = stringr::str_sub(string = filename_full, start = 29, end = -5))


# divide the goes_af into batches
n_batches <- 1
n_subbatches <- 20 # number of cores

base::set.seed(1959)
# Only need to process the GOES file if processed data don't yet exist
batches <- 
  goes_af %>% 
  dplyr::mutate(filebase = stringr::str_sub(string = filename, start = 1, end = -4)) %>% 
  dplyr::filter(!(filebase %in% processed_goes$filename)) %>% 
  dplyr::filter(!(filebase %in% c("OR_ABI-L2-FDCF-M3_G16_s20172632115407_e20172632126173_c20172632126283",
                                  "OR_ABI-L2-FDCF-M3_G16_s20181231215382_e20181231226149_c20181231226258",
                                  "OR_ABI-L2-FDCF-M3_G16_s20183241845341_e20183241856108_c20183241856213",
                                  "OR_ABI-L2-FDCF-M6_G16_s20202471650186_e20202471659494_c20202471700318"))) %>% 
  base::split(f = sample(1:n_batches, size = nrow(.), replace = TRUE))

# multicore processing for batch j
j <- 1

subbatches <- 
  batches[[j]] %>% 
  base::split(f = sample(1:n_subbatches, size = nrow(.), replace = TRUE))

(start <- Sys.time())

future::plan(strategy = "multiprocess", workers = n_subbatches)

furrr::future_map(.x = subbatches, .f = function(this_batch) {
  
  out <- 
    this_batch %>% 
    slider::slide(.f = ~ .) %>%  # Using slider::slide() as a rowwise iterator
    purrr::map(.f = function(this_goes) {
      
      aws_path <- this_goes$aws_path
      filename <- stringr::str_sub(this_goes$filename, start = 1, end = -4)
      scan_center <- this_goes$scan_center
      local_path <- glue::glue("data/data_raw/{target_goes}/{scan_center}_{filename}.nc")
      
      get_goes_points(aws_path, filename, scan_center, local_path)
      
    })
  
})

(difftime(Sys.time(), start))
