library(dplyr)
library(readr)
library(glue)
library(lubridate)
library(purrr)

aws_ls_goes <- function(target_goes, get_latest_goes = FALSE) {
  if(get_latest_goes | !file.exists(glue::glue("data/out/{target_goes}_conus-filenames.csv"))) {
    # GOES-16 record begins on 2017-05-24
    # List all the GOES-16 files available on AWS
    # Takes 13 seconds for the 2017 data (May to December)
    # Takes 20 seconds for the 2018 data (full year)
    goes_aws_files <- 
      system2(command = "aws", args = glue::glue("s3 ls noaa-{target_goes}/ABI-L2-FDCC/ --recursive --no-sign-request"), stdout = TRUE)
    
    # bundle the list of filenames and extract some attributes from the
    # metadata embedded in those filenames
    goes_af <-
      tibble::tibble(target_goes = target_goes,
                     aws_path_raw = goes_aws_files,
                     aws_path = stringr::str_sub(string = aws_path_raw, start = 32, end = -1),
                     data_timestamp = stringr::str_sub(string = aws_path_raw, start = 1, end = 19)) %>% 
      tidyr::separate(col = aws_path, into = c("data_product", "year", "doy", "hour", "filename"), sep = "/", remove = FALSE) %>% 
      dplyr::mutate(doy = as.numeric(doy), year = as.numeric(year)) %>% 
      dplyr::mutate(tmp_date = as.Date(doy, origin = glue::glue("{year}-01-01")),
                    month = lubridate::month(tmp_date),
                    day = lubridate::day(tmp_date)) %>% 
      dplyr::mutate(scan_start = stringr::str_sub(filename, start = 24, end = 37),
                    scan_end = stringr::str_sub(filename, start = 40, end = 53),
                    scan_start_year = as.numeric(stringr::str_sub(scan_start, start = 1, end = 4)),
                    scan_end_year = as.numeric(stringr::str_sub(scan_end, start = 1, end = 4)),
                    scan_start_doy = as.numeric(stringr::str_sub(scan_start, start = 5, end = 7)),
                    scan_end_doy = as.numeric(stringr::str_sub(scan_end, start = 5, end = 7)),
                    scan_start_hour = as.numeric(stringr::str_sub(scan_start, start = 8, end = 9)),
                    scan_end_hour = as.numeric(stringr::str_sub(scan_end, start = 8, end = 9)),
                    scan_start_min = as.numeric(stringr::str_sub(scan_start, start = 10, end = 11)),
                    scan_end_min = as.numeric(stringr::str_sub(scan_end, start = 10, end = 11)),
                    scan_start_sec = as.numeric(stringr::str_sub(scan_start, start = 12, end = 14)) / 10,
                    scan_end_sec = as.numeric(stringr::str_sub(scan_end, start = 12, end = 14)) / 10,
                    scan_start_date = as.character(as.Date(scan_start_doy, origin = glue::glue("{scan_start_year}-01-01"))),
                    scan_end_date = as.character(as.Date(scan_end_doy, origin = glue::glue("{scan_end_year}-01-01"))),
                    scan_start_full = lubridate::ymd_hms(glue::glue("{scan_start_date} {scan_start_hour}:{scan_start_min}:{scan_start_sec}")),
                    scan_end_full = lubridate::ymd_hms(glue::glue("{scan_end_date} {scan_end_hour}:{scan_end_min}:{scan_end_sec}")),
                    scan_center_full = scan_start_full + difftime(scan_end_full, scan_start_full) / 2,
                    scan_center_full = round(scan_center_full, units = "secs")) %>% # The midpoint between scan start and scan end; we'll use this to define a single scan time
      dplyr::mutate(year = lubridate::year(scan_center_full),
                    month = lubridate::month(scan_center_full),
                    day = lubridate::day(scan_center_full),
                    hour = lubridate::hour(scan_center_full),
                    min = lubridate::minute(scan_center_full),
                    sec = lubridate::second(scan_center_full),
                    scan_center = paste0(year,
                                         stringr::str_pad(string = month, width = 2, side = 'left', pad = '0'),
                                         stringr::str_pad(string = day, width = 2, side = 'left', pad = '0'),
                                         stringr::str_pad(string = hour, width = 2, side = 'left', pad = '0'),
                                         stringr::str_pad(string = min, width = 2, side = 'left', pad = '0'),
                                         stringr::str_pad(string = sec, width = 2, side = 'left', pad = '0'))) %>% 
      dplyr::select(target_goes, data_product, year, month, day, hour, min, sec, doy, filename, scan_start_full, scan_end_full, scan_center_full, scan_start, scan_end, scan_center, aws_path,  aws_path_raw)
    
  } # end if statement checking if file exists
} # end function


target_goes <- c("goes16", "goes17")
get_latest_goes <- TRUE

sapply(glue::glue("data/out/{target_goes}_conus"), FUN = dir.create, recursive = TRUE, showWarnings = FALSE)
goes <- purrr::map_dfr(target_goes, .f = aws_ls_goes, get_latest_goes = TRUE)

readr::write_csv(x = goes, file = glue::glue("data/out/goes_conus-filenames.csv"))