dependencies <- c("dplyr", "readr", "glue", "lubridate", "purrr", "ncdf4", "stringr", "here")

needs_install <- !sapply(dependencies, FUN = require, character.only = TRUE)

install.packages(dependencies[needs_install])

sapply(dependencies[needs_install], FUN = require, character.only = TRUE)

dir.create(here::here("data/out/"), recursive = TRUE, showWarnings = FALSE)
dir.create(here::here("data/raw/"), recursive = TRUE, showWarnings = FALSE)

ls_goes <- function(target_goes, get_latest_goes = FALSE) {
  if(get_latest_goes | !file.exists(here::here("data/out/goes_conus-filenames.csv"))) {
    # GOES-16 record begins on 2017-05-24
    
    goes_raw_files <- 
      list.files(glue::glue("data/raw/{target_goes}"), recursive = TRUE, full.names = TRUE)
    
    # bundle the list of filenames and extract some attributes from the
    # metadata embedded in those filenames
    goes_af <-
      tibble::tibble(target_goes = target_goes,
                     data_product = "ABI-L2-FDCC",
                     local_path_full = goes_raw_files,
                     aws_file = stringr::str_sub(string = local_path_full, start = 17, end = -1)) %>% 
      tidyr::separate(col = aws_file, into = c("year", "doy", "hour", "filename"), sep = "/", remove = FALSE) %>% 
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
      dplyr::select(target_goes, data_product, year, month, day, hour, min, sec, doy, filename, scan_start_full, scan_end_full, scan_center_full, scan_start, scan_end, scan_center, local_path_full, aws_file)
    
  } # end if statement checking if file exists
} # end function


target_goes <- c("goes16", "goes17")

goes_meta <- 
  purrr::map_dfr(target_goes, .f = aws_ls_goes, get_latest_goes = TRUE) %>% 
  dplyr::mutate(filebasename = stringr::str_sub(filename, start = 1, end = -4)) %>% 
  dplyr::mutate(aws_url = glue::glue("s3://noaa-{target_goes}/{aws_path}"),
                local_path = glue::glue("data/raw/goes/california/{scan_center}_{filename}"))

readr::write_csv(x = goes_meta, file = here::here("data/out/goes_conus-filenames.csv"))

### write the Mask and DQF meanings to disk

# GOES-16 and GOES-17
# Get the flag values that are important using an example .nc file if not done already
# flag_vals                            flag_meanings
# 10                                   good_fire_pixel
# 11                              saturated_fire_pixel
# 12                     cloud_contaminated_fire_pixel
# 13                       high_probability_fire_pixel
# 14                     medium_probability_fire_pixel
# 15                        low_probability_fire_pixel
# 30               temporally_filtered_good_fire_pixel
# 31          temporally_filtered_saturated_fire_pixel
# 32 temporally_filtered_cloud_contaminated_fire_pixel
# 33   temporally_filtered_high_probability_fire_pixel
# 34 temporally_filtered_medium_probability_fire_pixel
# 35    temporally_filtered_low_probability_fire_pixel
###

if(!file.exists(here::here("data/out/goes-mask-meanings.csv")) | !file.exists(here::here("data/out/goes-dqf-meanings.csv"))) {

  # Get example .nc file
  ex_aws_path <- goes_meta$aws_url[1]
  ex_filename <- goes_meta$filename[1]
  ex_local_path <- here::here("data/raw/goes-example.nc")
  ex_local_path <- "data/raw/goes-example.nc"
  
  system2(command = "aws", args = glue::glue("s3 cp {ex_aws_path} {here::here(ex_local_path)} --no-sign-request"))
  
  this_nc <- ncdf4::nc_open(ex_local_path) %>% ncdf4::ncatt_get(varid = "Mask")
  flag_vals <- this_nc[["flag_values"]]
  flag_meanings <- this_nc[["flag_meanings"]] %>% stringr::str_split(pattern = " ", simplify = TRUE) %>% as.vector()
  flag_df <- data.frame(flag_vals, flag_meanings)
  
  readr::write_csv(x = flag_df, file = here::here("data/out/goes-mask-meanings.csv"))
  
  this_nc <- ncdf4::nc_open(ex_local_path) %>% ncdf4::ncatt_get(varid = "DQF")
  flag_vals <- this_nc[["flag_values"]]
  flag_meanings <- this_nc[["flag_meanings"]] %>% stringr::str_split(pattern = " ", simplify = TRUE) %>% as.vector()
  flag_df <- data.frame(flag_vals, flag_meanings)
  
  readr::write_csv(x = flag_df, file = here::here("data/out/goes-dqf-meanings.csv"))
  
}
