# One script to do all GOES processing, so we can work with chunks of it at
# a time.

#### Dependencies ####
dependencies <- c("dplyr", 
                  "furrr", "future",
                  "glue", 
                  "here",
                  "lubridate",
                  "ncdf4",
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
dir.create(here::here("data/out/goes_california"), recursive = TRUE, showWarnings = FALSE)

#### Global variables ####

california_geom <- USAboundaries::us_states(resolution = "high", states = "California")
expected_cols <- c("x", "y", "Area", "Temp", "Mask", "Power", "DQF", "cell")
n_workers <- 48 # number of cores to split the GOES subsetting process up into

#### Functions ####

#### Sync goes detections from AWS

sync_goes <- function(target_goes, year) {
  (start <- Sys.time())
  system2(command = "aws", args = glue::glue("s3 sync s3://noaa-{target_goes}/ABI-L2-FDCC/{year} data/raw/{target_goes}/ABI-L2-FDCC/{year} --no-sign-request"), stdout = FALSE)
  
  return(NULL)
}

#### End sync function

#### Get metadata from filenames of downloaded GOES images
ls_goes <- function(target_goes, year, upload = TRUE) {
  goes_raw_files <- 
    list.files(glue::glue("data/raw/{target_goes}/ABI-L2-FDCC/{year}"), recursive = TRUE, full.names = TRUE)
  
  # bundle the list of filenames and extract some attributes from the
  # metadata embedded in those filenames
  goes_af <-
    tibble::tibble(target_goes = target_goes,
                   local_path_full = goes_raw_files,
                   aws_file = stringr::str_sub(string = local_path_full, start = 17, end = -1)) %>% 
    tidyr::separate(col = aws_file, into = c("data_product", "year", "doy", "hour", "filename"), sep = "/", remove = FALSE) %>% 
    dplyr::mutate(filebasename = stringr::str_sub(string = filename, start = 1, end = -4)) %>% 
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
                                       stringr::str_pad(string = sec, width = 2, side = 'left', pad = '0')),
                  processed_filename = glue::glue("{scan_center}_{filebasename}.csv")) %>% 
    dplyr::select(target_goes, data_product, year, month, day, hour, min, sec, doy, filename, scan_start_full, scan_end_full, scan_center_full, scan_start, scan_end, scan_center, local_path_full, processed_filename, filebasename, aws_file)
  
  # Write the filenames for the target_goes/year combination to disk
  readr::write_csv(x = goes_af, file = glue::glue("{here::here()}/data/out/{target_goes}_{year}_conus-filenames.csv"))
  
  if(upload) {
    # Write the filenames for the target_goes/year combination to S3 bucket
    system2(command = "aws", args = glue::glue("s3 cp {here::here()}/data/out/{target_goes}_{year}_conus-filenames.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/{target_goes}_{year}_conus-filenames.csv --acl public-read"))
  }
  
  return(goes_af)
}

#### end function to get metadata from goes image filenames

#### Create mask and DQF attribute lookup tables

create_mask_lookup_table <- function(target_goes, year, upload = TRUE) {
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
    
    goes_af <- readr::read_csv(file = glue::glue("{here::here()}/data/out/{target_goes}_{year}_conus-filenames.csv"))
    
    # Get example .nc file
    ex_local_path_full <- goes_af$local_path_full[1]
    
    this_nc <- ncdf4::nc_open(ex_local_path_full) %>% ncdf4::ncatt_get(varid = "Mask")
    flag_vals <- this_nc[["flag_values"]]
    flag_meanings <- this_nc[["flag_meanings"]] %>% stringr::str_split(pattern = " ", simplify = TRUE) %>% as.vector()
    flag_df <- data.frame(flag_vals, flag_meanings)
    
    readr::write_csv(x = flag_df, file = here::here("data/out/goes-mask-meanings.csv"))
    
    this_nc <- ncdf4::nc_open(ex_local_path_full) %>% ncdf4::ncatt_get(varid = "DQF")
    flag_vals <- this_nc[["flag_values"]]
    flag_meanings <- this_nc[["flag_meanings"]] %>% stringr::str_split(pattern = " ", simplify = TRUE) %>% as.vector()
    flag_df <- data.frame(flag_vals, flag_meanings)
    
    readr::write_csv(x = flag_df, file = here::here("data/out/goes-dqf-meanings.csv"))
    
    if (upload) {
      system2(command = "aws", 
              args = glue::glue("s3 cp {here::here()}/data/out/goes-mask-meanings.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes-mask-meanings.csv --acl public-read"))
      
      system2(command = "aws", 
              args = glue::glue("s3 cp {here::here()}/data/out/goes-dqf-meanings.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes-dqf-meanings.csv  --acl public-read"))
    }
  }
  
  fire_flags <- 
    readr::read_csv(file = here::here("data/out/goes-mask-meanings.csv")) %>% 
    dplyr::filter(stringr::str_detect(flag_meanings, pattern = "_fire_pixel")) %>% 
    dplyr::filter(stringr::str_detect(flag_meanings, pattern = "no_fire_pixel", negate = TRUE)) %>% 
    dplyr::pull(flag_vals)
  
  return(fire_flags)
}
#### subset GOES images to fire detections in California

subset_goes_to_california <- function(local_path_full, processed_filename, ...) {
  this <- stars::read_stars(here::here(local_path_full))
  
  this_crs <- sf::st_crs(this)$wkt
  
  ca_goes_geom <- 
    sf::st_transform(california_geom, crs = sf::st_crs(this))
  
  this_ca <-
    this %>% 
    sf::st_crop(ca_goes_geom) %>% # crop to just California
    dplyr::mutate(cell = 1:prod(dim(.))) %>% # explicitly add the 'cell number' by multiplying nrow by ncol
    as_tibble()
  
  missing_cols <- expected_cols[!(expected_cols %in% names(this_ca))]
  
  for (j in seq_along(missing_cols)) {
    print(local_path_full)
    this_ca[, missing_cols[j]] <- NA_real_
  }
  
  this_ca <-
    this_ca %>% 
    dplyr::select(dplyr::all_of(expected_cols)) %>%  # convert to data frame
    dplyr::filter(!is.na(Mask)) %>%  # filter out all of the masked cells
    dplyr::filter(Mask %in% fire_flags) %>% # filter to just fire pixels
    sf::st_as_sf(coords = c("x", "y"), crs = sf::st_crs(this), remove = FALSE) %>% 
    sf::st_transform(crs = sf::st_crs(3310)) %>% 
    dplyr::mutate(x_3310 = sf::st_coordinates(.)[, 1],
                  y_3310 = sf::st_coordinates(.)[, 2]) %>%
    sf::st_drop_geometry()
  
  readr::write_csv(x = this_ca, file = glue::glue("{here::here()}/data/out/california_goes/{target_goes}_{year}/{processed_filename}"))
  
  crs_df <- data.frame(local_path_full, processed_filename, crs = this_crs)
  
  return(crs_df)
}

#### End function to subset goes images to just fire detections in california




goes_year_buckets <-
  lapply(c("goes16", "goes17"), FUN = function(target_goes) {
    years <- 
      system2(command = "aws", args = glue::glue("s3 ls s3://noaa-{target_goes}/ABI-L2-FDCC/  --no-sign-request"), stdout = TRUE) %>% 
      stringr::str_extract_all(pattern = "[0-9]+") %>% 
      unlist()
    
    return(dplyr::tibble(target_goes = target_goes, year = years))
  }) %>% 
  dplyr::bind_rows()

for (i in 1:nrow(goes_year_buckets)) {
  target_goes <- goes_year_buckets$target_goes[i]
  year <- goes_year_buckets$year[i]
  
  dir.create(glue::glue("{here::here()}/data/out/california_goes/{target_goes}_{year}/"), recursive = TRUE, showWarnings = FALSE)
  
  sync_goes(target_goes, year)
  goes_meta <- ls_goes(target_goes, year, upload = TRUE)
  fire_flags <- create_mask_lookup_table(target_goes, year, upload = TRUE)
  
  # set up batches of goes metadata to iterate over for processing
  goes_meta_batches <-
    goes_meta %>% 
    dplyr::mutate(group = sample(x = 1:n_workers, size = nrow(.), replace = TRUE)) %>% 
    dplyr::group_by(group) %>% 
    dplyr::group_split()
  
  # set up parallelization
  (start <- Sys.time())
  future::plan(strategy = "multiprocess", workers = n_workers)
  
  # parallelize over batches (parallelize to put each batch on a separate core)
  # iterate over rows in the batch (this gets done sequentially on each core)
  goes_with_crs <-
    furrr::future_map_dfr(goes_meta_batches, .f = function(x) {
      x %>% 
        dplyr::select(local_path_full, processed_filename) %>% 
        purrr::pmap_dfr(.f = subset_goes_to_california)
    })
  
  future::plan(strategy = "sequential")
  
  (difftime(time1 = Sys.time(), time2 = start, units = "mins"))
  
  readr::write_csv(x = goes_with_crs, file = glue::glue("{here::here()}/data/out/{target_goes}_{year}_conus-crs.csv"))
  
  system2(command = "aws", args = glue::glue("s3 cp {here::here()}/data/out/{target_goes}_{year}_conus-crs.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/{target_goes}_{year}_conus-crs.csv --acl public-read"))
  
  system2(command = "aws", args = glue::glue("s3 sync {here::here()}/data/out/ s3://earthlab-mkoontz/megafire-fine-scale-drivers/california_goes/{target_goes}_{year}/ --acl public-read"))
  
} # end for loop; move on to the next combination of target_goes/year