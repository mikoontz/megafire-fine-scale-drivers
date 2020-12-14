# One script to do all GOES processing, so we can work with chunks of it at
# a time.

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

#### Global variables ####

california_geom <- USAboundaries::us_states(resolution = "high", states = "California")
expected_cols <- c("x", "y", "Area", "Temp", "Mask", "Power", "DQF", "cell")
n_workers <- 10 # number of cores to split the GOES subsetting process up into
pbo <- pbapply::pboptions() # original pbapply options (to easily reset)

# pb_precision becomes the multiplier for how many list items the goes
# it represents the number of steps between a progress bar of 0 and a progress bar of 100%
# metadata gets broken down into; e.g., total number of list items = n_workers * pb_precision
pb_precision <- 100

#### Functions ####

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
                  processed_filename = glue::glue("{scan_center}_{filebasename}.gpkg")) %>% 
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
  
  no_fire_flags <-
    readr::read_csv(file = here::here("data/out/goes-mask-meanings.csv")) %>% 
    dplyr::filter(stringr::str_detect(flag_meanings, pattern = "no_fire_pixel")) %>% 
    dplyr::pull(flag_vals)
  
  return(list(no_fire_flags = no_fire_flags, fire_flags = fire_flags))
}


#### subset GOES images to fire detections in specific fire perimeter, write to disk as .gpkg
subset_goes_to_target_geom <- function(this_batch, target_geom, target_crs, target_dir, larger_geom, ...) {
  
  out <- vector(mode = "list", length = nrow(this_batch))
  
  for (k in 1:nrow(this_batch)) {
    this <- stars::read_stars(here::here(this_batch$local_path_full[k]), quiet = TRUE)
    
    this_crs <- sf::st_crs(this)$wkt
    
    target_geom_goes_transform <- 
      sf::st_transform(target_geom, crs = target_crs)
    
    larger_geom_target_transform <- 
      sf::st_transform(target_geom, crs = sf::st_crs(this))
    
    this_within_target_geom <-
      this %>% 
      dplyr::mutate(cell = as.integer(1:prod(dim(.)))) %>%  # explicitly add the 'cell number' by multiplying nrow by ncol
      sf::st_crop(larger_geom_target_transform) %>% # crop to less than CONUS, but larger than fire
      stars::st_transform_proj(crs = target_crs) %>% 
      sf::st_as_sf() %>% 
      sf::st_intersection(target_geom_goes_transform) %>% # Crop to specific fire outline
      dplyr::mutate(scan_center = this_batch$scan_center[k],
                    satellite = this_batch$target_goes[k]) %>% 
      dplyr::select(scan_center, satellite, everything())
    
    sf::st_write(obj = this_within_target_geom, dsn = glue::glue("{target_dir}/{this_batch$processed_filename[k]}"), 
                 delete_dsn = TRUE,
                 quiet = TRUE)
    
    out[[k]] <- dplyr::tibble(this_batch$local_path_full[k], this_batch$processed_filename[k], crs = this_crs)
  }
  
  crs_df <- data.table::rbindlist(out)
  
  return(crs_df)
}

#### End function to subset goes images to just fire detections in california
overwrite <- FALSE

if(overwrite | !file.exists(here::here("data/out/goes16_2020_conus-filenames.csv"))) {
  goes16_2020_meta <- ls_goes(target_goes = "goes16", year = "2020")
} else {
  goes16_2020_meta <- readr::read_csv(file = "data/out/goes16_2020_conus-filenames.csv")
}

if(overwrite | !file.exists(here::here("data/out/goes17_2020_conus-filenames.csv"))) {
  goes17_2020_meta <- ls_goes(target_goes = "goes17", year = "2020")
} else {
  goes17_2020_meta <- readr::read_csv(file = "data/out/goes17_2020_conus-filenames.csv")
}

goes_meta <- dplyr::bind_rows(goes16_2020_meta, goes17_2020_meta)

fire_flag_meanings <- create_mask_lookup_table("goes16", "2020", upload = TRUE)
no_fire_flags <- fire_flag_meanings$no_fire_flags
fire_flags <- fire_flag_meanings$fire_flags

# Get the fire perimeters that we'll be using!

fires <- sf::st_read("data/out/megafire-events.gpkg")

i = 1
this_fire <- fires[i, ]

#### Create directories
this_fire_dir <- glue::glue("{here::here()}/data/out/fires/{this_fire$IncidentName}")

dir.create(glue::glue("{this_fire_dir}/goes"), recursive = TRUE, showWarnings = FALSE)

# set up batches of goes metadata to iterate over for processing

this_fire_goes_meta_batches <-
  goes_meta %>%
  # dplyr::filter(scan_center_full >= this_fire$alarm_date & scan_center_full <= this_fire$cont_date)
  dplyr::filter(scan_center_full >= this_fire$alarm_date & scan_center_full <= lubridate::ymd("2020-10-05")) %>% 
  dplyr::mutate(group = sample(x = 1:(pb_precision * n_workers), size = nrow(.), replace = TRUE)) %>% 
  dplyr::group_by(group) %>% 
  dplyr::group_split()

target_geom <- sf::st_geometry(this_fire)

this_batch <-   this_fire_goes_meta_batches %>%
  magrittr::extract2(1)

crs_table_per_batch <- 
  this_fire_goes_meta_batches %>%
  magrittr::extract(1) %>% 
  pbapply::pblapply(X = ., FUN = subset_goes_to_target_geom, target_geom = target_geom, target_crs = 3310, target_dir = glue::glue("{this_fire_dir}/goes"), larger_geom = california_geom)

crs_table <- data.table::rbindlist(crs_table_per_batch)

goes_test <- stars::read_stars("data/raw/goes16/ABI-L2-FDCC/2020/023/05/OR_ABI-L2-FDCC-M6_G16_s20200230501132_e20200230503505_c20200230504185.nc")
sf::st_buffer(target_geom)

larger_geom_target_transform <- 
  sf::st_transform(target_geom, crs = sf::st_crs(goes_test))

sf::st_buffer(larger_geom_target_transform, 1)

test <- 
  glue::glue("{this_fire_dir}/goes/{this_fire_goes_meta_batches[[1]]$processed_filename[10]}") %>% 
  sf::st_read()

plot(test$geom)

(difftime(time1 = Sys.time(), time2 = start, units = "mins"))

data.table::fwrite(x = crs_table, file = glue::glue("{this_fire_dir}/goes-crs-table.csv"))

# system2(command = "aws", args = glue::glue("s3 cp {here::here()}/data/out/{target_goes}_{year}_crs.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/{target_goes}_{year}_crs.csv --acl public-read"))
# 
# system2(command = "aws", args = glue::glue("s3 sync {here::here()}/data/out/california_goes/{target_goes}_{year}/ s3://earthlab-mkoontz/megafire-fine-scale-drivers/california_goes/{target_goes}_{year}/ --acl public-read"))
