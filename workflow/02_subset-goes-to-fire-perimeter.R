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

#### Create directories
dir.create(here::here("data/out/california_goes"), recursive = TRUE, showWarnings = FALSE)

#### Global variables ####

california_geom <- USAboundaries::us_states(resolution = "high", states = "California")
expected_cols <- c("x", "y", "Area", "Temp", "Mask", "Power", "DQF", "cell")
n_workers <- 16 # number of cores to split the GOES subsetting process up into
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
  
  no_fire_flags <-
    readr::read_csv(file = here::here("data/out/goes-mask-meanings.csv")) %>% 
    dplyr::filter(stringr::str_detect(flag_meanings, pattern = "no_fire_pixel")) %>% 
    dplyr::pull(flag_vals)
  
  return(list(no_fire_flags = no_fire_flags, fire_flags = fire_flags))
}
#### subset GOES images to fire detections in California

subset_goes_to_target_geom <- function(local_path_full, target_geom, target_crs, ...) {
  
  # out <- vector(mode = "list", length = nrow(this_batch))
  
  # for (k in 1:nrow(this_batch)) {
    this <- stars::read_stars(here::here(local_path_full), quiet = TRUE)
    
    this_crs <- sf::st_crs(this)$wkt
    
    target_geom_goes_transform <- 
      sf::st_transform(target_geom, crs = sf::st_crs(this))
    
    this_within_target_geom <-
      this %>% 
      sf::st_crop(target_geom_goes_transform) %>% # crop to just California
      dplyr::mutate(cell = 1:prod(dim(.))) %>%  # explicitly add the 'cell number' by multiplying nrow by ncol
      stars::st_transform_proj(crs = target_crs)
  #   missing_cols <- expected_cols[!(expected_cols %in% names(this_ca))]
  #   
  #   for (j in seq_along(missing_cols)) {
  #     print(local_path_full)
  #     this_ca[, missing_cols[j]] <- NA_real_
  #   }
  #   
  #   this_within_target_geom <-
  #     this_within_target_geom %>% 
  #     dplyr::select(dplyr::all_of(expected_cols)) %>%  # convert to data frame
  #     dplyr::filter(!is.na(Mask)) %>%  # filter out all of the masked cells
  #     dplyr::filter(!(Mask %in% no_fire_flags)) %>% # filter out all pixels that are correcly processed but reported as "not fire" 
  #     # dplyr::filter(Mask %in% fire_flags) %>% # filter to just fire pixels
  #     sf::st_as_sf(coords = c("x", "y"), crs = sf::st_crs(this), remove = FALSE) %>% 
  #     sf::st_transform(crs = sf::st_crs(target_crs)) %>% 
  #     dplyr::mutate(x_local = sf::st_coordinates(.)[, 1],
  #                   y_local = sf::st_coordinates(.)[, 2]) %>%
  #     sf::st_drop_geometry()
  #   
  #   readr::write_csv(x = this_ca, file = glue::glue("{here::here()}/data/out/california_goes/{target_goes}_{year}/{processed_filename}"))
  #   
  #   out[[k]] <- dplyr::tibble(local_path_full, processed_filename, crs = this_crs)
  # }
  # 
  # crs_df <- data.table::rbindlist(out)
  
  return(this_within_target_geom)
}

#### End function to subset goes images to just fire detections in california

goes16_2020_meta <- readr::read_csv(file = "data/out/goes16_2020_conus-filenames.csv")
goes17_2020_meta <- readr::read_csv(file = "data/out/goes17_2020_conus-filenames.csv")
goes_meta <- dplyr::bind_rows(goes16_2020_meta, goes17_2020_meta)

fire_flag_meanings <- create_mask_lookup_table("goes16", "2020", upload = TRUE)
no_fire_flags <- fire_flag_meanings$no_fire_flags
fire_flags <- fire_flag_meanings$fire_flags

# Get the fire perimeters that we'll be using!

fires <- sf::st_read("data/out/megafire-events.gpkg")

i = 1
this_fire <- fires[i, ]
# set up batches of goes metadata to iterate over for processing

goes_meta_batches <-
  goes_meta %>%
  # dplyr::filter(scan_center_full >= this_fire$alarm_date & scan_center_full <= this_fire$cont_date)
  dplyr::filter(scan_center_full >= this_fire$alarm_date & scan_center_full <= lubridate::ymd("2020-10-05")) %>% 
  dplyr::mutate(n_fire_pixels = ncdf4::ncvar_get(ncdf4::nc_open(nc_path), varid = "total_number_of_pixels_with_fires_detected")) %>% 
  dplyr::filter(n_fire_pixels > 0)

target_geom <- sf::st_geometry(this_fire)

test <- 
  goes_meta_batches %>% 
  dplyr::slice(1:5) %>% 
  dplyr::mutate(star = purrr::pmap(.l = ., .f = subset_goes_to_target_geom, target_geom = target_geom, target_crs = 3310))

nc_path <- goes_meta_batches$local_path_full[1]
nc <- ncdf4::nc_open(nc_path)
test <- 
  goes_meta_batches %>% 
  dplyr::slice(1:100) %>% 
  



goes_meta_batches %>% 
  dplyr::group_by(year, month, day, hour, min) %>% 
  tally()

  dplyr::mutate(group = sample(x = 1:(n_workers * pb_precision), size = nrow(.), replace = TRUE)) %>% 
  dplyr::group_by(group) %>% 
  dplyr::group_split()

goes_with_crs <-
  pbapply::pblapply(goes_meta_batches, cl = n_workers, FUN = subset_goes_to_california,
                    target_goes = target_goes,
                    year = year,
                    california_geom = california_geom,
                    expected_cols = expected_cols,
                    no_fire_flags = no_fire_flags)

this_target_goes_year_crs <- data.table::rbindlist(goes_with_crs)

(difftime(time1 = Sys.time(), time2 = start, units = "mins"))

data.table::fwrite(x = this_target_goes_year_crs, file = glue::glue("{here::here()}/data/out/{target_goes}_{year}_crs.csv"))

system2(command = "aws", args = glue::glue("s3 cp {here::here()}/data/out/{target_goes}_{year}_crs.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/{target_goes}_{year}_crs.csv --acl public-read"))

system2(command = "aws", args = glue::glue("s3 sync {here::here()}/data/out/california_goes/{target_goes}_{year}/ s3://earthlab-mkoontz/megafire-fine-scale-drivers/california_goes/{target_goes}_{year}/ --acl public-read"))

} # end for loop; move on to the next combination of target_goes/year