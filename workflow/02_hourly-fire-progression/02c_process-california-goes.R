library(dplyr)
library(vroom)
library(stringr)
library(glue)
library(USAboundaries)
library(terra)
library(sf)
library(purrr)
library(furrr)
library(future)

install.packages("USAboundariesData", repos = "http://packages.ropensci.org", type = "source")

# Read in the GOES metadata acquired from Amazon Earth using 01_ls-goes-files-from-aws.R script
goes_meta_orig <- readr::read_csv(file = glue::glue("data/out/goes_conus-filenames.csv"), col_types = "cciiiiinicTTTccccc")

dir.create(glue::glue("data/raw/goes/california"), 
           showWarnings = FALSE, recursive = TRUE)

california_geom <- USAboundaries::us_states(resolution = "high", states = "California")

goes_meta <-
  goes_meta_orig %>% 
  dplyr::mutate(filebasename = stringr::str_sub(filename, start = 1, end = -4)) %>% 
  dplyr::mutate(aws_url = glue::glue("s3://noaa-{target_goes}/{aws_path}"),
                local_path = glue::glue("data/raw/goes/california/{scan_center}_{filename}"))

if(!file.exists("data/out/goes-mask-meanings.csv") | !file.exists("data/out/goes-dqf-meanings.csv")) {
  # Get example .nc file
  ex_aws_path <- goes_meta$aws_url[1]
  ex_filename <- goes_meta$filename[1]
  ex_local_path <- "data/raw/goes-example.nc"
  
  system2(command = "aws", args = glue::glue("s3 cp {ex_aws_path} {ex_local_path} --no-sign-request"))
  
  this_nc <- ncdf4::nc_open(ex_local_path) %>% ncdf4::ncatt_get(varid = "Mask")
  flag_vals <- this_nc[["flag_values"]]
  flag_meanings <- this_nc[["flag_meanings"]] %>% str_split(pattern = " ", simplify = TRUE) %>% as.vector()
  flag_df <- data.frame(flag_vals, flag_meanings)
  
  readr::write_csv(x = flag_df, file = "data/out/goes-mask-meanings.csv")
  
  this_nc <- ncdf4::nc_open(ex_local_path) %>% ncdf4::ncatt_get(varid = "DQF")
  flag_vals <- this_nc[["flag_values"]]
  flag_meanings <- this_nc[["flag_meanings"]] %>% str_split(pattern = " ", simplify = TRUE) %>% as.vector()
  flag_df <- data.frame(flag_vals, flag_meanings)
  
  readr::write_csv(x = flag_df, file = "data/out/goes-dqf-meanings.csv")
  
}

subset_goes_to_california <- function(aws_url, local_path) {
  
  # download all the raw .nc files for the goes detections
  system2(command = "aws", args = glue::glue("s3 cp {aws_url} {local_path} --no-sign-request"))
  
  this <- 
    terra::rast(local_path)
  
  this <-
    stars::read_stars(local_path)
  
  this_ca <- this %>% sf::st_crop(ca_goes_geom)
  plot(this_ca$Mask)
  
  ca_goes_geom <- sf::st_transform(california_geom, crs = terra::crs(this))# %>% terra::vect()
  
  this_ca <-
    this %>% 
    terra::crop(ca_goes_geom) %>%
    terra::mask(mask = ca_goes_geom) %>% 
    as.data.frame(xy = TRUE, cell = TRUE) %>% 
    dplyr::filter(!is.na(Mask))
  
  readr::write_csv(this_ca, file = "data/out/test.csv")
}

new_cat_df <- data.frame(mask = c(0,   10,  11,  12,  13,  14,  15,  30,  31,  32,  33,  34,
                                  35,  40,  50,  60,  100, 120, 121, 123, 124, 125, 126, 127,
                                  150, 151, 152, 153, 170, 180, 182, 185, 186, 187, 188, 200,
                                  201, 205, 210, 215, 220, 225, 230, 240, 245),
                         new_cat = c(7,  1,  2,  3,  4,  5,  6,  1,  2,  3,  4,  5,  6,  7,  7,
                                     7,  0,  7,  7,  8,  8,  8,  8,  8,  9,  9,  9,  9,  10, 10,
                                     10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11))

this_ca$new_cat = new_cat_df$new_cat[match(this_ca$Mask, new_cat_df$mask)]
this_ca %>% filter(new_cat != 9)

'8BC34A',  // 0: Good quality fire-free pixel
'FF5722',  // 1: Good quality fire pixel
'FF1493',  // 2: Saturated fire pixel
'9C27B0',  // 3: Cloud contaminated fire pixel
'FF9800',  // 4: High probability fire pixel
'FFC107',  // 5: Medium probability fire pixel
'FFEB3B',  // 6:Low probability fire pixel
'F5DEB3',  // 7: Invalid pixel due to sunglint, LZA threshold exceeded, off earth, missing input data, or unprocessed
'A9A9A9',  // 8: Invalid pixel due to bad input data
'2196F3',  // 9: Invalid pixel due to to surface type (water or desert)
'C5CAE9',  // 10: Invalid pixel due to algorithm failure
'B3E5FC',  // 11: Invalid pixel due to opaque cloud
]
aws_url <- goes_meta$aws_url[500000]
local_path <- goes_meta$local_path[500000]

this_megafire_goes <-
  list.files(glue::glue("data/raw/goes/{this_megafire_name}"), full.names = TRUE) %>% 
  lapply(stars::read_stars)

megafire_crs <- sapply(this_megafire_goes, terra::crs)


this_megafire_goes <-
  list.files(glue::glue("data/raw/goes/{this_megafire_name}"), full.names = TRUE) %>% 
  lapply(FUN = function(x) {
    
  })












this_megafire_geom <- sf::st_geometry(this_megafire)

goes16 <- this_megafire_goes[[1]] %>% dplyr::mutate(cell = 1:ncell(.))
goes17 <- this_megafire_goes[[2]] %>% dplyr::mutate(cell = 1:ncell(.))

goes16_ca <- goes16 %>% sf::st_crop(sf::st_transform(california_geom, crs = sf::st_crs(goes16)))
goes17_ca <- goes17 %>% sf::st_crop(sf::st_transform(california_geom, crs = sf::st_crs(goes17))) 

goes16_this_megafire <- goes16_ca %>% stars::st_transform_proj(crs = sf::st_crs(3310)) %>% sf::st_as_sf() %>% sf::st_crop(this_megafire_geom)
goes17_this_megafire <- goes17_ca %>% stars::st_transform_proj(crs = sf::st_crs(3310)) %>% sf::st_as_sf() %>% sf::st_crop(this_megafire_geom)

new_grid <- sf::st_intersection(x = goes16_this_megafire, y = goes17_this_megafire) %>% sf::st_intersection(this_megafire_geom)

dir.create("figs/", showWarnings = FALSE)
pdf("figs/creek-goes-overlap.pdf")
plot(new_grid$geometry)
dev.off()

# # Create directory to hold the raw GOES-16 active fire data until it gets deleted
# dir.create(glue::glue("data/raw/{target_goes}_conus/"), showWarnings = FALSE, recursive = TRUE)
# 
# # Create directory to hold all the processed GOES-16 active fire data
# dir.create(glue::glue("data/out/{target_goes}_conus/"), showWarnings = FALSE, recursive = TRUE)


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

# if(!file.exists(glue::glue("data/out/{target_goes}_conus-flag-mask-meanings.csv"))) {
#   # Get example .nc file
#   ex_aws_path <- goes_af$aws_path[1]
#   ex_filename <- goes_af$filename[1]
#   ex_local_path <- glue::glue("data/raw/{target_goes}_conus-example.nc")
#   
#   system2(command = "aws", args = glue::glue("s3 cp s3://noaa-{target_goes}/{ex_aws_path} {ex_local_path} --no-sign-request"))
#   
#   nc <- ncdf4::nc_open(ex_local_path) %>% ncdf4::ncatt_get(varid = "Mask")
#   flag_vals <- nc[["flag_values"]]
#   flag_meanings <- nc[["flag_meanings"]] %>% stringr::str_split(pattern = " ", simplify = TRUE) %>% as.vector()
#   flag_df <- data.frame(flag_vals, flag_meanings)
#   
#   readr::write_csv(x = flag_df, file = glue::glue("data/out/{target_goes}_conus-flag-mask-meanings.csv"))
# }
# 
# fire_flags <- 
#   readr::read_csv(file = glue::glue("data/out/{target_goes}_conus-flag-mask-meanings.csv")) %>% 
#   dplyr::filter(stringr::str_detect(flag_meanings, pattern = "_fire_pixel")) %>% 
#   dplyr::filter(stringr::str_detect(flag_meanings, pattern = "no_fire_pixel", negate = TRUE)) %>% 
#   dplyr::pull(flag_vals)

# Function that downloads the next GOES-16 image, reads it into memory using the {terra} package
get_goes_points <- function(aws_path, filename, scan_center, local_path) {
  
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
  
  system2(command = "aws", args = glue::glue("s3 cp s3://noaa-{target_goes}/{aws_path} {local_path} --no-sign-request"), stdout = FALSE)
  
  # Read in the .nc file using the {terra} package in order to preserve CRS data and values properly (and its fast!)
  goes <- terra::rast(local_path)
  
  # For joining to our curvilinear grid later, we also want to include an attribute
  # representing the cell index values
  
  # Get the crs of the .nc file in order to make the centroids themselves a spatial
  # object
  goes_crs <- terra::crs(goes)
  
  goes_modis_sinu <-
    goes %>%
    terra::as.data.frame(xy = TRUE, cell = TRUE) %>%
    dplyr::filter(Mask %in% fire_flags) %>%
    sf::st_as_sf(coords = c("x", "y"), crs = goes_crs, remove = FALSE) %>%
    sf::st_transform("+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs") %>%
    dplyr::mutate(scan_center = lubridate::parse_date_time2(x = scan_center, orders = "%Y%m%d%H%M%S")) %>%
    dplyr::mutate(rounded_datetime = rounded_datetime) %>%
    dplyr::select(scan_center, rounded_datetime, cellindex, x, y, dplyr::everything()) %>%
    dplyr::mutate(sinu_x = sf::st_coordinates(.)[, 1],
                  sinu_y = sf::st_coordinates(.)[, 2]) %>%
    sf::st_drop_geometry()
  
  readr::write_csv(x = goes_modis_sinu, file = glue::glue("data/out/{target_goes}_conus/{rounded_datetime_txt}_{scan_center}_{filename}.csv"))
  
  system2(command = "aws", args = glue::glue("s3 cp data/out/{target_goes}_conus/{rounded_datetime_txt}_{scan_center}_{filename}.csv s3://earthlab-mkoontz/{target_goes}/{rounded_datetime_txt}_{scan_center}_{filename}.csv"), stdout = FALSE)
  
  unlink(local_path)
  unlink(glue::glue("data/out/{target_goes}_conus/{rounded_datetime_txt}_{scan_center}_{filename}.csv"))
  
  rm(goes)
  rm(goes_modis_sinu)
  rm(cellindex)
  
  return(NULL)
}

































# # Get the file names of the data that have already been processed
# processed_goes <-
#   tibble::tibble(aws_files_raw = system2(command = "aws", args = glue::glue("s3 ls s3://earthlab-mkoontz/{target_goes}_conus/{target_goes} --recursive"), stdout = TRUE)) %>%
#   dplyr::filter(nchar(aws_files_raw) == 139) %>%
#   dplyr::mutate(filename_full = stringr::str_sub(string = aws_files_raw, start = 39),
#                 filename = stringr::str_sub(string = filename_full, start = 29, end = -5))

# going for a parallelized parallelization approach
# Divide data into 4 separate batches, work on a different EC2 instance for each
# Parallelize on each of the EC2 instances

# divide the goes_af into batches
n_batches <- 1
n_subbatches <- 5 # number of cores

# Only need to process the GOES file if processed data don't yet exist
batches <- 
  goes_af %>% 
  dplyr::mutate(filebase = stringr::str_sub(string = filename, start = 1, end = -4)) %>% 
  dplyr::filter(!(filebase %in% processed_goes$filename)) %>% 
  base::split(f = sample(1:n_batches, size = nrow(.), replace = TRUE))

# multicore processing for batch j
j <- 1

subbatches <- 
  batches[[j]] %>% 
  base::split(f = sample(1:n_subbatches, size = nrow(.), replace = TRUE))

(start <- Sys.time())
subbatches <- subbatches[1]
this_batch <- subbatches[[1]]
future::plan(strategy = "sequential")

furrr::future_map(.x = subbatches, .f = function(this_batch) {
  
  this_goes <- 
    this_batch %>% 
    slider::slide(.f = ~ .) %>% 
    magrittr::extract2(1) %>%  # Using slider::slide() as a rowwise iterator
    purrr::map(.f = function(this_goes) {
      
      aws_path <- this_goes$aws_path
      filename <- stringr::str_sub(this_goes$filename, start = 1, end = -4)
      scan_center <- this_goes$scan_center
      local_path <- glue::glue("data/raw/{target_goes}_conus/{scan_center}_{filename}.nc")
      
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
      
      system2(command = "aws", args = glue::glue("s3 cp s3://noaa-{target_goes}/{aws_path} {local_path} --no-sign-request"), stdout = FALSE)
      
      # Read in the .nc file using the {terra} package in order to preserve CRS data and values properly (and its fast!)
      prof <- profvis::profvis(expr = {
        goes <- terra::rast(local_path)
        
        # For joining to our curvilinear grid later, we also want to add a new raster layer with the cellindex values
        cellindex <- goes["DQF"]
        cellindex <- terra::setValues(x = cellindex, values = terra::cells(cellindex)) %>% stats::setNames("cellindex")
      })
      # Stack the original .nc file with the `cellindex` raster layer
      goes <- c(goes, cellindex)
      
      # Get the crs of the .nc file; This will be important later because the satellite moved in 2017
      # from its initial testing position to its operational position, and so the raster cells
      # are representing different areas on the Earth when that happened (encoded in the CRS though)
      goes_crs <- terra::crs(goes)
      
      prof <- profvis::profvis(expr = {
        goes_modis_sinu <- terra::as.data.frame(x = goes, xy = TRUE, cells = TRUE)
        goes_modis_sinu <- dplyr::filter(goes_modis_sinu, Mask %in% fire_flags)
        goes_modis_sinu <- sf::st_as_sf(goes_modis_sinu, coords = c("x", "y"), crs = goes_crs, remove = FALSE)
        goes_modis_sinu <- sf::st_transform(goes_modis_sinu, "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs")
        goes_modis_sinu <- dplyr::mutate(goes_modis_sinu, scan_center = lubridate::parse_date_time2(x = scan_center, orders = "%Y%m%d%H%M%S"))
        goes_modis_sinu <- dplyr::mutate(goes_modis_sinu, rounded_datetime = rounded_datetime)
        goes_modis_sinu <- dplyr::select(goes_modis_sinu, scan_center, rounded_datetime, cellindex, x, y, dplyr::everything()) 
        goes_modis_sinu_coords <- sf::st_coordinates(goes_modis_sinu)
        goes_modis_sinu <- dplyr::mutate(goes_modis_sinu, sinu_x = goes_modis_sinu_coords[, 1],
                                         sinu_y = goes_modis_sinu_coords[, 2])
        goes_modis_sinu <- sf::st_drop_geometry(goes_modis_sinu)
      })
      
      readr::write_csv(x = goes_modis_sinu, file = glue::glue("data/out/{target_goes}_conus/{rounded_datetime_txt}_{scan_center}_{filename}.csv"))
      
      system2(command = "aws", args = glue::glue("s3 cp data/out/{target_goes}_conus/{rounded_datetime_txt}_{scan_center}_{filename}.csv s3://earthlab-mkoontz/{target_goes}/{rounded_datetime_txt}_{scan_center}_{filename}.csv"), stdout = FALSE)
      
      unlink(local_path)
      unlink(glue::glue("data/out/{target_goes}_conus/{rounded_datetime_txt}_{scan_center}_{filename}.csv"))
      
      rm(goes)
      rm(goes_modis_sinu)
      rm(cellindex)
    })
})
# })

prof

# future::plan(strategy = "multiprocess", workers = n_subbatches)

furrr::future_map(.x = subbatches, .f = function(this_batch) {
  
  out <- 
    this_batch %>% 
    slider::slide(.f = ~ .) %>%  # Using slider::slide() as a rowwise iterator
    purrr::map(.f = function(this_goes) {
      
      aws_path <- this_goes$aws_path
      filename <- stringr::str_sub(this_goes$filename, start = 1, end = -4)
      scan_center <- this_goes$scan_center
      local_path <- glue::glue("data/raw/{target_goes}_conus/{scan_center}_{filename}.nc")
      
      get_goes_points(aws_path, filename, scan_center, local_path)
      
    })
  
})

(difftime(Sys.time(), start))
