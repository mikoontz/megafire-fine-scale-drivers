install.packages("USAboundariesData", repos = "http://packages.ropensci.org", type = "source")

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

dir.create("data/out/goes/california", recursive = TRUE, showWarnings = FALSE)

source("workflow/00_credentials.R")

# Read in the GOES metadata acquired from Amazon Earth using 01_ls-goes-files-from-aws.R script
goes_meta <- readr::read_csv(file = glue::glue("data/out/goes_conus-filenames.csv"), col_types = "cciiiiinicTTTcccccccc")

dir.create(glue::glue("data/raw/goes/california"), 
           showWarnings = FALSE, recursive = TRUE)

california_geom <- USAboundaries::us_states(resolution = "high", states = "California")

fire_flags <- 
  readr::read_csv(file = "data/out/goes-mask-meanings.csv") %>% 
  dplyr::filter(stringr::str_detect(flag_meanings, pattern = "_fire_pixel")) %>% 
  dplyr::filter(stringr::str_detect(flag_meanings, pattern = "no_fire_pixel", negate = TRUE)) %>% 
  dplyr::pull(flag_vals)

subset_goes_to_california <- function(aws_url, local_path, scan_center, filebasename, ...) {
  
  # download all the raw .nc files for the goes detections
  system2(command = "aws", args = glue::glue("s3 cp {aws_url} {local_path} --no-sign-request"))
  
  this <- terra::rast(local_path)
  
  ca_goes_geom <- 
    sf::st_transform(california_geom, crs = terra::crs(this)) %>% 
    sf::st_set_crs(NA) %>% # need to use this weird trick because terra doesn't do well with sf CRS's right now
    terra::vect()
  
  terra::crs(ca_goes_geom) <- terra::crs(this) # finish the trick by setting the crs to what we know it is
  
  this_ca <-
    this %>% 
    terra::crop(ca_goes_geom) %>% # crop to just California
    terra::mask(mask = ca_goes_geom) %>%  # mask out all the cells outside california (turn to NA)
    as.data.frame(xy = TRUE, cell = TRUE) %>%  # convert to data frame
    dplyr::filter(!is.na(Mask)) %>%  # filter out all of the masked cells
    dplyr::filter(Mask %in% fire_flags) %>% # filter to just fire pixels
    sf::st_as_sf(coords = c("x", "y"), crs = terra::crs(this), remove = FALSE) %>% 
    sf::st_transform(crs = sf::st_crs(3310)$wkt) %>% 
    dplyr::mutate(x_3310 = sf::st_coordinates(.)[, 1],
                  y_3310 = sf::st_coordinates(.)[, 2]) %>%
    sf::st_drop_geometry()
  
  readr::write_csv(x = this_ca, file = glue::glue("data/out/goes/california/{scan_center}_{filebasename}.csv"))
  
  system2(command = "aws", args = glue::glue("s3 cp data/out/goes/california/{scan_center}_{filebasename}.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes_california/{scan_center}_{filebasename}.csv --acl public-read"), stdout = FALSE)
  
  unlink(local_path)
  unlink(glue::glue("data/out/goes/california/{scan_center}_{filebasename}.csv"))
  
  return(terra::crs(this)[[1]])
}

processed_goes <-
  tibble::tibble(aws_files_raw = system2(command = "aws", args = glue::glue("s3 ls s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes_california --recursive"), stdout = TRUE)) %>%
  dplyr::filter(nchar(aws_files_raw) == 163) %>%
  dplyr::mutate(filename_full = stringr::str_sub(string = aws_files_raw, start = 32),
                filename = stringr::str_sub(string = filename_full, start = 45, end = -1))

goes_meta_with_crs_batches <-
  goes_meta %>% 
  dplyr::mutate(processed_name = glue::glue("{scan_center}_{filebasename}.csv")) %>% 
  dplyr::filter(!(processed_name %in% processed_goes$filename)) %>% 
  dplyr::group_by(group = sample(x = 1:96, size = nrow(.), replace = TRUE)) %>% 
  dplyr::group_split()


(start <- Sys.time())
future::plan(strategy = "multiprocess", workers = 96)

goes_meta_with_crs <-
  furrr::future_map_dfr(goes_meta_with_crs_batches, .f = function(x) {
    x %>% dplyr::mutate(crs = purrr::pmap(., .f = subset_goes_to_california))
  })

readr::write_csv(x = goes_meta_with_crs, file = "data/out/goes_conus-filenames-with-crs.csv")

system2(command = "aws", args = glue::glue("s3 cp data/out/goes_conus-filenames-with-crs.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes_conus-filenames-with-crs.csv --acl public-read"))

(difftime(Sys.time(), start))
