dependencies <- c("dplyr", "stringr", "readr", "glue", "USAboundaries", "terra", "sf", "purrr", "furrr", "future", "here")

needs_install <- !sapply(dependencies, FUN = require, character.only = TRUE)

install.packages(dependencies[needs_install])

sapply(dependencies[needs_install], FUN = require, character.only = TRUE)

if(!require(USAboundariesData)) {
  install.packages("USAboundariesData", repos = "http://packages.ropensci.org", type = "source")
}

require(USAboundariesData)

dir.create(here::here("data/out/goes/california"), recursive = TRUE, showWarnings = FALSE)

# source("workflow/00_credentials.R")

if(!file.exists(here::here("data/out/goes_conus-filenames.csv")) | !file.exists(here::here("data/out/goes-mask-meanings.csv")) | !file.exists(here::here("data/out/goes-dqf-meanings.csv"))) {
  # GOES-16 record begins on 2017-05-24
  system2(command = "aws", args = glue::glue("s3 cp s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes_conus-filenames.csv {here::here()}/data/out/goes_conus-filenames.csv"))
  
  system2(command = "aws", args = glue::glue("s3 cp s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes-mask-meanings.csv {here::here()}/data/out/goes-mask-meanings.csv"))
  
  system2(command = "aws", args = glue::glue("s3 cp s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes-dqf-meanings.csv {here::here()}/data/out/goes-dqf-meanings.csv"))
}

# Read in the GOES metadata acquired from Amazon Earth using 01_ls-goes-files-from-aws.R script
goes_meta <- readr::read_csv(file = here::here("data/out/goes_conus-filenames.csv"), col_types = "cciiiiinicTTTcccccccc")

dir.create(here::here("data/raw/goes/california"), 
           showWarnings = FALSE, recursive = TRUE)

california_geom <- USAboundaries::us_states(resolution = "high", states = "California")

fire_flags <- 
  readr::read_csv(file = here::here("data/out/goes-mask-meanings.csv")) %>% 
  dplyr::filter(stringr::str_detect(flag_meanings, pattern = "_fire_pixel")) %>% 
  dplyr::filter(stringr::str_detect(flag_meanings, pattern = "no_fire_pixel", negate = TRUE)) %>% 
  dplyr::pull(flag_vals)

expected_cols <- c("x", "y", "Area", "Temp", "Mask", "Power", "DQF", "cell")

subset_goes_to_california <- function(local_path_full, processed_name, ...) {
  this <- stars::read_stars(here::here(local_path_full))
  
  ca_goes_geom <- 
    sf::st_transform(california_geom, crs = sf::st_crs(this))
  
  this_ca <-
    this %>% 
    sf::st_crop(ca_goes_geom) %>% # crop to just California
    dplyr::mutate(cell = 1:prod(dim(.))) %>% # explicitly add the 'cell number' by multiplying nrow by ncol
    as_tibble()
  
  missing_cols <- expected_cols[!(expected_cols %in% names(this_ca))]
  
  for (j in seq_along(missing_cols)) {
    this_ca[, missing_cols[j]] <- NA_real_
  }
  
  this_ca <-
    this_ca %>% 
    dplyr::select(expected_cols) %>%  # convert to data frame
    dplyr::filter(!is.na(Mask)) %>%  # filter out all of the masked cells
    dplyr::filter(Mask %in% fire_flags) %>% # filter to just fire pixels
    sf::st_as_sf(coords = c("x", "y"), crs = sf::st_crs(this), remove = FALSE) %>% 
    sf::st_transform(crs = sf::st_crs(3310)) %>% 
    dplyr::mutate(x_3310 = sf::st_coordinates(.)[, 1],
                  y_3310 = sf::st_coordinates(.)[, 2]) %>%
    sf::st_drop_geometry()
  
  readr::write_csv(x = this_ca, file = glue::glue("{here::here()}/data/out/goes/california/{processed_filename}"))
  
  rm(this)
  rm(this_ca)
  rm(ca_goes_geom)
  gc()
  
  return(NULL)
}

processed_goes <-
  tibble::tibble(aws_files_raw = system2(command = "aws", args = glue::glue("s3 ls s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes_california --recursive"), stdout = TRUE)) %>%
  dplyr::filter(nchar(aws_files_raw) == 163) %>%
  dplyr::mutate(filename_full = stringr::str_sub(string = aws_files_raw, start = 32),
                filename = stringr::str_sub(string = filename_full, start = 45, end = -1))

nrow(processed_goes) # how many have been processed?

n_workers <- 15

goes_meta_with_crs_batches <-
  goes_meta %>% 
  dplyr::mutate(processed_name = glue::glue("{scan_center}_{filebasename}.csv")) %>% 
  dplyr::filter(!(processed_name %in% processed_goes$filename)) %>% 
  dplyr::group_by(group = sample(x = 1:n_workers, size = nrow(.), replace = TRUE)) %>% 
  dplyr::group_split()

# remove all variables no longer needed (in case they get copied over to each core?)
rm(processed_goes)
rm(goes_meta)

(start <- Sys.time())
future::plan(strategy = "multiprocess", workers = n_workers)

furrr::future_walk(goes_meta_with_crs_batches, .f = function(x) {
  purrr::pwalk(.l = x, .f = subset_goes_to_california)
})

future::plan(strategy = "sequential")

(difftime(Sys.time(), start))
