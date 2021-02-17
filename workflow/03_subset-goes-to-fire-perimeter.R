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
                  "parallel",
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

source("workflow/fxn_create-mask-lookup-table.R")

#### subset GOES images to fire detections in specific fire perimeter, write to disk as .gpkg
subset_goes_to_target_geom <- function(this_batch, target_geom, target_crs, target_dir, ...) {
  
  out <- vector(mode = "list", length = nrow(this_batch))
  
  for (k in 1:nrow(this_batch)) {
    this <- stars::read_stars(here::here(this_batch$local_path_full[k]), quiet = TRUE)
    
    this_crs <- sf::st_crs(this)$wkt
    
    buffered_geom_goes_transform <- 
      target_geom %>% 
      sf::st_transform(crs = sf::st_crs(this)) %>% 
      sf::st_bbox() %>% # bbox will be a much simpler geometry to buffer, since we're just using this as a first pass
      sf::st_as_sfc() %>% 
      sf::st_buffer(dist = 5000) # add an extra 3000 meters to the bounding box of the fire perimeter to capture neighboring GOES pixels 
    
    this_within_target_geom <-
      this %>% 
      dplyr::mutate(cell = as.integer(1:prod(dim(.)))) %>%  # explicitly add the 'cell number' by multiplying nrow by ncol
      sf::st_crop(buffered_geom_goes_transform) %>% # crop to less than CONUS, but larger than fire
      stars::st_transform_proj(crs = target_crs) %>% 
      sf::st_as_sf() %>% 
      dplyr::mutate(scan_center = this_batch$scan_center[k],
                    satellite = this_batch$target_goes[k]) %>% 
      dplyr::select(scan_center, satellite, everything())
    
    sf::st_write(obj = this_within_target_geom, dsn = glue::glue("{target_dir}/{this_batch$processed_filename[k]}"), 
                 delete_dsn = TRUE,
                 quiet = TRUE)
    
    out[[k]] <- dplyr::tibble(local_path_full = this_batch$local_path_full[k], processed_filename = this_batch$processed_filename[k], crs = this_crs)
  }
  
  crs_df <- data.table::rbindlist(out)
  
  return(crs_df)
}
#### End function to subset goes images to just fire detections in target geom

####

### Get the GOES metadata
download <- TRUE

if(!file.exists(here::here("data/out/goes16_2020_conus-filenames.csv"))) {
  if(download) {
    system2(command = "aws", args = glue::glue("s3 cp s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes16_2020_conus-filenames.csv {here::here()}/data/out/goes16_2020_conus-filenames.csv"))
  } else {
    source("workflow/02_get-goes-and-metadata.R")
  }
}

if(!file.exists(here::here("data/out/goes17_2020_conus-filenames.csv"))) {
  if(download) {
    system2(command = "aws", args = glue::glue("s3 cp s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes17_2020_conus-filenames.csv {here::here()}/data/out/goes17_2020_conus-filenames.csv"))
  } else {
    source("workflow/02_get-goes-and-metadata.R")
  }
}

goes16_2020_meta <- readr::read_csv(file = "data/out/goes16_2020_conus-filenames.csv")
goes17_2020_meta <- readr::read_csv(file = "data/out/goes17_2020_conus-filenames.csv")

goes_meta <- dplyr::bind_rows(goes16_2020_meta, goes17_2020_meta)

### Get the fire flag and no fire flag values
fire_flags <-
  readr::read_csv(file = here::here("data/out/goes-mask-meanings.csv")) %>%
  dplyr::filter(stringr::str_detect(flag_meanings, pattern = "_fire_pixel")) %>%
  dplyr::filter(stringr::str_detect(flag_meanings, pattern = "no_fire_pixel", negate = TRUE)) %>%
  dplyr::pull(flag_vals)

no_fire_flags <-
  readr::read_csv(file = here::here("data/out/goes-mask-meanings.csv")) %>% 
  dplyr::filter(stringr::str_detect(flag_meanings, pattern = "no_fire_pixel")) %>% 
  dplyr::pull(flag_vals)

# Get the fire perimeters that we'll be using!

fires <- sf::st_read("data/out/megafire-events.gpkg")

# We're building a prototype using the Creek Fire
i = 1
this_fire <- fires[i, ]
target_geom <- sf::st_geometry(this_fire)

#### Create directories
this_fire_dir <- glue::glue("{here::here()}/data/out/fires/{this_fire$IncidentName}")
dir.create(glue::glue("{this_fire_dir}/goes"), recursive = TRUE, showWarnings = FALSE)

if(!file.exists(glue::glue("{this_fire_dir}/goes-crs-table.csv"))) {
  system2(command = "aws", args = glue::glue("s3 cp s3://earthlab-mkoontz/megafire-fine-scale-drivers/{this_fire$IncidentName}/goes-crs-table.csv {this_fire_dir}/goes-crs-table.csv"))
}

# set up batches of goes metadata to iterate over for processing
old_crs_table <- try(readr::read_csv(glue::glue("{this_fire_dir}/goes-crs-table.csv")))

if("try-error" %in% class(old_crs_table)) {
  already_processed_goes <- character(0)
} else {
  
  already_processed_goes <- 
    old_crs_table %>% 
    dplyr::pull(processed_filename)
}

this_fire_goes_meta_batches <-
  goes_meta %>%
  dplyr::filter(!(processed_filename %in% already_processed_goes)) %>%
  dplyr::filter(scan_center_full >= this_fire$alarm_date & scan_center_full <= this_fire$cont_date) %>% 
  dplyr::mutate(group = sample(x = 1:(pb_precision * n_workers), size = nrow(.), replace = TRUE)) %>% 
  dplyr::group_by(group) %>% 
  dplyr::group_split()

(start <- Sys.time())
# setup parallelization
if (.Platform$OS.type == "windows") {
  cl <- parallel::makeCluster(n_workers)
  parallel::clusterEvalQ(cl = cl, expr = {
    library(dplyr)
    library(sf)
    library(data.table)
    library(stars)
  })
} else {
  cl <- n_workers
}

# work all of the relevant GOES images for the Creek fire and write them to .gpkg files on disk
crs_table_per_batch <- 
  this_fire_goes_meta_batches %>%
  pbapply::pblapply(X = ., 
                    cl = cl,
                    FUN = subset_goes_to_target_geom, # function to be applied to each batch
                    target_geom = target_geom, # extra arguments
                    target_crs = 3310, # the crs to transform the GOES detections to
                    target_dir = glue::glue("{this_fire_dir}/goes")) # directory to store the processed files

crs_table <- data.table::rbindlist(crs_table_per_batch) # function returns the crs, so rbind them all together and write to disk
crs_table <- 
  crs_table %>% 
  dplyr::as_tibble() %>% 
  dplyr::bind_rows(old_crs_table) %>% 
  dplyr::arrange(local_path_full)

data.table::fwrite(x = crs_table, file = glue::glue("{this_fire_dir}/goes-crs-table.csv"))

if (.Platform$OS.type == "windows") {
  parallel::stopCluster(cl)
}

(difftime(time1 = Sys.time(), time2 = start, units = "mins"))

system2(command = "aws", args = glue::glue("s3 cp {this_fire_dir}/goes-crs-table.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/{this_fire$IncidentName}/goes-crs-table.csv --acl public-read"))

system2(command = "aws", args = glue::glue("s3 sync {this_fire_dir}/goes s3://earthlab-mkoontz/megafire-fine-scale-drivers/{this_fire$IncidentName}/goes --acl public-read"))
