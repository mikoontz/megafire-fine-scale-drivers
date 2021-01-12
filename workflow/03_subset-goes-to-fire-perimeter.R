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

source("workflow/fxn_ls-goes.R")
source("workflow/fxn_create-mask-lookup-table.R")
source("workflow/fxn_subset-goes-to-target-geom.R")

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

# We're building a prototype using the Creek Fire
i = 1
this_fire <- fires[i, ]
target_geom <- sf::st_geometry(this_fire)

#### Create directories
this_fire_dir <- glue::glue("{here::here()}/data/out/fires/{this_fire$IncidentName}")
dir.create(glue::glue("{this_fire_dir}/goes"), recursive = TRUE, showWarnings = FALSE)

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
