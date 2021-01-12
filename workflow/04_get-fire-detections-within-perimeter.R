# Figure out which GOES images actually have fire detections

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

n_workers <- 10 # number of cores to split the GOES subsetting process up into
pbo <- pbapply::pboptions() # original pbapply options (to easily reset)

# pb_precision becomes the multiplier for how many list items the goes
# it represents the number of steps between a progress bar of 0 and a progress bar of 100%
# metadata gets broken down into; e.g., total number of list items = n_workers * pb_precision
pb_precision <- 100

#### Functions
source("workflow/fxn_create-mask-lookup-table.R")
source("workflow/fxn_extract-active-fire-detections.R")


### Which Mask values are for fires?
flag_lookup <- create_mask_lookup_table("goes16", "2020", upload = FALSE)
no_fire_flags <- flag_lookup$no_fire_flags
fire_flags <- flag_lookup$fire_flags
fire_flag_meanings <- readr::read_csv("data/out/goes-mask-meanings.csv")

fires <- sf::st_read("data/out/megafire-events.gpkg")

# We're building a prototype using the Creek Fire
i = 1
this_fire <- fires[i, ]
target_geom <- sf::st_geometry(this_fire)

#### Create directories
this_fire_dir <- glue::glue("{here::here()}/data/out/fires/{this_fire$IncidentName}")

goes_crs_table <- readr::read_csv(glue::glue("{this_fire_dir}/goes-crs-table.csv"))

# set up batches of goes metadata to iterate over for processing
goes_sf_batches <-
  goes_crs_table %>%
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
  })
} else {
  cl <- n_workers
}

nonfire_filtered <- 
  goes_sf_batches %>%
  pbapply::pblapply(X = ., 
                    cl = cl,
                    FUN = extract_active_fire_detections, # function to be applied to each batch
                    no_fire_flags = no_fire_flags,
                    this_fire_dir = this_fire_dir)

nonfire_filtered <- 
  data.table::rbindlist(nonfire_filtered) %>% 
  sf::st_as_sf()

sf::st_write(obj = nonfire_filtered, dsn = glue::glue("{this_fire_dir}/goes-nonfire-filtered.gpkg"), delete_dsn = TRUE)

parallel::stopCluster(cl = cl)
(difftime(time1 = Sys.time(), time2 = start, units = "mins"))

system2(command = "aws", args = glue::glue("s3 cp {this_fire_dir}/goes-nonfire-filtered.gpkg s3://earthlab-mkoontz/megafire-fine-scale-drivers/{this_fire$IncidentName}/goes-nonfire-filtered.gpkg --acl public-read"))

just_fire <- 
  nonfire_filtered %>% 
  dplyr::filter(Mask %in% fire_flags)

sf::st_write(obj = just_fire, dsn = glue::glue("{this_fire_dir}/goes-active-fire-detections.gpkg"), delete_dsn = TRUE)

system2(command = "aws", args = glue::glue("s3 cp {this_fire_dir}/goes-active-fire-detections.gpkg s3://earthlab-mkoontz/megafire-fine-scale-drivers/{this_fire$IncidentName}/goes-active-fire-detections.gpkg --acl public-read"))