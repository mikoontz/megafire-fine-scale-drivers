# get the coordinate reference system for all of the GOES fire images

dependencies <- c("dplyr", "stringr", "readr", "glue", "USAboundaries", "terra", "sf", "purrr", "furrr", "future", "here")

needs_install <- !sapply(dependencies, FUN = require, character.only = TRUE)

install.packages(dependencies[needs_install])

sapply(dependencies[needs_install], FUN = require, character.only = TRUE)

if(!require(USAboundariesData)) {
  install.packages("USAboundariesData", repos = "http://packages.ropensci.org", type = "source")
}

require(USAboundariesData)

get_goes_crs <- function(local_path_full, processed_filename, ...) {
  this <- stars::read_stars(here::here(local_path_full))
  this_crs <- sf::st_crs(this)$wkt
  
  crs_df <- data.frame(local_path_full, processed_filename, crs = this_crs)
  # ca_goes_geom <- 
  #   sf::st_transform(california_geom, crs = sf::st_crs(this))
  
  # readr::write_csv(x = this_ca, file = glue::glue("{here::here()}/data/out/goes/california/{processed_filename}"))
  # 
  # rm(this)
  # rm(this_ca)
  # rm(ca_goes_geom)
  # gc()
  
  return(crs_df)
}

n_workers <- 7

goes_meta_with_crs_batches <-
  goes_meta %>% 
  # dplyr::filter(!(processed_filename %in% processed_goes$filename)) %>%
  dplyr::mutate(group = sample(x = 1:n_workers, size = nrow(.), replace = TRUE)) %>% 
  dplyr::group_by(group) %>% 
  dplyr::group_split()

(start <- Sys.time())
future::plan(strategy = "multiprocess", workers = n_workers)

goes_with_crs <-
  furrr::future_map_dfr(goes_meta_with_crs_batches, .f = function(x) {
  purrr::pmap_dfr(.l = x, .f = get_goes_crs)
})

future::plan(strategy = "sequential")

(difftime(Sys.time(), start))