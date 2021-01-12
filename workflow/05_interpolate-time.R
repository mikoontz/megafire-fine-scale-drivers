# earliest fire detection in each cell

#### Dependencies ####
dependencies <- c("data.table",
                  "dplyr", 
                  "furrr", "future",
                  "ggplot2",
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

creek <- 
  sf::st_read("data/out/megafire-events.gpkg") %>% 
  slice(1) %>% 
  sf::st_transform(3310)

afd <- 
  sf::st_read(glue::glue("{this_fire_dir}/goes-active-fire-detections.gpkg")) %>% 
  dplyr::mutate(scan_center_full = lubridate::ymd_hms(scan_center)) %>% 
  dplyr::mutate(rounded_datetime = round(scan_center_full, "mins"))

afd_crop <-
  afd %>% sf::st_intersection(creek)

earliest_afd <-
  afd %>% 
  dplyr::filter(scan_center_full < lubridate::ymd("2020-10-14")) %>% 
  dplyr::group_by(satellite, cell) %>% 
  dplyr::filter(scan_center_full == min(scan_center_full))

earliest_afd %>% arrange(scan_center_full)

ggplot(earliest_afd, aes(fill = scan_center_full)) +
  geom_sf() +
  facet_grid(cols = vars(satellite)) +
  geom_sf(data = creek, inherit.aes = FALSE, alpha = 0.4, fill = "pink")
