# Subset ongoing fires to big megafires of interest

library(tidyverse)
library(sf)
library(lubridate)
library(glue)

fire_files <- 
  data.frame(perim_dirs = list.files(path = "data/data_raw/wildfire-perimeters/")) %>% 
  dplyr::mutate(date = lubridate::ymd(substr(perim_dirs, start = 1, stop = 10)))

latest_fire_file <- 
  list.files(path = "data/data_raw/wildfire-perimeters", pattern = as.character(max(fire_files$date)), full.names = TRUE)

fires <- sf::st_read(glue::glue("{latest_fire_file}/Public_NIFS_Perimeters.shp"))

target_complexes <- c("August Complex", "SCU Lightning Complex", "LNU Lightning Complex", "North Complex", "SQF", "CZU Lightning Complex")
target_fires <- c("Creek")

megafires2020 <-
  fires %>% 
  dplyr::filter(ComplexNam %in% target_complexes | IncidentNa %in% target_fires) %>% 
  dplyr::arrange(dplyr::desc(GISAcres))

plot(megafires2020$geometry)

sf::st_write(obj = megafires2020, dsn = "data/data_output/2020-09-24_2020-califorina-megafires.gpkg")
