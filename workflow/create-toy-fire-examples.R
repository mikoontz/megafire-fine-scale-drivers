### Create a toy example of daily fire spread using the Rim fire

library(tidyverse)
library(sf)
library(lubridate)
library(USAboundaries)
library(here)

fired_daily <- sf::st_read(here::here("data/data_raw/daily_polys_w_stats_landcover_cus.gpkg"))
ca <- 
  USAboundaries::us_boundaries(states = "California") %>% 
  st_transform(sf::st_crs(fired_daily))

fired_ca <-
  fired_daily %>% 
  sf::st_intersection(ca)

king_id <- 
  fired_ca %>% 
  dplyr::mutate(ig_year = year(ignition_date),
                ig_month = month(ignition_date),
                ig_day = day(ignition_date)) %>% 
  dplyr::filter(ig_year == 2014 & ig_month == 9) %>% 
  dplyr::arrange(dplyr::desc(total_area_km2)) %>% 
  dplyr::slice(1) %>% 
  dplyr::pull(id)
  
king <-
  fired_ca %>% 
  dplyr::filter(id == king_id)
  
rim <-
  fired_ca %>% 
  filter(id == 57206)

if(!dir.exists(here::here("data/data_output"))) {
  dir.create(here::here("data/data_output"))
}

sf::st_write(obj = king, here::here("data/data_output/king-fire_ca_2014_daily-spread.gpkg"))

sf::st_write(obj = rim, here::here("data/data_output/rim-fire_ca_2013_daily-spread.gpkg"))
