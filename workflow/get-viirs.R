library(tidyverse)
library(sf)
library(mapview)
library(lubridate)

system2(command = "wget",
        args = '-e robots=off -m -np -R .html,.tmp -nH --cut-dirs=4 "https://nrt4.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/viirs/USA_contiguous_and_Hawaii" --header "Authorization: Bearer 4771A3A2-F2B8-11EA-A9AE-F719DE621C4C" -P data/data_raw')

wpb <- sf::st_read("data/data_raw/wpb-site-bounds.kml")
wpb_buffer <- sf::st_buffer(wpb, dist = 0.01)

viirs <- 
  list.files("data/data_raw/FIRMS/viirs/USA_contiguous_and_Hawaii/", full.names = TRUE) %>% 
  lapply(FUN = readr::read_csv) %>% 
  dplyr::bind_rows() %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326)

viirs_wpb <- sf::st_intersection(viirs, wpb)

mapview::mapview(viirs_wpb) + mapview::mapview(wpb, color = "red")

recent_viirs <-
  viirs %>% 
  dplyr::filter(acq_date > (max(acq_date) - days(3)))

mapview::mapview(recent_viirs) + mapview::mapview(wpb)

