library(tidyverse)
library(sf)

creek <- 
  sf::st_read("data/data_raw/wildfire-perimeters/2020-09-16_Wildfire_Perimeters-shp/Public_NIFS_Perimeters.shp") %>% 
  dplyr::filter(IncidentNa == "Creek")

dir.create("data/data_output/creek-fire/", showWarnings = FALSE)
sf::st_write(obj = creek, dsn = "data/data_output/creek-fire/2020-09-16_creek-fire-perim.kml")
plot(st_geometry(creek))

wpb <- lapply(X = list.files("~/dev/moore-megafires/data/data_raw/site-bounds/", full.names = TRUE), FUN = st_read) %>% do.call("rbind", .) %>% dplyr::mutate(site = str_sub(list.files("~/dev/moore-megafires/data/data_raw/site-bounds/"), start = 1, end = -21)) %>% dplyr::filter(site != "stan_3k_2") %>% dplyr::mutate(site = ifelse(str_detect(site, "stan_3k_2"), yes = "stan_3k_2", no = site))

sf::st_write(obj = wpb, dsn = "~/dev/presentations/2020-09-14_CIRES-science-at-home_fire-beetles-drones/wpb-flight-footprints.kml")
