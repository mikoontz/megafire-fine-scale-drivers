# correct for the parallax

library(sf)
library(dplyr)
library(lubridate)
library(ggplot2)
library(magrittr)
library(tidyr)

this_fire <- 
  sf::st_read("data/out/megafire-events.gpkg") %>% 
  slice(1) %>% 
  sf::st_transform(3310)

this_fire_dir <- glue::glue("{here::here()}/data/out/fires/{this_fire$IncidentName}")

goes <-
  st_read(dsn = glue::glue("{this_fire_dir}/goes-active-fire-detections.gpkg")) %>% 
  mutate(acq_datetime = ymd_hms(scan_center)) %>% 
  mutate(acq_datetime_minute = round(acq_datetime, units = "mins"),
         acq_datetime_hour = round(acq_datetime, units = "hour"))

goes_list <-
  goes %>% 
  group_by(acq_datetime_hour) %>% 
  filter(length(unique(satellite)) == 2) %>% 
  group_split()

afd <- goes_list[[3]]

offsets <-
  afd %>% 
  group_by(acq_datetime_hour, satellite) %>% 
  summarize() %>% 
  st_centroid() %>% 
  mutate(x = sapply(X = geom, FUN = `[`, 1),
         y = sapply(X = geom, FUN = `[`, 2)) %>% 
  st_drop_geometry() %>% 
  ungroup() %>% 
  pivot_wider(id_cols = c(acq_datetime_hour), names_from = satellite, values_from = c(x, y)) %>% 
  mutate(x_offset_goes16 = x_goes17 - x_goes16,
         x_offset_goes17 = -x_offset_goes16,
         y_offset_goes16 = y_goes17 - y_goes16,
         y_offset_goes17 = -y_offset_goes16) %>% 
  dplyr::select(acq_datetime_hour, contains("offset"))

offsets

ggplot() +
  geom_sf(data = st_geometry(this_fire)) +
  geom_sf(data = goes_list[[3]]) +
  facet_wrap(facets = "satellite")

afd + c()
?sf
ggplot() +
  geom_sf(data = goes_list[[3]], mapping = aes(fill = satellite), alpha = 0.2)
