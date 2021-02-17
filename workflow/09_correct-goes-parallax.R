# correct for the parallax

library(sf)
library(dplyr)
library(lubridate)
library(ggplot2)
library(magrittr)
library(tidyr)
library(slider)
library(pbapply)

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

# work on each group of rounded hour detections
offsets <-
  afd %>% 
  group_by(acq_datetime_hour, satellite) %>% 
  summarize() %>% 
  st_centroid() %>% 
  mutate(x = sapply(X = geom, FUN = `[`, 1),
         y = sapply(X = geom, FUN = `[`, 2)) %>% 
  mutate(lon = sapply(X = st_transform(geom, 4326), FUN = `[`, 1),
         lat = sapply(X = st_transform(geom, 4326), FUN = `[`, 2)) %>%  
  st_drop_geometry() %>% 
  ungroup() %>% 
  pivot_wider(id_cols = c(acq_datetime_hour), names_from = satellite, values_from = c(x, y, lon, lat)) %>% 
  mutate(lon_diff_goes16 = abs(-75.2 - lon_goes16),
         lon_diff_goes17 = abs(-137.2 - lon_goes17),
         lon_diff_prop_goes16 = lon_diff_goes16 / (lon_diff_goes16 + lon_diff_goes17),
         lon_diff_prop_goes17 = lon_diff_goes17 / (lon_diff_goes16 + lon_diff_goes17),
         goes16 = (x_goes17 - x_goes16) * lon_diff_prop_goes16,
         goes17 = (x_goes16 - x_goes17) * lon_diff_prop_goes17,
  ) %>% 
  dplyr::select(acq_datetime_hour, goes16, goes17) %>% 
  tidyr::pivot_longer(cols = c(goes16, goes17), names_to = "satellite", values_to = "offset")

offsets

l <- 
  afd %>% 
  group_by(satellite) %>% 
  group_split()

new16 <- l[[1]] %>% 
  mutate(geom = geom + c(offsets$offset[offsets$satellite == "goes16"], 0))
new17 <- l[[2]] %>% 
  mutate(geom = geom + c(offsets$offset[offsets$satellite == "goes17"], 0))

new <- rbind(new16, new17)



%>% 
  purrr::map()
  mutate(geom = geom + cbind(offset, 0))
  dplyr::select(acq_datetime_hour, satellite, offset)

ggplot() +
  geom_sf(data = st_geometry(this_fire)) +
  geom_sf(data = goes_list[[3]]) +
  facet_wrap(facets = "satellite")

afd + c()

ggplot() +
  geom_sf(data = afd, mapping = aes(fill = satellite), alpha = 0.2) +
  geom_sf(data = offsets)

ggplot() +
  geom_sf(new, mapping = aes(fill = satellite), alpha = 0.2)
