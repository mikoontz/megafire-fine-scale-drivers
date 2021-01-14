library(sf)
library(dplyr)

this_fire <- 
  sf::st_read("data/out/megafire-events.gpkg") %>% 
  slice(1) %>% 
  sf::st_transform(3310)

this_fire_bbox_buffered <- 
  this_fire %>%
  st_bbox() %>%
  st_as_sfc() %>%
  st_buffer(1000) %>%
  st_bbox()

this_fire_dir <- glue::glue("{here::here()}/data/out/fires/{this_fire$IncidentName}")

modis <- sf::st_read("data/raw/DL_FIRE_M6_11558/fire_archive_M6_11558.shp")
modis_nrt <- sf::st_read("data/raw/DL_FIRE_M6_11558/fire_nrt_M6_11558.shp")

noaa20 <- sf::st_read("data/raw/DL_FIRE_J1V-C2_11559/fire_nrt_J1V-C2_11559.shp")
suomi <- sf::st_read("data/raw/DL_FIRE_V1_11560/fire_archive_V1_11560.shp")
suomi_nrt <- sf::st_read("data/raw/DL_FIRE_V1_11560/fire_nrt_V1_11560.shp")

modis <- 
  modis %>% 
  filter(TYPE == 0) %>% 
  dplyr::select(-TYPE) %>% 
  bind_rows(modis_nrt)

suomi <-  
  suomi %>% 
  filter(TYPE == 0) %>% 
  dplyr::select(-TYPE) %>% 
  bind_rows(suomi_nrt) %>% 
  mutate(SATELLITE = "suomi")

noaa20 <-
  noaa20 %>% 
  mutate(SATELLITE = "noaa20")

viirs <- bind_rows(suomi, noaa20)

modis_this_fire <-
  modis %>% 
  filter(ACQ_DATE >= this_fire$alarm_date & ACQ_DATE <= this_fire$cont_date) %>%
  st_transform(3310) %>%
  mutate(local_x = st_coordinates(.)[, 1],
         local_y = st_coordinates(.)[, 2])

viirs_this_fire <-
  viirs %>% 
  filter(ACQ_DATE >= this_fire$alarm_date & ACQ_DATE <= this_fire$cont_date) %>%
  st_transform(3310) %>%
  mutate(local_x = st_coordinates(.)[, 1],
         local_y = st_coordinates(.)[, 2])


modis_this_fire <-
  modis_this_fire %>%
  filter(local_x >= this_fire_bbox_buffered$xmin & 
           local_x <= this_fire_bbox_buffered$xmax &
           local_y >= this_fire_bbox_buffered$ymin &
           local_y <= this_fire_bbox_buffered$ymax)


viirs_this_fire <-
  viirs_this_fire %>%
  filter(local_x >= this_fire_bbox_buffered$xmin & 
           local_x <= this_fire_bbox_buffered$xmax &
           local_y >= this_fire_bbox_buffered$ymin &
           local_y <= this_fire_bbox_buffered$ymax)

st_write(obj = modis_this_fire, dsn = glue::glue("{this_fire_dir}/modis-active-fire-detections.gpkg"))
st_write(obj = viirs_this_fire, dsn = glue::glue("{this_fire_dir}/viirs-active-fire-detections.gpkg"))