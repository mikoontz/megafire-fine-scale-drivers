library(sf)
library(dplyr)
library(lubridate)

this_fire <- 
  sf::st_read("data/out/megafire-events.gpkg") %>% 
  slice(1) %>% 
  sf::st_transform(3310)

this_fire_dir <- glue::glue("{here::here()}/data/out/fires/{this_fire$IncidentName}")

if(!file.exists(glue::glue("{this_fire_dir}/modis-active-fire-detections.gpkg"))) {
  system2(command = "aws", args = glue::glue("s3 cp s3://earthlab-mkoontz/megafire-fine-scale-drivers/{this_fire$IncidentName}/modis-active-fire-detections.gpkg {this_fire_dir}/modis-active-fire-detections.gpkg"))
}

if(!file.exists(glue::glue("{this_fire_dir}/viirs-active-fire-detections.gpkg"))) {
  system2(command = "aws", args = glue::glue("s3 cp s3://earthlab-mkoontz/megafire-fine-scale-drivers/{this_fire$IncidentName}/viirs-active-fire-detections.gpkg {this_fire_dir}/viirs-active-fire-detections.gpkg"))
}

modis <- 
  st_read(dsn = glue::glue("{this_fire_dir}/modis-active-fire-detections.gpkg")) %>% 
  mutate(acq_datetime = ymd_hm(glue("{ACQ_DATE} {ACQ_TIME}")))

viirs <- 
  st_read(dsn = glue::glue("{this_fire_dir}/viirs-active-fire-detections.gpkg")) %>% 
  mutate(acq_datetime = ymd_hm(glue("{ACQ_DATE} {ACQ_TIME}")))

goes <-
  st_read(dsn = glue::glue("{this_fire_dir}/goes-active-fire-detections.gpkg")) %>% 
  mutate(acq_datetime = ymd_hms(scan_center))

modis_list <- 
  modis %>% 
  group_by(acq_datetime) %>% 
  group_split()

viirs_list <-
  viirs %>% 
  group_by(acq_datetime) %>% []
  group_split()

viirs %>% group_by(acq_datetime) %>% tally()
this_modis <- modis_list[[11]]
this_viirs <- viirs_list[[8]]

this_modis_acq_datetime <- unique(this_modis$acq_datetime)
this_viirs_acq_datetime <- unique(this_viirs$acq_datetime)

this_goes16 <-
  goes %>% 
  mutate(modis_diff = abs(as.numeric(difftime(time1 = acq_datetime, time2 = this_modis_acq_datetime, units = "mins")))) %>% 
  mutate(viirs_diff = abs(as.numeric(difftime(time1 = acq_datetime, time2 = this_viirs_acq_datetime, units = "mins")))) %>% 
  filter(satellite == "goes16") %>% 
  # filter(modis_diff == min(modis_diff))
  filter(viirs_diff == min(viirs_diff))

this_goes17 <-
  goes %>% 
  mutate(modis_diff = abs(as.numeric(difftime(time1 = acq_datetime, time2 = this_modis_acq_datetime, units = "mins")))) %>% 
  mutate(viirs_diff = abs(as.numeric(difftime(time1 = acq_datetime, time2 = this_viirs_acq_datetime, units = "mins")))) %>% 
  filter(satellite == "goes17") %>% 
  # filter(modis_diff == min(modis_diff))
  filter(viirs_diff == min(viirs_diff))

# this_viirs <-
#   viirs %>% 
#   mutate(modis_diff = abs(as.numeric(difftime(time1 = acq_datetime, time2 = this_modis_acq_datetime, units = "mins")))) %>% 
  # filter(modis_diff == min(modis_diff))
  
ggplot() +
  geom_sf(data = this_goes16) +
  geom_sf(data = this_goes17) +
  geom_sf(data = this_viirs)
