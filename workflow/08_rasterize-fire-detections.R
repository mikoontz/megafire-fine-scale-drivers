library(sf)
library(dplyr)
library(lubridate)
library(fasterize)
library(terra)

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

modis_buff <- sf::st_buffer(x = modis, dist = 1000 / 2)[, c("SATELLITE", "acq_datetime")]
viirs_buff <- sf::st_buffer(x = viirs, dist = 375 / 2)[, c("SATELLITE", "acq_datetime")]

afd <- rbind(modis_buff, viirs_buff)

afd$hours_elapsed <- difftime(time1 = afd$acq_datetime, time2 = min(afd$acq_datetime), units = "hours")

r <- raster::raster(this_fire, res = c(500, 500))

rafd <- fasterize::fasterize(sf = afd, raster = r, field = "hours_elapsed", fun = "min")

plot(rafd)
plot(st_geometry(this_fire), add = TRUE)

fw <- raster::focalWeight(x = rafd, d = 3, type = "Gauss")

frafd <- raster::focal(x = rafd, w = fw)
plot(frafd)

fire_speed <- 1 / raster::terrain(x = frafd, opt = "slope", unit = "tangent")
fire_direction <- cos(raster::terrain(x = frafd, opt = "aspect", unit = "radians") + pi / 2)

plot(fire_speed)
plot(fire_direction)
raster::terrain
?raster::terrain
