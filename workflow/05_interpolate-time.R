# earliest fire detection in each cell

#### Dependencies ####
dependencies <- c("atakrig",
                  "data.table",
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
                  "sp",
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

this_fire <- 
  sf::st_read("data/out/megafire-events.gpkg") %>% 
  slice(1) %>% 
  sf::st_transform(3310)

this_fire_dir <- glue::glue("{here::here()}/data/out/fires/{this_fire$IncidentName}")

system2(command = "aws", args = glue::glue("s3 cp s3://earthlab-mkoontz/megafire-fine-scale-drivers/{this_fire$IncidentName}/goes-active-fire-detections.gpkg {this_fire_dir}/goes-active-fire-detections.gpkg"))

afd <- 
  sf::st_read(glue::glue("{this_fire_dir}/goes-active-fire-detections.gpkg")) %>% 
  dplyr::mutate(scan_center_full = lubridate::ymd_hms(scan_center)) %>% 
  dplyr::mutate(rounded_datetime = round(scan_center_full, "mins"))

earliest_afd <-
  afd %>% 
  dplyr::filter(scan_center_full < lubridate::ymd("2020-10-08")) %>% 
  # dplyr::filter(scan_center_full < lubridate::ymd("2020-09-08") & scan_center_full > lubridate::ymd("2020-09-07")) %>% 
  dplyr::group_by(satellite, cell) %>% 
  dplyr::filter(scan_center_full == min(scan_center_full))%>%
  dplyr::ungroup() %>% 
  dplyr::mutate(lon = sf::st_coordinates(sf::st_centroid(.))[, 1],
                lat = sf::st_coordinates(sf::st_centroid(.))[, 2]) %>% 
  dplyr::filter(lon < 80000)

ggplot(earliest_afd, aes(fill = scan_center_full)) +
  geom_sf() +
  facet_grid(cols = vars(satellite)) +
  geom_sf(data = this_fire, inherit.aes = FALSE, alpha = 0.4, fill = "pink") +
  labs(fill = "Earliest detection date")

ggsave(filename = "figs/creek-fire_earliest-goes_parallax.png")


sqrt(st_area(earliest_afd))








goes17 <- 
  earliest_afd %>% 
  dplyr::filter(satellite == "goes17")

dp <- 
  goes17 %>% 
  dplyr::mutate(time_elapsed = as.numeric(difftime(time1 = scan_center_full, time2 = min(scan_center_full), units = "mins"))) %>% 
  as("Spatial") %>% 
  atakrig::discretizePolygon(cellsize = 1000, id = "cell", value = "time_elapsed")

pointsv <- atakrig::deconvPointVgm(x = dp, model = "Exp", ngroup = 12, rd = 0.75, fig = TRUE)

pred <- atakrig::ataKriging(dp, unknown = dp, pointsv$pointVariogram)
pred <-
  pred %>% 
  sf::st_as_sf(coords = c("centx", "centy"), crs = 3310)

plot(pred[, "pred"])
