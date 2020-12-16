# Define what the California megafires we want to look at are, 
# and get their attributes and perimeters

library(dplyr)
library(sf)
library(readr)

dir.create("data/raw", recursive = TRUE, showWarnings = FALSE)
dir.create("data/out", recursive = TRUE, showWarnings = FALSE)

# Get CalFire FRAP perimeters
if(!dir.exists("data/raw/fire19_1.gdb/")){
  # CalFire FRAP database of California Fire perimeters for fires up to 2019
  download.file(url = "https://frap.fire.ca.gov/media/10969/fire19_1.zip", 
                destfile = "data/raw/fire19_1.zip", 
                method = "curl")
  
  unzip(zipfile = "data/raw/fire19_1.zip", exdir = "data/raw")
  unlink("data/raw/fire19_1.zip")
}

# Get FIRED perimeters so we can get some better datetimes on their containment
if(!dir.exists("data/raw/fired_events_conus_nov2001-jan2019.gpkg")){
 download.file(url = "https://scholar.colorado.edu/downloads/hx11xg09n", 
                destfile = "data/raw/fired_events_conus_nov2001-jan2019.gpkg", 
                method = "curl")
}

frap_perims <- sf::st_read("data/raw/fire19_1.gdb", layer = "firep19_1")
# nifc_perims_orig <- sf::st_read("data/raw/4b9ff16cabb84a79ad24ddc4e12e1938.gdb")
ca_perims_orig <- sf::st_read("data/raw/25f5e1ae414f42878eb613f44cd8c54d.gdb")
# fired_perims <- sf::st_read("data/raw/fired_events_conus_nov2001-jan2019.gpkg")

# nifc_perims <-
#   nifc_perims_orig %>% 
#   dplyr::mutate(year = lubridate::year(GDB_FROM_DATE)) %>% 
#   dplyr::arrange(desc(year))

ca_perims <-
  ca_perims_orig %>%
  dplyr::mutate(year = lubridate::year(PolygonDateTime)) %>%
  dplyr::filter(year == 2020) %>%
  dplyr::arrange(desc(SHAPE_Area))

megafires_nifc <-
  ca_perims %>%
  dplyr::filter(IncidentName == "Creek") %>%
  dplyr::mutate(IncidentName = tolower(IncidentName),
                alarm_date = lubridate::ymd("2020-09-04"),
                cont_date = lubridate::today())

sf::st_write(obj = megafires_nifc, dsn = "data/out/megafire-events.gpkg", delete_dsn = TRUE)

# frap_perims %>% filter(YEAR_ == 2018) %>% arrange(desc(GIS_ACRES))
# # Camp, Carr, Woolsey, Mendocino Complex
# megafire_inc_num <- c("00016737", "00007808", "00338981", "00008646")
# 
# megafires_frap <-
#   frap_perims %>% 
#   dplyr::filter(INC_NUM %in% megafire_inc_num) %>% 
#   dplyr::rename_all(tolower) %>% 
#   dplyr::rename(year = year_) %>% 
#   dplyr::mutate(fire_name = stringr::str_trim(fire_name)) %>% # strip white space from fire names (e.g., CARR fire)
#   dplyr::mutate(fire_name = tolower(fire_name))
# 
# sf::st_write(obj = megafires_frap, dsn = "data/out/megafire-events.gpkg", delete_dsn = TRUE)
