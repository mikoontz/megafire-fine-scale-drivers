# Define what the California megafires we want to look at are, 
# and get their attributes and perimeters

library(dplyr)
library(sf)
library(readr)

if(!dir.exists("data/raw/fire19_1.gdb/")){
  # CalFire FRAP database of California Fire perimeters for fires up to 2019
  download.file(url = "https://frap.fire.ca.gov/media/10969/fire19_1.zip", 
                destfile = "data/raw/fire19_1.zip", 
                method = "curl")
  
  unzip(zipfile = "data/raw/fire19_1.zip", exdir = "data/raw/")
}

frap_perims <- sf::st_read("data/raw/fire19_1.gdb/", layer = "firep19_1")

# Camp, Carr, Woolsey, Mendocino Complex
megafire_inc_num <- c("00016737", "00007808", "00338981", "00008646")

megafires <-
  frap_perims %>% 
  dplyr::filter(INC_NUM %in% megafire_inc_num)

sf::st_write(obj = megafires, dsn = "data/out/megafire-events.gpkg")
