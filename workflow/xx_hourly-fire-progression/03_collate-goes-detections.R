# collate all the California GOES 16/17 active fire detections into a single dataframe

dependencies <- c("dplyr", "stringr", "readr", "glue", "USAboundaries", "terra", "sf", "purrr", "furrr", "future", "here", "data.table")

needs_install <- !sapply(dependencies, FUN = require, character.only = TRUE)

install.packages(dependencies[needs_install])

sapply(dependencies[needs_install], FUN = require, character.only = TRUE)

# First, download all the processed GOES files for the area of interest
# Note this takes a bit of time, as there are almost 600,000 files (though all very small)
# About 1.5 hours on Macbook Pro over (fast) wireless. Probably faster with more cores
# because of aws threading in the sync command and with a wired internet connection
system2(command = "aws", args = glue::glue("s3 sync s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes_california data/out/goes/california"))

goes_fire_detections_files <- list.files(path = "data/out/goes/california", full.names = TRUE)

goes_fire_detections_list <- sapply(goes_fire_detections_files, 
                                    simplify = FALSE, 
                                    USE.NAMES = TRUE, 
                                    FUN = data.table::fread, 
                                    colClasses = list(numeric = c(1:4, 6, 9:10), integer = c(5, 7:8)))

total_detections_in_each_img_list <- 
  purrr::map2(goes_fire_detections_list, names(goes_fire_detections_list), .f = function(dat, name) {
    return(data.frame(filename = name, total_detections_in_image = nrow(dat)))
  })

goes_fire_detections <- data.table::rbindlist(goes_fire_detections_list, idcol = "filename")
total_detections_in_each_img <- data.table::rbindlist(total_detections_in_each_img_list)

goes_fire_detections <- data.table::merge.data.table(x = goes_fire_detections, y = total_detections_in_each_img, by = "filename", all = TRUE)

goes_fire_detections <- 
  goes_fire_detections %>% 
  dplyr::mutate(processed_name = stringr::str_sub(string = filename, start = 26, end = -1),
                scan_center = stringr::str_sub(string = processed_name, start = 1, end = 14),
                filebasename = stringr::str_sub(string = processed_name, start = 16, end = -1),
                satellite = as.integer(stringr::str_sub(string = filebasename, start = 20, end = 21)),
                scan_center_full = lubridate::parse_date_time2(x = scan_center, orders = "%Y%m%d%H%M%S"),
                year = lubridate::year(scan_center_full),
                month = lubridate::month(scan_center_full),
                day = lubridate::day(scan_center_full),
                hour = lubridate::hour(scan_center_full),
                min = lubridate::minute(scan_center_full),
                sec = lubridate::second(scan_center_full))

data.table::fwrite(x = goes_fire_detections, file = "data/out/goes_california.csv")
