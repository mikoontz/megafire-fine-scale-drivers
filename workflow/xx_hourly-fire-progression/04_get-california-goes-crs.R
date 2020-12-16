# get the coordinate reference system for all of the GOES fire images

dependencies <- c("dplyr", "stringr", "readr", "glue", "USAboundaries", "terra", "sf", "purrr", "furrr", "future", "here")

needs_install <- !sapply(dependencies, FUN = require, character.only = TRUE)

install.packages(dependencies[needs_install])

sapply(dependencies[needs_install], FUN = require, character.only = TRUE)

if(!require(USAboundariesData)) {
  install.packages("USAboundariesData", repos = "http://packages.ropensci.org", type = "source")
}

require(USAboundariesData)

# Option 1 is to download the whole GOES record to local machine first
# system2(command = "aws", args = glue::glue("s3 sync s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes_california data/out/goes/california"))
