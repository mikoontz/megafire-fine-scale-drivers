# download the whole GOES 16/17 record to local disk
# how long does this take? how much space does it take up?

dir.create("data/raw/goes16", recursive = TRUE, showWarnings = FALSE)
dir.create("data/raw/goes17", recursive = TRUE, showWarnings = FALSE)

(start <- Sys.time())
system2(command = "aws", args = glue::glue("s3 sync s3://noaa-goes16/ABI-L2-FDCC/ data/raw/goes16 --no-sign-request"), stdout = FALSE)
(difftime(Sys.time(), start, units = "mins"))

system2(command = "aws", args = glue::glue("s3 sync s3://noaa-goes17/ABI-L2-FDCC/ data/raw/goes17 --no-sign-request"), stdout = FALSE)
(difftime(Sys.time(), start, units = "mins"))
