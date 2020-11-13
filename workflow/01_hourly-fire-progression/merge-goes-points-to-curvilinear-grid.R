# Link point data to GOES grid transformed to curvilinear 

# template <- 
#   terra::rast(this_goes_local_file)["DQF"] %>% 
#   stats::setNames("cellindex")
# 
# xy <- terra::as.data.frame(template, xy = TRUE)
# 
# template <- 
#   terra::setValues(x = template, values = terra::cells(template)) 
# 
# x <- y <- terra::rast(template)
# x <- terra::setValues(x = x, values = xy[, 1]) %>% setNames("x")
# y <- terra::setValues(x = x, values = xy[, 2]) %>% setNames("y")
# 
# template <-
#   terra::rast(list(x, y, template)) %>%
#   terra::project("EPSG:4326", method = "near")
# 
# lonlat <- terra::as.data.frame(template, xy = TRUE)
# lon <- lat <- terra::rast(template["cellindex"])
# 
# lon <- terra::setValues(x = lon, values = lonlat[, 1]) %>% setNames("lon")
# lat <- terra::setValues(x = lat, values = lonlat[, 2]) %>% setNames("lat")
# 
# template_4326 <-
#   terra::rast(list(lon, lat, template)) %>% 
#   terra::writeRaster(filename = "data/data_output/goes-template.tif", overwrite = TRUE)
# 
# rm("x", "y", "lon", "lat", "template", "template_4326")
# 
# template_sf <- 
#   raster::stack("data/data_output/goes-template.tif") %>% 
#   stars::st_as_stars() %>% 
#   sf::st_as_sf()
# 
# sf::st_write(obj = template_sf, dsn = "data/data_output/goes-template.gpkg")



# Create a template vector object with polygons representing each pixel
### Build a template curvilinear raster for the common CRS's
template <- 
  terra::rast(this_goes_local_file)["DQF"] %>% 
  stats::setNames("cellindex")

xy <- terra::as.data.frame(template, xy = TRUE)

template <- 
  terra::setValues(x = template, values = terra::cells(template)) 

x <- y <- terra::rast(template)
x <- terra::setValues(x = x, values = xy[, 1]) %>% setNames("x")
y <- terra::setValues(x = x, values = xy[, 2]) %>% setNames("y")

template <-
  terra::rast(list(x, y, template)) %>%
  terra::writeRaster(filename = "data/data_output/goes-template_native-crs.tif", overwrite = TRUE)

template_sf <- 
  raster::stack("data/data_output/goes-template_native-crs.tif") %>% 
  stars::st_as_stars() %>% 
  sf::st_as_sf() %>% 
  sf::st_transform(4326)

template_sf2 <- 
  raster::stack("data/data_output/goes-template_native-crs.tif") %>% 
  stars::st_as_stars() %>% 
  stars::st_transform_proj(crs = 4326) %>%
  sf::st_as_sf()



(Sys.time() - start)

little <- template_sf[1000000:1000020, ]
little2 <- template_sf2[1000000:1000020, ]

plot(little)
little_4326 <- sf::st_transform(little, crs = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs")
sf::st_dimension(little_4326)

# Just go straight to the transform for every image
goes <- 
  terra::rast(this_goes_local_file) %>% 
  terra::writeRaster(filename = "data/data_output/this-goes.tif", overwrite = TRUE)

%>% 
  setNames(c("Area", "Temp", "Mask", "Power", "DQF"))


xy <- terra::as.data.frame(r, xy = TRUE)

template <- 
  terra::setValues(x = template, values = terra::cells(template)) 

x <- y <- terra::rast(template)
x <- terra::setValues(x = x, values = xy[, 1]) %>% setNames("x")
y <- terra::setValues(x = x, values = xy[, 2]) %>% setNames("y")





dir.create("data/data_output/goes16", showWarnings = FALSE)
goes_path_tmp <- "data/data_output/goes16/temp.tif"
goes_path_geo_tmp <- "data/data_output/goes16/temp_geo.tif"

# this_goes <- ncdf4::nc_open(this_goes_local_file)
sds_names <- gdalUtils::get_subdatasets(this_goes_local_file) %>% stringr::str_split(pattern = ":", simplify = TRUE) %>% as.data.frame(stringsAsFactors = FALSE) %>% dplyr::pull(3)

(start <- Sys.time())
r <-
  terra::rast(this_goes_local_file)

dqf <- stars::read_stars(this_goes_local_file)

r_4326 <-
  r %>%
  terra::project("EPSG:4326", method = "near")

(start <- Sys.time())
r <-
  terra::rast(this_goes_local_file) %>%
  terra::writeRaster(filename = "data/data_output/goes16/temp.tif", overwrite = TRUE)

sds_names <- base::names(r)

# http://edc.occ-data.org/goes16/gdal/
# gdalUtils::gdal_translate(src_dataset = this_goes_local_file,
#                           dst_dataset = goes_path_tmp,
#                           ot = 'float32',
#                           unscale = TRUE,
#                           co = "COMPRESS=deflate", sds = TRUE)
# (difftime(Sys.time(), start, units = "secs"))
# 
# r <- terra::rast(list.files("data/data_output/goes16", full.names = TRUE, pattern = "temp"))
# terra::writeRaster(r, filename = "data/data_output/goes16/temp.tif", overwrite = TRUE)

(difftime(Sys.time(), start, units = "secs"))

# Transform to EPSG:4326
gdalUtils::gdalwarp(srcfile = goes_path_tmp,
                    dstfile = goes_path_geo_tmp,
                    t_srs = "EPSG:4326",
                    overwrite = TRUE)
(difftime(Sys.time(), start, units = "secs"))

r <- terra::rast("data/data_output/goes16/temp_geo.tif") %>% setNames(sds_names)

r_df <-
  r %>% 
  terra::as.data.frame(xy = TRUE) %>% 
  dplyr::mutate(cellindex = 1:nrow(.))

(difftime(Sys.time(), start, units = "secs"))

fires <- r_df %>% dplyr::filter(DQF == 0)
(difftime(Sys.time(), start, units = "secs"))





tictoc::toc()

# We can get the nodata values from the ncdf file
nodata_vals <- 
  sapply(sds_names, FUN = function(name) {
    ifelse(ncdf4::ncatt_get(this_goes, varid = name, attname = "_FillValue")[[1]], 
           yes = ncdf4::ncatt_get(this_goes, varid = name, attname = "_FillValue")[[2]],
           no = NA)
  })

gdalUtils::gdalwarp(srcfile = goes_path_tmp, 
                    dstfile = goes_path_geo_tmp,
                    s_srs = gdalUtils::gdalsrsinfo(srs_def = goes_path_tmp, o = "proj4", verbose = TRUE)[2],
                    t_srs = "EPSG:4326", 
                    overwrite = TRUE,
                    verbose = TRUE)


top_ten <- test[1:10, ]

fires <-
  test %>% 
  filter(temp_geo.tif == 0)

plot(top_ten$geometry)
plot(fires[1:5, ])
fires

sf::st_write(obj = fires, dsn = "data/data_output/fires.gpkg")
fires <- sf::st_read("data/data_output/fires.gpkg")
test_df[!is.na(test_df$temp_geo), ]

gdalUtils::gdalwarp(srcfile = goes_path_tmp, 
                    dstfile = goes_path_tmp, 
                    t_srs = crs(empty_grid), 
                    tr = c(250, 250), 
                    overwrite = TRUE,
                    r = "bilinear")

unlink(paste0(hazard_path_tmp, ".aux.xml"))

hazard <- gdalUtils::align_rasters(unaligned = hazard_path_tmp, 
                                   reference = empty_grid@file@name, 
                                   dstfile = hazard_path_out, 
                                   overwrite = TRUE,
                                   output_Raster = TRUE)

unlink(hazard_path_tmp)









ncvar_get(this_goes, varid = "Power")

ncatt_get(this_goes, varid = "Power")
ncatt_get(this_goes, varid = "DQF")

test <- oceanmap::nc2raster(this_goes, varname = "Power")
test <- oceanmap::nc2raster(this_goes, varname = "ABI L2+ Fire-Hot Spot Characterization: Fire Temperature")

ncdf4::nc_open(nfiles[1])

sat_h <- ifelse(ncdf4::ncatt_get(nc = this_goes, varid = "goes_imager_projection",  attname = "perspective_point_height")[[1]], 
                yes = ncdf4::ncatt_get(nc = this_goes, varid = "goes_imager_projection",  attname = "perspective_point_height")[[2]],
                no = NA)

# Satellite longitude
sat_lon <- ifelse(ncdf4::ncatt_get(nc = this_goes, varid = "goes_imager_projection",  attname = "longitude_of_projection_origin")[[1]], 
                  yes = ncdf4::ncatt_get(nc = this_goes, varid = "goes_imager_projection",  attname = "longitude_of_projection_origin")[[2]],
                  no = NA)

# Satellite sweep
sat_sweep <- ifelse(ncdf4::ncatt_get(nc = this_goes, varid = "goes_imager_projection",  attname = "sweep_angle_axis")[[1]], 
                    yes = ncdf4::ncatt_get(nc = this_goes, varid = "goes_imager_projection",  attname = "sweep_angle_axis")[[2]],
                    no = NA)

remotes::install_github("rspatial/terra")
library(terra)

path <- system.file("test_files", package="oceanmap")
nfiles <- Sys.glob(paste0(path,'/*.nc'))[1] # load sample-'.nc'-files

nc2raster(nfiles[1],"Conc",layer=1) # RasterLayer


test <- xarray$open_dataset(this_goes_local_file)
dat = test$metpy$parse_cf('DQF')
geos = dat$metpy$cartopy_crs

ax.imshow(RGB, origin='upper',
          extent=(x.min(), x.max(), y.min(), y.max()),
          transform=geos)

FILE = ('http://ramadda-jetstream.unidata.ucar.edu/repository/opendap/4ef52e10-a7da-4405-bff4-e48f68bb6ba2/entry.das#fillmismatch')
C = xarray$open_dataset(FILE)


(attributes(r)$dimensions$x$refsys$wkt)
(attributes(r)$dimensions$y$refsys$wkt)

ncatt_get(nc = this_goes, varid = "DQF")
r <- 
  raster::raster(this_goes_local_file, varname = "DQF") %>% 
  setNames("DQF") %>% 
  as.data.frame(xy = TRUE) 
%>% 
  dplyr::filter(!is.na(Temp) & Temp != -1) %>% 
  
  # st_crs(r) <- 
  # 
  attributes(r)$dimensions$x$refsys

rr <- st_as_sf(r)
rrr <- st_transform(rr, crs = 4326)


st_crs(r)

data(meuse, package = "sp")
meuse <- 
  st_as_sf(meuse, coords = c("x", "y"), crs = glue::glue("+proj=goes h={sat_h} +lon_0={sat_lon} +sweep={sat_sweep} +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"))
st_crs(meuse)
st_crs(meuse) <- glue::glue("+proj=goes h={sat_h} +lon_0={sat_lon} +sweep={sat_sweep} +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs")
?st_crs
# Create a pyproj geostationary map object
p <- pyproj$Proj(proj = 'geos', 
                 h = sat_h, 
                 lon_0 = sat_lon, 
                 sweep = sat_sweep)



this_goes$Temp$data
this_goes["Temp"]
this_goes$sel(indexers = dict(Temp = "nan"))

this_goes$y

this_goes$sel()
pyproj$transform(p)

# Perform cartographic transformation. That is, convert image projection coordinates (x and y)
# to latitude and longitude values.
XX, YY = np.meshgrid(x, y)
lons, lats = p(XX, YY, inverse=True)
