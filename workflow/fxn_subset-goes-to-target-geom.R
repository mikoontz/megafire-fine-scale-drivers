#### subset GOES images to fire detections in specific fire perimeter, write to disk as .gpkg
subset_goes_to_target_geom <- function(this_batch, target_geom, target_crs, target_dir, ...) {
  
  out <- vector(mode = "list", length = nrow(this_batch))
  
  for (k in 1:nrow(this_batch)) {
    this <- stars::read_stars(here::here(this_batch$local_path_full[k]), quiet = TRUE)
    
    this_crs <- sf::st_crs(this)$wkt
    
    buffered_geom_goes_transform <- 
      target_geom %>% 
      sf::st_transform(crs = sf::st_crs(this)) %>% 
      sf::st_bbox() %>% # bbox will be a much simpler geometry to buffer, since we're just using this as a first pass
      sf::st_as_sfc() %>% 
      sf::st_buffer(dist = 5000) # add an extra 3000 meters to the bounding box of the fire perimeter to capture neighboring GOES pixels 
    
    this_within_target_geom <-
      this %>% 
      dplyr::mutate(cell = as.integer(1:prod(dim(.)))) %>%  # explicitly add the 'cell number' by multiplying nrow by ncol
      sf::st_crop(buffered_geom_goes_transform) %>% # crop to less than CONUS, but larger than fire
      stars::st_transform_proj(crs = target_crs) %>% 
      sf::st_as_sf() %>% 
      dplyr::mutate(scan_center = this_batch$scan_center[k],
                    satellite = this_batch$target_goes[k]) %>% 
      dplyr::select(scan_center, satellite, everything())
    
    sf::st_write(obj = this_within_target_geom, dsn = glue::glue("{target_dir}/{this_batch$processed_filename[k]}"), 
                 delete_dsn = TRUE,
                 quiet = TRUE)
    
    out[[k]] <- dplyr::tibble(local_path_full = this_batch$local_path_full[k], processed_filename = this_batch$processed_filename[k], crs = this_crs)
  }
  
  crs_df <- data.table::rbindlist(out)
  
  return(crs_df)
}
