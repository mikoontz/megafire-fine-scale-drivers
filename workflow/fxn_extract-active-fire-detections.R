# keep all cells that aren't determined to be "not fire" (including fire, clouds, etc.)

extract_active_fire_detections <- function(this_batch, no_fire_flags, this_fire_dir, ...) {
  out <- vector(mode = "list", length = nrow(this_batch))
  
  for (k in 1:nrow(this_batch)) {
    this <- 
      sf::st_read(glue::glue("{this_fire_dir}/goes/{this_batch$processed_filename[k]}"), quiet = TRUE) %>% 
      dplyr::filter(!(Mask %in% no_fire_flags)) %>% 
      dplyr::mutate(scan_center = as.character(scan_center))
    
    if(nrow(this) == 0) {
      next()
    }
    
    out[[k]] <- this
  }
  
  active_fires <- 
    data.table::rbindlist(out) %>% 
    sf::st_as_sf()
  
  return(active_fires)
  
}
