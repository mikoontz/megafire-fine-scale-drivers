#### Create mask and DQF attribute lookup tables

create_mask_lookup_table <- function(target_goes, year, upload = TRUE) {
  ### write the Mask and DQF meanings to disk
  
  # GOES-16 and GOES-17
  # Get the flag values that are important using an example .nc file if not done already
  # flag_vals                            flag_meanings
  # 10                                   good_fire_pixel
  # 11                              saturated_fire_pixel
  # 12                     cloud_contaminated_fire_pixel
  # 13                       high_probability_fire_pixel
  # 14                     medium_probability_fire_pixel
  # 15                        low_probability_fire_pixel
  # 30               temporally_filtered_good_fire_pixel
  # 31          temporally_filtered_saturated_fire_pixel
  # 32 temporally_filtered_cloud_contaminated_fire_pixel
  # 33   temporally_filtered_high_probability_fire_pixel
  # 34 temporally_filtered_medium_probability_fire_pixel
  # 35    temporally_filtered_low_probability_fire_pixel
  ###
  
  if(!file.exists(here::here("data/out/goes-mask-meanings.csv")) | !file.exists(here::here("data/out/goes-dqf-meanings.csv"))) {
    
    goes_af <- readr::read_csv(file = glue::glue("{here::here()}/data/out/{target_goes}_{year}_conus-filenames.csv"))
    
    # Get example .nc file
    ex_local_path_full <- goes_af$local_path_full[1]
    
    this_nc <- ncdf4::nc_open(ex_local_path_full) %>% ncdf4::ncatt_get(varid = "Mask")
    flag_vals <- this_nc[["flag_values"]]
    flag_meanings <- this_nc[["flag_meanings"]] %>% stringr::str_split(pattern = " ", simplify = TRUE) %>% as.vector()
    flag_df <- data.frame(flag_vals, flag_meanings)
    
    readr::write_csv(x = flag_df, file = here::here("data/out/goes-mask-meanings.csv"))
    
    this_nc <- ncdf4::nc_open(ex_local_path_full) %>% ncdf4::ncatt_get(varid = "DQF")
    flag_vals <- this_nc[["flag_values"]]
    flag_meanings <- this_nc[["flag_meanings"]] %>% stringr::str_split(pattern = " ", simplify = TRUE) %>% as.vector()
    flag_df <- data.frame(flag_vals, flag_meanings)
    
    readr::write_csv(x = flag_df, file = here::here("data/out/goes-dqf-meanings.csv"))
    
    if (upload) {
      system2(command = "aws", 
              args = glue::glue("s3 cp {here::here()}/data/out/goes-mask-meanings.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes-mask-meanings.csv --acl public-read"))
      
      system2(command = "aws", 
              args = glue::glue("s3 cp {here::here()}/data/out/goes-dqf-meanings.csv s3://earthlab-mkoontz/megafire-fine-scale-drivers/goes-dqf-meanings.csv  --acl public-read"))
    }
  }
  
  fire_flags <-
    readr::read_csv(file = here::here("data/out/goes-mask-meanings.csv")) %>%
    dplyr::filter(stringr::str_detect(flag_meanings, pattern = "_fire_pixel")) %>%
    dplyr::filter(stringr::str_detect(flag_meanings, pattern = "no_fire_pixel", negate = TRUE)) %>%
    dplyr::pull(flag_vals)
  
  no_fire_flags <-
    readr::read_csv(file = here::here("data/out/goes-mask-meanings.csv")) %>% 
    dplyr::filter(stringr::str_detect(flag_meanings, pattern = "no_fire_pixel")) %>% 
    dplyr::pull(flag_vals)
  
  return(list(no_fire_flags = no_fire_flags, fire_flags = fire_flags))
}
