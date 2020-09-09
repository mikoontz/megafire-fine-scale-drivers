library(tidyverse)
library(sf)
library(gganimate)
library(transformr)
library(lubridate)

rim <- sf::st_read("data/data_output/rim-fire_ca_2013_daily-spread.gpkg")
king <- sf::st_read("data/data_output/king-fire_ca_2014_daily-spread.gpkg")

rim_animate <- 
  ggplot(rim) +
  geom_sf(aes(fill = event_day)) +
  transition_states(event_day, 
                    wrap = FALSE, 
                    state_length = 1.5, 
                    transition_length = 1) +
  shadow_mark(alpha = 0.3) +
  scale_fill_viridis_c(option = "inferno") +
  labs(title = "{days(closest_state) + lubridate::ymd('2013-08-12')}",
       fill = "Days since ignition")

anim_save(filename = "figures/rim-fire-spread-animated.gif", animation = rim_animate)

king_animate <- 
  ggplot(king) +
  geom_sf(aes(fill = event_day)) +
  transition_states(event_day, 
                    wrap = FALSE, 
                    state_length = 1.5, 
                    transition_length = 1) +
  shadow_mark(alpha = 0.3) +
  scale_fill_viridis_c(option = "inferno") +
  labs(title = "{days(closest_state) + lubridate::ymd('2014-09-11')}",
       fill = "Days since ignition")

anim_save(filename = "figures/king-fire-spread-animated.gif", animation = king_animate)
