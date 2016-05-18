library(ggplot2)
library(dplyr)

summarize_outputs = function(output_files_list){
  
  output_list = list()
  for (i in 1:length(output_files_list)){
    output = output_files_list[[i]]
    df = read.csv(output)
    
    sf = group_by(df, year) %>%
      summarize(cap_gw = sum(installed_capacity)/1000/1000,
                systems = sum(number_of_adopters)
      ) %>% 
      as.data.frame()
    sf$seed = i
    
    output_list[[i]] = sf
    
  }
  
  all_sf = do.call(rbind, output_list)
  all_sf$seed = as.factor(all_sf$seed)
  
  return(all_sf)
}

setwd('/Users/mgleason/NREL_Projects/github/diffusion/runs/results_block_microdata_sensitivity')
all_files = list.files(recursive = T)
outputs_wind_blocks = all_files[grepl('*/outputs_wind.csv.gz',all_files)]
outputs_solar_blocks = all_files[grepl('*/outputs_solar.csv.gz',all_files)]
sf_wind_blocks = summarize_outputs(outputs_wind_blocks)
sf_solar_blocks = summarize_outputs(outputs_solar_blocks)

setwd('/Users/mgleason/NREL_Projects/github/diffusion/runs/results_point_microdata_sensitivity')
all_files = list.files(recursive = T)
outputs_wind_points = all_files[grepl('*/outputs_wind.csv.gz',all_files)]
outputs_solar_points = all_files[grepl('*/outputs_solar.csv.gz',all_files)]
sf_wind_points = summarize_outputs(outputs_wind_points)
sf_solar_points = summarize_outputs(outputs_solar_points)


ggplot() +
  geom_line(data = sf_wind_points, aes(x = year, y = cap_gw, fill = seed), colour = 'gray', stat = 'identity') +
  geom_line(data = sf_wind_blocks, aes(x = year, y = cap_gw, fill = seed), colour = 'black', stat = 'identity')


ggplot() +
  geom_line(data = sf_solar_points, aes(x = year, y = cap_gw, fill = seed), colour = 'gray', stat = 'identity') +
  geom_line(data = sf_solar_blocks, aes(x = year, y = cap_gw, fill = seed), colour = 'black', stat = 'identity')
