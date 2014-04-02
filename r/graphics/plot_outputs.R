library(ggplot2)
library(plyr)
library(maps)
library(scales)
library(data.table)
library(knitr)
library(RColorBrewer)
library(ggthemes)
library(reshape2)
library(xtable)

source("output_funcs.R")

df<- fread('C:/Users/bsigrin/Desktop/diffusion/runs/30per111/outputs.csv', header = T, sep = ',')
#state_lookup<-read.csv('state_lookup.csv',header = T, sep = ',', stringsAsFactor = F)
df<-data.frame(df)

create_report()

###TODO:

# x: CF vs y: installed capacity (binned)

# pp_state<-subset(df, year == min(df$year))
# 
# #' Payback period by state - First Year
# pp_state<-subset(df, year == min(df$year))
# pp_state<-ddply(df, .(sector, state_abbr), summarise, 
#                 pp_95 = quantile(payback_period, 0.95),
#                 mean = quantile(payback_period, 0.5),
#                 pp_5 = quantile(payback_period, 0.05))
# 
# pp_state<- join(pp_state, state_lookup)
# us_state_map <- map_data('state')
# map_data <- join(us_state_map, pp_state, by='region', type='left')
# map_data <- map_data[order(map_data$order), ]
# 
# qplot(long, lat, data=map_data, geom="polygon", group=group, fill=mean)+
#   theme_bw() + labs(x="", y="", fill="")+
#   scale_fill_gradient(low='#EEEEEE', high='darkgreen')+
#   facet_wrap(~sector)+
#   ggtitle(paste0('Mean Payback Period by State and Sector in ',min(df$year)))
# 
# #' Payback period by state - Last Year
# pp_state<-subset(df, year == max(df$year))
# pp_state<-ddply(df, .(sector, state_abbr), summarise, 
#                 pp_95 = quantile(payback_period, 0.95),
#                 mean = quantile(payback_period, 0.5),
#                 pp_5 = quantile(payback_period, 0.05))
# 
# pp_state<- join(pp_state, state_lookup)
# us_state_map <- map_data('state')
# map_data <- join(us_state_map, pp_state, by='region', type='left')
# map_data <- map_data[order(map_data$order), ]
# 
# qplot(long, lat, data=map_data, geom="polygon", group=group, fill=mean)+
#   theme_bw() + labs(x="", y="", fill="")+
#   scale_fill_gradient(low='#EEEEEE', high='darkgreen')+
#   facet_wrap(~sector)+
#   ggtitle(paste0('Mean Payback Period by State and Sector in ',max(df$year)))
