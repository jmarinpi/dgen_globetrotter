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
library(RPostgreSQL)

source("../r/graphics/output_funcs.R")

runpath<-commandArgs(TRUE)[1]
con<-make_con(driver = "PostgreSQL", host = 'gispgdb', dbname="dav-gis", user = 'bsigrin', password = 'bsigrin')
df<- read.csv(paste0(runpath,'/outputs.csv'), header = T, sep = ',')

opts_knit$set(base.dir = runpath)
knit2html("../r/graphics/plot_outputs.md", output = paste0(runpath,"/DG Wind report.html"), title = "DG Wind report", stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"))
dbDisconnect(con)


###TODO:

# x: CF vs y: installed capacity (binned)
#state_lookup<-read.csv('state_lookup.csv',header = T, sep = ',', stringsAsFactor = F)
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
