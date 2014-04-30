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
sql = 'SELECT * FROM wind_ds.outputs_all'
df =  dbGetQuery(con,sql)

opts_knit$set(base.dir = runpath)
knit2html("../r/graphics/plot_outputs.md", output = paste0(runpath,"/DG Wind report.html"), title = "DG Wind report", stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"))
dbDisconnect(con)

national_installed_capacity_by_turb_size_bar<-function(df){
data<-ddply(df, .(year, sector, nameplate_capacity_kw), summarise, 
           nat_installed_capacity  = sum(installed_capacity,na.rm=TRUE)/1e6, 
           nat_market_share = mean(market_share,na.rm=TRUE), 
           nat_max_market_share = mean(max_market_share,na.rm=TRUE),
           nat_market_value = sum(ic * number_of_adopters, na.rm = TRUE),
           nat_generation = sum(number_of_adopters * aep, na.rm = TRUE),
           nat_number_of_adopters = sum(number_of_adopters,na.rm=TRUE))

colourCount = length(unique(data$nameplate_capacity_kw))
getPalette = colorRampPalette(brewer.pal(9, "YlOrRd"))
 
ggplot(data, aes(x = year, fill = factor(nameplate_capacity_kw), y = nat_installed_capacity), color = 'black')+
  facet_wrap(~sector,scales="free_y")+
  geom_area()+
  geom_line(aes(ymax = nameplate_capacity_kw), position = 'stack')+
  theme_few()+
  scale_fill_manual(name = 'Turbine Size', values = getPalette(colourCount))+#, values = sector_fil) +
  scale_y_continuous(name ='National Installed Capacity (GW)')+#, labels = comma)+
  theme(strip.text.x = element_text(size = 12, angle = 0))+
  theme(axis.text.x = element_text(angle = 45, hjust = 1))+
  ggtitle('National Installed Capacity by Turbine Size (GW)')
}

# ## Under development
# dr<- 0.1
# n <- 30
# d = data.frame()
# for(cost in c(0,0.1, 0.2, 0.3, 0.4, 0.5, 0.6,0.7)){
#   tmp<-data.frame(npc = seq(0,10,.01))
#   tmp$cf <- 1000 * tmp$npc / (cost * 8760 * ((1 - (1 + dr)^-n)/dr))
#   tmp$cf_max <- 1000 * tmp$npc / (cost * 8760 * ((1 - (1 + dr)^-n)/dr))
#   tmp$cf_min <- 1000 * tmp$npc / ((cost + 0.1) * 8760 * ((1 - (1 + dr)^-n)/dr))
#   tmp$lcoe <- cost
#   d <- rbind(d,tmp)
# }
# 
# d[is.na(d)] <- 0.5
# d[d$cf_max > 0.5, 'cf_max'] <- 0.5
# d[d$cf_min > 0.5, 'cf_min'] <- 0.5
# d[d$lcoe == 0.7 , 'cf_min'] <- 0
# 
# ggplot()+
#   geom_ribbon(data = d, aes(x = npc, y = cf, ymin = cf_min, ymax = cf_max, fill = factor(lcoe), alpha = 0.5))+
#   theme_few()+
#   scale_x_continuous(name = 'Net Present Cost ($/W)', limits = c(0,8))+
#   scale_y_continuous(name = 'Annual Capacity Factor', limits = c(0,.5))+
#   scale_fill_brewer(name = 'LCOE Range ($/kWh)', 
#                     labels=c('< 0.1' ,"0.1 - 0.2", "0.2 - 0.3", "0.3 - 0.4", "0.4 - 0.5", "0.5 - 0.6", "0.6 - 0.7", "> 0.7"), 
#                     palette = 'Spectral')+
#   #geom_point(data = df[sample(nrow(subset(df, year = 2014)), 1000),], aes(x = 0.001 * installed_costs_dollars_per_kw, y = naep/8760), color = 'black', size = 1)
#   geom_point(data = df[sample(nrow(subset(df, year = 2014)), 100000),], aes(x = 0.001 * installed_costs_dollars_per_kw, y = naep/8760), color = 'black', size = 1)
# 
# ##


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

# data$turb_size<-"Small: < 50 kW"
# data[data$nameplate_capacity_kw<=500 & data$nameplate_capacity_kw>50,'turb_size']<-'Mid: 51 - 500 kW' 
# data[data$nameplate_capacity_kw>500,'turb_size']<-'Large: 501 - 3,000 kW'
