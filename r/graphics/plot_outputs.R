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
library (dplyr)


# use for testing/debugging only:
# setwd('/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/python')
# setwd('S:/mgleason/DG_Wind/diffusion_repo/python')

source("../r/graphics/output_funcs.R")
source('../r/maps/map_functions.R', chdir = T)

runpath<-commandArgs(TRUE)[1]
scen_name<-commandArgs(TRUE)[2]

# two different connetions to postgres (1 used by RPostgreSQL and the other by dplyr)
con<-make_con(driver = "PostgreSQL", host = 'gispgdb', dbname="dav-gis", user = 'bsigrin', password = 'bsigrin')
src = src_postgres(host = 'gispgdb', dbname="dav-gis", user = 'bsigrin', password = 'bsigrin')
# lazy load the output table from postgres
df = tbl(src,sql('SELECT * FROM wind_ds.outputs_all'))

# df =  read.csv(paste0(runpath,'/outputs.csv.gz'))

opts_knit$set(base.dir = runpath)
knit2html("../r/graphics/plot_outputs.md", output = paste0(runpath,"/DG Wind report.html"), title = "DG Wind report", stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"))
dbDisconnect(con)


# ggplot(data = df, aes(x = excess_generation_factor, color = sector))+
#   stat_ecdf()
# 
# ggplot(data = data.frame(x = rnorm(n=10000,mean=0.5,sd=0.1)), aes(x))+
#   stat_ecdf()




# lcoe_boxplot<-function(df){
# # Boxplot of LCOE over time, faceted by sector
# ggplot(df, aes(x = factor(year), y = lcoe, fill = sector))+
#   geom_boxplot()+
#   facet_wrap(~sector)+
#   scale_y_continuous(name = 'LCOE (c/kWh)',lim = c(0,100))+
#   scale_x_discrete(name = 'Year')+
#   ggtitle('Cost of Energy by Sector For All Sites Modeled')+
#   scale_fill_manual(name = 'Sector', values = sector_fil)+
#   theme_few()
# 
# data<-ddply(df,.(year,sector),summarise, median = median(lcoe), lql = quantile(lcoe, .25), uql = quantile(lcoe, .75))
# write.csv(data, lcoe_trend.csv)
# }
# 
# lcoe_cdf<-function(df){
# # CDF of lcoe for the first and final model year, faceted by sector. Note that
# # CDF is of bins, and is not weighted by # of custs in bin
# yrs<-c(min(df$year),max(df$year))
# data<-subset(df, year %in% yrs)
# prices<-ddply(data, .(year),summarise, price = median(elec_rate_cents_per_kwh))
# 
# ggplot(data=data,aes(x = lcoe, colour = factor(year)))+
#   stat_ecdf()+
#   geom_vline(data = ddply(data, .(year, sector),summarise, price = median(elec_rate_cents_per_kwh)), aes(xintercept = price, color = factor(year)))+
#   #geom_vline(data = prices, aes(xintercept = price, color = factor(year)))+
#   facet_wrap(~sector)+
#   coord_cartesian(xlim = c(0,60))+
#   theme_few()+
#   ggtitle('Cumulative Probability of Site LCOE (c/kWh)\n Vertical Lines are median retail elec prices')+
#   scale_x_continuous(name = 'Levelized Cost of Energy (c/kWh)')+
#   scale_y_continuous(name = 'Cumulative Probability', label = percent)+
#   scale_color_discrete(name = 'Model Years')
# }






















# lcoe_ribbon<-function(df){ 
# ggplot(data, aes(x = year, y = median, ymin = lql, max = uql, color = sector, fill = sector))+
#   geom_ribbon(alpha = 0.4)+
#   theme_few()+
#   ggtitle('Trends in LCOE by Sector for All Sites')+
#   scale_x_continuous(name = 'Year')+
#   scale_y_continuous(name = 'LCOE (c/kWh)')+
#   scale_fill_manual(name = 'LCOE Interquartile Range', values = sector_fil)+
#   scale_color_manual(values = sector_fil)+
#   guides(color = F)
# }

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
