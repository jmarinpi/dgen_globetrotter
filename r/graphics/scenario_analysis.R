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
scen_folders<- unlist(strsplit(commandArgs(TRUE)[1],","))
out_folder <- commandArgs(T)[2]

# Practice paths
# scen_folders<-c('C:/Users/bsigrin/Desktop/diffusion/runs/results_20140606_153935/HighPTC',
#                'C:/Users/bsigrin/Desktop/diffusion/runs/results_20140606_153935/MedPTC',
#                'C:/Users/bsigrin/Desktop/diffusion/runs/results_20140606_153935/LowPTC')
# out_folder<-"C:/Users/bsigrin/Desktop/diffusion/runs/results_20140606_153935/scenario_comparison"

diff_trends<-get_r_data(scen_folders,'diffusion_trends')
cap_selected_trends<-get_r_data(scen_folders,'cap_selected_trends')
height_selected_trends<-get_r_data(scen_folders,'height_selected_trends')
payback_period_trends<-get_r_data(scen_folders,'payback_period_trends')


dir.create(out_folder)
opts_knit$set(base.dir = out_folder)
knit2html("../r/graphics/scenario_analysis.md", output = sprintf("%s/Scenario Analysis Report.html",out_folder), title = "DG Wind Scenario Analysis Report", stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"))



# pp_trends_ribbon<-function(df){
#   # Median payback period over time and sector  
#   p<-ggplot(df, aes(x = year, y = median, ymin = lql, ymax = uql, color = scenario, fill = scenario), size = 0.75)+
#     geom_smooth(stat = 'identity', alpha = 0.4)+
#     geom_line()+
#     facet_wrap(~sector)+
#     scale_y_continuous(name ='Payback Period (years)', lim = c(0,40))+
#     scale_x_continuous(name ='Year')+
#     theme_few()+
#     theme(strip.text.x = element_text(size=12, angle=0,))+
#     ggtitle('National Payback Period (Median and Inner-Quartile Range)')
#   return(p)
# }
# 
# turb_trends_hist<-function(df){
#   # What size system are customers selecting in 2014?
#   df1<-subset(df, year == min(year))
#   df2<-subset(df, year == max(year))
#   
#   p1<-ggplot(df1, aes(x = factor(cap), weight = p, fill = scenario))+
#     geom_histogram(position = 'dodge')+
#     facet_wrap(~sector)+
#     theme_few()+
#     scale_y_continuous(name ='Percent of Customers Selecting Turbine Size', labels = percent)+
#     scale_x_discrete(name ='Optimal Size Turbine for Customer (kW)')+
#     #scale_color_manual(values = sector_col) +
#     #scale_fill_manual(values = sector_fil) +
#     theme(axis.text.x = element_text(angle = 45, hjust = 1))+
#     theme(strip.text.x = element_text(size=12, angle=0,))+
#     #guides(color = FALSE, fill=FALSE)+
#     ggtitle(sprintf('Size of Turbines Considered in %s', min(df$year)))
#   
#   p2<-ggplot(df2, aes(x = factor(cap), weight = p, fill = scenario))+
#     geom_histogram(position = 'dodge')+
#     facet_wrap(~sector)+
#     theme_few()+
#     scale_y_continuous(name ='Percent of Customers Selecting Turbine Size', labels = percent)+
#     scale_x_discrete(name ='Optimal Size Turbine for Customer (kW)')+
#     #scale_color_manual(values = sector_col) +
#     #scale_fill_manual(values = sector_fil) +
#     theme(axis.text.x = element_text(angle = 45, hjust = 1))+
#     theme(strip.text.x = element_text(size=12, angle=0,))+
#     #guides(color = FALSE, fill=FALSE)+
#     ggtitle(sprintf('Size of Turbines Considered in %s', max(df$year)))
# 
#   out = list('p1' = p1, 'p2' = p2)  
# return(out)
# }
  
  

