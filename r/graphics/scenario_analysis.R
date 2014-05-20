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
#<-c("high_itc_20140516_140632", "low_itc_20140516_140747",  "med_itc_20140516_140901",  "no_itc_20140516_141015")  
#runpath<-commandArgs(TRUE)[1]
#scen_name<-commandArgs(TRUE)[2]

diff_trends<-get_diffusion_trends_data(scen_folders)

out_folder = sprintf('../runs/scenario_comparison_%s',format(Sys.time(), '%Y%m%d_%H%M%S'))
dir.create(out_folder)
opts_knit$set(base.dir = out_folder)
knit2html("../r/graphics/scenario_analysis.md", output = sprintf("%s/Scenario Analysis Report.html",out_folder), title = "DG Wind Scenario Analysis Report", stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"))

