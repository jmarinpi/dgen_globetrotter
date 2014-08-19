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
library (dplyr,quietly = T)

# use for testing/debugging only:
# setwd('C:/Users/bsigrin/Desktop/diffusion/python')
# setwd('/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/python')
# setwd('S:/mgleason/DG_Wind/diffusion_repo/python')

source("../r/graphics/output_funcs.R")
source('../r/maps/map_functions.R', chdir = T)

runpath<-commandArgs(TRUE)[1]
scen_name<-commandArgs(TRUE)[2]

# two different connetions to postgres (1 used by RPostgreSQL and the other by dplyr)
con<-make_con(driver = "PostgreSQL", host = 'gispgdb.nrel.gov', dbname="dav-gis", user = 'bsigrin', password = 'bsigrin')
src = src_postgres(host = 'gispgdb.nrel.gov', dbname="dav-gis", user = 'bsigrin', password = 'bsigrin')
# lazy load the output table from postgres
df = tbl(src,sql("SELECT *,  CASE WHEN turbine_size_kw = 1500 AND nturb > 1 THEN '1500+'::TEXT ELSE turbine_size_kw::TEXT END as system_size_factors FROM diffusion_wind.outputs_all"))
# get the start year and end year for the model run
start_year = as.numeric(collect(summarise(df, min(year))))
end_year = as.numeric(collect(summarise(df, max(year))))

#start_year = 2014
#end_year = 2050 

opts_knit$set(base.dir = runpath)
knit2html("../r/graphics/plot_outputs.md", output = paste0(runpath,"/DG Wind report.html"), title = "DG Wind report", stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"))
dbDisconnect(con)

