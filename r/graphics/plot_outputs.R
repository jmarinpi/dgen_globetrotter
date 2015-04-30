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
library(jsonlite)
# library(tidyr)
library (dplyr,quietly = T)
library(grid)
library(gtable)

# use for testing/debugging only:
# setwd('C:/Users/bsigrin/Desktop/diffusion/python')
# setwd('/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/python')
# setwd('S:/mgleason/DG_Wind/diffusion_repo/python')
setwd('/Users/jduckwor/WorkRelated/managed_code/diffusion/python')

# source('../r/maps/map_functions.R', chdir = T)
source("../r/maps/r2js/r2js.R")
source("../r/graphics/output_funcs.R")

# runpath<-'/Users/jduckwor/WorkRelated/managed_code/diffusion/runs_solar/results_20150422_074738/dSolar'
# scen_name<-'solar_test'
# tech = 'solar'
# schema = 'diffusion_solar'

runpath<-commandArgs(T)[1]
scen_name<-commandArgs(T)[2]
tech = commandArgs(T)[3]
schema = commandArgs(T)[4]

# get pg connection params
pg_params = fromJSON(txt = '../python/pg_params.json')



# two different connetions to postgres (1 used by RPostgreSQL and the other by dplyr)
con<-make_con(driver = "PostgreSQL", host = pg_params[['host']], pg_params[['dbname']], user = pg_params[['user']], password = pg_params[['password']], port = pg_params[['port']])
src = src_postgres(host = pg_params[['host']], dbname=pg_params[['dbname']], user = pg_params[['user']], password = pg_params[['password']], port = pg_params[['port']])

# lazy load the output table from postgres
sql = sprintf("SELECT *
                FROM %s.outputs_all",schema)
df = tbl(src,sql(sql))

# get the start year and end year for the model run
start_year = as.numeric(collect(summarise(df, min(year))))
end_year = as.numeric(collect(summarise(df, max(year))))

# get the sectors
sectors = as.character(collect(summarise(df, distinct(sector))))

# set up markdown params and knit markdown file
opts_knit$set(base.dir = runpath)
report_title = sprintf('d%s Report', tech)
report_filepath = sprintf('%s/d%s_Report.html', runpath, tech)
opts_chunk$set(fig.path = sprintf('%s/figure/',runpath ))
source("../r/graphics/output_funcs.R")
source("../r/maps/r2js/r2js.R")
knit2html("../r/graphics/plot_outputs.md", output = report_filepath, title = report_title, 
            stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"))
