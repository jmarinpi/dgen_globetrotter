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
library(rjson)
# library(tidyr)
library (dplyr,quietly = T)

# use for testing/debugging only:
# setwd('C:/Users/bsigrin/Desktop/diffusion/python')
# setwd('/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/python')
# setwd('S:/mgleason/DG_Wind/diffusion_repo/python')

source("../r/graphics/output_funcs.R")
source('../r/maps/map_functions.R', chdir = T)

runpath<-commandArgs(T)[1]
scen_name<-commandArgs(T)[2]
tech = commandArgs(T)[3]
schema = commandArgs(T)[4]

# get pg connection params
library(rjson)
pg_params = fromJSON(file = '../python/pg_params.json')



# two different connetions to postgres (1 used by RPostgreSQL and the other by dplyr)
con<-make_con(driver = "PostgreSQL", host = pg_params[['host']], pg_params[['dbname']], user = pg_params[['user']], password = pg_params[['password']])
src = src_postgres(host = pg_params[['host']], dbname=pg_params[['dbname']], user = pg_params[['user']], password = pg_params[['password']])

# lazy load the output table from postgres
if (tech == 'wind'){
  sql = sprintf("SELECT *,  
                  CASE WHEN turbine_size_kw = 1500 AND nturb > 1 THEN '1500+'::TEXT 
                  ELSE turbine_size_kw::TEXT 
                  END as system_size_factors 
                FROM %s.outputs_all",schema)
} else if (tech == 'solar'){
  sql = sprintf("SELECT *
                FROM %s.outputs_all",schema)
}
df = tbl(src,sql(sql))

# get the start year and end year for the model run
start_year = as.numeric(collect(summarise(df, min(year))))
end_year = as.numeric(collect(summarise(df, max(year))))

# set up markdown params and knit markdown file
opts_knit$set(base.dir = runpath)
report_title = sprintf('d%s Report', tech)
report_filepath = sprintf('%s/d%s_Report.html', runpath, tech)
opts_chunk$set(fig.path = sprintf('%s/figure',runpath ))
knit2html("../r/graphics/plot_outputs.md", output = report_filepath, title = report_title, 
            stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"))
dbDisconnect(con)

