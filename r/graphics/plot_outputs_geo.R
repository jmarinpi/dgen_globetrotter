library(ggplot2, quietly = T)
library(plyr, quietly = T)
library(maps, quietly = T)
library(scales, quietly = T)
suppressMessages(library(data.table, quietly = T, verbose = F, warn.conflicts = F))
library(knitr, quietly = T)
library(RColorBrewer, quietly = T)
library(ggthemes, quietly = T)
library(reshape2, quietly = T)
library(xtable, quietly = T)
library(RPostgreSQL, quietly = T)
library(jsonlite, quietly = T, warn.conflicts = F)
# library(tidyr)
library (dplyr, quietly = T, warn.conflicts = F)
library(grid, quietly = T)
library(gtable, quietly = T)
library(gridExtra, quietly = T)


# use for testing/debugging only:
# setwd('C:/Users/bsigrin/Desktop/diffusion/python')
# setwd('/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/python')
# setwd('S:/mgleason/DG_Wind/diffusion_repo/python')
# setwd('/Users/jduckwor/WorkRelated/managed_code/diffusion/python')
# setwd('/Users/mgleason/NREL_Projects/github/diffusion/python')
setwd('/Users/kmccabe/Projects/diffusion/python')

# source('../r/maps/map_functions.R', chdir = T)
source("../r/maps/r2js/r2js.R")
source("../r/graphics/output_funcs_geo.R")


runpath<-'/Users/kmccabe/Projects/diffusion/runs/'
scen_name<-'test'
tech = 'ghp'
schema = 'diffusion_results_2016_09_12_14h30m17s'
pg_params_file = 'pg_params_bigde.json'
file_suffix = 'test'

# runpath<-'/Users/kmccabe/Projects/diffusion/runs/'
# scen_name<-'test'
# tech = 'du'
# schema = 'diffusion_results_du_beta'
# pg_params_file = 'pg_params_gis.json'
# file_suffix = 'test'

# runpath<-commandArgs(T)[1]
# scen_name<-commandArgs(T)[2]
# tech = commandArgs(T)[3]
# schema = commandArgs(T)[4]
# pg_params_file = commandArgs(T)[5]
# file_suffix = commandArgs(T)[6]

# get pg connection params
pg_params = fromJSON(txt = sprintf('../python/%s', pg_params_file))



# two different connetions to postgres (1 used by RPostgreSQL and the other by dplyr)
con<-make_con(driver = "PostgreSQL", host = pg_params[['host']], pg_params[['dbname']], user = pg_params[['user']], password = pg_params[['password']], port = pg_params[['port']])
src = src_postgres(host = pg_params[['host']], dbname=pg_params[['dbname']], user = pg_params[['user']], password = pg_params[['password']], port = pg_params[['port']])

if(tech=='ghp') {
  # lazy load the output table from postgres
  sql = sprintf("SELECT *
                  FROM %s.agent_outputs_ghp
                   WHERE tech = '%s'", schema, tech)
  df = tbl(src,sql(sql))
} else{
  sql_capacity = sprintf("SELECT *
                FROM %s.output_market_summary_du", schema)
  df_capacity = tbl(src,sql(sql_capacity))
  
  sql = sprintf("SELECT *
                FROM %s.agent_outputs_du
                WHERE tech = '%s'", schema, tech)
  df = tbl(src,sql(sql))
}


# get the start year and end year for the model run
start_year = as.numeric(collect(summarise(df, min(year))))
end_year = as.numeric(collect(summarise(df, max(year))))

# get the sectors
sectors = as.character(collect(summarise(df, distinct(sector))))

# set up markdown params and knit markdown file
opts_knit$set(base.dir = runpath)
report_title = sprintf('d%s Report', tech)
report_filepath = sprintf('%s/d%s_Report%s.html', runpath, tech, file_suffix)
ppt_dir = sprintf('%s/ppt_figures', runpath)
dir.create(ppt_dir)
opts_chunk$set(fig.path = sprintf('%s/figure/',runpath ))
# source("../r/graphics/output_funcs_geo.R")
# source("../r/maps/r2js/r2js.R")
knit2html("../r/graphics/plot_outputs_geo.md", output = report_filepath, title = report_title,
            stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"),
            quiet = T)

# if(tech=='ghp') {
#   out<-diffusion_trends(df,runpath,scen_name)
# } else if(tech=='du') {
#   out<-diffusion_trends_du(df,df_capacity,runpath,scen_name)
# }

dbDisconnect(con)

