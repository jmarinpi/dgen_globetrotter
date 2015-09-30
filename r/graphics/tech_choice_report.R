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

################################################################################################
# use for testing/debugging only:
# setwd('/Users/mgleason/NREL_Projects/github/diffusion/python')
# runpath = '/Users/mgleason/NREL_Projects/github/diffusion/runs/results_20150930_100229/BAU'
# scen_name = 'BAU'
# schema = 'diffusion_results_2015_09_29_16h53m03s'
# pg_params_file = 'pg_params_db.json'
################################################################################################

source("../r/maps/r2js/r2js.R")
source("../r/graphics/output_funcs.R")

runpath<-commandArgs(T)[1]
scen_name<-commandArgs(T)[2]
schema = commandArgs(T)[3]
pg_params_file = commandArgs(T)[4]


# get pg connection params
pg_params = fromJSON(txt = sprintf('../python/%s', pg_params_file))
# two different connetions to postgres (1 used by RPostgreSQL and the other by dplyr)
con<-make_con(driver = "PostgreSQL", host = pg_params[['host']], pg_params[['dbname']], user = pg_params[['user']], password = pg_params[['password']], port = pg_params[['port']])
src = src_postgres(host = pg_params[['host']], dbname=pg_params[['dbname']], user = pg_params[['user']], password = pg_params[['password']], port = pg_params[['port']])

# lazy load the output table from postgres
sql = sprintf("SELECT *
                FROM %s.outputs_all", schema)
df = tbl(src,sql(sql))

# get the start year and end year for the model run
start_year = as.numeric(collect(summarise(df, min(year))))
end_year = as.numeric(collect(summarise(df, max(year))))

# get the sectors
sectors = as.character(collect(summarise(df, distinct(sector))))

# set up markdown params and knit markdown file
source("../r/graphics/output_funcs.R") # TODO: REMOVE AFTER DEBUGGING
opts_knit$set(base.dir = runpath)
report_title = 'dGen Tech Choice Report'
report_filepath = sprintf('%s/dGen_Tech_Choice_Report.html', runpath)
opts_chunk$set(fig.path = sprintf('%s/figure/',runpath ))
knit2html("../r/graphics/tech_choice_report.md", output = report_filepath, title = report_title, 
            stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"),
            quiet = T)

dbDisconnect(con)
