library(dplyr)
library(reshape2)
library(ggplot2)
library(RPostgreSQL)
library(gstat)
library(sp)
library(ggthemes)
library(grid)
library(scales)
library(fitdistrplus)

################################################################################################
# CONSTANTS 
outpath = '/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/egs_resource/graphics/for_report'
theme_custom =    theme(panel.grid.minor = element_blank()) +
  theme(text = element_text(colour = ggthemes_data$fivethirtyeight["dkgray"])) +
  theme(plot.margin = unit(c(1, 1, 1, 1), "lines")) +
  theme(axis.title = element_text(size = rel(1.2), face = 'bold')) +
  theme(axis.title.x = element_text(vjust = 0.1)) +
  theme(axis.title.y = element_text(vjust = 1.1)) +
  theme(axis.text = element_text(size = rel(1))) +
  theme(plot.title = element_text(size = rel(1.5), face = "bold")) +
  theme(legend.text = element_text(size = rel(1))) +
  theme(legend.title=element_blank()) +
  theme(legend.key=element_blank()) +
  theme(axis.line = element_line(colour =  ggthemes_data$fivethirtyeight["dkgray"], size = 1)) +
  theme(panel.grid.major = element_line(colour = "light grey")) +
  theme(panel.background = element_rect(fill = "white")) +
  theme(legend.background = element_rect(fill = alpha('white', 0.5)))

################################################################################################


################################################################################################
# CONNECT TO PG
drv <- dbDriver("PostgreSQL")
# connect to postgres
con <- dbConnect(drv, host="gispgdb.nrel.gov", dbname="dav-gis", user="mgleason", password="mgleason")

sql = "SET ROLE 'dgeo-writers';"
dbSendQuery(con, sql)

################################################################################################


sql = "SELECT gid, t_500, t_1000, t_1500, t_2000, t_2500, t_3000,
              ci95_500 as ci_95, area_sqm
       FROM dgeo.egs_temp_at_depth_all_update"
df = dbGetQuery(con, sql)

dfm = melt(df, id.vars = c('gid', 'ci_95', 'area_sqm'), value.name = 't')
dfm$z_slice = as.numeric(substring(dfm$variable, 3, 6))
# check values
summary(dfm$z_slice)
# look right

# assign thicknesses
dfm$thickness_m = 500
dfm[dfm$z_slice == 500, 'thickness_m'] = 450

# calculate volume (in cm3 for each calculations)
dfm$volume_cm3 = dfm$area_sqm * dfm$thickness_m * 100**3
# calculate resource in joules
dfm$acc_res_j = 2.6 * dfm$volume_cm3 * (dfm$t - 15)
# cap negative values at zero
dfm[dfm$acc_res_j < 0, 'acc_res_j'] = 0
# convert to kwh
dfm$acc_res_gwh = dfm$acc_res_j/3.6e+12

# TO DO: add code to deal with mins and maxes