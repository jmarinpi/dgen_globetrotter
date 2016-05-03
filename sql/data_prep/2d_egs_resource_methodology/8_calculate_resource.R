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


sql = "SELECT gid, 

t_500 as t_0500_est,
t_500 - ci95_500 as t_0500_min,
t_500 + ci95_500 as t_0500_max,

t_1000 as t_1000_est,
t_1000 - ci95_1000 as t_1000_min,
t_1000 + ci95_1000 as t_1000_max,

t_1500 as t_1500_est,
t_1500 - ci95_1500 as t_1500_min,
t_1500 + ci95_1500 as t_1500_max,

t_2000 as t_2000_est,
t_2000 - ci95_2000 as t_2000_min,
t_2000 + ci95_2000 as t_2000_max,

t_2500 as t_2500_est,
t_2500 - ci95_2500 as t_2500_min,
t_2500 + ci95_2500 as t_2500_max,

t_3000 as t_3000_est,
t_3000 - ci95_3000 as t_3000_min,
t_3000 + ci95_3000 as t_3000_max,

area_sqm

FROM dgeo.egs_temp_at_depth_all_update"
df = dbGetQuery(con, sql)

dfm = melt(df, id.vars = c('gid', 'area_sqm'), value.name = 't')
dfm$z_slice = as.numeric(substring(dfm$variable, 3, 6))
dfm$type = substring(dfm$variable, 8, 11)
# check values
unique(dfm$z_slice)
unique(dfm$type)
# look right

# fix the minimum temperature that are below zero
# how many temperatures are below zero?
sum(dfm$t < 0)/nrow(dfm) # about 2%
# are they all mins?
unique(dfm[dfm$t < 0, 'type']) # yes
# fix them
dfm[dfm$t < 0, 't'] = 0

# also change any temperatures that are outside of focus range (30-150) to zero
dfm[dfm$t < 30, 't'] = 0
dfm[dfm$t > 150, 't'] = 0

# assign thicknesses
dfm$thickness_m = 500
dfm[dfm$z_slice == 500, 'thickness_m'] = 450 # 500 m slice only runs from 300 - 750 m

# calculate volume (in cm3 for each calculations)
dfm$volume_cm3 = dfm$area_sqm * dfm$thickness_m * 1e6

# calculate resource in joules
dfm$acc_res_j = 2.6 * dfm$volume_cm3 * (dfm$t - 15)
# cap negative values at zero
dfm[dfm$acc_res_j < 0, 'acc_res_j'] = 0
# convert to kwh
dfm$acc_res_gwh = dfm$acc_res_j/3.6e+12

# cast the sliced data back to wide
dfw = dcast(dfm, gid ~ z_slice + type, value.var = 'acc_res_gwh')
names(dfw)[2:length(names(dfw))] = sprintf('res_%s', names(dfw)[2:length(names(dfw))])

# summarize the data up for each gid
dfs = group_by(dfm, gid, type) %>%
  summarize(total_acc_res_gwh = sum(acc_res_gwh),
            total_volume_m3 = sum(volume_cm3)/1e6,
            total_thickness_m = sum(thickness_m))
# cast to wide
dfsw = dcast(dfs, gid ~ type , value.var = 'total_acc_res_gwh')
names(dfsw)[2:length(names(dfsw))] = sprintf('res_tot_%s', names(dfsw)[2:length(names(dfsw))])

# write results to postgres
dbWriteTable(con, c('dgeo', 'egs_accessible_resource_by_depth'), dfw, row.names = F, overwrite = T)
dbWriteTable(con, c('dgeo', 'egs_accessible_resource_total'), dfsw, row.names = F, overwrite = T)




