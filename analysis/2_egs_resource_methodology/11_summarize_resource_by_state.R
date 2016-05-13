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
# CONNECT TO PG
drv <- dbDriver("PostgreSQL")
# connect to postgres
con <- dbConnect(drv, host="gispgdb.nrel.gov", dbname="dav-gis", user="mgleason", password="mgleason")

sql = "SET ROLE 'dgeo-writers';"
dbSendQuery(con, sql)

################################################################################################

theme_custom =    theme(panel.grid.minor = element_blank()) +
  theme(text = element_text(colour = ggthemes_data$fivethirtyeight["dkgray"])) +
  theme(plot.margin = unit(c(0.1, 0.1, 0.1, 0.1), "lines")) +
  theme(axis.title = element_text(size = rel(1.1), face = 'bold')) +
  theme(axis.title.x = element_text(vjust = 0.1)) +
  theme(axis.title.y = element_text(vjust = 1.1)) +
  theme(axis.text.y = element_text(size = rel(1))) +
  theme(axis.text.x = element_text(size = rel(0.85), angle = 60, hjust = 1)) +
  theme(plot.title = element_text(size = rel(1.5), face = "bold")) +
  theme(legend.text = element_text(size = rel(1))) +
  theme(legend.title=element_blank()) +
  theme(legend.key=element_blank()) +
  theme(axis.line = element_line(colour =  ggthemes_data$fivethirtyeight["dkgray"], size = 1)) +
  theme(panel.grid.major = element_line(colour = "light grey")) +
  theme(panel.background = element_rect(fill = "white")) +
  theme(legend.background = element_rect(fill = alpha('white', 0.5)))




################################################################################################
# Absolute resource
sql = "select state_abbr,
                sum(restotmin)/(1000. * 1e6) as min, 
                sum(restotest)/(1000. * 1e6) as est, 
                sum(restotmax)/(1000. * 1e6) as max
      from dgeo.egs_resource_shallow_lowt
      where state_abbr is not null
      GROUP BY state_abbr
      ORDER BY 2;"
# note: resource is in million TWh
df = dbGetQuery(con, sql)

# cast state_abbr to char
df = df[with(df, order(-est)), ]
df$state_abbr = factor(df$state_abbr, levels = df$state_abbr)
# melt
# dfm = melt(df, id.vars = c('state_abbr'), value.name = 'resource_twh', variable.name = 'type')

ggplot(data = df) +
  geom_errorbar(aes(x = state_abbr, ymin = min, ymax = max), stat = 'identity', colour = '#cb181d') +
  geom_point(aes(x = state_abbr, y = est), stat = 'identity', size = rel(2.5), fill = '#cb181d', colour = 'white', shape = 21) +
  scale_y_continuous(name = 'Accessible Resource (million TWh)', breaks = seq(0, 100, 12.5), labels = comma) +
  xlab('State') +
  theme_custom

outpng = '/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/egs_resource/graphics/for_report/resource_by_state.png'
ggsave(outpng, width = 6, height  = 4, dpi = 300)


################################################################################################
# Normalized resource (by area)
sql = "select state_abbr,
      sum(restotmin)/1000. as min, 
      sum(restotest)/1000. as est, 
      sum(restotmax)/1000. as max,
      sum(area_sqm)/1e6 as area_sqkm
      from dgeo.egs_resource_shallow_lowt
      where state_abbr is not null
      GROUP BY state_abbr
      ORDER BY 2;"
# note: resource is in TWh
dfn = dbGetQuery(con, sql)
# normalize the values
dfn$min = dfn$min/dfn$area_sqkm
dfn$est = dfn$est/dfn$area_sqkm
dfn$max = dfn$max/dfn$area_sqkm

# cast state_abbr to char
dfn = dfn[with(dfn, order(-est)), ]
dfn$state_abbr = factor(dfn$state_abbr, levels = dfn$state_abbr)




ggplot(data = dfn) +
  geom_errorbar(aes(x = state_abbr, ymin = min, ymax = max), stat = 'identity', colour = '#cb181d') +
  geom_point(aes(x = state_abbr, y = est), stat = 'identity', size = rel(2.5), fill = '#cb181d', colour = 'white', shape = 21) +
  scale_y_continuous(name = expression(paste('Accessible Resource (TWh/', km^2, ')')), breaks = seq(0, 150, 25), labels = comma) +
  coord_cartesian(ylim = c(0, 175)) +
  xlab('State') +
  theme_custom


outpng = '/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/egs_resource/graphics/for_report/normalized_resource_by_state.png'
ggsave(outpng, width = 6, height  = 4, dpi = 300)
