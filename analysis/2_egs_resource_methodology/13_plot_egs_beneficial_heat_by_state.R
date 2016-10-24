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
# Absolute beneficial heat
sql = "select b.state_abbr,
              SUM(ben_heat_mwh_totmin)/1000./1e6 as min,
              SUM(ben_heat_mwh_totest)/1000./1e6 as est,
            	SUM(ben_heat_mwh_totmax)/1000./1e6 as max
      from dgeo.egs_resource_shallow_lowt_extractable_and_beneficial_heat a
      LEFT JOIN dgeo.egs_resource_shallow_lowt b
        ON a.gid = b.gid
      where state_abbr is not null
      GROUP BY b.state_abbr
      ORDER BY 3;"
# note: resource is in million TWh
df = dbGetQuery(con, sql)

# cast state_abbr to char
df = df[with(df, order(est)), ]
df$state_abbr = factor(df$state_abbr, levels = df$state_abbr)
# melt
# dfm = melt(df, id.vars = c('state_abbr'), value.name = 'resource_twh', variable.name = 'type')

ggplot(data = df) +
  geom_errorbar(aes(x = state_abbr, ymin = min, ymax = max), stat = 'identity', colour = '#cb181d') +
  geom_point(aes(x = state_abbr, y = est), stat = 'identity', size = rel(2.5), fill = '#cb181d', colour = 'white', shape = 21) +
  scale_y_continuous(name = 'Beneficial Heat (million GWh)', breaks = seq(0, 1000, 200), labels = comma) +
  coord_flip() +
  xlab('State') +
  theme_custom

outpng = '/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/grc_2016/egs_hdr_bh_by_state.png'
ggsave(outpng, width = 4, height  = 6, dpi = 300)



# BULLET CHART
ggplot(data = df) +
  geom_bar(aes(x = state_abbr, y = max), stat = 'identity', width = .5, fill = '#a50f15', colour = 'white') +
  geom_bar(aes(x = state_abbr, y = est), stat = 'identity', width = .75, fill = '#cb181d', colour = 'white') +
  geom_bar(aes(x = state_abbr, y = min), stat = 'identity', width = 1, fill = '#ef3b2c', colour = 'white') +
  scale_y_continuous(name = 'Beneficial Heat (million GWh)', breaks = seq(0, 1000, 200), labels = comma) +
  coord_flip() +
  xlab('State') +
  theme_custom

outpng = '/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/grc_2016/egs_hdr_bh_by_state_bullet.png'
ggsave(outpng, width = 4, height  = 6, dpi = 300)

