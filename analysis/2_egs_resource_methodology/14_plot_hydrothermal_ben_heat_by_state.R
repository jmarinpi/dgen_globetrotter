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
sql = "with a as
        (
            select case when state = 'Washinton' then 'Washington'
                        when state = 'Massachuttes' then 'Massachusetts'
                   else state
                   end as state, beneficial_heat_mwh/1000./1e6 as ben_heat_million_gwh
            from diffusion_geo.resources_hydrothermal_pt
          	UNION ALL
          	select case when state = 'Washinton' then 'Washington'
			when state = 'Massachuttes' then 'Massachusetts'
                   else state
                   end as state, beneficial_heat_mwh/1000./1e6 as ben_heat_million_gwh
          	from diffusion_geo.resources_hydrothermal_poly
        )
        select b.state_abbr, 
		sum(ben_heat_million_gwh) as est
        from a
        LEFT JOIN diffusion_shared.state_abbr_lkup b
          ON a.state = b.state
        group by b.state_abbr
        order by est"
# note: resource is in million TWh
df = dbGetQuery(con, sql)

# cast state_abbr to char
df = df[with(df, order(est)), ]
df$state_abbr = factor(df$state_abbr, levels = df$state_abbr)
# drop zeros
df = df[df$est > 0, ]
# melt
# dfm = melt(df, id.vars = c('state_abbr'), value.name = 'resource_twh', variable.name = 'type')

ggplot(data = df) +
  geom_bar(aes(x = state_abbr, y = est), stat = 'identity', fill = '#cb181d', colour = 'white', shape = 21) +
 scale_y_continuous(name = 'Beneficial Heat (million GWh)', breaks = seq(0, 2.5, .5)) +
  coord_flip() +
  xlab('State') +
  theme_custom

outpng = '/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/grc_2016/hydrothermal_bh_by_state.png'
ggsave(outpng, width = 4, height  = 6, dpi = 300)

