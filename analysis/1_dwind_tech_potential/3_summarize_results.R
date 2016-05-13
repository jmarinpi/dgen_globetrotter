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


theme_custom =    theme(panel.grid.minor = element_blank()) +
  theme(text = element_text(colour = ggthemes_data$fivethirtyeight["dkgray"])) +
  theme(plot.margin = unit(c(0.2, 0.2, 0.2, 0.2), "lines")) +
  theme(axis.title = element_text(size = rel(1.1), face = 'bold')) +
  theme(axis.title.x = element_text(vjust = 0.1)) +
  theme(axis.title.y = element_text(vjust = 1.1)) +
  theme(axis.text.y = element_text(size = rel(1))) +
  theme(axis.text.x = element_text(size = rel(1))) +
  theme(plot.title = element_text(size = rel(1.5), face = "bold")) +
  theme(legend.text = element_text(size = rel(1))) +
  theme(legend.title=element_blank()) +
  theme(legend.key=element_blank()) +
  theme(axis.line = element_line(colour =  ggthemes_data$fivethirtyeight["dkgray"], size = 1)) +
  theme(panel.grid.major = element_line(colour = "light grey")) +
  theme(panel.background = element_rect(fill = "white")) +
  theme(legend.background = element_rect(fill = alpha('white', 0.5)))

################################################################################################
# CONNECT TO PG
drv <- dbDriver("PostgreSQL")
# connect to postgres
con <- dbConnect(drv, host="gispgdb.nrel.gov", dbname="dav-gis", user="mgleason", password="mgleason")

sql = "SET ROLE 'diffusion-writers';"
dbSendQuery(con, sql)

################################################################################################

setwd('/Users/mgleason/NREL_Projects/Projects/local_data/dwind_misc/technical_potential_analysis/graphics')



################################################################################################
# BY SIZE CLASS
sql = "SELECT turbine_size_kw, sum(systems_count)/1e6 as systems_count_million, 
              sum(total_capacity_kw)/1000/1000/1000 as capacity_tw,
              sum(total_generation_kwh)/1000/1000/1000 as generation_twh
       FROM diffusion_data_wind.tech_pot_block_turbine_size_selected
        GROUP BY turbine_size_kw;"

df_size = dbGetQuery(con, sql)
df_size$turbine_size_kw = factor(df_size$turbine_size_kw)

ggplot(data = df_size) +
  geom_bar(aes(x = turbine_size_kw, y = systems_count_million), stat = 'identity', fill = '#2b8cbe') +
  theme_custom +
  scale_y_continuous(name = 'Count of Systems (millions)', breaks = seq(0,16,4), label = comma) +
  coord_cartesian(ylim = c(0, 18)) +
  scale_x_discrete(name = 'Turbine Size (kW)')

outpng = 'systems_count_by_size.png'
ggsave(outpng, width = 6, height  = 4, dpi = 300)

ggplot(data = df_size) +
  geom_bar(aes(x = turbine_size_kw, y = capacity_tw), stat = 'identity', fill = '#2b8cbe') +
  theme_custom +
  scale_y_continuous(name = 'Capacity (TW)', breaks = seq(0, 3, 0.5), label = comma) +
  coord_cartesian(ylim = c(0, 3)) +
  scale_x_discrete(name = 'Turbine Size (kW)')
outpng = 'capacity_by_size.png'
ggsave(outpng, width = 6, height  = 4, dpi = 300)


ggplot(data = df_size) +
  geom_bar(aes(x = turbine_size_kw, y = generation_twh), stat = 'identity', fill = '#2b8cbe') +
  theme_custom +
  scale_y_continuous(name = 'Generation (TWh)', breaks = seq(0, 8000, 2000), label = comma) +
  coord_cartesian(ylim = c(0, 8300)) +
  scale_x_discrete(name = 'Turbine Size (kW)')
outpng = 'generation_by_size.png'
ggsave(outpng, width = 6, height  = 4, dpi = 300)


################################################################################################
# BY STATE
sql = "SELECT state_abbr, sum(systems_count)/1e6 as systems_count_million, 
              sum(total_capacity_kw)/1000/1000 as capacity_gw,
              sum(total_generation_kwh)/1000/1000/1000 as generation_twh
FROM diffusion_data_wind.tech_pot_block_turbine_size_selected
GROUP BY state_abbr;"

df_state = dbGetQuery(con, sql)
df_state$state_abbr = factor(df_state$state_abbr, levels = sort(unique(df_state$state_abbr), decreasing = T))

ggplot(data = df_state) +
  geom_bar(aes(x = state_abbr, y = systems_count_million), stat = 'identity', fill = '#2b8cbe') +
  theme_custom +
  scale_y_continuous(name = 'Count of Systems (millions)', breaks = seq(0, 4, 1), label = comma) +
  coord_cartesian(ylim = c(0, 4)) +
  scale_x_discrete(name = 'State') +
  coord_flip() +
  theme(axis.text.x = element_text(size = rel(0.8)))

outpng = 'systems_count_by_state.png'
ggsave(outpng, width = 3, height  = 6, dpi = 300)

ggplot(data = df_state) +
  geom_bar(aes(x = state_abbr, y = capacity_gw), stat = 'identity', fill = '#2b8cbe') +
  theme_custom +
  scale_y_continuous(name = 'Capacity (GW)', breaks = seq(0, 600, 150), label = comma) +
  coord_cartesian(ylim = c(0, 700)) +
  scale_x_discrete(name = 'State') +
  coord_flip() +
  theme(axis.text.x = element_text(size = rel(0.8)))
outpng = 'capacity_by_state.png'
ggsave(outpng, width = 3, height  = 6, dpi = 300)


ggplot(data = df_state) +
  geom_bar(aes(x = state_abbr, y = generation_twh), stat = 'identity', fill = '#2b8cbe') +
  theme_custom +
  scale_y_continuous(name = 'Generation (TWh)', breaks = seq(0, 2000, 500), label = comma) +
  coord_cartesian(ylim = c(0, 2200)) +
  scale_x_discrete(name = 'State') +
  coord_flip() +
  theme(axis.text.x = element_text(size = rel(0.8)))
outpng = 'generation_by_state.png'
ggsave(outpng, width = 3, height  = 6, dpi = 300)



################################################################################################
# BY STATE and SIZE
sql = "SELECT state_abbr, turbine_size_kw, sum(systems_count)/1e3 as systems_count_thousand, 
sum(total_capacity_kw)/1000/1000 as capacity_gw,
sum(total_generation_kwh)/1000/1000/1000 as generation_twh
FROM diffusion_data_wind.tech_pot_block_turbine_size_selected
GROUP BY state_abbr, turbine_size_kw;"

df_both = dbGetQuery(con, sql)
df_both$state_abbr = factor(df_both$state_abbr, levels = sort(unique(df_both$state_abbr)))
df_both$turbine_size_kw = factor(df_both$turbine_size_kw)

ggplot(data = df_both) +
  geom_bar(aes(x = turbine_size_kw, y = systems_count_thousand), stat = 'identity', fill = '#2b8cbe') +
  facet_wrap(~state_abbr, scales = 'free') + 
  theme_custom +
  scale_y_continuous(name = 'Count of Systems (thousands)', label = comma) +
  scale_x_discrete(name = 'Turbine Size (kW)') +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  theme(strip.background = element_rect(fill = 'white')) +
  theme(strip.text = element_text(face = 'bold'))

outpng = 'systems_count_by_state_and_size.png'
ggsave(outpng, width = 16, height  = 10, dpi = 300)

ggplot(data = df_both) +
  geom_bar(aes(x = turbine_size_kw, y = capacity_gw), stat = 'identity', fill = '#2b8cbe') +
  facet_wrap(~state_abbr, scales = 'free') + 
  theme_custom +
  scale_y_continuous(name = 'Capacity (GW)', label = comma) +
  scale_x_discrete(name = 'Turbine Size (kW)') +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  theme(strip.background = element_rect(fill = 'white')) +
  theme(strip.text = element_text(face = 'bold'))

outpng = 'capacity_by_state_and_size.png'
ggsave(outpng, width = 16, height  = 10, dpi = 300)


ggplot(data = df_both) +
  geom_bar(aes(x = turbine_size_kw, y = generation_twh), stat = 'identity', fill = '#2b8cbe') +
  facet_wrap(~state_abbr, scales = 'free') + 
  theme_custom +
  scale_y_continuous(name = 'Generation (TWh)', label = comma) +
  scale_x_discrete(name = 'Turbine Size (kW)') +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  theme(strip.background = element_rect(fill = 'white')) +
  theme(strip.text = element_text(face = 'bold'))

outpng = 'generation_by_state_and_size.png'
ggsave(outpng, width = 16, height  = 10, dpi = 300)