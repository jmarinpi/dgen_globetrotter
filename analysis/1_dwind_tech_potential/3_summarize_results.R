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


# colors
large = '#3398D7'
mid = '#F4A814'
com = '#86C12F'
res = '#E5601B'

################################################################################################
# BY SIZE CLASS
sql = "SELECT turbine_size_kw, sum(systems_count)/1e6 as systems_count_million, 
              sum(total_capacity_kw)/1000/1000/1000 as capacity_tw,
              sum(total_generation_kwh)/1000/1000/1000 as generation_twh
       FROM diffusion_data_wind.tech_pot_block_turbine_size_selected
        GROUP BY turbine_size_kw;"

df_size = dbGetQuery(con, sql)
# add size class field
breaks = c(0, 10, 100, 750, 1500)
labels = c('Residential','Commercial','Mid-Size','Large')
df_size$size_class = cut(df_size$turbine_size_kw, breaks, labels)
# convert size to factor
df_size$turbine_size_kw = factor(df_size$turbine_size_kw)


colors = c(res, com, mid, large)
ggplot(data = df_size) +
  geom_bar(aes(x = turbine_size_kw, y = systems_count_million, fill = size_class), stat = 'identity') +
  theme_custom +
  scale_y_continuous(name = 'Count of Systems (millions)', breaks = seq(0,16,4), label = comma) +
  coord_cartesian(ylim = c(0, 18)) +
  scale_fill_manual(values = colors) +
  scale_x_discrete(name = 'Turbine Size (kW)')

outpng = 'systems_count_by_size.png'
ggsave(outpng, width = 6, height  = 3, dpi = 300)

# for capacity, break out large from the rest

ggplot(data = filter(df_size, size_class %in% c('Residential', 'Commercial', 'Mid-Size')) ) +
  geom_bar(aes(x = turbine_size_kw, y = capacity_tw, fill = size_class), stat = 'identity') +
  theme_custom +
  scale_y_continuous(name = 'Capacity (TW)', breaks = seq(0, 1.25, 0.25), label = comma) +
  coord_cartesian(ylim = c(0, 1.25)) +
  scale_x_discrete(name = 'Turbine Size (kW)') +
  scale_fill_manual(values = colors)

outpng = 'capacity_by_size_sub_mw.png'
ggsave(outpng, width = 6, height  = 3, dpi = 300)

ggplot(data = filter(df_size, size_class %in% c('Large')) ) +
  geom_bar(aes(x = turbine_size_kw, y = capacity_tw, fill = size_class), stat = 'identity') +
  theme_custom +
  scale_y_continuous(name = 'Capacity (TW)', breaks = seq(0, 3, 0.5), label = comma) +
  coord_cartesian(ylim = c(0, 3)) +
  scale_x_discrete(name = 'Turbine Size (kW)') +
  scale_fill_manual(values = c(large))

outpng = 'capacity_by_size_mw.png'
ggsave(outpng, width = 3, height  = 3, dpi = 300)

###########################
# group the data by size classes
df_size_class = group_by(df_size, size_class) %>%
                summarize(capacity_gw = sum(capacity_tw)*1000,
                          systems_count_million = sum(systems_count_million) )
# system count
fill = '#1D5AB2'
ggplot(data = filter(df_size_class, size_class != 'Large')) +
  geom_bar(aes(x = size_class, y = capacity_gw), stat = 'identity', fill = fill, width = .3) +
  theme_custom +
  scale_y_continuous(name = 'Capacity Potential (GW)', breaks = seq(0, 2500, 500), label = comma) +
  coord_cartesian(ylim = c(0, 2500)) +
  scale_x_discrete(name = 'Turbine Class')

outpng = 'capacity_by_size_class.png'
ggsave(outpng, width = 6, height  = 4, dpi = 300)

# system count
fill = '#1D5AB2'
ggplot(data = df_size_class) +
  geom_bar(aes(x = size_class, y = systems_count_million), stat = 'identity', fill = fill, width = .225) +
  theme_custom +
  scale_y_continuous(name = 'Systems Count Potential (Millions)', breaks = seq(0,40,5), label = comma) +
  coord_cartesian(ylim = c(0, 40)) +
  scale_x_discrete(name = 'Turbine Class')

outpng = 'systems_count_by_size_class.png'
ggsave(outpng, width = 6, height  = 4, dpi = 300)



################################################################################################
# # BY STATE
# sql = "SELECT state_abbr, sum(systems_count)/1e6 as systems_count_million, 
#               sum(total_capacity_kw)/1000/1000 as capacity_gw,
#               sum(total_generation_kwh)/1000/1000/1000 as generation_twh
# FROM diffusion_data_wind.tech_pot_block_turbine_size_selected
# GROUP BY state_abbr;"
# 
# df_state = dbGetQuery(con, sql)
# df_state$state_abbr = factor(df_state$state_abbr, levels = sort(unique(df_state$state_abbr), decreasing = T))
# 
# ggplot(data = df_state) +
#   geom_bar(aes(x = state_abbr, y = systems_count_million), stat = 'identity', fill = '#2b8cbe') +
#   theme_custom +
#   scale_y_continuous(name = 'Count of Systems (millions)', breaks = seq(0, 4, 1), label = comma) +
#   coord_cartesian(ylim = c(0, 4)) +
#   scale_x_discrete(name = 'State') +
#   coord_flip() +
#   theme(axis.text.x = element_text(size = rel(0.8)))
# 
# outpng = 'systems_count_by_state.png'
# ggsave(outpng, width = 3, height  = 6, dpi = 300)
# 
# ggplot(data = df_state) +
#   geom_bar(aes(x = state_abbr, y = capacity_gw), stat = 'identity', fill = '#2b8cbe') +
#   theme_custom +
#   scale_y_continuous(name = 'Capacity (GW)', breaks = seq(0, 600, 150), label = comma) +
#   coord_cartesian(ylim = c(0, 700)) +
#   scale_x_discrete(name = 'State') +
#   coord_flip() +
#   theme(axis.text.x = element_text(size = rel(0.8)))
# outpng = 'capacity_by_state.png'
# ggsave(outpng, width = 3, height  = 6, dpi = 300)
# 
# 
# ggplot(data = df_state) +
#   geom_bar(aes(x = state_abbr, y = generation_twh), stat = 'identity', fill = '#2b8cbe') +
#   theme_custom +
#   scale_y_continuous(name = 'Generation (TWh)', breaks = seq(0, 2000, 500), label = comma) +
#   coord_cartesian(ylim = c(0, 2200)) +
#   scale_x_discrete(name = 'State') +
#   coord_flip() +
#   theme(axis.text.x = element_text(size = rel(0.8)))
# outpng = 'generation_by_state.png'
# ggsave(outpng, width = 3, height  = 6, dpi = 300)
# 


################################################################################################
# BY STATE and SIZE
sql = "SELECT state_abbr, turbine_size_kw, sum(systems_count)/1e3 as systems_count_thousand, 
              sum(total_capacity_kw)/1000/1000 as capacity_gw,
              sum(total_generation_kwh)/1000/1000/1000 as generation_twh
      FROM diffusion_data_wind.tech_pot_block_turbine_size_selected
      GROUP BY state_abbr, turbine_size_kw;"

df_both = dbGetQuery(con, sql)
df_both$state_abbr = factor(df_both$state_abbr, levels = sort(unique(df_both$state_abbr)))
# add size class field
breaks = c(0, 10, 100, 750, 1500)
labels = c('Residential','Commercial','Mid-Size','Large')
df_both$size_class = cut(df_both$turbine_size_kw, breaks, labels)
df_both$turbine_size_kw = factor(df_both$turbine_size_kw)
# 
# 
# ggplot(data = df_both) +
#   geom_bar(aes(x = turbine_size_kw, y = systems_count_thousand), stat = 'identity', fill = '#2b8cbe') +
#   facet_wrap(~state_abbr, scales = 'free') + 
#   theme_custom +
#   scale_y_continuous(name = 'Count of Systems (thousands)', label = comma) +
#   scale_x_discrete(name = 'Turbine Size (kW)') +
#   theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
#   theme(strip.background = element_rect(fill = 'white')) +
#   theme(strip.text = element_text(face = 'bold'))
# 
# outpng = 'systems_count_by_state_and_size.png'
# ggsave(outpng, width = 16, height  = 10, dpi = 300)
# 
# ggplot(data = df_both) +
#   geom_bar(aes(x = turbine_size_kw, y = capacity_gw), stat = 'identity', fill = '#2b8cbe') +
#   facet_wrap(~state_abbr, scales = 'free') + 
#   theme_custom +
#   scale_y_continuous(name = 'Capacity (GW)', label = comma) +
#   scale_x_discrete(name = 'Turbine Size (kW)') +
#   theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
#   theme(strip.background = element_rect(fill = 'white')) +
#   theme(strip.text = element_text(face = 'bold'))
# 
# outpng = 'capacity_by_state_and_size.png'
# ggsave(outpng, width = 16, height  = 10, dpi = 300)
# 
# 
# ggplot(data = df_both) +
#   geom_bar(aes(x = turbine_size_kw, y = generation_twh), stat = 'identity', fill = '#2b8cbe') +
#   facet_wrap(~state_abbr, scales = 'free') + 
#   theme_custom +
#   scale_y_continuous(name = 'Generation (TWh)', label = comma) +
#   scale_x_discrete(name = 'Turbine Size (kW)') +
#   theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
#   theme(strip.background = element_rect(fill = 'white')) +
#   theme(strip.text = element_text(face = 'bold'))
# 
# outpng = 'generation_by_state_and_size.png'
# ggsave(outpng, width = 16, height  = 10, dpi = 300)


# group the data up into MW vs subMW scale
df_both$mw_scale = df_both$size_class == 'Large'
df_scale = group_by(df_both, mw_scale, state_abbr) %>%
              summarize(capacity_gw = sum(capacity_gw))
df_mw_scale = filter(df_scale, mw_scale == T)
df_submw_scale = filter(df_scale, mw_scale == F)
write.csv(df_mw_scale, '/Users/mgleason/NREL_Projects/Projects/local_data/dwind_misc/technical_potential_analysis/graphics/capacity_by_state_and_mw_scale.csv')


library(albersusa)
library(sp)
library(rgeos)
library(maptools)
library(ggplot2)
library(ggthemes)
library(scales)
library(rgdal)
library(grid)


states_path = '/Users/mgleason/NREL_Projects/Projects/local_data/dwind_misc/technical_potential_analysis/states_nad83.geojson'
states = readOGR(states_path, 'OGRGeoJSON', stringsAsFactors = FALSE, verbose = FALSE)
proj = '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs'
# states = spTransform(states, CRS(proj))
plot(states)
# drop AK and HI
states = states[!(states@data$STUSPS %in% c('AK', 'HI', 'PR')), ]
plot(states)
# # add data
state_albers_mw_scale = merge(states, df_mw_scale, by.x = c('STUSPS'), by.y = c('state_abbr'))
states@data = state_albers_mw_scale
# state_albers_submw_scale = merge(states_albers, df_submw_scale, by.x = c('STUSPS'), by.y = c('state_abbr'))  


theme_map <- function(base_size = 9, base_family = "") {
  theme_bw(base_size = base_size, base_family = base_family) %+replace%
    theme(axis.line = element_blank(),
          axis.text = element_blank(),
          axis.ticks = element_blank(),
          axis.title = element_blank(),
          panel.background = element_blank(),
          panel.border = element_blank(),
          panel.grid = element_blank(),
          panel.margin = unit(0, "lines"),
          plot.background = element_blank(),
          legend.justification = c(0, 0),
          legend.position = c(0, 0))
}

states_map = fortify(states, region = 'STUSPS')

gg <- ggplot()
gg <- gg + geom_map(data=states_map, map=states_map,
                    aes(x=long, y=lat, map_id=id),
                    color="#2b2b2b", size=0.1, fill=NA)
gg = gg + geom_map(data = df_mw_scale, map = states_map,
                   aes(fill = capacity_gw, map_id = state_abbr),
                   color="#b3b3b3", size=0.15)
                   
gg + coord_map("albers", 23, 30) + theme_map()

