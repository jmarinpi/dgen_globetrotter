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
library(jsonlite)
# library(tidyr)
library (dplyr,quietly = T)
library(grid)

setwd('/Users/mgleason/NREL_Projects/git_repos/diffusion/r')
source("../r/graphics/output_funcs.R")

########################################################################
# INPUT PAREMETERS
tech = 'wind'
schema = sprintf('diffusion_%s', tech)

out_res = 300
out_height = 5.8
out_width = 8
out_units = 'in'
out_folder = '/Users/mgleason/NREL_Projects/git_repos/diffusion/runs_wind/graphics'
########################################################################


########################################################################
# CONNECT TO POSTGRES
pg_params = fromJSON(txt = '../python/pg_params.json')

# two different connetions to postgres (1 used by RPostgreSQL and the other by dplyr)
con<-make_con(driver = "PostgreSQL", host = pg_params[['host']], pg_params[['dbname']], user = pg_params[['user']], password = pg_params[['password']], port = pg_params[['port']])
src = src_postgres(host = pg_params[['host']], dbname=pg_params[['dbname']], user = pg_params[['user']], password = pg_params[['password']], port = pg_params[['port']])

# lazy load the output table from postgres
sql = sprintf("SELECT *
                FROM %s.outputs_all",schema)
df = tbl(src,sql(sql))

# get the start year and end year for the model run
start_year = as.numeric(collect(summarise(df, min(year))))
end_year = as.numeric(collect(summarise(df, max(year))))
########################################################################


########################################################################
# PLOTTING PARAMETERS
year_colors = c('2020' = '#d0d1e6', '2030' = '#74a9cf', '2050' = '#034e7b')

custom_theme = theme(strip.text.x = element_text(size=14, face = 'bold')) +
  theme(plot.title = element_text(size=15, face = 'bold', vjust = 1)) +
  theme(axis.title.x = element_text(size=14, face = 'bold', vjust = -.5)) +
  theme(axis.title.y = element_text(size=14, face = 'bold', vjust = 1)) +
  theme(axis.text = element_text(size=12)) +  
  theme(legend.text = element_text(size = 12)) +
  theme(legend.title = element_text(size = 14)) +
  theme(legend.key.size = unit(1, 'cm')) +
  theme(legend.key = element_rect(colour = 'white', size = 2))
########################################################################


########################################################################
# Mean Payback Period by Capacity Factor
cf_payback_data = filter(df, metric == 'payback_period') %>%
  filter(year %in% c(2020, 2030, 2050)) %>%
  select(year, metric_value, naep) %>%
  group_by(year, cf = round(naep/8760, as.integer(2))) %>%
  summarise(avg_payback = mean(metric_value)) %>%
  # for best 5% of sites use this instead
#   summarise(avg_payback = r_quantile(array_agg(metric_value), .05)) %>%
  collect()

out_plot = sprintf('%s/payback_vs_cf.png', out_folder)
png(out_plot, width = out_width, height = out_height, units = out_units, res = out_res)
g = ggplot(cf_payback_data) +
  geom_point(aes(x = cf, y = avg_payback, color = factor(year))) +
  scale_y_continuous(name ='Average Payback Period (years)', lim = c(0,30)) +
  scale_x_continuous(name ='Capacity Factor') +
  theme_few() +
  scale_color_manual(name = 'Year', values = year_colors, labels = names(year_colors)) +
  ggtitle('Average Payback Period by Capacity Factor') +
  custom_theme
print(g)
dev.off()
########################################################################


########################################################################
# Capacity factor supply curve
cf_supplycurve_data =  select(df, year, naep, system_size_kw,customers_in_bin) %>%
  filter(year %in% c(2020, 2030, 2050)) %>%
  filter(naep >= 8760 * 0.1) %>%
  mutate(cf = naep/8760) %>%
  group_by(year) %>%
  arrange(desc(naep/8760)) %>%
  mutate(totload = cumsum(customers_in_bin * system_size_kw/(1e6))) %>%
  collect()

out_plot = sprintf('%s/installable_cap_vs_cf.png', out_folder)
png(out_plot, width = out_width, height = out_height, units = out_units, res = out_res)
g = ggplot(cf_supplycurve_data)+
  geom_line(aes(x = cf, y = totload, color = factor(year)))+
  scale_y_continuous(name ='Max Installable Capacity (GW)')+
  scale_x_continuous(name ='Annual Average Capacity Factor', labels = percent)+
  theme_few() +
  scale_color_manual(name = 'Year', values = year_colors, labels = names(year_colors)) +
  ggtitle('Maximum Installable Capacity by Wind Resource') +
  custom_theme
print(g)
dev.off()
########################################################################



########################################################################
# Payback vs INstallable Capacity
capacity_by_payback_data =  filter(df, metric == 'payback_period') %>%
  filter(year %in% c(2020, 2030, 2050)) %>%
  filter(metric_value <= 25) %>%
  select(year, metric_value, system_size_kw, customers_in_bin) %>%
  group_by(year) %>%
  arrange(metric_value) %>%
  mutate(totload = cumsum(customers_in_bin * system_size_kw/(1e6))) %>%
  collect()

out_plot = sprintf('%s/installable_cap_vs_payback.png', out_folder)
png(out_plot, width = out_width, height = out_height, units = out_units, res = out_res)
g = ggplot(capacity_by_payback_data) +
  geom_line(aes(x = metric_value, y = totload, color = factor(year)))+
  theme_few()+
  guides(size = FALSE)+
  scale_y_continuous(name ='Maximum Installable Capacity (GW)')+
  scale_x_continuous(name ='Payback Period (years)')+
  theme_few() +
  scale_color_manual(name = 'Year', values = year_colors, labels = names(year_colors)) +
  ggtitle('Maximum Installable Capacity by Economics') +
  custom_theme  
print(g)
dev.off()
########################################################################


