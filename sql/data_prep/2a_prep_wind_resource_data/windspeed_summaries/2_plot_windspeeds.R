library(RPostgreSQL)
library(dplyr)
library(ggplot2)
library(scales)
library(reshape2)

host = 'gispgdb.nrel.gov'
port = 5432
dbname = 'dav-gis'
user = 'mgleason'
password = 'mgleason'
schema = 'supply_chain'

setwd('/Users/mgleason/NREL_Projects/github/diffusion/sql/data_prep/2a_prep_wind_resource_data/windspeed_summaries')

# MAIN
################################################################################
# connect to postgres
con = dbConnect(dbDriver("PostgreSQL"), host = host, port = port, dbname = dbname,
                user = user, password = password)

sql = "SELECT 'Industrial'::TEXT as sector, height, windspeed_avg_ms
FROM diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf
WHERE height IS NOT NULL;"
df_ind = dbGetQuery(con, sql)
df_ind$sector = 'INDUSTRIAL'


sql = "SELECT 'Commercial'::TEXT as sector, height, windspeed_avg_ms
FROM diffusion_data_wind.pt_grid_us_com_windspeeds_hdf
WHERE height IS NOT NULL;"
df_com = dbGetQuery(con, sql)
df_com$sector = 'COMMERCIAL'

sql = "SELECT 'Residential'::TEXT as sector, height, windspeed_avg_ms
FROM diffusion_data_wind.pt_grid_us_res_windspeeds_hdf
WHERE height IS NOT NULL;"
df_res = dbGetQuery(con, sql)
df_res$sector = 'RESIDENTIAL'

df = rbind(df_ind, df_com, df_res)

breaks = seq(0, 13, 0.5)
xbreaks = seq(1, 13, 1)


ggplot(data = df) +
  # this doesn't give correct results when using facets because sum(..count..) is over all facets
  geom_histogram(aes(x = windspeed_avg_ms, y = ..count../tapply(..count..,..PANEL..,sum)[..PANEL..]), 
                 breaks = breaks, fill = 'black') +   
  facet_grid(sector ~ height) +
  scale_y_continuous(labels = percent_format(), name = "Percentage") +
  scale_x_continuous(breaks = xbreaks, labels = xbreaks, name = "Windspeed (m/s)") +
  theme(strip.background = element_rect(fill = 'black')) +
  theme(strip.text = element_text(colour = 'white', face = 'bold')) 



# TODO:
# add conditional formatting to summary xlsx
# export and send to Robert, Ian, Trudy, and Ben

ggplot(data = df) +
  geom_boxplot(aes(x = as.factor(height), y = windspeed_avg_ms)) + 
  facet_grid(sector ~ .) +
  scale_y_continuous(breaks = xbreaks, labels = xbreaks, name = "Windspeed (m/s)") +
  scale_x_discrete(name = "Height (m)") +
  theme(strip.background = element_rect(fill = 'black')) +
  theme(strip.text = element_text(colour = 'white', face = 'bold')) 



# summary table
df$windspeed_class = cut(df$windspeed_avg_ms, breaks)

s = group_by(df, sector, height, windspeed_class) %>%
  summarize(count = sum(!is.na(windspeed_avg_ms)))
s2 = group_by(df, sector, height) %>%
  summarize(total = sum(!is.na(windspeed_avg_ms)))

s3 = merge(s, s2, by = c('sector', 'height'))

s3$pct = s3$count/s3$total

d = dcast(s3, sector + height ~ windspeed_class, value.var = 'pct' )
write.csv(d, 'summary.csv', 
          row.names = F)


# clean up the histogram
# try boxplots
# other stats they may want?
# the percent of customers above 5, above 5.5, above 6, and above 7
# boxplots
# percentiles?
# 


