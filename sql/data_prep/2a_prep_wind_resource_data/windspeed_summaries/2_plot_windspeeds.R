library(RPostgreSQL)
library(dplyr)
library(ggplot2)
library(scales)

host = 'gispgdb.nrel.gov'
port = 5432
dbname = 'dav-gis'
user = 'mgleason'
password = 'mgleason'
schema = 'supply_chain'

# MAIN
################################################################################
# connect to postgres
con = dbConnect(dbDriver("PostgreSQL"), host = host, port = port, dbname = dbname,
                user = user, password = password)

sql = "SELECT 'ind'::TEXT as sector, height, windspeed_avg_ms
       FROM diffusion_data_wind.pt_grid_us_ind_windspeeds_hdf
       WHERE height IS NOT NULL;"
df_ind = dbGetQuery(con, sql)

sql = "SELECT 'com'::TEXT as sector, height, windspeed_avg_ms
       FROM diffusion_data_wind.pt_grid_us_com_windspeeds_hdf
       WHERE height IS NOT NULL;"
df_com = dbGetQuery(con, sql)

sql = "SELECT 'res'::TEXT as sector, height, windspeed_avg_ms
       FROM diffusion_data_wind.pt_grid_us_res_windspeeds_hdf
       WHERE height IS NOT NULL;"
df_res = dbGetQuery(con, sql)

df = rbind(df_ind, df_com, df_res)

ggplot(data = df_ind) +
  geom_histogram(aes(x = windspeed_avg_ms, y = ..count../sum(..count..))) +
  facet_grid(sector ~ height) +
  scale_y_continuous(labels = percent_format(), )


# clean up the histogram
# try boxplots
# other stats they may want?
  # the percent of customers above 5, above 5.5, above 6, and above 7
# boxplots
# percentiles?
# 

