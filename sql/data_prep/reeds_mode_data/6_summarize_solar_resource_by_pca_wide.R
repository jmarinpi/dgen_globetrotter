library(RPostgreSQL)
library(reshape2)


#############################################
# CONNECT TO POSTGRES
drv = dbDriver("PostgreSQL") 
conn = dbConnect(drv, host='gispgdb', port=5432, dbname='dgen_db', user='mgleason', password='mgleason') 
#############################################

sql = "SELECT *
       FROM diffusion_solar.reeds_solar_resource_by_pca_summary_tidy
       WHERE solar_re_9809 = 2000"
df = dbGetQuery(conn, sql)


df_wide = dcast(df, cf_avg ~ pca_reg + npoints + tilt + azimuth)


dbWriteTable(conn, c('diffusion_solar', 'reeds_solar_resource_by_pca_summary_wide'), df_wide, row.names = F)