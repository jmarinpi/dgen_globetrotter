library(dplyr)
library(reshape2)
library(ggplot2)
library(RPostgreSQL)

################################################################################################################################################
drv <- dbDriver("PostgreSQL")
# connect to postgres
con <- dbConnect(drv, host="gispgdb.nrel.gov", dbname="dav-gis", user="mgleason", password="mgleason")

sql = "SET ROLE 'dgeo-writers';"
dbSendQuery(con, sql)

################################################################################################################################################


df = read.csv('/Volumes/Staff/mgleason/dGeo/Data/Source_Data/BHT_Data/AASG_BHT_Data_Compilation_from_kmccabe/well_data_cleaned_02.csv', 
              stringsAsFactors = F)

# convert field names to lower case
names(df) = tolower(names(df))

# change name of lat and long fields
names(df)[which(names(df) == 'latdegree')] = 'lat'
names(df)[which(names(df) == 'longdegree')] = 'lon'

# write to postgres
dbWriteTable(con, c('dgeo', 'bht_compilation'), df, row.names = F, overwrite = T)


