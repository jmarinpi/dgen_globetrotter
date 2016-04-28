library(dplyr)
library(reshape2)
library(ggplot2)
library(RPostgreSQL)

# TODO: parameterize inputs and outputs to run on any data in the AASG BHT content model format

################################################################################################################################################
drv <- dbDriver("PostgreSQL")
# connect to postgres
con <- dbConnect(drv, host="gispgdb.nrel.gov", dbname="dav-gis", user="mgleason", password="mgleason")

sql = "SET ROLE 'dgeo-writers';"
dbSendQuery(con, sql)

################################################################################################################################################


df = read.csv('/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/egs_resource/data/source/SMU/core.template_borehole_materialized.csv', 
              stringsAsFactors = F)

# drop fields we won't need
fields = c('ObservationURI', 
           'WellName', 
           'APINo', 
           'HeaderURI', 
           'LatDegree', 
           'LongDegree', 
           'SRS', 
           'DrillerTotalDepth', 
           'LengthUnits', 
           'TrueVerticalDepth', 
           'MeasuredTemperature', 
           'CorrectedTemperature', 
           'TemperatureUnits', 
           'TimeSinceCirculation', 
           'DepthOfMeasurement', 
           'InformationSource'
)

df = df[, fields]

# convert field names to lower case
names(df) = tolower(names(df))

# replace any values of nil:missing
for (col in names(df)){
  if (class(df[, col]) == 'character'){
    df[df[, col] == 'nil:missing', col] = NA
  }
}

# check that SRS is always 4326 (WGS84)
if (unique(df$srs) != 'WGS84'){
  print('Multiple coordinate systems used')
  # TODO: Add code or logic to convert
}

# check that temperatures are always C
if (unique(df$temperatureunits) != 'C'){
  print('Multiple temperature units used') 
  # TODO: Add code to convert
}

# check that depths are always m
if (unique(df$lengthunits) != 'm'){
  print('Multiple depth units used') 
  # TODO: Add code to convert
}
# filter out values where depth of measurement is not provided
df = filter(df, !is.na(depthofmeasurement))

# fix them
harrison_corrected = df$measuredtemperature + (-16.512 + 0.0183 * df$depthofmeasurement - 2.43e-6 * df$depthofmeasurement**2)
# apply the corrections
rowstofix = which(is.na(df$correctedtemperature) & !is.na(df$measuredtemperature))
df[rowstofix, 'correctedtemperature'] = harrison_corrected[rowstofix]

fixed = length(rowstofix)
remaining_nulls = sum(is.na(df$correctedtemperature))
msg = sprintf('%s corrected temperatures derived from Harrison correction', fixed)
print(msg)
msg = sprintf('%s corrected temperatures are still missing and will be dropped', remaining_nulls)
print(msg)

df = filter(df, !is.na(correctedtemperature))

# change name of lat and long fields
names(df)[which(names(df) == 'latdegree')] = 'lat'
names(df)[which(names(df) == 'longdegree')] = 'lon'

# write to postgres
dbWriteTable(con, c('dgeo', 'bht_smu'), df, row.names = F, overwrite = T)


