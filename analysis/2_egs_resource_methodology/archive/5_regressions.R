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

sql = "SELECT lat, lon, t35km, depthofmeasurement as depth_m, correctedtemperature as t_c  
      FROM dgeo.bht_smu
      WHERE depthofmeasurement > 300 and depthofmeasurement < 3000;"
df = dbGetQuery(con, sql)

# look at the data spatially, look for values with no t35
ggplot(data = df) +
  geom_point(aes(x = lon, y = lat, colour = is.na(t35km)))
# all offshore -- all set

# filter out the values without t35km
df = filter(df, !is.na(t35km))

# plot the relationship between t35 and tc
ggplot(data = df) +
   geom_point(aes(x = t35km, y = t_c, colour = depth_m))

# break the data up into depth classes
breaks = seq(0, 3000, 500)
df$slice = cut(df$depth_m, breaks, labels = breaks[2:length(breaks)])


# plot the relationship between t35 and tc
ggplot(data = df) +
  geom_point(aes(x = t35km, y = t_c, colour = depth_m)) +
  facet_wrap(~slice, scales = 'free')

# plot the histograms
ggplot(data = df) +
  geom_histogram(aes(x = t_c)) +
  facet_wrap(~slice, scales = 'free')

# plot the histograms
ggplot(data = df) +
  geom_histogram(aes(x = t35km)) +
  facet_wrap(~slice)
# reasonable symmetric in both cases, prob not essential to transform

df_list = list()
for (s in unique(df$slice)){
  df_list[[as.character(s)]] = filter(df, slice == s)
}

# test a regression
m = lm(t_c ~ t35km + slice, df)
summary(m)
# all vars vary significant
# Residual standard error: 15.15 on 58046 degrees of freedom
# Multiple R-squared:  0.7961,  Adjusted R-squared:  0.7961 
# F-statistic: 3.778e+04 on 6 and 58046 DF,  p-value: < 2.2e-16
plot(m$residuals)
hist(m$residuals)
qqnorm(m$residuals)
# a little screwy at the ends, but mostly good

# map the residuals
resids_df = df[, c('lat', 'lon')]
resids_df$resid = m$residuals
ggplot(data = resids_df) +
  geom_point(aes(x = lon, y = lat, colour = resid))
# TODO: classify the color scheme

# apply the predicted values and resids to each value in the df
df$ols_t_c = m$fitted.values
df$ols_resid = m$residuals

# plot the comparison of fitted to acutal values
ggplot(data = df) +
  geom_point(aes(x = t_c, y = ols_t_c, colour = depth_m))
# errors appear to be a function of very high original t_c rather than depth
ggplot(data = df) +
  geom_point(aes(x = t_c, y = ols_t_c, colour = slice))




df500 = filter(df, slice == 500)
df1000 = filter(df, slice == 1000)
df1500 = filter(df, slice == 1500)
df2000 = filter(df, slice == 2000)
df2500 = filter(df, slice == 2500)
df3000 = filter(df, slice == 3000)

