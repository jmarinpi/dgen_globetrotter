library(dplyr)
library(reshape2)
library(ggplot2)
library(RPostgreSQL)
library(gstat)
library(sp)

################################################################################################################################################
drv <- dbDriver("PostgreSQL")
# connect to postgres
con <- dbConnect(drv, host="gispgdb.nrel.gov", dbname="dav-gis", user="mgleason", password="mgleason")

sql = "SET ROLE 'dgeo-writers';"
dbSendQuery(con, sql)

################################################################################################################################################

# TODO:
# Try a better linear model for relating to t35km


sql = "SELECT x_96703 as x, y_96703 as y, 
t35km, 
depthofmeasurement as depth_m, 
correctedtemperature as t  
FROM dgeo.bht_smu
WHERE depthofmeasurement >= 304.8 and depthofmeasurement <= 3250
AND t35km IS NOT NULL;"
df = dbGetQuery(con, sql)

sql = "SELECT gid, x_96703 as x, y_96703 as y,  
t35km
FROM dgeo.egs_empty_grid"
grid = dbGetQuery(con, sql)




################################################################################################
# preprocessing 

# 1 - "jitter" all points by a random amount in the range of +/- 1 m  (draw from uniform random sample)
# this will fix points with identical coordinates (identical coordinates will break the kriging algorithm)
set.seed(1)
df$x2 = df$x + runif(nrow(df), -1, 1)
df$y2 = df$y + runif(nrow(df), -1, 1)
# cehck results
hist(df$x2 - df$x)
hist(df$y2 - df$y)
# looks perfect

# check there are no remaining dupes
# find locs with multiple rows
dupes = group_by(df, x2, y2) %>%
  summarize(
    count = sum(!is.na(x2))
  ) %>%
  filter(count > 1) %>%
  as.data.frame()
nrow(dupes)
# 0 -- all set

# drop the x and y columns(so as to not mistakenly use them later)
df = df[, c('x2', 'y2', 't35km', 'depth_m', 't')]
# rename x2 and y2
names(df)[1:2] = c('x', 'y')

# 2 - Cut the data into depth "slices" (500 m intervals surrounding 500, 1000, 1500, 2000, 2500, and 3000 m)
labels = seq(500, 3000, 500)
# 500 1000 1500 2000 2500 3000
breaks = c(0, 750, 1250, 1750, 2250, 2750, 3250)
# 0  750 1250 1750 2250 2750 3250
df$z_slice = cut(df$depth_m, breaks = breaks, labels = labels)


# 3-  Calculate gradients from T@3.5km
df$g = (df$t35km - df$t) / (3500 - df$depth_m)
# inspect the results
summary(df$g)
# Min.   1st Qu.    Median      Mean   3rd Qu.      Max. 
# -0.656700  0.001008  0.014520  0.006584  0.020210  0.237000 
hist(df$g)

# does g vary spatially?
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour = g)) +
  scale_colour_distiller(palette = 'Spectral', breaks =  c(-1, -.5, seq(0, 0.25, 0.05)))
# generally speaking, not a lot of obvious spatial variation here

# does g vary with the depth of the well?
ggplot(data = df) +
  geom_point(aes(x = depth_m, y = g))
# pretty constant over 300-2000 m, but then much more variance and drift between 2-3 km

# can also see this in a boxplot
ggplot(data = df) +
  geom_boxplot(aes(x = z_slice, y = g))
# drift is towards a NEGATIVE gradient as you approach 3000 km

# so, only apply g to extrapolate temps within 500 m intervals
# tz = t - g(z - depth_m)
df$t500 = df$t - df$g * (df$depth_m - 500)
df$t1000 = df$t - df$g * (df$depth_m - 1000)
df$t1500 = df$t - df$g * (df$depth_m - 1500)
df$t2000 = df$t - df$g * (df$depth_m - 2000)
df$t2500 = df$t - df$g * (df$depth_m - 2500)
df$t3000 = df$t - df$g * (df$depth_m - 3000)

##########################################################################################
# create a regression model based on depth_m and t35km

m = lm(t ~ t35km + depth_m, data = df)
# do not transform t35km -- even though it make it more normal, it doenst approve the prediction
summary(m) # R2 = 0.8385
mean(m$residuals) # ~0
mean(m$residuals**2)**.5 # 14.8
hist(m$residuals)
min(m$residuals) # -95
max(m$residuals) # 231.1028
sd(m$residuals)*2 # 29.6
summary(m$residuals)
# Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
# -91.7000  -8.0990  -0.7514   0.0000   7.7000 231.1000 
# is there any relationship between depth and resid?
plot(m$residuals ~ m$model$depth_m)
# no, not really

# how about t35km and residuals?
plot(m$residuals ~ m$model$t35km)
# not a systematica relatinoship, but def some anomalies where t35 km is high

df$resid = m$residuals
df$t_pred = m$fitted.values

# create boxplot of residuals and temp by slice
ggplot(data = df) +
  geom_boxplot(aes(x = z_slice, y = resid))
# definitely not much of a relationship here

ggplot(data = df) +
  geom_boxplot(aes(x = z_slice, y = t_pred))
# definitely a strong relationship here (as expected)

# map the results 
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour = t_pred), size = 1.1) +
  scale_colour_distiller(palette = 'YlOrRd', breaks = seq(0, 200, 25)) +
  facet_wrap(~z_slice)

# is there a spatial pattern in the residuals?
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour = resid), size = 1.1) +
  scale_colour_distiller(palette = 'Spectral', breaks = seq(-100, 250, 25)) +
  facet_wrap(~z_slice)

# there are definitely patterns, not sure how they will affect stationarity of the procss
# 
# #
# # save to csv
# write.csv(dfg, '/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/egs_resource/data/analysis/well_lm_residuals.csv', row.names = F)
# # there is definitely a spatial pattern. is any of it due to depth?

# KRIGING
# convert empty grid to spatial grid
sgrid = grid
coordinates(sgrid) = ~ x + y

sf = df
coordinates(sf) = ~ x + y

# experimental variogram 
ve = variogram(resid ~ 1, data = sf, width = 5000, cutoff = 800000)
plot(ve)

# estimate variogram model form and parameters by eye
vtme <- vgm(psill = 200, model = "Exc", range = 20000, nugget = 90)
plot(ve, model = vtme)
# fit the model 
vtmf <- fit.variogram(ve, vtme)
plot(ve, model = vtmf)
vtmf
# model     psill    range kappa
# 1   Nug  69.77877     0.00   0.0
# 2   Exc 137.49414 56258.62   0.5

# run cross validation
cvk <- krige.cv(resid ~ 1, sf, model = vtmf, nfold = 1000, nmax = 100)
summary(cvk)
res <- as.data.frame(cvk)$residual
sqrt(mean(res^2)) #  9.811998
mean(res) # -0.01018456
mean(res^2/as.data.frame(cvk)$var1.var) # 0.9673841
sd(res)*2 # 19.62414
# results are comparble to but better than ok

# run the kriging operation
sgrid = grid
sgrid$depth_m = 1500
coordinates(sgrid) = ~ x + y
k1500 = krige(resid ~ 1, locations = sf, newdata = sgrid, model = vtmf, nmax = 100)

# map the results
rkresults = k1500@data
names(rkresults)[1:2] = c('est', 'var')
rkresults$x = k1500@coords[,1]
rkresults$y = k1500@coords[,2]
max(rkresults$var, na.rm= T) # 261
rkresults$ci95 = sqrt(rkresults$var) * 2
summary(rkresults$ci95) 
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 18.13   23.23   25.70   25.33   27.37   32.33 
hist(rkresults$ci95) # most values will be in +/- 20-30 deg

# map the results
ggplot(data = rkresults) +
  geom_point(aes(x = x, y = y, colour = est)) +
  scale_colour_distiller(palette = 'YlOrRd')

# map the results
ggplot(data = rkresults) +
  geom_point(aes(x = x, y = y, colour = ci95)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks =  seq(0, 33, 3))

