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
# consider removing outlier high T wells?
# look into why there is a systematic relationship of obs vs residuals in CV results for RK?
  # ( best guess is taht these two things are related )

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

# # so, only apply g to extrapolate temps within 500 m intervals
# # tz = t - g(z - depth_m)
# df$t500 = df$t - df$g * (df$depth_m - 500)
# df$t1000 = df$t - df$g * (df$depth_m - 1000)
# df$t1500 = df$t - df$g * (df$depth_m - 1500)
# df$t2000 = df$t - df$g * (df$depth_m - 2000)
# df$t2500 = df$t - df$g * (df$depth_m - 2500)
# df$t3000 = df$t - df$g * (df$depth_m - 3000)

##########################################################################################
# METHOD 1: 3-D ORDINARY KRIGING
sf = df
sf$z = sf$depth_m * -1
coordinates(sf) = ~ x + y + z

# build exp. variogram
# go out to 1/2 distance of entire field (= 800 km)
v_xyz = variogram(t ~ 1, data = sf, width = 5000, cutoff = 800000, beta = 270)
plot(v_xyz)
# fit a variogram model by eye
vm_xyz <- vgm(psill = 500, model = "Exc", range = 200000, nugget = 100, kappa = 0.7)
# fit the model
vm_xyz <- fit.variogram(v_xyz, vm_xyz)
plot(v_xyz, model = vm_xyz)

# cross validate the model
cvk_ok <- krige.cv(t ~ 1, sf, model = vm_xyz, nfold = 1000, nmax = 100)
res_ok <- as.data.frame(cvk_ok)$residual
sqrt(mean(res_ok**2)) #  14.41895
mean(res_ok) # -0.07111871
mean(res_ok^2/as.data.frame(cvk_ok)$var1.var) # 1.050807
sd(res_ok)*2 #28.83776

# is there a systematic relationship between observer t and residuals?
plot(cvk_ok@data$residual ~ cvk_ok@data$obs) #yes, a little bit -- trends higher with higher temp wells
# how about with depth?
plot(cvk_ok@data$residual ~ cvk_ok@coords[, 3]) # yes, a little bit ... more underprediction at shallow depths and overprediction at deeper depths
# overall, this suggests the model isn't ideal

# nonetheless:
# make predictions for one depth: 1500 m
sgrid = grid
sgrid$z = 1500
coordinates(sgrid) = ~ x + y + z
# run the kriging operation
k_ok = krige(t ~ 1, locations = sf, newdata = sgrid, model = vm_xyz, nmax = 100)
summary(k_ok)

# extract the predictions and variances
results_ok = k_ok@data
names(results_ok)[1:2] = c('est', 'var')
results_ok$x = k_ok@coords[,1]
results_ok$y = k_ok@coords[,2]
max(results_ok$var, na.rm= T) # 1312.869
results_ok$ci95 = sqrt(results_ok$var) * 2
summary(results_ok$ci95) 
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 27.56   35.21   44.17   44.45   52.53   72.47 
hist(results_ok$ci95) # most values will be in +/- 30-60 deg

# map the results
ggplot(data = results_ok) +
  geom_point(aes(x = x, y = y, colour = est)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks = seq(0, 200, 25))
# decent amount of spatial patterning

# map the results
ggplot(data = results_ok) +
  geom_point(aes(x = x, y = y, colour = ci95)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks =  seq(25, 75, 5))
# not very good -- some massive errors where we have no data, and even where we have data results are +/- 30-35 degrees

##########################################################################################
# METHOD 2: 2-D UNIVERSAL KRIGING (controlling for depth and t35km)
sf = df
sf$z = sf$depth_m * -1
coordinates(sf) = ~ x + y

# build exp. variogram
# go out to 1/2 distance of entire field (= 800 km)
v_uk = variogram(t ~ t35km + depth_m, data = sf, width = 5000, cutoff = 800000)
plot(v_uk)
# fit a variogram model by eye
vm_uk <- vgm(psill = 150, model = "Exc", range = 100000, nugget = 90, kappa = 0.7)
plot(v_uk, model = vm_uk)
# fit the model 
vm_uk <- fit.variogram(v_uk, vm_uk)
plot(v_uk, model = vm_uk)
attributes(vm_uk)$SSErr
# 3.606715

# CAN't get this to run right now... "singular matrix error
# cross validate the model
cvk_uk <- krige.cv(t ~ t35km + depth_m, sf, model = vm_uk, nfold = 2, nmax = 200)
res_uk <- as.data.frame(cvk_uk)$residual
sqrt(mean(res_uk**2)) 
mean(res_uk) 
mean(res_uk^2/as.data.frame(cvk_uk)$var1.var) 
sd(res_uk)
# is there a systematic relationship between observer t and residuals?
plot(cvk_uk@data$residual ~ cvk_uk@data$obs) 
# how about with depth?
plot(cvk_uk@data$residual ~ cvk_uk@coords[, 3]) 

# can't get predictions to work either
# make predictions for one depth: 1500 m
sgrid = grid
sgrid$depth_m = 1500
coordinates(sgrid) = ~ x + y
# run the kriging operation
k_uk = krige(t ~ t35km + depth_m, locations = sf, newdata = sgrid, model = vm_uk, nmax = 100)
summary(k_uk)

# extract the predictions and variances
results_uk = k_uk@data
names(results_uk)[1:2] = c('est', 'var')
results_uk$x = k_uk@coords[,1]
results_uk$y = k_uk@coords[,2]
max(results_uk$var, na.rm= T) 
results_uk$ci95 = sqrt(results_uk$var) * 2
summary(results_uk$ci95) 
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 

hist(results_uk$ci95) # most values will be in +/-

# map the results
ggplot(data = results_uk) +
  geom_point(aes(x = x, y = y, colour = est)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks = seq(0, 200, 25))

# map the results
ggplot(data = results_uk) +
  geom_point(aes(x = x, y = y, colour = ci95)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks =  seq(25, 75, 5))


##########################################################################################
# METHOD 3: Regression Kriging (by depth and t@3.5km)
# create regression model
m = lm(t ~ t35km + depth_m, data = df)
# do not transform t35km -- even though it make it more normal, it doenst approve the prediction

summary(m) # R2 = 0.8385
mean(m$residuals) # ~0
mean(m$residuals**2)**.5 # 14.8
hist(m$residuals)
min(m$residuals) # -91
max(m$residuals) # 231.1028
sd(m$residuals)*2 # 29.6
summary(m$residuals)
# Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
# -91.7000  -8.0990  -0.7514   0.0000   7.7000 231.1000 
# is there any relationship between depth and resid?
plot(m$residuals ~ m$model$depth_m)
# nope
cor(m$residuals, m$model$depth_m) # effectivel yzero correlation

# how about t35km and residuals?
plot(m$residuals ~ m$model$t35km)
cor(m$residuals, m$model$t35km)
# zero correlation, but def some anomalies where t35 km is high

# how about observed t and actual t
plot(m$residuals ~ m$model$t)
# yes -- this is due to high outlier temps mostly
boxplot(df$t) # everything above 200 degrees
# there are only a handful of these in the country
outliers = filter(df, t > 200)
ggplot(data = outliers) + geom_point(aes(x = x, y = y)) 

df$resid = m$residuals
df$t_pred = m$fitted.values

# two additional checks
# create boxplot of residuals and temp by slice
ggplot(data = df) +
  geom_boxplot(aes(x = z_slice, y = resid))
# definitely not much of a relationship here
# boxplot of predictions by slice
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


# KRIGING
sf = df
coordinates(sf) = ~ x + y

# experimental variogram 
v_rk = variogram(resid ~ 1, data = sf, width = 5000, cutoff = 800000)
plot(v_rk)
# estimate variogram model form and parameters by eye
vm_rk <- vgm(psill = 200, model = "Exc", range = 100000, nugget = 90)
plot(v_rk, model = vm_rk)
# fit the model 
vm_rk <- fit.variogram(v_rk, vm_rk)
plot(v_rk, model = vm_rk)
vm_rk
# model     psill    range kappa
# 1   Nug  69.78407     0.00   0.0
# 2   Exc 137.50671 56291.09   0.5

# run cross validation
cvk_rk <- krige.cv(resid ~ 1, sf, model = vm_rk, nfold = 1000, nmax = 100)
summary(cvk_rk)
res_rk <- as.data.frame(cvk_rk)$residual
sqrt(mean(res_rk^2)) # 9.816535 (origninal LM had rmse of about 15)
mean(res_rk) # -0.009614156
mean(res_rk^2/as.data.frame(cvk_rk)$var1.var) # 0.968001
sd(res_rk)*2 # 19.63321 (the original LM had sd*2 of about 30)
# results are much better than ok
# is there a systematic relationship between observer t and residuals?
plot(cvk_rk@data$residual ~ cvk_rk@data$observed)  # yes, not ideal  -- undepredictions common for positive resids, overpredictions common for negative resids
cor(cvk_rk@data$residual,cvk_rk@data$obs ) # pretty strong - 0.67
  # TODO: look into why this is happening
# how about with depth?
cvk_rk_data = cvk_rk@data
cvk_rk_data$x = cvk_rk@coords[, 1]
cvk_rk_data$y = cvk_rk@coords[, 2]
dfcv = merge(cvk_rk_data, df, by = c('x', 'y'))
plot(dfcv$residual ~ dfcv$depth_m) 
# no relationship with depth -- this is an improvement over ok


# run the kriging operation
sgrid = grid
sgrid$depth_m = 1500
coordinates(sgrid) = ~ x + y
k_rk = krige(resid ~ 1, locations = sf, newdata = sgrid, model = vm_rk, nmax = 100)

# map the results
results_rk = k_rk@data
names(results_rk)[1:2] = c('est', 'var')
results_rk$x = k_rk@coords[,1]
results_rk$y = k_rk@coords[,2]
max(results_rk$var, na.rm= T) # 261
results_rk$ci95 = sqrt(results_rk$var) * 2
summary(results_rk$ci95) 
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 18.13   23.23   25.70   25.33   27.37   32.33 
hist(results_rk$ci95) # most values will be in +/- 20-30 deg

# to map the results, apply the lm to the grid and then combine
sgrid$lm_t = predict.lm(m, as.data.frame(sgrid))
results_rk = merge(results_rk, sgrid, by = c('x', 'y'))
results_rk$resid_est = results_rk$est
results_rk$est = results_rk$lm_t + results_rk$resid_est

# map the results
ggplot(data = results_rk) +
  geom_point(aes(x = x, y = y, colour = est)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks = seq(0, 200, 25))
# more subtleties than ok, reproduces known hot spots better

# map the results
ggplot(data = results_rk) +
  geom_point(aes(x = x, y = y, colour = ci95)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks =  seq(0, 33, 3))
# big improvement 





