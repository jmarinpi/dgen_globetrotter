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
sgrid = grid
coordinates(sgrid) = ~ x + y
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
sf = df
sf$z = sf$depth_m * -1
coordinates(sf) = ~ x + y + z

# anisotropic 

v_xy = variogram(t ~ 1, data = sf, width = 5000, cutoff = 800000, beta = 0)
v_x = variogram(t ~ 1, data = sf, width = 5000, cutoff = 800000, alpha = 90, tol.hor = 45)
v_y = variogram(t ~ 1, data = sf, width = 5000, cutoff = 800000, alpha = 0, tol.hor = 45)
plot(v_xy)
plot(v_x)
plot(v_y)

v_xyz = variogram(t ~ 1, data = sf, width = 5000, cutoff = 800000, beta = 270)
plot(v_xyz)

v_uni = variogram(t ~ t35km + depth_m, data = sf, width = 5000, cutoff = 800000, beta = 270)
plot(v_uni)

# fit a variogram model
vm_uni <- vgm(psill = 150, model = "Exc", range = 10000, nugget = 90, kappa = 0.7)
plot(v_uni, model = vm_uni)
# fit the model 
vm_uni <- fit.variogram(v_uni, vm_uni)
plot(v_uni, model = vm_uni)

cvk <- krige.cv(t ~ 1, sf, model = vm_uni, nfold = 1000, nmax = 100)
res <- as.data.frame(cvk)$residual
sqrt(mean(res^2)) #  15.16329
mean(res) # -0.01412533
mean(res^2/as.data.frame(cvk)$var1.var) # 1.158995
sd(res) # 12.4033

plot(cvk@data$var1.pred ~ cvk@data$obs)
plot(cvk@data$residual ~ cvk@coords[, 3])

# make predictions for 2000 m
sgrid = grid
sgrid$z = 2000
coordinates(sgrid) = ~ x + y + z
# run the kriging operation
k = krige(t ~ 1, locations = sf, newdata = sgrid, model = vm_uni, nmax = 100)
summary(k)


# map the results
kresults = k@data
names(kresults)[1:2] = c('est', 'var')
kresults$x = k@coords[,1]
kresults$y = k@coords[,2]
max(kresults$var, na.rm= T) # 259
kresults$ci95 = sqrt(kresults$var) * 2
summary(kresults$ci95) 
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 20.29   23.29   25.91   25.44   27.48   32.21 
hist(kresults$ci95) # most values will be in +/- 30 deg

# map the results
ggplot(data = kresults) +
  geom_point(aes(x = x, y = y, colour = est)) +
  scale_colour_distiller(palette = 'YlOrRd')
# not a whole lot of spatial patterning is picked up 

# map the results
ggplot(data = kresults) +
  geom_point(aes(x = x, y = y, colour = ci95)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks =  seq(0, 33, 3))
# pretty poor -- even where we have data, CI is +/0- 20 degrees





# make predictions for 1500 m
sgrid = grid
sgrid$z = 1500
coordinates(sgrid) = ~ x + y + z
# run the kriging operation
k = krige(t ~ 1, locations = sf, newdata = sgrid, model = vm_uni, nmax = 100)
summary(k)


# map the results
kresults = k@data
names(kresults)[1:2] = c('est', 'var')
kresults$x = k@coords[,1]
kresults$y = k@coords[,2]
max(kresults$var, na.rm= T) # 259
kresults$ci95 = sqrt(kresults$var) * 2
summary(kresults$ci95) 
# TRYING TO BEAT:
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 20.06   23.26   25.91   25.42   27.48   32.21 
hist(kresults$ci95) # most values will be in +/- 30 deg

# map the results
ggplot(data = kresults) +
  geom_point(aes(x = x, y = y, colour = est)) +
  scale_colour_distiller(palette = 'YlOrRd')

# map the results
ggplot(data = kresults) +
  geom_point(aes(x = x, y = y, colour = ci95)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks =  seq(0, 33, 3))
# pretty poor -- even where we have data, CI is +/0- 20 degrees

##########################################################################################
# MODELING - slice = 2000
# extract out a z slice
df2000 = filter(df, z_slice == 2000)
# we should not see a relationship between depth and temp anymore
ggplot(df2000) +
  geom_point(aes(x = depth_m, y = t500))

# convert data frame to spatial data frame
df2000$t = df2000$t2000
sf2000 = df2000
# convert to coordinates
coordinates(sf2000) = ~ x + y

# variogram can only run out to 1/2 the distance of the entire field
# to find that distance, run a quick test variogram
ve = variogram(t ~ 1, data = sf2000, width = 10000)
max(ve$dist)/2
# 805118.3

# calculate experimental variogram
ve = variogram(t ~ 1, data = sf2000, width = 1000, cutoff = 800000)
plot(ve)
var(df2000$t)

# now the sill seems to occur in the range of 200k - 400 k
ve = variogram(t ~ 1, data = sf2000, width = 1000, cutoff = 200000)
plot(ve)

ve = variogram(t ~ 1, data = sf2000, width = 1000, cutoff = 400000)
plot(ve)

ve = variogram(t ~ 1, data = sf2000, width = 1000, cutoff = 600000)
plot(ve)
# sill is more distinctive at 200 km, so use that as cutoff -- at 400km, the trend pattern is more evident

# how to set the bin widths -- try 200 and 500 m
ve200 = variogram(t ~ 1, data = sf2000, width = 200, cutoff = 200000)
plot(ve200)
# is this width appropriate?
summary(ve200$np) # at least 1771 point pairs in each bin, average of 12,600 in each bin

# this is too far, the important relationship is < 200km
ve500 = variogram(t ~ 1, data = sf2000, width = 500, cutoff = 200000)
plot(ve500)
# is this width appropriate?
summary(ve500$np) # at least 5417 point pairs in each bin, average of 30,000 in each bin

# fit a variogram for each
# estimate variogram model form and parameters by eye
vtme200 <- vgm(psill = 500, model = "Exc", range = 275000, nugget = 75, kappa = 0.5)
plot(ve200, model = vtme200)
# fit the model 
vtmf200 <- fit.variogram(ve200, vtme200)
plot(ve200, model = vtmf200)
# how well does it fit?
attributes(vtmf200)$SSErr
# 9.639252

# estimate variogram model form and parameters by eye
vtme500 <- vgm(psill = 300, model = "Exc", range = 70000, nugget = 60, kappa = 0.5)
plot(ve500, model = vtme500)
# fit the model 
vtmf500 <- fit.variogram(ve500, vtme500)
plot(ve500, model = vtmf200)
# how well does it fit?
attributes(vtmf500)$SSErr
#3.089081

# test which one is better using cross validation
# run cross validation
cvk200 <- krige.cv(t ~ 1, sf2000, model = vtmf200, nfold = 1000, nmax = 100)
res <- as.data.frame(cvk200)$residual
sqrt(mean(res^2)) # 12.44728
mean(res) # -0.0504372
mean(res^2/as.data.frame(cvk200)$var1.var) # 1.163612
sd(res)*2 # 24.89535 (95% CI)

cvk500 <- krige.cv(t ~ 1, sf2000, model = vtmf500, nfold = 1000, nmax = 100)
res <- as.data.frame(cvk500)$residual
sqrt(mean(res^2)) #  12.40562
mean(res) # -0.01412533
mean(res^2/as.data.frame(cvk200)$var1.var) # 1.158995
sd(res) # 12.4033

# in general, the two models are very similar
# but the 500 m one is slightly better

# now interpet it
vtmf500
# model     psill   range kappa
# 1   Nug  59.20112     0.0   0.0
# 2   Exc 268.19786 36109.5   0.5
# interpretation (http://www.statios.com/Resources/04-variogram.pdf)
# http://faculty.washington.edu/edford/Variogram.pdf
# http://people.ku.edu/~gbohling/cpe940/Variograms.pdf
# "the variogram for lag distance h is defined as the average squared difference 
# of values separated approximately by h"
# "nugget effect = sum of geological microstructure and measurement error"
  # "sparse data may also lead to a higher than expected nugget effect"
# at points spaced at distances >= range, the relationship between all points is effectively uniform 
# i.e., there is no reason to assume points spaced at 40km are more related than points spaced at 80 km
# so effectively what you are doing at that distance is simply averaging the data, and assuming a confidence
# interval of the variance of the estimates

sqrt(vtmf500$psill[1])# averaged difference at 0 m is 7.694226 degrees
vtmf500$range[2] # 36109.5 # data are no longer correlated beyond this distance
sqrt(vtmf500$psill[2])# average difference at >72 km is 16.37675 degrees

# run the kriging operation
k2000 = krige(t ~ 1, locations = sf2000, newdata = sgrid, model = vtmf500, nmax = 100)
summary(k2000)

# map the results
kresults2000 = k2000@data
names(kresults2000)[1:2] = c('est', 'var')
kresults2000$x = k2000@coords[,1]
kresults2000$y = k2000@coords[,2]
max(kresults2000$var, na.rm= T) # 603
kresults2000$ci95 = sqrt(kresults2000$var) * 2
summary(kresults2000$ci95) 
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 19.09   34.66   36.26   35.22   36.78   40.46 
hist(sqrt(kresults2000$var)*2) # most values will be in +/- 40 degrees

# map the results
ggplot(data = kresults2000) +
  geom_point(aes(x = x, y = y, colour = est)) +
  scale_colour_distiller(palette = 'YlOrRd')
# not a whole lot of spatial patterning is picked up 

# map the results
ggplot(data = kresults2000) +
  geom_point(aes(x = x, y = y, colour = ci95)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks =  seq(0, 45, 5))
# pretty poor -- even where we have data, CI is +/0- 20 degrees


##########################################################################################



##########################################################################################
# MODELING - slice = 500
# extract out a z slice
df500 = filter(df, z_slice == 500)
# we should not see a relationship between depth and temp anymore
ggplot(df500) +
  geom_point(aes(x = depth_m, y = t))
# there still is a relationship...
# how strong
m = lm(t ~ depth_m, data = df500)
summary(m) # very weak -- linear relationship is not significant at p0.05

# convert data frame to spatial data frame
sf500 = df500
# convert to coordinates
coordinates(sf500) = ~ x + y

# variogram can only run out to 1/2 the distance of the entire field
# to find that distance, run a quick test variogram
ve = variogram(t ~ 1, data = sf500, width = 10000)
max(ve$dist)/2
# 861756.1

# calculate experimental variogram
ve = variogram(t ~ 1, data = sf500, width = 1000, cutoff = 800000)
plot(ve$gamma ~ ve$dist, pch = 20)
abline(h = var(df500$t)) # 287.4688
# no trend -- we don't need to go out nearly this far

ve = variogram(t ~ 1, data = sf500, width = 1000, cutoff = 200000)
plot(ve)

ve = variogram(t ~ 1, data = sf500, width = 1000, cutoff = 100000)
plot(ve)

ve = variogram(t ~ 1, data = sf500, width = 1000, cutoff = 20000)
plot(ve)
# sill is  distinctive at 20 km, so use that as cutoff

# how to set the bin widths -- try 200 and 500 m
ve200 = variogram(t ~ 1, data = sf500, width = 200, cutoff = 20000)
plot(ve200)
# is this width appropriate?
summary(ve200$np) # at least 579 point pairs in each bin, average of 1491 in each bin

ve500 = variogram(t ~ 1, data = sf500, width = 500, cutoff = 20000)
plot(ve500)
# is this width appropriate?
summary(ve500$np) # at least 1729 point pairs in each bin, average of 3727 in each bin

# fit a variogram for each
# estimate variogram model form and parameters by eye
vtme200 <- vgm(psill = 25, model = "Sph", range = 10000, nugget = 20)
plot(ve200, model = vtme200)
# fit the model 
vtmf200 <- fit.variogram(ve200, vtme200)
# cannot fit -- it produces a singular model because of weirdness in the first two lags

# estimate variogram model form and parameters by eye
vtme500 <- vgm(psill = 25, model = "Sph", range = 10000, nugget = 20)
plot(ve500, model = vtme500)
# fit the model 
vtmf500 <- fit.variogram(ve500, vtme500)
plot(ve500, model = vtmf500)
# how well does it fit?
attributes(vtmf500)$SSErr
# 0.08677552

# run cross validation
cvk500 <- krige.cv(t ~ 1, sf500, model = vtmf500, nfold = 1000, nmax = 100)
res <- as.data.frame(cvk500)$residual
sqrt(mean(res^2)) #  11.3179
mean(res) # -0.6141803
mean(res^2/as.data.frame(cvk500)$var1.var) # 3.590456
sd(res) # 11.30274
# results aren't great -- there is some bias but precision is good

# now interpet it
vtmf500
# model     psill    range kappa
# model    psill    range
# 1   Nug 16.05520    0.000
# 2   Sph 25.93329 7604.573
sqrt(vtmf500$psill[1])# averaged difference at 0 m is 4 degrees
vtmf500$range[2] # 7604 # data are no longer correlated beyond this distance
sqrt(vtmf500$psill[2])# average difference at >7604 m is 5 degrees

# run the kriging operation
k500 = krige(t ~ 1, locations = sf500, newdata = sgrid, model = vtmf500, nmax = 100)
summary(k500)

# map the results
kresults500 = k500@data
names(kresults500)[1:2] = c('est', 'var')
kresults500$x = k500@coords[,1]
kresults500$y = k500@coords[,2]
max(kresults500$var) # 50.18986
kresults500$ci95 = sqrt(kresults500$var) * 2
summary(kresults500$ci95) 
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 8.769  13.030  13.040  13.050  13.060  14.170 
hist(sqrt(kresults500$var)*2) # most values will be in +/- 13 degrees

# map the results
ggplot(data = kresults500) +
  geom_point(aes(x = x, y = y, colour = est)) +
  scale_colour_distiller(palette = 'YlOrRd')
# pretty much just averages for most of the country

ggplot(data = kresults500) +
  geom_point(aes(x = x, y = y, colour =ci95)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks =  seq(8, 15, 1))
# confidence isn't BAD, but it's basically just averaged values for most of the country

##########################################################################################



##########################################################################################
# MODELING - slice = 1500
# extract out a z slice
df1500 = filter(df, z_slice == 1500)
df1500$t = df1500$t1500
# we should not see a relationship between depth and temp anymore
ggplot(df1500) +
  geom_point(aes(x = depth_m, y = t))

# convert data frame to spatial data frame
sf1500 = df1500
# convert to coordinates
coordinates(sf1500) = ~ x + y

# variogram can only run out to 1/2 the distance of the entire field
# to find that distance, run a quick test variogram
ve = variogram(t ~ 1, data = sf1500, width = 15000)
max(ve$dist)/2
# 803869.3

# calculate experimental variogram
ve = variogram(t ~ 1, data = sf1500, width = 2000, cutoff = 800000)
plot(ve)
var(df1500$t) # 221
# vtme <- vgm(psill = 100, model = "Exc", range = 10000, nugget = 20)
# plot(ve, model = vtme)
# vtme <- fit.variogram(ve, vtme)
# plot(ve, model = vtme)
# vtme
# this result suggest a TREND in the data -- but not a massive one
# simply accept the trend for now
# the initial sill seems to fall in the range o f 100-200 km

# now the sill seems to occur in the range of 200k - 400 k
ve = variogram(t ~ 1, data = sf1500, width = 1500, cutoff = 200000)
plot(ve)

ve = variogram(t ~ 1, data = sf1500, width = 1500, cutoff = 150000)
plot(ve)
# sill is more distinctive at 150 km

# this is too far, the important relationship is < 200km
ve1000 = variogram(t ~ 1, data = sf1500, width = 1000, cutoff = 150000)
plot(ve1000)
# is this width appropriate?
summary(ve1000)

# fit a variogram for each
# estimate variogram model form and parameters by eye
vtme1000 <- vgm(psill = 70, model = "Cir", range = 50000, nugget = 20)
plot(ve1000, model = vtme1000)
# fit the model 
vtmf1000 <- fit.variogram(ve1000, vtme1000)
plot(ve1000, model = vtmf1000)
# how well does it fit?
attributes(vtmf1000)$SSErr
#0.0483285


# run cross validation
cvk1000 <- krige.cv(t ~ 1, sf1500, model = vtmf1000, nfold = 1500, nmax = 100)
res <- as.data.frame(cvk1000)$residual
sqrt(mean(res^2)) # 6.866207
mean(res) # -0.002883389
mean(res^2/as.data.frame(cvk1000)$var1.var) #  1.452457
sd(res)*2 # 13.73304 (95% CI)

# now interpet it
vtmf1000
# model    psill    range
# 1   Nug 20.47587     0.00
# 2   Cir 53.00168 58103.64
sqrt(vtmf1000$psill[1])# averaged difference at 0 m is 4.5 degrees
vtmf1000$range[2] # 58103.64 # data are no longer correlated beyond this distance
sqrt(vtmf1000$psill[2])# average difference at >4.5 km is 7.280225 degrees

# run the kriging operation
k1500 = krige(t ~ 1, locations = sf1500, newdata = sgrid, model = vtmf1000, nmax = 100)
summary(k1500)

# map the results
kresults1500 = k1500@data
names(kresults1500)[1:2] = c('est', 'var')
kresults1500$x = k1500@coords[,1]
kresults1500$y = k1500@coords[,2]
max(kresults1500$var, na.rm= T) # 603
kresults1500$ci95 = sqrt(kresults1500$var) * 2
summary(kresults1500$ci95) 
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 9.141  13.580  13.660  13.330  13.710  15.900 
hist(sqrt(kresults1500$var)*2) # most values will be in +/- 17 degrees

# map the results
ggplot(data = kresults1500) +
  geom_point(aes(x = x, y = y, colour = est)) +
  scale_colour_distiller(palette = 'YlOrRd')
# this one is actually relatively decent

# map the results
ggplot(data = kresults1500) +
  geom_point(aes(x = x, y = y, colour = ci95)) +
  scale_colour_distiller(palette = 'YlOrRd', breaks =  seq(9, 20, 1))
# not terrible -- this is pretty good, possibly because we have so many wells for this range


##########################################################################################