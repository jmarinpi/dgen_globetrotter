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

################################################################################################

# Calculate gradients from T@3.5km
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

# extract out a z slice
df2000 = filter(df, z_slice == 2000)
# we should not see a relationship between depth and temp anymore
ggplot(df2000) +
  geom_point(aes(x = depth_m, y = t))
# there still is a relationship...
# how strong
m = lm(t ~ depth_m, data = df2000)
summary(m) # fairly weak -- about 0.0933

# no easy fix for this; however, it justifies the use of narrow intervals

# convert data frame to spatial data frame
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
# this result suggest a TREND in the data -- could it be due to depth?
ve = variogram(t ~ depth_m, data = sf2000, width = 1000, cutoff = 800000)
plot(ve)
# no -- there is some other underlying spatial pattern driving the trend

# is t a function of x and y and depth?
m = lm(t ~ x + y + depth_m, data = df2000)
summary(m) # yes - r2 - 0.3

# this is better
ve = variogram(t ~ y + x + depth_m, data = sf2000, width = 1000, cutoff = 800000)
plot(ve)

# now the sill seems to occur in the range of 200k - 400 k
ve = variogram(t ~ y + x + depth_m, data = sf2000, width = 1000, cutoff = 200000)
plot(ve)

ve = variogram(t ~ y + x + depth_m, data = sf2000, width = 1000, cutoff = 400000)
plot(ve)
# sill is more distinctive at 400 km, so use that as cutoff

# how to set the bin widths -- try 200 and 500 m
ve200 = variogram(t ~ y + x + depth_m, data = sf2000, width = 200, cutoff = 400000)
plot(ve200)
# is this width appropriate?
summary(ve200$np) # at least 1771 point pairs in each bin, average of 12,600 in each bin

# this is too far, the important relationship is < 200km
ve500 = variogram(t ~ y + x + depth_m, data = sf2000, width = 500, cutoff = 400000)
plot(ve500)
# is this width appropriate?
summary(ve500$np) # at least 5417 point pairs in each bin, average of 30,000 in each bin

# fit a variogram for each
# estimate variogram model form and parameters by eye
vtme200 <- vgm(psill = 300, model = "Exc", range = 60000, nugget = 60, kappa = 0.5)
plot(ve200, model = vtme200)
# fit the model 
vtmf200 <- fit.variogram(ve200, vtme200)
plot(ve200, model = vtmf200)
# how well does it fit?
attributes(vtmf200)$SSErr
# 11.4358

# estimate variogram model form and parameters by eye
vtme500 <- vgm(psill = 300, model = "Exc", range = 30000, nugget = 60, kappa = 0.5)
plot(ve500, model = vtme500)
# fit the model 
vtmf500 <- fit.variogram(ve500, vtme500)
plot(ve500, model = vtmf200)
# how well does it fit?
attributes(vtmf500)$SSErr
# 6.938


# what are the params of the two models
vtmf200
# model     psill  range kappa
# 1   Nug  74.45704      0   0.0
# 2   Exc 340.27730 121603   0.5

vtmf500
# model     psill    range kappa
# 1   Nug  60.30439     0.00   0.0
# 2   Exc 237.72118 27491.16   0.5

# test which one is better using cross validation
# run cross validation
cvk200 <- krige.cv(t ~ x + y + depth_m, sf2000, model = vtmf200, nfold = 1000, nmax = 100)
res <- as.data.frame(cvk200)$residual
sqrt(mean(res^2))
mean(res)
mean(res^2/as.data.frame(cvk200)$var1.var)
sd(res)*2

cvk500 <- krige.cv(t ~ x + y + depth_m, sf2000, model = vtmf500, nfold = 100, nmax = 100)
res <- as.data.frame(cvk500)$residual
sqrt(mean(res^2))
mean(res)
mean(res^2/as.data.frame(cvk200)$var1.var)
sd(res)*2





# interpretation (http://www.statios.com/Resources/04-variogram.pdf)
# http://faculty.washington.edu/edford/Variogram.pdf
# http://people.ku.edu/~gbohling/cpe940/Variograms.pdf
# "the variogram for lag distance h is defined as the average squared difference 
# of values separated approximately by h"
# "nugget effect = sum of geological microstructure and measurement error"
  # "sparse data may also lead to a higher than expected nugget effect"
sqrt(vtmf200$psill[1])# averaged difference at 0 m is 8.704328 degrees 
sqrt(vtmf500$psill[1])# averaged difference at 0 m is 7.694 degrees
vtmf200$range[2] # 279,251.8 # data are no longer correlated beyond this distance
vtmf500$range[2] # 36109.96 # data are no longer correlated beyond this distance
sqrt(vtmf200$psill[2])# average difference at >279 km is 22 degrees 
sqrt(vtmf500$psill[2])# average difference at >36 km is 16 degrees








# run the kriging operation
k2000 = krige(resid ~ 1, locations = sf2000, newdata = sgrid, model = vtmf, nmax = 100)
summary(k2000)
str(k2000)

# summarize the kriging results
kresults = k2000@data
names(kresults)[1:2] = c('fitted_resid', 'resid_variance')
kresults$x = k2000@coords[,1]
kresults$y = k2000@coords[,2]
max(kresults$resid_variance) #207.4742
sqrt(max(kresults$resid_variance)) # 14.40396

# if we are targetting areas with confidence in +/- 15
# plot the results
ggplot(data = kresults) +
  geom_point(aes(x = x, y = y, colour = 2* sqrt(resid_variance) < 25))

# compile the full predictions
grid2000 = grid
grid2000$depth_m = 2000
grid2000$fitted_t = predict.lm(m, grid2000)
# combine with the kriging results
grid2000 = merge(grid2000, kresults, by = c('x', 'y'))
grid2000$t_est = grid2000$fitted_t + grid2000$fitted_resid

# map the results
breaks = seq(0, 350, 25)
grid2000$t_interval = cut(grid2000$t_est, breaks = breaks, labels = breaks[2:length(breaks)])


ggplot(data = grid2000) +
  geom_point(aes(x = x, y = y, colour = t_interval)) +
  scale_colour_manual(values = rev(c('#a50026', '#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850', '#006837')))

# filter out grid cells with high uncertainty
grid2000$ci = sqrt(grid2000$resid_variance) * 2

# save to csv
write.csv(grid2000, '/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/egs_resource/data/analysis/t2km.csv', row.names = F)
grid2000_hici = filter(grid2000, ci <= 25)


ggplot(data = grid2000_hici) +
  geom_point(aes(x = x, y = y, colour = resid_variance)) +
  geom_point(data = df2000, aes(x = x, y = y), size = 0.3)

df$resid_interval = cut(abs(df$resid), breaks = c(0, 10, 20, 30, 40, 50, 300 ), labels = c(10, 20, 30, 40, 50, 300))
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour =resid_interval)) 


