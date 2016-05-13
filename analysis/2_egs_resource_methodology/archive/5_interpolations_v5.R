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
correctedtemperature as t_c  
FROM dgeo.bht_smu
WHERE depthofmeasurement >= 304.8 and depthofmeasurement <= 3250
AND t35km IS NOT NULL;"
df = dbGetQuery(con, sql)

sql = "SELECT gid, x_96703 as x, y_96703 as y,  
t35km
FROM dgeo.egs_empty_grid"
grid = dbGetQuery(con, sql)


# map the t_c values 
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour = t_c))
# map the full grid t35km values
ggplot(data = grid) +
  geom_point(aes(x = x, y = y, colour = t35km))

# simplify the data to deal with identical coords
dfg = group_by(df, x, y) %>%
  summarize(depth_m = median(depth_m),
            t = median(t_c),
            t35km = median(t35km)
  ) %>%
  as.data.frame()

m = lm(t ~ t35km + depth_m, data = dfg)
summary(m) # R2 = 0.8396
mean(m$residuals) # ~0
mean(m$residuals**2)**.5 # 14.9
hist(m$residuals)
min(m$residuals) # -95
max(m$residuals) # 230
sd(m$residuals)*2

# is there any relationship between depth and resid?
plot(m$residuals ~ dfg$depth_m)
# no, not really

# how about t35km and residuals?
plot(m$residuals ~ dfg$t35km)
# not a systematica relatinoship, but def some anomalies

dfg$resid = m$residuals
dfg$t_pred = m$fitted.values

# map the results -- is there a pattern in the residuals?
# first, cut the residuals into brackets
breaks = c(floor(min(dfg$resid)/10)*10, seq(-40, 40, 10), ceiling(max(dfg$resid)/10)*10)
labels = breaks[2:length(breaks)]
dfg$resid_int = cut(dfg$resid, breaks, labels = labels)
ggplot(data = dfg) +
  geom_point(aes(x = x, y = y, colour = resid_int), alpha = 0.5, size = 0.9) +
  scale_colour_manual(values = c('#a50026', '#d73027', '#f46d43', '#fdae61', '#fee090', '#ffffbf', '#e0f3f8', '#abd9e9', '#74add1', '#4575b4', '#313695'))

ggplot(data = dfg) +
  geom_point(aes(x = x, y = y, colour = abs(resid) <= 30), alpha = 0.5, size = 0.9) 
#
# save to csv
write.csv(dfg, '/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/egs_resource/data/analysis/well_lm_residuals.csv', row.names = F)
# there is definitely a spatial pattern. is any of it due to depth?

# slice the data into depth categories
labels = seq(500, 3000, 500)
breaks = c(0, 750, 1250, 1750, 2250, 2750, 3250)
dfg$z_slice = cut(dfg$depth_m, breaks = breaks, labels = labels)

ggplot(data = dfg) +
  geom_point(aes(x = x, y = y, colour = resid_int), alpha = 0.5) +
  facet_wrap(~ z_slice) +
  scale_colour_manual(values = c('#a50026', '#d73027', '#f46d43', '#fdae61', '#fee090', '#ffffbf', '#e0f3f8', '#abd9e9', '#74add1', '#4575b4', '#313695'))
# segmenting by depth slice shows that there is still a lot of spatial autocorrelation in each set of slices
# for those sets, use them to interpolate resiuals

# KRIGING
# convert empty grid to spatial grid
sgrid = grid
coordinates(sgrid) = ~ x + y

# slice out some data for kriging
# and collapse points at the same location
df2000 = filter(dfg, z_slice == 2000)
sf2000 = df2000
# convert to coordinates
coordinates(sf2000) = ~ x + y

# experimental variogram 
ve = variogram(resid ~ 1, data = sf2000, width = 500, cutoff = 150000)
plot(ve)

# estimate variogram model form and parameters by eye
vtme <- vgm(psill = 90, model = "Exp", range = 20000, nugget = 60)
plot(ve, model = vtme)
# fit the model 
vtmf <- fit.variogram(ve, vtme)
plot(ve, model = vtmf)
vtmf

# run cross validation
cvk <- krige.cv(resid ~ 1, sf2000, model = vtmf, nfold = 1000, nmax = 100)
summary(cvk)
res <- as.data.frame(cvk)$residual
sqrt(mean(res^2))
mean(res)
mean(res^2/as.data.frame(cvk)$var1.var)
sd(res)*2

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


