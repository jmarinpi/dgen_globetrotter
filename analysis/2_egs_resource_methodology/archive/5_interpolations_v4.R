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

# break the data up into depth classes
labels = seq(500, 3000, 500)
breaks = c(0, 750, 1250, 1750, 2250, 2750, 3250)

df$slice = cut(df$depth_m, breaks = breaks, labels = labels)

# check that slices look right
ggplot(data = df) +
  geom_boxplot(aes(x = slice, y = depth_m))
# looks good

# calculate the thermal gradient for every well (based on the t35 km data)
df$delta_t = df$t35km - df$t_c
df$delta_z = 35000 - df$depth_m
df$g = df$delta_t / df$delta_z

m = lm(g ~ depth_m + t35km, data = df)
summary(m)
r = m$residuals
mean(r)
mean(r**2)**.5
# very small errors
hist(r)

# what effect do they have on the actual temperature measurements
df$m_t = df$t35km + m$fitted.values * (df$depth_m - 3500)
plot(df$t_c ~ df$m_t)
df$m_r = df$t_c - df$m_t
hist(df$m_r)
mean(df$m_r)
mean(df$m_r**2)**.5

####################################################################################
# BEST APPROACH SO FAR
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
# there is definitely a spatial pattern. is any of it due to depth?

labels = seq(500, 3000, 500)
breaks = c(0, 750, 1250, 1750, 2250, 2750, 3250)
dfg$z_slice = cut(dfg$depth_m, breaks = breaks, labels = labels)

ggplot(data = dfg) +
  geom_point(aes(x = x, y = y, colour = resid_int), alpha = 0.5) +
  facet_wrap(~ z_slice) +
  scale_colour_manual(values = c('#a50026', '#d73027', '#f46d43', '#fdae61', '#fee090', '#ffffbf', '#e0f3f8', '#abd9e9', '#74add1', '#4575b4', '#313695'))
# segmenting by depth slice shows that there is still a lot of spatial autocorrelation in each set of slices
# for those sets, use them to interpolate resiuals

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

# run the kriging operation
k2000 = krige(resid ~ 1, locations = sf2000, newdata = sgrid, model = vtmf, nmax = 100)
summary(k2000)
str(k2000)

# run cross validation
# cvk <- krige.cv(resid ~ 1, sf2000, model = vtmf, nfold = 1000, nmax = 100)
# summary(cvk)
# res <- as.data.frame(cvk)$residual
# sqrt(mean(res^2))
# mean(res)
# mean(res^2/as.data.frame(cvk)$var1.var)

# x = data.frame(resid_k = cvk@data$var1.pred)
# x$x = cvk@coords[,1]
# x$y = cvk@coords[,2]
# 
# dfm = merge(x, df2000, by = c('x', 'y'))
# dfm$t_pred_k = dfm$t_pred + dfm$resid_k
# dfm$t_pred_final_resid = dfm$t - dfm$t_pred_k
# hist(dfm$t_pred_final_resid)
# mean(dfm$t_pred_final_resid)
# mean(dfm$t_pred_final_resid**2)**0.5

kresults = k2000@data
names(kresults)[1:2] = c('fitted_resid', 'resid_variance')
kresults$x = k2000@coords[,1]
kresults$y = k2000@coords[,2]

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
grid2000_hici = filter(grid2000, ci <= 25)


ggplot(data = grid2000_hici) +
  geom_point(aes(x = x, y = y, colour = resid_variance)) +
  geom_point(data = df2000, aes(x = x, y = y), size = 0.3)

df$resid_interval = cut(abs(df$resid), breaks = c(0, 10, 20, 30, 40, 50, 300 ), labels = c(10, 20, 30, 40, 50, 300))
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour =resid_interval)) 

####################################################################################
# does g vary with depth?
ggplot(data = df) +
  geom_point(aes(x = depth_m, y = g))
# yes, for sure
# so, it may not may sense to interpolate every well to every depth

# what is the range of thermal gradient
summary(df$g)
# -0.007 to 0.005

# check the distribution of the thermal gradient within each slice
ggplot(data = df) +
  geom_boxplot(aes(x = slice, y = g))
# the mean of the thermal gradient decreases as slice move deeper
# range runs from 0.0050 to -0.0050, meaning, over each 500 slice,
# the difference in gradient for different wells could account for +/- 2.5 degree 
# difference in the results

# within each 500 m distance band, it's probably fine to interpolate
# because the max error will be +/- 2.5 degrees
ggplot(data = df) +
  geom_boxplot(aes(x = slice, y = g * 500))

# within 1000 m, it could be worse (+/- 5 degrees) but probably still okay
ggplot(data = df) +
  geom_boxplot(aes(x = slice, y = g * 1000))


# create new slices
df$for_500 = (df$depth_m >= (500 - 1000)) & (df$depth_m <= (500 + 1000))
df$for_1000 = df$depth_m > (1000 - 1000) & df$depth_m < (1000 + 1000)
df$for_1500 = df$depth_m > (1500 - 1000) & df$depth_m < (1500 + 1000)
df$for_2000 = df$depth_m > (2000 - 1000) & df$depth_m < (2000 + 1000)
df$for_2500 = df$depth_m > (2500 - 1000) & df$depth_m < (2500 + 1000)
df$for_3000 = df$depth_m > (3000 - 1000) & df$depth_m < (3000 + 1000)

# apply the gradients to correct well values to centerpoints of each interval
# (only where applicable based on new slices)
df$t_500 = df$t_c + df$g * (500 - df$depth_m)
df$t_1000 = df$t_c + df$g * (1000 - df$depth_m)
df$t_1500 = df$t_c + df$g * (1500 - df$depth_m)
df$t_2000 = df$t_c + df$g * (2000 - df$depth_m)
df$t_2500 = df$t_c + df$g * (2500 - df$depth_m)
df$t_3000 = df$t_c + df$g * (3000 - df$depth_m)

# filter out thevaleus to ignore
df[df$for_500 == F, 't_500'] = NA
df[df$for_1000 == F, 't_1000'] = NA
df[df$for_1500 == F, 't_1500'] = NA
df[df$for_2000 == F, 't_2000'] = NA
df[df$for_2500 == F, 't_2500'] = NA
df[df$for_3000 == F, 't_3000'] = NA


# create a map of each value
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour = t_500))
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour = t_1000))
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour = t_1500)
 ggplot(data = df) +
   geom_point(aes(x = x, y = y, colour = t_1500)


# plot the depth interpolated values against their absolute values for each slice
ggplot(data = df) +
  geom_point(aes(x = t_c, y = t_500, colour = depth_m))
ggplot(data = df) +
  geom_point(aes(x = t_c, y = t_3000, colour = depth_m))

# convert grid to spatial grid
sgrid = grid
coordinates(sgrid) = ~ x + y

# slice out some data for kriging
# and collapse points at the same location
df2000 = filter(df, for_2000) %>%
          group_by(x, y) %>%
          summarize(t = median(t_2000)
  ) %>%
  as.data.frame()
sf2000 = df2000
# convert to coordinates
coordinates(sf2000) = ~ x + y

# experimental variogram 
ve = variogram(t ~ 1, data = sf2000, width = 1000, cutoff = 600000)
plot(ve)

# estimate variogram model form and parameters by eye
vtme <- vgm(psill = 450, model = "Exc", range = 100000, nugget = 110)
plot(ve, model = vtme)
vtmf <- fit.variogram(ve, vtme)
plot(ve, model = vtmf)
vtmf

# run the kriging operation
k2000 = krige(t ~ 1, locations = sf2000, newdata = sgrid, model = vtmf, nmax = 100, maxdist = 353391.3)
summary(k2000)
str(k2000)

# run cross validation
cvk <- krige.cv(t ~ 1, sf2000, model = vtmf, nfold = 1000, nmax = 100)
summary(cvk)
res <- as.data.frame(cvk)$residual
sqrt(mean(res^2))
mean(res)
mean(res^2/as.data.frame(cvk)$var1.var)


# convert to a plain data frame
kdf2000 = as.data.frame(k2000@data)
kdf2000$x = k2000@coords[, 1]
kdf2000$y = k2000@coords[, 2]
names(kdf2000)[1:2] = c('pred', 'var')

ggplot(data = kdf2000) +
  geom_point(aes(x = x, y = y, colour = pred))
ggplot(data = kdf2000) +
  geom_point(aes(x = x, y = y, colour = var))

# combine the predictions with the regression predictions
grid2000_final = merge(grid2000, kdf2000, by = c('x', 'y'))
grid2000_final$t_c_final = grid2000_final$ols_t_c + grid2000_final$pred
grid2000_final$t_c_min = grid2000_final$t_c_final - grid2000_final$var
grid2000_final$t_c_max = grid2000_final$t_c_final + grid2000_final$var

breaks = seq(0, 350, 25)
grid2000_final$t_c_interval = cut(grid2000_final$t_c_final, breaks = breaks, labels = breaks[2:length(breaks)])

ggplot(data = grid2000_final) +
  geom_point(aes(x = x, y = y, colour = t_c_interval)) +
  scale_colour_manual(values = rev(c('#a50026', '#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850', '#006837')))


grid2000_final$var_interval = cut(grid2000_final$var, breaks = breaks, labels = breaks[2:length(breaks)])
ggplot(data = grid2000_final) +
  geom_point(aes(x = x, y = y, colour = var_interval)) +
  scale_colour_manual(breaks = breaks, values = rev(c('#a50026', '#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850', '#006837')))





grid$t_c_interval = cut(grid$t35km, breaks = breaks, labels = breaks[2:length(breaks)])
ggplot(data = as.data.frame(grid)) +
  geom_point(aes(x = x, y = y, colour = t_c_interval)) +
  scale_colour_manual(values = rev(c('#a50026', '#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850', '#006837')))



ggplot(data = grid2000_final) +
  geom_point(aes(x = x, y = y, colour = var))

# filter to the points that have uncertainty less than 25 degrees
grid2000_final_hc = filter(grid2000_final, var <= 50)






















# plot the relationship between t35 and tc
ggplot(data = df) +
  geom_point(aes(x = t35km, y = t_c, colour = depth_m)) +
  facet_wrap(~slice, scales = 'free')
# pretty good until you get down to 500 m

# plot the histograms
ggplot(data = df) +
  geom_histogram(aes(x = t_c)) +
  facet_wrap(~slice, scales = 'free')

# plot the histograms
ggplot(data = df) +
  geom_histogram(aes(x = t35km)) +
  facet_wrap(~slice)
# reasonable symmetric in both cases, prob not essential to transform

# where there are multiple temp measurements at the same x/y location and depth slice, just average them
dfg = group_by(df, x, y, slice) %>%
  summarize(t35km = median(t35km),
            depth_m = median(depth_m),
            t_c = median(t_c)
  ) %>%
  as.data.frame()


df_list = list()
m_list = list()
grid_list = list()
for (s in sort(unique(df$slice))){
  print(s)
  df_slice = filter(dfg, slice == s)
  
  df$delta_t = df$t35km - df$t_c
  df$delta_z = 35000 - df$depth_m
  df$g = df$delta_t / df$delta_z
  df$t_500 = df$t_c + df$g * (500 - df$depth_m)
  df$t_1000 = df$t_c + df$g * (1000 - df$depth_m)
  df$t_1500 = df$t_c + df$g * (1500 - df$depth_m)
  df$t_2000 = df$t_c + df$g * (2000 - df$depth_m)
  df$t_2500 = df$t_c + df$g * (2500 - df$depth_m)
  df$t_3000 = df$t_c + df$g * (3000 - df$depth_m)
  
  m = lm(t_2000 ~ t35km, df)  
  summary(m)
  
  d = data.frame()
  for (i in 1:10){
    k = kmeans(df$g, i)
    ratio = k$betweenss/k$totss
    d[i, 'i'] = i
    d[i, 'ratio'] = ratio
    
  }
  plot(d$ratio ~ d$i)
  # 4 
  k = kmeans(df$g, 4)
  df$g_clust = k$cluster
  

  # map it
  ggplot(data = df) +
    geom_point(aes(x = x, y = y, colour = g))
  
  ggplot(data = df) +
    geom_point(aes(x = x, y = y, colour = as.factor(g_clust)))
  
  
  m = lm(t_2000 ~ t35km * as.factor(g_clust), df)  
  summary(m)
  plot(m)

  
  df$resid = m$residuals
  ggplot(data = df) +
    geom_point(aes(x = x, y = y, colour = abs(resid) < 20))

  
  
  m = lm(t_3000 ~ t35km * as.factor(g_clust), df)  
  summary(m)
  plot(m)
  
  m = lm(g ~ depth_m, df)
  df$resid = m$residuals
  ggplot(data = df) +
    geom_point(aes(x = x, y = y, colour = resid))
  df$g_pred = m$fitted.values
  df$t_pred = df$t35km + df$g_pred * (df$depth_m - 3500)
  df$t_resid = df$t_c - df$t_pred
  hist(df$t_resid)
  mean(df$t_resid)
  mean(df$t_resid**2)**.5
  ggplot(data = df) +
    geom_point(aes(x = x, y = y, colour = abs(t_resid)<50))  
  
  
  m_slice = lm(t_c ~ depth_m + t35km, df_slice)
  m = lm(t_c ~ depth_m + t35km + as.factor(g_clust), df)
  mean(m$residuals)
  mean(m_slice$residuals)
  mean(m$residuals**2)**.5
  mean(m_slice$residuals**2)**.5
  hist(m$residuals)
  hist(m_slice$residuals)
  summary(m$residuals)
  summary(m_slice$residuals)
  summary(m)
  summary(m)
  plot(m)  
  hist(m$residuals)
  m = lm(delta_t ~ delta_z, df_slice)
  
  df_slice$ols_t_c = m$fitted.values
  df_slice$ols_resid = m$residuals
  
  df_name = sprintf('df%s', s)
  df_list[[as.character(s)]] = df_slice
  assign(df_name, df_slice)
  
  m_name = sprintf('m%s', s)
  assign(m_name, m)
  m_list[[as.character(s)]] = m
  
#   print(summary(m))
  #   hist(m$residuals)
  #   plot(m$residuals)
  
  
  # map the residuals
  #   g = ggplot(data = df_slice) +
  #     geom_point(aes(x = x, y = y, colour = ols_resid))
  #   print(g)
  
  # plot the comparison of fitted to acutal values
  #   g = ggplot(data = df_slice) +
  #     geom_point(aes(x = t_c, y = ols_t_c, colour = depth_m))
  #   print(g)
  
  # predict the values for the full grid
  pgrid = grid
  pgrid$depth_m = as.numeric(s)
  pgrid$ols_t_c = predict.lm(m, pgrid)
  grid_name = sprintf('grid%s', s)
  assign(grid_name, pgrid)
  grid_list[[as.character(s)]] = pgrid
  
  # map the full grid predictions
  #   g = ggplot(data = pgrid) +
  #     geom_point(aes(x = x, y = y, colour = ols_t_c))
  #   print(g)
  
}



##################################################################
# work on kriging the residuals
coordinates(df3000) = ~ x + y
coordinates(grid) = ~ x + y

# experimental variogram (cloud)
# vc = variogram(ols_resid ~ 1, data= df3000, cloud = T)
ve = variogram(ols_resid ~ 1, data= df3000, width = 100, cutoff = 60000)

plot(ve)

# estimate variogram model form and parameters by eye
vtme <- vgm(psill = 136, model = "Exc", range = 4000, nugget = 97)
plot(ve, model = vtme)
vtmf <- fit.variogram(ve, vtme)
plot(ve, model = vtmf)
vtmf

k3000 = krige(ols_resid ~ 1, locations = df3000, newdata = grid, model = vtmf, nmax = 100)
summary(k3000)
str(k3000)

# convert to a plain data frame
kdf3000 = as.data.frame(k3000@data)
kdf3000$x = k3000@coords[, 1]
kdf3000$y = k3000@coords[, 2]
names(kdf3000)[1:2] = c('pred', 'var')

ggplot(data = kdf3000) +
  geom_point(aes(x = x, y = y, colour = pred))
ggplot(data = kdf3000) +
  geom_point(aes(x = x, y = y, colour = var))

# combine the predictions with the regression predictions
grid3000_final = merge(grid3000, kdf3000, by = c('x', 'y'))
grid3000_final$t_c_final = grid3000_final$ols_t_c + grid3000_final$pred
grid3000_final$t_c_min = grid3000_final$t_c_final - grid3000_final$var
grid3000_final$t_c_max = grid3000_final$t_c_final + grid3000_final$var

ggplot(data = grid3000_final) +
  geom_point(aes(x = x, y = y, colour = t_c_final))

# filter to the points that have uncertainty less than 25 degrees
grid3000_final_hc = filter(grid3000_final, var <= 200)

ggplot(data = grid3000_final_hc) +
  geom_point(aes(x = x, y = y, colour = t_c_final))

ggplot(data = as.data.frame(df3000)) +
  geom_point(aes(x = x, y = y))
##################################################################
# repeat for a different depth
coordinates(df2000) = ~ x + y

# experimental variogram (cloud)
# vc = variogram(ols_resid ~ 1, data= df3000, cloud = T)
ve = variogram(t_c ~ 1, data= df2000, width = 100, cutoff = 80000)

plot(ve)

# estimate variogram model form and parameters by eye
vtme <- vgm(psill = 78, model = "Sph", range = 50000, nugget = 74)
plot(ve, model = vtme)
vtmf <- fit.variogram(ve, vtme)
plot(ve, model = vtmf)
vtmf

k2000 = krige(ols_resid ~ 1, locations = df2000, newdata = grid, model = vtmf, nmax = 100)
summary(k2000)
str(k2000)

# convert to a plain data frame
kdf2000 = as.data.frame(k2000@data)
kdf2000$x = k2000@coords[, 1]
kdf2000$y = k2000@coords[, 2]
names(kdf2000)[1:2] = c('pred', 'var')

ggplot(data = kdf2000) +
  geom_point(aes(x = x, y = y, colour = pred))
ggplot(data = kdf2000) +
  geom_point(aes(x = x, y = y, colour = var))

# combine the predictions with the regression predictions
grid2000_final = merge(grid2000, kdf2000, by = c('x', 'y'))
grid2000_final$t_c_final = grid2000_final$ols_t_c + grid2000_final$pred
grid2000_final$t_c_min = grid2000_final$t_c_final - grid2000_final$var
grid2000_final$t_c_max = grid2000_final$t_c_final + grid2000_final$var

breaks = seq(0, 350, 25)
grid2000_final$t_c_interval = cut(grid2000_final$t_c_final, breaks = breaks, labels = breaks[2:length(breaks)])

ggplot(data = grid2000_final) +
  geom_point(aes(x = x, y = y, colour = t_c_interval)) +
  scale_colour_manual(values = rev(c('#a50026', '#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850', '#006837')))


grid2000_final$var_interval = cut(grid2000_final$var, breaks = breaks, labels = breaks[2:length(breaks)])
ggplot(data = grid2000_final) +
  geom_point(aes(x = x, y = y, colour = var_interval)) +
  scale_colour_manual(breaks = breaks, values = rev(c('#a50026', '#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850', '#006837')))





grid$t_c_interval = cut(grid$t35km, breaks = breaks, labels = breaks[2:length(breaks)])
ggplot(data = as.data.frame(grid)) +
  geom_point(aes(x = x, y = y, colour = t_c_interval)) +
  scale_colour_manual(values = rev(c('#a50026', '#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850', '#006837')))



ggplot(data = grid2000_final) +
  geom_point(aes(x = x, y = y, colour = var))

# filter to the points that have uncertainty less than 25 degrees
grid2000_final_hc = filter(grid2000_final, var <= 50)


cv.o <- krige.cv(ols_resid ~ 1, df2000, model=vtmf, nfold=nrow(df2000), nmax = 100)
summary(cv.o)
res <- as.data.frame(cv.o)$residual
sqrt(mean(res^2))
mean(res)
mean(res^2/as.data.frame(cv.o)$var1.var)



##########################################################################################


##########################################################################################
# simpler method -- just plain kriging of the data
# experimental variogram (cloud)
# vc = variogram(ols_resid ~ 1, data= df3000, cloud = T)
coordinates(df3000) = ~ x + y
ve = variogram(t_c ~ 1, data= df3000, width = 100, cutoff = 60000)

plot(ve) # results are going to be just as bad because nugget = 100



