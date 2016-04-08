library(dplyr)
library(reshape2)
library(ggplot2)
library(RPostgreSQL)
library(gstat)
library(sp)
library(ggthemes)
library(grid)
library(scales)


# TODO: 
# find an additional predictive variable uncorelated with t35km and depth_m?

################################################################################################
# CONSTANTS 
outpath = '/Users/mgleason/NREL_Projects/Projects/local_data/dgeo_misc/egs_resource/graphics/for_report'
theme_custom =    theme(panel.grid.minor = element_blank()) +
  theme(text = element_text(colour = ggthemes_data$fivethirtyeight["dkgray"])) +
  theme(plot.margin = unit(c(1, 1, 1, 1), "lines")) +
  theme(axis.title = element_text(size = rel(1.2), face = 'bold')) +
  theme(axis.title.x = element_text(vjust = 0.1)) +
  theme(axis.title.y = element_text(vjust = 1.1)) +
  theme(axis.text = element_text(size = rel(1))) +
  theme(plot.title = element_text(size = rel(1.5), face = "bold")) +
  theme(legend.text = element_text(size = rel(1))) +
  theme(legend.title=element_blank()) +
  theme(legend.key=element_blank()) +
  theme(axis.line = element_line(colour =  ggthemes_data$fivethirtyeight["dkgray"], size = 1)) +
  theme(panel.grid.major = element_line(colour = "light grey")) +
  theme(panel.background = element_rect(fill = "white")) +
  theme(legend.background = element_rect(fill = alpha('white', 0.5)))

################################################################################################


################################################################################################
# CONNECT TO PG
drv <- dbDriver("PostgreSQL")
# connect to postgres
con <- dbConnect(drv, host="gispgdb.nrel.gov", dbname="dav-gis", user="mgleason", password="mgleason")

sql = "SET ROLE 'dgeo-writers';"
dbSendQuery(con, sql)

################################################################################################################################################


################################################################################################################################################
# LOAD DATA FROM PG

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

################################################################################################
# PREPROCESSING 

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
# TODO: TURN THIS INTO A NICE GRAPHIC!!!
ggplot(data = df) +
  geom_boxplot(aes(x = z_slice, y = g))
# drift is towards a NEGATIVE gradient as you approach 3000 km
################################################################################################


################################################################################################
# BUILD REGRESSION MODEL
# explore univariate relationships between t35km, depth, and t
ggplot(data = df) +
  geom_point(aes(x = depth_m, y = t, colour = t35km))
# strong positive linear relationship between depth and t, with heteroskedasticity

ggplot(data = df) +
  geom_point(aes(x = t35km, y = t, colour = depth_m))
# less strong relationship here, but still appears present and positive

ggplot(data = df) +
  geom_point(aes(x = t35km, y = depth_m, colour = depth_m))
# no relationship here, as expected

# create univariate model -- depth only
m1 = lm(t ~ depth_m, data = df)
summary(m1) # r2 = 73.71
# t = -2.648 + 0.0409 * depth_m
plot(m1$residuals ~ m1$model$depth_m)
hist(m1$residuals)
summary(m1$residuals)
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# -95.170 -10.940  -1.912   0.000  11.150 255.800 

# next univariate model, t35km only
m2 = lm(t ~ t35km, data = df)
summary(m2) # r2 = 0.386
# t = -30 + 1.09 * t35km
plot(m2$residuals ~ m2$model$t35km)
hist(m2$residuals)
summary(m2$residuals)
# Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
# -163.300  -20.180   -0.516    0.000   20.650  228.400 

# create multivariate model
m3 = lm(t ~ t35km + depth_m, data = df)
summary(m3) # R2 = 0.8385 (better than univariate models)
# t= -50.27 + 0.6052 * t35km + 0.03467 * depth_m
# what is the t at depth 0 assuming a t35km = 100
# -50.27 + 0.6052*100 + 0.03467 * 0 = 10.25  -- this seems reasonable
# -50.27 + 0.6052*75 + 0.03467 * 0 = -4 -- this is not reasonable but it points out
# the limitations of applying this method below the depth range of the data
################################################################################################


################################################################################################
# REGRESSION RESIDUAL ANALYSIS/MODEL VALIDATION
mean(m3$residuals) # ~0
mean(m3$residuals**2)**.5 # 14.80692
hist(m3$residuals) # mostly between +/- 50 degrees
min(m3$residuals) # -91.70114
max(m3$residuals) # 231.1028
sd(m3$residuals)*2 # 29.61406
summary(m3$residuals)
# Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
# -91.7000  -8.0990  -0.7514   0.0000   7.7000 231.1000 
# some pretty massive outliers, particularly on the high side
# but the only way to fix this is awith a better regression model

# is there any relationship between depth and resid?
plot(m3$residuals ~ m3$model$depth_m) # nope
cor(m3$residuals, m3$model$depth_m) # 0

# how about t35km and residuals?
plot(m3$residuals ~ m3$model$t35km)
cor(m3$residuals, m3$model$t35km) # nope
# zero correlation, but def some anomalies where t35 km is high

# how about fitted and residuals?
plot(m3$residuals ~ m3$fitted.values)
cor(m3$residuals, m3$fitted.values) # nope -- all set on all resid checks

# merge the results back to the df
# check that orders match between model and df
all(df$t35km == m3$model$t35km)
all(df$depth_m == m3$model$depth_m)
# TRUE for both, all set

# extract the residuals and fitted values
df$resid = m3$residuals
df$t_pred = m3$fitted.values

# two additional checks
# create boxplot of residuals and temp by slice
ggplot(data = df) +
  geom_boxplot(aes(x = z_slice, y = resid))
# definitely not much of a relationship here
# boxplot of predictions by slice
ggplot(data = df) +
  geom_boxplot(aes(x = z_slice, y = t_pred))
# definitely a strong relationship here (as expected)

# also merge in results for the depth_m only model
df$resid_m1 = m1$residuals
df$t_m1 = m1$fitted.values

# two additional diagnostics
# create boxplot of residuals and temp by slice
ggplot(data = df) +
  geom_boxplot(aes(x = z_slice, y = resid))
# definitely not much of a relationship here -- residuals are roughly the same regardless of well depth
# boxplot of predictions by z slice 
ggplot(data = df) +
  geom_boxplot(aes(x = z_slice, y = t_pred))
# definitely a strong relationship between temp and depth here (as expected)
# how does this compare to the actual temperatures?
ggplot(data = df) +
  geom_boxplot(aes(x = z_slice, y = t))
# trend is the same, 
# but predicted values don't capture the same variance as the observed data
# (which is to be expected)
# only way to improve this is to add another independent variable to the model

# map the results 
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour = t_pred), size = 1.1) +
  scale_colour_distiller(palette = 'YlOrRd', breaks = seq(0, 200, 25)) +
  facet_wrap(~z_slice)

# is there a spatial pattern in the residuals?
ggplot(data = df) +
  geom_point(aes(x = x, y = y, colour = resid), size = 1.1) +
  scale_colour_distiller(palette = 'Spectral', breaks = seq(-100, 250, 25))
# there are definitely patterns, not sure how they will affect stationarity of the procss
# definitely some spatial autocorrelation, but trends are not super obvious
################################################################################################


################################################################################################
# BUILD VARIOGRAM MODEL
# conver df to spatial data frame
sf = df
coordinates(sf) = ~ x + y

# creat and view experimental variogram 
v_rk = variogram(resid ~ 1, data = sf, width = 5000, cutoff = 800000)
plot(v_rk) # TO DO: Make this a real plot
var(df$resid) # 219.2482
# compared to above, no trend -- variance doesnt grow as much over space 
# and results ends up at 2x of nuggest instead of 4.5x

# estimate variogram model form and parameters by eye
vm_rk <- vgm(psill = 220, model = "Exc", range = 200000, nugget = 100, kappa = 0.3)
plot(v_rk, model = vm_rk)
# fit the model 
vm_rk <- fit.variogram(v_rk, vm_rk)
plot(v_rk, model = vm_rk) # TODO: make this a nice graphic
vm_rk
# model     psill    range kappa
# 1   Nug  39.06616      0.0   0.0
# 2   Exc 217.80226 141931.5   0.3
################################################################################################

################################################################################################
# CROSS VALIDATION OF MODEL (DO THIS BEFORE RUNNING ACTUAL KRIGIN)
cvk_rk <- krige.cv(resid ~ 1, sf, model = vm_rk, nfold = 1000, nmax = 100)
summary(cvk_rk)
res_rk <- as.data.frame(cvk_rk)$residual
sqrt(mean(res_rk^2)) # 9.72167 (origninal LM had rmse of about 15)
mean(res_rk) # 0.02269205
mean(res_rk^2/as.data.frame(cvk_rk)$var1.var) # 1.033864
sd(res_rk)*2 # 19.44344 (the original LM had sd*2 of about 30)

# results are much better than ok
# how about with IVs?
cvk_rk_data = cvk_rk@data
cvk_rk_data$x = cvk_rk@coords[, 1]
cvk_rk_data$y = cvk_rk@coords[, 2]
dfcv = merge(cvk_rk_data, df, by = c('x', 'y'))
plot(dfcv$residual ~ dfcv$depth_m)
# no relationship with depth -- this is an improvement over ok
plot(dfcv$residual ~ dfcv$t35km) 
# how about with t35km?
################################################################################################


################################################################################################
# KRIGING AND FINAL PREDICTIONS
# for each depth slice, run the krigin operation
for (depth in c(500, 1000, 1500, 2000, 2500, 3000)){
  print(sprintf('Working on %s', depth))
  # run the kriging operation
  sgrid = grid
  sgrid$depth_m = z
  coordinates(sgrid) = ~ x + y
  # krige the residuals
  k_rk = krige(resid ~ 1, locations = sf, newdata = sgrid, model = vm_rk, nmax = 100)
  
  # map the results
  results_rk = k_rk@data
  names(results_rk)[1:2] = c('est', 'var')
  results_rk$x = k_rk@coords[,1]
  results_rk$y = k_rk@coords[,2]
  results_rk$ci95 = sqrt(results_rk$var) * 2
  # check the histogram
  ggplot(data = results_rk) +
    geom_histogram(aes(x = ci95))
  # dump to png
  out_png = sprintf('%s/uncertainty_%sm.png', outpath, depth)
  ggsave(out_png, width = 6, height = 4)
  
  # to map the results, apply the lm to the grid and then combine
  sgrid$lm_t = predict.lm(m3, as.data.frame(sgrid))
  results_rk = merge(results_rk, sgrid, by = c('x', 'y'))
  results_rk$resid_est = results_rk$est
  results_rk$est = results_rk$lm_t + results_rk$resid_est
  
  # write to postgres
  out_cols = c('x', 'y', 'est', 'ci95', 'depth_m')
  out_df = results_rk[, out_cols]
  out_table = sprintf('egs_temp_at_depth_%s', depth)
  dbWriteTable(con, c('dgeo', out_table), out_df, row.names = F, overwrite = T)
  
  
}
################################################################################################


################################################################################################
# REPORT FIGURES


# FIGURE 2
# to explore the effects of "sparseness",
# determine what the range of the data in exp. variograms
# (create variograms for different depth slices to control for depth effects)
# 1000
d = 1000
sfd = filter(df, z_slice == d)
coordinates(sfd) = ~ x + y
v_d = variogram(t ~ 1, data = sfd, width = 5000, cutoff = 800000)
plot(v_d) 
var(sfd$t)

# estimate variogram model form and parameters by eye
vm_d <- vgm(psill = 150, model = "Exp", range = 200000, nugget = 20, kappa = 0.5)
plot(v_d, model = vm_d)
# fit the model 
vm_d <- fit.variogram(v_d, vm_d)
vm_d 

vm_d_line = variogramLine(vm_d, maxdist = max(v_d$dist), n = 200)
vm_range = vm_d$range[2]/1000
vm_sill = yend =vm_d$psill[2]
ggplot() +
  geom_point(data = v_d, aes(x = dist/1000, y = gamma), colour = '#737373', size = 1.0) +
  geom_line(data = vm_d_line, aes(x = dist/1000, y = gamma)) +
  #   geom_vline(aes(xintercept = vm_d$range[2]/1000), linetype = 2) +
  geom_segment(aes(x = vm_range, xend = vm_range, y = 0, yend = vm_sill), linetype = 2) +
  geom_text(aes(x = vm_range, y = vm_sill + 0.25 * vm_sill, label = sprintf("Range = %s km", round(vm_range, 0))), colour = 'Blue', fontface = 'bold') +
  scale_x_continuous(name = 'Distance (km)', labels = comma) +
  scale_y_continuous(name = 'Semivariance') +
  theme_custom
out_png = sprintf('%s/vgm_zslice_1000m.png', outpath)
ggsave(out_png, width = 6, height = 4)

# FIGURE 3
# 2000
d = 2000
sfd = filter(df, z_slice == d)
coordinates(sfd) = ~ x + y
v_d = variogram(t ~ 1, data = sfd, width = 5000, cutoff = 800000)
plot(v_d) 
var(sfd$t)

# estimate variogram model form and parameters by eye
vm_d <- vgm(psill = 150, model = "Exc", range = 200000, nugget = 20, kappa = 0.5)
plot(v_d, model = vm_d)
# fit the model 
vm_d <- fit.variogram(v_d, vm_d)
vm_d 


vm_d_line = variogramLine(vm_d, maxdist = max(v_d$dist), n = 200)
vm_range = vm_d$range[2]/1000
vm_sill = yend =vm_d$psill[2]
ggplot() +
  geom_point(data = v_d, aes(x = dist/1000, y = gamma), colour = '#737373', size = 1.0) +
  geom_line(data = vm_d_line, aes(x = dist/1000, y = gamma)) +
  #   geom_vline(aes(xintercept = vm_d$range[2]/1000), linetype = 2) +
  geom_segment(aes(x = vm_range, xend = vm_range, y = 0, yend = vm_sill), linetype = 2) +
  geom_text(aes(x = vm_range, y = vm_sill + 0.25 * vm_sill, label = sprintf("Range = %s km", round(vm_range, 0))), colour = 'Blue', fontface = 'bold') +
  scale_x_continuous(name = 'Distance (km)', labels = comma) +
  scale_y_continuous(name = 'Semivariance') +
  theme_custom
out_png = sprintf('%s/vgm_zslice_2000m.png', outpath)
ggsave(out_png, width = 6, height = 4)

# FIGURE 4
# gradient variation over depth
ggplot(data = df) +
  geom_point(aes(x = depth_m, y = g)) +
  geom_smooth(aes(x = depth_m, y = g)) +
  theme_custom +
  scale_x_continuous(name = 'Depth of Measurement (m)') +
  scale_y_continuous(name = expression(Delta(T)/Delta(Z)))
out_png = sprintf('%s/gradient_vs_depth.png', outpath)
ggsave(out_png, width = 6, height = 4)

# FIGURE 5
# variogram showing global trend in bht data
# there is a trend in the data -- covariance grows beyond global variance
# and ends at about 4.5x of nugget

v_depth = variogram(resid_m1 ~ 1, data = sf, width = 5000, cutoff = 800000)
globvar = var(df$resid_m1) # 356.8411

ggplot() +
  geom_point(data = v_depth, aes(x = dist/1000, y = gamma), colour = '#737373', size = 1.0) +
  geom_hline(aes(yintercept = globvar), linetype = 2) +
  geom_text(aes(x = 200, y = globvar + 0.05 * globvar, label = sprintf("Global Variance = %s", round(globvar, 0))), colour = 'Blue', fontface = 'bold') +
  scale_x_continuous(name = 'Distance (km)', labels = comma) +
  scale_y_continuous(name = 'Semivariance') +
  coord_cartesian(ylim = c(0, 500)) +
  theme_custom
out_png = sprintf('%s/vgm_t_trend.png', outpath)
ggsave(out_png, width = 6, height = 4)



# FIGURE 6
# variogram for the actual prediction model
vm_rk_line = variogramLine(vm_rk, maxdist = max(v_rk$dist))
globvar = var(df$resid)
ggplot() +
  geom_point(data = v_rk, aes(x = dist/1000, y = gamma), colour = '#737373', size = 1.0) +
  geom_line(data = vm_rk_line, aes(x = dist/1000, y = gamma)) +
  geom_hline(aes(yintercept = globvar), linetype = 2) +
  geom_text(aes(x = 200, y = globvar + 0.05 * globvar, label = sprintf("Global Variance = %s", round(globvar, 0))), colour = 'Blue', fontface = 'bold') +
  scale_x_continuous(name = 'Distance (km)', labels = comma) +
  scale_y_continuous(name = 'Semivariance') +
  coord_cartesian(ylim = c(0, 300)) +
  theme_custom
out_png = sprintf('%s/vgm_best.png', outpath)
ggsave(out_png, width = 6, height = 4)


# for plotting results in R
# # map the results
# ggplot(data = results_rk) +
#   geom_point(aes(x = x, y = y, colour = est)) +
#   scale_colour_distiller(palette = 'YlOrRd', breaks = seq(0, 200, 25))
# # more subtleties than ok, reproduces known hot spots better
# 
# # map the confidence intervals
# ggplot(data = results_rk) +
#   geom_point(aes(x = x, y = y, colour = ci95)) +
#   scale_colour_distiller(palette = 'YlOrRd', breaks =  seq(0, 33, 3))
# # big improvement 
################################################################################################
