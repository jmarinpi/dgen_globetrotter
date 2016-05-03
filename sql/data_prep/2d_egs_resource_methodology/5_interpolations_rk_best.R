library(dplyr)
library(reshape2)
library(ggplot2)
library(RPostgreSQL)
library(gstat)
library(sp)
library(ggthemes)
library(grid)
library(scales)
library(fitdistrplus)

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

sql = "SELECT gid, grid_gid, 
x_96703 as x, y_96703 as y, 
t35km, 
depthfinal as depth_m, 
temperaturefinal as t,
g
FROM dgeo.bht_compilation
WHERE depthfinal >= 300 and depthfinal <= 3250
AND gid <> 349391 -- outlier point
AND t35km IS NOT NULL;"
df = dbGetQuery(con, sql)

sql = "SELECT gid, x_96703 as x, y_96703 as y,  
temp_c as t35km
FROM dgeo.smu_t35km_2016"
grid = dbGetQuery(con, sql)

################################################################################################

################################################################################################
# PREPROCESSING 
# 1 - Cut the data into depth "slices" (500 m intervals surrounding 500, 1000, 1500, 2000, 2500, and 3000 m)
labels = seq(500, 3000, 500)
# 500 1000 1500 2000 2500 3000
breaks = c(0, 750, 1250, 1750, 2250, 2750, 3250)
# 0  750 1250 1750 2250 2750 3250
df$z_slice = cut(df$depth_m, breaks = breaks, labels = labels)

# recalibrate valeus to depth slice
df$t_adj = df$g * (as.numeric(as.character(df$z_slice)) - df$depth_m) + df$t
# drop records where t_adj < 0
dfpos = filter(df, t_adj > 0)

# group up to grid cells and slices
dfa = group_by(dfpos, grid_gid, z_slice) %>%
  summarize(t = median(t_adj)) %>%
  as.data.frame()
names(dfa)[1] = 'gid'
dfg = merge(dfa, grid, by = 'gid')
dfg$z = as.numeric(as.character(dfg$z_slice))

# 1 - "jitter" all points by a random amount in the range of +/- 1 m  (draw from uniform random sample)
# this will fix points with identical coordinates (identical coordinates will break the kriging algorithm)
set.seed(1)
x = dfg$x + runif(nrow(dfg), -1, 1)
y = dfg$y + runif(nrow(dfg), -1, 1)
dfg$x = x
dfg$y = y
# check there are no remaining dupes
# find locs with multiple rows
dupes = group_by(dfg, x, y) %>%
  summarize(
    count = sum(!is.na(x))
  ) %>%
  filter(count > 1) %>%
  as.data.frame()
nrow(dupes)
# 0 -- all set
################################################################################################

################################################################################################
# create regression model
m = lm(t ~ z + t35km, data = dfg)
summary(m)# 0.7899 

# REGRESSION RESIDUAL ANALYSIS/MODEL VALIDATION
mean(m$residuals) # ~0
mean(m$residuals**2)**.5 # 13.63139
sd(m$residuals)
hist(m$residuals) # mostly between +/- 25 degrees
boxplot(m$residuals)
min(m$residuals) #   -100.4867
max(m$residuals) # 399.7916
sd(m$residuals)*2  # 27.26298
summary(m$residuals)
# Min.   1st Qu.    Median      Mean   3rd Qu.      Max. 
# -100.5000   -6.0430   -0.1635    0.0000    4.9480  399.8000 
# some pretty massive outliers, particularly on the high side
# but the only way to fix this is awith a better regression model
# or removing more outliers...

# is there any relationship between depth and resid?
plot(m$residuals ~ m$model$z) # yes at the high side...
cor(m$residuals, m$model$z) # 0 # but not significant

# how about t35km and residuals?
plot(m$residuals ~ m$model$t35km)
cor(m$residuals, m$model$t35km) # nope
# zero correlation, but def some anomalies where t35 km is high

# how about fitted and residuals?
plot(m$residuals ~ m$fitted.values)
cor(m$residuals, m$fitted.values) # nope -- all set on all resid checks

# residuals are not normally distributed, but this is the best we can do with the data currently available
hist(m$residuals)
plotdist(m$residuals)
descdist(m$residuals, boot = 1000)

# merge the results back to the df
# check that orders match between model and df
all(dfg$t35km == m$model$t35km)
all(dfg$depth_m == m$model$depth_m)
# TRUE for both, all set

# extract the residuals and fitted values
dfg$resid = m$residuals
dfg$t_pred = m$fitted.values

# two additional checks
# create boxplot of residuals and temp by slice
ggplot(data = dfg) +
  geom_boxplot(aes(x = z_slice, y = resid))
# definitely not much of a relationship here -- slightly more variance at greater depths
# boxplot of predictions by slice
ggplot(data = dfg) +
  geom_boxplot(aes(x = z_slice, y = t_pred))
# definitely a strong relationship here (as expected)

################################################################################################
# build variogram model
sf = dfg
coordinates(sf) = ~ x + y
v = variogram(resid ~ 1, data = sf, width = 10000, cutoff = 800000)
plot(v)
var(dfg$resid) # 185.8175

# estimate variogram model form and parameters by eye
vm <- vgm(psill = 75, model = "Sph", range = 600000, nugget = 80)
plot(v, model = vm)
# fit the model 
vm <- fit.variogram(v, vm)
plot(v, model = vm) # TODO: make this a nice graphic
vm
# model    psill    range
# 1   Nug 79.82231      0.0
# 2   Sph 74.50193 593282.4
# 
# ################################################################################################

################################################################################################
# CROSS VALIDATION OF MODEL (DO THIS BEFORE RUNNING ACTUAL KRIGIN)


cvk_rk <- krige.cv(formula = resid ~ 1, sf, model = vm, nfold = 100, nmax = 100)
summary(cvk_rk)
res_rk <- as.data.frame(cvk_rk)$residual
sqrt(mean(res_rk^2)) # 10.31593
sd(res_rk)
mean(res_rk) # -0.005039385
mean(res_rk^2/as.data.frame(cvk_rk)$var1.var) # 1.259834
sd(res_rk)*2 # 20.63201 (the original LM had sd*2 of about 27)

# check relationship between cv resids and IVs
cvk_rk_data = cvk_rk@data
cvk_rk_data$x = cvk_rk@coords[, 1]
cvk_rk_data$y = cvk_rk@coords[, 2]
dfcv = merge(cvk_rk_data, dfg, by = c('x', 'y'))
boxplot(dfcv$residual ~ dfcv$z)
# no visual relationship with depth -- this is an improvement over regression alone
plot(dfcv$residual ~ dfcv$t35km) 
# this is consitent with regresion kriging
################################################################################################


################################################################################################
# KRIGING AND FINAL PREDICTIONS
# run the kriging operation (only need to do this once since it is depth independent)
sgrid = grid
coordinates(sgrid) = ~ x + y
# krige the residuals
k_rk = krige(resid ~ 1, locations = sf, newdata = sgrid, model = vm, nmax = 100)

# extract the results
results_rk = k_rk@data
names(results_rk)[1:2] = c('est', 'var')
results_rk$x = k_rk@coords[,1]
results_rk$y = k_rk@coords[,2]
results_rk$ci95 = sqrt(results_rk$var) * 2
# check the histogram
ggplot(data = results_rk) +
  geom_histogram(aes(x = ci95))
# dump to png
out_png = sprintf('%s/uncertainty_update.png', outpath)
ggsave(out_png, width = 6, height = 4)

# for each depth slice, run the LM predictions and combine with the kriging results for final results
for (z in c(500, 1000, 1500, 2000, 2500, 3000)){
  print(sprintf('Working on %s', z))
  lgrid = grid
  lgrid$z = z
  
  # apply the lm to the grid and then combine
  lgrid$lm_t = predict.lm(m, lgrid)
  # combine with the kriging results
  combined_results = merge(results_rk, lgrid, by = c('x', 'y'))
  combined_results$resid_est = combined_results$est
  combined_results$est = combined_results$lm_t + combined_results$resid_est
  
  # write to postgres
  out_cols = c('x', 'y', 'est', 'ci95', 'z')
  out_df = combined_results[, out_cols]
  out_table = sprintf('egs_temp_at_depth_update_%s', z)
  dbWriteTable(con, c('dgeo', out_table), out_df, row.names = F, overwrite = T)
} 
# FOR PLOTTING RESULTS
#   ggplot(data = out_df) +
#     geom_point(aes(x = x, y = y, colour = est)) +
#     scale_colour_distiller(palette = 'YlOrRd', breaks = seq(0, 150, 25))
#   
#   ggplot(data = out_df) +
#     geom_point(aes(x = x, y = y, colour = ci95)) +
#     scale_colour_distiller(palette = 'Blues', breaks = seq(18, 30, 2))

################################################################################################



################################################################################################
# REPORT FIGURES
# variogram for the actual prediction model
vm_line = variogramLine(vm, maxdist = max(v$dist))
globvar = var(dfg$resid)
ggplot() +
  geom_point(data = v, aes(x = dist/1000, y = gamma), colour = '#737373', size = 1.0) +
  geom_line(data = vm_line, aes(x = dist/1000, y = gamma)) +
  geom_hline(aes(yintercept = globvar), linetype = 2) +
  geom_text(aes(x = 200, y = globvar + 0.05 * globvar, label = sprintf("Global Variance = %s", round(globvar, 0))), colour = 'Blue', fontface = 'bold') +
  scale_x_continuous(name = 'Distance (km)', labels = comma) +
  scale_y_continuous(name = 'Semivariance') +
  coord_cartesian(ylim = c(0, 300)) +
  theme_custom
out_png = sprintf('%s/vgm_best_update.png', outpath)
ggsave(out_png, width = 6, height = 4)

################################################################################################

#  additional exploration of plain temperature data in depth slices
d = 500
sfd = filter(dfg, z_slice == d)
coordinates(sfd) = ~ x + y
v_d = variogram(t ~ 1, data = sfd, width = 10000, cutoff = 800000)
plot(v_d) 
var(sfd$t)
# range of about 400km

d = 1000
sfd = filter(dfg, z_slice == d)
coordinates(sfd) = ~ x + y
v_d = variogram(t ~ 1, data = sfd, width = 10000, cutoff = 800000)
plot(v_d) 
var(sfd$t)

d = 1500
sfd = filter(dfg, z_slice == d)
coordinates(sfd) = ~ x + y
v_d = variogram(t ~ 1, data = sfd, width = 10000, cutoff = 800000)
plot(v_d) 
var(sfd$t)


d = 2000
sfd = filter(dfg, z_slice == d)
coordinates(sfd) = ~ x + y
v_d = variogram(t ~ 1, data = sfd, width = 10000, cutoff = 800000)
plot(v_d) 
var(sfd$t)


