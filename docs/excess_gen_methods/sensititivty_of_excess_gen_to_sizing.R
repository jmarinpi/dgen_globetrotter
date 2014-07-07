
# This script calculates the sensitivity of the excess generation factor to relative sizing of annual generation
# to load. The original calculation assumes that sum(hourly_gen) = sum(hourly_load) for an annual time series.
# That value is the excess_generation factor. However, how does that value change is the system is smaller or
# larger?
# 
# => I find that the relationship is roughly linear when generation is bounded by 50% to 115% of annual load.
# The relationship is excess_gen_factor = 0.31 * (gen/load) + 0.25

library(ggplot2)

# Load hourly generation profiles
gen_profiles<-read.csv('C:/Users/bsigrin/Desktop/gen_hrly_profiles.csv.gz', header = F)
gen_profiles = t(gen_profiles)
row.names(gen_profiles) <- NULL 

# Start with assumption that annual gen = annual load = 100
g = 100
l = 100

df = data.frame("ratio" = 1, "per_excess" = 0)

# Do 2000 simulations where we i) randomly vary l from 20% to 150%; ii) take a random generation profile
for(i in 1:2000){
  
g = runif(1,40,120)

# Take a random gen profile
gen = gen_profiles[,sample(1:99,1)] 

# Assume load follows a weibull distribution
load = rweibull(8760,1)

gsum = sum(gen)
lsum = sum(load)

gen =  gen * (g/gsum)
load = load * (l/lsum)

amount_absorbed = sum(pmin(gen,load))
amount_excess = sum(pmax(gen-load,0))

# This is the percentage of excess (spilled) energy
per_excess = amount_excess/g

out = data.frame("ratio" = g/l, "per_excess" = per_excess)
df = rbind(df,out)
}

lm_eqn = function(df){
    names(df)<-c('x','y')
    m = lm(y ~ x, df);
    eq <- substitute(italic(y) == a + b %.% italic(x)*","~~italic(r)^2~"="~r2, 
         list(a = format(coef(m)[1], digits = 2), 
              b = format(coef(m)[2], digits = 2), 
             r2 = format(summary(m)$r.squared, digits = 3)))
    as.character(as.expression(eq));                 
}


ggplot(df, aes(x = ratio, y = per_excess))+
  geom_point()+
  stat_smooth(method = 'lm', formula = y ~ x, color = 'red', size = 3)+
  geom_text(aes(x = .8, y = 1, label = lm_eqn(df)), parse = TRUE)+
  xlab('Annual Generation/Annual Load')+
  ylab('Percent of Excess Generation')+
  ggtitle('Sensitivity of the excess generation factor to relative sizing of system')