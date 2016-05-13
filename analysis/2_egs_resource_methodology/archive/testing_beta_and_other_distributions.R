library(fitdistrplus)
plotdist(dfg$t)
descdist(dfg$t, boot = 1000)
fln = fitdist(dfg$t, 'lnorm')
qqcomp(fln)

plotdist(dfg$t_scale)
descdist(dfg$t_scale, boot = 1000)
fbeta = fitdist(dfg$t_scale, 'beta', method = 'mme')
qqcomp(fbeta)


fgamma = fitdist(dfg$t_scale, 'gamma', method = 'mme')
qqcomp(fgamma)

fitdistr(dfg$t_scale, 'beta', list(shape1 = 2, shape2 = 5))

library(betareg)
# rescale the data to zeros and ones
dfg$t_scale = (dfg$t-min(dfg$t))/max(dfg$t)
summary(dfg$t_scale)
# drop the zero
dfx = filter(dfg, t_scale > 0)
bm = betareg(t_scale ~ t35km + z, dfx)
hist(bm$residuals)
fitted = bm$fitted.values*max(dfg$t) + min(dfg$t)
resids = dfx$t - fitted
sd(resids)