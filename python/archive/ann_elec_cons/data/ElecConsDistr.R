library(ggplot2)
library(plyr)

cbecs<-read.csv('cbecs_file15.csv')
cbecs<-subset(cbecs,ELCNS8 != 'NA')


# 1. Separate into regions
# 2. Find quantiles
# 3. x (consumption) = weighted mean of ELCNS8 by ADJWT8
# 4. y (prob) = sum of ADJWT8

n<-100

df<-data.frame()
for(region in 1:4){
  print(region)
  tmp<-subset(cbecs, REGION8 == region)
  tmp$pbin<-cut(tmp$ELCNS8,quantile(x=tmp$ELCNS8,probs=seq(0,1,(1/n))))
  tmp2<-ddply(tmp,.(pbin),summarise, x = weighted.mean(x=ELCNS8,w=ADJWT8), y = sum(ADJWT8))
  tmp2$region<-region
  df<-rbind(df,tmp2)
}
  


c<-cbecs$ELCNS8
#
#cbecs<-subset(cbecs,ELCNS8 < 5000000)
#
ggplot(cbecs, aes(ELCNS8, weight = ADJWT8)) + geom_histogram(binwidth = 1000000) + facet_wrap(~REGION8,scales="free")


n<-100

cbecs$pbin<-cut(c,quantile(x=c,probs=seq(0,1,(1/n))))

foo<-ddply(cbecs,.(cuts),summarise, x = weighted.mean(x=ELCNS8,w=ADJWT8), y = sum(ADJWT8))


foo<-ddply(cbecs,.(cuts),summarise, tot = sum(ADJWT8))
foo<-transform(foo, yper = y/sum(y))

plot(foo$x,foo$y)

##
df<-read.csv("C:/Users/bsigrin/Downloads/cbp11us/cbp11us.txt")
mecs<-read.csv('mecs_elec_cons_by_naics.csv')
names(mecs)[1]<-"naics"
names(mecs)[3]<-"cons_gwh"
mecs$cons_gwh<-as.numeric(mecs$cons_gwh)

# est - Total number of establishments
# naics

## Clean up naics code

df<-subset(df,lfo == "-")
df$naics<-as.numeric(as.character((gsub("[-/]","",df$naics))))
df<-df[,c('naics','est')]

df<-join(mecs,df,type="left")
df<-subset(df, naics != 'NA')

df<-transform(df,kwh_per_est = (1e6*cons_gwh)/est)
df<-subset(df,Region == 'US')
df<-transform(df,prob = est/sum(est))
df[,'consumption']<-df[,'kwh_per_est']
out<-df[,c("consumption","prob")]
write.csv(out,"mecs_discrete_elec_cons_distr.csv")
