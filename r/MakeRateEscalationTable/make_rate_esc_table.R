# make_rate_esc_table.R
# 
# Electricity rate escalations by state and sector in 2012$ based on AEO 2014 Reference scenario 
# Value is multiplicative scalar relative to 2011 rates.

library(reshape2)
library(plyr)

df<-read.csv("AEO2014_RateForecast.csv")
regions<-read.csv("region_lookup.csv")

df<-melt(df)
df<-df[,c('Region','Sector','variable','value')]
names(df)<-c('Region','Sector','Year','value')
df$Year<-as.character(df$Year)
df$Year<-substr(df$Year,2,6)

df<-join(x=df,y=regions)
write.csv(df,'rate_esc_by_state_and_sector.csv')
