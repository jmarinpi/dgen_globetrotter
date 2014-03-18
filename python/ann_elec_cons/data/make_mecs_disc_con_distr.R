## Make manufacturing distribution on annual electricity consumption.
# Assumes: Manufacturing is representative of industry as a whole
#          Uniform or symmetrical distribution of consumption within a NAICS code
#
# Out: Discrete distribution of annual electricity consumption

cbp<-read.csv("data/cbp11us.txt") # Formated census business patterns data-- # of business per NAICS
mecs<-read.csv('data/mecs_elec_cons_by_naics.csv') # Formated MECS survey-- Electricity consumption by NAICS code
names(mecs)[1]<-"naics"
names(mecs)[3]<-"cons_gwh"
mecs$cons_gwh<-as.numeric(mecs$cons_gwh)

## Clean up naics code
cbp<-subset(cbp,lfo == "-")
cbp$naics<-as.numeric(as.character((gsub("[-/]","",cbp$naics))))
cbp<-cbp[,c('naics','est')]

df<-join(mecs,cbp,type="left")
df<-subset(df, naics != 'NA')

# Find the mean consumption by NAICS code
df<-transform(df,kwh_per_est = (1e6*cons_gwh)/est)

# Data only valid for US
df<-subset(df,Region == 'US')
df<-transform(df,prob = est/sum(est))
df[,'ann_cons_kwh']<-df[,'kwh_per_est']
out<-df[,c("ann_cons_kwh","prob")]
write.csv(out,"mecs_discrete_elec_cons_distr.csv")
