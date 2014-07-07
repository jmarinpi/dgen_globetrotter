gen<-read.csv('C:/Users/bsigrin/Desktop/gen_hrly_profiles.csv.gz',header = F)

g<-runif(n=10000,0,1)
l<-runif(n=10000,0,1)

gs<-sum(g)
ls<-sum(l)

g<-g*(5000/gs)
l<-l*(5000/ls)

x = g - l
xf = data.frame('x' = x)

ggplot(data = xf, aes(x))+
  stat_ecdf()


x<-as.numeric(gen)
y<-load[,'load_mw']


sapply(1:10,function(i) cor(gen[,i],y[i,]))

df<-data.frame()
load<-matrix(nrow = 8760, ncol = 98)
for(j in unique(d$transmission_zone)){
  print(sprintf('Working on zone %s', j))
  l<-subset(d, transmission_zone == j)[,'load_mw']
  load[,1:98] = l
  c<-cor(load,gen)[1,]
  df<-rbind(df,data.frame('utility' = j, correlation = c))
}


ggplot(df, aes(x = correlation, color = utility))+
  stat_ecdf()+
  xlab('Correlation Factor')+
  ylab('Cumulative Probability')+
  ggtitle('Analysis of Correlation between load and generation profiles'

          
p<-c()          
for(i in 1:98){
  p0<-shapiro.test(gen[1:5000,i])$p.value
  p<-c(p,p0)
}


ggplot(d, aes(x = load_mw, color = transmission_zone))+
  geom_density()+
  xlim(0,5000)+
  ylim(0,.5)
