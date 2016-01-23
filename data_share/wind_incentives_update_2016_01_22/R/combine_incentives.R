library(xlsx)
library(dplyr)
setwd('/Users/mgleason/Desktop/plane_to_do/wind_incentives/curated_by_type')
files = c('incentives_cap_rebate.xlsx',
          'incentives_fit.xlsx',
          'incentives_itc.xlsx',
          'incentives_itd.xlsx',
          'incentives_prod_rebate.xlsx',
          'incentives_ptc.xlsx')

l = list()
for (n in 1:length(files)){
  df_x = read.xlsx(files[n], sheetIndex = 1)
  df_x$exp_date = as.Date(df_x$exp_date)
  l[[n]] = df_x

}

df = rbind_all(l)

# write to csv
write.csv(df, 'incentives_all.csv', row.names = F)

# summary stats
# how many states?
length(unique(df$state_abbr)) # 18
# how many com incentives?
sum(df$sector_abbr == 'com') # 20
# how many res incentives?
sum(df$sector_abbr == 'res') # 21
# how many of each type
as.list(table(df$incentive_type))


