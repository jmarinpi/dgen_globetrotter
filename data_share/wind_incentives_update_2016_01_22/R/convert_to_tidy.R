library(reshape2)

df = read.csv('/Users/mgleason/Desktop/plane_to_do/wind_incentives/from_alice/pnnl_incentives_092015.csv')
m = melt(df, id.vars = c('State.Abbreviation'))
# convert "" to NA
m$value = ifelse(m$value == "", NA, m$value)
# drop nas
m = m[!is.na(m$value),]
# order by states, then variable
m = m[order(m$State.Abbreviation, m$variable),]

# drop columns for non-profit
unique(m$variable)
names(m)[1] = 'state_abbr'
unique(m$state_abbr)
# drop data for US
m = m[m$state_abbr != 'US',]

# load field descriptions
fields = read.csv('/Users/mgleason/Desktop/plane_to_do/wind_incentives/field_descriptions.csv')
# join to m
m = merge(m, fields, by = c('variable'))
# drop any fields that are NP/Govt:
m = m[!(m$sector %in% c('Government Non-Profit', 'Govt Non-Profit', 'Non-Taxed')),]
# re-sort by state again
m = m[order(m$state_abbr, m$variable),]

# write.csv(m, '/Users/mgleason/Desktop/plane_to_do/wind_incentives/pnnl_incentives_tidy.csv', row.names = F)


# how many states have incentives?
# how many states have residential incentives?
length(unique(m[m$sector %in% c('Residential', 'All'), 'state_abbr']))
length(unique(m[m$sector %in% c('Commercial', 'All'), 'state_abbr']))

