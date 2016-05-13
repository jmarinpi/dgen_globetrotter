df = read.csv('/Users/mgleason/Desktop/core.surface_site_county_state_materialized_view.csv')
library(dplyr)
df2 = filter(df, (!is.na(bht)) & (depth > 300) & (depth < 3000))
plot(df2$latitude ~ df2$longitude)
g = group_by(df2, state) %>%
    summarize(count = sum(!is.na(bht)))


ggplot(data = g) + 
  geom_bar(aes(x = state, y = count), stat = 'identity') +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

write.csv(df2, '/Users/mgleason/Desktop/smu_data_filtered.csv', row.names = F)
