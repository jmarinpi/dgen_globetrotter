source('./map_functions.R')


results = read.csv('/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/runs/results_20140604_140900/all_us_demo/outputs.csv.gz')

diffusion_all = ddply(results, .(state_abbr,year), summarize,
      Market.Share = sum(market_share),
      Market.Value = sum(market_value),
      Number.of.Adopters = sum(number_of_adopters),
      Installed.Capacity = sum(installed_capacity)
      )

names(diffusion_all)[1:2] = c('State','Year')
diffusion_all$State = as.character(diffusion_all$State)

# diffusion_sector = ddply(results, .(state_abbr,year, sector), summarize,
#                       Market.Share = sum(market_share),
#                       Market.Value = sum(market_value),
#                       Number.of.Adopters = sum(number_of_adopters),
#                       Installed.Capacity = sum(installed_capacity)
# )
# 
# sector_split = split(diffusion_sector, diffusion_sector$sector)
# diffusion_res = sector_split$residential
# diffusion_ind = sector_split$industrial
# diffusion_com = sector_split$commercial

# display.brewer.all()
m1 = anim_choro_multi(diffusion_all, 'State', 
                      c('Market.Share','Market.Value', 'Number.of.Adopters', 'Installed.Capacity'),
                      pals = list(Market.Share = 'Blues', Market.Value = 'Greens', Number.of.Adopters = 'Purples', Installed.Capacity = 'Reds'),
                      ncuts = list(Market.Share = 5, Market.Value = 5, Number.of.Adopters = 5, Installed.Capacity = 5), 
                      classification = 'quantile',
                      height = 400, width = 800, scope = 'usa', label_precision = 0, big.mark = ',',
                      legend = T, labels = T, 
                      slider_var = 'Year', slider_step = 0, map_title = 'Diffusion', horizontal_legend = F, slider_width = 300,
                      legend_titles = list(Market.Share = 'Market Share (%)', Market.Value = 'Market Value ($)',
                                           Number.of.Adopters = 'Number of Adopters (Count)', Installed.Capacity = 'Installed Capacity (kw)'))

# m1$show(cdn = T)
m1$save('/Users/mgleason/d.html', cdn =T)
#classification = "fixed", "sd", "equal", "pretty", "quantile", "kmeans", "hclust", "bclust", 
              # "fisher", or "jenks"

