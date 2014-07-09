sector_col <- c(residential = "#4daf4a", commercial = "#377eb8", industrial = "#e41a1c")
sector_fil <- c(residential = "#4daf4a", commercial = "#377eb8", industrial = "#e41a1c")
turb_size_fil <- c('Small: < 50 kW' = "#a1dab4", 'Mid: 51 - 500 kW' = "#41b6c4", 'Large: 501 - 3,000 kW' = "#253494") 
# ======================= DATA FUNCTIONS =================================================

make_con<-function(driver = "PostgreSQL", host = 'gispgdb', dbname="dav-gis", user = 'bsigrin', password = 'bsigrin'){
  # Make connection to dav-gis database
  dbConnect(dbDriver(driver), host = host, dbname = dbname, user = user, password = password)  
}

simpleCap <- function(x) {
  # converts a given string to proper case
  # For formatting scenario options
  s <- strsplit(x, "_")[[1]]
  paste(toupper(substring(s, 1,1)), substring(s, 2),
        sep="", collapse=" ")
}

mean_value_by_state_table<-function(df,val){
  # Create table of mean value by year and state. value is string of variable to take mean of. 
  val = as.symbol(val)
  g = group_by(df, year, state_abbr)
  by_state = collect(summarise(g,
                               round(mean(val),as.integer(2))
                              )
                     )
  names(by_state)<-c('year','State',val)
  g = group_by(df, year)
  national = collect(summarise(g,
                               round(mean(val),as.integer(2))
                              )
                     )
  national$State<-'U.S'
  names(national)<-c('year',val,'State')
  by_state<-dcast(data = by_state,formula= State ~ year, value.var = as.character(val))
  national<-dcast(data = national,formula= State ~ year, value.var = as.character(val))
  rbind(national,by_state)
}

total_value_by_state_table<-function(df,val){
  # Create table of summed value by year and state. value is string of variable to take sum over. 
  val = as.symbol(val)
  g = group_by(df, year, state_abbr)
  by_state = collect(summarise(g,
                               round(sum(val),as.integer(2))
                              )
                      )
  names(by_state)<-c('year','State',val)
  g = group_by(df, year)
  national = collect(summarise(g,
                               round(sum(val),as.integer(2))
                              )
                      )
  national$State<-'U.S'
  names(national)<-c('year',val,'State')
  by_state<-dcast(data = by_state,formula= State ~ year, value.var =  as.character(val))
  national<-dcast(data = national,formula= State ~ year, value.var =  as.character(val))
  rbind(national,by_state)
}

create_report <- function(runpath) {
  knit2html("../r/graphics/plot_outputs.md", output = "DG Wind report.html", title = "DG Wind report", stylesheet = "plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc = yes"))
}



# ======================= GRAPHING FUNCTIONS =================================================

cf_by_sector_and_year<-function(df){
  g = group_by(df, year, sector)
  # Median and inner-quartile range of CF over sector and year
  data =  collect(summarise(g,
                            tql = r_quantile(array_agg(naep), 0.75)/ 8760,
                            median = r_quantile(array_agg(naep), 0.5)/ 8760,
                            fql = r_quantile(array_agg(naep), 0.25)/ 8760))
  ggplot(data, aes(x = year, y = median, ymin = fql, ymax = tql, color = sector, fill = sector))+
    geom_smooth(aes(alpha = .1),stat = "identity")+
    facet_wrap(~sector)+
    geom_line(size = 0.75)+
    theme_few()+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(values = sector_fil) +
    scale_y_continuous(name = 'Annual average capacity factor', label = percent)+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    guides(color = FALSE, fill=FALSE)+
    ggtitle('Median Capacity Factor by Sector')
}

lcoe_contour<-function(df,dr = 0.05,n = 30){
# Map out the net present cost and annual capacity factor to achieve a given LCOE
# LCOE is calculate as the net present cost divided by the net present generation over lifetime
# To calculate NPC for the model, assume a 30yr life and 1c/kWh VOM
# Plot a sample of model points for the first and final year for all sectors,sizes, etc.
  
d = data.frame()
present_value_factor = ((1 - (1 + dr)^-n)/dr)

# Calculate the min and max capacity factors for a given lcoe and net present cost (npc)
for(lcoe in c(0,0.1, 0.2, 0.3, 0.4, 0.5, 0.6,0.7)){
  tmp<-data.frame(npc = seq(0,10,.01))
  tmp$cf <- 1000 * tmp$npc / (lcoe * 8760 * present_value_factor)
  tmp$cf_max <- 1000 * tmp$npc / (lcoe * 8760 * present_value_factor)
  tmp$cf_min <- 1000 * tmp$npc / ((lcoe + 0.1) * 8760 * present_value_factor)
  tmp$lcoe <- lcoe
  d <- rbind(d,tmp)
}

d[is.na(d)] <- 0.5
d[d$cf_max > 0.5, 'cf_max'] <- 0.5
d[d$cf_min > 0.5, 'cf_min'] <- 0.5
d[d$lcoe == 0.7 , 'cf_min'] <- 0

# Subset of model points for first and last year
pts = subset(df, year %in% c(min(df$year),max(df$year)))
pts = pts[sample(nrow(pts), min(1000,nrow(pts))),]

ggplot()+
  geom_ribbon(data = d, aes(x = npc, y = cf, ymin = cf_min, ymax = cf_max, fill = factor(lcoe)), alpha = 0.5)+
  theme_few()+
  scale_x_continuous(name = 'Net Present Cost ($/W)', limits = c(0,10))+
  scale_y_continuous(name = 'Annual Capacity Factor', limits = c(0,.5))+
  scale_fill_brewer(name = 'LCOE Range ($/kWh)', 
                    labels=c('< 0.1' ,"0.1 - 0.2", "0.2 - 0.3", "0.3 - 0.4", "0.4 - 0.5", "0.5 - 0.6", "0.6 - 0.7", "> 0.7"), 
                    palette = 'Spectral')+
  geom_point(data = pts, aes(x = 0.001 * (installed_costs_dollars_per_kw + naep * 0.01 * present_value_factor), 
                                y = naep/8760, color = factor(year), 
                                shape = factor(year)))+
  scale_colour_discrete(name = 'Sample From Model')+
  scale_shape_discrete(name = 'Sample From Model')+
  ggtitle("LCOE Contour Map For First and Final Model Years")
}

excess_gen_figs<-function(df,con){
  
  # Get the scenario options
  table<-dbGetQuery(con,"select * from wind_ds.scenario_options")
  nem_availability <- table[1,'net_metering_availability']
  
  if(nem_availability == 'Full_Net_Metering_Everywhere'){
  df$percent_of_gen_monetized = 1   
  } else if(nem_availability == 'Partial_Avoided_Cost'){
  df$percent_of_gen_monetized = 1 - 0.5 * df$excess_generation_factor 
  } else if(nem_availability == 'Partial_No_Outflows'){
  df$percent_of_gen_monetized = 1 - excess_generation_factor 
  } else if(nem_availability == 'No_Net_Metering_Anywhere'){
  df$percent_of_gen_monetized = 1 - df$excess_generation_factor  
  } else {percent_of_gen_monetized = 0}
 
  if(nem_availability != 'No_Net_Metering_Anywhere'){
  df[df$nem_system_limit_kw >= df$cap,'percent_of_gen_monetized'] <- 1
  }
  
  excess_gen_pt<-ggplot(df, aes(x =percent_of_gen_monetized, y = payback_period, color = nem_system_limit_kw>0))+
    geom_point()+
    theme_few()+
    scale_x_continuous(name = 'Percent of Generation Value at Retail Rate', labels = percent)+
    scale_y_continuous(name = 'Payback Period (years)')+
    ggtitle('Relationship Between NEM Availability and Payback Period')
  
  excess_gen_cdf<-ggplot(data = df, aes(x = percent_of_gen_monetized))+
    stat_ecdf()+
    theme_few()+
    scale_x_continuous(name = 'Percent of Generation Value at Retail Rate', labels = percent)+
    scale_y_continuous(name = 'Cumulative Percentage', labels = percent)+
    ggtitle('Distribution of Generation Valued at Retail Rate and Payback Period')

list('excess_gen_pt' = excess_gen_pt, 'excess_gen_cdf' = excess_gen_cdf)
}

cf_supply_curve<-function(df){
  #' National capacity factor supply curve
  # get the starting year
  start_year = as.numeric(collect(summarise(df, min(year))))
  # filter to only the start year, returning only the naep and load_kwh_in_bin cols
  data = collect(select(filter(df, year == start_year),naep,load_kwh_in_bin))
  data <- transform(data, cf = naep/8760)
  data<-data[order(-data$cf),]
  data$load<-cumsum(data$load_kwh_in_bin/(1e6*8760))
  
  ggplot(data,aes(x = cf, y = load, size = 0.75))+
    geom_line()+
    theme_few()+
    guides(size = FALSE)+
    scale_y_continuous(name ='Customer Load (GW)')+
    scale_x_continuous(name ='Annual Average Capacity Factor', labels = percent)+
    ggtitle('Capacity Factor Supply Curve (Available Cust Load in 2014)')
}

elec_rate_supply_curve<-function(df){
  #' National electricity rate supply curve
  # get the starting year
  start_year = as.numeric(collect(summarise(df, min(year))))
  # filter to only the start year, returning only the elec_rate_cents_per_kwh and load_kwh_in_bin cols
  data = collect(select(filter(df, year == start_year),elec_rate_cents_per_kwh,load_kwh_in_bin))  
  data <- transform(data, load = load_kwh_in_bin/(1e6 * 8760), rate = elec_rate_cents_per_kwh)
  data<-data[order(-data$rate),]
  data$load<-cumsum(data$load)
  
  ggplot(data,aes(x = rate, y = load, size = 0.75))+
    geom_line()+
    theme_few()+
    guides(size = FALSE)+
    scale_y_continuous(name ='Customer Load (GW)')+
    scale_x_continuous(name ='Average Electric Rate (c/kWh)', lim = c(0,25))+
    ggtitle('Electricity Rate Supply Curve (Available Cust Load in 2014)')
}

dist_of_cap_selected<-function(df,scen_name){
  # What size system are customers selecting in 2014?
  
  # get the starting and end years
  start_year = as.numeric(collect(summarise(df, min(year))))
  end_year = as.numeric(collect(summarise(df, max(year))))
  
  # filter to only the start year, returning only the elec_rate_cents_per_kwh and load_kwh_in_bin cols
  f = filter(df, year %in% c(start_year,end_year))  
  g = group_by(f, nameplate_capacity_kw, sector, year)
  cap_picked = collect(summarise(g,
                                 cust_num = sum(customers_in_bin)
  )
  )
  tmp<-ddply(cap_picked,.(sector,year),summarise, n = sum(cust_num))
  cap_picked<-merge(cap_picked,tmp)
  cap_picked<-transform(cap_picked, p = cust_num/n)
  
  p<-ggplot(cap_picked, aes(x = factor(nameplate_capacity_kw), weight = p, fill = factor(year)))+
    geom_histogram(position = 'dodge')+
    facet_wrap(~sector)+
    theme_few()+
    scale_y_continuous(name ='Percent of Customers Selecting Turbine Size', labels = percent)+
    scale_x_discrete(name ='Optimal Size Turbine for Customer (kW)')+
    #scale_color_manual(values = sector_col) +
    scale_fill_manual(name = 'Year', values = c('black','gray'))+
    theme(axis.text.x = element_text(angle = 45, hjust = 1))+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    #guides(color = FALSE)+
    ggtitle('Size of Turbines Being Considered')
  cap_picked$scenario<-scen_name
  write.csv(cap_picked,paste0(runpath,'/cap_selected_trends.csv'),row.names = FALSE)
  return(p)
}

dist_of_height_selected<-function(df,scen_name){
  #What heights are prefered?
  
  # get the starting year
  start_year = as.numeric(collect(summarise(df, min(year))))
  # filter to starting year
  f = filter(df, year == start_year)  
  g = group_by(f, cap, turbine_height_m, sector)
  height_picked = collect(summarise(g, 
                                    load_in_gw = sum(load_kwh_in_bin)/(1e6*8760)
  )
  )
  p<-ggplot(height_picked)+
    geom_point(aes(x = factor(cap), y = factor(turbine_height_m), size = load_in_gw, color = sector), aes = 0.2)+
    scale_size_continuous(name = 'Potential Customer Load (GW)', range = c(4,12))+
    theme_few()+
    facet_wrap(~sector,scales="free_y")+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(values = sector_fil) +
    #scale_size_continuous()+
    scale_y_discrete(name ='Turbine Height')+
    scale_x_discrete(name ='Optimal Size Turbine for Customer (kW)')+
    theme(axis.text.x = element_text(angle = 45, hjust = 1))+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    guides(color = FALSE, fill=FALSE)+
    ggtitle('What Height-Size Combinations are Most-Prefered?')
  height_picked$scenario<-scen_name
  write.csv(height_picked,paste0(runpath,'/height_selected_trends.csv'),row.names = FALSE)
  return(p)
}

national_pp_line<-function(df,scen_name){
  # Median payback period over time and sector
  g = group_by(df, year, sector)
  data = collect(summarise(g, 
                           uql = r_quantile(array_agg(payback_period), 0.95),
                           median = r_quantile(array_agg(payback_period), 0.5),
                           lql = r_quantile(array_agg(payback_period), 0.05)
  )
  )
  
  p<-ggplot(data, aes(x = year, y = median, ymin = lql, ymax = uql, color = sector, fill = sector), size = 0.75)+
    geom_smooth(stat = 'identity')+
    geom_line()+
    facet_wrap(~sector)+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(values = sector_fil) +
    scale_y_continuous(name ='Payback Period (years)', lim = c(0,30))+
    scale_x_continuous(name ='Year')+
    theme_few()+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    guides(color = FALSE, fill=FALSE)+
    ggtitle('National Payback Period (Median and Inner-Quartile Range)')
  data$scenario<-scen_name
  write.csv(data,paste0(runpath,'/payback_period_trends.csv'),row.names = FALSE)
  return(p)
}

diffusion_trends<-function(df,runpath,scen_name){
  # Diffusion trends
  g = group_by(df, year, sector)
  # We have no way calculating CFs for existing capacity, so assume it had a 23% capacity factor
  data = collect(summarise(g, nat_installed_capacity_gw  = sum(installed_capacity)/1e6, 
                           nat_market_share = sum(number_of_adopters)/sum(customers_in_bin), 
                           nat_max_market_share = mean(max_market_share),
                           nat_market_value = sum(market_value),
                           nat_generation_kwh = sum(((number_of_adopters-initial_number_of_adopters) * aep) + (0.23 * 8760 * initial_capacity_mw * 1000)), 
                           nat_number_of_adopters = sum(number_of_adopters)
  )
  )
  data<-melt(data=data,id.vars=c('year','sector'))
  data$scenario<-scen_name
  write.csv(data,paste0(runpath,'/diffusion_trends.csv'),row.names = FALSE)
  
  #' National market share trends
  national_adopters_trends_bar<-ggplot(subset(data, variable %in% c('nat_market_share', 'nat_max_market_share')), 
                                       aes(x = year, y = value, color = sector, linetype = variable))+
    geom_line(size = 0.75)+
    facet_wrap(~sector)+
    theme_few()+
    geom_line()+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(values = sector_fil) +
    scale_y_continuous(name ='Market Share (% of adopters in pop bin)', labels = percent)+
    scale_x_continuous(name ='Year')+
    expand_limits(y=0)+
    theme(strip.text.x = element_text(size=12, angle=0))+
    guides(color = FALSE)+
    ggtitle('National Adoption Trends')
  
  national_installed_capacity_bar<-ggplot(subset(data, variable %in% c("nat_installed_capacity_gw")), 
                                          aes(x = factor(year), fill = sector, weight = value))+
    geom_bar()+
    theme_few()+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(name = 'Sector', values = sector_fil) +
    scale_y_continuous(name ='National Installed Capacity (GW)', labels = comma)+
    expand_limits(weight=0)+
    scale_x_discrete(name ='Year')+
    theme(strip.text.x = element_text(size=12, angle=0))+
    ggtitle('National Installed Capacity (GW)')
  
  national_num_of_adopters_bar<-ggplot(subset(data, variable %in% c("nat_number_of_adopters")), 
                                       aes(x = factor(year), fill = sector, weight = value))+
    geom_bar()+
    theme_few()+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(name = 'Sector', values = sector_fil) +
    scale_y_continuous(name ='Number of Adopters', labels = comma)+
    expand_limits(weight=0)+
    scale_x_discrete(name ='Year')+
    theme(strip.text.x = element_text(size=12, angle=0))+
    ggtitle('National Number of Adopters')  
  
  national_market_cap_bar<-ggplot(subset(data, variable %in% c("nat_market_value")), 
                                  aes(x = factor(year), fill = sector, weight = value/1e9))+
    geom_bar()+
    theme_few()+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(name = 'Sector', values = sector_fil) +
    scale_y_continuous(name ='Value of Installed Capacity (Billion $)', labels = comma)+
    expand_limits(weight=0)+
    scale_x_discrete(name ='Year')+
    theme(strip.text.x = element_text(size=12, angle=0))+
    ggtitle('National Value of Installed Capacity (Billion $)')
  
  national_generation_bar<-ggplot(subset(data, variable %in% c("nat_generation_kwh")), 
                                  aes(x = factor(year), fill = sector, weight = value/1e9))+
    geom_bar()+
    theme_few()+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(name = 'Sector', values = sector_fil) +
    scale_y_continuous(name ='National Annual Generation (TWh)', labels = comma)+
    expand_limits(weight=0)+
    scale_x_discrete(name ='Year')+
    theme(strip.text.x = element_text(size=12, angle=0))+
    ggtitle('National Annual Generation (TWh)')  
  
  list("national_installed_capacity_bar" = national_installed_capacity_bar,
       "national_adopters_trends_bar" = national_adopters_trends_bar,
       "national_num_of_adopters_bar" = national_num_of_adopters_bar,
       "national_market_cap_bar" = national_market_cap_bar,
       "national_generation_bar" = national_generation_bar)
}


scenario_opts_table<-function(con){
  table<-dbGetQuery(con,"select * from wind_ds.scenario_options")
  names(table) <- unlist(lapply(names(table),simpleCap))
  table<-melt(table, id.vars = c(),variable.name='Switch',value.name="Value")
  print_table(table, caption = 'Scenario Options')    
}

# Shortcut to print tables in nicely formatted HTML
print_table <- function(...){
  print(xtable(...), type = "html", include.rownames = FALSE, caption.placement = "top")
}

national_installed_capacity_by_turb_size_bar<-function(df){
  g = group_by(df, year, sector, nameplate_capacity_kw)
  data<- collect(summarise(g, 
                           nat_installed_capacity  = sum(installed_capacity)/1e6
  )
  )
  
  # order the data correctly
  data = data[order(data$year,data$nameplate_capacity_kw),]
  colourCount = length(unique(data$nameplate_capacity_kw))
  getPalette = colorRampPalette(brewer.pal(9, "YlOrRd"))
  
  ggplot(data, aes(x = year, fill = factor(nameplate_capacity_kw), y = nat_installed_capacity), color = 'black')+
    facet_wrap(~sector,scales="free_y")+
    geom_area()+
    geom_line(aes(ymax = nameplate_capacity_kw), position = 'stack')+
    theme_few()+
    scale_fill_manual(name = 'Turbine Size', values = getPalette(colourCount))+#, values = sector_fil) +
    scale_y_continuous(name ='National Installed Capacity (GW)')+#, labels = comma)+
    theme(strip.text.x = element_text(size = 12, angle = 0))+
    theme(axis.text.x = element_text(angle = 45, hjust = 1))+
    ggtitle('National Installed Capacity by Turbine Size (GW)')
}


lcoe_boxplot<-function(df){
  data = collect(select(df,year,lcoe,sector))
  # Boxplot of LCOE over time, faceted by sector
  p<-ggplot(data, aes(x = factor(year), y = lcoe, fill = sector))+
    geom_boxplot()+
    facet_wrap(~sector)+
    scale_y_continuous(name = 'LCOE (c/kWh)',lim = c(0,100))+
    scale_x_discrete(name = 'Year')+
    ggtitle('Cost of Energy by Sector For All Sites Modeled')+
    scale_fill_manual(name = 'Sector', values = sector_fil)+
    theme_few()
  # aggregate summary stas on median and iqr to save to csv
  g = group_by(df,year,sector)
  out_data = collect(summarise(g, 
                               median = r_median(array_agg(lcoe)), 
                               lql = r_quantile(array_agg(lcoe), .25), 
                               uql = r_quantile(array_agg(lcoe), .75)
  )
  )
  write.csv(out_data,paste0(runpath,'/lcoe_trends.csv'),row.names = FALSE)
  return(p)
}

lcoe_cdf<-function(df){
  # CDF of lcoe for the first and final model year, faceted by sector. Note that
  # CDF is of bins, and is not weighted by # of custs in bin
  
  start_year = as.numeric(collect(summarise(df, min(year))))
  end_year = as.numeric(collect(summarise(df, max(year))))
  yrs = c(start_year, end_year)
  
  # filter the data to rows in the start or end year
  f = select(filter(df, year %in% yrs),year, sector, lcoe, elec_rate_cents_per_kwh)
  data = collect(f)
  g = group_by(f, year, sector)
  prices = collect(summarise(g,
                             price = r_median(array_agg(elec_rate_cents_per_kwh))
  )
  )
  
  
  ggplot(data=data,aes(x = lcoe, colour = factor(year)))+
    stat_ecdf()+
    geom_vline(data = prices, aes(xintercept = price, color = factor(year)))+
    #geom_vline(data = prices, aes(xintercept = price, color = factor(year)))+
    facet_wrap(~sector)+
    coord_cartesian(xlim = c(0,60))+
    theme_few()+
    ggtitle('Cumulative Probability of Site LCOE (c/kWh)\n Vertical Lines are median retail elec prices')+
    scale_x_continuous(name = 'Levelized Cost of Energy (c/kWh)')+
    scale_y_continuous(name = 'Cumulative Probability', label = percent)+
    scale_color_discrete(name = 'Model Years')
}



diffusion_all_map <- function(df){
  # aggregate the data
  g = group_by(df, state_abbr, year)
  diffusion_all = collect(summarise(g,
                                    Market.Share = sum(number_of_adopters)/sum(customers_in_bin)*100,
                                    Market.Value = sum(market_value),
                                    Number.of.Adopters = sum(number_of_adopters),
                                    Installed.Capacity = sum(installed_capacity)/1000,
                                    Annual.Generation =  sum(((number_of_adopters-initial_number_of_adopters) * aep) + (initial_capacity_mw*1000))/1e6
                                  )
                          )
  # reset variable names
  names(diffusion_all)[1:2] = c('State','Year')
  # make sure states are treated as character and not factor
  diffusion_all$State = as.character(diffusion_all$State)
  # create the map
  map = anim_choro_multi(diffusion_all, 'State', 
                         c('Market.Share','Market.Value', 'Number.of.Adopters', 'Installed.Capacity', 'Annual.Generation'),
                         pals = list(Market.Share = 'Blues', Market.Value = 'Greens', Number.of.Adopters = 'Purples', Installed.Capacity = 'Reds', Annual.Generation = 'YlOrRd'),
                         ncuts = list(Market.Share = 5, Market.Value = 5, Number.of.Adopters = 5, Installed.Capacity = 5, Annual.Generation = 5), 
                         classification = 'quantile',
                         height = 400, width = 800, scope = 'usa', label_precision = 0, big.mark = ',',
                         legend = T, labels = T, 
                         slider_var = 'Year', slider_step = 2, map_title = 'Diffusion (Total)', horizontal_legend = F, slider_width = 300,
                         legend_titles = list(Market.Share = 'Market Share (%)', Market.Value = 'Market Value ($)',
                                              Number.of.Adopters = 'Number of Adopters (Count)', Installed.Capacity = 'Installed Capacity (mw)',
                                              Annual.Generation = 'Annual Generation (GWh)'))
  # save the map
  showIframeSrc(map, cdn = T)
}


diffusion_sectors_map <- function(df){
  # aggregate the data
  # get the unique sectors in the table
  sectors = collect(summarise(df, distinct(sector)))[,1]
  for (sector in sectors){
    f = filter(df, sector == sector)
    g = group_by(df, state_abbr, year)
    diffusion_sector = collect(summarise(g,
                                         Market.Share = sum(number_of_adopters)/sum(customers_in_bin)*100,
                                         Market.Value = sum(market_value),
                                         Number.of.Adopters = sum(number_of_adopters),
                                         Installed.Capacity = sum(installed_capacity)/1000,
                                         Annual.Generation = sum(((number_of_adopters-initial_number_of_adopters) * aep) + (initial_capacity_mw*1000))/1e6
                                        )
                              )

    # reset variable names
    names(diffusion_sector)[1:2] = c('State','Year')
    # make sure states are treated as character and not factor
    diffusion_sector$State = as.character(diffusion_sector$State)
    # create the map
    map = anim_choro_multi(diffusion_sector, 'State', 
                           c('Market.Share','Market.Value', 'Number.of.Adopters', 'Installed.Capacity', 'Annual.Generation'),
                           pals = list(Market.Share = 'Blues', Market.Value = 'Greens', Number.of.Adopters = 'Purples', Installed.Capacity = 'Reds', Annual.Generation = 'YlOrRd'),
                           ncuts = list(Market.Share = 5, Market.Value = 5, Number.of.Adopters = 5, Installed.Capacity = 5, Annual.Generation = 5), 
                           classification = 'quantile',
                           height = 400, width = 800, scope = 'usa', label_precision = 0, big.mark = ',',
                           legend = T, labels = T, 
                           slider_var = 'Year', slider_step = 2, map_title = sprintf('Diffusion (%s)',toProper(sector)), horizontal_legend = F, slider_width = 300,
                           legend_titles = list(Market.Share = 'Market Share (%)', Market.Value = 'Market Value ($)',
                                                Number.of.Adopters = 'Number of Adopters (Count)', Installed.Capacity = 'Installed Capacity (mw)',
                                                Annual.Generation = 'Annual Generation (GWh)'))
    # save the map
    showIframeSrc(map, cdn = T)
  }
#   return(iframes)
}



###### Functions for the batch-mode scenario analysis ######################

get_csv_data<-function(scen_folders, file_name){
  df<-data.frame()
  for(path in scen_folders){
    tmp<-read.csv(paste0(path,'/',file_name,'.csv'))
    df<-rbind(df,tmp)
  }
  return(df)
}


all_sectors_diff_trends<-function(df){
  df<-ddply(df,.(year,variable, scenario), summarise, value = sum(value))
  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    ggtitle('National Diffusion Trends (All Sectors)')
}

res_diff_trends<-function(df){
  df<-subset(df, sector ==  "residential")
  if(nrow(df) == 0){
    print('No Residential Installations Modeled')
  } else {
  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    expand_limits(y=0)+
    ggtitle('National Residential Diffusion Trends \n Note different axis Limits than above')
  }
}

com_diff_trends<-function(df){
  df<-subset(df, sector ==  "commercial")
  if(nrow(df) == 0){
    print('No Commercial Installations Modeled')
  } else {
  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    expand_limits(y=0)+
    ggtitle('National Commercial Diffusion Trends \n Note different axis Limits than above')
  }
}

ind_diff_trends<-function(df){
  df<-subset(df, sector ==  "industrial")
  if(nrow(df) == 0){
    print('No Industrial Installations Modeled')
  } else {
  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    expand_limits(y=0)+
    ggtitle('National Industrial Diffusion Trends \n Note different axis Limits than above')
  }
}

diff_trends_table<-function(diff_trends){
  
  nat_trends_by_sector<-dcast(diff_trends, variable + sector + scenario ~ year)
  nat_trends_by_sector$variable<-gsub(pattern='nat',replacement='National',x=nat_trends_by_sector$variable)
  nat_trends_by_sector$variable<-gsub(pattern='_',replacement=' ',x=nat_trends_by_sector$variable)
  
  nat_trends<-dcast(diff_trends, variable + scenario ~ year, fun.aggregate= sum)
  nat_trends$variable<-gsub(pattern='nat',replacement='National',x=nat_trends$variable)
  nat_trends$variable<-gsub(pattern='_',replacement=' ',x=nat_trends$variable)
  
  print_table(nat_trends, caption = 'National Diffusion Trends')
  
  for(trend in unique(nat_trends_by_sector$variable)){
    tab<-subset(nat_trends_by_sector, variable == trend)
    tab<-tab[,names(tab) != 'variable']
    print_table(tab, caption = trend)
  } 
}

turb_trends_hist<-function(df){
  # What size system are customers selecting in 2014?
  df1<-subset(df, year == min(year))
  df2<-subset(df, year == max(year))
  
  p1<-ggplot(df1, aes(x = factor(cap), weight = p, fill = scenario))+
    geom_histogram(position = 'dodge')+
    facet_wrap(~sector)+
    theme_few()+
    scale_y_continuous(name ='Percent of Customers Selecting Turbine Size', labels = percent)+
    scale_x_discrete(name ='Optimal Size Turbine for Customer (kW)')+
    #scale_color_manual(values = sector_col) +
    #scale_fill_manual(values = sector_fil) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    #guides(color = FALSE, fill=FALSE)+
    ggtitle(sprintf('Size of Turbines Considered in %s', min(df$year)))
  
  p2<-ggplot(df2, aes(x = factor(cap), weight = p, fill = scenario))+
    geom_histogram(position = 'dodge')+
    facet_wrap(~sector)+
    theme_few()+
    scale_y_continuous(name ='Percent of Customers Selecting Turbine Size', labels = percent)+
    scale_x_discrete(name ='Optimal Size Turbine for Customer (kW)')+
    #scale_color_manual(values = sector_col) +
    #scale_fill_manual(values = sector_fil) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    #guides(color = FALSE, fill=FALSE)+
    ggtitle(sprintf('Size of Turbines Considered in %s', max(df$year)))

  out = list('p1' = p1, 'p2' = p2)  
return(out)
}

pp_trends_ribbon<-function(df){
  # Median payback period over time and sector  
  p<-ggplot(df, aes(x = year, y = median, ymin = lql, ymax = uql, color = scenario, fill = scenario), size = 0.75)+
    geom_smooth(stat = 'identity', alpha = 0.4)+
    geom_line()+
    facet_wrap(~sector)+
    scale_y_continuous(name ='Payback Period (years)', lim = c(0,40))+
    scale_x_continuous(name ='Year')+
    theme_few()+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    ggtitle('National Payback Period (Median and Inner-Quartile Range)')
  return(p)
}



