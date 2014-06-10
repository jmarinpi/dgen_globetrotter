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
  by_state<-ddply(df,.(year,state_abbr), function(d) round(mean(d[[val]],na.rm=T),digits=2))
  names(by_state)<-c('year','State',val)
  national<-ddply(df,.(year), function(d) round(mean(d[[val]],na.rm=T),digits=2))
  national$State<-'U.S'
  names(national)<-c('year',val,'State')
  by_state<-dcast(data = by_state,formula= State ~ year, value.var = val)
  national<-dcast(data = national,formula= State ~ year, value.var = val)
  rbind(national,by_state)
}

total_value_by_state_table<-function(df,val){
  # Create table of summed value by year and state. value is string of variable to take sum over. 
  by_state<-ddply(df,.(year,state_abbr), function(d) round(sum(d[[val]],na.rm=T),digits=2))
  names(by_state)<-c('year','State',val)
  national<-ddply(df,.(year), function(d) round(sum(d[[val]],na.rm=T),digits=2))
  national$State<-'U.S'
  names(national)<-c('year',val,'State')
  by_state<-dcast(data = by_state,formula= State ~ year, value.var = val)
  national<-dcast(data = national,formula= State ~ year, value.var = val)
  rbind(national,by_state)
}

create_report <- function(runpath) {
  knit2html("../r/graphics/plot_outputs.md", output = "DG Wind report.html", title = "DG Wind report", stylesheet = "plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc = yes"))
}



# ======================= GRAPHING FUNCTIONS =================================================

cf_by_sector_and_year<-function(df){
  # Median and inner-quartile range of CF over sector and year
  data<-ddply(df, .(year,sector), summarise, 
              tql = quantile(naep, 0.75,na.rm=T)/ 8760,
              median = quantile(naep, 0.5,na.rm=T)/ 8760,
              fql = quantile(naep, 0.25,na.rm=T)/ 8760)
  
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

cf_supply_curve<-function(df){
  #' National capacity factor supply curve
  data <- subset(df, year == min(df$year))
  data <- data[,c("naep", "load_kwh_in_bin")]
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
  data <- subset(df, year == min(df$year))
  data <- data[,c("elec_rate_cents_per_kwh", "load_kwh_in_bin")]
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
  cap_picked<-subset(df, year %in% c(min(df$year), max(df$year)))
  cap_picked<-ddply(cap_picked,.(cap,sector,year),summarise, cust_num = sum(customers_in_bin,na.rm=T))
  
  tmp<-ddply(cap_picked,.(sector,year),summarise, n = sum(cust_num))
  cap_picked<-merge(cap_picked,tmp)
  cap_picked<-transform(cap_picked, p = cust_num/n)
  
  p<-ggplot(cap_picked, aes(x = factor(cap), weight = p, fill = factor(year)))+
    geom_histogram(position = 'dodge')+
    facet_wrap(~sector)+
    theme_few()+
    scale_y_continuous(name ='Percent of Customers Selecting Turbine Size', labels = percent)+
    scale_x_discrete(name ='Optimal Size Turbine for Customer (kW)')+
    #scale_color_manual(values = sector_col) +
    scale_fill_manual(name = '')+
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
  height_picked <- subset(df, year == 2014)
  height_picked<-ddply(height_picked,.(cap,turbine_height_m,sector),summarise, load_in_gw = sum(load_kwh_in_bin,na.rm=T)/(1e6*8760))
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
  data<-ddply(df, .(year, sector), summarise, 
              uql = quantile(payback_period, 0.95,na.rm=T),
              median = quantile(payback_period, 0.5,na.rm=T),
              lql = quantile(payback_period, 0.05,na.rm=T))
  
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
  data <- ddply(df, .(year, sector), summarise, 
                nat_installed_capacity  = sum(installed_capacity,na.rm=TRUE)/1e6, 
                nat_market_share = sum(number_of_adopters, na.rm = TRUE)/sum(customers_in_bin, na.rm = TRUE), 
                nat_max_market_share = mean(max_market_share,na.rm=TRUE),
                nat_market_value = sum(market_value, na.rm = TRUE),
                nat_generation = sum(((number_of_adopters-initial_number_of_adopters) * aep) + (initial_capacity_mw*1000), na.rm = TRUE),
                nat_number_of_adopters = sum(number_of_adopters,na.rm=TRUE))
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
    theme(strip.text.x = element_text(size=12, angle=0))+
    guides(color = FALSE)+
    ggtitle('National Adoption Trends')
  
  national_installed_capacity_bar<-ggplot(subset(data, variable %in% c("nat_installed_capacity")), 
                                          aes(x = factor(year), fill = sector, weight = value))+
    geom_bar()+
    theme_few()+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(name = 'Sector', values = sector_fil) +
    scale_y_continuous(name ='National Installed Capacity (GW)', labels = comma)+
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
    scale_x_discrete(name ='Year')+
    theme(strip.text.x = element_text(size=12, angle=0))+
    ggtitle('National Value of Installed Capacity (Billion $)')
  
  national_generation_bar<-ggplot(subset(data, variable %in% c("nat_generation")), 
                                  aes(x = factor(year), fill = sector, weight = value/1e9))+
    geom_bar()+
    theme_few()+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(name = 'Sector', values = sector_fil) +
    scale_y_continuous(name ='National Annual Generation (TWh)', labels = comma)+
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
  data<-ddply(df, .(year, sector, nameplate_capacity_kw), summarise, 
              nat_installed_capacity  = sum(installed_capacity,na.rm=TRUE)/1e6, 
              nat_market_share =sum(number_of_adopters, na.rm = TRUE)/sum(customers_in_bin, na.rm = TRUE), 
              nat_max_market_share = mean(max_market_share,na.rm=TRUE),
              nat_market_value = sum(ic * number_of_adopters, na.rm = TRUE),
              nat_generation = sum(((number_of_adopters-initial_number_of_adopters) * aep) + (initial_capacity_mw*1000), na.rm = TRUE),
              nat_number_of_adopters = sum(number_of_adopters,na.rm=TRUE))
  
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
# Boxplot of LCOE over time, faceted by sector
p<-ggplot(df, aes(x = factor(year), y = lcoe, fill = sector))+
  geom_boxplot()+
  facet_wrap(~sector)+
  scale_y_continuous(name = 'LCOE (c/kWh)',lim = c(0,100))+
  scale_x_discrete(name = 'Year')+
  ggtitle('Cost of Energy by Sector For All Sites Modeled')+
  scale_fill_manual(name = 'Sector', values = sector_fil)+
  theme_few()

data<-ddply(df,.(year,sector),summarise, median = median(lcoe), lql = quantile(lcoe, .25), uql = quantile(lcoe, .75))
write.csv(data,paste0(runpath,'/lcoe_trends.csv'),row.names = FALSE)
return(p)
}

lcoe_cdf<-function(df){
# CDF of lcoe for the first and final model year, faceted by sector. Note that
# CDF is of bins, and is not weighted by # of custs in bin
yrs<-c(min(df$year),max(df$year))
data<-subset(df, year %in% yrs)
prices<-ddply(data, .(year),summarise, price = median(elec_rate_cents_per_kwh))

ggplot(data=data,aes(x = lcoe, colour = factor(year)))+
  stat_ecdf()+
  geom_vline(data = ddply(data, .(year, sector),summarise, price = median(elec_rate_cents_per_kwh)), aes(xintercept = price, color = factor(year)))+
  #geom_vline(data = prices, aes(xintercept = price, color = factor(year)))+
  facet_wrap(~sector)+
  coord_cartesian(xlim = c(0,60))+
  theme_few()+
  ggtitle('Cumulative Probability of Site LCOE (c/kWh)\n Vertical Lines are median retail elec prices')+
  scale_x_continuous(name = 'Levelized Cost of Energy (c/kWh)')+
  scale_y_continuous(name = 'Cumulative Probability', label = percent)+
  scale_color_discrete(name = 'Model Years')
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
    ggtitle('National Residential Diffusion Trends')
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
    ggtitle('National Commercial Diffusion Trends')
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
    ggtitle('National Industrial Diffusion Trends')
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

diffusion_all_map <- function(df, runpath){
  # aggregate the data
  diffusion_all = ddply(df, .(state_abbr,year), summarize,
                        Market.Share = sum(number_of_adopters)/sum(customers_in_bin)*100,
                        Market.Value = sum(market_value),
                        Number.of.Adopters = sum(number_of_adopters),
                        Installed.Capacity = sum(installed_capacity)/1000,
                        Annual.Generation =  sum(((number_of_adopters-initial_number_of_adopters) * aep) + (initial_capacity_mw*1000), na.rm = F)/1e6
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
  map$save(sprintf('%s/figure/diffusion_all.html', runpath), cdn =T)
  iframe = "<iframe src='./figure/diffusion_all.html' name='diffusion_all_map' height=600px width=1100px style='border:none;'></iframe>"
  return(iframe)
}


diffusion_sectors_map <- function(df, runpath){
  iframes = c()
  # aggregate the data
  for (sector in unique(df$sector)){
    diffusion_sector = ddply(df[df$sector == sector,], .(state_abbr,year), summarize,
                             Market.Share = sum(number_of_adopters)/sum(customers_in_bin)*100,
                             Market.Value = sum(market_value),
                             Number.of.Adopters = sum(number_of_adopters),
                             Installed.Capacity = sum(installed_capacity)/1000,
                             Annual.Generation = sum(((number_of_adopters-initial_number_of_adopters) * aep) + (initial_capacity_mw*1000), na.rm = F)/1e6
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
    map$save(sprintf('%s/figure/diffusion_%s.html', runpath, sector), cdn =T)     
    iframe = sprintf("<iframe src='./figure/diffusion_%s.html' name='diffusion_%s_map' height=600px width=1100px style='border:none;'></iframe>",sector,sector)
    iframes = c(iframes,iframe)
  }
  return(iframes)
  
}


