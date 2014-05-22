sector_col <- c(residential = "#4daf4a", commercial = "#377eb8", industrial = "#e41a1c")
sector_fil <- c(residential = "#4daf4a", commercial = "#377eb8", industrial = "#e41a1c")
turb_size_fil <- c('Small: < 50 kW' = "#a1dab4", 'Mid: 51 - 500 kW' = "#41b6c4", 'Large: 501 - 3,000 kW' = "#253494") 
# ======================= DATA FUNCTIONS =================================================

make_con<-function(driver = "PostgreSQL", host = 'gispgdb', dbname="dav-gis", user = 'bsigrin', password = 'bsigrin'){
  # Make connection to dav-gis database
  dbConnect(dbDriver(driver), host = host, dbname = dbname, user = user, password = password)  
}

simpleCap <- function(x) {
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

dist_of_cap_selected<-function(df){
  # What size system are customers selecting in 2014?
  cap_picked<-subset(df, year == 2014)
  cap_picked<-ddply(cap_picked,.(cap,sector),summarise, cust_num = sum(customers_in_bin,na.rm=T))
  ggplot(cap_picked, aes(x = factor(cap), weight = cust_num, fill = sector))+
    geom_bar()+
    facet_wrap(~sector,scales="free_y")+
    theme_few()+
    scale_y_continuous(name ='Number of Customers', labels = comma)+
    scale_x_discrete(name ='Optimal Size Turbine for Customer (kW)')+
    scale_color_manual(values = sector_col) +
    scale_fill_manual(values = sector_fil) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    guides(color = FALSE, fill=FALSE)+
    ggtitle('Size of Turbines Being Considered\n Note: y-axis scales not equal')
}

dist_of_height_selected<-function(df){
  #What heights are prefered?
  height_picked <- subset(df, year == 2014)
  height_picked<-ddply(height_picked,.(cap,turbine_height_m,sector),summarise, load_in_gw = sum(load_kwh_in_bin,na.rm=T)/(1e6*8760))
  ggplot(height_picked)+
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
}

national_pp_line<-function(df){
  # Median payback period over time and sector
  data<-ddply(df, .(year, sector), summarise, 
              uql = quantile(payback_period, 0.95,na.rm=T),
              median = quantile(payback_period, 0.5,na.rm=T),
              lql = quantile(payback_period, 0.05,na.rm=T))
  
  ggplot(data, aes(x = year, y = median, ymin = lql, ymax = uql, color = sector, fill = sector), size = 0.75)+
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
}

diffusion_trends<-function(df,runpath,scen_name){
  # Diffusion trends
  starting_values = df[df$year == 2014, c('gid','number_of_adopters_last_year', 'installed_capacity_last_year', 'market_share_last_year','market_value_last_year')]
  names(starting_values)[2:5] = c('number_of_adopters_initial', 'installed_capacity_initial', 'market_share_initial','market_value_initial')
  df = merge(df,starting_values)
  data <- ddply(df, .(year, sector), summarise, 
                nat_installed_capacity  = sum(installed_capacity,na.rm=TRUE)/1e6, 
                nat_market_share = mean(market_share,na.rm=TRUE), 
                nat_max_market_share = mean(max_market_share,na.rm=TRUE),
                nat_market_value = sum(ic * number_of_adopters, na.rm = TRUE),
                nat_new_generation = sum((number_of_adopters-number_of_adopters_initial) * aep, na.rm = TRUE),
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
              nat_market_share = mean(market_share,na.rm=TRUE), 
              nat_max_market_share = mean(max_market_share,na.rm=TRUE),
              nat_market_value = sum(ic * number_of_adopters, na.rm = TRUE),
              nat_generation = sum(number_of_adopters * aep, na.rm = TRUE),
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

###### Functions for the batch-mode scenario analysis ######################

get_diffusion_trends_data<-function(scen_folders){
  diff_trends<-data.frame()
  for(p in scen_folders){
    tmp<-read.csv(paste0(p,'/diffusion_trends.csv'))
    diff_trends<-rbind(diff_trends,tmp)
  }
  return(diff_trends)
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
  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    ggtitle('National Residential Diffusion Trends')
}

com_diff_trends<-function(df){
  df<-subset(df, sector ==  "commercial")
  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    ggtitle('National Commercial Diffusion Trends')
}

com_diff_trends<-function(df){
  df<-subset(df, sector ==  "industrial")
  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    ggtitle('National Industrial Diffusion Trends')
}
