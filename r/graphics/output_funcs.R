sector_col <- c(residential = "#4daf4a", commercial = "#377eb8", industrial = "#e41a1c")
sector_fil <- c(residential = "#4daf4a", commercial = "#377eb8", industrial = "#e41a1c")

# ======================= DATA FUNCTIONS =================================================
mean_value_by_state_table<-function(df,val){
  # Create table of mean value by year and state. value is string of variable to take mean of. 
  by_state<-ddply(df,.(year,state_abbr), function(d) round(mean(d[[val]]),digits=2))
  names(by_state)<-c('year','State',val)
  national<-ddply(df,.(year), function(d) round(mean(d[[val]]),digits=2))
  national$State<-'U.S'
  names(national)<-c('year',val,'State')
  by_state<-dcast(data = by_state,formula= State ~ year, value.var = val)
  national<-dcast(data = national,formula= State ~ year, value.var = val)
  rbind(national,by_state)
}

total_value_by_state_table<-function(df,val){
  # Create table of summed value by year and state. value is string of variable to take sum over. 
  by_state<-ddply(df,.(year,state_abbr), function(d) round(sum(d[[val]]),digits=2))
  names(by_state)<-c('year','State',val)
  national<-ddply(df,.(year), function(d) round(sum(d[[val]]),digits=2))
  national$State<-'U.S'
  names(national)<-c('year',val,'State')
  by_state<-dcast(data = by_state,formula= State ~ year, value.var = val)
  national<-dcast(data = national,formula= State ~ year, value.var = val)
  rbind(national,by_state)
}

create_report <- function() {
  knit2html("plot_outputs.md", output = "plot_outputs.html", title = "DG Wind report", stylesheet = "plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"))
}



# ======================= GRAPHING FUNCTIONS =================================================

cf_by_sector_and_year<-function(df){
# Median and inner-quartile range of CF over sector and year
data<-ddply(df, .(year,sector), summarise, 
                tql = quantile(naep, 0.75)/ 8760,
                median = quantile(naep, 0.5)/ 8760,
                fql = quantile(naep, 0.25)/ 8760)

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
cap_picked<-ddply(cap_picked,.(cap,sector),summarise, cust_num = sum(customers_in_bin))
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
height_picked<-ddply(height_picked,.(cap,turbine_height_m,sector),summarise, load_in_gw = sum(load_kwh_in_bin)/(1e6*8760))
ggplot(height_picked)+
  geom_point(aes(x = factor(cap), y = factor(turbine_height_m), size = load_in_gw, color = sector), aes = 0.2)+
  scale_size_continuous(name = 'Potential Customer Load', range = c(4,12))+
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
                uql = quantile(payback_period, 0.95),
                median = quantile(payback_period, 0.5),
                lql = quantile(payback_period, 0.05))

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

diffusion_trends<-function(df){
# Diffusion trends
data <- ddply(df, .(year, sector), summarise, 
           installed_capacity  = sum(installed_capacity)/1e6, 
           market_share = mean(market_share), 
           max_market_share = mean(max_market_share), 
           number_of_adopters = sum(number_of_adopters))
data<-melt(data=data,id.vars=c('year','sector'))

#' National market share trends
national_adopters_trends_bar<-ggplot(subset(data, variable %in% c('market_share', 'max_market_share')), 
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

national_installed_capacity_bar<-ggplot(subset(data, variable %in% c("installed_capacity")), 
              aes(x = factor(year), fill = sector, weight = value))+
  geom_bar()+
  theme_few()+
  scale_color_manual(values = sector_col) +
  scale_fill_manual(name = 'Sector', values = sector_fil) +
  scale_y_continuous(name ='National Installed Capacity (GW)', labels = comma)+
  scale_x_discrete(name ='Year')+
  theme(strip.text.x = element_text(size=12, angle=0))+
  ggtitle('National Installed Capacity (GW)')

national_num_of_adopters_bar<-ggplot(subset(data, variable %in% c("number_of_adopters")), 
              aes(x = factor(year), fill = sector, weight = value))+
  geom_bar()+
  theme_few()+
  scale_color_manual(values = sector_col) +
  scale_fill_manual(name = 'Sector', values = sector_fil) +
  scale_y_continuous(name ='Number of Adopters', labels = comma)+
  scale_x_discrete(name ='Year')+
  theme(strip.text.x = element_text(size=12, angle=0))+
  ggtitle('National Number of Adopters')  
  
list("national_installed_capacity_bar" = national_installed_capacity_bar,
          "national_adopters_trends_bar" = national_adopters_trends_bar,
     "national_num_of_adopters_bar" = national_num_of_adopters_bar)
}

# Shortcut to print tables in nicely formatted HTML
print_table <- function(...){
  print(xtable(...), type = "html", include.rownames = FALSE, caption.placement = "top")
}
