library(ggplot2)
library(plyr)
library(maps)
library(scales)
library(data.table)
library(knitr)
library(RColorBrewer)
library(ggthemes)
library(reshape2)
library(xtable)
library(RPostgreSQL)

# use for testing/debugging only:
# setwd('/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/python')

source("../r/graphics/output_funcs.R")
source('../r/maps/map_functions.R')

runpath<-commandArgs(TRUE)[1]
scen_name<-commandArgs(TRUE)[2]

con<-make_con(driver = "PostgreSQL", host = 'gispgdb', dbname="dav-gis", user = 'mgleason', password = 'mgleason')
# sql = "SELECT * FROM wind_ds.outputs_all;"
sql = "SELECT 'residential'::text as sector, 

      a.gid, a.year, a.customer_expec_elec_rates, a.ownership_model, a.loan_term_yrs, 
      a.loan_rate, a.down_payment, a.discount_rate, a.tax_rate, a.length_of_irr_analysis_yrs, 
      a.market_share_last_year, a.number_of_adopters_last_year, a.installed_capacity_last_year, 
      a.market_value_last_year, a.value_of_increment, a.value_of_pbi_fit, 
      a.value_of_ptc, a.pbi_fit_length, a.ptc_length, a.value_of_rebate, a.value_of_tax_credit_or_deduction, 
      a.cap, a.ic, a.aep, a.payback_period, a.lcoe, a.payback_key, a.max_market_share, 
      a.diffusion_market_share, a.new_market_share, a.new_adopters, a.new_capacity, 
      a.new_market_value, a.market_share, a.number_of_adopters, a.installed_capacity, 
      a.market_value,
      
      b.county_id, b.state_abbr, b.census_division_abbr, b.utility_type, 
      b.census_region, b.row_number, b.max_height, b.elec_rate_cents_per_kwh, 
      b.carbon_price_cents_per_kwh, b.cap_cost_multiplier, b.fixed_om_dollars_per_kw_per_yr, 
      b.variable_om_dollars_per_kwh, b.installed_costs_dollars_per_kw, 
      b.ann_cons_kwh, b.prob, b.weight, b.customers_in_bin, b.initial_customers_in_bin, 
      b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
      b.nem_system_limit_kw, b.excess_generation_factor, b.i, b.j, b.cf_bin, 
      b.aep_scale_factor, b.derate_factor, b.naep, b.nameplate_capacity_kw, 
      b.power_curve_id, b.turbine_height_m, b.scoe,

      c.initial_market_share, c.initial_number_of_adopters,
      c.initial_capacity_mw
      
      FROM wind_ds.outputs_res a

      LEFT JOIN wind_ds.pt_res_best_option_each_year b
      ON a.gid = b.gid
      and a.year = b.year
      
      LEFT JOIN wind_ds.pt_res_initial_market_shares c
      ON a.gid = c.gid

      UNION ALL
      
      SELECT 'commercial'::text as sector, 
      
      a.gid, a.year, a.customer_expec_elec_rates, a.ownership_model, a.loan_term_yrs, 
      a.loan_rate, a.down_payment, a.discount_rate, a.tax_rate, a.length_of_irr_analysis_yrs, 
      a.market_share_last_year, a.number_of_adopters_last_year, a.installed_capacity_last_year, 
      a.market_value_last_year, a.value_of_increment, a.value_of_pbi_fit, 
      a.value_of_ptc, a.pbi_fit_length, a.ptc_length, a.value_of_rebate, a.value_of_tax_credit_or_deduction, 
      a.cap, a.ic, a.aep, a.payback_period, a.lcoe, a.payback_key, a.max_market_share, 
      a.diffusion_market_share, a.new_market_share, a.new_adopters, a.new_capacity, 
      a.new_market_value, a.market_share, a.number_of_adopters, a.installed_capacity, 
      a.market_value,
      
      b.county_id, b.state_abbr, b.census_division_abbr, b.utility_type, 
      b.census_region, b.row_number, b.max_height, b.elec_rate_cents_per_kwh, 
      b.carbon_price_cents_per_kwh, b.cap_cost_multiplier, b.fixed_om_dollars_per_kw_per_yr, 
      b.variable_om_dollars_per_kwh, b.installed_costs_dollars_per_kw, 
      b.ann_cons_kwh, b.prob, b.weight, b.customers_in_bin, b.initial_customers_in_bin, 
      b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
      b.nem_system_limit_kw, b.excess_generation_factor, b.i, b.j, b.cf_bin, 
      b.aep_scale_factor, b.derate_factor, b.naep, b.nameplate_capacity_kw, 
      b.power_curve_id, b.turbine_height_m, b.scoe,

      c.initial_market_share, c.initial_number_of_adopters,
      c.initial_capacity_mw
      
      FROM wind_ds.outputs_com a
      
      LEFT JOIN wind_ds.pt_com_best_option_each_year b
      ON a.gid = b.gid
      and a.year = b.year

      LEFT JOIN wind_ds.pt_com_initial_market_shares c
      ON a.gid = c.gid
      
      UNION ALL
      SELECT 'industrial'::text as sector, 
      
      a.gid, a.year, a.customer_expec_elec_rates, a.ownership_model, a.loan_term_yrs, 
      a.loan_rate, a.down_payment, a.discount_rate, a.tax_rate, a.length_of_irr_analysis_yrs, 
      a.market_share_last_year, a.number_of_adopters_last_year, a.installed_capacity_last_year, 
      a.market_value_last_year, a.value_of_increment, a.value_of_pbi_fit, 
      a.value_of_ptc, a.pbi_fit_length, a.ptc_length, a.value_of_rebate, a.value_of_tax_credit_or_deduction, 
      a.cap, a.ic, a.aep, a.payback_period, a.lcoe, a.payback_key, a.max_market_share, 
      a.diffusion_market_share, a.new_market_share, a.new_adopters, a.new_capacity, 
      a.new_market_value, a.market_share, a.number_of_adopters, a.installed_capacity, 
      a.market_value,
      
      b.county_id, b.state_abbr, b.census_division_abbr, b.utility_type, 
      b.census_region, b.row_number, b.max_height, b.elec_rate_cents_per_kwh, 
      b.carbon_price_cents_per_kwh, b.cap_cost_multiplier, b.fixed_om_dollars_per_kw_per_yr, 
      b.variable_om_dollars_per_kwh, b.installed_costs_dollars_per_kw, 
      b.ann_cons_kwh, b.prob, b.weight, b.customers_in_bin, b.initial_customers_in_bin, 
      b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
      b.nem_system_limit_kw, b.excess_generation_factor, b.i, b.j, b.cf_bin, 
      b.aep_scale_factor, b.derate_factor, b.naep, b.nameplate_capacity_kw, 
      b.power_curve_id, b.turbine_height_m, b.scoe,

      c.initial_market_share, c.initial_number_of_adopters,
      c.initial_capacity_mw      
      
      FROM wind_ds.outputs_ind a

      LEFT JOIN wind_ds.pt_ind_best_option_each_year b
      ON a.gid = b.gid
      and a.year = b.year

      LEFT JOIN wind_ds.pt_ind_initial_market_shares c
      ON a.gid = c.gid"

df =  dbGetQuery(con,sql)

opts_knit$set(base.dir = runpath)
knit2html("../r/graphics/plot_outputs.md", output = paste0(runpath,"/DG Wind report.html"), title = "DG Wind report", stylesheet = "../r/graphics/plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc"))
dbDisconnect(con)


ggplot(data = df, aes(x = excess_generation_factor, color = sector))+
  stat_ecdf()

ggplot(data = data.frame(x = rnorm(n=10000,mean=0.5,sd=0.1)), aes(x))+
  stat_ecdf()




# lcoe_boxplot<-function(df){
# # Boxplot of LCOE over time, faceted by sector
# ggplot(df, aes(x = factor(year), y = lcoe, fill = sector))+
#   geom_boxplot()+
#   facet_wrap(~sector)+
#   scale_y_continuous(name = 'LCOE (c/kWh)',lim = c(0,100))+
#   scale_x_discrete(name = 'Year')+
#   ggtitle('Cost of Energy by Sector For All Sites Modeled')+
#   scale_fill_manual(name = 'Sector', values = sector_fil)+
#   theme_few()
# 
# data<-ddply(df,.(year,sector),summarise, median = median(lcoe), lql = quantile(lcoe, .25), uql = quantile(lcoe, .75))
# write.csv(data, lcoe_trend.csv)
# }
# 
# lcoe_cdf<-function(df){
# # CDF of lcoe for the first and final model year, faceted by sector. Note that
# # CDF is of bins, and is not weighted by # of custs in bin
# yrs<-c(min(df$year),max(df$year))
# data<-subset(df, year %in% yrs)
# prices<-ddply(data, .(year),summarise, price = median(elec_rate_cents_per_kwh))
# 
# ggplot(data=data,aes(x = lcoe, colour = factor(year)))+
#   stat_ecdf()+
#   geom_vline(data = ddply(data, .(year, sector),summarise, price = median(elec_rate_cents_per_kwh)), aes(xintercept = price, color = factor(year)))+
#   #geom_vline(data = prices, aes(xintercept = price, color = factor(year)))+
#   facet_wrap(~sector)+
#   coord_cartesian(xlim = c(0,60))+
#   theme_few()+
#   ggtitle('Cumulative Probability of Site LCOE (c/kWh)\n Vertical Lines are median retail elec prices')+
#   scale_x_continuous(name = 'Levelized Cost of Energy (c/kWh)')+
#   scale_y_continuous(name = 'Cumulative Probability', label = percent)+
#   scale_color_discrete(name = 'Model Years')
# }






















# lcoe_ribbon<-function(df){ 
# ggplot(data, aes(x = year, y = median, ymin = lql, max = uql, color = sector, fill = sector))+
#   geom_ribbon(alpha = 0.4)+
#   theme_few()+
#   ggtitle('Trends in LCOE by Sector for All Sites')+
#   scale_x_continuous(name = 'Year')+
#   scale_y_continuous(name = 'LCOE (c/kWh)')+
#   scale_fill_manual(name = 'LCOE Interquartile Range', values = sector_fil)+
#   scale_color_manual(values = sector_fil)+
#   guides(color = F)
# }

# ## Under development
# dr<- 0.1
# n <- 30
# d = data.frame()
# for(cost in c(0,0.1, 0.2, 0.3, 0.4, 0.5, 0.6,0.7)){
#   tmp<-data.frame(npc = seq(0,10,.01))
#   tmp$cf <- 1000 * tmp$npc / (cost * 8760 * ((1 - (1 + dr)^-n)/dr))
#   tmp$cf_max <- 1000 * tmp$npc / (cost * 8760 * ((1 - (1 + dr)^-n)/dr))
#   tmp$cf_min <- 1000 * tmp$npc / ((cost + 0.1) * 8760 * ((1 - (1 + dr)^-n)/dr))
#   tmp$lcoe <- cost
#   d <- rbind(d,tmp)
# }
# 
# d[is.na(d)] <- 0.5
# d[d$cf_max > 0.5, 'cf_max'] <- 0.5
# d[d$cf_min > 0.5, 'cf_min'] <- 0.5
# d[d$lcoe == 0.7 , 'cf_min'] <- 0
# 
# ggplot()+
#   geom_ribbon(data = d, aes(x = npc, y = cf, ymin = cf_min, ymax = cf_max, fill = factor(lcoe), alpha = 0.5))+
#   theme_few()+
#   scale_x_continuous(name = 'Net Present Cost ($/W)', limits = c(0,8))+
#   scale_y_continuous(name = 'Annual Capacity Factor', limits = c(0,.5))+
#   scale_fill_brewer(name = 'LCOE Range ($/kWh)', 
#                     labels=c('< 0.1' ,"0.1 - 0.2", "0.2 - 0.3", "0.3 - 0.4", "0.4 - 0.5", "0.5 - 0.6", "0.6 - 0.7", "> 0.7"), 
#                     palette = 'Spectral')+
#   #geom_point(data = df[sample(nrow(subset(df, year = 2014)), 1000),], aes(x = 0.001 * installed_costs_dollars_per_kw, y = naep/8760), color = 'black', size = 1)
#   geom_point(data = df[sample(nrow(subset(df, year = 2014)), 100000),], aes(x = 0.001 * installed_costs_dollars_per_kw, y = naep/8760), color = 'black', size = 1)
# 
# ##


###TODO:

# x: CF vs y: installed capacity (binned)
#state_lookup<-read.csv('state_lookup.csv',header = T, sep = ',', stringsAsFactor = F)
# pp_state<-subset(df, year == min(df$year))
# 
# #' Payback period by state - First Year
# pp_state<-subset(df, year == min(df$year))
# pp_state<-ddply(df, .(sector, state_abbr), summarise, 
#                 pp_95 = quantile(payback_period, 0.95),
#                 mean = quantile(payback_period, 0.5),
#                 pp_5 = quantile(payback_period, 0.05))
# 
# pp_state<- join(pp_state, state_lookup)
# us_state_map <- map_data('state')
# map_data <- join(us_state_map, pp_state, by='region', type='left')
# map_data <- map_data[order(map_data$order), ]
# 
# qplot(long, lat, data=map_data, geom="polygon", group=group, fill=mean)+
#   theme_bw() + labs(x="", y="", fill="")+
#   scale_fill_gradient(low='#EEEEEE', high='darkgreen')+
#   facet_wrap(~sector)+
#   ggtitle(paste0('Mean Payback Period by State and Sector in ',min(df$year)))
# 
# #' Payback period by state - Last Year
# pp_state<-subset(df, year == max(df$year))
# pp_state<-ddply(df, .(sector, state_abbr), summarise, 
#                 pp_95 = quantile(payback_period, 0.95),
#                 mean = quantile(payback_period, 0.5),
#                 pp_5 = quantile(payback_period, 0.05))
# 
# pp_state<- join(pp_state, state_lookup)
# us_state_map <- map_data('state')
# map_data <- join(us_state_map, pp_state, by='region', type='left')
# map_data <- map_data[order(map_data$order), ]
# 
# qplot(long, lat, data=map_data, geom="polygon", group=group, fill=mean)+
#   theme_bw() + labs(x="", y="", fill="")+
#   scale_fill_gradient(low='#EEEEEE', high='darkgreen')+
#   facet_wrap(~sector)+
#   ggtitle(paste0('Mean Payback Period by State and Sector in ',max(df$year)))

# data$turb_size<-"Small: < 50 kW"
# data[data$nameplate_capacity_kw<=500 & data$nameplate_capacity_kw>50,'turb_size']<-'Mid: 51 - 500 kW' 
# data[data$nameplate_capacity_kw>500,'turb_size']<-'Large: 501 - 3,000 kW'
