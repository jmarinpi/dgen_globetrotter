```{r options, echo=FALSE}
  # Set options
  opts_chunk$set(echo = FALSE, warning = FALSE, fig.width = 12, fig.height = 8, cache = FALSE, fig.align = "center", results = "asis")
```



# Diffusion
```{r Diffusion}
options(warn=-1)
out<-diffusion_trends(df,runpath,scen_name)
grid.draw(out$national_installed_capacity_bar)
g = arrangeGrob(out$national_installed_capacity_bar)
ggsave(sprintf('%s/total_capacity.png', ppt_dir), g, dpi = 600, height = 5, width = 8, units = 'in' )
grid.newpage()

grid.draw(out$national_num_of_adopters_bar)
g = arrangeGrob(out$national_num_of_adopters_bar)
ggsave(sprintf('%s/total_adopters.png', ppt_dir), g, dpi = 600, height = 5, width = 8, units = 'in' )
grid.newpage()

grid.draw(out$national_market_cap_bar)
g = arrangeGrob(out$national_market_cap_bar)
ggsave(sprintf('%s/total_market.png', ppt_dir), g, dpi = 600, height = 5, width = 8, units = 'in' )
grid.newpage()

grid.draw(out$national_generation_bar)
g = arrangeGrob(out$national_generation_bar)
ggsave(sprintf('%s/total_generation.png', ppt_dir), g, dpi = 600, height = 5, width = 8, units = 'in' )
grid.newpage()

print_table(total_value_by_state_table(df,'installed_capacity', unit_factor = 0.001), caption = "Installed Capacity (MW)")
print_table(mean_value_by_state_table(df,'market_share'), caption = "Mean Adoption Share")
out$national_adopters_trends_bar
ggsave(sprintf('%s/mms_and_ms.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
national_installed_capacity_by_system_size_bar(df,tech)
ggsave(sprintf('%s/cumulative_capacity_by_system_size.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
```

# Diffusion Maps
```{r Diffusion Maps}
diffusion_all_map(df)
diffusion_sectors_map(df)
```
# Economics
```{r Economics}
options(warn=-1)
national_econ_attractiveness_line(df,scen_name)
ggsave(sprintf('%s/payback_and_mbs.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
print_table(mean_value_by_state_table(filter(df, metric == 'payback_period'),'metric_value'), caption = "Mean Payback Period")
print_table(mean_value_by_state_table(filter(df, metric == 'percent_monthly_bill_savings'),'metric_value'), caption = "Mean Monthly Bill Savings (%)")
lcoe_contour(df, schema, tech, start_year, end_year, dr = 0.05, n = 30)
ggsave(sprintf('%s/lcoe_contour.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )

lcoe_boxplot(df)
ggsave(sprintf('%s/lcoe_boxplot.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
lcoe_cdf(df, start_year, end_year)
ggsave(sprintf('%s/lcoe_cdf.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
print_table(mean_value_by_state_table(df,'lcoe'), caption = "Mean LCOE by State and Year")
```

# Buy vs Lease
```{r Business_Model}
options(warn=-1)
out<-leasing_mkt_share(df, start_year, end_year, sectors)
out$plot
ggsave(sprintf('%s/leasing_market_share.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
print_table(out$table, caption = "Annual Lease Market Share: Fraction of New Systems That Were Leased")
cum_installed_capacity_by_bm(df, start_year, end_year)
ggsave(sprintf('%s/cumulative_capacity_by_business_model.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )

```
# System Characteristics
```{r System_Characteristics}
options(warn=-1)
cf_by_sector_and_year(df)
if (tech == 'wind'){
  print(dist_of_cap_selected(df, scen_name, start_year, end_year, 'customers'))
  ggsave(sprintf('%s/selected_capacities_by_customers.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
  print(dist_of_cap_selected(df, scen_name, start_year, end_year, 'capacity'))
  ggsave(sprintf('%s/selected_capacities_by_capacity.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
  print(dist_of_height_selected(df,scen_name,start_year))
  ggsave(sprintf('%s/selected_heights.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
} else if(tech == 'solar'){
  dist_of_azimuth_selected(df, start_year)
  ggsave(sprintf('%s/selected_azimuths.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
}
```
# Supply Curves
``` {r Resource_Potential}
options(warn=-1)
cf_supply_curve(df, start_year)
ggsave(sprintf('%s/cf_supply_curve_by_sector.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
elec_rate_supply_curve(df, start_year)
ggsave(sprintf('%s/elec_rate_supply_curve_by_sector.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
make_npv_supply_curve(df, years = c(2014,2020,2030,2040,2050))
ggsave(sprintf('%s/npv_supply_curve.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
make_npv_supply_curve_by_sector(df, years = 2014)
ggsave(sprintf('%s/npv_supply_curve_by_sector.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
make_lcoe_supply_curve(df, years = c(2014,2020,2030,2040,2050))
ggsave(sprintf('%s/lcoe_supply_curve.png', ppt_dir), dpi = 600, height = 5, width = 8, units = 'in' )
```

# Scenario options
```{r Scenario_Options}
options(warn=-1)
scenario_opts_table(con, schema)
```
