```{r options, echo=FALSE}
  # Set options
  opts_chunk$set(echo = FALSE, warning = FALSE, fig.width = 12, fig.height = 8, cache = FALSE, fig.align = "center", results = "asis")
```



# Diffusion
```{r Diffusion}
options(warn=-1)
out<-diffusion_trends(df,runpath,scen_name)
grid.draw(out$national_installed_capacity_bar)
grid.newpage()
grid.draw(out$national_num_of_adopters_bar)
grid.newpage()
grid.draw(out$national_market_cap_bar)
grid.newpage()
grid.draw(out$national_generation_bar)
grid.newpage()
print_table(total_value_by_state_table(df,'installed_capacity', unit_factor = 0.001), caption = "Installed Capacity (MW)")
print_table(mean_value_by_state_table(df,'market_share'), caption = "Mean Adoption Share")
out$national_adopters_trends_bar
national_installed_capacity_by_system_size_bar(df,tech)
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
print_table(mean_value_by_state_table(filter(df, metric == 'payback_period'),'metric_value'), caption = "Mean Payback Period")
print_table(mean_value_by_state_table(filter(df, metric == 'percent_monthly_bill_savings'),'metric_value'), caption = "Mean Monthly Bill Savings (%)")
lcoe_contour(df, schema, tech, start_year, end_year, dr = 0.05, n = 30)

lcoe_boxplot(df)
lcoe_cdf(df, start_year, end_year)
print_table(mean_value_by_state_table(df,'lcoe'), caption = "Mean LCOE by State and Year")
```

# Buy vs Lease
```{r Business_Model}
options(warn=-1)
out<-leasing_mkt_share(df, start_year, end_year, sectors)
out$plot
print_table(out$table, caption = "Annual Lease Market Share: Fraction of New Systems That Were Leased")
cum_installed_capacity_by_bm(df, start_year, end_year)

```
# System Characteristics
```{r System_Characteristics}
options(warn=-1)
cf_by_sector_and_year(df)
if (tech == 'wind'){
  print(dist_of_cap_selected(df,scen_name,start_year,end_year))
  dist_of_height_selected(df,scen_name,start_year)
} else if(tech == 'solar'){
  dist_of_azimuth_selected(df, start_year)
}
```
# Supply Curves
``` {r Resource_Potential}
options(warn=-1)
cf_supply_curve(df, start_year)
elec_rate_supply_curve(df, start_year)
make_npv_supply_curve(df, years = c(2014,2020,2030,2040,2050))
make_npv_supply_curve_by_sector(df, years = 2014)
make_lcoe_supply_curve(df, years = c(2014,2020,2030,2040,2050))
```

# Scenario options
```{r Scenario_Options}
options(warn=-1)
scenario_opts_table(con, schema)
```
