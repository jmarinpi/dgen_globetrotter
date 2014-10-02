```{r options, echo=FALSE}
  # Set options
  opts_chunk$set(echo = FALSE, warning = FALSE, fig.width = 12, fig.height = 8, cache = FALSE, fig.align = "center", results = "asis")
```



# Diffusion
```{r Diffusion}
out<-diffusion_trends(df,runpath,scen_name)
out$national_installed_capacity_bar
out$national_num_of_adopters_bar
out$national_market_cap_bar
out$national_generation_bar
print_table(total_value_by_state_table(df,'installed_capacity', unit_factor = 0.001), caption = "Installed Capacity (MW)")
print_table(mean_value_by_state_table(df,'market_share'), caption = "Mean Adoption Share")
out$national_adopters_trends_bar
national_installed_capacity_by_system_size_bar(df)
```

# Diffusion Maps
```{r Diffusion Maps}
diffusion_all_map(df)
diffusion_sectors_map(df)
```

# Economics
```{r Economics}
national_pp_line(df,scen_name)
print_table(mean_value_by_state_table(df,'payback_period'), caption = "Mean Payback Period (years")
lcoe_contour(df, schema, start_year, end_year, dr = 0.05, n = 30)

lcoe_boxplot(df)
lcoe_cdf(df, start_year, end_year)
print_table(mean_value_by_state_table(df,'lcoe'), caption = "Mean LCOE by State and Year")
```
# System Characteristics
```{r System_Characteristics}
cf_by_sector_and_year(df)
if (tech == 'wind'){
  dist_of_cap_selected(df,scen_name,start_year,end_year)
  dist_of_height_selected(df,scen_name,start_year)
}
excess_gen_out<-excess_gen_figs(df, con, schema)
excess_gen_out$excess_gen_pt
excess_gen_out$excess_gen_cdf
```
# Resource Potential
``` {r Resource_Potential}
cf_supply_curve(df, start_year)
elec_rate_supply_curve(df, start_year)
```

# Scenario options
```{r Scenario_Options}
scenario_opts_table(con, schema)
```
