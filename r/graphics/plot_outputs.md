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
print_table(mean_value_by_state_table(df,'market_share'), caption = "Mean Adoption Share")
out$national_adopters_trends_bar
national_installed_capacity_by_turb_size_bar(df)
```

# Diffusion Maps
```{r Diffusion Maps}
diffusion_all_map(df, runpath)
noquote("<iframe src='./figure/diffusion_all.html' name='diffusion_all_map' height=600px width=1100px style='border:none;'></iframe>")
sector_iframes = diffusion_sectors_map(df, runpath)
noquote(sector_iframes)
```

# Economics
```{r Economics}
national_pp_line(df,scen_name)
print_table(mean_value_by_state_table(df,'payback_period'), caption = "Mean Payback Period (years")

lcoe_boxplot(df)
lcoe_cdf(df)
print_table(mean_value_by_state_table(df,'lcoe'), caption = "Mean LCOE by State and Year")
```
# System Characteristics
```{r System_Characteristics}
cf_by_sector_and_year(df)
dist_of_cap_selected(df,scen_name)
dist_of_height_selected(df,scen_name)

```
# Resource Potential
``` {r Resource_Potential}
cf_supply_curve(df)
elec_rate_supply_curve(df)
```

# Scenario options
```{r Scenario_Options}
scenario_opts_table(con)
```
