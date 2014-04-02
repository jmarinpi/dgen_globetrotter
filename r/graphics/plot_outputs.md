```{r options, echo=FALSE}
  # Set options
  opts_chunk$set(echo = FALSE, warning = FALSE, fig.width = 12, fig.height = 8, cache = FALSE, fig.align = "center", results = "asis")
```

# Diffusion
```{r Diffusion}
out<-diffusion_trends(df)
out$national_installed_capacity_bar
out$national_num_of_adopters_bar
print_table(mean_value_by_state_table(df,'market_share'), caption = "Mean Adoption Share")
out$national_adopters_trends_bar
```

# Economics
```{r Economics}
national_pp_line(df)
print_table(mean_value_by_state_table(df,'payback_period'), caption = "Mean Payback Period (years")
```
# System Characteristics
```{r System Characteristics}
cf_by_sector_and_year(df)
dist_of_cap_selected(df)
dist_of_height_selected(df)

```
# Resource Potential
``` {r Resource Potential}
cf_supply_curve(df)
elec_rate_supply_curve(df)
```
