```{r options, echo=FALSE}
  # Set options
  opts_chunk$set(echo = FALSE, warning = FALSE, fig.width = 12, fig.height = 8, cache = FALSE, fig.align = "center", results = "asis")
```



# Diffusion
```{r Diffusion}
out<-diffusion_trends(df, runpath, scen_name, by_tech = T, save_results = F)
grid.draw(out$national_installed_capacity_bar)
grid.newpage()
grid.draw(out$national_num_of_adopters_bar)
grid.newpage()
grid.draw(out$national_market_cap_bar)
grid.newpage()
grid.draw(out$national_generation_bar)
grid.newpage()
print_table(total_value_by_state_table(df, 'installed_capacity', unit_factor = 0.001, by_tech = T), caption = "Installed Capacity (MW)")
print_table(total_value_by_state_table(df, 'number_of_adopters', unit_factor = 1, by_tech = T), caption = "Installed Systems (Count)")
out$national_adopters_trends_bar
```

# Economics
```{r Economics}
npv4_by_year(df, by_tech = T)

```
# Resource
```{r Resource}
df_cf = mutate(df, v = cf)
boxplot_by_year(df_cf, 'Capacity Factor', by_tech = T, adopters_only = T, label = percent)

df_elec_costs = mutate(df, v = cost_of_elec_dols_per_kwh)
boxplot_by_year(df_elec_costs, 'Cost of Electricity ($/kwh)', by_tech = T, adopters_only = T, label = dollar)

df_sys_size = mutate(df, v = system_size_kw)
boxplot_by_year(df_sys_size, 'System Sizes (kw)', by_tech = T, adopters_only = T, label = comma)

df_load = mutate(df, v = load_kwh_per_customer_in_bin)
boxplot_by_year(df_load, 'Annual Customer Load (kwh)', by_tech = T, adopters_only = T, label = comma)

df_incentives = mutate(df, v = total_value_of_incentives/system_size_kw)
boxplot_by_year(df_incentives, 'Value of Incentives per kW ($/kW)', by_tech = T, adopters_only = T, label = dollar)
```