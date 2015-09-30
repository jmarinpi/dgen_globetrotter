```{r options, echo=FALSE}
  # Set options
  opts_chunk$set(echo = FALSE, warning = FALSE, fig.width = 12, fig.height = 8, cache = FALSE, fig.align = "center", results = "asis")
```



# Diffusion
```{r Diffusion}
options(warn=-1)
print_table(total_value_by_state_table(df, 'installed_capacity', unit_factor = 0.001, by_tech = T), caption = "Installed Capacity (MW)")
print_table(total_value_by_state_table(df, 'number_of_adopters', unit_factor = 1, by_tech = T), caption = "Installed Systems (Count)")
```

