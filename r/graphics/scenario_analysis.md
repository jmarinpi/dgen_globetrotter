```{r options, echo=FALSE}
  # Set options
  opts_chunk$set(echo = FALSE, warning = FALSE, fig.width = 12, fig.height = 8, cache = FALSE, fig.align = "center", results = "asis")
```
# Diffusion Trends
```{r Diffusion_Trends}

all_sectors_diff_trends(diff_trends)
res_diff_trends(diff_trends)
com_diff_trends(diff_trends)
ind_diff_trends(diff_trends)

diff_trends_table(diff_trends)
```

# Economics
```{r Economics_Trends}
out<-metric_trends_ribbon(metric_value_trends)
out$p1
out$p2
```
# Systems Installed
```{r Systems_Installed_Trends}

out<-tryCatch(
{
turb_trends_hist(cap_selected_trends)
out$p1
out$p2
},
error = function(cond){
  message('Unable to compare turbine trends')
}
)

```

