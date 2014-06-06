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
pp_trends_ribbon(payback_period_trends)
```
# Systems Installed
```{r Systems_Installed_Trends}

out<-turb_trends_hist(cap_selected_trends)
out$p1
out$p2
```