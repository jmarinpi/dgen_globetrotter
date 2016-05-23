sector_col <- c(Commercial = "#377eb8", Industrial = "#e41a1c", Residential = "#4daf4a")
sector_fil <- sector_col
tech_col = c(Solar = "#fe9929", Wind = "#3690c0")
year_colors = c("2014" = '#fd8d3c', '2020' = '#fc4e2a', '2030' = '#e31a1c', '2040' = '#bd0026',
                '2050' = '#800026')
turb_size_fil <- c('Small: < 50 kW' = "#a1dab4", 'Mid: 51 - 500 kW' = "#41b6c4", 'Large: 501 - 3,000 kW' = "#253494") 
# ======================= DATA FUNCTIONS =================================================

# hack ggsave to skip checking of input plot type
# (only necessary up to version 2.0.0)
sI = sessionInfo()
ggplot_major_version = as.integer((strsplit(sI$otherPkgs$ggplot2$Version, '.', fixed = T))[[1]][1])
if (ggplot_major_version < 2){
  ggsave_hack = args(ggsave)
  body(ggsave_hack) = body(ggplot2::ggsave)[-2]
  environment(ggsave_hack) = asNamespace('ggplot2')
} else{
  ggsave_hack = ggplot2::ggsave
}

standard_formatting = theme_few() +
  theme(strip.text = element_text(size = 14, face = 'bold')) +
  theme(plot.title = element_text(size = 15, face = 'bold', vjust = 1)) +
  theme(axis.title.x = element_text(size = 14, face = 'bold', vjust = -.5)) +
  theme(axis.title.y = element_text(size = 14, face = 'bold', vjust = 1)) +
  theme(axis.text = element_text(size = 12)) +  
  theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1)) +
  theme(legend.text = element_text(size = 12)) +
  theme(legend.title = element_text(size = 14)) +
  theme(legend.key.size = unit(1, 'cm')) +
  theme(legend.key = element_rect(colour = 'white', size = 2)) +
  theme(strip.text.x = element_text(size = 14, face = 'bold'))

theme_custom =    
  theme(panel.grid.minor = element_blank()) +
  theme(text = element_text(colour = "#9D9D9D")) +
  theme(plot.margin = unit(c(1, 1, 1, 1), "lines")) +
  theme(axis.title = element_text(size = rel(1.2))) +
  theme(axis.title.x = element_text(vjust = 0.1)) +
  theme(axis.title.y = element_text(vjust = 1.1)) +
  theme(axis.text = element_text(size = rel(1))) +
  theme(plot.title = element_text(size = rel(1.5), face = "bold")) +
  theme(legend.text = element_text(size = rel(1))) +
  theme(legend.title = element_text(size = rel(1.2))) +
  theme(legend.key=element_blank()) +
  theme(axis.line = element_line(colour =  "#9D9D9D", size = 1)) +
  theme(panel.grid.major = element_line(colour = "light grey")) +
  theme(panel.background = element_rect(fill = "white")) +
  theme(legend.background = element_rect(fill = alpha('white', 0.5))) +
  theme(strip.text = element_text(size = rel(1.2), face = 'bold')) +
  theme(strip.background = element_blank())

sector2factor = function(sector_vector){
  f = factor(toProper(as.character(sector_vector)), levels = names(sector_col))
  return(f)
} 

add_data_source_note = function(g){
  gt <- ggplot_gtable(ggplot_build(g))
  final_gt = gtable_add_grob(gt, textGrob("Note: \n2012 data\nare historical.\nAll other years\nare model outputs.", 
                                                             y = .11, x = .1, just = 'left',
                                                             #hjust = -.5, vjust = .5, 
                                                             gp=gpar(fontsize=12)), 
                                                    t = 4, l = 9, r = 10, clip = 'off', name = 'note')
  return(final_gt)
}

add_generation_data_source_note = function(g){
  gt <- ggplot_gtable(ggplot_build(g))
  final_gt = gtable_add_grob(gt, textGrob("Note: \n2012 data based on\nhistorical installed ca-\npacity. All other years\nare model outputs.", 
                                          y = .1, x = .1, just = 'left',
                                          #hjust = -.5, vjust = .5, 
                                          gp=gpar(fontsize=12)), 
                             t = 3, l = 5, clip = 'off', name = 'note')
  return(final_gt)
}

add_market_cap_data_source_note = function(g){
  gt <- ggplot_gtable(ggplot_build(g))
  final_gt = gtable_add_grob(gt, textGrob("Note: \n2012 data based\non historical installed\ncapacity and 2012\ncosts. All other years\nare model outputs.", 
                                          y = .11, x = .1, just = 'left',
                                          #hjust = -.5, vjust = .5, 
                                          gp=gpar(fontsize=12)), 
                             t = 4, l = 9, r = 10, clip = 'off', name = 'note')
  return(final_gt)
}


make_con<-function(driver = "PostgreSQL", host, dbname, user, password, port = 5432){
  # Make connection to dav-gis database
  dbConnect(dbDriver(driver), host = host, dbname = dbname, user = user, password = password, port = port)  
}


simpleCapHelper <- function(x) {
  # converts a given string to proper case
  # For formatting scenario options
  s <- strsplit(x, "_")[[1]]
  paste(toupper(substring(s, 1,1)), substring(s, 2),
        sep="", collapse=" ")
}

simpleCap <- function(x){
  s = sapply(x, simpleCapHelper)
  return(unname(s))
}

toProper <- function(s, strict = FALSE) {
  cap <- function(s) paste(toupper(substring(s, 1, 1)),
{s <- substring(s, 2); if(strict) tolower(s) else s},
sep = "", collapse = " " )
sapply(strsplit(s, split = " "), cap, USE.NAMES = !is.null(names(s)))
}



mean_value_by_state_table<-function(df,val){
  # Create table of mean value by year and state. value is string of variable to take mean of. 
  val = as.symbol(val)
  g = group_by(df, year, state_abbr)
  by_state = collect(summarise(g,
                               round(mean(val),as.integer(2))
                              )
                     )
  names(by_state)<-c('year','State',val)
  g = group_by(df, year)
  national = collect(summarise(g,
                               round(mean(val),as.integer(2))
                              )
                     )
  national$State<-'U.S'
  names(national)<-c('year',val,'State')
  by_state<-dcast(data = by_state,formula= State ~ year, value.var = as.character(val))
  national<-dcast(data = national,formula= State ~ year, value.var = as.character(val))
  rbind(national,by_state)
}

total_value_by_state_table<-function(df, val, unit_factor = 1, by_tech = F){
  # Create table of summed value by year and state. value is string of variable to take sum over.
  # Use unit_factor to convert units if needed i.e. unit_factor = 1e-6 will convert kW to GW
  val_symb = as.symbol(val)
  if (by_tech == T){
    g = group_by(df, year, state_abbr, tech)
  } else {
    g = group_by(df, year, state_abbr)
  }
  by_state = as.data.frame(collect(summarise(g, sum(val_symb * unit_factor))))
  names(by_state)[ncol(by_state)] = val
  names(by_state)[which(names(by_state) == 'state_abbr')] = 'State'
  
  if (by_tech == T){
    g = group_by(by_state, year, tech)    
  } else {
    g = group_by(by_state, year)    
  }
  
  national = summarise(g, 
                       sum(val_symb))
  
  names(national)[ncol(national)] = val
  national$State ='U.S'
  
  
  # round the results
  by_state[, val] = round(by_state[, val], 2)
  national[, val] = round(national[, val], 2)
  by_state = dcast(data = by_state, formula= State + tech ~ year, value.var =  val)
  national = dcast(data = national, formula= State + tech ~ year, value.var =  val)
  
  result_df = rbind(national,by_state)
  
  return(result_df)
}

create_report <- function(runpath) {
  knit2html("../r/graphics/plot_outputs.md", output = "DG Wind report.html", title = "DG Wind report", stylesheet = "plot_outputs.css",
            options = c("hard_wrap", "use_xhtml", "base64_images", "toc = yes"))
}



# ======================= GRAPHING FUNCTIONS =================================================

cf_by_sector_and_year<-function(df){
  g = group_by(df, year, sector)
  # Median and inner-quartile range of CF over sector and year
  data =  collect(summarise(g,
                            tql = diffusion_shared.r_quantile(array_agg(naep), 0.75)/ 8760,
                            median = diffusion_shared.r_quantile(array_agg(naep), 0.5)/ 8760,
                            fql = diffusion_shared.r_quantile(array_agg(naep), 0.25)/ 8760))
  data$sector = sector2factor(data$sector)
  ggplot(data)+
    geom_ribbon(aes(x = year, ymin = fql, ymax = tql, fill = sector), alpha = .3, stat = "identity")+
    geom_line(aes(x = year, y = median, color = sector)) +
    facet_wrap(~sector)+
    scale_color_manual(values = sector_col, name = 'Median and IQR') +
    scale_fill_manual(values = sector_fil, name = 'Median and IQR') +
    scale_y_continuous(name = 'Annual average capacity factor', label = percent)+
    scale_x_continuous(name = 'Year', breaks = unique(data$year)) +
    theme_custom +
    ggtitle('Range of Capacity Factor by Sector and Year') +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1)) +
    annotate("segment", x=-Inf, xend=-Inf, y=-Inf, yend=Inf, colour =  "#9D9D9D", lwd = 2) +
    theme(axis.line.y = element_blank())
}

lcoe_contour<-function(df, schema, tech, start_year, end_year, dr = 0.05, n = 30){
  # Map out the net present cost and annual capacity factor to achieve a given LCOE
  # LCOE is calculate as the net present cost divided by the net present generation over lifetime
  # To calculate NPC for the model, assume a 30yr life and 1c/kWh VOM
  # Plot a sample of model points for the first and final year for all sectors,sizes, etc.
    
  d = data.frame()
  
  present_value_factor = ((1 - (1 + dr)^-n)/dr)
  
  # Calculate the min and max capacity factors for a given lcoe and net present cost (npc)
  for(lcoe in c(0,0.1, 0.2, 0.3, 0.4, 0.5, 0.6,0.7)){
    tmp<-data.frame(npc = seq(0,10,.01))
    tmp$cf <- 1000 * tmp$npc / (lcoe * 8760 * present_value_factor)
    tmp$cf_max <- 1000 * tmp$npc / (lcoe * 8760 * present_value_factor)
    tmp$cf_min <- 1000 * tmp$npc / ((lcoe + 0.1) * 8760 * present_value_factor)
    tmp$lcoe <- lcoe
    d <- rbind(d,tmp)
  }
  
  d[is.na(d)] <- 0.5
  d[d$cf_max > 0.5, 'cf_max'] <- 0.5
  d[d$cf_min > 0.5, 'cf_min'] <- 0.5
  d[d$lcoe == 0.7 , 'cf_min'] <- 0
  
  # Subset of model points for first and last year
  sql = sprintf("SELECT * FROM %s.outputs_all_%s WHERE year in (%s,%s) ORDER BY RANDOM() LIMIT 1000", schema, tech, start_year, end_year)
  f = tbl(src,sql(sql))       
  pts = collect(select(f, installed_costs_dollars_per_kw,naep,year))
  pts$present_value_factor <- present_value_factor
  
  ggplot()+
    geom_ribbon(data = d, aes(x = npc, y = cf, ymin = cf_min, ymax = cf_max, fill = factor(lcoe)), alpha = 0.5)+
    scale_x_continuous(name = 'Net Present Cost ($/W)', limits = c(0,10))+
    scale_y_continuous(name = 'Capacity Factor', limits = c(0,.5))+
    scale_fill_brewer(name = 'LCOE Range ($/kWh)', 
                      labels=c('< 0.1' ,"0.1 - 0.2", "0.2 - 0.3", "0.3 - 0.4", "0.4 - 0.5", "0.5 - 0.6", "0.6 - 0.7", "> 0.7"), 
                      palette = 'Spectral')+
    geom_point(data = pts, aes(x = 0.001 * (installed_costs_dollars_per_kw + naep * 0.01 * present_value_factor), 
                                  y = naep/8760, color = factor(year), 
                                  shape = factor(year)))+
    scale_colour_discrete(name = 'Sample From Model')+
    scale_shape_discrete(name = 'Sample From Model')+
    ggtitle("LCOE Contour Map") +
    theme_custom
}


dist_of_cap_selected<-function(df,scen_name, start_year, end_year, weight, first_year_only = T){
  # What size system are customers selecting in 2014?
  
  # filter to only the start year, returning only the cost_of_elec_dols_per_kwh and load_kwh_in_bin cols
  if (first_year_only == T){
    f = filter(df, year == start_year)
  } else{
    f = filter(df, year %in% c(start_year,end_year))      
  }

  g = group_by(f, system_size_factors, year)
  if (weight == 'systems'){
    cap_picked = collect(summarise(g,
                                   s = sum(number_of_adopters)
    ))    
    y_title = 'Percent of Installed Systems'
  } else if (weight == 'capacity') {
    cap_picked = collect(summarise(g,
                                   s = sum(installed_capacity)
    ))    
    y_title = 'Percent of Installed Capacity'
  }


  tmp<-group_by(cap_picked, year) %>%
	summarise(total = sum(s, na.rm = T))

  cap_picked<-merge(cap_picked, tmp)
  cap_picked<-transform(cap_picked, p = s/total)
  cap_picked = filter(cap_picked, system_size_factors != '0')
  cap_picked$system_size_factors <- ordered(cap_picked$system_size_factors, levels = c('2.5', '5.0','10.0','20.0','50.0','100.0','250.0','500.0','750.0','1000.0','1500.0','1500+'))

  
  p<-ggplot(cap_picked) +
    geom_histogram(aes(x = system_size_factors, weight = p), position = 'dodge', fill = '#0076BC') +
    scale_y_continuous(name = y_title, labels = percent) +
    scale_x_discrete(name = 'Selected System Size (kW)') +
    theme_custom +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1)) +
    ggtitle('System Sizing') +
    annotate("segment", x=-Inf, xend=-Inf, y=-Inf, yend=Inf, colour =  "#9D9D9D", lwd = 2) +
    theme(axis.line.y = element_blank())
  
  cap_picked$scenario<-scen_name
#   write.csv(cap_picked,paste0(runpath,'/cap_selected_trends.csv'),row.names = FALSE)
  if (weight == 'capacity') {
    save(cap_picked,file = paste0(runpath,'/cap_selected_trends.RData'),compress = T, compression_level = 1)
  }
  return(p)
}

dist_of_height_selected<-function(df,scen_name,start_year){
  #What heights are prefered?
  
  # Distinguish between 1500 kW and 1500+ kW projects
  
  # filter to starting year
  f = filter(df, year == start_year)  
  g = group_by(f, system_size_factors, turbine_height_m, sector)
  height_picked = collect(summarise(g, 
                                    load_in_gw = sum(load_kwh_in_bin)/(1e6*8760)
  )
  )
  height_picked$system_size_factors = ifelse(height_picked$system_size_factors == '0', 'Excluded', height_picked$system_size_factors)
  height_picked$system_size_factors <- ordered( height_picked$system_size_factors, levels = c('Excluded', '2.5','5.0','10.0','20.0','50.0','100.0','250.0','500.0','750.0','1000.0','1500.0','1500+'))
  height_picked$sector = sector2factor(height_picked$sector)
  
  p<-ggplot(height_picked)+
    geom_point(aes(x = factor(system_size_factors), y = factor(turbine_height_m), size = load_in_gw, color = sector), aes = 0.2)+
    scale_size_continuous(name = 'Potential Customer Load (GW)', range = c(3,10))+
    facet_wrap(~sector,scales="free_y")+
    scale_color_manual(values = sector_col, name = "Sector") +
    scale_fill_manual(values = sector_fil, name = "Sector") +
    scale_y_discrete(name ='Turbine Hub Height (m)')+
    scale_x_discrete(name ='System Size (kW)')+
    theme_custom +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1)) +
    ggtitle('System Sizes and Heights')
  height_picked$scenario<-scen_name
#   write.csv(height_picked,paste0(runpath,'/height_selected_trends.csv'),row.names = FALSE)
  save(height_picked,file = paste0(runpath,'/height_selected_trends.RData'),compress = T, compression_level = 1)
  return(p)
}

national_econ_attractiveness_line<-function(df,scen_name){
  # Median payback period over time and sector
  g = group_by(df, year, sector,metric)
  data = collect(summarise(g, 
                           uql = diffusion_shared.r_quantile(array_agg(metric_value), 0.95),
                           median = diffusion_shared.r_quantile(array_agg(metric_value), 0.5),
                           lql = diffusion_shared.r_quantile(array_agg(metric_value), 0.05)
  )
  )
  
  data$sector = sector2factor(data$sector)
  data$metric = simpleCap(data$metric)
  data$metric = ifelse(data$metric == 'Percent Monthly Bill Savings', '% Bill Savings', data$metric)
  
  p <- ggplot(data) +
    geom_ribbon(aes(x = year, ymin = lql, ymax = uql, fill = sector), alpha = 0.3, stat = 'identity', size = 0.75) +
    geom_line(aes(x = year, y = median, color = sector), size = 0.75) +
    facet_grid(metric~sector, scales = 'free') +
    scale_color_manual(values = sector_col, name = 'Median and IQR') +
    scale_fill_manual(values = sector_fil, name = 'Median and IQR') +
    scale_y_continuous('') +
    annotate("segment", x=-Inf, xend=Inf, y=-Inf, yend=-Inf, size = 2, colour = "#9D9D9D") +
    scale_x_continuous(name = 'Year', breaks = c(unique(data$year))) +
    ggtitle('Economic Attractiveness') +
    theme_custom +
    theme(axis.line.x = element_blank()) +
    theme(strip.text.y = element_text(angle = 270, vjust = 1)) +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1)) +
    annotate("segment", x=-Inf, xend=-Inf, y=-Inf, yend=Inf, colour =  "#9D9D9D", lwd = 2) +
    theme(axis.line.y = element_blank())
  # move facet lables to left side
  g <- ggplotGrob(p)
  g$layout[g$layout$name == "strip-right",c("l", "r")] <- 2
  grid.newpage()
  grid.draw(g)
  
  data$scenario<-scen_name
  save(data,file = paste0(runpath,'/metric_value_trends.RData'),compress = T, compression_level = 1)
}


npv4_by_year <-function(df, by_tech = F){
  # Median payback period over time and sector
  if (by_tech == T){
    g = group_by(df, year, sector, tech)
    color_var = 'tech'
    colors = tech_col
  } else {
    g = group_by(df, year, sector)    
    color_var = 'sector'
    colors = sector_col
  }

  data = collect(summarise(g, 
                           uql = diffusion_shared.r_quantile(array_agg(npv4), 0.95),
                           median = diffusion_shared.r_quantile(array_agg(npv4), 0.5),
                           lql = diffusion_shared.r_quantile(array_agg(npv4), 0.05)
  )
  )  
  data$sector = sector2factor(data$sector)
  data$tech = simpleCap(data$tech)
  
  p <- ggplot(data) +
    geom_ribbon(aes_string(x = 'year', ymin = 'lql', ymax = 'uql', fill = color_var), alpha = 0.3, stat = 'identity', size = 0.75) +
    geom_line(aes_string(x = 'year', y = 'median', color = color_var), size = 0.75) +
    facet_grid(. ~ sector, scales = 'free_y') +
    scale_color_manual(values = colors, name = 'Median and 5th/95th Percentiles') +
    scale_fill_manual(values = colors, name = 'Median and 5th/95th Percentiles') +
    scale_y_continuous('') +
    scale_x_continuous(name = 'Year', breaks = c(unique(data$year))) +
    ggtitle('Net Present Value (4%) (Median and 90% Spread)') +
    theme_custom +
    theme(strip.text.y = element_text(angle = 90, vjust = 1))

  # move facet lables to left side
  g <- ggplotGrob(p)
  g$layout[g$layout$name == "strip-right",c("l", "r")] <- 2
  grid.newpage()
  grid.draw(g)
}

boxplot_stats = function(x, stat = NA){
  b = boxplot(x, plot = F)
  stats = as.numeric(b$stats)
  names(stats) = c('lw', 'lq', 'm', 'uq', 'uw')
  if (!is.na(stat)){
    return(stats[stat])
  } else {
    return(stats)
  }
}

# ----------------------------------------
# Authors: Stefan Kraft and Andreas Alfons
#          Vienna University of Technology
# ----------------------------------------

spBwplotStats <- function(x, weights = NULL, coef = 1.5, 
                          zeros = TRUE, do.out = TRUE) {
  # initializations
  if(!is.numeric(x)) stop("'x' must be a numeric vector")
  if(!is.numeric(coef) || length(coef) != 1 || coef < 0) {
    stop("'coef' must be a single non-negative number")
  }
  # get quantiles
  if(isTRUE(zeros)) {
    zero <- ifelse(is.na(x), FALSE, x == 0)
    x <- x[!zero]
    if(is.null(weights)) nzero <- sum(zero)
    else {
      # if 'zeros' is not TRUE, these checks are done in 'quantileWt'
      # but here we need them since we use subscripting
      if(!is.numeric(weights)) stop("'weights' must be a numeric vector")
      else if(length(weights) != length(zero)) {
        stop("'weights' must have the same length as 'x'")
      }
      nzero <- sum(weights[zero])
      weights <- weights[!zero]
    }
  } else nzero <- NULL
  ok <- !is.na(x)
  n <- if(is.null(weights)) sum(ok) else sum(weights[ok])
  if(n == 0) stats <- rep.int(NA, 5)
  else stats <- quantileWt(x, weights)
  iqr <- diff(stats[c(2, 4)])  # inter quartile range
  if(coef == 0) do.out <- FALSE
  else {
    if(is.na(iqr)) out <- is.infinite(x) 
    else {
      lower <- stats[2] - coef * iqr
      upper <- stats[4] + coef * iqr
      out <- ifelse(ok, x < lower | x > upper, FALSE)
    }
    if(any(out)) stats[c(1, 5)] <- range(x[!out], na.rm=TRUE)
  }
  res <- list(stats=stats, n=n, nzero=nzero, 
              out=if(isTRUE(do.out)) x[out] else numeric())
  class(res) <- "spBwplotStats"
  res
}

quantileWt <- function(x, weights = NULL, 
                       probs = seq(0, 1, 0.25), na.rm = TRUE) {
  # initializations
  if(!is.numeric(x)) stop("'x' must be a numeric vector")
  x <- unname(x)  # unlike 'quantile', this never returns a named vector
  if(is.null(weights)) {
    return(quantile(x, probs, na.rm=na.rm, names=FALSE, type=1))
  } else if(!is.numeric(weights)) stop("'weights' must be a numeric vector")
  else if(length(weights) != length(x)) {
    stop("'weights' must have the same length as 'x'")
  } else if(!all(is.finite(weights))) stop("missing or infinite weights")
  if(!is.numeric(probs) || all(is.na(probs)) || 
       isTRUE(any(probs < 0 | probs > 1))) {
    stop("'probs' must be a numeric vector with values in [0,1]")
  }
  if(length(x) == 0) return(rep.int(NA, length(probs)))
  if(!isTRUE(na.rm) && any(is.na(x))) {
    stop("missing values and NaN's not allowed if 'na.rm' is not TRUE")
  }
  # sort values and weights
  ord <- order(x, na.last=NA)
  x <- x[ord]
  weights <- weights[ord]
  # some preparations
  rw <- cumsum(weights)/sum(weights)
  rm <- rw / rw[length(rw)]
  rw[length(rw)] <- 1  # just to make sure
  # obtain quantiles
  select <- sapply(probs, function(p) min(which(rw >= p)))
  q <- x[select]
  return(q)
}

# # ----------------------------------------


boxplot_by_year = function(df, title, by_tech = F, adopters_only = T, label = NULL){
  
  if (adopters_only == T){
    f = filter(df, new_adopters > 0)    
  } else {
    f = df
  }

  if (by_tech == T){
    g = group_by(f, year, sector, tech)
  } else {
    g = group_by(f, year, sector)
  }

  s = summarize(g, lw = r_boxplot_stats(array_agg(v), 'lw'),
               lq = r_boxplot_stats(array_agg(v), 'lq'),
               m = r_boxplot_stats(array_agg(v), 'm'),
               uq = r_boxplot_stats(array_agg(v), 'uq'),
               uw = r_boxplot_stats(array_agg(v), 'uw')
            ) %>% collect()
  s = as.data.frame(s)
  
  if (by_tech == T){
    s$tech = simpleCap(s$tech)
    x_var = 'tech'
    facet_formula = 'sector ~ year'
    color_var = 'tech'
    colors = tech_col
  } else {
    x_var = 'year'
    facet_formula = '~ sector'
    color_var = 'sector'
    colors = sector_col
  }
  s$sector = sector2factor(s$sector)
  s$year = as.factor(s$year)
  
  ggplot(data = s) +
    geom_boxplot(aes_string(x = x_var, ymin = 'lw', lower = 'lq', middle = 'm', upper = 'uq', ymax = 'uw', fill = color_var), stat = 'identity') +
    facet_grid(facet_formula, scales = 'fixed') +
    scale_fill_manual(name = simpleCap(color_var), values = colors) +
    scale_y_continuous(name = title, label = label) +
    scale_x_discrete(name = simpleCap(x_var)) +
    theme_custom +
    ggtitle(sprintf('Range of %s by Sector and Year (Adopters Only)', title))
  
}


summarize_deployment_by_state_sector_year = function(df, runpath, scen_name, by_tech = F){
  if (by_tech == T){  
    g = group_by(df, year, state_abbr, sector, tech)
    melt_vars = c('year', 'state_abbr', 'sector', 'tech')
  } else {
    g = group_by(df, year, state_abbr, sector)
    melt_vars = c('year', 'state_abbr', 'sector')
  }
  data = collect(summarise(g, nat_installed_capacity_gw  = sum(installed_capacity)/1e6, 
                           # We have no way calculating CFs for existing capacity, so assume it had a 23% capacity factor
                           nat_market_share = sum(number_of_adopters)/sum(customers_in_bin), 
                           nat_max_market_share = sum(max_market_share * customers_in_bin)/sum(customers_in_bin),
                           nat_market_value = sum(market_value),
                           nat_generation_twh = sum(total_gen_twh), 
                           nat_number_of_adopters = sum(number_of_adopters)
  )
  )  
  
  data = melt(data = data, id.vars = melt_vars)
  data$scenario = scen_name
  data$data_type = 'Cumulative'
  
  save(data,file = paste0(runpath,'/diffusion_trends_by_state.RData'), compress = T, compression_level = 1)    
  
}
  


diffusion_trends<-function(df, runpath, scen_name, by_tech = F, save_results = T){
  # Diffusion trends
  df_2014 = filter(df, year == 2014)
  if (by_tech == T){
    g_2014 = group_by(df_2014, sector, tech)
    melt_vars = c('year', 'sector', 'tech')
  } else{
    g_2014 = group_by(df_2014, sector)
    melt_vars = c('year', 'sector')
  }
  baseline_data = g_2014 %>%
                  summarise(year = 2012,
                            nat_installed_capacity_gw  = sum(installed_capacity_last_year)/1e6, 
                            # We have no way calculating CFs for existing capacity, so assume it had a 23% capacity factor
                            nat_market_value = sum(market_value_last_year),
                            nat_generation_twh = sum(0.23 * 8760 * initial_capacity_mw * 1e-6), 
                            nat_number_of_adopters = sum(number_of_adopters_last_year)                    
                    ) %>%
                  collect()
  baseline_data = melt(baseline_data, id.vars = melt_vars)
  baseline_data$scenario = scen_name
  baseline_data$data_type = 'Cumulative'
  
  if (by_tech == T){  
    g = group_by(df, year, sector, tech)
  } else {
    g = group_by(df, year, sector)
  }
  data = collect(summarise(g, nat_installed_capacity_gw  = sum(installed_capacity)/1e6, 
  # We have no way calculating CFs for existing capacity, so assume it had a 23% capacity factor
                           nat_market_share = sum(number_of_adopters)/sum(customers_in_bin), 
                           nat_max_market_share = sum(max_market_share * customers_in_bin)/sum(customers_in_bin),
                           nat_market_value = sum(market_value),
                           nat_generation_twh = sum(total_gen_twh),
                           nat_number_of_adopters = sum(number_of_adopters)
  )
  )
  data = melt(data = data, id.vars = melt_vars)
  data$scenario = scen_name
  data$data_type = 'Cumulative'
  
  if (save_results == T){
    save(data,file = paste0(runpath,'/diffusion_trends.RData'),compress = T, compression_level = 1)    
  }

  # summarize state level cumulative deployment too
  summarize_deployment_by_state_sector_year(df, runpath, scen_name, by_tech)
  
  # order the data by sector and year
  yearly_data = collect(summarise(g, nat_installed_capacity_gw  = sum(installed_capacity-installed_capacity_last_year)/1e6, 
                           # We have no way calculating CFs for existing capacity, so assume it had a 23% capacity factor
                           nat_market_value = sum(market_value-market_value_last_year),
                           nat_number_of_adopters = sum(number_of_adopters-number_of_adopters_last_year)
  )
  )
  # melt the dataframe for ggplot
  yearly_data = melt(data = yearly_data, id.vars = melt_vars)
  yearly_data$scenario = scen_name
  yearly_data$data_type = 'Annual'

  cumulative_data = rbind(data[, names(yearly_data)], baseline_data[, names(yearly_data)])

  combined_data = rbind(cumulative_data, yearly_data)
  combined_data$data_type = factor(combined_data$data_type, levels = c('Cumulative', 'Annual'))
  
  combined_data$sector = sector2factor(combined_data$sector)
  cumulative_data$sector = sector2factor(cumulative_data$sector)
  yearly_data$sector = sector2factor(yearly_data$sector)
  if (by_tech == T){
    combined_data$tech = simpleCap(combined_data$tech)
    cumulative_data$tech = simpleCap(cumulative_data$tech)
    yearly_data$tech = simpleCap(yearly_data$tech)
    data$tech = simpleCap(data$tech)
    color_var = 'tech'
    colors = tech_col
  } else {
    color_var = 'sector'
    colors = sector_col
  }
  
  # National market share trends
  trends_data = subset(data, variable %in% c('nat_market_share', 'nat_max_market_share'))
  trends_data$variable = as.character(trends_data$variable)
  trends_data[trends_data$variable == 'nat_market_share', 'variable'] = 'Market Share'
  trends_data[trends_data$variable == 'nat_max_market_share', 'variable'] = 'Max Market Share'
  trends_data$sector = sector2factor(trends_data$sector)
  
  
  national_adopters_trends_bar <- ggplot(trends_data, 
                                       aes_string(x = 'year', y = 'value', color = color_var, linetype = 'variable'))+
    geom_line(size = 0.75) +
    facet_wrap(~sector) +
    geom_line() +
    scale_color_manual(values = colors) +
    scale_fill_manual(values = colors) +
    scale_y_continuous(name ='Market Share', labels = percent) +
    scale_x_continuous(name ='Year', breaks = c(unique(trends_data$year))) +
    guides(color = FALSE) +
    expand_limits(y = 0) + 
    ggtitle('National Adoption Trends') +
    theme_custom +
    theme(legend.title = element_blank()) +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1)) +
    annotate("segment", x=-Inf, xend=-Inf, y=-Inf, yend=Inf, colour =  "#9D9D9D", lwd = 2) +
    theme(axis.line.y = element_blank())
  
  # INSTALLED CAPACITY
  national_installed_capacity_bar <- add_data_source_note(
                                                          ggplot(data = subset(combined_data, variable %in% c("nat_installed_capacity_gw")))+
                                                          geom_bar(aes_string(x = 'factor(year)', fill = color_var, weight = 'value')) +  
                                                          facet_wrap(~ data_type, scales = 'free') +
                                                          scale_color_manual(values = colors) +
                                                          scale_fill_manual(name = simpleCap(color_var), values = colors, guide = guide_legend(reverse=TRUE)) +
                                                          scale_y_continuous(name ='Installed Capacity (GW)', labels = comma) +
                                                          expand_limits(weight=0) +
                                                          scale_x_discrete(name ='Year') +
                                                          ggtitle('Installed Capacity') +
                                                          theme_custom +
                                                          theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1))
                                                        )
  
  # NATIONAL NUMBER OF ADOPTERS
  national_num_of_adopters_bar <- add_data_source_note(
                                                        ggplot(data = subset(combined_data, variable %in% c("nat_number_of_adopters"))) +
                                                        geom_bar(aes_string(x = 'factor(year)', fill = color_var, weight = 'value')) +
                                                        facet_wrap(~data_type, scales = 'free') +
                                                        scale_color_manual(values = colors) +
                                                        scale_fill_manual(name = simpleCap(color_var), values = colors, guide = guide_legend(reverse=TRUE)) +
                                                        scale_y_continuous(name ='Number of Adopters', labels = comma) +
                                                        expand_limits(weight=0) +
                                                        scale_x_discrete(name ='Year') +
                                                        ggtitle('Number of Adopters') +
                                                        theme_custom +
                                                        theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1))
                                                      )
  
  # NATIONAL MARKET CAP
  national_market_cap_bar <- add_market_cap_data_source_note(
                                                    ggplot(subset(combined_data, variable %in% c("nat_market_value"))) +
                                                    geom_bar(aes_string(x = 'factor(year)', fill = color_var, weight = 'value/1e9')) +  
                                                    facet_wrap(~ data_type, scales = 'free') +
                                                    scale_color_manual(values = colors) +
                                                    scale_fill_manual(name = simpleCap(color_var), values = colors, guide = guide_legend(reverse=TRUE)) +
                                                    scale_y_continuous(name ='Value of Installed Capacity (Billion $)', labels = comma) +
                                                    expand_limits(weight=0) +
                                                    scale_x_discrete(name ='Year') +
                                                    ggtitle('Value of Installed Capacity') +
                                                    theme_custom +
                                                    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1))
                                                  )
  
  # NATIONAL GENERATION
  national_generation_bar <-  add_generation_data_source_note(
                                                              ggplot(subset(cumulative_data, variable %in% c("nat_generation_twh")))+
                                                              geom_bar(aes_string(x = 'factor(year)', weight = 'value', fill = color_var)) +
                                                              scale_color_manual(values = colors) +
                                                              scale_fill_manual(name = simpleCap(color_var), values = colors, guide = guide_legend(reverse=TRUE)) +
                                                              scale_y_continuous(name ='Annual Generation (TWh)', labels = comma) +
                                                              expand_limits(weight=0) +
                                                              scale_x_discrete(name ='Year') +
                                                              ggtitle('Annual Generation') +
                                                              theme_custom +
                                                              theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1))
                                                              )             

  list("national_installed_capacity_bar" = national_installed_capacity_bar,
       "national_adopters_trends_bar" = national_adopters_trends_bar,
       "national_num_of_adopters_bar" = national_num_of_adopters_bar,
       "national_market_cap_bar" = national_market_cap_bar,
       "national_generation_bar" = national_generation_bar)
}


scenario_opts_table<-function(con, schema){
  table<-dbGetQuery(con,sprintf("SELECT * from %s.input_main_scenario_options", schema))
  names(table) <- unlist(lapply(names(table),simpleCap))
  table<-melt(table, id.vars = c(),variable.name='Switch',value.name="Value")
  print_table(table, caption = 'Scenario Options')    
}

# Shortcut to print tables in nicely formatted HTML
print_table <- function(...){
  print(xtable(...), type = "html", include.rownames = FALSE, caption.placement = "top", comment = getOption("xtable.comment", F))
}

national_installed_capacity_by_system_size_bar<-function(df,tech){
  
  data = select(df, sector, system_size_factors, new_capacity, year) %>%
    group_by(year, sector, system_size_factors) %>%
    summarise(total_new_capacity = sum(new_capacity)) %>%
    group_by(sector, system_size_factors) %>%
    arrange(year) %>%
    mutate(installed_cap_by_size = cumsum(total_new_capacity)/1e6 ) %>%
    collect()
  
  if(tech == 'solar'){
    data$system_size_factors <- ordered( data$system_size_factors, levels = c("(0,2.5]", "(2.5,5]", "(5,10]", "(10,20]", "(20,50]", "(50,100]", "(100,250]", "(250,500]", "(500,750]", "(750,1e+03]", "(1e+03,1.5e+03]"))
  } else {
    # order the data correctly
    data$system_size_factors <- ordered(data$system_size_factors, levels = c('2.5','5.0','10.0','20.0','50.0','100.0','250.0','500.0','750.0','1000.0','1500.0','1500+'))
  }
  data = data[order(data$year,data$system_size_factors),]
  data$sector = sector2factor(data$sector)
  
  colourCount = length(unique(data$system_size_factors))
  getPalette = colorRampPalette(brewer.pal(9, "YlOrRd"))
  
  ggplot(data)+
    facet_wrap(~sector, scales="free_y")+
    geom_area(aes(x = year, fill = system_size_factors, y = installed_cap_by_size), position = 'stack')+
    scale_fill_manual(name = 'System Size (kW)', values = getPalette(colourCount)) +
    scale_y_continuous(name ='Installed Capacity (GW)') +
    scale_x_continuous(name = 'Year', breaks = c(unique(data$year))) +
    theme_custom +
    ggtitle('Installed Capacity by System Size (GW)') +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1))
}

boxplot_whisker = function(x, bound = 'lower') {
  if (bound == 'lower'){
    y = boxplot.stats(x)$stats[1]
  } else if (bound == 'upper'){
    y = boxplot.stats(x)$stats[5]
  }
  
  return(y)
}

lcoe_boxplot<-function(df){
  data = collect(select(df,year,lcoe,sector))
  data$sector = sector2factor(data$sector)
  # Boxplot of LCOE over time, faceted by sector
  boxstats = group_by(data, sector, year) %>%
             summarise(ymin = boxplot_whisker(lcoe, 'lower'),
                       lower = quantile(lcoe, c(0.25), na.rm = T),
                       middle = median(lcoe, na.rm = T),
                       upper = quantile(lcoe, c(0.75), na.rm = T),
                       ymax = boxplot_whisker(lcoe, 'upper')
                       )
  
  
  p<-ggplot(boxstats) +
    geom_boxplot(aes(x = factor(year), fill = sector, ymin = ymin, ymax = ymax, middle = middle, upper = upper, lower = lower ), stat = 'identity', outlier.shape = NA, na.rm = T) +
    facet_wrap(~sector) +
    scale_y_continuous(name = 'LCOE (c/kWh)') +
    scale_x_discrete(name = 'Year') +
    ggtitle('LCOE for All Agents') +
    scale_fill_manual(name = 'Sector', values = sector_fil) +
    theme_custom +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1)) 
  # aggregate summary stas on median and iqr to save to csv
  g = group_by(df,year,sector)
  out_data = collect(summarise(g, 
                               median = diffusion_shared.r_median(array_agg(lcoe)), 
                               lql = diffusion_shared.r_quantile(array_agg(lcoe), .25), 
                               uql = diffusion_shared.r_quantile(array_agg(lcoe), .75)
  )
  )
#   write.csv(out_data,paste0(runpath,'/lcoe_trends.csv'),row.names = FALSE)
  save(out_data,file = paste0(runpath,'/lcoe_trends.RData'),compress = T, compression_level = 1)
  return(p)
}

lcoe_cdf<-function(df, start_year, end_year){
  # CDF of lcoe for the first and final model year, faceted by sector. Note that
  # CDF is of bins, and is not weighted by # of custs in bin
  
  yrs = c(start_year, end_year)
  
  # filter the data to rows in the start or end year
  f = select(filter(df, year %in% yrs),year, sector, lcoe, cost_of_elec_dols_per_kwh)
  data = collect(f)
  data$sector = sector2factor(data$sector)
  g = group_by(f, year, sector)
  prices = collect(summarise(g,
                             price = diffusion_shared.r_median(array_agg(cost_of_elec_dols_per_kwh))
  )
  )
  prices$sector = sector2factor(prices$sector)
  
  
  ggplot(data=data)+
    stat_ecdf(aes(x = lcoe, colour = factor(year)))+
    geom_vline(data = prices, aes(xintercept = price, color = factor(year)), linetype = 'dashed') +    
    facet_wrap(~sector) +
    ggtitle('Cumulative Probability of Site LCOE (c/kWh)\n Dashed Lines are median retail elec prices')+
    scale_x_continuous(name = 'Levelized Cost of Energy (c/kWh)') +
    scale_y_continuous(name = 'Cumulative Probability', label = percent) +
    scale_color_discrete(name = 'Model Years') +
    theme_custom +
    annotate("segment", x=-Inf, xend=-Inf, y=-Inf, yend=Inf, colour =  "#9D9D9D", lwd = 2) +
    theme(axis.line.y = element_blank())
}



diffusion_all_map <- function(df){
  # aggregate the data
  g = group_by(df, state_abbr, year)
  diffusion_all = collect(summarise(g,
                                    Market.Share = sum(number_of_adopters)/sum(customers_in_bin)*100,
                                    Market.Value = sum(market_value),
                                    Number.of.Adopters = sum(number_of_adopters),
                                    Installed.Capacity = sum(installed_capacity)/1000,
                                    Annual.Generation =  sum(total_gen_twh)
                                  )
                          )
  # reset variable names
  names(diffusion_all)[1:2] = c('State','Year')
  # make sure states are treated as character and not factor
  diffusion_all$State = as.character(diffusion_all$State)
  # create the map
#   map = anim_choro_multi(diffusion_all, 'State', 
#                          c('Market.Share','Market.Value', 'Number.of.Adopters', 'Installed.Capacity', 'Annual.Generation'),
#                          pals = list(Market.Share = 'Blues', Market.Value = 'Greens', Number.of.Adopters = 'Purples', Installed.Capacity = 'Reds', Annual.Generation = 'YlOrRd'),
#                          ncuts = list(Market.Share = 5, Market.Value = 5, Number.of.Adopters = 5, Installed.Capacity = 5, Annual.Generation = 5), 
#                          classification = 'quantile',
#                          height = 400, width = 800, scope = 'usa', label_precision = 0, big.mark = ',',
#                          legend = T, labels = T, 
#                          slider_var = 'Year', slider_step = 2, map_title = 'Diffusion (Total)', horizontal_legend = F, slider_width = 300,
#                          legend_titles = list(Market.Share = 'Market Share (%)', Market.Value = 'Market Value ($)',
#                                               Number.of.Adopters = 'Number of Adopters (Count)', Installed.Capacity = 'Installed Capacity (MW)',
#                                               Annual.Generation = 'Annual Generation (GWh)'))
#   #save the map
#   showIframeSrc(map, cdn = T)
  
  valueVarConfigs <- list(
    c('Market.Share', 'Market Share', 'Blues', '%'), 
    c('Market.Value', 'Market Value', 'Greens', '$'), 
    c('Number.of.Adopters', 'Number of Adopters', 'Purples', 'Count'), 
    c('Installed.Capacity', 'Installed Capacity', 'Reds', 'MW'),
    c('Annual.Generation', 'Annual Generation', 'YlOrRd', 'GWh'))
  
  newMapParams <- "title: {floating: true, text: 'Diffusion (Total)', align: 'center', y: 50, style: {'fontSize': '32px', 'fontFamily': 'Verdana'}}"
  r_js_viz(data=diffusion_all,
     valueVarSettings=valueVarConfigs, 
     timeSettings=c('Year', 'Year'), 
     geogSettings=c('State', 'State'),
     mapParams=newMapParams,
     map='United States of America, mainland',
     nclasses=5,
     classification='quantile',
     tooltipPrecision=0,
     chart1Fixed=FALSE)
  }


diffusion_sectors_map <- function(df){
  # aggregate the data
  # get the unique sectors in the table
  sectors <- collect(summarise(df, distinct(sector)))[[1]]
  for (current_sector in sectors){
    f = filter(df, sector == current_sector)
    g = group_by(f, state_abbr, year)
    diffusion_sector = collect(summarise(g,
                                         Market.Share = sum(number_of_adopters)/sum(customers_in_bin)*100,
                                         Market.Value = sum(market_value),
                                         Number.of.Adopters = sum(number_of_adopters),
                                         Installed.Capacity = sum(installed_capacity)/1000,
                                         Annual.Generation =  sum(total_gen_twh)
                                        )
                              )
    
    newMapParams <- paste0("title: {floating: true, text: '", sprintf('Diffusion (%s)',toProper(current_sector)), "', y: 50, align: 'center', style: {'fontSize': '28px', 'fontFamily': 'Verdana'}}")
    
    # reset variable names
    names(diffusion_sector)[1:2] = c('State','Year')
    # make sure states are treated as character and not factor
    diffusion_sector$State = as.character(diffusion_sector$State)
    # create the map

    
    valueVarConfigs <- list(
      c('Market.Share', 'Market Share', 'Blues', '%'), 
      c('Market.Value', 'Market Value', 'Greens', '$'), 
      c('Number.of.Adopters', 'Number of Adopters', 'Purples', 'Count'), 
      c('Installed.Capacity', 'Installed Capacity', 'Reds', 'MW'),
      c('Annual.Generation', 'Annual Generation', 'YlOrRd', 'GWh'))
    
    r_js_viz(data=diffusion_sector,
       valueVarSettings=valueVarConfigs, 
       timeSettings=c('Year', 'Year'), 
       geogSettings=c('State', 'State'), 
       mapParams=newMapParams,
       map='United States of America, mainland',
       nclasses=5,
       classification='quantile',
       tooltipPrecision=0,
       chart1Fixed=FALSE)
    
  
  }
#   return(iframes)
}


################################################################################################################################################
# SUPPLY CURVES

cf_supply_curve<-function(df, by_load = T, by_tech = F, years = c(2014,2020,2030,2040,2050)){
  
  #' National capacity factor supply curve
  # filter to only the start year, returning only the naep and load_kwh_in_bin cols
#   data = collect(select(filter(df, year %in% years), naep, load_kwh_in_bin, tech, cf))
#   data<-data[order(-data$cf),]
#   data$load<-cumsum(data$load_kwh_in_bin/(1e6*8760))
  
  if (by_load == T){
    data = select(df, year, cf, load_kwh_in_bin, naep, sector, tech) %>%
      filter(year %in% years)
    if (by_tech == T){
      data = group_by(data, year, tech) %>%
        arrange(desc(cf)) %>%
        mutate(xmax = cumsum(load_kwh_in_bin/naep/1e6)) %>%
        collect() %>%
        mutate(xmin = 0) 
    } else {
      data = group_by(data, year) %>%
        arrange(desc(cf)) %>%
        mutate(xmax = cumsum(load_kwh_in_bin/naep/1e6)) %>%
        collect() %>%
        mutate(xmin = 0) 
    }
  } else {
    data = select(df, year, cf, customers_in_bin, naep, sector, tech) %>%
      filter(year %in% years)
    if (by_tech == T){
      data = group_by(data, year, tech) %>%
        arrange(desc(cf)) %>%
        mutate(xmax = cumsum(customers_in_bin)) %>%
        collect() %>%
        mutate(xmin = 0)
    } else {
      data = group_by(data, year) %>%
        arrange(desc(cf)) %>%
        mutate(xmax = cumsum(customers_in_bin)) %>%
        collect() %>%
        mutate(xmin = 0)      
    }
  }  
  
  if (by_tech == T){
    data$tech = simpleCap(data$tech)
    fgrid = facet_grid('~ year')
    color_var = 'tech'
    colors = tech_col
  } else {
    fgrid = geom_blank()
    color_var = 'year'
    colors = year_colors
  }
  
  if (by_load == T){
    title = 'Capacity (GW)'
  } else {
    title = 'Customers'
  }
  
  # If we chose to shade, this defines the width of the rectangle
  data[2:nrow(data),'xmin'] = data[1:nrow(data)-1,'xmax']
  data$year = as.factor(data$year)
  
  
  p = ggplot(data)+
    geom_line(aes_string(x = 'xmax', y = 'cf', color = color_var), size = 1.1)+
    fgrid + 
    scale_y_continuous(name = 'Capacity Factor', label = percent)+
    scale_x_continuous(name = title) +
    scale_color_manual(name = simpleCap(color_var), values = colors, labels = names(colors)) +
    ggtitle('Capacity Factor Supply Curve') +
    theme_custom +
    theme(axis.text.x = element_text(angle = 0, hjust = 0.5, vjust = 0.5)) +
    theme(legend.position = c(1, 0.79)) +
    theme(legend.justification = 'right')
  
  return(p)
}



elec_rate_supply_curve<-function(df, start_year){
  #' National electricity rate supply curve
  # filter to only the start year, returning only the cost_of_elec_dols_per_kwh and load_kwh_in_bin cols
  data = collect(select(filter(df, year == start_year),cost_of_elec_dols_per_kwh,load_kwh_in_bin))  
  data <- transform(data, load = load_kwh_in_bin/(1e6 * 8760), rate = cost_of_elec_dols_per_kwh) %>% arrange(desc(rate))
  data$load<-cumsum(data$load)
  
  ggplot(data)+
    geom_line(aes(x = rate, y = load), size = 1.1)+
    theme_custom +
    theme(axis.text.x = element_text(angle = 0, hjust = .5, vjust = 0)) +
    guides(size = FALSE)+
    scale_y_continuous(name ='Customer Load (GW)')+
    scale_x_continuous(name ='Average Cost of Electricity ($/kWh)', lim = c(0,quantile(data$rate,0.98)))+
    ggtitle('Electricity Rate Supply Curve') +
    theme(legend.position = c(1, 0.79)) +
    theme(legend.justification = 'right')
}


make_npv_supply_curve = function(df, by_load = T, by_tech = F, years = c(2014,2020,2030,2040,2050)){
  
  if (by_load == T){
    data = select(df, year, npv4, load_kwh_in_bin, naep, sector, tech) %>%
      filter(year %in% years & naep > 0)
    if (by_tech == T){
      data = group_by(data, year, tech) %>%
      arrange(desc(npv4)) %>%
      mutate(xmax = cumsum(load_kwh_in_bin/naep/1e6)) %>%
      collect() %>%
      mutate(xmin = 0) 
    } else {
      data = group_by(data, year) %>%
        arrange(desc(npv4)) %>%
        mutate(xmax = cumsum(load_kwh_in_bin/naep/1e6)) %>%
        collect() %>%
        mutate(xmin = 0) 
    }
  } else {
    data = select(df, year, npv4, customers_in_bin, naep, sector, tech) %>%
      filter(year %in% years)
    if (by_tech == T){
      data = group_by(data, year, tech) %>%
        arrange(desc(npv4)) %>%
        mutate(xmax = cumsum(customers_in_bin)) %>%
        collect() %>%
        mutate(xmin = 0)
    } else {
      data = group_by(data, year) %>%
        arrange(desc(npv4)) %>%
        mutate(xmax = cumsum(customers_in_bin)) %>%
        collect() %>%
        mutate(xmin = 0)      
    }
  }

  if (by_tech == T){
    data$tech = simpleCap(data$tech)
    fgrid = facet_grid('~ year')
    color_var = 'tech'
    colors = tech_col
  } else {
    fgrid = geom_blank()
    color_var = 'year'
    colors = year_colors
  }
  
  if (by_load == T){
    title = 'Capacity (GW)'
  } else {
    title = 'Customers'
  }
  
  # If we chose to shade, this defines the width of the rectangle
  data[2:nrow(data),'xmin'] = data[1:nrow(data)-1,'xmax']
  data$year = as.factor(data$year)
  
  p = ggplot(data)+
    geom_line(aes_string(x = 'xmax', y = 'npv4', color = color_var), size = 1.1)+
    fgrid + 
    scale_y_continuous(name ='Net Present Value ($2014/kW)')+
    scale_x_continuous(name = title) +
    scale_color_manual(name = simpleCap(color_var), values = colors, labels = names(colors)) +
    ggtitle('Net Present Value per kW ($/kW)\n(4% discount rate)') +
    theme_custom +
    theme(axis.text.x = element_text(angle = 0, hjust = 0.5, vjust = 0.5)) +
    theme(legend.position = c(1, 0.79)) +
    theme(legend.justification = 'right')
  
  return(p)
}


make_npv_supply_curve_by_sector = function(df, years = 2014){
  
  data = select(df, year, npv4, load_kwh_in_bin, naep,sector) %>%
    filter(year == years & naep > 0) %>%
    group_by(year) %>%
    arrange(desc(npv4)) %>%
    mutate(load_xmax = cumsum(load_kwh_in_bin/naep/1e6)) %>%
    collect() %>%
    mutate(load_xmin = 0)
  
  # If we chose to shade, this defines the width of the rectangle-- the xmin starts at the xmax of the previous row 
  data[2:nrow(data),'load_xmin'] = data[1:nrow(data)-1,'load_xmax']
  data$sector = sector2factor(data$sector)
  
  p = ggplot(data)+
    #geom_line(aes(x = load_xmax,y = npv4, color = factor(year)), size = 2)+
    geom_rect(aes(xmin = load_xmin, xmax = load_xmax, ymin = 0, ymax = npv4, fill = sector), alpha = 1)+
    scale_y_continuous(name ='Net Present Value ($2014/kW)')+
    scale_x_continuous(name ='Capacity (GW)')+
    scale_fill_manual(name = 'Sector', values = sector_fil) +
    ggtitle('Net Present Value per kW ($/kW)\n(4% discount rate)') +
    theme_custom +
    theme(axis.text.x = element_text(angle = 0, hjust = 0.5, vjust = 0.5)) +
    theme(legend.position = c(0, 0.2)) +
    theme(legend.justification = 'left')
  
  return(p)
}


make_lcoe_supply_curve = function(df, years = c(2014,2020,2030,2040,2050), max_lcoe = .30){
  
  data = select(df, year, lcoe, load_kwh_in_bin, naep,sector) %>%
    filter(year %in% years & naep > 0) %>%
    group_by(year) %>%
    arrange(lcoe) %>%
    mutate(load_xmax = cumsum(load_kwh_in_bin/naep/1e6)) %>%
    collect() %>%
    mutate(load_xmin = 0)
  
  # If we chose to shade, this defines the width of the rectangle
  data[2:nrow(data),'load_xmin'] = data[1:nrow(data)-1,'load_xmax']
  data$lcoe = data$lcoe/100.
  ymax = min(1.2*max(data$lcoe), max_lcoe)
  data = filter(data, lcoe <= ymax)
  xmax = max(data$load_xmax)
          
  
  p = ggplot(data)+
    geom_line(aes(x = load_xmax, y = lcoe, color = factor(year)), size = 1.1)+
    scale_y_continuous(name ='LCOE ($/kWh)', limits =c(0, ymax), breaks = seq(0, ymax, 0.05)) +
    scale_x_continuous(name ='Capacity (GW)', limits = c(0, xmax))+
    scale_color_manual(name = 'Year', values = year_colors, labels = names(year_colors)) +
    theme_custom + 
    theme(legend.position = c(1, 0.2)) +
    theme(legend.justification = 'right')
  
  
  return(p)
}






###### Functions for the batch-mode scenario analysis ######################

get_csv_data<-function(scen_folders, file_name){
  df<-data.frame()
  for(path in scen_folders){
    tmp<-read.csv(paste0(path,'/',file_name,'.csv'))
    df<-rbind(df,tmp)
  }
  return(df)
}

get_r_data<-function(scen_folders, file_name){
  df<-data.frame()
  for(path in scen_folders){
    full_path = paste0(path,'/',file_name,'.RData')
    if(file.exists(full_path)){
      
      tmp_name<-load(full_path)
      df<-rbind(df,get(tmp_name))
      rm(list = c(tmp_name))
      
    } else {
      sprintf("file: \'%s\' does not exist", full_path)
    }
  }
  return(df)
}

all_sectors_diff_trends<-function(df){
  
  df = group_by(df, year, variable, scenario) %>%
	summarise(value = sum(value, na.rm = T))

  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    theme(strip.text.x = element_text(size = 16))+
    theme(axis.text.y = element_text(size = 16))+
    ggtitle('National Diffusion Trends (All Sectors)')
}

res_diff_trends<-function(df){
  df<-subset(df, sector ==  "residential")
  if(nrow(df) == 0){
    print('No Residential Installations Modeled')
  } else {
  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    expand_limits(y=0)+
    ggtitle('National Residential Diffusion Trends \n Note different axis Limits than above')
  }
}

com_diff_trends<-function(df){
  df<-subset(df, sector ==  "commercial")
  if(nrow(df) == 0){
    print('No Commercial Installations Modeled')
  } else {
  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    expand_limits(y=0)+
    ggtitle('National Commercial Diffusion Trends \n Note different axis Limits than above')
  }
}

ind_diff_trends<-function(df){
  df<-subset(df, sector ==  "industrial")
  if(nrow(df) == 0){
    print('No Industrial Installations Modeled')
  } else {
  ggplot(data=df,aes(x = year, y = value, color = scenario, fill = scenario))+
    geom_line(size = 1)+
    facet_wrap(~variable,scales="free_y")+
    theme_few()+
    expand_limits(y=0)+
    ggtitle('National Industrial Diffusion Trends \n Note different axis Limits than above')
  }
}

diff_trends_table<-function(diff_trends){
  
  nat_trends_by_sector<-dcast(diff_trends, variable + sector + scenario ~ year)
  nat_trends_by_sector$variable<-gsub(pattern='nat',replacement='National',x=nat_trends_by_sector$variable)
  nat_trends_by_sector$variable<-gsub(pattern='_',replacement=' ',x=nat_trends_by_sector$variable)
  
  nat_trends<-dcast(diff_trends, variable + scenario ~ year, fun.aggregate= sum)
  nat_trends$variable<-gsub(pattern='nat',replacement='National',x=nat_trends$variable)
  nat_trends$variable<-gsub(pattern='_',replacement=' ',x=nat_trends$variable)
  
  print_table(nat_trends, caption = 'National Diffusion Trends')
  
  for(trend in unique(nat_trends_by_sector$variable)){
    tab<-subset(nat_trends_by_sector, variable == trend)
    tab<-tab[,names(tab) != 'variable']
    print_table(tab, caption = trend)
  } 
}

turb_trends_hist<-function(df){
  
  # Correct order of factors
  df$system_size_factors <- ordered( df$system_size_factors, levels = c(2.5,5,10,20,50,100,250,500,750,1000,1500,'1500+'))
  
  # What size system are customers selecting in 2014?
  df1<-subset(df, year == min(year))
  df2<-subset(df, year == max(year))
  
  p1<-ggplot(df1, aes(x = factor(system_size_factors), weight = p, fill = scenario))+
    geom_histogram(position = 'dodge')+
    facet_wrap(~sector)+
    theme_few()+
    scale_y_continuous(name ='Percent of Customers Selecting System Size', labels = percent)+
    scale_x_discrete(name ='Optimal Size System for Customer (kW)')+
    theme(axis.text.x = element_text(angle = 45, hjust = 1))+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    ggtitle(sprintf('Size of System Considered in %s', min(df$year)))
  
  p2<-ggplot(df2, aes(x = factor(system_size_factors), weight = p, fill = scenario))+
    geom_histogram(position = 'dodge')+
    facet_wrap(~sector)+
    theme_few()+
    scale_y_continuous(name ='Percent of Customers Selecting System Size', labels = percent)+
    scale_x_discrete(name ='Optimal Size System for Customer (kW)')+
    theme(axis.text.x = element_text(angle = 45, hjust = 1))+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    ggtitle(sprintf('Size of System Considered in %s', max(df$year)))

  out = list('p1' = p1, 'p2' = p2)  
return(out)
}

metric_trends_ribbon<-function(df){
  
  df = as.data.frame(collect(df))
  # Create two plots for the two metrics
  pp<-filter(df, metric == 'Payback Period')
  mbs<-filter(df, metric == 'Percent Monthly Bill Savings')
  
  # Median econ attractiveness over time and sector  
  p1<-ggplot(pp, aes(x = year, y = median, ymin = lql, ymax = uql, color = scenario, fill = scenario), size = 0.75)+
    geom_smooth(stat = 'identity', alpha = 0.2)+
    geom_line()+
    facet_wrap(~sector)+
    scale_y_continuous(name ='Median Payback Period (years)', lim=c(0,40))+
    scale_x_continuous(name ='Year')+
    theme_few()+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    ggtitle('National Payback Period Range (Median and Inner-Quartile Range)')
  
  p2<-ggplot(mbs, aes(x = year, y = median, ymin = lql, ymax = uql, color = scenario, fill = scenario), size = 0.75)+
    geom_smooth(stat = 'identity', alpha = 0.2)+
    geom_line()+
    facet_wrap(~sector)+
    scale_y_continuous(name ='Median Monthly Bill Savings (% of pre-adoption bill)', lim=c(0,2), label = percent)+
    scale_x_continuous(name ='Year')+
    theme_few()+
    theme(strip.text.x = element_text(size=12, angle=0,))+
    ggtitle('National Monthly Bill Savings Range (Median and Inner-Quartile Range)')
  
  out = list("p1" = p1, "p2" = p2)
  return(out)
}

dist_of_azimuth_selected<-function(df, start_year){
  
  d<-filter(df, year == start_year) %.%
    group_by(year, sector, azimuth) %.%
    summarise(cap = sum(system_size_kw)) %.%
    collect()
  
  # Reorder the azimuth category order
  d$azimuth2 <- factor(d$azimuth, levels = c('W','SW','S','SE','E'))
  d$sector = sector2factor(d$sector)
  
  ggplot(d, aes(x = azimuth2, weight = cap, fill = sector))+
    geom_bar(position = 'dodge', alpha = 0.5)+
    facet_wrap(~sector, scales = "free_y")+
    scale_y_continuous(name ='Number of systems selected')+
    scale_fill_manual(values = sector_fil)+
    xlab('Azimuth')+
    theme_custom +
    theme(axis.text.x = element_text(size=12, angle=0, hjust = 0.5, vjust = 0.5))+
    ggtitle('Optimal system orientations in 2014')
}

leasing_mkt_share<-function(df, start_year, end_year, sectors){
  data = collect(
    select(df, year,sector,business_model,new_capacity) %>%
    group_by(year,sector,business_model)%>%
    summarise(annual_capacity_gw = sum(new_capacity)/1e6))
  
  data2 = group_by(data, year, sector) %>%
    summarise(tot_cap = sum(annual_capacity_gw))
  
  data3 = merge(data,data2) %>%
    mutate(per_mkt_share = annual_capacity_gw/tot_cap) %>%
    filter(business_model == 'tpo')
  
  data3$sector = sector2factor(data3$sector)
  if (length(sectors) > 1){
    for (current_sector in sectors){
      if (!(current_sector %in% unique(data3$sector)) )
        data3[nrow(data3)+1,] = data.frame(min(data3$year), current_sector, 'tpo', as.numeric(0), as.numeric(0), as.numeric(0)) 
    }
  }

  
  plot<-ggplot(data3) +
    geom_area(aes(x = year, y = per_mkt_share, fill = sector), alpha = 0.75) +
    facet_wrap(~sector)+
    scale_fill_manual(values = sector_col, name = 'Sector')+
    scale_y_continuous("% of Added Capacity", label = percent) +
    scale_x_continuous(name ='Year', breaks = seq(start_year, end_year, 2)) +
    ggtitle("Leasing Market Share") +
    theme_custom +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1)) +
    annotate("segment", x=-Inf, xend=-Inf, y=-Inf, yend=Inf, colour =  "#9D9D9D", lwd = 2) +
    theme(axis.line.y = element_blank())
  
  # Table of market share by state and year (aggregating sectors)
  data = collect(
    select(df, year, state_abbr,business_model,new_capacity) %>%
    group_by(year,state_abbr,business_model)%>%
    summarise(annual_capacity_gw = sum(new_capacity)/1e6))
  
  data2 = group_by(data, year, state_abbr) %>%
    summarise(tot_cap = sum(annual_capacity_gw))
  
  table <- merge(data,data2) %>%
    mutate(per_mkt_share = annual_capacity_gw/tot_cap) %>%
    filter(business_model == 'tpo')%>%
    select(year, state_abbr, market_share = per_mkt_share)%>%
    dcast(state_abbr ~ year, value.var = 'market_share')
  
  l = list("plot" = plot, "table" = table)  
  return(l)
}

cum_installed_capacity_by_bm<-function(df, start_year, end_year){
    
  data = collect(
    select(df, year,sector,business_model,new_capacity) %>%
    group_by(year, sector, business_model) %>%
    summarise(cap = sum(new_capacity/1e6)) %>%
    arrange(year)
    )
  
  # make sure all years are represented
  years = data.frame(year = seq(start_year, end_year, 2))
  sectors = data.frame(sector = unique(data$sector))
  bms = data.frame(business_model = c('tpo', 'host_owned'))
  all_combos = merge(merge(years, sectors, all = T), bms, all = T)
  
  data2 = merge(all_combos, data, by = c('year', 'sector', 'business_model'), all.x = T)
  # fill NAs
  data2[is.na(data2$cap), 'cap'] = 0
  
  data3 = arrange(data2, sector, business_model, year) %>%
    group_by(business_model, sector) %>%
    mutate(cs = cumsum(cap)) %>%
    arrange(year, business_model, sector) %>%
    collect()
  
  data3$sector = sector2factor(data3$sector)
  data3$business_model = as.character(data3$business_model)
  data3[data3$business_model == 'tpo', 'business_model'] = 'TPO'
  data3[data3$business_model == 'host_owned', 'business_model'] = 'Host-Owned'
  data3$business_model = factor(data3$business_model, levels = c('Host-Owned', 'TPO'))
  
  plot<-ggplot(data3)+
    geom_area(aes(x = year, y = cs, fill = business_model), position = 'stack')+
    facet_wrap(~sector, scales = 'free_y')+
    xlab("")+
    scale_y_continuous("Installed Capacity (GW)") +
    scale_x_continuous(name = "Year", breaks = seq(start_year, end_year, 2)) +
    scale_fill_manual(name = 'Business Model', guide = guide_legend(reverse=TRUE), values = c('dark green','light green')) +
    ggtitle("Cumulative Installed Capacity Since 2014") +
    theme_custom +
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1))
  
  return(plot)
  
}
