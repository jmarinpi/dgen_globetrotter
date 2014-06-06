library(rMaps)

# prototyping
i = ichoropleth(Crime ~ State, data = violent_crime, animate = "Year")

d = rMaps::Datamaps$new()
d$templates$script = '/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/r/maps/dmap.html'
d$params$id = 'chart1'
d$set(height = 400)
d$set(width = 800)
d$set(scope = 'usa')
d$set(legend = T)
d$set(labels = T)
slider_min = 1960
slider_max = 2008
slider_step = 1
d$set(bodyattrs = "ng-app ng-controller='rChartsCtrl'")
d$setTemplate(chartDiv = sprintf("
                                   <div class='container ng-scope' ng-app ng-controller = 'rChartsCtrl'>
                                   <label for=slider>Year</label>
                                   <input id='slider' type='range' min=%s max=%s step=%s ng-model='time' width=200 oninput='outputUpdate(value)' onchange='outputUpdate(value)'>
                                   <output for=slider id=current_slide_val>%s</output>
                                   <script>
                                   function outputUpdate(vol) {
                                   document.querySelector('#current_slide_val').value = vol;
                                   }
                                   </script>
                                   
                                   <br></br>
                                   <div id='{{chartId}}' class='rChart datamaps'></div>  
                                   </div>
                                   <script>
                                   function rChartsCtrl($scope){
                                   $scope.time = %s;
                                   $scope.$watch('time', function(newTime){
                                    map{{chartId}}.updateChoropleth(chartParams.newData[newTime]);
                                   })
                                   }
                                   </script>", slider_min, slider_max, slider_step, slider_min,slider_min )
)

# critiical to include jshead
d$addAssets(jshead = "http://cdnjs.cloudflare.com/ajax/libs/angular.js/1.2.1/angular.min.js")
d$set(bodyattrs = "ng-app ng-controller='rChartsCtrl'")
d$params$data = i$params$data
d$params$newData = i$params$newData
d$params$fills = i$params$fills






# try out button

d$setTemplate(chartDiv = sprintf("
                                   <div class='container ng-scope' ng-app ng-controller = 'rChartsCtrl'>
                                   <label for=slider>Year</label>
                                   <input id='slider' type='range' min=%s max=%s step=%s ng-model='time' width=200 oninput='outputUpdate(value)' onchange='outputUpdate(value)'>
                                   <output for=slider id=current_slide_val>2010</output>
                                   <br></br>
                                   <select ng-model='selection' class='form-control'
                                               ng-options=\"selection for selection in ['blue', 'red']\">
                                              {{ selection }}
                                            </select>
                                   <script>
                                   function outputUpdate(vol) {
                                   document.querySelector('#current_slide_val').value = vol;
                                   }
                                   </script>
                                   
                                   <br></br>
                                   <div id='{{chartId}}' class='rChart datamaps'></div>  
                                   </div>

                                  
                                   <script>

                                    function removeElementsByClass(className){
                                        elements = document.getElementsByClassName(className);
                                        while(elements.length > 0){
                                            elements[0].parentNode.removeChild(elements[0]);
                                        }
                                    }

                                   function rChartsCtrl($scope){
                                   $scope.time = %s;
                                   $scope.$watch('time', function(newTime){
                                    map{{chartId}}.updateChoropleth(chartParams.data[newTime]);
                                   });
                                   $scope.selection = 'red';
                                   $scope.$watch('selection', function(newSelection, document){
                                    chartParams.data = chartParams.newData[newSelection];
                                    chartParams.fills = chartParams.allFills[newSelection];
                                    map{{chartId}}.updateChoropleth(chartParams.data[$scope.time]); 
                                    removeElementsByClass('datamaps-legend')
                                    map{{chartId}}.legend();
                                   })
                                   }
                                   </script>", slider_min, slider_max, slider_step, slider_min)
)


i = ichoropleth(Crime ~ State, data = violent_crime, animate = "Year")
i2 = ichoropleth(Crime ~ State, data = violent_crime, animate = "Year", ncuts = 7, pal = 'Reds')
orig_data = d$params$newData

m = list(blue = i$params$newData, red = i2$params$newData)
d$params$newData = m
d$params$data = d$params$newData[[1]][[1]]
d$params$allFills = list(blue = i$params$fills, red = i2$params$fills)
d$params$fills = d$params$allFills[['red']]
d$save('/Users/mgleason/d.html')




##############################################################################
#  formal functions

cut.format = function (x, breaks, labels = NULL, include.lowest = FALSE, right = TRUE, 
          dig.lab = 3L, ordered_result = FALSE, ...) 
{
  if (!is.numeric(x)) 
    stop("'x' must be numeric")
  if (length(breaks) == 1L) {
    if (is.na(breaks) || breaks < 2L) 
      stop("invalid number of intervals")
    nb <- as.integer(breaks + 1)
    dx <- diff(rx <- range(x, na.rm = TRUE))
    if (dx == 0) 
      dx <- abs(rx[1L])
    breaks <- seq.int(rx[1L] - dx/1000, rx[2L] + dx/1000, 
                      length.out = nb)
  }
  else nb <- length(breaks <- sort.int(as.double(breaks)))
  if (anyDuplicated(breaks)) 
    stop("'breaks' are not unique")
  codes.only <- FALSE
  if (is.null(labels)) {
    for (dig in dig.lab:max(12L, dig.lab)) {
      ch.br <- formatC(breaks, digits = dig, width = 1L)
      if (ok <- all(ch.br[-1L] != ch.br[-nb])) 
        break
    }
    labels <- if (ok) 
      paste0(ch.br[-nb], " - ", ch.br[-1L])
    else paste("Range", seq_len(nb - 1L), sep = "_")
    if (ok && include.lowest) {
      if (right) 
        substr(labels[1L], 1L, 1L) <- "["
      else substring(labels[nb - 1L], nchar(labels[nb - 
                                                     1L], "c")) <- "]"
    }
  }
  else if (is.logical(labels) && !labels) 
    codes.only <- TRUE
  else if (length(labels) != nb - 1L) 
    stop("lengths of 'breaks' and 'labels' differ")
  code <- .bincode(x, breaks, right, include.lowest)
  if (codes.only) 
    code
  else factor(code, seq_along(labels), labels, ordered = ordered_result)
}




prep_choro_data = function (formula, data, pal = "Blues", ncuts = 5, slider = NULL)
{
  fml = lattice::latticeParseFormula(formula, data = data)
  data = transform(data, fillKey = cut.format(fml$left, quantile(fml$left, 
                                                          seq(0, 1, 1/ncuts)), ordered_result = TRUE, dig.lab = 10))
  fillColors = RColorBrewer::brewer.pal(ncuts, pal)
  fills = as.list(setNames(fillColors, levels(data$fillKey)))
  if (!is.null(slider)) {
    range_ = summary(data[[slider]])
    data = plyr::dlply(data, slider, function(x) {
      y = rCharts::toJSONArray2(x, json = F)
      names(y) = lapply(y, "[[", fml$right.name)
      return(y)
    })
  } else {
    data = plyr::dlply(data, fml$right.name)
  }
  return(list(data = data, fills = fills))
}

x1 = prep_choro_data(Crime ~ State, data = violent_crime, slider = "Year", pal = "Blues", ncuts = 5)
x2 = prep_choro_data(Crime ~ State, data = violent_crime, slider = "Year", pal = "Reds", ncuts = 8)

combined_data = list(blues = x1$data, reds = x2$data)
combined_fills = list(blues = x1$fills, reds = x2$fills)




anim_choro_multi = function(data, fills, height = 400, width = 800, scope = 'usa', legend = T, labels = T, 
                            slider_step = 2){
  
  d = rMaps::Datamaps$new()
  d$templates$script = '/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/r/maps/dmap.html'
  d$params$id = 'chart1'
  d$set(height = height)
  d$set(width = width)
  d$set(scope = scope)
  d$set(legend = legend)
  d$set(labels = labels)
  # critical to include jshead and body attrs
  d$addAssets(jshead = "http://cdnjs.cloudflare.com/ajax/libs/angular.js/1.2.1/angular.min.js")
  d$set(bodyattrs = "ng-app ng-controller='rChartsCtrl'")
  
  d$params$newData = data
#   if (slider == T){
#     d$params$data = d$params$newData[[1]][[1]]
#   } else {
#     d$params$data = d$params$newData[[1]]    
#   }

  d$params$data = d$params$newData[[1]][[1]]

  d$params$allFills = fills
  d$params$fills = d$params$allFills[[1]]
  
  slider_maxs = c()
  slider_mins = c()
  for (i in seq(1,length(names(data)))) {
    slider_vals = summary(as.integer(names(data[[i]])))
    slider_mins = c(slider_vals[1],slider_mins)
    slider_maxs = c(slider_vals[6],slider_maxs)
  }
  slider_min = min(slider_mins)
  slider_max = max(slider_maxs)
  selections = gsub('[ ]','',paste("'",names(data),"'",collapse = ','))
  default_selection = names(data)[1]
  d$setTemplate(chartDiv = sprintf("
                                   <div class='container ng-scope' ng-app ng-controller = 'rChartsCtrl'>
                                   <label for=slider>Year</label>
                                   <input id='slider' type='range' min=%s max=%s step=%s ng-model='time' width=200 oninput='outputUpdate(value)' onchange='outputUpdate(value)'>
                                   <output for=slider id=current_slide_val>%s</output>
                                   <br></br>
                                   <select ng-model='selection' class='form-control'
                                   ng-options=\"selection for selection in [%s]\">
                                    {{ selection }}
                                   </select>
                                   <script>
                                   function outputUpdate(vol) {
                                   document.querySelector('#current_slide_val').value = vol;
                                   }
                                   </script>
                                   
                                   <br></br>
                                   <div id='{{chartId}}' class='rChart datamaps'></div>  
                                   </div>
                                   
                                   
                                   <script>
                                   
                                   function removeElementsByClass(className){
                                   elements = document.getElementsByClassName(className);
                                   while(elements.length > 0){
                                   elements[0].parentNode.removeChild(elements[0]);
                                   }
                                   }
                                   
                                   function rChartsCtrl($scope){
                                   $scope.time = %s;
                                   $scope.$watch('time', function(newTime){
                                   map{{chartId}}.updateChoropleth(chartParams.data[newTime]);
                                   });
                                   $scope.selection = '%s';
                                   $scope.$watch('selection', function(newSelection, document){
                                   chartParams.data = chartParams.newData[newSelection];
                                   chartParams.fills = chartParams.allFills[newSelection];
                                   map{{chartId}}.updateChoropleth(chartParams.data[$scope.time]); 
                                   removeElementsByClass('datamaps-legend')
                                   map{{chartId}}.legend();
                                   })
                                   }
                                   </script>", slider_min, slider_max, slider_step, slider_min, selections, slider_min, default_selection )
        )

  return(d)
}


m = anim_choro_multi(combined_data, combined_fills, height = 400, width = 900, scope = 'usa', legend = T, labels = T, slider_step = 2)
m$save('/Users/mgleason/d.html')

# to include in markdown
# put:
<iframe src="file:///Users/mgleason/d.html" name="map" height=400 width=800>
  </iframe>
# in the md file`














ba_map <- function(include_slider = T, slider_min = 2010, slider_max = 2030, slider_step = 5){
  # need to use rMaps because rCharts does not correctly translate setProjection function (or any js literals)
d = rMaps::Datamaps$new()
d$templates$script = './dmap.html'
d$params$id = 'chart1'
d$set(height = 720)
d$set(width = 800)
# relative path is very important here
d$set(geographyConfig = list(dataUrl = "./data/weccbas.topo.json"))
d$params$geographyConfig['popupTemplate'] = "#! function(geography, data) { //this function should just return a string
return '<div class=hoverinfo><strong>' + geography.properties.ba + '</strong></div>';
}  !#"
d$set(scope = 'weccbas')
d$set(setProjection = "#! function(element, options) {
      var projection = d3.geo.conicConformal()
      .scale(1400)
      .center([-120,65])
      .rotate([35,35,-15]);
      
      var path = d3.geo.path().projection(projection);
      return {path: path, projection: projection};
      } !#")
# set palette for fill colors
d$set(fills = map_palette)

# add slider
if (include_slider){
  # these two lines have no effect in shiny, but work if you save map out to html
  #     d$set(bodyattrs = "ng-app ng-controller='rChartsCtrl'") # for shiny, embed this into the setTemplate string
  #     d$addAssets(jshead = "http://cdnjs.cloudflare.com/ajax/libs/angular.js/1.2.1/angular.min.js")# for shiny, embed this in the html template as: <script src='http://cdnjs.cloudflare.com/ajax/libs/angular.js/1.2.1/angular.min.js' type='text/javascript'></script>
  
  
  d$setTemplate(chartDiv = sprintf("
                                   <div class='container' ng-app ng-controller = 'rChartsCtrl'>
                                   <label for=slider>Year</label>
                                   <input id='slider' type='range' min=%s max=%s step=%s ng-model='time' width=200 oninput='outputUpdate(value)' onchange='outputUpdate(value)'>
                                   <output for=slider id=current_slide_val>2010</output>
                                   <script>
                                   function outputUpdate(vol) {
                                   document.querySelector('#current_slide_val').value = vol;
                                   }
                                   </script>
                                   
                                   <br></br>
                                   <div id='{{chartId}}' class='rChart datamaps'></div>  
                                   </div>
                                   <script>
                                   function rChartsCtrl($scope){
                                   $scope.time = %s;
                                   $scope.$watch('time', function(newTime){
                                   map{{chartId}}.bubbles(chartParams.bubbles[newTime], chartParams.bubbleOptions);
                                   })
                                   }
                                   </script>", slider_min, slider_max, slider_step, slider_min)
  )
}

return(d)  

}


base_map = ba_map()

update_capacity_map <-function(ba_map, df, max_bubble_radius = 30) {
  
  ba_map$set(bubbles = c())
  
  #   input = list('bigQ' = 'Hydro', 'bubble_scale' = 'Constant for All Techs')
  #   yr = input$yr_slider
  bigQ = input$cap_tech
  
  if (bigQ == 'All Technologies'){
    sub_df = aggregate(value ~ ba + allyears, data = df, sum)
    color = techs[[bigQ]]
  } else {
    sub_df =  df[df$bigQ == bigQ,]
    color = techs[[bigQ]]
  }
  
  
  if (input$bubble_scale == 'Constant for All Techs') {
    max_value = max(df$value)
  } else {
    max_value = max(sub_df$value)
  }
  
  # bubbles 
  wecc_pts_w_cap = merge(sub_df,wecc_pts,by = 'ba' )
  wecc_pts_w_cap$radius = wecc_pts_w_cap$value/max_value * max_bubble_radius
  wecc_pts_w_cap$fillKey = color
  
  # split into separate dataframes by year
  wecc_pt_bubbles = list()
  split_df = split(wecc_pts_w_cap, wecc_pts_w_cap$allyears)
  for (year in names(split_df)){
    year_data = dlply(split_df[[year]],'ba',as.list)
    names(year_data) = c()
    attributes(year_data) = NULL  
    wecc_pt_bubbles[[year]] = year_data
  }
  
  # add the bubble sto the map and set the popup info
  ba_map$set(bubbles = wecc_pt_bubbles)
  popup_js = sprintf("#! function(geography, data) { //this function should just return a string
                             return '<div class=hoverinfo><strong>Balancing Area: ' + data.ba +
                              '<br/>%s Capacity: ' + data.value + ' MW</strong></div>';
                                  }  !#", bigQ)
  ba_map$set(bubbleOptions = list(popupTemplate = popup_js
                                  , exitDelay = 10000))
  
  if (input$include_trans){
    ba_map$set(lines = trans_lines)
    ba_map$set(lineOptions = list(strokeColor = '#FFDF23', 
                                  strokeDashArray = c(3,6),
                                  animation = 'slowflow 2s linear alternate infinite',
                                  #                                 animation = 'flow 10s linear infinite',
                                  popupOnHover = T
    ))
    ba_map$params$lineOptions['popupTemplate'] = "#! function(geography, data) { //this function should just return a string
    return '<div class=hoverinfo><strong>' + data.capacity_mw + '</strong></div>';
  }  !#"
  } else {
    ba_map$set(lines = c())
  }
  
  #   ba_map$save('./data/index.html', cdn = T)
  return(ba_map)  
}