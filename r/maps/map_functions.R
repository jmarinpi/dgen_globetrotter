library(rMaps)
library(rCharts)
library(reshape2)
library(plyr)
library(RColorBrewer)
library(lattice)


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
      ch.br <- formatC(breaks, digits = dig, format = 'f')
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




prep_choro_data = function (formula, data, pal = "Blues", ncuts = 5, slider = NULL, label_precision = 2)
{
  fml = lattice::latticeParseFormula(formula, data = data)
  data = transform(data, fillKey = cut.format(fml$left, quantile(fml$left, 
                                                                 seq(0, 1, 1/ncuts)), ordered_result = TRUE, dig.lab = label_precision))
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





anim_choro_multi = function(data_frame, region_var, value_vars, pals = list(), ncuts = list(), height = 400, width = 800, scope = 'usa', legend = T, labels = T, 
                            slider_var = NULL, slider_step = 2, legend_title = T, map_title = NULL,
                            label_precision = 2){
  
  data = list()
  fills = list()
  for (value_var in value_vars){
    formula = as.formula(paste(value_var,region_var, sep = '~'))
    pal = pals[[value_var]]
    ncut = ncuts[[value_var]]
    var_prep = prep_choro_data(formula, data_frame, pal = pal, ncuts = ncut, slider = slider_var, label_precision = label_precision)
    data[[value_var]] = var_prep$data
    fills[[value_var]] = var_prep$fills
  }
  
  d = rMaps::Datamaps$new()
  d$templates$script = '/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/r/maps/dmap.html'
  d$params$id = 'chart1'
  d$set(map_title = map_title)
  d$set(height = height)
  d$set(width = width)
  d$set(scope = scope)
  d$set(legend = legend)
  d$set(labels = labels)
  # critical to include jshead and body attrs
  d$addAssets(jshead = "http://cdnjs.cloudflare.com/ajax/libs/angular.js/1.2.1/angular.min.js")
  d$set(bodyattrs = "ng-app ng-controller='rChartsCtrl'")
  
  # assign data to the datamap
  d$params$newData = data
    if (!is.null(slider_var)){
      if (length(value_vars) > 1){
        # set to the first variable's data, and first element of slider
        d$params$data = d$params$newData[[1]][[1]]
      } else {
        d$params$data = d$params$newData[[1]]
      }
      
    } else {
        # set to the first varaible's data
        d$params$data = d$params$newData[[1]]            
    }
  
  # assign fills to the datamap
  d$params$allFills = fills
  # set to the first set of fills (corresponding to the first dataset)
  d$params$fills = d$params$allFills[[1]]
  
  if (!is.null(slider_var)){
    slider_maxs = c()
    slider_mins = c()
    for (i in seq(1,length(names(data)))) {
      slider_vals = summary(as.integer(names(data[[i]])))
      slider_mins = c(slider_vals[1],slider_mins)
      slider_maxs = c(slider_vals[6],slider_maxs)
    }
    slider_min = min(slider_mins)
    slider_max = max(slider_maxs)
    
    slider_div = sprintf("<label for=slider>%s</label>
                                <input id='slider' type='range' min=%s max=%s step=%s ng-model='time' width=200 oninput='outputUpdate(value)' onchange='outputUpdate(value)'>
                                <output for=slider id=current_slide_val>%s</output>
                                <br></br>
                         <script>
                               function outputUpdate(vol) {
                               document.querySelector('#current_slide_val').value = vol;
                               }
                         </script>", slider_var, slider_min, slider_max, slider_step, slider_min)
    slider_function = sprintf("$scope.time = %s;
                                $scope.$watch('time', function(newTime){
                                  map{{chartId}}.updateChoropleth(chartParams.data[newTime]);
                                });", slider_min)
    select_update = 'map{{chartId}}.updateChoropleth(chartParams.data[$scope.time]);'
    
  } else {
    select_update = 'map{{chartId}}.updateChoropleth(chartParams.data);'
    slider_div = ''
    slider_function = ''
  }
  
  if (length(value_vars) > 1){
    selections = gsub('[ ]','',paste("'",names(data),"'",collapse = ','))
    default_selection = names(data)[1]
    select_div = sprintf("<select ng-model='selection' class='form-control'
                                     ng-options=\"selection for selection in [%s]\">
                                     {{ selection }}
                                     </select>
                         <br></br>",selections)
    if (legend_title == T){
      legend_options = '{legendTitle: newSelection}'  
    } else {
      legend_options = ''
    }
    
    if (legend == T){
      legend_function = sprintf("map{{chartId}}.legend(%s);", legend_options)
    } else {
      legend_function = ''
    }
    
    select_function = sprintf("$scope.selection = '%s';
                              $scope.$watch('selection', function(newSelection, document){
                                chartParams.data = chartParams.newData[newSelection];
                                chartParams.fills = chartParams.allFills[newSelection];
                                %s                                
                                removeElementsByClass('datamaps-legend');
                                %s
                              })", default_selection, select_update, legend_function)
  } else {
    select_div = ''
    select_function = ''
    if (legend_title == T){
      d$set(legendOptions = list(legendTitle = value_vars[1]))
    }
  }
    
  #/map{{chartId}}.updateChoropleth(chartParams.data[$scope.time]); 
if (!is.null(slider_var) | length(value_vars) > 1){
  chartDiv = sprintf("<div class='container ng-scope' ng-app ng-controller = 'rChartsCtrl'>
                     %s
                     %s
                    <div id='{{chartId}}' class='rChart datamaps'></div>  
                    <script>                                     
                       function removeElementsByClass(className){
                         elements = document.getElementsByClassName(className);
                         while(elements.length > 0){
                           elements[0].parentNode.removeChild(elements[0]);
                         }
                       }

                       function rChartsCtrl($scope){
                        %s
                        %s
                       }
                       </script>", slider_div, select_div, slider_function, select_function)
  d$setTemplate(chartDiv = chartDiv)
  
#   popup_js = sprintf("#! function(geography, data) { 
#                              return '<div class=hoverinfo><strong>
#                               Balancing Area: ' + data.ba +'<br/>
#                               %s Capacity: ' + data.value + ' MW</strong></div>';
#                                   }  !#", bigQ)
  
  
  } 

  return(d)
}



# d$params$geographyConfig['popupTemplate'] = "#! function(geography, data) { //this function should just return a string
# return '<div class=hoverinfo><strong>' + geography.properties.ba + '</strong></div>';
# }  !#"


######################################################
# tests
violent_crime$rand = rnorm(nrow(violent_crime), mean = 100, sd = 25)

m1 = anim_choro_multi(violent_crime, 'State', c('Crime','rand'), pals = list(Crime = 'Reds', rand = 'Blues'),
                            ncuts = list(Crime = 5, rand = 5), height = 200, width = 400, scope = 'usa', 
                            legend = T, labels = T, slider_var = 'Year', slider_step = 1)


m2 = anim_choro_multi(violent_crime, 'State', c('Crime'), pals = list(Crime = 'Reds'),
                     ncuts = list(Crime = 5), height = 200, width = 400, scope = 'usa', 
                     legend = T, labels = T, slider_var = 'Year', slider_step = 1)





vc2010 = violent_crime[violent_crime$Year == 2010,]
m3 = anim_choro_multi(vc2010, 'State', c('Crime','rand'), pals = list(Crime = 'Reds', rand = 'Blues'),
                     ncuts = list(Crime = 5, rand = 5), height = 200, width = 400, scope = 'usa', 
                     legend = T, labels = T, map_title = 'My Map')

m4 = anim_choro_multi(vc2010, 'State', c('Crime'), pals = list(Crime = 'Reds'),
                      ncuts = list(Crime = 5), height = 200, width = 400, scope = 'usa', 
                      legend = T, labels = T, map_title = 'My Map')


##### OTHER STUFF
# save to html
m4$save('/Users/mgleason/d.html')

# to include in markdown
# put:
<iframe src="file:///Users/mgleason/d.html" name="map" height=400 width=800>
  </iframe>
# in the md file`

