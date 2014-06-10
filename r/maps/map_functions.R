library(rMaps)
library(rCharts)
library(reshape2)
library(plyr)
library(RColorBrewer)
library(lattice)
library(classInt)

toProper <- function(s, strict = FALSE) {
  cap <- function(s) paste(toupper(substring(s, 1, 1)),
{s <- substring(s, 2); if(strict) tolower(s) else s},
sep = "", collapse = " " )
sapply(strsplit(s, split = " "), cap, USE.NAMES = !is.null(names(s)))
}


cut.format = function (x, breaks, labels = NULL, include.lowest = TRUE, right = TRUE, 
                       dig.lab = 3L, ordered_result = FALSE, big.mark = '', ...) 
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
      ch.br <- formatC(breaks, digits = dig, format = 'f', big.mark = big.mark)
      if (ok <- all(ch.br[-1L] != ch.br[-nb])) 
        break
    }
    labels <- if (ok) 
      paste0(ch.br[-nb], " - ", ch.br[-1L])
    else paste("Range", seq_len(nb - 1L), sep = "_")
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




prep_choro_data = function (formula, data, pal = "Blues", ncuts = 5, classification = 'equal',
                            slider = NULL, label_precision = 2, big.mark = ',')
{
  fml = lattice::latticeParseFormula(formula, data = data)
  if (length(unique(fml$left)) == 1){
    # if there is only one unique value, set it as the break value and fillKey, and set ncuts to 1
    breaks = formatC(unique(fml$left),big.mark = big.mark, format = 'f', digits = label_precision)
    data = transform(data,fillKey = as.factor(breaks))
    ncuts = 1
  } else {
    # otherwise, break the data up according to the specified classification
    intervals = classIntervals(fml$left,ncuts, classification)  
    # set breaks from the intervals object
    breaks = intervals$brks
    # set the fil key
    data = transform(data, fillKey = cut.format(fml$left, breaks, ordered_result = TRUE, dig.lab = label_precision, big.mark = big.mark))
    # reset ncuts to length(breaks)-1 --> this accounts for cases where there are fewer unique values than the original ncuts specified
    ncuts = length(breaks)-1
  }
  fillColors = RColorBrewer::brewer.pal(ncuts, pal)[1:ncuts]
  fills = as.list(setNames(fillColors, levels(data$fillKey)))
  if (!is.null(slider)) {
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





anim_choro_multi = function(data_frame, region_var, value_vars, pals = list(), ncuts = list(), classification = 'equal',
                            height = 400, width = 800, 
                            scope = 'usa', legend = T, labels = T, 
                            slider_var = NULL, slider_step = 2, legend_title = T, legend_titles = NULL, map_title = NULL,
                            label_precision = 2, big.mark = ',', show_data_popup = T, popup_label_precision = 2,
                            horizontal_legend = F, slider_width = 300){
  
  data = list()
  fills = list()
  for (value_var in value_vars){
    formula = as.formula(paste(value_var,region_var, sep = '~'))
    pal = pals[[value_var]]
    ncut = ncuts[[value_var]]
    var_prep = prep_choro_data(formula, data_frame, pal = pal, ncuts = ncut, classification = classification, slider = slider_var, label_precision = label_precision, big.mark = big.mark)
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
  d$set(horizontal_legend = horizontal_legend)
  # critical to include jshead and body attrs
  d$addAssets(jshead = "http://cdnjs.cloudflare.com/ajax/libs/angular.js/1.2.1/angular.min.js")
  d$set(bodyattrs = "ng-app ng-controller='rChartsCtrl'")
  
  # assign data to the datamap
  d$params$newData = data
  if (!is.null(slider_var)){
      # set to the first timestep of the first variable
      d$params$data = d$params$newData[[1]][[1]]
  } else {
      # set to the first variable's data
      d$params$data = d$params$newData[[1]]            
  }

  # assign fills to the datamap
  d$params$allFills = fills
  # set to the first set of fills (corresponding to the first dataset)
  d$params$fills = d$params$allFills[[1]]
  
  # set up popups
  if (show_data_popup == T){
    # create number formatter
    number_formatter = sprintf("var numfmt = d3.format(',.%sf')",popup_label_precision)
    # create a list to store different popup scripts by value var
    popup_scripts = list()
    for (value_var in value_vars){
      popup_scripts[[value_var]] = sprintf("#! function(geography, data) { 
                   return '<div class=hoverinfo><strong>' + data['%s'] + 
                   '</br>%s: ' + numfmt(data['%s']) + '</strong></div>';
                   }  !#", region_var, ifelse(is.null(legend_titles),value_var,legend_titles[[value_var]]), value_var)
    }
    # if there is only one value variable, set the popupTemplate
    if (length(value_vars) == 1){
      d$params$geographyConfig['popupTemplate'] = popup_scripts[[1]]
      popup_function = ""
    } else {
      # otherwise, store the scripts in the chart params
      d$params$popup_scripts = popup_scripts
      # and write a function to update the popupTemplate based on the selected var
      popup_function = "chartParams.geographyConfig.popupTemplate = chartParams.popup_scripts[newSelection];"
    }  
  } else {
    popup_function = ""
    number_formatter = ""
  }
  
  if (!is.null(slider_var)){
    slider_min = min(data_frame[,slider_var], na.rm = T)
    slider_max = max(data_frame[,slider_var], na.rm = T)
    
    slider_div = sprintf("<label class='label' for=slider>%s</label>
                                <input id='slider' type='range' min=%s max=%s step=%s ng-model='time' style='width:%spx;' oninput='outputUpdate(value)' onchange='outputUpdate(value)'>
                                <output  class='label' for=slider id=current_slide_val>%s</output>
                                <br></br>
                         <script>
                               function outputUpdate(vol) {
                               document.querySelector('#current_slide_val').value = vol;
                               }
                         </script>", slider_var, slider_min, slider_max, slider_step, slider_width, slider_min)
    slider_function = sprintf("$scope.time = %s;
                                $scope.$watch('time', function(newTime){
                                  chartParams.data = chartParams.selectedData[newTime];
                                  map{{chartId}}.updateChoropleth(chartParams.data);
                                });", slider_min)
    select_update = 'chartParams.data = chartParams.selectedData[$scope.time];
                     map{{chartId}}.updateChoropleth(chartParams.data);'
    
  } else {
    select_update = 'chartParams.data = chartParams.selectedData;
                     map{{chartId}}.updateChoropleth(chartParams.data);'
    slider_div = ''
    slider_function = ''
  }
  
  if (length(value_vars) > 1){
    if (!is.null(legend_titles)){
      selections = '['
      for (var_name in names(legend_titles)){
        selections = sprintf("%s{'id':'%s','label':'%s'},",selections, var_name, legend_titles[[var_name]])
      }
      selections = sprintf('selection.id as selection.label for selection in %s]',substr(selections,1,nchar(selections)-1))
    } else {
      selections = sprintf('selection for selection in [%s]',gsub('[ ]','',paste("'",names(data),"'",collapse = ',')))
    }
    default_selection = names(data)[1]
    select_div = sprintf("<label class='label' for=selector>Variable</label>
                          <select id='selector' ng-model='selection' class='form-control'
                                     ng-options=\"%s\">
                                     {{ selection }}
                                     </select>
                         <br></br>",selections)
    if (legend_title == T){
      if (!is.null(legend_titles)){
        d$params$legend_titles = legend_titles
        legend_options = "{legendTitle: chartParams.legend_titles[newSelection]}" 
      } else {
        legend_options = "{legendTitle: newSelection}"
      }
    } else {
      legend_options = ''
    }
    
    if (legend == T){
      if (horizontal_legend == T){
        legend_function = sprintf("map{{chartId}}.legend(%s);", legend_options) 
      } else {
        legend_function = sprintf("map{{chartId}}.addVerticalLegend(%s);", legend_options) 
      }
    } else {
      legend_function = ''
    }
    
    select_function = sprintf("$scope.selection = '%s';
                              $scope.$watch('selection', function(newSelection, document){
                                chartParams.selectedData = chartParams.newData[newSelection];
                                chartParams.fills = chartParams.allFills[newSelection];
                                %s                                
                                removeElementsByClass('datamaps-legend');
                                %s
                                %s
                              })", default_selection, select_update, legend_function, popup_function)
  } else {
    select_div = ''
    select_function = ''
    # warning: if something is going wrong, look here
#     d$params$selectedData = d$params$newData[[1]]
    if (legend_title == T){
      if (!is.null(legend_titles)){
        d$params$legendOptions = list(legendTitle = legend_titles[[1]])
      } else {
        d$params$legendOptions = list(legendTitle = value_vars[1]) 
      }
      
    }
  }
  # and here..
  d$params$selectedData = d$params$newData[[1]]
     
if (!is.null(slider_var) | length(value_vars) > 1){
  chartDiv = sprintf("<div class='container ng-scope' ng-app ng-controller = 'rChartsCtrl'>
                     %s
                     %s
                    <div id='{{chartId}}' class='rChart datamaps'></div>  
                    <script>    
                       %s
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
                       </script>", select_div, slider_div, number_formatter, slider_function, select_function)
  d$setTemplate(chartDiv = chartDiv)
  
  } 
  
  return(d)
}





