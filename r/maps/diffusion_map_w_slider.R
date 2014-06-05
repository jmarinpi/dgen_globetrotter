library(rMaps)
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
# d$addAssets(js = '                                   <script>
#                                     function removeElement(className) {
#                                     
#                                       var divs = document.getElementsByClass(className);
#                                       i = divs.length;
#                                       while (i){
#                                        i -= 1;
#                                        divs[i].parentNode.removeChild(divs[i]);
#                                       }
#                                     }
#                                    </script>')

###
###










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