#df2json altered functions
#######################################
is.installed <- function(pkg) {
  is.element(pkg, installed.packages()[,1])
}

is.valid <- function(x){
  !is.na(x) & !is.nan(x) & !is.infinite(x)
}

quote_for_JSON <- function(element){
  paste0("\"", element, "\"")
}

translate_to_JSON <- function(value){
  if (!is.valid(value)){
    value <- "null"
  } else if (is.logical(value)){
    value <- if (value)  "true" else "false"
  } else if (is.character(value)){
    value <- quote_for_JSON(value)
  } else if (is.factor(value)){
    value <- quote_for_JSON(as.character(value))
  } else {
    # numeric, don't do anything
  }
  
  value
}

prepare_for_JSON <- function(data){
  data <- lapply(colnames(data), function(key){
    sapply(data[, key], function(value){
      key <- quote_for_JSON(gsub(".", "", key, fixed=TRUE))
      value <- translate_to_JSON(value)
      paste0(key, ':', value)
    })
  })
  as.data.frame(data)
}

df2json <- function(df, geogSettings){
  df <- prepare_for_JSON(df)
  objects <- apply(df, 1, function(row) {paste(row, collapse = ',')})
  geogField <- gsub(".", "", geogSettings[1], fixed=TRUE)
  objects <- gsub(geogField, 'geogUnit', objects, fixed=TRUE)
  objects <- paste0('{', objects, '}')
  objects <- paste0('[', paste(objects, collapse = ',\n'), ']')  
}

#custom functions
#######################################
#combines default JavaScript object strings and user inputs to form JavaScript Object for various parameter types (map, chart, etc.)
optionsWriter <- function(optionType, optionList, fieldList=list()) {
  optionsString<- do.call("paste", c(fieldList, optionList, sep=", \n"))
  paste0("var ", optionType,"Params = {\n\t", optionsString, "\n};\n\n")
}

#converts user input list of value variables and corresponding labels and creates Javascript object output
fListToString <- function(df, valueVarList, nclasses, classification, legendPrecision) {
  fieldString <- sapply(valueVarList, function (item, legendPrecision) {
    
    sequential <- c('Blues', 'BuGn', 'BuPu', 'GnBu', 'Greens', 'Greys', 'Oranges', 'OrRd', 'PuBu', 'PuBuGn', 'PuRd', 'Purples', 'RdPu', 'Reds', 'YlGn', 'YlGnBu', 'YlOrBr', 'YlOrRd')
    diverging <- c('BrBG', 'PiYG', 'PRGn', 'PuOr', 'RdBu', 'RdGy', 'RdYlBu', 'RdYlGn', 'Spectral')
    column <- df[[item[1]]]
    
    palette <- item[3]

    #corrects issue with Javascript rounding max number to above data class max
    max1 <- max(column)+(10^(-(legendPrecision+2)))
    min1 <- min(column)
    
    max <- paste0("maxValue: ", max1)
    min <- paste0("minValue: ", min1)
    
    item[1] <- paste0("valueVariable: '", gsub(".", "", item[1], fixed=TRUE), "'")
    item[2] <- paste0("labelName: '", item[2], "'")
    item[4] <- paste0("units: '", item[4], "'")
    
    if (!is.null(nclasses)) {
      
      if (palette %in% sequential) {
        if (nclasses > 8) {
          nclasses <- 8
          show("Sequential palettes are limited to 8 classes. Data will be displayed with 8 classes.")
        }
        else if (nclasses < 3) {
          nclasses <- 3
          show("Sequential palettes must have at least 3 classes. Data will be displayed with 3 classes.")
        }
      } else if (palette %in% diverging){
        if (nclasses > 10) {
          nclasses <- 10
          show("Diverging palettes are limited to 10 classes. Data will be displayed with 10 classes.")
        } else if (nclasses < 3) {
          nclasses <- 3
          show("Sequential palettes must have at least 3 classes. Data will be displayed with 3 classes.")
        }
      }

      intervals <- suppressWarnings(classIntervals(column, nclasses, classification))
      breaks <- intervals$brks
      fillColors = RColorBrewer::brewer.pal(nclasses+1, palette)[2:(nclasses+1)]
      
      colorList <- sapply(1:(length(breaks)-1), function (x) {
        if (x == 1) {
          paste0("{from: 0, to: ", breaks[x+1], ", color: '", fillColors[x], "'}")
        } else if (x == length(breaks)-1) {
          paste0("{from: ", breaks[x], ", to: ", max1, ", color: '", fillColors[x], "'}")
        }else{
          paste0("{from: ", breaks[x], ", to: ", breaks[x+1], ", color: '", fillColors[x], "'}")
        }
      })
      
      colorList <- paste0(colorList, collapse=", ")
      colorList <- paste0("[", colorList, "]")
      colorAxis <- paste0("colorAxis: {dataClasses: ", colorList, "}")
      
    } else {
      fillColors <- RColorBrewer::brewer.pal(3, palette)[1:3]
      colorMin <- fillColors[1]
      colorMax <- fillColors[3]
      colorAxis <- paste0("colorAxis: {minColor: '", colorMin, "', maxColor: '", colorMax, "', min: ", min1, ", max: ", max1, ", }")
      
    }
    
    item[3] <- colorAxis
    item[5] = max
    item[6] = min
    
    fieldString <- paste0(item, collapse=", ")
    fieldString <- paste("{", fieldString, "}", sep="")
  }, legendPrecision)
  
  fieldString <- paste0(fieldString, collapse=", \n\t\t")
  fieldString <- paste0("[", fieldString, "]")
  fieldString <- paste0("valueVariables: ", fieldString)
  
  fieldString
}

r_js_viz <- function (data, valueVarSettings, timeSettings, geogSettings, nclasses=5, classification='equal', mapParams='', chart1Type='line', chart1Params='', chart1Fixed=TRUE, chart2Type='column', chart2Params='', chart2Fixed=TRUE, mapGroup='Countries', map='United States of America', joinField='hc-a2', tooltipPrecision=2, legendPrecision=2, displayInBrowser=FALSE, compiledHTML=NULL, geoJSON=NULL) {
  
  columns <- c(geogSettings[1], timeSettings[1])
  #flag for whether all columns exist in data frame
  columnsCheck <- TRUE
  rowsCheck <- TRUE
  packagesCheck <- TRUE
  
  #checks to see if RColorBrewer and classInt packages are installed, shows message if not
  if (!(is.installed("RColorBrewer")&&is.installed("classInt"))) {
    # install.packages(c("classInt", "RColorBrewer"))
    show("Please install RColorBrewer and classInt packages before using")
    packagesCheck <- FALSE
  } 
  
  #checks if each column in valueVarSettings is in the data frame
  #gets column names from valueVarList input
  valueVars <- sapply(valueVarSettings, function (x) {
    x[1]
  })
  columnsToViz <- append(valueVars, c(geogSettings[1], timeSettings[1]), after=0)
  
  columnsCheckList <- lapply(columnsToViz, function(x) {
    if (!(x[1] %in% names(data))) {
      show (paste0(x[1], " is not a column of the selected data frame."))
      columnsCheck <- FALSE
      columnsCheck
    }
  })
  
  columnsCheck <- !(FALSE %in% columnsCheckList)
  
  #tests for duplicate row (more than one row for a given unit at a given time)
  if (columnsCheck&&(TRUE %in% duplicated(data[, columns]))) {
    duplicateRows <- as.list(which(duplicated(data[, columns]) %in% TRUE))
    message <- do.call("paste", duplicateRows)
    show(paste("Duplicate geographic and time data in rows:", message))
    rowsCheck <- FALSE
  } 
  
  #runs script if columns, rows, and packages all check out, otherwise prints message to console that script was aborted
  if (columnsCheck&&rowsCheck&&packagesCheck) {
    require(classInt, quietly=T)
    require(RColorBrewer)
        
    valueVarString <- fListToString(data, valueVarSettings, nclasses, classification, legendPrecision)
    timeColumnString <- paste0("{timeUnit: '", timeSettings[1], "', timeUnitLabel: '", timeSettings[2], "'}")
    geogColumnString <- paste0("{geogUnit: 'geogUnit', geogUnitLabel: '", geogSettings[2], "'}")
  
  
    mapDefaults <- list("chart: {
                      spacingTop: -10, 
                      spacingRight: 10, 
                      spacingBottom: 10, 
                      spacingLeft: 10
                      }",
                      paste0("legend: {
                         layout: 'vertical', 
                         align: 'left',
                         verticalAlign: 'bottom', 
                         floating: true, 
                         valueDecimals: ", legendPrecision, "
                      }"),
                      "tooltip: {
                        enabled: true
                      }",
                      "series: [{}]",
                      "subtitle: {text: ''}",
                      paste0("plotOptions: {map: {joinBy: ['", joinField,"', 'geogUnit']}, series: {cursor: 'pointer'}}"),
                      "credits: {enabled: false}",
                      "title: {text: null}"
                      )
  mapOptionsJSON <- optionsWriter('map', mapParams)
  
  mapDefaultsJSON <- optionsWriter('mapDefault', mapDefaults)
  
  chartDefaults <- list("title: {
                          text: null
                        }",
                        paste0("chart: {
                           type: '", chart1Type, "'
                        }"),
                        "yAxis: {
                          labels: {enabled: true},
                         title: {text: null},
                          type: 'linear'
                        }",
                        "xAxis: {
                          allowDecimals: false
                        }",
                        "tooltip: {
                          enabled: true,
                          crosshairs: true
                        }",
                        "series: [{}]",
                        "credits: {enabled: false}"
                        )
  chart2Defaults <- list("title: {
                          text: null
                         }",
                        paste0("chart: {
                           type: '",chart2Type, "'
                        }"),
                        "tooltip: {
                          enabled: true
                        }",
                        "yAxis: {
                          title: {text: null}, 
                          type: 'linear',
                          labels: {enabled: true}
                        }",
                        "xAxis: {
                          allowDecimals: false, 
                          type: 'category', 
                          labels: {enabled: false}
                        }",
                        "credits: {
                           enabled: false
                        }",
                        "legend: {
                          enabled: false
                        }",    
                        "series: [{}]",
                       "credits: {enabled: false}")
  chartOptionsJSON <- optionsWriter('chart', chart1Params)
  
  chartDefaultsJSON <- optionsWriter('chartDefault', chartDefaults)
  
  chart2OptionsJSON <- optionsWriter('chart2', chart2Params)
  
  chart2DefaultsJSON <- optionsWriter('chart2Default', chart2Defaults)
  
  globalDefaults <- list("title: {text: null}",
                         paste0("geogUnitObject: ", geogColumnString),
                         paste0("timeUnitObject: ", timeColumnString),
                         paste0("mapGroup: '", mapGroup, "'"),
                         paste0("map: '", map, "'"),
                         paste0("joinField: '", joinField, "'"),
                         paste0("chart1FixedYAxis: ", translate_to_JSON(chart1Fixed)),
                         paste0("chart2FixedYAxis: ", translate_to_JSON(chart2Fixed)),
                         paste0("dataPrecision: ", tooltipPrecision),
                         paste0("geoJSONPath: '", geoJSON, "'")
  )
  
  globalDefaultsJSON <- optionsWriter('globalDefault', globalDefaults, valueVarString)
    
  order.time <- order(data[[timeSettings[1]]])
  data <- data[order.time,]
  order.geog <- order(data[[geogSettings[1]]])
  data <- data[order.geog,]
  data <- data[, columnsToViz]
  
  baseDir <- file.path(getwd(), '../maps/r2js')
  dataJSON <- df2json(data, geogSettings)
  dataJSON <- paste("var data_raw = ", dataJSON, ";")
  JSONdata <- paste(mapOptionsJSON, mapDefaultsJSON, chartOptionsJSON, chartDefaultsJSON, chart2OptionsJSON, chart2DefaultsJSON, globalDefaultsJSON, dataJSON)
  page <- readChar(file.path(baseDir, "assets/index_template.html"), nchars=file.info(file.path(baseDir, "assets/index_template.html"))$size)
  stylesheet <- readChar(file.path(baseDir, "assets/css/main.css"), nchars=file.info(file.path(baseDir, "assets/css/main.css"))$size)
  appmodel <- readChar(file.path(baseDir, "assets/js/models/appmodel.js"), nchars=file.info(file.path(baseDir, "assets/js/models/appmodel.js"))$size)
  sliderview <- readChar(file.path(baseDir, "assets/js/views/sliderview.js"), nchars=file.info(file.path(baseDir, "assets/js/views/sliderview.js"))$size)
  mapview <- readChar(file.path(baseDir, "assets/js/views/mapview.js"), nchars=file.info(file.path(baseDir, "assets/js/views/mapview.js"))$size)
  chartview <- readChar(file.path(baseDir, "assets/js/views/chartview.js"), nchars=file.info(file.path(baseDir, "assets/js/views/chartview.js"))$size)
  selectview <- readChar(file.path(baseDir, "assets/js/views/selectview.js"), nchars=file.info(file.path(baseDir, "assets/js/views/selectview.js"))$size)
  app_edit <- readChar(file.path(baseDir, "assets/js/app_edit.js"), nchars=file.info(file.path(baseDir, "assets/js/app_edit.js"))$size)
  
  if (!is.null(geoJSON)) {
    
    if (!is.installed("RJSONIO")) {
      show("Install RJSONIO package to work with custom GeoJSON objects")
    } else {
      require(RJSONIO)
      if (isValidJSON(geoJSON)) {
        customGeom <- readChar(geoJSON, nchars=file.info(geoJSON)$size)
        customGeom <- paste0("var geojson = ", customGeom, ";")
        page <- gsub(pattern="/*GeoJSON*/", replacement=customGeom, x=page, fixed=TRUE)
      } else {
        
        jsontest <- fromJSON(geoJSON)
        geogUnit <- 
          
          invalidJSON <- paste(geoJSON, "is not a valid JSON file.")
        show(invalidJSON)
      }
    }
  }
  page <- gsub(pattern="/*JSONdata*/", replacement=JSONdata, x=page, fixed=TRUE)
  page <- gsub(pattern="/*Stylesheet*/", replacement=stylesheet, x=page, fixed=TRUE)
  page <- gsub(pattern="/*appmodel*/", replacement=appmodel, x=page, fixed=TRUE)
  page <- gsub(pattern="/*sliderview*/", replacement=sliderview, x=page, fixed=TRUE)
  page <- gsub(pattern="/*mapview*/", replacement=mapview, x=page, fixed=TRUE)
  page <- gsub(pattern="/*chartview*/", replacement=chartview, x=page, fixed=TRUE)
  page <- gsub(pattern="/*selectview*/", replacement=selectview, x=page, fixed=TRUE)
  page <- gsub(pattern="/*app_edit*/", replacement=app_edit, x=page, fixed=TRUE)

  if (!is.null(compiledHTML)) {
    cat(page, file=compiledHTML, append=FALSE)
  }

  } else {
    show("SCRIPT ABORTED")
  }
  
  tmp <- URLencode(page)
  cat('<iframe src="data:text/html;charset=utf-8,', tmp ,
    '" style="border: none; seamless:seamless; width: 1250px; height: 750px"></iframe><br/><br/>')

}



