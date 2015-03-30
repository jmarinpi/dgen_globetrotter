packages = c('ggthemes',
             'httr',
             'devtools',
             'classInt',
             'data.table',
             'rjson',
             'dplyr',
             'devtools',
             'rCharts',
             'rMaps',
             'RPostgreSQL',
             'maps',
             'reshape2',
             'RColorBrewer',
             'lattice',
             'xtable',
             'jsonlite',
             'scales',
             'knitr'
             )

error = F
for (package in packages){
  installed = suppressMessages(require(package, quietly = T, character.only = T, warn.conflicts = F))
  if (!installed){
    print(sprintf('Error: %s is not installed.', package))
    error = T
  }
}

if (!error){
  'Success. All R dependencies are loaded.'
} else {
  print('Failure. One or more R dependncies are missing.')
}