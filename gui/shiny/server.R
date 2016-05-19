library(shiny)
library(shinyTable)
library(shinysky)

configuration = read.csv('/Users/mgleason/NREL_Projects/github/diffusion/gui/shiny/config/elements.csv', stringsAsFactors = F)
# set the ordering correctly
configuration = configuration[with(configuration, order(tab, position)), ]

createElements = function(output, configuration){
  
  # create elements on each tab
  x = read.csv('/Users/mgleason/NREL_Projects/github/diffusion/gui/shiny/config/costs.csv', check.names = F, row.names = 1)
  elements = list()

  for (row in 1:nrow(configuration)){
    local({ 
    tabname = configuration[row, 'tab']
    position = configuration[row, 'position']
    name = configuration[row, 'name']
    type = configuration[row, 'type']
    nrow = configuration[row, 'nrow']
    ncol = configuration[row, 'ncol']
    src = configuration[row, 'src']

          output[[name]] = renderHtable(x)
    }) 
  }
 
  
  return(output)
  
}



server = shinyServer(function(input, output) {
  
  output = createElements(output, configuration)
#   output$tbl1 =  renderHtable(tbl)
#   output$tbl2 =  renderHtable(tbl)
#   output$tbl3 =  renderHtable(tbl)
}  
)