library(shiny)
library(shinyTable)


configuration = read.csv('/Users/mgleason/NREL_Projects/github/diffusion/gui/shiny/config/elements.csv', stringsAsFactors = F)
# set the ordering correctly
configuration = configuration[with(configuration, order(tab, position)), ]

createElements = function(output, configuration){
  
  # create elements on each tab
  for (row in 1:nrow(configuration)){
    
    tabname = configuration[row, 'tab']
    position = configuration[row, 'position']
    name = configuration[row, 'name']
    type = configuration[row, 'type']
    nrow = configuration[row, 'nrow']
    ncol = configuration[row, 'ncol']
    src = configuration[row, 'src']
    
    tbl = matrix(0, nrow = nrow, ncol = ncol)
    output[[name]] = renderHtable(tbl)
    
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