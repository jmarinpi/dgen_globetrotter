library(shiny)
library(shinyTable)

server = shinyServer(function(input, output) {
  
  tbl <- matrix(0, nrow=3, ncol=3)
  for (i in seq(1,3)){
      tbl_id = sprintf('tbl%s', i)
      output[[tbl_id]] = renderHtable(tbl)
      }
#   output$tbl1 =  renderHtable(tbl)
#   output$tbl2 =  renderHtable(tbl)
#   output$tbl3 =  renderHtable(tbl)
}  
)