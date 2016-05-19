library(shiny)
library(shinyTable)
library(shinysky)
library(rhandsontable)
setwd('/Users/mgleason/NREL_Projects/github/diffusion/gui/shiny')
source('./utilities.R')

content = get_inputs()

createElements = function(input, output, configuration){
  
  # create elements on each tab
  elements = list()
  
  for (row in 1:nrow(content$df)){
    local({ 
          fpath = content$df[row, 'fpath']
          elid = content$df[row, 'elid']
          name = sprintf('el_%s', elid)
          temp_df = read.csv(fpath, check.names = F, stringsAsFactors = F, row.names = 1)

          elements[[name]] = reactiveValues(data=temp_df)
          output[[name]] = renderRHandsontable({
            rhandsontable(elements[[name]]$data)                                          
          })


    }) 
  }
 
  
  return(output)
  
}



server = shinyServer(function(input, output) {
  
  output = createElements(input, output, configuration)

observe({
  # Assuming your saveButton is an actionButton, from the shiny-incubator package
  if (input$saveButton == 0)
    return()
  isolate({
#     print(names(output))
    df = hot_to_r(input$el_1)
#     print(input$el_1)
    print(df)
#     write.csv(hot_to_r(input$el_1), '/Users/mgleason/NREL_Projects/github/diffusion/gui/shiny/config/outputs/test.csv')
          })
})

}  
)