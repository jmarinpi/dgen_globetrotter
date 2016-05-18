library(shiny)
library(shinyTable)

tabnames = c('cost', 'performance', 'finances')

createTabs = function(tabnames){
  tabset = tabsetPanel()
  for (i in 1:length(tabnames)){
    tabset$children[[i]] = tabPanel(tabnames[i])
  }
  
  return(tabset)  
}

createElements = function(tabset, tabname, position, name, type){
  
  tabset$children[[tabname]]$children[[position]] = htable(name)
  
  return(tabset)
  
}

createMainPanel = function(){
  m = mainPanel()
  
  for (i in seq(1,3)){
    tbl_id = sprintf('tbl%s', i)
    #editable table
    m$children[[i]] =  htable(tbl_id)
  }
  
  return(m)
}


ui <- shinyUI(pageWithSidebar(
  
  headerPanel("shinyTable with actionButton to apply changes"),
  
  sidebarPanel(
    helpText(HTML("A simple editable matrix with a functioning update button. 
                   Using actionButton not submitButton. 
                   Make changes to the upper table, press the button and they will appear in the lower. 
                  <p>Created using <a href = \"http://github.com/trestletech/shinyTable\">shinyTable</a>."))
  ),
  
  createMainPanel()
  # Show the simple table
#   m = mainPanel(
#     htable('tbl1'), 
#     htable('tbl2'),
#     htable('tbl3')
#   )
)
)
