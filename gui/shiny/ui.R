library(shiny)
library(shinyTable)
library(dplyr)
library(shinysky)
library(rhandsontable)


configuration = read.csv('/Users/mgleason/NREL_Projects/github/diffusion/gui/shiny/config/elements.csv', stringsAsFactors = F)
# set the ordering correctly
configuration = configuration[with(configuration, order(tab, position)), ]


# createTabs = function(tabnames){
# 
#   tabs = list()
#   for (i in 1:length(tabnames)){
#     tabs[[i]] = tabPanel(tabnames[i])
#   }
#   
#   tabset = do.call(tabsetPanel, tabs)
# 
#   return(tabset)  
# }

createElement = function(name, type){
  
  element = rHandsontableOutput(name)
  
  return(element)
  
}


createMainPanel = function(){
  
  # get tabs
  tabnames = unique(configuration$tab)
  
  # create elements on each tab
  tab_list = list()
  for (t in 1:length(tabnames)) {
    tabname = tabnames[[t]]

    tab_elements = dplyr::filter(as.data.frame(configuration), tab == tabname)
    # set the ordering correctly
    tab_elements = tab_elements[with(tab_elements, order(position)), ]
    
    # initialize empty list
    element_list = list()
    
    for (row in 1:nrow(tab_elements)){
      tabname = tab_elements[row, 'tab']
      position = tab_elements[row, 'position']
      name = tab_elements[row, 'name']
      type = tab_elements[row, 'type']
      nrow = tab_elements[row, 'nrow']
      ncol = tab_elements[row, 'ncol']
      src = tab_elements[row, 'src']
      element =  createElement(name, type)
      element_list[[row]] = element
    }
    
    tp = tabPanel(tabname, element_list)
    
    tab_list[[t]] = tp
    
  }
  
  tabset = do.call(tabsetPanel, tab_list)

  main_panel = mainPanel(tabset)
  
  return(main_panel)
  
}

ui <- shinyUI(pageWithSidebar(
  
  headerPanel("shinyTable with actionButton to apply changes"),
  
  sidebarPanel(
    helpText(HTML("Test"))
  ),
  
  m = createMainPanel()

)
)
