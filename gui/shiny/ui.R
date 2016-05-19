library(shiny)
library(shinyTable)
library(dplyr)
library(shinysky)
library(rhandsontable)
setwd('/Users/mgleason/NREL_Projects/github/diffusion/gui/shiny')
source('./utilities.R')

content = get_inputs()

createElement = function(name, type, title){
  
  element = rHandsontableOutput(name)
  element_with_header = tagList(h3(title), element)
  return(element_with_header)
  
}


createMainPanel = function(){
  
  # get tabs
  tabnames = content$tabs
  
  # create elements on each tab
  tab_list = list()
  for (t in 1:length(tabnames)) {
    tabname = tabnames[[t]]

    tab_elements = dplyr::filter(as.data.frame(content$df), tab == tabname)
    
    # initialize empty list
    element_list = list()
    
    for (row in 1:nrow(tab_elements)){
      id = tab_elements[row, 'elid']
      name = sprintf('el_%s', id)
      title = tab_elements[row, 'title']
      
      
      element =  createElement(name, type, title)
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
    helpText(HTML("Test")),
    actionButton("saveButton", "Save")
  ),
  
  m = createMainPanel()

)
)
