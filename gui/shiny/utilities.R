
get_inputs = function(){
  setwd('/Users/mgleason/NREL_Projects/github/diffusion/gui/shiny/config/inputs')
  inputs = list.files(recursive = T)
  inputs_df = as.data.frame(do.call(rbind, strsplit(inputs, '/')))
  colnames(inputs_df) = c('tab', 'input')
  rownames(inputs_df) = 1:nrow(inputs_df)
  inputs_df$tab = as.character(inputs_df$tab)
  inputs_df$input = as.character(inputs_df$input)  
  inputs_df$fpath = inputs
  inputs_df$title = toupper(gsub('_', ' ', as.vector(do.call(rbind, strsplit(inputs_df$input, '.csv')))))
  inputs_df$elid = 1:nrow(inputs_df)
  out_info = list()
  out_info[['tabs']] = unique(inputs_df$tab)
  out_info[['df']] = inputs_df
 
  return(out_info)
}
