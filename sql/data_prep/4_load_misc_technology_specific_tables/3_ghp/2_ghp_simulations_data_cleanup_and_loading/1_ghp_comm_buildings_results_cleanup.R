library(xlsx)
library(reshape)

setwd('/Users/mgleason/NREL_Projects/github/diffusion/sql/data_prep/4_load_misc_technology_specific_tables/3_ghp/2_ghp_simulations_data_cleanup_and_loading')

in_xlsx = 'source/Commercial GHP Simulation Results (7-15-2016).xlsx'
wb = loadWorkbook(in_xlsx)
sheets = getSheets(wb)
sheet_names = names(sheets)

sheets_to_skip = c('GHX Comparison', 'Comparison Charts')
ranges = read.csv('helper/ghp_range_lkup.csv', stringsAsFactors = F)

sheet_dfs = list()
for (sheet_name in sheet_names){
  if (sheet_name %in% c(sheets_to_skip)){
    # do nothing
  } else {
    sheet = sheets[sheet_name]
    df_names = c()
    for (row in 1:nrow(ranges)){
      startColumn = ranges[row, 'startColumn']
      endColumn = ranges[row, 'endColumn']
      startRow = ranges[row, 'startRow']
      endRow = ranges[row, 'endRow']
      header = ranges[row, 'header']
      tc_val = ranges[row, 'tc_val']
      df_prefix = ranges[row, 'df_prefix']
      col_prefix = ranges[row, 'col_prefix']
      if (is.na(col_prefix)){
        col_prefix = ''
      }
      
      df = readColumns(sheet[[1]], startColumn, endColumn, startRow, endRow, header = header, stringsAsFactors = F)
      # column name cleanup
      names(df) = tolower(gsub('[\\.]+', '_', names(df)))
      names(df) = gsub('building_tye', 'building_type', names(df))
      names(df) = gsub('_mc|_lc|_hc', '', names(df))
      names(df) = gsub('_$', '', names(df))
      names(df) = sprintf('%s%s', col_prefix, names(df))
      names(df) = gsub('savings_pct_building_type', 'building_type', names(df))
      names(df) = gsub('savings_abs_building_type', 'building_type', names(df))
      
      
      df[, 'tc_val'] = tc_val
      df_name = sprintf('%s_%s', df_prefix, tc_val)
      assign(df_name, df)
      df_names = c(df_names, df_name)
    }
    # combine data into a single csv
    # merge tc vals
    gtc = rbind(ground_thermal_conductivity_btu_per_hftF_tc_1, 
                ground_thermal_conductivity_btu_per_hftF_tc_2,
                ground_thermal_conductivity_btu_per_hftF_tc_3)
    names(gtc)[1] = 'gtc_btu_per_hftF'
    for (df_name in df_names){
      if (!(grepl('ground_thermal', df_name))){
        df = get(df_name)
        df = merge(df, gtc, by = 'tc_val')
        assign(df_name, df)
      }
    }
    # now merge across tc_vals
    prefixes = c('baseline', 'ghp', 'savings_abs', 'savings_pct', 'ghx_sizing')
    merged_dfs = list()
    for (tc_val in gtc$tc_val){
      dfs = sprintf('%s_%s', prefixes, tc_val)
      df_list = list()
      for (df_name in dfs){
        df_list[[df_name]] = get(df_name)
      }
      # merge
      merged_df = Reduce(function(...) merge(..., all=T), df_list)
      merged_dfs[[tc_val]] = merged_df
    }
    # rbind the merged dfs
    complete_sheet_df = Reduce(function(...) rbind(...), merged_dfs)
    # city and climate_zone
    city = strsplit(names(sheet), '-')[[1]][1]
    climate_zone = strsplit(names(sheet), '-')[[1]][2]
    complete_sheet_df[, 'city'] = city
    complete_sheet_df[, 'climate_zone'] = climate_zone
    sheet_dfs[[names(sheet)]] = complete_sheet_df
  }
}

# merge everything into one
complete_df = Reduce(function(...) rbind(...), sheet_dfs)
# drop rows where multiple vlaues are NA
complete_df_no_nas = complete_df[rowSums(is.na(complete_df)) == 0,]
# drop the tc_val column
out_cols = !grepl('tc_val', names(complete_df_no_nas))
complete_df_no_nas = complete_df_no_nas[, out_cols]
# replace values of NA with NA (applies only to energy savings pct)
complete_df_no_nas[complete_df_no_nas == 'NA'] = NA
# write to csv
write.csv(complete_df_no_nas, 'output/ghp_results.csv', row.names = F, na = '')



