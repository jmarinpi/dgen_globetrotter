in_csv = '/Users/mgleason/NREL_Projects/github/diffusion/sql/data_prep/2a_prep_wind_resource_data/2_update_wind_generation_data/1_create_powercurve_csvs/powercurve_minor_update_2016_01_12.csv'
out_folder = '/Users/mgleason/NREL_Projects/github/windpy/windspeed2power/powercurves'
cur_date = format(Sys.time(), '%Y_%m_%d')
precision_digits = 3

pcm = read.csv(in_csv, check.names = F)

for (col in 2:ncol(pcm)){
  turbine_name = names(pcm)[col]
  pc = pcm[, c(1, col)]
  names(pc) = c('windspeed_ms', 'generation_kw')
  pc[, 'generation_kw'] = round(pc[, 'generation_kw'], 3)
  out_file = sprintf('dwind_turbine_%s_%s.csv', turbine_name, cur_date)
  out_file_path = file.path(out_folder, out_file)
  write.csv(pc, out_file_path, row.names = F)
}