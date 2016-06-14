library(dplyr)

tech = 'wind'
oops_dir = '/Users/mgleason/NREL_Projects/github/diffusion/runs/results_20160614_112400'

benchmark_dir = '/Users/mgleason/NREL_Projects/github/diffusion/runs/results_benchmark_2016_06_13'
benchmark_file = sprintf('%s/BAU/%s/outputs_%s.csv.gz', benchmark_dir, tech, tech)
oops_file = sprintf('%s/BAU/%s/outputs_%s.csv.gz', oops_dir, tech, tech)
one  = read.csv(benchmark_file)
two = read.csv(oops_file)

column_mapping_file = sprintf('/Users/mgleason/NREL_Projects/github/diffusion/debugging_testing_validation/diff_model_run_outputs/column_mapping_%s.csv', tech)
column_mapping = read.csv(column_mapping_file, stringsAsFactors = F)

# check sizes
nrow(one) 
nrow(two)

# filter to first year
one = filter(one, year == 2014)
two = filter(two, year == 2014)

# filter to specific county
one = filter(one, county_id == 117)
two = filter(two, county_id == 117)

# sort
one = one[with(one, order(sector, county_id, bin_id, tech)), ]
two = two[with(two, order(sector, county_id, bin_id, tech)), ]

# align the columns
for (row in 1:nrow(column_mapping)){
  b_col = column_mapping$benchmark[row]
  o_col = column_mapping$oops[row]
  names(one)[which(names(one) == b_col)] = o_col
  
}

mismatched = c()
for (col in column_mapping$oops){
    print(col) 
    match = all.equal(one[, col], two[, col], na.rm = T)
    if (match != T){
      print(n)
      mismatched = c(mismatched, col)
    }
}

cat(mismatched, sep = '\n')

# save to csv
write.csv(one[, mismatched], '/Users/mgleason/NREL_Projects/github/diffusion/runs/debug/benchmark.csv', row.names = F)
write.csv(two[, mismatched], '/Users/mgleason/NREL_Projects/github/diffusion/runs/debug/oops.csv', row.names = F)

