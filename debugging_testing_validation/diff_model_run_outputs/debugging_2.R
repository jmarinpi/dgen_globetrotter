library(dplyr)

one  = read.csv('/Users/mgleason/NREL_Projects/github/diffusion/runs/results_benchmark_2016_06_13/BAU/solar/outputs_solar.csv.gz')
two = read.csv('/Users/mgleason/NREL_Projects/github/diffusion/runs/results_20160613_162058/BAU/solar/outputs_solar.csv.gz')


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

# reorder columns
all_cols = sort(unique(c(names(one), names(two))))

one_cols = intersect(all_cols, names(one))  
two_cols = intersect(all_cols, names(two))

one = one[, one_cols]
two = two[, two_cols]

# save to csv
write.csv(one, '/Users/mgleason/NREL_Projects/github/diffusion/runs/debug/benchmark.csv', row.names = F)
write.csv(two, '/Users/mgleason/NREL_Projects/github/diffusion/runs/debug/oops.csv', row.names = F)

