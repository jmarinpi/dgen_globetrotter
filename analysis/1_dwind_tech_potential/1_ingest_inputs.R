library(dplyr)
library(reshape2)
library(ggplot2)
library(RPostgreSQL)
library(gstat)
library(sp)
library(ggthemes)
library(grid)
library(scales)
library(fitdistrplus)

################################################################################################
# CONNECT TO PG
drv <- dbDriver("PostgreSQL")
# connect to postgres
con <- dbConnect(drv, host="gispgdb.nrel.gov", dbname="dav-gis", user="mgleason", password="mgleason")

sql = "SET ROLE 'diffusion-writers';"
dbSendQuery(con, sql)

################################################################################################

# ASSUMPTIONS:
# this is tech potential constrained by proximity to demand, 
# but without any consideration of actual demand constraints.
# (ie.., turbines must be sited in areas near buildings, but they will not be sized
# with any regards to the demand of those buildings.)

# assume no required setback from buildings on the same property
# assume that average parcel charactersitics (size, canopy) on each block is representative of the entire block
# assume no constraints due to inter-turbine (i.e., wake) effects
# each parcel only gets one turbine, and that turbine is the maximum permissible turbine
  # from the perspective of capacity factor




# THEN SUMMARIZE INTO A SHORT POWER POINT

################################################################################################
# GLOBAL SETTINGS
path = '/Users/mgleason/NREL_Projects/github/diffusion/sql/data_prep/2e_dwind_tech_potential'
setwd(path)

# INPUT PARAMETERS
blade_height_setback_factor = 1.1 # based on literature review
required_parcel_size_cap_acres = Inf # this is based on input from Eric
canopy_clearance_rotor_factor = 1
canopy_clearance_static_adder_m = 12 # this is based on input from Robert
canopy_pct_requiring_clearance = 0.10 # feedback from eric and robert 

# INPUT DATASETS
power_curves_all_lkup = read.csv('input_files/power_curve_lkup.csv')
turbine_size_to_power_curve_lkup = read.csv('input_files/turbine_size_to_power_curve_name_lkup.csv')
turbine_size_to_hub_height_lkup = read.csv('input_files/turbine_size_to_hubheight_lkup.csv')
turbine_size_to_rotor_radius_lkup = read.csv('input_files/turbine_size_to_rotor_radius_lkup.csv')

################################################################################################
# MERGE INPUT DATASETS TO A SINGLE DF AND WRITE TO PG
turbine_sizes = merge(turbine_size_to_power_curve_lkup, power_curves_all_lkup, by = c('size_class', 'perf_improvement_factor')) %>%
                merge(turbine_size_to_rotor_radius_lkup, by = c('turbine_size_kw')) %>%
                merge(turbine_size_to_hub_height_lkup, by = c('turbine_size_kw'))

# write to postgres
dbWriteTable(con, c('diffusion_data_wind', 'tech_pot_turbine_sizes'), turbine_sizes, row.names = F, overwrite = T)
################################################################################################

################################################################################################
# MERGE INPUT VARIABLES AND WRITE TO POSTGRES
input_settings = data.frame(blade_height_setback_factor = blade_height_setback_factor,
                      required_parcel_size_cap_acres = required_parcel_size_cap_acres,
                      canopy_clearance_rotor_factor = canopy_clearance_rotor_factor,
                      canopy_clearance_static_adder_m = canopy_clearance_static_adder_m,
                      canopy_pct_requiring_clearance = canopy_pct_requiring_clearance
  )

# write to postgres
dbWriteTable(con, c('diffusion_data_wind', 'tech_pot_settings'), input_settings, row.names = F, overwrite = T)
################################################################################################

