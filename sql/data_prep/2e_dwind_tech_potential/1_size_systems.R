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


# REVISIONS TO MAKE:
# edit to maximize capacity factor or generation?
# add option for minimum parcel size(Not essential!)

# THEN SUMMARIZE INTO A SHORT POWER POINT

################################################################################################

# INPUT PARAMETERS
blade_height_setback_factor = 1.1 # based on literature review
required_parcel_size_cap_acres = Inf # this is based on input from Eric
canopy_clearance_rotor_factor = 1
canopy_clearance_static_adder_m = 12 # this is based on input from Robert
canopy_pct_requiring_clearance = 0.10 # feedback from eric and robert 

# INPUT DATASETS
power_curves_all_lkup = read.csv('/Users/mgleason/NREL_Projects/Projects/local_data/dwind_misc/technical_potential_analysis/power_curve_lkup.csv')
turbine_size_to_power_curve_lkup = read.csv('/Users/mgleason/NREL_Projects/Projects/local_data/dwind_misc/technical_potential_analysis/turbine_size_to_power_curve_name_lkup.csv')
turbine_size_to_hub_height_lkup = read.csv('/Users/mgleason/NREL_Projects/Projects/local_data/dwind_misc/technical_potential_analysis/turbine_size_to_hubheight_lkup.csv')
turbine_size_to_rotor_radius_lkup = read.csv('/Users/mgleason/NREL_Projects/Projects/local_data/dwind_misc/technical_potential_analysis/turbine_size_to_rotor_radius_lkup.csv')

# merge these to a single df
turbine_sizes = merge(turbine_size_to_power_curve_lkup, power_curves_all_lkup, by = c('size_class', 'perf_improvement_factor')) %>%
                merge(turbine_size_to_rotor_radius_lkup, by = c('turbine_size_kw')) %>%
                merge(turbine_size_to_hub_height_lkup, by = c('turbine_size_kw'))

################################################################################################


################################################################################################
# CONNECT TO PG
drv <- dbDriver("PostgreSQL")
# connect to postgres
con <- dbConnect(drv, host="gispgdb.nrel.gov", dbname="dav-gis", user="mgleason", password="mgleason")

sql = "SET ROLE 'diffusion-writers';"
dbSendQuery(con, sql)

################################################################################################

################################################################################################
# EXTRACT THE BLOCK DATA FROM PG
# *** NOTE: THIS WILL ONLY RUN FOR VERMONT CURRENTLY ***
sql = "
        select a.pgid,
              a.state_abbr, a.state_fips, a.county_fips,
              a.aland_sqm/1000/1000 as aland_sqkm,
              --a.the_poly_96703, a.the_point_96703,
              c.acres_per_bldg,
              d.canopy_pct,
              e.canopy_ht_m,
              f.bldg_count_all,
              g.iiijjjicf_id
        FROM  diffusion_blocks.block_geoms a
        INNER JOIN diffusion_blocks.blocks_with_buildings b
        ON a.pgid = b.pgid
        LEFT JOIN diffusion_blocks.block_parcel_size c
        ON a.pgid = c.pgid
        LEFT JOIN diffusion_blocks.block_canopy_cover d
        on a.pgid = d.pgid
        LEFT JOIN diffusion_blocks.block_canopy_height e
        on a.pgid = e.pgid
        LEFT JOIN diffusion_blocks.block_bldg_counts f
        on a.pgid = f.pgid
        LEFT JOIN diffusion_blocks.block_resource_id_wind g
        ON a.pgid = g.pgid
        where a.state_abbr = 'VT';
"

blocks = dbGetQuery(con, sql)

# determine the minimum blade height (in meters above ground) required for canopy clearance
blocks$min_allowable_blade_height_m = ifelse(
              blocks$canopy_pct >= canopy_pct_requiring_clearance * 100,
                   blocks$canopy_ht_m + canopy_clearance_static_adder_m,
                   0
              )
# determine the maximum blade height allowed (in meters above ground) given parcel size
blocks$max_allowable_blade_height_m = ifelse(
              blocks$acres_per_bldg <= required_parcel_size_cap_acres,
              sqrt(blocks$acres_per_bldg * 4046.86)/(2 * blade_height_setback_factor),
              Inf
              )

# calculate the effective minimum and maximum blade heights for each turbine size
# accounting for the required canopy_clearance_rotor_factor
turbine_sizes$effective_min_blade_height_m = turbine_sizes$hub_height_m - turbine_sizes$rotor_radius_m * canopy_clearance_rotor_factor
turbine_sizes$effective_max_blade_height_m = turbine_sizes$hub_height_m + turbine_sizes$rotor_radius_m

# merge the turbine_sizes and block datasets together (cross join)
block_turbine_size_options = merge(blocks, turbine_sizes, all = T) %>%
    filter(min_allowable_blade_height_m <= effective_min_blade_height_m) %>%
    filter(max_allowable_blade_height_m >= effective_max_blade_height_m)


# select the optimal turbine for each block
block_turbine_size_selected = group_by(block_turbine_size_options, pgid) %>%
    mutate(height_m_score = min_rank(hub_height_m) * 1000) %>%
    mutate(turbine_size_kw_score = min_rank(turbine_size_kw)) %>%
    mutate(combined_score = height_m_score + turbine_size_kw_score ) %>%
    filter(min_rank(desc(combined_score)) == 1) %>%
    as.data.frame()
  
# determine how total capacity of systems each block can support
 # ( = # of buildings x system size in kw)
block_turbine_size_selected$total_capacity_kw = block_turbine_size_selected$turbine_size_kw * block_turbine_size_selected$bldg_count_all

# what does this sum to?
sum(block_turbine_size_selected$total_capacity_kw)/1000/1000

# push the results back to postgres
dbWriteTable(con, c('diffusion_data_wind', 'block_turbine_size_selected'), block_turbine_size_selected, row.names = F, overwrite = T)
