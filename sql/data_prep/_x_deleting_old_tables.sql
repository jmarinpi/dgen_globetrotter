ALTER SCHEMA diffusion_solar_config RENAME to diffusion_trash_solar_config;
ALTER SCHEMA diffusion_wind_config RENAME to diffusion_trash_wind_config;

set role 'server-superusers';
SELECT add_schema('diffusion_trash_solar', 'diffusion');
SELECT add_schema('diffusion_trash_wind', 'diffusion');


-- solar inputs from excel
ALTER TABLE diffusion_solar.scenario_options
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.solar_cost_projections
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.cost_multipliers
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.learning_rates
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.solar_performance_improvements
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.manual_carbon_intensities
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.manual_carbon_intensities
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.system_sizing_factors
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.cost_projections_to_model
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.nem_scenario
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.nem_scenario_avoided_costs
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.user_defined_electric_rates
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.rate_type_weights
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.leasing_availability
set schema diffusion_trash_solar;

ALTER TABLE diffusion_solar.user_defined_max_market_share 
SET SCHEMA diffusion_trash_solar;

ALTER TABLE diffusion_solar.manual_incentives 
SET SCHEMA diffusion_trash_solar;

ALTER TABLE diffusion_solar.depreciation_schedule 
SET SCHEMA diffusion_trash_solar;

ALTER TABLE diffusion_solar.financial_parameters 
SET SCHEMA diffusion_trash_solar;

ALTER TABLE diffusion_solar.market_projections 
SET SCHEMA diffusion_trash_solar;

-- wind excel inputs
ALTER TABLE diffusion_wind.scenario_options SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.wind_cost_projections SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.wind_performance_improvements SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.wind_generation_derate_factors SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.system_sizing_factors SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.min_acres_per_hu_lkup SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.max_hi_dev_pct_lkup SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.required_canopy_clearance_lkup SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.nem_scenario SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.nem_scenario_avoided_costs SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.user_defined_electric_rates SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.rate_type_weights SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.leasing_availability SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.user_defined_max_market_share SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.manual_incentives SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.depreciation_schedule SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.financial_parameters SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.market_projections SET SCHEMA diffusion_trash_wind;

-- views
ALTER TABLE diffusion_wind.counties_to_model SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_solar.counties_to_model SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_wind.carbon_intensities_to_model SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_solar.carbon_intensities_to_model SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_wind.point_microdata_ind_us_joined SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.point_microdata_com_us_joined SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.point_microdata_res_us_joined SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_solar.point_microdata_ind_us_joined SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.point_microdata_com_us_joined SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.point_microdata_res_us_joined SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_wind.sectors_to_model SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_solar.sectors_to_model SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_wind.max_market_curves_to_model SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_solar.max_market_curves_to_model SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_wind.rate_escalations_to_model SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_solar.rate_escalations_to_model SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_wind.turbine_costs_per_size_and_year SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.all_rate_jsons SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_solar.all_rate_jsons SET SCHEMA diffusion_trash_solar;

-- old outputs tables
ALTER TABLE diffusion_wind.outputs_res SET schema diffusion_trash_wind;
ALTER TABLE diffusion_wind.outputs_com SET schema diffusion_trash_wind;
ALTER TABLE diffusion_wind.outputs_ind SET schema diffusion_trash_wind;
ALTER TABLE diffusion_wind.outputs_all SET schema diffusion_trash_wind;

ALTER TABLE diffusion_solar.outputs_res SET schema diffusion_trash_solar;
ALTER TABLE diffusion_solar.outputs_com SET schema diffusion_trash_solar;
ALTER TABLE diffusion_solar.outputs_ind SET schema diffusion_trash_solar;
ALTER TABLE diffusion_solar.outputs_all SET schema diffusion_trash_solar;
ALTER TABLE diffusion_solar.reeds_outputs SET SCHEMA diffusion_trash_solar;

-- old wind resource data
ALTER TABLE diffusion_wind.archive_wind_resource_annual SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.archive_wind_resource_current_large_turbine SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.archive_wind_resource_current_mid_turbine SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.archive_wind_resource_current_small_commercial_turbine_2014_11 SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.archive_wind_resource_current_small_turbine SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.archive_wind_resource_far_future_small_turbine_2014_11 SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.archive_wind_resource_future_mid_and_large_turbine SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.archive_wind_resource_future_small_turbine SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.archive_wind_resource_nearfuture_mid_and_large_turbine SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.archive_wind_resource_nearfuture_small_turbine SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.turbines_old SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.normalized_wind_power_curves_old SET SCHEMA diffusion_trash_wind;
-- drop these two old hourly ones -- we have old backups if we need elsewhere
DROP TABLE diffusion_wind.archive_wind_resource_hourly_current_small_comm_turbine_2014_11;
DROP TABLE diffusion_wind.archive_wind_resource_hourly_far_future_small_turbine_2014_11;

-- old fixed seed stuff
ALTER TABLE diffusion_wind.prior_seeds_com SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.prior_seeds_ind SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.prior_seeds_res SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.county_load_bins_random_lookup_res_pg SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.random_lookup_com_0p1 SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.random_lookup_ind_0p1 SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.random_lookup_res_0p1 SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.random_lookup_res_0p2 SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.random_lookup_res_1 SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_solar.sample_sets_res SET schema diffusion_trash_solar;

-- model intermediate outputs
ALTER TABLE diffusion_solar.temporal_factors SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.pt_com_best_option_each_year SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.pt_com_elec_costs SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.pt_com_initial_market_shares SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.pt_res_best_option_each_year SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.pt_res_elec_costs SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.pt_res_initial_market_shares SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.pt_ind_best_option_each_year SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.pt_ind_elec_costs SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.pt_ind_initial_market_shares SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.utilityrate3_results SET SCHEMA diffusion_trash_solar;
ALTER TABLE diffusion_solar.unique_rate_gen_load_combinations SET SCHEMA diffusion_trash_solar;

ALTER TABLE diffusion_wind.temporal_factors SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.temporal_factors_market SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.temporal_factors_technology SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_com_best_option_each_year SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_com_elec_costs SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_com_initial_market_shares SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_res_best_option_each_year SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_res_elec_costs SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_res_initial_market_shares SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_ind_best_option_each_year SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_ind_elec_costs SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_ind_initial_market_shares SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.utilityrate3_results SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.unique_rate_gen_load_combinations SET SCHEMA diffusion_trash_wind;

-- miscellanous
ALTER TABLE diffusion_solar.max_market_curves_to_model2 SET SCHEMA diffusion_trash_solar;

-- old incentives
ALTER TABLE diffusion_wind.incentives_raw SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_com_incentives SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_res_incentives SET SCHEMA diffusion_trash_wind;
ALTER TABLE diffusion_wind.pt_ind_incentives SET SCHEMA diffusion_trash_wind;

-------------------------------------------------------------------------------
-- stuff to archive
-- old starting capacities
ALTER TABLE diffusion_wind.starting_capacities_mw_2014_us SET SCHEMA diffusion_wind_data;
ALTER TABLE diffusion_solar.starting_capacities_mw_2014_us SET SCHEMA diffusion_solar_data;

-- incentives
-- the following 3 must stay!!!
-- diffusion_[tech].dsire_incentives_simplified_lkup_res
-- diffusion_[tech].dsire_incentives_simplified_lkup_com
-- diffusion_[tech].dsire_incentives_simplified_lkup_ind
ALTER TABLE diffusion_wind.dsire_incentives_lookup_res SET SCHEMA diffusion_wind_data;
ALTER TABLE diffusion_wind.dsire_incentives_lookup_com SET SCHEMA diffusion_wind_data;
ALTER TABLE diffusion_wind.dsire_incentives_lookup_ind SET SCHEMA diffusion_wind_data;

ALTER TABLE diffusion_solar.dsire_incentives_lookup_res SET SCHEMA diffusion_solar_data;
ALTER TABLE diffusion_solar.dsire_incentives_lookup_com SET SCHEMA diffusion_solar_data;
ALTER TABLE diffusion_solar.dsire_incentives_lookup_ind SET SCHEMA diffusion_solar_data;

-- rooftop orientation related data
ALTER TABLE diffusion_solar.solar_ds_rooftop_availability SET SCHEMA diffusion_solar_data;
ALTER TABLE diffusion_solar.solar_ds_rooftop_orientations SET SCHEMA diffusion_solar_data;
ALTER TABLE diffusion_solar.roof_material_to_roof_style_cbecs SET SCHEMA diffusion_solar_data;
ALTER TABLE diffusion_solar.roof_material_to_roof_style_recs SET SCHEMA diffusion_solar_data;

-- old excess generation stuff
ALTER TABLE diffusion_solar.solar_re_9809_tzone_lookup SET SCHEMA diffusion_solar_data;
ALTER TABLE diffusion_wind.ij_tzone_lookup SET SCHEMA diffusion_wind_data;

-------------------------------------------------------------------------------
-- done cleaning wind and solar schemas

-- back up the "trash" schemas to disk:
-- pg_dump -h localhost -U mgleason -O -n diffusion_trash_wind_config -n diffusion_trash_wind -n diffusion_trash_solar_config -n diffusion_trash_solar -v dav-gis | gzip -6 > diffusion_wind_and_solar_trash_schemas_archive_20150717.gz
-- (in /srv2/mgleason_backups/diffusion_database)

-- drop the "trash" schemas
DROP SCHEMA diffusion_trash_solar_config CASCADE;
DROP SCHEMA diffusion_trash_wind_config CASCADE;
DROP SCHEMA diffusion_trash_solar CASCADE;
DROP SCHEMA diffusion_trash_wind CASCADE;

-- tested the model one more time for both wind and solar --- all ran fine
-------------------- tested ok to here ----------------------------------------

-- next: clean up diffusion_shared
set role 'server-superusers';
SELECT add_schema('diffusion_trash_shared', 'diffusion');
SELECT add_schema('diffusion_shared_data', 'diffusion');
set role mgleason;

-- trash
-- old binned load data
ALTER TABLE diffusion_shared.binned_annual_load_kwh_100_bins SET SCHEMA diffusion_trash_shared;
ALTER TABLE diffusion_shared.binned_annual_load_kwh_10_bins SET SCHEMA diffusion_trash_shared;
ALTER TABLE diffusion_shared.binned_annual_load_kwh_500_bins SET SCHEMA diffusion_trash_shared;
ALTER TABLE diffusion_shared.binned_annual_load_kwh_50_bins SET SCHEMA diffusion_trash_shared;

-- rates
ALTER TABLE diffusion_shared.urdb3_rate_sam_jsons_backup SET SCHEMA diffusion_trash_shared;

-- max market curve
ALTER TABLE diffusion_shared.max_market_share SET SCHEMA diffusion_trash_shared;
ALTer TABLE diffusion_shared.max_market_share_revised RENAME TO max_market_share;

-- point data
DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_res_new_bad;
DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_res_new_0hu;


-------------------------------------------------------------------------------
-- archive
-- old aeo data
ALTER TABLE diffusion_shared.aeo_load_growth_projections_2013 SET SCHEMA diffusion_shared_data;
-- electric rates
ALTER TABLE diffusion_shared.annual_ave_elec_rates_2011 SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.ann_ave_elec_rates_by_utility_2012 SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.ann_ave_elec_rates_by_state_2012 SET SCHEMA diffusion_shared_data;
-- no hu res points
ALTER TABLE diffusion_shared.pt_grid_us_res_new_no_hu SET SCHEMA diffusion_shared_data;

-- urdb processing intermediate files
ALTER TABLE diffusion_shared.urdb_rates_geoms_res SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.urdb_rates_geoms_com SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.urdb_rates_geom_rate_counts SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.unique_ranked_rate_arrays_res SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.unique_ranked_rate_arrays_ind SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.unique_ranked_rate_arrays_com SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_rate_isect_lkup_res SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_rate_isect_lkup_ind SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_rate_isect_lkup_com SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_ranked_rates_lkup_res SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_ranked_rates_lkup_ind SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_ranked_rates_lkup_com SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_ranked_rate_arrays_res SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_ranked_rate_arrays_ind SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_ranked_rate_arrays_com SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_ranked_rate_array_lkup_res SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_ranked_rate_array_lkup_ind SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.pt_ranked_rate_array_lkup_com SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.applicable_rate_load_combinations_res SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.applicable_rate_load_combinations_ind SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.applicable_rate_load_combinations_com SET SCHEMA diffusion_shared_data;

-- energy plus/load profile intermediate files
ALTER TABLE diffusion_shared.energy_plus_load_meta SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.solar_re_9809_to_eplus_load_com SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.solar_re_9809_to_eplus_load_res SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.cbecs_2003_max_demand_by_tmy_and_eia_region SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.cbecs_2003_pba_to_eplus_crbs SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.eia_microdata_cbecs_2003_pba_lookup SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.eia_microdata_cbecs_2003_pbaplus8_lookup SET SCHEMA diffusion_shared_data;
ALTER TABLE diffusion_shared.eia_reportable_domain_to_state_recs_2009 SET SCHEMA diffusion_shared_data;




-- back up the "shared_trash" schema to disk:
-- pg_dump -h localhost -U mgleason -O -n diffusion_trash_shared -v dav-gis | gzip -6 > diffusion_shared_trash_schema_archive_20150720.gz
-- (in /srv2/mgleason_backups/diffusion_database)

-- drop the "shared_trash" schema
DROP SCHEMA diffusion_trash_shared cASCADE;

-- tested the model one more time for both wind and solar --- all ran fine