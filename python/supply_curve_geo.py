# -*- coding: utf-8 -*-
"""
Created on Thu May 26 11:29:02 2016

@author: mgleason
"""
import psycopg2 as pg
import numpy as np
import pandas as pd
import decorators
import utility_functions as utilfunc
import multiprocessing
import traceback
import data_functions as datfunc
from agent import Agent, Agents, AgentsAlgorithm
from cStringIO import StringIO
import pssc_mp

#%% GLOBAL SETTINGS

# load logger
logger = utilfunc.get_logger()

# configure psycopg2 to treat numeric values as floats (improves performance of pulling data from the database)
DEC2FLOAT = pg.extensions.new_type(
    pg.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
pg.extensions.register_type(DEC2FLOAT)


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def setup_resource_data(cur, con, schema, seed):
    
    setup_resource_data_egs_hdr(cur, con, schema, seed)
    setup_resource_data_hydrothermal(cur, con, schema, seed)
    
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def setup_resource_data_egs_hdr(cur, con, schema, seed):
    
    inputs = locals().copy()
    
    sql = """DROP TABLE IF EXISTS %(schema)s.resources_egs_hdr;
             CREATE UNLOGGED TABLE %(schema)s.resources_egs_hdr AS
             WITH a AS
            (
                	SELECT a.tract_id_alias, 
                         a.cell_gid as gid, 
                         a.area_of_intersection_sqkm as area_sqkm,
                         b.depth_km, 
                         b.thickness_km,
                		diffusion_shared.r_rnorm_rlnorm(b.t_deg_c_mean, 
                                                        b.t_deg_c_sd, 
                                                        'normal'::TEXT, 
                                                        a.tract_id_alias * %(seed)s) as t_deg_c_est
                	FROM diffusion_geo.egs_tract_id_alias_lkup a
                	LEFT JOIN diffusion_geo.egs_hdr_temperature_at_depth b
                    	ON a.cell_gid = b.gid
                  INNER JOIN %(schema)s.tracts_to_model c
                      ON a.tract_id_alias = c.tract_id_alias
            ),
            b as
            (
                	SELECT tract_id_alias, gid, area_sqkm,
                		depth_km, thickness_km,
                		case when t_deg_c_est > 150 or t_deg_c_est < 30 then 0 -- bound temps between 30 and 150
                		     else t_deg_c_est
                		end as res_temp_deg_c,
                		area_sqkm * thickness_km as volume_km3
                	FROM a
            ),
            c as
            (
                	SELECT c.year,
                         b.tract_id_alias,
                         b.gid,
                         b.depth_km,
                         ROUND(b.area_sqkm/c.area_per_wellset_sqkm,0)::INTEGER as n_wellsets_in_tract,
                	 	diffusion_geo.extractable_resource_joules_recovery_factor(b.volume_km3, 
            									    b.res_temp_deg_c, 
            									    c.resource_recovery_factor)/3.6e+9 as extractable_resource_mwh
                 FROM b
                 CROSS JOIN %(schema)s.input_du_egs_reservoir_factors c
            )
            SELECT c.year,
                	c.tract_id_alias,
                 c.gid as resource_id,
                 'egs'::TEXT as resource_type,
                 'hdr'::TEXT as system_type,
                 c.depth_km * 1000 as depth_m,
                 c.n_wellsets_in_tract,
                	CASE WHEN extractable_resource_mwh < 0 THEN 0 -- prevent negative values
                	ELSE c.extractable_resource_mwh/c.n_wellsets_in_tract
                	END as extractable_resource_per_wellset_in_tract_mwh
            FROM c
            WHERE c.n_wellsets_in_tract > 0;""" % inputs
    cur.execute(sql)
    con.commit()
    # TODO: set this up to use p_run?
    


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def setup_resource_data_hydrothermal(cur, con, schema, seed):
    
    inputs = locals().copy()

    sql = """DROP TABLE IF EXISTS %(schema)s.resources_hydrothermal;
             CREATE UNLOGGED TABLE %(schema)s.resources_hydrothermal AS
             SELECT a.tract_id_alias,
                    a.resource_id,
                    a.resource_type,
                    a.system_type,
                	  round(
                		diffusion_shared.r_runif(a.min_depth_m, 
                				  a.max_depth_m, 
                				 1, 
                				 %(seed)s * a.tract_id_alias),
                		0)::INTEGER as depth_m,
                   n_wells_in_tract as n_wellsets_in_tract,
                   extractable_resource_per_well_in_tract_mwh as extractable_resource_per_wellset_in_tract_mwh -- todo: just rename this in the source table
             FROM diffusion_geo.hydrothermal_resource_data_dummy a -- TODO: replace with actual resource data from meghan -- may need to merge pts and polys;""" % inputs
    cur.execute(sql)
    con.commit()
    # TODO: add some mechanism for only compiling data for tracts in states to model
    # TODO: set this up to use p_run?


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_resource_data(con, schema, year):
    
    inputs = locals().copy()
        
    sql = """SELECT %(year)s as year,
                    a.tract_id_alias,
                    a.resource_id,
                    a.resource_type,
                    a.system_type,
                    a.depth_m,
                    a.n_wellsets_in_tract,
                    a.extractable_resource_per_wellset_in_tract_mwh as resource_per_wellset_mwh
             FROM %(schema)s.resources_hydrothermal a
             
             UNION ALL
             
             SELECT b.year,
                     b.tract_id_alias,
                    b.resource_id::TEXT as resource_id,
                    b.resource_type,
                    b.system_type,
                    b.depth_m,
                    b.n_wellsets_in_tract,
                    b.extractable_resource_per_wellset_in_tract_mwh as resource_per_wellset_mwh
             FROM %(schema)s.resources_egs_hdr b
             WHERE b.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_plant_cost_and_performance_data(con, schema, year):
    
    inputs = locals().copy()
    sql = """SELECT a.year, 
                  a.future_drilling_cost_improvements_pct, 
                	a.exploration_slim_well_cost_pct_of_normal_well, 
                	a.exploration_fixed_costs_dollars, 
                	b.plant_installation_costs_dollars_per_kw,
                	b.om_labor_costs_dlrs_per_kw_per_year,
                	b.om_plant_costs_pct_plant_cap_costs_per_year,
                	b.om_well_costs_pct_well_cap_costs_per_year,
                	b.distribution_network_construction_costs_dollars_per_m,
                	b.operating_costs_reservoir_pumping_costs_dollars_per_gal,
                	b.operating_costs_distribution_pumping_costs_dollars_per_gal_m,
                	b.natural_gas_peaking_boilers_dollars_per_kw,
                	c.peaking_boilers_pct_of_peak_demand,
                	c.max_acceptable_drawdown_pct_of_initial_capacity
            FROM %(schema)s.input_du_cost_plant_subsurface a
            LEFT JOIN %(schema)s.input_du_cost_plant_surface b
                         ON a.year = b.year
            LEFT JOIN %(schema)s.input_du_performance_projections c
                         ON a.year = c.year
            where a.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_reservoir_factors(con, schema, year):
    
    inputs = locals().copy()

    sql = """SELECT 'egs'::text as resource_type, 
                	a.wells_per_wellset, 
                	a.expected_drawdown_pct_per_year,
                  a.max_sustainable_well_production_liters_per_second * .264172 * 3.154e7 as max_sustainable_well_production_gallons_per_year,
                	b.reservoir_stimulation_costs_per_wellset_dlrs
            FROM %(schema)s.input_du_egs_reservoir_factors a
            LEFT JOIN  %(schema)s.input_du_cost_plant_subsurface b
                	ON a.year = b.year
            WHERE a.year = %(year)s
            
            UNION ALL
            
            SELECT 'hydrothermal'::text as resource_type, 
                	c.wells_per_wellset, 
                 c.expected_drawdown_pct_per_year,
                 31.5 * .264172 * 3.154e7::NUMERIC as max_sustainable_well_production_gallons_per_year,
                 0::NUMERIC as reservoir_stimulation_costs_per_wellset_dlrs
            FROM %(schema)s.input_du_hydrothermal_reservoir_factors c
            WHERE c.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_tract_demand_profiles(con, cur, schema):
    
    inputs = locals().copy()
    scale_factor = 1/1e8
    inputs['scale_factor'] = scale_factor
    
    sql = """DROP TABLE IF EXISTS %(schema)s.tract_aggregate_heat_demand_profiles;
             CREATE UNLOGGED TABLE %(schema)s.tract_aggregate_heat_demand_profiles AS
             WITH com as
             (
                 SELECT a.tract_id_alias,
                         a.total_heat_kwh_in_bin,
                         b.nkwh
                 FROM %(schema)s.agent_core_attributes_com a
                 LEFT JOIN diffusion_load_profiles.energy_plus_normalized_water_and_space_heating_com b
                 ON a.crb_model = b.crb_model
                 AND a.hdf_load_index = b.hdf_index
             ),
             res AS
             (
                 SELECT a.tract_id_alias,
                         a.total_heat_kwh_in_bin,
                         b.nkwh
                 FROM %(schema)s.agent_core_attributes_res a
                 LEFT JOIN diffusion_load_profiles.energy_plus_normalized_water_and_space_heating_res b
                 ON a.crb_model = b.crb_model
                 AND a.hdf_load_index = b.hdf_index             
             ),
             combined as
             (
                 SELECT *
                 FROM com
                 UNION ALL
                 SELECT *
                 FROM res             
             ),
             scaled as
             (
                 SELECT tract_id_alias,
                        diffusion_shared.r_scale_array_sum(
                            diffusion_shared.r_scale_array_precision(b.nkwh, %(scale_factor)s),
                            total_heat_kwh_in_bin
                        ) as kwh
                 FROM combined             
             )
             SELECT tract_id_alias, diffusion_shared.r_sum_arrays(array_agg_mult(ARRAY[kwh])) as tract_thermal_load_profile
             FROM scaled
             GROUP BY tract_id_alias;""" % inputs
    
    cur.execute(sql)
    con.commit()
 
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_tract_demand_profiles(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT tract_id_alias, tract_thermal_load_profile
            FROM %(schema)s.tract_aggregate_heat_demand_profiles;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df  
    
#%%
def drilling_costs_per_depth_m_deep(depth_m, future_drilling_cost_improvements_pct):
    
    # applies to wells > 500 m deep
    costs_dlrs_base = (1.72 * 10**-7 * depth_m**2 + 2.3 * 10**-3 * depth_m - 0.62) * 1e6
    costs_dlrs = costs_dlrs_base * (1 - future_drilling_cost_improvements_pct)
    
    return costs_dlrs


#%%
def drilling_costs_per_depth_m_shallow(depth_m, future_drilling_cost_improvements_pct):
    
    # applies to wells < 500 m deep
    costs_dlrs_base = 1146 * depth_m
    costs_dlrs = costs_dlrs_base * (1 - future_drilling_cost_improvements_pct)
    
    return costs_dlrs    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_distribution_network_data(con, schema): # todo: add con, schema

    inputs = locals().copy()

    # TODO:-- convert the mwh to mw using demand curves/load profiles (not 8760 constant assumption)
    sql = """WITH a as 
            (
                SELECT tract_id_alias,
	                (sum(space_heat_kbtu_in_bin + water_heat_kbtu_in_bin)) * 0.0002930711/8760 as heat_demand_mw
	            FROM %(schema)s.agent_core_attributes_all
	            WHERE tech = 'du'
	            GROUP BY tract_id_alias
             )
            SELECT a.tract_id_alias,
                b.road_meters / a.heat_demand_mw as distribution_m_per_mw,
                b.road_meters as distribution_total_m
            FROM a
            LEFT JOIN diffusion_geo.tract_road_length b
            ON a.tract_id_alias = b.tract_id_alias;""" % inputs

    df = pd.read_sql(sql, con, coerce_float = False)

    return df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_cost_and_performance_data(resource_df, costs_and_performance_df, reservoir_factors_df, plant_finances_df,
                                    distribution_df):
    
    inputs = locals().copy()


    
    # merge resources with reservoir factors (left join on resource_type (egs or hydrothermal))
    dataframe = pd.merge(resource_df, reservoir_factors_df, how = 'left', on = ['resource_type'])
    # merge the costs and performance (effecively a cross join)
    dataframe = pd.merge(dataframe, costs_and_performance_df, how = 'left', on = ['year'])
    # merge the finances (effecively a cross join)
    dataframe = pd.merge(dataframe, plant_finances_df, how = 'left', on = ['year'])
    # merge the distribution demand density info (left join on tract id alias)
    dataframe = pd.merge(dataframe, distribution_df, how = 'left', on = ['tract_id_alias'])
    
 
    # for testing:
    #dataframe = pd.read_csv('/Users/mgleason/Desktop/plant_dfs/dataframe.csv')

    # get count of rows
    nrows = dataframe.shape[0]
    # get number of years of plant lifetime (should be the same for all resources)
    plant_lifetime = dataframe['plant_lifetime_yrs'].unique()[0]
    # build some helper arrays (used later)
    constant_lifetime_array = np.ones((nrows, plant_lifetime))
    years_array = np.array([np.arange(1, 31)] * nrows)
    
   
    # determine the capacity per well (based on energy and plant lifetime in years)
    # ***
    dataframe['capacity_per_wellset_mw'] = dataframe['resource_per_wellset_mwh']/(dataframe['plant_lifetime_yrs'] * 8760)
    # ***
    
    # Drilling Costs
    dataframe['drilling_cost_per_well_dlrs'] = np.where(dataframe['depth_m'] >= 500, 
                                                   drilling_costs_per_depth_m_deep(dataframe['depth_m'], dataframe['future_drilling_cost_improvements_pct']), 
                                                   drilling_costs_per_depth_m_shallow(dataframe['depth_m'], dataframe['future_drilling_cost_improvements_pct'])
                                                   )
    # ***
    dataframe['drilling_cost_per_wellset_dlrs'] = dataframe['drilling_cost_per_well_dlrs'] * dataframe['wells_per_wellset']
    # ***

    # Exploration Costs
    dataframe['exploration_well_costs_per_wellset_dlrs'] = dataframe['drilling_cost_per_well_dlrs'] * dataframe['exploration_slim_well_cost_pct_of_normal_well']
    # ***    
    dataframe['exploration_total_costs_per_wellset_dlrs'] = dataframe['exploration_well_costs_per_wellset_dlrs'] + dataframe['exploration_fixed_costs_dollars']
    # ***

    # Surface Plant Capital Costs
    # ***
    dataframe['plant_installation_costs_per_wellset_dlrs'] = dataframe['capacity_per_wellset_mw'] * 1000 * dataframe['plant_installation_costs_dollars_per_kw']
    # ***
    
    # O&M Costs
    dataframe['om_labor_costs_per_wellset_per_year_dlrs'] = dataframe['om_labor_costs_dlrs_per_kw_per_year'] * 1000 * dataframe['capacity_per_wellset_mw']
    dataframe['om_plant_costs_per_wellset_per_year_dlrs'] = dataframe['om_plant_costs_pct_plant_cap_costs_per_year'] * dataframe['plant_installation_costs_per_wellset_dlrs']
    dataframe['om_well_costs_per_wellset_per_year_dlrs'] = dataframe['om_well_costs_pct_well_cap_costs_per_year'] * dataframe['drilling_cost_per_wellset_dlrs']
    dataframe['om_total_costs_per_wellset_per_year_dlrs'] = dataframe['om_labor_costs_per_wellset_per_year_dlrs'] + dataframe['om_plant_costs_per_wellset_per_year_dlrs'] + dataframe['om_well_costs_per_wellset_per_year_dlrs']
    # ***  
    # convert to a time series
    dataframe['om_total_costs_per_wellset_dlrs'] = (dataframe['om_total_costs_per_wellset_per_year_dlrs'].values[:,None] * constant_lifetime_array).tolist()
    # ***

    # ***
    # Reservoir Stimulation Costs
    # need this as is
    #dataframe['reservoir_stimulation_costs_per_wellset_dlrs']
    # ***

    # Distribution Network Construction Costs
    # TODO: double-check this logic makes sense
    # ***
    dataframe['distribution_network_construction_costs_per_wellset_dlrs'] = dataframe['distribution_network_construction_costs_dollars_per_m'] * dataframe['distribution_m_per_mw'] * dataframe['capacity_per_wellset_mw']
    # ***

    # Operating Costs
    # TODO: make sure thesee make sense given plant capacity factor (won't be pumping all the time)
    dataframe['operating_costs_reservoir_pumping_costs_per_wellset_per_year_dlrs'] = dataframe['operating_costs_reservoir_pumping_costs_dollars_per_gal'] * dataframe['max_sustainable_well_production_gallons_per_year']
    dataframe['operating_costs_distribution_pumping_costs_per_wellset_per_year_dlrs'] = dataframe['operating_costs_distribution_pumping_costs_dollars_per_gal_m'] * dataframe['max_sustainable_well_production_gallons_per_year'] *  dataframe['distribution_m_per_mw'] * dataframe['capacity_per_wellset_mw']
    dataframe['total_pumping_costs_per_wellset_per_year_dlrs'] = dataframe['operating_costs_reservoir_pumping_costs_per_wellset_per_year_dlrs'] + dataframe['operating_costs_distribution_pumping_costs_per_wellset_per_year_dlrs']
    # convert to a time series
    # ***
    dataframe['total_pumping_costs_per_wellset_dlrs'] = (dataframe['total_pumping_costs_per_wellset_per_year_dlrs'].values[:,None] * constant_lifetime_array).tolist()
    # ***

    # Peaking Boiler Capital Construction Costs
    # TODO: check this logic makes sense - is peak demand simply the wellset capacity?
    dataframe['peaking_boilers_capacity_kw_per_wellset'] = dataframe['capacity_per_wellset_mw']/(1 - dataframe['peaking_boilers_pct_of_peak_demand']) * dataframe['peaking_boilers_pct_of_peak_demand'] * 1000
    # ***    
    dataframe['peaking_boilers_cost_per_wellset_dlrs'] = dataframe['peaking_boilers_capacity_kw_per_wellset'] * dataframe['natural_gas_peaking_boilers_dollars_per_kw'] 
    # ***

    # Peaking Boiler Operating Costs
    

    # Additional Boiler Costs due to Reservoir Drawdown
    # determine which years, if any, will require purchase of additional boilers due to drawdown
    dataframe['years_to_drawdown'] = np.floor(np.log(1-dataframe['max_acceptable_drawdown_pct_of_initial_capacity'])/np.log(1-dataframe['expected_drawdown_pct_per_year'])).astype(np.int64)
    min_years_to_drawdown = dataframe['years_to_drawdown'].min()   
    max_multiples = plant_lifetime/min_years_to_drawdown
    multiples = np.arange(1, max_multiples+1)
    years_exceeding_drawdown = dataframe['years_to_drawdown'].values.reshape(nrows, 1) * multiples
    years_exceeding_drawdown_during_lifetime = np.where(years_exceeding_drawdown < plant_lifetime, years_exceeding_drawdown, -100)
    bool_years_exceeding_drawdown = np.sum((years_array[:,:,None] - years_exceeding_drawdown_during_lifetime[:,None,:] == 0), 2)
    dataframe['boiler_purchase_years'] = bool_years_exceeding_drawdown.tolist()

    # determine the cost of boilers in each of these years
    dataframe['drawdown_boilers_capacity_kw_per_wellset'] = dataframe['capacity_per_wellset_mw'] * dataframe['max_acceptable_drawdown_pct_of_initial_capacity'] * 1000.
    dataframe['drawdown_boilers_cost_per_wellset_per_purchase_dlrs'] = dataframe['drawdown_boilers_capacity_kw_per_wellset'] * dataframe['natural_gas_peaking_boilers_dollars_per_kw'] 
    # convert to a time series (using the purchase years from above)
    # ***
    dataframe['drawdown_boilers_cost_per_wellset_dlrs'] = (np.array(dataframe['boiler_purchase_years'].tolist(), dtype = np.float64) * dataframe['drawdown_boilers_cost_per_wellset_per_purchase_dlrs'].values[:, None]).tolist()
    # ***

    # combine all upfront costs
    dataframe['upfront_costs_per_wellset_dlrs'] = ( dataframe['peaking_boilers_cost_per_wellset_dlrs'] +
                                                    dataframe['distribution_network_construction_costs_per_wellset_dlrs'] +
                                                    dataframe['plant_installation_costs_per_wellset_dlrs'] +
                                                    dataframe['exploration_total_costs_per_wellset_dlrs'] +
                                                    dataframe['drilling_cost_per_wellset_dlrs'] +
                                                    dataframe['reservoir_stimulation_costs_per_wellset_dlrs']
                                                    )
    # combine all annual costs
    dataframe['annual_costs_per_wellset_dlrs'] = (  np.array(dataframe['drawdown_boilers_cost_per_wellset_dlrs'].tolist(), dtype = np.float64) +
                                                    np.array(dataframe['total_pumping_costs_per_wellset_dlrs'].tolist(), dtype = np.float64) +
                                                    np.array(dataframe['om_total_costs_per_wellset_dlrs'].tolist(), dtype = np.float64)
                                                    ).tolist()

    out_cols = ['tract_id_alias',
                   'resource_id',
                   'resource_type',
                   'depth_m',
                   'system_type',
                   'n_wellsets_in_tract',
                   'resource_per_wellset_mwh',
                   'capacity_per_wellset_mw',
                   'upfront_costs_per_wellset_dlrs', 
                   'annual_costs_per_wellset_dlrs', 
                   'inflation_rate',
                   'interest_rate_nominal',
                   'interest_rate_during_construction_nominal',
                   'rate_of_return_on_equity',
                   'debt_fraction',
                   'tax_rate',
                   'construction_period_yrs',
                   'plant_lifetime_yrs',
                   'depreciation_period']
    dataframe = dataframe[out_cols]                                                    
    #dataframe.to_csv('/Users/mgleason/Desktop/plant_dfs/dataframe_costs_and_finances.csv', index = False)
    
    return dataframe
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_plant_finance_data(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT year,
                    inflation_rate,
                    interest_rate_nominal,
                    interest_rate_during_construction_nominal,
                    rate_of_return_on_equity,
                    debt_fraction,
                    tax_rate,
                    construction_period_yrs,
                    plant_lifetime_yrs,
                    depreciation_period
            FROM %(schema)s.input_du_plant_finances
            where year = %(year)s;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df        
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_plant_construction_factor_data(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT year, 
                    year_of_construction, 
                    capital_fraction
            FROM %(schema)s.input_du_plant_construction_finance_factor
            where year = %(year)s;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df       


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_plant_depreciation_data(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT year,
                    year_of_operation,
                    depreciation_fraction
            FROM %(schema)s.input_du_plant_depreciation_factor
            where year = %(year)s;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df       