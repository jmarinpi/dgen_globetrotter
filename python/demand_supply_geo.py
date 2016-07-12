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
import agent_preparation_geo as agent_prep

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
def setup_resource_data(cur, con, schema, seed, pg_procs, pg_conn_string):
    
    
    # break tracts into subsets for parallel processing
    chunks, pg_procs = agent_prep.split_tracts(cur, schema, pg_procs)       
 
    # create the pool of multiprocessing workers
    # (note: do this after splitting counties because, for small states, split_counties will adjust the number of pg_procs)
    pool = multiprocessing.Pool(processes = pg_procs) 
    
    try:    
        setup_resource_data_egs_hdr(cur, con, schema, seed, pool, pg_conn_string, chunks)
        setup_resource_data_hydrothermal(cur, con, schema, seed, pool, pg_conn_string, chunks)
    except:
        # roll back any transactions
        con.rollback()
        # re-raise the exception
        raise
    finally:
        # close the multiprocessing pool
        pool.close() 
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def setup_resource_data_egs_hdr(cur, con, schema, seed, pool, pg_conn_string, chunks):
    
    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'
    
    # create the output table
    sql = """DROP TABLE IF EXISTS %(schema)s.resources_egs_hdr;
             CREATE UNLOGGED TABLE %(schema)s.resources_egs_hdr
            (
                  year INTEGER,
                  tract_id_alias INTEGER,
                  resource_id INTEGER,
                  resource_type TEXT,
                  system_type TEXT,
                  depth_m NUMERIC,
                  n_wellsets_in_tract INTEGER,
                  extractable_resource_per_wellset_in_tract_mwh NUMERIC
            );""" % inputs
    cur.execute(sql)
    con.commit()
    
    # construct the sql statement to run in parallel
    sql = """INSERT INTO %(schema)s.resources_egs_hdr          
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
                  WHERE c.tract_id_alias IN (%(chunk_place_holder)s)
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
    agent_prep.p_run(pg_conn_string, sql, chunks, pool)
    
    # add an index on year
    sql = """CREATE INDEX resources_egs_hdr_btree_year
             ON %(schema)s.resources_egs_hdr 
             USING BTREE(year);""" % inputs
    cur.execute(sql)
    con.commit()
    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def setup_resource_data_hydrothermal(cur, con, schema, seed, pool, pg_conn_string, chunks):
    
    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'

    sql = """DROP TABLE IF EXISTS %(schema)s.resources_hydrothermal;
             CREATE UNLOGGED TABLE %(schema)s.resources_hydrothermal 
             (
                  tract_id_alias INTEGER,
                  resource_id VARCHAR(5),
                  resource_type TEXT,
                  system_type TEXT,
                  depth_m INTEGER,
                  n_wellsets_in_tract INTEGER,
                  extractable_resource_per_wellset_in_tract_mwh NUMERIC
             );""" % inputs
    cur.execute(sql)
    con.commit()
    
    sql = """INSERT INTO %(schema)s.resources_hydrothermal
            SELECT a.tract_id_alias,
                	b.resource_id,
                	b.resource_type,
                	 b.system_type,
                		  round(
                			diffusion_shared.r_runif(b.min_depth_m, 
                					  b.max_depth_m, 
                					 1, 
                					 %(seed)s * a.tract_id_alias),
                			0)::INTEGER as depth_m,
                	b.n_wells_in_tract as n_wellsets_in_tract,
                	b.extractable_resource_per_well_in_tract_mwh as extractable_resource_per_wellset_in_tract_mwh -- TODO: just rename this in the source table
            FROM %(schema)s.tracts_to_model a 
            CROSS JOIN diffusion_geo.hydrothermal_resource_data_dummy b
            WHERE a.tract_id_alias IN (%(chunk_place_holder)s);""" % inputs # TODO: replace with actual resource data from meghan -- may need to merge pts and poly
    agent_prep.p_run(pg_conn_string, sql, chunks, pool)


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_resource_data(con, schema, year):
    
    inputs = locals().copy()
        
    sql = """SELECT a.tract_id_alias,
                    a.resource_id,
                    a.resource_type,
                    a.system_type,
                    a.depth_m,
                    a.n_wellsets_in_tract,
                    a.extractable_resource_per_wellset_in_tract_mwh as lifetime_resource_per_wellset_mwh
             FROM %(schema)s.resources_hydrothermal a
             
             UNION ALL
             
             SELECT b.tract_id_alias,
                    b.resource_id::TEXT as resource_id,
                    b.resource_type,
                    b.system_type,
                    b.depth_m,
                    b.n_wellsets_in_tract,
                    b.extractable_resource_per_wellset_in_tract_mwh as lifetime_resource_per_wellset_mwh
             FROM %(schema)s.resources_egs_hdr b
             WHERE b.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_plant_cost_and_performance_data(con, schema, year):
    
    inputs = locals().copy()
    sql = """WITH a as
             (
                SELECT a.tract_id_alias, 
                        b.cap_cost_multiplier_geo_blended as cap_cost_multiplier
                FROM %(schema)s.tracts_to_model a
                LEFT JOIN diffusion_geo.regional_cap_cost_multipliers b
                ON a.county_id = b.county_id             
             ),
             b as
             (
                SELECT a.year, 
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
                      c.peaking_boiler_efficiency,
                    	c.max_acceptable_drawdown_pct_of_initial_capacity,
                      c.avg_end_use_efficiency_factor
                FROM %(schema)s.input_du_cost_plant_subsurface a
                LEFT JOIN %(schema)s.input_du_cost_plant_surface b
                             ON a.year = b.year
                LEFT JOIN %(schema)s.input_du_performance_projections c
                             ON a.year = c.year
                WHERE a.year = %(year)s             
             )
             SELECT a.tract_id_alias, 
                     a.cap_cost_multiplier,
                    b.*
             FROM a
             CROSS JOIN b;""" % inputs
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
def calculate_tract_demand_profiles(con, cur, schema, pg_procs, pg_conn_string):
    
    inputs = locals().copy()
    
    logger.info("Calculating Aggregate Heat Demand Profiles for Tracts")    
    
    # break tracts into subsets for parallel processing
    chunks, pg_procs = agent_prep.split_tracts(cur, schema, pg_procs)       
 
    # create the pool of multiprocessing workers
    # (note: do this after splitting counties because, for small states, split_counties will adjust the number of pg_procs)
    pool = multiprocessing.Pool(processes = pg_procs) 

    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'
    
    try:
        sql = """DROP TABLE IF EXISTS %(schema)s.tract_aggregate_heat_demand_profiles;
                 CREATE UNLOGGED TABLE %(schema)s.tract_aggregate_heat_demand_profiles 
                 (
                    tract_id_alias integer,
                    heat_demand_profile_mw NUMERIC[]
                 );""" % inputs
        cur.execute(sql)
        con.commit()       
        
        sql = """INSERT INTO %(schema)s.tract_aggregate_heat_demand_profiles
                 WITH com as
                 (
                     SELECT a.tract_id_alias,
                             a.total_heat_kwh_in_bin,
                             b.nkwh
                     FROM %(schema)s.agent_core_attributes_com a
                     LEFT JOIN diffusion_load_profiles.energy_plus_normalized_water_and_space_heating_com b
                     ON a.crb_model = b.crb_model
                     AND a.hdf_load_index = b.hdf_index
                     WHERE a.tract_id_alias in (%(chunk_place_holder)s)
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
                     WHERE a.tract_id_alias in (%(chunk_place_holder)s)
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
                            diffusion_shared.r_scale_array_sum(nkwh, total_heat_kwh_in_bin/1000.) as mwh
                     FROM combined             
                 )
                 SELECT tract_id_alias, diffusion_shared.r_sum_arrays(array_agg_mult(ARRAY[mwh])) as heat_demand_profile_mw
                 FROM scaled
                 GROUP BY tract_id_alias;""" % inputs
        
        agent_prep.p_run(pg_conn_string, sql, chunks, pool)        
    except:
        # roll back any transactions
        con.rollback()
        # re-raise the exception
        raise
        
    finally:
        # close the multiprocessing pool
        pool.close() 
 

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_tract_peak_demand(cur, con, schema):
    
    inputs = locals().copy()

    logger.info("Calculating Peak Heat Demand for Tracts")    

    sql = """DROP TABLE IF EXISTS %(schema)s.tract_peak_heat_demand;
             CREATE UNLOGGED TABLE %(schema)s.tract_peak_heat_demand AS
            SELECT a.tract_id_alias,
                        r_array_max(a.heat_demand_profile_mw) as peak_heat_demand_mw
                FROM %(schema)s.tract_aggregate_heat_demand_profiles a;""" % inputs
    cur.execute(sql)
    con.commit()
    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_tract_demand_profiles(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT tract_id_alias, heat_demand_profile_mw
            FROM %(schema)s.tract_aggregate_heat_demand_profiles;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df 


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_tract_peak_demand(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT tract_id_alias, peak_heat_demand_mw
            FROM %(schema)s.tract_peak_heat_demand;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df 

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_plant_and_boiler_capacity_factors(tract_peak_demand_df, costs_and_performance_df, tract_demand_profiles_df, year):

    # join the two dataframes
    dataframe = pd.merge(tract_peak_demand_df, costs_and_performance_df, how = 'left', on = ['tract_id_alias'])
    # merge in the hourly profiles
    dataframe = pd.merge(dataframe, tract_demand_profiles_df, how = 'left', on = ['tract_id_alias'])
    # calculate the boiler capacity required to meet the full demand (accounting for target pct of demand and efficiency factors)
    dataframe['peaking_boiler_nameplate_capacity_mw'] = dataframe['peaking_boilers_pct_of_peak_demand'] * dataframe['peak_heat_demand_mw']/(dataframe['peaking_boiler_efficiency'] * dataframe['avg_end_use_efficiency_factor'])
    # calculate the plant capacity required to meet the full demand (accounting for boiler target pct of demand and efficiency factors)
    # (assume that plant heat is coming out of the ground at 100% efficiency factor)
    dataframe['plant_nameplate_capacity_mw'] = (1 - dataframe['peaking_boilers_pct_of_peak_demand']) * dataframe['peak_heat_demand_mw']/(dataframe['avg_end_use_efficiency_factor'])
    # calculate the effective capacity of each component (accounting for efficiency factors)
    dataframe['peaking_boiler_effective_capacity_mw'] = dataframe['peaking_boiler_nameplate_capacity_mw'] * dataframe['peaking_boiler_efficiency'] * dataframe['avg_end_use_efficiency_factor']
    dataframe['plant_effective_capacity_mw'] = dataframe['plant_nameplate_capacity_mw'] * dataframe['avg_end_use_efficiency_factor']    
    # convert hourly profiles to a 2D np array
    hourly_demand_mw = np.array(dataframe['heat_demand_profile_mw'].tolist(), dtype = 'float64')
    # create plant supply profile by capping the hourly demand profile to the plant effective capacity
    plant_hourly_supply_mw = np.where(hourly_demand_mw <= dataframe['plant_effective_capacity_mw'][:, None], hourly_demand_mw, dataframe['plant_effective_capacity_mw'][:, None])
    # create boiler supply profile by extracting the demand exceeding the plant effective capacity and then capping the hourly demand profile to the boiler effective capacity
    peaking_boiler_hourly_demand_mw = hourly_demand_mw - plant_hourly_supply_mw    
    peaking_boiler_hourly_supply_mw = np.where(peaking_boiler_hourly_demand_mw <= dataframe['peaking_boiler_effective_capacity_mw'][:, None], peaking_boiler_hourly_demand_mw, dataframe['peaking_boiler_effective_capacity_mw'][:, None])
    # calculate capacity factors based on the hourly supply vs. nameplate capacity
    dataframe['plant_capacity_factor'] = plant_hourly_supply_mw.sum(axis = 1)/(dataframe['plant_nameplate_capacity_mw'] * 8760)
    dataframe['peaking_boiler_capacity_factor'] = peaking_boiler_hourly_supply_mw.sum(axis = 1)/(dataframe['peaking_boiler_nameplate_capacity_mw'] * 8760)
    # calculate a combined capacity factor
    dataframe['total_blended_capacity_factor'] = (plant_hourly_supply_mw + peaking_boiler_hourly_supply_mw).sum(axis = 1)/((dataframe['plant_nameplate_capacity_mw'] + dataframe['peaking_boiler_nameplate_capacity_mw']) * 8760)
    
    # FOR testing only
    # deriving supply profiles
#    test = pd.DataFrame()
#    test['demand_mw'] = peaking_boiler_hourly_demand_mw[50,:]
#    test['supply_mw'] = peaking_boiler_hourly_supply_mw[50,:]
#    test['cap_mw'] = dataframe['peaking_boiler_effective_capacity_mw'][50]
#    test.to_csv('/Users/mgleason/Desktop/capped_demand.csv', index = False)

    # checking capacity factors
#    test = pd.DataFrame()
#    test['demand_mw'] = hourly_demand_mw[50,:]
#    test['supply_mw'] = plant_hourly_supply_mw[50,:]
#    test['effective_cap_mw'] = dataframe['plant_effective_capacity_mw'][50]
#    test['np_cap_mw'] = dataframe['plant_nameplate_capacity_mw'][50]
#    test['capacity_factor'] = dataframe['plant_capacity_factor'][50]
#    test.to_csv('/Users/mgleason/Desktop/capped_demand.csv', index = False)

    return_cols = ['tract_id_alias', 'plant_capacity_factor', 'peaking_boiler_capacity_factor', 'total_blended_capacity_factor']
    dataframe = dataframe[return_cols]

    return dataframe

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
def get_natural_gas_prices(con, schema, year):
    
    inputs = locals().copy()

    sql = """SELECT tract_id_alias, array_agg(dlrs_per_mwh order by year) as ng_price_dlrs_per_mwh
            FROM %(schema)s.tract_industrial_natural_gas_prices_to_model
            WHERE year between %(year)s and %(year)s + 29
            GROUP BY tract_id_alias;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    return df    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_distribution_network_data(con, schema): # todo: add con, schema

    inputs = locals().copy()

    sql = """SELECT a.tract_id_alias,
                b.road_meters / a.peak_heat_demand_mw as distribution_m_per_mw,
                b.road_meters as distribution_total_m
            FROM %(schema)s.tract_peak_heat_demand a
            LEFT JOIN diffusion_geo.tract_road_length b
            ON a.tract_id_alias = b.tract_id_alias;""" % inputs

    df = pd.read_sql(sql, con, coerce_float = False)

    return df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_cost_and_performance_data(resource_df, costs_and_performance_df, reservoir_factors_df, plant_finances_df,
                                    distribution_df, capacity_factors_df, ng_prices_df):
    
    inputs = locals().copy()
    
    # merge resources with reservoir factors (left join on resource_type (egs or hydrothermal))
    dataframe = pd.merge(resource_df, reservoir_factors_df, how = 'left', on = ['resource_type'])
    # merge the costs and performance
    dataframe = pd.merge(dataframe, costs_and_performance_df, how = 'left', on = ['tract_id_alias'])
    # merge the finances (effecively a cross join)
    dataframe = pd.merge(dataframe, plant_finances_df, how = 'left', on = ['year'])
    # merge the distribution demand density info (left join on tract id alias)
    dataframe = pd.merge(dataframe, distribution_df, how = 'left', on = ['tract_id_alias'])
    # merge the capacity factor data
    dataframe = pd.merge(dataframe, capacity_factors_df, how = 'left', on = ['tract_id_alias'])
    # merge the natural gas prices
    dataframe = pd.merge(dataframe, ng_prices_df, how = 'left', on = ['tract_id_alias'])
 
    # for testing:
    #dataframe = pd.read_csv('/Users/mgleason/Desktop/plant_dfs/dataframe.csv')

    # get count of rows
    nrows = dataframe.shape[0]
    # get number of years of plant lifetime (should be the same for all resources)
    plant_lifetime = dataframe['plant_lifetime_yrs'].unique()[0]
    # build some helper arrays (used later)
    constant_lifetime_array = np.ones((nrows, plant_lifetime))
    years_array = np.array([np.arange(1, 31)] * nrows)
    
   
    # determine the nameplate capacity per well (based on energy and plant lifetime in years)
    # note: this assumes that lifetime energy is based on maximum sustainable production over the plant lifetime
    # ***
    dataframe['plant_nameplate_capacity_per_wellset_mw'] = dataframe['lifetime_resource_per_wellset_mwh']/(dataframe['plant_lifetime_yrs'] * 8760)
    # ***
    # calculate the effective nameplate capacity per well set
    # this is based on nameplate capacity * end use efficiency
    dataframe['plant_effective_capacity_per_wellset_mw'] = dataframe['plant_nameplate_capacity_per_wellset_mw'] * dataframe['avg_end_use_efficiency_factor']
    
    # calaculate the actual demand that can be met by the wellset (including peaking boilers)
    dataframe['total_effective_capacity_per_wellset_mw'] = dataframe['plant_effective_capacity_per_wellset_mw']/(1 - dataframe['peaking_boilers_pct_of_peak_demand'])
    
    # also calculate the peaking boiler nameplate and effective capacities per wellset
    dataframe['peaking_boilers_nameplate_capacity_per_wellset_mw'] = dataframe['peaking_boilers_pct_of_peak_demand'] * dataframe['total_effective_capacity_per_wellset_mw']/(dataframe['peaking_boiler_efficiency'] * dataframe['avg_end_use_efficiency_factor'])
    dataframe['peaking_boilers_effective_capacity_per_wellset_mw'] = dataframe['peaking_boilers_nameplate_capacity_per_wellset_mw'] * dataframe['peaking_boiler_efficiency'] * dataframe['avg_end_use_efficiency_factor']

    # calculate the total nameplate capacity of the plant + boilers
    dataframe['total_nameplate_capacity_per_wellset_mw'] =  dataframe['plant_nameplate_capacity_per_wellset_mw'] + dataframe['peaking_boilers_nameplate_capacity_per_wellset_mw']
    
    # Drilling Costs
    dataframe['drilling_cost_per_well_dlrs'] = np.where(dataframe['depth_m'] >= 500, 
                                                   drilling_costs_per_depth_m_deep(dataframe['depth_m'], dataframe['future_drilling_cost_improvements_pct']), 
                                                   drilling_costs_per_depth_m_shallow(dataframe['depth_m'], dataframe['future_drilling_cost_improvements_pct'])
                                                   ) * dataframe['cap_cost_multiplier']
    # ***
    dataframe['drilling_cost_per_wellset_dlrs'] = dataframe['drilling_cost_per_well_dlrs'] * dataframe['wells_per_wellset']
    # ***
    # Exploration Costs
    dataframe['exploration_well_costs_per_wellset_dlrs'] = dataframe['drilling_cost_per_well_dlrs'] * dataframe['exploration_slim_well_cost_pct_of_normal_well'] * dataframe['cap_cost_multiplier']
    # ***    
    dataframe['exploration_total_costs_per_wellset_dlrs'] = dataframe['exploration_well_costs_per_wellset_dlrs'] + dataframe['exploration_fixed_costs_dollars']
    # ***


    # Surface Plant Capital Costs
    # ***
    dataframe['plant_installation_costs_per_wellset_dlrs'] = dataframe['plant_nameplate_capacity_per_wellset_mw'] * 1000 * dataframe['plant_installation_costs_dollars_per_kw'] * dataframe['cap_cost_multiplier']
    # ***
    
    # O&M Costs
    # use total nameplate capacity here since this is labor for everything
    dataframe['om_labor_costs_per_wellset_per_year_dlrs'] = dataframe['om_labor_costs_dlrs_per_kw_per_year'] * 1000 * dataframe['total_nameplate_capacity_per_wellset_mw']
    dataframe['om_plant_costs_per_wellset_per_year_dlrs'] = dataframe['om_plant_costs_pct_plant_cap_costs_per_year'] * dataframe['plant_installation_costs_per_wellset_dlrs']
    dataframe['om_well_costs_per_wellset_per_year_dlrs'] = dataframe['om_well_costs_pct_well_cap_costs_per_year'] * dataframe['drilling_cost_per_wellset_dlrs']
    dataframe['om_total_costs_per_wellset_per_year_dlrs'] = dataframe['om_labor_costs_per_wellset_per_year_dlrs'] + dataframe['om_plant_costs_per_wellset_per_year_dlrs'] + dataframe['om_well_costs_per_wellset_per_year_dlrs']
    # ***  
    # convert to a time series
    dataframe['om_total_costs_per_wellset_dlrs'] = (dataframe['om_total_costs_per_wellset_per_year_dlrs'].values[:,None] * constant_lifetime_array).tolist()
    # ***

    # ***
    # Reservoir Stimulation Costs
    dataframe.loc[:, 'reservoir_stimulation_costs_per_wellset_dlrs'] = dataframe['reservoir_stimulation_costs_per_wellset_dlrs'] * dataframe['cap_cost_multiplier']
    # ***

    # Distribution Network Construction Costs
    # ***
    # use achievable peak demand of entire plant (including boilers) (=total_effective_capacity_per_wellset_mw)
    # don't use nameplate because distribution_network_construction_costs_dollars_per_m is based on actual demand
    dataframe['distribution_m_per_wellset'] = dataframe['distribution_m_per_mw'] * dataframe['total_effective_capacity_per_wellset_mw']
    dataframe['distribution_network_construction_costs_per_wellset_dlrs'] = dataframe['distribution_network_construction_costs_dollars_per_m'] * dataframe['distribution_m_per_wellset'] * dataframe['cap_cost_multiplier']
    # ***

    # Operating Costs
    # costs for reservoir pumping are based on plant capacity factor
    dataframe['reservoir_pumping_gallons_per_year'] = dataframe['max_sustainable_well_production_gallons_per_year'] * dataframe['plant_capacity_factor']
    dataframe['operating_costs_reservoir_pumping_costs_per_wellset_per_year_dlrs'] = dataframe['operating_costs_reservoir_pumping_costs_dollars_per_gal'] * dataframe['reservoir_pumping_gallons_per_year']
    # costs for distribution pumping are based on blended capacity factor
    dataframe['distribution_pumping_gallons_per_year'] = dataframe['max_sustainable_well_production_gallons_per_year'] * dataframe['total_blended_capacity_factor']
    dataframe['operating_costs_distribution_pumping_costs_per_wellset_per_year_dlrs'] = dataframe['operating_costs_distribution_pumping_costs_dollars_per_gal_m'] * dataframe['distribution_pumping_gallons_per_year'] *  dataframe['distribution_m_per_wellset']
    # combined costs
    dataframe['total_pumping_costs_per_wellset_per_year_dlrs'] = dataframe['operating_costs_reservoir_pumping_costs_per_wellset_per_year_dlrs'] + dataframe['operating_costs_distribution_pumping_costs_per_wellset_per_year_dlrs']
    # convert to a time series
    # ***
    dataframe['total_pumping_costs_per_wellset_dlrs'] = (dataframe['total_pumping_costs_per_wellset_per_year_dlrs'].values[:,None] * constant_lifetime_array).tolist()
    # ***

    # Peaking Boiler Capital Construction Costs
    # ***    
    dataframe['peaking_boilers_construction_cost_per_wellset_dlrs'] = dataframe['peaking_boilers_nameplate_capacity_per_wellset_mw'] * 1000. * dataframe['natural_gas_peaking_boilers_dollars_per_kw'] 
    # ***

    # Peaking Boiler Annual Fuel Costs
    dataframe['peaking_boilers_mwh_per_year_per_wellset'] = dataframe['peaking_boilers_nameplate_capacity_per_wellset_mw'] * 8760. * dataframe['peaking_boiler_capacity_factor']
    ng_prices_array = np.array(dataframe['ng_price_dlrs_per_mwh'].tolist(), dtype = 'float64')
    ng_annual_costs = dataframe['peaking_boilers_mwh_per_year_per_wellset'][:, None] * ng_prices_array
    # ***    
    dataframe['peaking_boilers_fuel_costs_per_wellset_dlrs'] = ng_annual_costs.tolist()
    # ***

    # combine all upfront costs
    dataframe['upfront_costs_per_wellset_dlrs'] = ( dataframe['peaking_boilers_construction_cost_per_wellset_dlrs'] +
                                                    dataframe['distribution_network_construction_costs_per_wellset_dlrs'] +
                                                    dataframe['plant_installation_costs_per_wellset_dlrs'] +
                                                    dataframe['exploration_total_costs_per_wellset_dlrs'] +
                                                    dataframe['drilling_cost_per_wellset_dlrs'] +
                                                    dataframe['reservoir_stimulation_costs_per_wellset_dlrs']
                                                    )
    # combine all annual costs
    dataframe['annual_costs_per_wellset_dlrs'] = (  np.array(dataframe['total_pumping_costs_per_wellset_dlrs'].tolist(), dtype = np.float64) +
                                                    np.array(dataframe['om_total_costs_per_wellset_dlrs'].tolist(), dtype = np.float64) +
                                                    np.array(dataframe['peaking_boilers_fuel_costs_per_wellset_dlrs'].tolist(), dtype = np.float64)
                                                    ).tolist()

    out_cols = ['tract_id_alias',
                   'resource_id',
                   'resource_type',
                   'depth_m',
                   'system_type',
                   'n_wellsets_in_tract',
                   'lifetime_resource_per_wellset_mwh',
                   'plant_nameplate_capacity_per_wellset_mw',
                   'plant_effective_capacity_per_wellset_mw',
                   'peaking_boilers_nameplate_capacity_per_wellset_mw',
                   'peaking_boilers_effective_capacity_per_wellset_mw',
                   'total_effective_capacity_per_wellset_mw',
                   'total_nameplate_capacity_per_wellset_mw',
                   'upfront_costs_per_wellset_dlrs', 
                   'annual_costs_per_wellset_dlrs', 
                   'plant_capacity_factor',
                   'peaking_boiler_capacity_factor',
                   'total_blended_capacity_factor',
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


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def build_supply_curves():
    
    # TODO: replace with actual function from Ben
    dataframe = pd.DataFrame()
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def build_demand_curves(agents_df):
    
    # TODO: replace with actual function from Ben
    dataframe = agents_df[['tract_id_alias']]
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_plant_sizes_econ(demand_curves_df, supply_curves_df):

    # TODO: replace with actual function from Ben    
    dataframe = demand_curves_df.copy()
    dataframe['plant_size_econ_mw'] = 5.
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_plant_sizes_market(demand_curves_df, supply_curves_df):

    # TODO: replace with actual function from Ben    
    dataframe = demand_curves_df.copy()
    dataframe['plant_size_market_mw'] = 5. * 0.25
    
    return dataframe

    
#%%