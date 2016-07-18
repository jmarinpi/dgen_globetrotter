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
        create_resource_id_sequence(schema, con, cur)
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
def create_resource_id_sequence(schema, con, cur):
    
    msg = '\tCreating Sequence for Resource IDs'    
    logger.info(msg)
    
    
    inputs = locals().copy()
     # create a sequence that will be used to populate a new primary key across all table partitions
    # using a sequence ensure ids will be unique across all partitioned tables
    sql = """DROP SEQUENCE IF EXISTS %(schema)s.resource_id_sequence;
            CREATE SEQUENCE %(schema)s.resource_id_sequence
            INCREMENT 1
            START 1;""" % inputs
    cur.execute(sql)
    con.commit()   
    
    
    
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
                  resource_uid INTEGER,
                  year INTEGER,
                  tract_id_alias INTEGER,
                  source_resource_id TEXT,
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
                	SELECT nextval('%(schema)s.resource_id_sequence') as resource_uid,
                         a.tract_id_alias, 
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
                	SELECT resource_uid, tract_id_alias, gid, area_sqkm,
                		depth_km, thickness_km,
                		case when t_deg_c_est > 150 or t_deg_c_est < 30 then 0 -- bound temps between 30 and 150
                		     else t_deg_c_est
                		end as res_temp_deg_c,
                		area_sqkm * thickness_km as volume_km3
                	FROM a
            ),
            c as
            (
                	SELECT b.resource_uid,
                         c.year,
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
            SELECT c.resource_uid,
                    c.year,
                	c.tract_id_alias,
                 c.gid::TEXT as source_resource_id,
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

    # add primary key
    sql = """ALTER TABLE %(schema)s.resources_egs_hdr 
             ADD PRIMARY KEY (resource_uid, year);""" % inputs
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
                  resource_uid INTEGER,
                  tract_id_alias INTEGER,
                  source_resource_id VARCHAR(5),
                  resource_type TEXT,
                  system_type TEXT,
                  depth_m INTEGER,
                  n_wellsets_in_tract INTEGER,
                  extractable_resource_per_wellset_in_tract_mwh NUMERIC
             );""" % inputs
    cur.execute(sql)
    con.commit()
    
    sql = """INSERT INTO %(schema)s.resources_hydrothermal
            SELECT nextval('%(schema)s.resource_id_sequence') as resource_uid,
                    a.tract_id_alias,
                	a.resource_uid as source_resource_id,
                	a.resource_type,
                	 a.system_type,
                		  round(
                			diffusion_shared.r_runif(a.min_depth_m, 
                                                        a.max_depth_m, 
                                                        1, 
                                                        %(seed)s * a.tract_id_alias),
                			0)::INTEGER as depth_m,
                	a.n_wells_in_tract as n_wellsets_in_tract,
                	a.extractable_resource_per_well_in_tract_mwh as extractable_resource_per_wellset_in_tract_mwh
            FROM  %(schema)s.du_resources_hydrothermal a
            WHERE a.tract_id_alias IN (%(chunk_place_holder)s);""" % inputs
    agent_prep.p_run(pg_conn_string, sql, chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.resources_hydrothermal 
             ADD PRIMARY KEY (resource_uid);""" % inputs
    cur.execute(sql)
    con.commit()


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_resource_data(con, schema, year):
    
    inputs = locals().copy()
        
    sql = """SELECT a.resource_uid,
                    a.tract_id_alias,
                    a.source_resource_id,
                    a.resource_type,
                    a.system_type,
                    a.depth_m,
                    a.n_wellsets_in_tract,
                    a.extractable_resource_per_wellset_in_tract_mwh as lifetime_resource_per_wellset_mwh
             FROM %(schema)s.resources_hydrothermal a
             WHERE a.extractable_resource_per_wellset_in_tract_mwh > 0
             
             UNION ALL
             
             SELECT b.resource_uid,
                    b.tract_id_alias,
                    b.source_resource_id,
                    b.resource_type,
                    b.system_type,
                    b.depth_m,
                    b.n_wellsets_in_tract,
                    b.extractable_resource_per_wellset_in_tract_mwh as lifetime_resource_per_wellset_mwh
             FROM %(schema)s.resources_egs_hdr b
             WHERE b.year = %(year)s
             AND b.extractable_resource_per_wellset_in_tract_mwh > 0;""" % inputs
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
                    year integer,
                    heat_demand_profile_mw NUMERIC[]
                 );""" % inputs
        cur.execute(sql)
        con.commit()       
        
        sql = """INSERT INTO %(schema)s.tract_aggregate_heat_demand_profiles
                 WITH com as
                 (
                     SELECT a.tract_id_alias,
                             a.year,
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
                             a.year,
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
                     SELECT tract_id_alias, year,
                            diffusion_shared.r_scale_array_sum(nkwh, total_heat_kwh_in_bin/1000.) as mwh
                     FROM combined             
                 ),
                 yearly_increments AS
                 (
                     SELECT tract_id_alias, year, 
                         diffusion_shared.r_sum_arrays(array_agg_mult(ARRAY[mwh])) as mwh
                     FROM scaled
                     GROUP BY tract_id_alias, year    
                 )
                 -- calculate cumulative sums for each year
                 SELECT tract_id_alias,
                         year,
                         diffusion_shared.r_sum_arrays(
                             array_agg_mult(ARRAY[mwh]) OVER (PARTITION BY tract_id_alias ORDER BY year ASC) 
                             ) AS heat_demand_profile_mw
                 FROM yearly_increments;""" % inputs
        
        agent_prep.p_run(pg_conn_string, sql, chunks, pool)        


        # add index on year
        sql = """CREATE INDEX tract_aggregate_heat_demand_profiles_btree_year
                 ON %(schema)s.tract_aggregate_heat_demand_profiles
                 USING BTREE(year);""" % inputs
        cur.execute(sql)
        con.commit()
        
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
def get_tract_demand_profiles(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT tract_id_alias, heat_demand_profile_mw
            FROM %(schema)s.tract_aggregate_heat_demand_profiles
            WHERE year = %(year)s;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df 
 

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_tract_peak_demand(tract_demand_profiles_df):
    
    # convert profiles to a numpy array
    profiles_array = np.array(tract_demand_profiles_df['heat_demand_profile_mw'].tolist(), dtype = 'float64')
    dataframe = tract_demand_profiles_df[['tract_id_alias']].copy()
    
    # calculate max for each row
    dataframe['peak_heat_demand_mw'] = np.max(profiles_array, axis = 1)
    
    # subset the return cols
    return_cols = ['tract_id_alias', 'peak_heat_demand_mw']
    dataframe = dataframe[return_cols]
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def filter_year(dataframe, year):
    
    return dataframe[dataframe['year'] == year]


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
def get_distribution_network_data(con, schema):

    inputs = locals().copy()
    # these two values come from Reber's thesis
    inputs['road_scalar'] = 0.75
    inputs['max_m_per_sqkm'] = 7500.
    
    sql = """WITH a AS
            (
                SELECT a.tract_id_alias,
                    a.road_meters * %(road_scalar)s as road_meters,
                    c.aland_sqm/1000./1000. * %(max_m_per_sqkm)s as max_distribution_m
                FROM diffusion_geo.tract_road_length a
                INNER JOIN %(schema)s.tracts_to_model b
                    ON a.tract_id_alias = b.tract_id_alias
                LEFT JOIN diffusion_blocks.tract_geoms c
                    ON a.tract_id_alias = c.tract_id_alias
            )
            SELECT tract_id_alias,
                   CASE WHEN road_meters > max_distribution_m THEN max_distribution_m
                   ELSE road_meters
                   END as distribution_total_m
            from a;""" % inputs

    df = pd.read_sql(sql, con, coerce_float = False)

    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_distribution_demand_density(tract_peak_demand_df, distribution_df):
    
    dataframe = pd.merge(tract_peak_demand_df, distribution_df, how = 'left', on = 'tract_id_alias')
    dataframe['distribution_m_per_mw'] = dataframe['distribution_total_m'] / dataframe['peak_heat_demand_mw']
    
    return_cols = ['tract_id_alias', 'distribution_total_m', 'distribution_m_per_mw']
    dataframe = dataframe[return_cols]
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_cost_and_performance_data(resource_df, costs_and_performance_df, reservoir_factors_df, plant_finances_df,
                                    demand_density_df, capacity_factors_df, ng_prices_df):
    
    inputs = locals().copy()
    
    # merge resources with reservoir factors (left join on resource_type (egs or hydrothermal))
    dataframe = pd.merge(resource_df, reservoir_factors_df, how = 'left', on = ['resource_type'])
    # merge the costs and performance
    dataframe = pd.merge(dataframe, costs_and_performance_df, how = 'left', on = ['tract_id_alias'])
    # merge the finances (effecively a cross join)
    dataframe = pd.merge(dataframe, plant_finances_df, how = 'left', on = ['year'])
    # merge the distribution demand density info (left join on tract id alias)
    dataframe = pd.merge(dataframe, demand_density_df, how = 'left', on = ['tract_id_alias'])
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
    
    # calculate the total energy that will actually be used from each wellset based on nameplate capacity and capacity factor
    dataframe['total_consumable_energy_per_wellset_mwh'] = dataframe['total_nameplate_capacity_per_wellset_mw'] * dataframe['total_blended_capacity_factor'] * 8760.
    
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
    # use nameplate peak demand of entire plant (including boilers) (because losses occur at end use sites)
    # don't use nameplate because distribution_network_construction_costs_dollars_per_m is based on actual demand
    dataframe['distribution_m_per_wellset'] = dataframe['distribution_m_per_mw'] * dataframe['total_nameplate_capacity_per_wellset_mw']
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

    out_cols = [   'year',
                   'tract_id_alias',
                   'resource_uid',
                   'resource_type',
                   'depth_m',
                   'system_type',
                   'n_wellsets_in_tract',
                   'lifetime_resource_per_wellset_mwh',
                   'total_consumable_energy_per_wellset_mwh',
                   'plant_nameplate_capacity_per_wellset_mw',
                   'plant_effective_capacity_per_wellset_mw',
                   'peaking_boilers_nameplate_capacity_per_wellset_mw',
                   'peaking_boilers_effective_capacity_per_wellset_mw',
                   'total_effective_capacity_per_wellset_mw',
                   'total_nameplate_capacity_per_wellset_mw',
                   'upfront_costs_per_wellset_dlrs', 
                   'annual_costs_per_wellset_dlrs', 
                   
                   'plant_installation_costs_per_wellset_dlrs',
                   'exploration_total_costs_per_wellset_dlrs',
                   'drilling_cost_per_wellset_dlrs',
                   'distribution_network_construction_costs_per_wellset_dlrs',
                   'distribution_m_per_wellset',
                   'peaking_boilers_construction_cost_per_wellset_dlrs',
                   'reservoir_pumping_gallons_per_year',
                   'operating_costs_reservoir_pumping_costs_per_wellset_per_year_dlrs',
                   'distribution_pumping_gallons_per_year',                   
                   'operating_costs_distribution_pumping_costs_per_wellset_per_year_dlrs',
                   'reservoir_stimulation_costs_per_wellset_dlrs',
                   'om_labor_costs_per_wellset_per_year_dlrs',
                   'om_plant_costs_per_wellset_per_year_dlrs',
                   'om_well_costs_per_wellset_per_year_dlrs',
                   
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
def calc_plant_sizes_market(demand_curves_df, supply_curves_df, plant_sizes_economic_df):

    # TODO: replace with actual function from Ben    
    dataframe = plant_sizes_economic_df.copy()
    dataframe['plant_size_market_mw'] = dataframe['capacity_mw'] * 1.
    
    return dataframe


#%%
def calc_npv(cfs, dr):
    ''' Vectorized NPV calculation based on (m x n) cashflows and (n x 1) 
    discount rate
    
    IN: cfs - numpy array - project cash flows ($/yr)
        dr  - numpy array - annual discount rate (decimal)
        
    OUT: npv - numpy array - net present value of cash flows ($) 
    
    '''
    dr = dr[:,np.newaxis]
    tmp = np.empty(cfs.shape)
    tmp[:,0] = 1
    tmp[:,1:] = 1/(1+dr)
    drm = np.cumprod(tmp, axis = 1)        
    npv = (drm * cfs).sum(axis = 1)   
    
    return npv
    
 
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_plant_lcoe(resources_with_costs_df, plant_depreciation_df, plant_construction_finance_factor):
    ''' LCOE calculation, following ATB assumptions. There will be some small differences
    since the model is already in real terms and doesn't need conversion of nominal terms
    
    IN: df
        deprec schedule
        inflation rate
        econ life -- investment horizon, may be different than system lifetime.
    
    OUT: lcoe - numpy array - Levelized cost of energy (c/kWh) 
    '''
    
    # extract a list of the input columns
    in_cols = resources_with_costs_df.columns.tolist()

    # convert depreciation schedule to an array
    depreciation_schedule = plant_depreciation_df['depreciation_fraction'].values    
        
    resources_with_costs_df['WACC'] = ((1 + ((1-resources_with_costs_df['debt_fraction'])*((1+resources_with_costs_df['rate_of_return_on_equity'])*(1+resources_with_costs_df['inflation_rate'])-1)) + (resources_with_costs_df['debt_fraction'] * ((1+resources_with_costs_df['interest_rate_nominal'])*(1+resources_with_costs_df['inflation_rate']) - 1) *  (1 - resources_with_costs_df['tax_rate'])))/(1 + resources_with_costs_df['inflation_rate'])) -1
    resources_with_costs_df['CRF'] = (resources_with_costs_df['WACC'])/ (1 - (1/(1+resources_with_costs_df['WACC'])**resources_with_costs_df['plant_lifetime_yrs']))# real crf
    
    depreciation_schedule = depreciation_schedule[np.newaxis,:] * np.ones((resources_with_costs_df.shape[0],depreciation_schedule.shape[0]))
    resources_with_costs_df['PVD'] = calc_npv(depreciation_schedule,((1+resources_with_costs_df['WACC'] * 1+ resources_with_costs_df['inflation_rate'])-1)) # Discount rate used for depreciation is 1 - (WACC + 1)(Inflation + 1)
    resources_with_costs_df['PVD'] /= (1 + resources_with_costs_df['WACC']) # In calc_npv we assume first entry of an array corresponds to year zero; the first entry of the depreciation schedule is for the first year, so we need to discount the PVD by one additional year
    
    resources_with_costs_df['PFF'] = (1 - resources_with_costs_df['tax_rate'] * resources_with_costs_df['PVD'])/(1 - resources_with_costs_df['tax_rate'])#project finance factor
    # Construction finance factor -- passed as input now since coding it directly will take a lot of time for little benefit
    resources_with_costs_df['CFF'] = plant_construction_finance_factor # construction finance factor -- cost of capital during construction, assume projects are built overnight, which is not true for larger systems   
    # Assume all wellsets for given resource are identical?
    resources_with_costs_df['OCC'] = resources_with_costs_df['upfront_costs_per_wellset_dlrs']/resources_with_costs_df['total_nameplate_capacity_per_wellset_mw'] # Overnight capital cost $/MW
    resources_with_costs_df['GCC'] = 0 # grid connection cost $/MW, assume cost of interconnecting included in OCC
    # Take the mean annual costs per wellset as the FOM
    resources_with_costs_df['FOM'] = resources_with_costs_df['annual_costs_per_wellset_dlrs'].apply(np.mean, axis = 0)/resources_with_costs_df['total_nameplate_capacity_per_wellset_mw'] # fixed o&m $/MW-yr

    resources_with_costs_df['lcoe_dlrs_mwh'] = ((resources_with_costs_df['CRF'] * resources_with_costs_df['PFF'] * resources_with_costs_df['CFF'] * (resources_with_costs_df['OCC'] * 1 + resources_with_costs_df['GCC']) + resources_with_costs_df['FOM'])/(resources_with_costs_df['total_blended_capacity_factor'] * 8760))# LCOE 2014$/MWh
    
    out_cols = ['lcoe_dlrs_mwh']
    return_cols = in_cols + out_cols
    
    resources_with_costs_df = resources_with_costs_df[return_cols]

    return resources_with_costs_df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def lcoe_to_supply_curve(resources_with_costs_df):


    replicate_indices = np.repeat(resources_with_costs_df.index.values, resources_with_costs_df['n_wellsets_in_tract'].astype('int64'))
    out_cols = ['tract_id_alias',
                'resource_uid',
                'lcoe_dlrs_mwh',
                'total_nameplate_capacity_per_wellset_mw',
                'total_consumable_energy_per_wellset_mwh'
                ]
    # replicate each row in the source df for the number of wellsets associated with it
    # also subsetting to the columns of interest
    supply_curve_df = resources_with_costs_df.loc[replicate_indices, out_cols]
    
    # check the shape -- nrows should equal sum of n_wellsets in tract
    if supply_curve_df.shape[0] <> resources_with_costs_df['n_wellsets_in_tract'].sum():
        raise ValueError("Number of rows in supply_curve_df is not equal to the total number of all wellsets")
        
    # rename a couple of columns
    rename_map = {'total_nameplate_capacity_per_wellset_mw' : 'capacity_mw',
                  'total_consumable_energy_per_wellset_mwh' : 'energy_mwh'}
    supply_curve_df = supply_curve_df.rename(columns = rename_map)             

    return supply_curve_df
    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_agent_lcoe(dataframe, plant_lifetime, ignore_sunk_cost_of_existing_equipment = True, apply_interconnection_costs_to_demand = True):

    # ignore_sunk_cost_of_existing_equipment = Do We want to understand either the marginal cost of heating, or that and the sunk costs of existing equipment?

    
    # extract a list of the input columns
    in_cols = dataframe.columns.tolist()       

    dataframe['total_heat_mwh_per_building_in_bin'] = dataframe['total_heat_kwh_per_building_in_bin']/1000.     
    
    # As a standing assumption we'll assume the DU system provides 100% or 0% of the energy needed. It can only supply one source of energy demand
    # Thus, we calculate cost of energy, weighted by the amount needed by end-use. This is the price the consumer uses to evaluate investment against the supply LCOE
    if ignore_sunk_cost_of_existing_equipment == True:
        dataframe['weighted_cost_of_energy_dlrs_per_mwh'] = np.where(dataframe['total_heat_mwh_per_building_in_bin'] == 0, 0,
                                                                     ( dataframe['space_heat_dlrs_per_kwh'] * dataframe['space_heat_kwh_per_building_in_bin'] +
                                                                         dataframe['water_heat_dlrs_per_kwh'] * dataframe['water_heat_kwh_per_building_in_bin']
                                                                         ) / dataframe['total_heat_mwh_per_building_in_bin'])
        
    else:
        raise ValueError("Functionality for calculating demand curve including sunk costs does not yet exist")

    
    if apply_interconnection_costs_to_demand == True:
        dataframe['system_installation_costs_dlrs_per_sf'] = np.where(dataframe['new_construction'] == True, dataframe['new_sys_installation_costs_dollars_sf'], dataframe['new_sys_installation_costs_dollars_sf'] * dataframe['retrofit_new_sys_installation_cost_multiplier'])
        dataframe['system_installation_costs_dlrs'] = dataframe['system_installation_costs_dlrs_per_sf'] * dataframe['totsqft_heat']
        dataframe['upfront_costs_dlrs'] = dataframe['system_installation_costs_dlrs'] + dataframe['sys_connection_cost_dollars']
        dataframe['levelized_upfront_costs_dlrs_per_yr'] = dataframe['upfront_costs_dlrs'] / plant_lifetime
        dataframe['fixed_om_costs_dollars_per_yr'] = dataframe['fixed_om_costs_dollars_sf_yr'] * dataframe['totsqft_heat']
        dataframe['annual_costs_dlrs'] = dataframe['fixed_om_costs_dollars_per_yr'] + dataframe['levelized_upfront_costs_dlrs_per_yr']
        dataframe['annual_costs_dlrs_per_mwh'] = dataframe['annual_costs_dlrs'] / dataframe['total_heat_mwh_per_building_in_bin']
    else: 
        dataframe['annual_costs_dlrs_per_mwh'] = 0.
    
    
    dataframe['lcoe_dlrs_mwh'] = np.maximum(dataframe['weighted_cost_of_energy_dlrs_per_mwh'] - dataframe['annual_costs_dlrs_per_mwh'], 0)
    
    out_cols = ['total_heat_mwh_per_building_in_bin', 
                'weighted_cost_of_energy_dlrs_per_mwh',
                'system_installation_costs_dlrs',
                'upfront_costs_dlrs',
                'levelized_upfront_costs_dlrs_per_yr',
                'fixed_om_costs_dollars_per_yr',
                'annual_costs_dlrs_per_mwh',
                'lcoe_dlrs_mwh']
    return_cols = in_cols + out_cols
    
    dataframe = dataframe[return_cols]
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def lcoe_to_demand_curve(dataframe, building_set_size = 10):

    # every group of building_set_size buildings will be represented by one agent
    dataframe['replicate_count'] = np.maximum(np.round(dataframe['buildings_in_bin']/building_set_size, 0), 1).astype('int64')
    dataframe['buildings_in_replicate'] = dataframe['buildings_in_bin'] / dataframe['replicate_count']
    dataframe['energy_mwh_per_replicate'] = dataframe['total_heat_mwh_per_building_in_bin'] * dataframe['buildings_in_replicate']
    replicate_indices = np.repeat(dataframe.index.values, dataframe['replicate_count'])
    out_cols = ['tract_id_alias',
                'agent_id',
                'buildings_in_replicate',
                'lcoe_dlrs_mwh',
                'energy_mwh_per_replicate'
                ]
    # replicate each row in the source df for the number of wellsets associated with it
    # also subsetting to the columns of interest
    demand_curve_df = dataframe.loc[replicate_indices, out_cols]
    
    # check the shape -- nrows should equal sum of replicate_count
    if demand_curve_df.shape[0] <> dataframe['replicate_count'].sum():
        raise ValueError("Number of rows in demand_curve_df is not equal to the total sum of replicate_count")
        
    # rename energy column
    # rename a couple of columns
    rename_map = {'energy_mwh_per_replicate' : 'energy_mwh'}
    demand_curve_df = demand_curve_df.rename(columns = rename_map)             

    # add a capacity field (We only care about capacity for the supply curve, but need it here for consistency)
    demand_curve_df['capacity_mw'] = 0

    return demand_curve_df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def intersect_supply_demand_curves(demand_curves_df, supply_curves_df):
    
    # We need to determine intersection by tract. While this can probably be vectorized, for now it is not
    # Declare an empty dataframe for settling price and quantities by tract
    settling_pq_values = pd.DataFrame({'tract_id_alias':[],
                                       'capacity_mw':[],
                                       'energy_mwh':[],
                                       'lcoe_dlrs_mwh':[]})
    
    for tract in supply_curves_df['tract_id_alias'].unique():
        cols = ['tract_id_alias', 'capacity_mw', 'energy_mwh', 'lcoe_dlrs_mwh']
        supply_curve = supply_curves_df[(supply_curves_df['tract_id_alias'] == tract)][cols].copy()
        demand_curve = demand_curves_df[(demand_curves_df['tract_id_alias'] == tract)][cols].copy()
        

            
        # Three Primary Outcomes
        if supply_curve['lcoe_dlrs_mwh'].min() > demand_curve['lcoe_dlrs_mwh'].max(): 
            #All supply prices are higher price than demand: build no capacity
            settling_price = {'tract_id_alias' : tract,
                              'capacity_mw' : 0,
                              'energy_mwh' : 0,
                              'lcoe_dlrs_mwh' : 1e99}
                                       
        elif supply_curve['lcoe_dlrs_mwh'].max() < demand_curve['lcoe_dlrs_mwh'].min(): 
            #All supply prices are lower price than demand: build all capacity
            settling_price = supply_curve.iloc[-1][cols].to_dict() # Take the last row, which should be the largest system size
            
        else: # There is at least one settling price. 
            supply_curve.sort('lcoe_dlrs_mwh', inplace = True)
            supply_curve['capacity_mw'] = supply_curve['capacity_mw'].cumsum()
            supply_curve['energy_mwh'] = supply_curve['energy_mwh'].cumsum()
            supply_curve['metric'] = 'supply'
    
            demand_curve.sort('lcoe_dlrs_mwh', ascending = False, inplace = True)
            demand_curve['capacity_mw'] = demand_curve['capacity_mw'].cumsum()
            demand_curve['energy_mwh'] = demand_curve['energy_mwh'].cumsum()
            demand_curve['metric'] = 'demand'
            
            # Check if curves are monotonic-- I think its impossible since we first sort by LCOE, then do cum.sum on energy
            supply_vals = supply_curve['energy_mwh'].diff().values[1:]
            demand_vals = supply_curve['energy_mwh'].diff().values[1:]
            if np.any(supply_vals<0):
                raise ValueError("""WARNING: Supply curve is not monotonic""")
                
            if np.any(demand_vals<0):
                raise ValueError("""WARNING: Demand curve is not monotonic""")            
            
            
            # Sort the concatenated supply and demand curves by quantity. 
            # Take the row-wise difference in price, and find the first supply index where the difference is negative
            # Since we have proven monotonicity above and that an intersection occurs, we are searching for the first supply price-quanity pair
            # for which the demand price is less than the supply price 
            combined = pd.concat([supply_curve,demand_curve], axis = 0, ignore_index = True)
            combined = combined.sort('energy_mwh')
            combined['diff'] = combined['lcoe_dlrs_mwh'].diff().shift(-1) # We want the leading difference e.g. the first entry(0th) should be df[1,'lcoe'] - df[0,'lcoe']
            combined = combined.reset_index(drop = True)
            
            # Add replace 'NA' last entry with a dummy value
            combined['diff'].iloc[-1] = 1e99
            settling_price = combined.loc[(combined['diff'] <0) & (combined['metric'] == 'supply')][cols]
            
            #Two other edge cases: all demand is satisfied at a consumer surplus: build all capacity up to demand total
            #                      all available supply could satisfy demand at a surplus
            
            if settling_price.shape[0] == 0:
                max_demand = combined[combined['metric'] == 'demand'].max().energy_mwh
                max_supply = combined[combined['metric'] == 'supply'].max().energy_mwh
                
                if max_demand > max_supply:
                    settling_price = combined[(combined['energy_mwh'] == max_demand)].iloc[0][cols]
                elif max_demand <= max_supply:
                    settling_price = combined[(combined['metric'] == 'supply') & (combined['energy_mwh'] > max_demand)].iloc[0][cols]
                    #Take the first supply that's greater than the max demand 
            else:
                settling_price = settling_price.iloc[0][cols]

            #settling_price.drop(['metric','diff'], inplace = True, axis = 1)


        settling_pq_values = settling_pq_values.append(settling_price, ignore_index = True)
    
    
    return settling_pq_values
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_previously_subscribed_agents(con, schema):
    
    inputs = locals().copy()
    sql = """SELECT agent_id, 
                    sum(new_adopters) as previously_subscribed_buildings
                    FROM %(schema)s.agent_outputs_du
                    WHERE new_adopters > 0
                    GROUP BY agent_id;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df                    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_previously_subscribed_wellsets(con, schema):
    
    inputs = locals().copy()
    sql = """SELECT resource_uid, 
                    sum(subscribed_wellsets) as previously_subscribed_wellsets
            FROM %(schema)s.resource_outputs_du
            WHERE subscribed_wellsets > 0
            GROUP BY resource_uid;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df                    


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def subtract_previously_subscribed_agents(dataframe, previously_subscribed_agents_df):
    
    in_cols = dataframe.columns.tolist()
    dataframe = pd.merge(dataframe, previously_subscribed_agents_df, how = 'left', on = 'agent_id')
    # fill nas in previously_subscribed_buildings
    dataframe['previously_subscribed_buildings'] = dataframe['previously_subscribed_buildings'].fillna(0)
    # subtract from buildings in bin
    dataframe.loc[:, 'buildings_in_bin'] = dataframe['buildings_in_bin'] - dataframe['previously_subscribed_buildings']    
    
    return_cols = in_cols
    dataframe = dataframe[return_cols]
    
    return dataframe
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def subtract_previously_subscribed_wellsets(resource_df, previously_subscribed_wellsets_df):
    
    in_cols = resource_df.columns.tolist()
    resource_df = pd.merge(resource_df, previously_subscribed_wellsets_df, how = 'left', on = 'resource_uid')
    # fill nas in previously_subscribed wellsets
    resource_df['previously_subscribed_wellsets'] = resource_df['previously_subscribed_wellsets'].fillna(0)
    # subtract from n_wellsets_in_tract
    resource_df.loc[:, 'n_wellsets_in_tract'] = (resource_df['n_wellsets_in_tract'] - resource_df['previously_subscribed_wellsets']).astype('int64')
    # drop any wellsets with zero remaining welslets
    resource_df = resource_df[resource_df['n_wellsets_in_tract'] > 0]
    
    return_cols = in_cols
    resource_df = resource_df[return_cols]
    
    return resource_df
 