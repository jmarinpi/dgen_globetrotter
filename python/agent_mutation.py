# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 11:35:14 2016

@author: mgleason
"""

import psycopg2 as pg
import numpy as np
import pandas as pd
import logging
reload(logging)
import decorators
from config import show_times
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
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_load_growth(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT sector_abbr, census_division_abbr, load_multiplier
            FROM %(schema)s.load_growth_to_model
            WHERE year = %(year)s;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df    


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_load_growth(dataframe, load_growth_df):
    
        dataframe = pd.merge(dataframe, load_growth_df, how = 'left', on = ['sector_abbr', 'census_division_abbr'])
        dataframe['customers_in_bin'] = dataframe['customers_in_bin'] * dataframe['load_multiplier']
        dataframe['load_kwh_in_bin'] = dataframe['load_kwh_in_bin'] * dataframe['load_multiplier']
        
        return dataframe


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def calculate_developable_customers_and_load(dataframe):

    dataframe['developable_customers_in_bin'] = np.where(dataframe['tech'] == 'solar', 
                                                         dataframe['pct_of_bldgs_developable'] * dataframe['customers_in_bin'],
                                                         np.where(dataframe['system_size_kw'] == 0, 
                                                                  0,
                                                                  dataframe['customers_in_bin']))
                                                        
    dataframe['developable_load_kwh_in_bin'] = np.where(dataframe['tech'] == 'solar', 
                                                        dataframe['pct_of_bldgs_developable'] * dataframe['load_kwh_in_bin'], 
                                                        np.where(dataframe['system_size_kw'] == 0, 
                                                                 0,
                                                                 dataframe['load_kwh_in_bin']))    
                                                            
    return dataframe
             

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_rate_structures(con, schema):
    
    inputs = locals().copy()
    
    sql = """
            	SELECT 'res' as sector_abbr, res_rate_structure as rate_structure
        	FROM %(schema)s.input_main_scenario_options
        	UNION
        	SELECT 'com' as sector_abbr, com_rate_structure as rate_structure
        	FROM %(schema)s.input_main_scenario_options
        	UNION
        	SELECT 'ind' as sector_abbr, ind_rate_structure as rate_structure
        	FROM %(schema)s.input_main_scenario_options;""" % inputs
    
    rate_structures_df = pd.read_sql(sql, con)
    rate_structures = dict(zip(rate_structures_df['sector_abbr'], rate_structures_df['rate_structure']))
    
    return rate_structures    


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_electric_rates(cur, con, schema, sectors, seed, pg_conn_string):

    # NOTE: This function creates a lookup table for the agents in each sector, providing
    #       the county_id and bin_id for each agent, along with the rate_id_alias and rate_source.
    #       This information is used in "get_electric_rate_tariffs" to load in the actual rate tariff for each agent.

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'
    excluded_rates = pd.read_csv('./excluded_rates_ids.csv', header=None)
    inputs['excluded_rate_ids'] = '(' + ', '.join([str(i[0]) for i in excluded_rates.values]) + ')'


    msg = "\tGenerating Electric Rate Tariff Lookup Table for Agents"
    logger.info(msg)
    
    # determine which type of rate to use for each sector
    rate_structures = get_rate_structures(con, schema)
    
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        rate_structure = rate_structures[sector_abbr]

        if rate_structure.lower() == 'complex rates':
    
            sql = """DROP TABLE IF EXISTS %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s;
                    CREATE UNLOGGED TABLE %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s AS
                    WITH a AS
                    (
                        	SELECT a.county_id, a.bin_id, a.state_abbr, a.max_demand_kw,
                        		unnest(b.rate_ids) as rate_id_alias,
                        		unnest(b.rate_ranks) as rate_rank
                        	FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                        	LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                                	ON a.pgid = b.pgid
                    ),
                    b AS
                    (
                            SELECT a.*,
                                    b.rate_type,
                                    b.pct_of_customers
                            FROM a 
                            LEFT JOIN diffusion_shared.urdb_rates_by_state_%(sector_abbr)s b
                                    ON a.rate_id_alias = b.rate_id_alias
                                    AND a.state_abbr = b.state_abbr
                            WHERE a.max_demand_kw <= b.urdb_demand_max
                                  AND a.max_demand_kw >= b.urdb_demand_min
                                  AND b.rate_id_alias NOT IN %(excluded_rate_ids)s
                    ),
                    c as
                    (
                        	SELECT *, rank() OVER (PARTITION BY county_id, bin_id ORDER BY rate_rank ASC) as rank
                        	FROM b
                    ), 
                    d as
                    (
                        SELECT c.*, COALESCE(c.pct_of_customers, d.%(sector_abbr)s_weight) as rate_type_weight
                        FROM c 
                        LEFT JOIN %(schema)s.input_main_market_rate_type_weights d
                        ON c.rate_type = d.rate_type
                        WHERE c.rank = 1
                    )
                    SELECT d.county_id, d.bin_id,
                            unnest(diffusion_shared.sample(
                                            array_agg(d.rate_id_alias ORDER BY d.rate_id_alias), 
                                            1, 
                                          (%(seed)s * d.county_id * d.bin_id), 
                                          False,
                                          array_agg(d.rate_type_weight ORDER BY d.rate_id_alias))) as rate_id_alias,
                            'urdb3'::CHARACTER VARYING(5) as rate_source
                    FROM d
                    GROUP BY d.county_id, d.bin_id;""" % inputs
            cur.execute(sql)
            con.commit()
    
        elif rate_structure.lower() == 'flat (annual average)':
            # flat annual average rate ids are already stored in the demandmax table as county_id
            # we simply need to duplicate and rename that field to rate_id_alias and specify the rate_source
            sql = """DROP TABLE IF EXISTS %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s;
                     CREATE UNLOGGED TABLE %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s AS
                     SELECT a.county_id, a.bin_id 
                            a.old_county_id as rate_id_alias, 
                          'aa%(sector_abbr)s'::CHARACTER VARYING(5) as rate_source
                    FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                    WHERE a.county_id in (%(chunk_place_holder)s);""" % inputs
            cur.execute(sql)
            con.commit()
                         
        elif rate_structure.lower() == 'flat (user-defined)':
            # user-defined rates are id'ed based on the state_fips, which is already stored in the demandmax table
            # we simply need to duplicate and rename that field to rate_id_alias and specify the rate_source
            sql = """DROP TABLE IF EXISTS %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s;
                    CREATE UNLOGGED TABLE %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s AS
                    SELECT a.county_id, a.bin_id 
                            a.state_fips::INTEGER as rate_id_alias, 
                            'ud%(sector_abbr)s'::CHARACTER VARYING(5) as rate_source
                    FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                    WHERE a.county_id in (%(chunk_place_holder)s);""" % inputs
            cur.execute(sql)
            con.commit()
    
        # add primary key to rates lkup table
        sql = """ALTER TABLE %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s
                 ADD PRIMARY KEY (county_id, bin_id);""" % inputs
        cur.execute(sql)
        con.commit()
        
        # get the rates
        sql = """SELECT a.county_id, a.bin_id, '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        b.sam_json as rate_json, a.rate_id_alias, a.rate_source 
               FROM  %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s a
               LEFT JOIN %(schema)s.all_rate_jsons b 
                   ON a.rate_id_alias = b.rate_id_alias
                   AND a.rate_source = b.rate_source;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    # combine the dfs
    df = pd.concat(df_list, axis = 0, ignore_index = True)

    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_net_metering_settings(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT state_abbr,
                    sector_abbr,
                    system_size_limit_kw as nem_system_size_limit_kw,
                    year_end_excess_sell_rate_dlrs_per_kwh as ur_nm_yearend_sell_rate,
                    hourly_excess_sell_rate_dlrs_per_kwh as ur_flat_sell_rate
             FROM %(schema)s.input_main_nem_scenario
             WHERE year = %(year)s;""" % inputs
             
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def select_electric_rates(dataframe, rates_df, net_metering_df):
    
    dataframe = pd.merge(dataframe, rates_df, how = 'left', on = ['county_id', 'bin_id', 'sector_abbr'])
    dataframe = pd.merge(dataframe, net_metering_df, how = 'left',  on = ['state_abbr', 'sector_abbr'])
                                                            
    return dataframe


#%%
def update_rate_json_w_nem_fields(row):
    
    nem_fields = ['ur_enable_net_metering', 'ur_nm_yearend_sell_rate', 'ur_flat_sell_rate']
    nem_dict = dict((k, row[k]) for k in nem_fields)
    row['rate_json'].update(nem_dict)
    
    return row
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def update_net_metering_fields(dataframe):
    
    dataframe = dataframe.apply(update_rate_json_w_nem_fields, axis = 1)    
    
    return dataframe

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def assemble_resource_data():
    
    # all in postgres
    # for wind, need to use %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s
    pass
    # for solar, not changes to columns:
        # available_roof_sqft --> developable_roof_sqft
        # pct_developable --> pct_of_bldgs_developable

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_core_agent_attributes(con, schema):
    
    inputs = locals().copy()
    sql = """SELECT *
             FROM %(schema)s.agent_core_attributes_all;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    agents = Agents(df)

    return agents

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_system_sizing_targets(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT 'solar'::VARCHAR(5) as tech, 
                     sector_abbr,
                     sys_size_target_nem,
                     sys_size_target_no_nem,
                     NULL::NUMERIC AS sys_oversize_limit_nem,
                     NULL::NUMERIC AS sys_oversize_limit_no_nem
             FROM %(schema)s.input_solar_performance_system_sizing_factors 
             
             UNION ALL
             
             SELECT 'wind'::VARCHAR(5) as tech, 
                     sector_abbr,
                     sys_size_target_nem,
                     sys_size_target_no_nem,
                     sys_oversize_limit_nem,
                     sys_oversize_limit_no_nem
             FROM %(schema)s.input_wind_performance_system_sizing_factors;""" % inputs

    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_technology_performance_solar(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT 'solar'::VARCHAR(5) as tech,
                    efficiency_improvement_factor as pv_efficiency_improvement_factor,
                    density_w_per_sqft as pv_density_w_per_sqft,
                    inverter_lifetime_yrs
             FROM %(schema)s.input_solar_performance_improvements
             WHERE year = %(year)s;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_technology_performance_wind(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT 'wind'::VARCHAR(5) as tech,
                    a.turbine_size_kw, 
                    a.derate_factor as wind_derate_factor
            FROM %(schema)s.input_wind_performance_gen_derate_factors a
            WHERE a.year = %(year)s
            
            UNION ALL
            
            SELECT 'wind'::VARCHAR(5) as tech,
                    0::NUMERIC as turbine_size_kw,
                    0::NUMERIC as wind_derate_factor;""" % inputs

    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_annual_resource_wind(con, schema, year, sectors):
    
    inputs = locals().copy()
    
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT 'wind'::VARCHAR(5) as tech,
                        '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        a.county_id, a.bin_id, 
                    	COALESCE(b.turbine_height_m, 0) as turbine_height_m,
                    	COALESCE(b.turbine_size_kw, 0) as turbine_size_kw,
                    	coalesce(c.interp_factor, 0) as power_curve_interp_factor,
                    	COALESCE(c.power_curve_1, -1) as power_curve_1,
                    	COALESCE(c.power_curve_2, -1) as power_curve_2,
                    	COALESCE(d.aep, 0) as naep_1,
                    	COALESCE(e.aep, 0) as naep_2
                FROM  %(schema)s.agent_core_attributes_%(sector_abbr)s a
                LEFT JOIN %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s b
                    	ON a.county_id = b.county_id
                    	and a.bin_id = b.bin_id
                LEFT JOIN %(schema)s.input_wind_performance_power_curve_transitions c
                    	ON b.turbine_size_kw = c.turbine_size_kw
                         AND c.year = %(year)s
                LEFT JOIN diffusion_resource_wind.wind_resource_annual d
                    	ON a.i = d.i
                    	AND a.j = d.j
                    	AND a.cf_bin = d.cf_bin
                    	AND b.turbine_height_m = d.height
                    	AND c.power_curve_1 = d.turbine_id
                LEFT JOIN diffusion_resource_wind.wind_resource_annual e
                    	ON a.i = e.i
                    	AND a.j = e.j
                    	AND a.cf_bin = e.cf_bin
                    	AND b.turbine_height_m = e.height
                    	AND c.power_curve_2 = e.turbine_id;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    df = pd.concat(df_list, axis = 0, ignore_index = True)
    
    return df

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_annual_resource_solar(con, schema, sectors):
    
    inputs = locals().copy()
    
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT 'solar'::VARCHAR(5) as tech,
                '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                a.county_id, a.bin_id,
                b.naep 
                FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                LEFT JOIN diffusion_resource_solar.solar_resource_annual b
                    ON a.solar_re_9809_gid = b.solar_re_9809_gid
                    AND a.tilt = b.tilt
                    AND a.azimuth = b.azimuth;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    df = pd.concat(df_list, axis = 0, ignore_index = True)
        
    return df
    
    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_technology_performance_solar(resource_solar_df, tech_performance_solar_df):
    
    resource_solar_df = pd.merge(resource_solar_df, tech_performance_solar_df, how = 'left', on = ['tech'])
    resource_solar_df['naep'] = resource_solar_df['naep'] * resource_solar_df['pv_efficiency_improvement_factor']
    
    return resource_solar_df

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_technology_performance_wind(resource_wind_df, tech_performance_wind_df):
    
    resource_wind_df = pd.merge(resource_wind_df, tech_performance_wind_df, how = 'left', on = ['tech', 'turbine_size_kw'])
    resource_wind_df['naep_1'] = resource_wind_df['naep_1'] * resource_wind_df['wind_derate_factor']
    resource_wind_df['naep_2'] = resource_wind_df['naep_2'] * resource_wind_df['wind_derate_factor']
    resource_wind_df['naep'] = resource_wind_df['power_curve_interp_factor'] * (resource_wind_df['naep_2'] - resource_wind_df['naep_1']) + resource_wind_df['naep_1']
    
    return resource_wind_df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def size_systems_wind(dataframe, system_sizing_targets_df, resource_df):
    
    in_cols = list(dataframe.columns)
          
    # join in system sizing targets df
    dataframe = pd.merge(dataframe, system_sizing_targets_df, how = 'left', on = ['sector_abbr', 'tech'])    
    
    # determine whether NEM is available in the state and sector
    dataframe['ur_enable_net_metering'] = dataframe['nem_system_size_limit_kw'] == 0
    
    # set the target kwh according to NEM availability
    dataframe['target_kwh'] = np.where(dataframe['ur_enable_net_metering'] == True, 
                                       dataframe['load_kwh_per_customer_in_bin'] * dataframe['sys_size_target_no_nem'],
                                       dataframe['load_kwh_per_customer_in_bin'] * dataframe['sys_size_target_nem'])
    # also set the oversize limit according to NEM availability
    dataframe['oversize_limit_kwh'] = np.where(dataframe['ur_enable_net_metering'] == True, 
                                       dataframe['load_kwh_per_customer_in_bin'] * dataframe['sys_oversize_limit_no_nem'],
                                       dataframe['load_kwh_per_customer_in_bin'] * dataframe['sys_oversize_limit_nem'])

    # join in the resource data
    dataframe = pd.merge(dataframe, resource_df, how = 'left', on = ['tech', 'sector_abbr', 'county_id', 'bin_id'])

    # calculate the system generation from naep and turbine_size_kw    
    dataframe['aep_kwh'] = dataframe['turbine_size_kw'] * dataframe['naep']

    # initialize values for score and n_units
    dataframe['score'] = np.absolute(dataframe['aep_kwh'] - dataframe['target_kwh'])
    dataframe['n_units'] = 1.
    
    # Handle Special Cases
    
    # Buildings requiring more electricity than can be generated by the largest turbine (1.5 MW)
    # Return very low rank score and the optimal continuous number of turbines
    big_projects = (dataframe['turbine_size_kw'] == 1500) & (dataframe['aep_kwh'] < dataframe['target_kwh'])
    dataframe.loc[big_projects, 'score'] = 0
    dataframe.loc[big_projects, 'n_units'] = np.minimum(4, dataframe['target_kwh'] / dataframe['aep_kwh']) 


    # identify oversized projects
    oversized_turbines = dataframe['aep_kwh'] > dataframe['oversize_limit_kwh']
    # also identify zero production turbines
    no_kwh = dataframe['aep_kwh'] == 0
    # where either condition is true, set a high score and zero turbines
    dataframe.loc[oversized_turbines | no_kwh, 'score'] = np.array([1e8]) + dataframe['turbine_size_kw'] * 100 + dataframe['turbine_height_m']
    dataframe.loc[oversized_turbines | no_kwh, 'n_units'] = 0.0
    # also disable net metering
    dataframe.loc[oversized_turbines | no_kwh, 'ur_enable_net_metering'] = False
    
    # check that the system is within the net metering size limit
    over_nem_limit = dataframe['turbine_size_kw'] > dataframe['nem_system_size_limit_kw']
    dataframe.loc[over_nem_limit, 'score'] = dataframe['score'] * 2
    dataframe.loc[over_nem_limit, 'ur_enable_net_metering'] = False

    # for each agent, find the optimal turbine
    dataframe['rank'] = dataframe.groupby(['county_id', 'bin_id', 'sector_abbr'])['score'].rank(ascending = True, method = 'first')
    dataframe_sized = dataframe[dataframe['rank'] == 1]
    # add in the system_size_kw field
    dataframe_sized['system_size_kw'] = dataframe_sized['turbine_size_kw'] * dataframe_sized['n_units']
    # recalculate the aep based on the system size (instead of plain turbine size)
    dataframe_sized['aep'] = dataframe_sized['system_size_kw'] * dataframe_sized['naep']
    
    # add dummy column for inverter lifetime 
    dataframe_sized['inverter_lifetime_yrs'] = np.nan
    dataframe_sized['inverter_lifetime_yrs'] = dataframe_sized['inverter_lifetime_yrs'].astype(np.float64)

    return_cols = ['ur_enable_net_metering', 'aep', 'system_size_kw', 'n_units', 'inverter_lifetime_yrs',
                   'turbine_height_m', 'turbine_size_kw', 'power_curve_1', 'power_curve_2', 'power_curve_interp_factor', 'wind_derate_factor']
    out_cols = list(pd.unique(in_cols + return_cols))
    
    dataframe_sized = dataframe_sized[out_cols]

    return dataframe_sized

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def size_systems_solar(dataframe, system_sizing_targets_df, resource_df, default_panel_size_sqft = 17.5):
    
    in_cols = list(dataframe.columns)  
    
    # join in system sizing targets df
    dataframe = pd.merge(dataframe, system_sizing_targets_df, how = 'left', on = ['sector_abbr', 'tech'])     
    
    # join in the resource data
    dataframe = pd.merge(dataframe, resource_df, how = 'left', on = ['tech', 'sector_abbr', 'county_id', 'bin_id'])
 
    dataframe['max_buildable_system_kw'] =  0.001 * dataframe['developable_roof_sqft'] * dataframe['pv_density_w_per_sqft']

    # initialize the system size targets
    dataframe['ideal_system_size_kw_no_nem'] = dataframe['load_kwh_per_customer_in_bin'] * dataframe['sys_size_target_no_nem']/dataframe['naep']
    dataframe['ideal_system_size_kw_nem'] = dataframe['load_kwh_per_customer_in_bin'] * dataframe['sys_size_target_nem']/dataframe['naep'] 
    
    # deal with special cases: no net metering, unlimited NEM, limited NEM
    no_net_metering = dataframe['nem_system_size_limit_kw'] == 0
    unlimited_net_metering = dataframe['nem_system_size_limit_kw'] == float('inf')
    dataframe['ideal_system_size_kw'] = np.where(no_net_metering, 
                                                dataframe['ideal_system_size_kw_no_nem'],
                                                np.where(unlimited_net_metering, 
                                                         dataframe['ideal_system_size_kw_nem'],
                                                         np.minimum(dataframe['ideal_system_size_kw_nem'], dataframe['nem_system_size_limit_kw']) # if limited NEM, maximize size up to the NEM limit
                                                         )
                                                )
    # change NEM enabled accordingly
    dataframe['ur_enable_net_metering'] = np.where(no_net_metering, False, True)
                                             
    # calculate the system size based on the target size and the availabile roof space
    dataframe['system_size_kw'] = np.round(np.minimum(dataframe['max_buildable_system_kw'], dataframe['ideal_system_size_kw']), 2)                      
    # derive the number of panels
    dataframe['n_units'] = dataframe['system_size_kw']/(0.001 * dataframe['pv_density_w_per_sqft'] * default_panel_size_sqft) # Denom is kW of a panel
    # calculate aep
    dataframe['aep'] = dataframe['system_size_kw'] * dataframe['naep']    

    # add in dummy columns for compatibility with wind
    for col in ['turbine_height_m', 'turbine_size_kw', 'power_curve_1', 'power_curve_2', 'power_curve_interp_factor', 'wind_derate_factor']:
        dataframe[col] = np.nan
        dataframe[col] = dataframe[col].astype(np.float64)


    return_cols = ['ur_enable_net_metering', 'aep', 'system_size_kw', 'n_units', 'inverter_lifetime_yrs',
                   'turbine_height_m', 'turbine_size_kw', 'power_curve_1', 'power_curve_2', 'power_curve_interp_factor', 'wind_derate_factor']
    out_cols = list(pd.unique(in_cols + return_cols))
    
    dataframe = dataframe[out_cols]

    return dataframe   


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_normalized_load_profiles(con, schema, sectors):
    
    inputs = locals().copy()
    
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        a.county_id, a.bin_id, 
                        b.nkwh as consumption_hourly,
                        1e8 as scale_offset
                 FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                 LEFT JOIN diffusion_load_profiles.energy_plus_normalized_load_%(sector_abbr)s b
                     ON a.crb_model = b.crb_model
                     AND a.hdf_load_index = b.hdf_index;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    df = pd.concat(df_list, axis = 0, ignore_index = True)
            
    return df


#%%
def scale_array_precision(row, array_col, prec_offset_col):
    
    row[array_col] = np.array(row[array_col], dtype = 'float64') / row[prec_offset_col]
    
    return row


#%%    
def scale_array_sum(row, array_col, scale_col):

    hourly_array = np.array(row[array_col], dtype = 'float64')
    row[array_col] = hourly_array/hourly_array.sum() * np.float64(row[scale_col])
    
    return row
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def scale_normalized_load_profiles(dataframe, load_df):
    
    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)
    # join the dataframe and load_df
    dataframe = pd.merge(dataframe, load_df, how  = 'left', on = ['county_id', 'bin_id', 'sector_abbr'])
    # apply the scale offset to convert values to float with correct precision
    dataframe = dataframe.apply(scale_array_precision, axis = 1, args = ('consumption_hourly', 'scale_offset'))
    # scale the normalized profile to sum to the total load
    dataframe = dataframe.apply(scale_array_sum, axis = 1, args = ('consumption_hourly', 'load_kwh_per_customer_in_bin'))    
    
    # subset to only the desired output columns
    out_cols = in_cols + ['consumption_hourly']
    
    dataframe = dataframe[out_cols]
    
    return dataframe
    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_normalized_hourly_resource_solar(con, schema, sectors):
    
    inputs = locals().copy()
    
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT 'solar'::VARCHAR(5) as tech,
                        '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        a.county_id, a.bin_id,
                        b.cf as generation_hourly,
                        1e6 as scale_offset
                FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                LEFT JOIN diffusion_resource_solar.solar_resource_hourly b
                    ON a.solar_re_9809_gid = b.solar_re_9809_gid
                    AND a.tilt = b.tilt
                    AND a.azimuth = b.azimuth;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    df = pd.concat(df_list, axis = 0, ignore_index = True)
        
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_normalized_hourly_resource_wind(con, schema, sectors, cur, agents):
    
    inputs = locals().copy()
    
    # isolate the information from agents regarding the power curves and hub heights for each agent
    system_sizes_df = agents.dataframe[agents.dataframe['tech'] == 'wind'][['sector_abbr', 'county_id', 'bin_id', 'i', 'j', 'cf_bin', 'turbine_height_m', 'power_curve_1', 'power_curve_2']]
    system_sizes_df['turbine_height_m'] = system_sizes_df['turbine_height_m'].astype(np.int64)   
    system_sizes_df['power_curve_1'] = system_sizes_df['power_curve_1'].astype(np.int64)   
    system_sizes_df['power_curve_2'] = system_sizes_df['power_curve_2'].astype(np.int64)   
        
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        # write the power curve(s) and turbine heights for each agent to postgres
        sql = """DROP TABLE IF EXISTS %(schema)s.agent_selected_turbines_%(sector_abbr)s;
                CREATE UNLOGGED TABLE %(schema)s.agent_selected_turbines_%(sector_abbr)s
                (
                    county_id integer,
                    bin_id integer,
                    i integer,
                    j integer,
                    cf_bin integer,
                    turbine_height_m integer,
                    power_curve_1 integer,
                    power_curve_2 integer
                );""" % inputs   
        cur.execute(sql)
        con.commit()
        
        system_sizes_sector_df = system_sizes_df[system_sizes_df['sector_abbr'] == sector_abbr][['county_id', 'bin_id', 'i', 'j', 'cf_bin', 'turbine_height_m', 'power_curve_1', 'power_curve_2']]
        system_sizes_sector_df['turbine_height_m'] = system_sizes_sector_df['turbine_height_m'].astype(np.int64)
        
        s = StringIO()
        # write the data to the stringIO
        system_sizes_sector_df.to_csv(s, index = False, header = False)
        # seek back to the beginning of the stringIO file
        s.seek(0)
        # copy the data from the stringio file to the postgres table
        cur.copy_expert('COPY %(schema)s.agent_selected_turbines_%(sector_abbr)s FROM STDOUT WITH CSV' % inputs, s)
        # commit the additions and close the stringio file (clears memory)
        con.commit()    
        s.close()
        
        # add primary key
        sql = """ALTER TABLE %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 ADD PRIMARY KEY (county_id, bin_id);""" % inputs        
        cur.execute(sql)
        con.commit()
        
        # add indices
        sql = """CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_i
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(i);
                 
                 CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_j
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(j);
                 
                 CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_cf_bin
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(cf_bin);
                 
                 CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_turbine_height_m
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(turbine_height_m);
                 
                 CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_power_curve_1
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(power_curve_1);
                 
                 CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_power_curve_2
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(power_curve_2);""" % inputs
        cur.execute(sql)
        con.commit()

        sql = """SELECT 'wind'::VARCHAR(5) as tech,
                        '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        a.county_id, a.bin_id,
                        COALESCE(b.cf, array_fill(1, array[8760])) as generation_hourly_1,
                        COALESCE(c.cf, array_fill(1, array[8760])) as generation_hourly_2,
                        1e3 as scale_offset
                FROM %(schema)s.agent_selected_turbines_%(sector_abbr)s a
                LEFT JOIN diffusion_resource_wind.wind_resource_hourly b
                    ON a.i = b.i
                    	AND a.j = b.j
                    	AND a.cf_bin = b.cf_bin
                    	AND a.turbine_height_m = b.height
                    	AND a.power_curve_1 = b.turbine_id
                LEFT JOIN diffusion_resource_wind.wind_resource_hourly c
                    ON a.i = c.i
                    	AND a.j = c.j
                    	AND a.cf_bin = c.cf_bin
                    	AND a.turbine_height_m = c.height
                    	AND a.power_curve_1 = c.turbine_id;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    df = pd.concat(df_list, axis = 0, ignore_index = True)
        
    return df    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_normalized_hourly_resource_solar(dataframe, hourly_resource_df):
    
    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)     
        
    # join resource data to dataframe
    dataframe = pd.merge(dataframe, hourly_resource_df, how = 'left', on = ['sector_abbr', 'tech', 'county_id', 'bin_id'])
    # apply the scale offset to convert values to float with correct precision
    dataframe = dataframe.apply(scale_array_precision, axis = 1, args = ('generation_hourly', 'scale_offset'))
    # scale the normalized profile by the system size
    dataframe = dataframe.apply(scale_array_sum, axis = 1, args = ('generation_hourly', 'aep'))    
    # subset to only the desired output columns
    out_cols = in_cols + ['generation_hourly']
    dataframe = dataframe[out_cols]    
    
    return dataframe


#%%
def interpolate_array(row, array_1_col, array_2_col, interp_factor_col, out_col):
    
    if row[interp_factor_col] <> 0:
        interpolated = row[interp_factor_col] * (row[array_2_col] - row[array_1_col]) + row[array_1_col]
    else:
        interpolated = row[array_1_col]
    row[out_col] = interpolated
    
    return row
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_normalized_hourly_resource_wind(dataframe, hourly_resource_df):
    
    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)     
    
    # join resource data to dataframe
    dataframe = pd.merge(dataframe, hourly_resource_df, how = 'left', on = ['sector_abbr', 'tech', 'county_id', 'bin_id'])
    # apply the scale offset to convert values to float with correct precision
    dataframe = dataframe.apply(scale_array_precision, axis = 1, args = ('generation_hourly_1', 'scale_offset'))
    dataframe = dataframe.apply(scale_array_precision, axis = 1, args = ('generation_hourly_2', 'scale_offset'))    
    # interpolate power curves
    dataframe = dataframe.apply(interpolate_array, axis = 1, args = ('generation_hourly_1', 'generation_hourly_2', 'power_curve_interp_factor', 'generation_hourly'))
    # scale the normalized profile by the system size
    dataframe = dataframe.apply(scale_array_sum, axis = 1, args = ('generation_hourly', 'aep'))    
    # subset to only the desired output columns
    out_cols = in_cols + ['generation_hourly']
    dataframe = dataframe[out_cols]

    return dataframe
    
    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_technology_costs_solar(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT 'solar'::VARCHAR(5) as tech,
                    a.sector_abbr, 
                    a.installed_costs_dollars_per_kw,
                    a.fixed_om_dollars_per_kw_per_yr,
                    a.variable_om_dollars_per_kwh,
                    a.inverter_cost_dollars_per_kw,
                    b.size_adjustment_factor as pv_size_adjustment_factor,
                    b.base_size_kw as pv_base_size_kw,
                    b.new_construction_multiplier as pv_new_construction_multiplier
            FROM %(schema)s.input_solar_cost_projections_to_model a
            LEFT JOIN %(schema)s.input_solar_cost_multipliers b
                ON a.sector_abbr = b.sector_abbr            
            WHERE a.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)

    return df
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_technology_costs_wind(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT 'wind'::VARCHAR(5) as tech,
                    a.turbine_size_kw,
                    a.turbine_height_m,
                    a.installed_costs_dollars_per_kw,
                    a.fixed_om_dollars_per_kw_per_yr,
                    a.variable_om_dollars_per_kwh
                FROM %(schema)s.turbine_costs_per_size_and_year a       
            WHERE a.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)

    return df
    
    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_tech_costs_solar(dataframe, tech_costs_df):    
    
    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)
    # join the data
    dataframe = pd.merge(dataframe, tech_costs_df, how = 'left', on = ['tech', 'sector_abbr'])
    # apply the capital cost multipliers and size adjustment factor
    dataframe['inverter_cost_dollars_per_kw'] = (dataframe['inverter_cost_dollars_per_kw'] * dataframe['cap_cost_multiplier'] * 
                                                    (1 - (dataframe['pv_size_adjustment_factor'] * dataframe['pv_base_size_kw'] - dataframe['system_size_kw'])))
    dataframe['installed_costs_dollars_per_kw'] = (dataframe['installed_costs_dollars_per_kw'] * dataframe['cap_cost_multiplier'] * 
                                                    (1 - (dataframe['pv_size_adjustment_factor'] * dataframe['pv_base_size_kw'] - dataframe['system_size_kw'])))                                                    
    # identify the new columns to return
    return_cols = ['inverter_cost_dollars_per_kw', 'installed_costs_dollars_per_kw', 'fixed_om_dollars_per_kw_per_yr', 'variable_om_dollars_per_kwh']    
    out_cols = in_cols + return_cols

    dataframe = dataframe[out_cols]

    return dataframe                                                


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_tech_costs_wind(dataframe, tech_costs_df):    

    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)    
    # join the data
    dataframe = pd.merge(dataframe, tech_costs_df, how = 'left', on = ['tech', 'turbine_size_kw', 'turbine_height_m'])  
    # fill nas (these occur where system size is zero)
    dataframe['installed_costs_dollars_per_kw'] = dataframe['installed_costs_dollars_per_kw'].fillna(0)
    dataframe['fixed_om_dollars_per_kw_per_yr'] = dataframe['fixed_om_dollars_per_kw_per_yr'].fillna(0)
    dataframe['variable_om_dollars_per_kwh'] = dataframe['variable_om_dollars_per_kwh'].fillna(0)
    # apply the capital cost multipliers to the installed costs
    dataframe['installed_costs_dollars_per_kw'] = dataframe['installed_costs_dollars_per_kw'] * dataframe['cap_cost_multiplier']
    
    # add an empty column for the inteverter costs (for compatibility with solar)
    dataframe['inverter_cost_dollars_per_kw'] = np.nan
    
    # identify the new columns to return
    return_cols = ['inverter_cost_dollars_per_kw', 'installed_costs_dollars_per_kw', 'fixed_om_dollars_per_kw_per_yr', 'variable_om_dollars_per_kwh']    
    out_cols = in_cols + return_cols

    dataframe = dataframe[out_cols]

    return dataframe


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def calculate_excess_generation_and_update_nem_settings(dataframe, gross_fit_mode = False):

    ''' Function to calculate percent of excess generation given 8760-lists of 
    consumption and generation. Currently function is configured to work only with
    the rate_input_df to avoid pulling generation and consumption profiles
    '''

    con = 'consumption_hourly'
    gen = 'generation_hourly'

    gen_array = np.array(list(dataframe[gen]))
    excess_gen_hourly = np.maximum(gen_array - np.array(list(dataframe[con])), 0)
    annual_generation = np.sum(gen_array, 1)
    excess_gen_annual = np.sum(excess_gen_hourly, 1)
    offset_generation = (gen_array - excess_gen_hourly).tolist()

    # add in a field called "apply_net_metering" and initialize to ur_enable_net_metering
    # the difference between the two fields is that ur_enable_net_metering only tells SAM what to do,
    # and becasue of the hacks we have to apply to overcome gross fit in SAM, it doesn't actually tell
    # whether actual net metering is being applied. this is what apply_net_metering is for: it tells
    # us whether an agent is actually having FULL net metering applied
    dataframe['full_net_metering'] = dataframe['ur_enable_net_metering']

    with np.errstate(invalid = 'ignore'):
        # Determine the percent of annual generation (kWh) that exceeds consumption,
        # and must be sold to the grid to receive value
        dataframe['excess_generation_percent'] = np.where(annual_generation == 0, 0, excess_gen_annual/annual_generation)
    
        
    if gross_fit_mode == True:
        # under gross fit, we will simply feed all inputs into SAM as-is and let the utilityrate3 module
        # handle all calculations with no modifications
    
        # no excess generation will be credited at the flat sell rate (outside of SAM)
        dataframe['flat_rate_excess_gen_kwh'] = 0
        
        # do not change ur_enable_net_metering
        
    else: # otherwise, we will make some modifications so that we can apply net fit for non-nem cases
        
        # if full net metering is availabile, there will be zero excess generation credited at the flat sell rate (it will all be valued at full retail rate)
        # otherwise, if full net metering is not availabl,e all excess generation will be credited at the flat sell rate
        dataframe['flat_rate_excess_gen_kwh'] = np.where(dataframe['full_net_metering'] == True, 0, excess_gen_annual)
        # if there is no net metering, calculate the non-excess portion of hourly generation and re-assign it to the gen column
        # this allows us to credit the non-excess (i.e., offsetting) portion of generation at full retail rates
        dataframe[gen] = np.where(dataframe['full_net_metering'] == True, dataframe[gen], pd.Series(offset_generation)) 
        # set SAM to run net metering calcs
        dataframe['ur_enable_net_metering'] = True

    # calculate the value of the excess generation (only applies to NET FIT when full net metering is not available)
    dataframe['net_fit_credit_dollars'] = dataframe['flat_rate_excess_gen_kwh'] * dataframe['ur_flat_sell_rate']

    # update the net metering fields in the rate_json
    dataframe = update_net_metering_fields(dataframe)

    return dataframe

        
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def calculate_electric_bills_sam(dataframe, n_workers):

    # record the input columns
    in_cols = list(dataframe.columns)
    # add a new unique identifier field for each row (for backwards compatibility with the way pssc_mp was set up)
    nrows = dataframe.shape[0]
    dataframe['uid'] = np.arange(1, nrows + 1)
    # isolat the rows that are required by SAM
    rate_cols = ['uid', 'rate_json', 'consumption_hourly', 'generation_hourly']
    # run SAM and get results ( in parallel )
    sam_results_df = pssc_mp.pssc_mp(dataframe.loc[:, rate_cols], n_workers)                                    
    # append the results to the original dataframe
    dataframe = pd.merge(dataframe, sam_results_df, on = 'uid')
    # adjust the first_year_bill_with_system to account for the net_fit_credit_dollars
    dataframe['first_year_bill_with_system'] = dataframe['elec_cost_with_system_year1'] - dataframe['net_fit_credit_dollars']      
    # rename elec_cost_without_system_year1 to first_year_bill_without_system 
    dataframe['first_year_bill_without_system'] = dataframe['elec_cost_without_system_year1']
    # isolate the return columns
    out_cols = ['first_year_bill_with_system', 'first_year_bill_without_system']
    return_cols = in_cols + out_cols
    dataframe = dataframe[return_cols]

    return dataframe   


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_depreciation_schedule(con, schema, year):
    ''' Pull depreciation schedule from dB
    
        IN: type - string - [all, macrs, standard] 
        OUT: df  - pd dataframe - year, depreciation schedule:

    '''
    inputs = locals().copy()    
    
    sql = '''SELECT tech, array_agg(deprec_rate ORDER BY ownership_year ASC)::DOUBLE PRECISION[] as deprec
            FROM %(schema)s.input_finances_depreciation_schedule
            WHERE year = %(year)s
            GROUP BY tech, year
            ORDER BY tech, year;''' % inputs
    df = pd.read_sql(sql, con)
    
    return df       

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_depreciation_schedule(dataframe, depreciation_df):
    
    dataframe = pd.merge(dataframe, depreciation_df, how = 'left', on = ['tech'])
    
    return dataframe    


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_system_degradation(con, schema):
    '''Return the annual system degradation rate as float.
        '''    
        
    inputs = locals().copy()

    sql = '''SELECT tech, ann_system_degradation
             FROM %(schema)s.input_performance_annual_system_degradation;''' % inputs
    system_degradation_df = pd.read_sql(sql, con)

    return system_degradation_df    
    
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_system_degradation(dataframe, system_degradation_df):
    
    dataframe = pd.merge(dataframe, system_degradation_df, how = 'left', on = ['tech'])
    
    return dataframe 
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_carbon_intensities(con, schema, year):
    ''' Pull depreciation schedule from dB
    
        IN: type - string - [all, macrs, standard] 
        OUT: df  - pd dataframe - year, depreciation schedule:

    '''
    inputs = locals().copy()    
    
    sql = '''SELECT state_abbr, carbon_price_cents_per_kwh
            FROM %(schema)s.carbon_intensities_to_model
            WHERE year = %(year)s;''' % inputs
    df = pd.read_sql(sql, con)
    
    return df       


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_carbon_intensities(dataframe, carbon_intensities_df):
    
    dataframe = pd.merge(dataframe, carbon_intensities_df, how = 'left', on = ['state_abbr'])
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_leasing_availability(con, schema, year):
    
    
    inputs = locals().copy()    
    
    sql = '''SELECT tech, state_abbr, leasing_allowed
             FROM %(schema)s.leasing_availability_to_model
             WHERE year = %(year)s;''' % inputs
    df = pd.read_sql(sql, con)
    
    return df     
    
    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_leasing_availability(dataframe, leasing_availability_df):
    
    dataframe = pd.merge(dataframe, leasing_availability_df, how = 'left', on = ['state_abbr', 'tech'])
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_state_starting_capacities(con, schema):

    inputs = locals().copy()    
    
    sql = '''SELECT *
             FROM %(schema)s.state_starting_capacities_to_model;''' % inputs
    df = pd.read_sql(sql, con)
    
    return df    
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def estimate_initial_market_shares(dataframe, state_starting_capacities_df):
    
    # record input columns
    in_cols = list(dataframe.columns)
    
    # find the total number of customers in each state (by technology and sector)
    state_total_developable_customers = dataframe[['state_abbr', 'sector_abbr', 'tech','developable_customers_in_bin']].groupby(['state_abbr', 'sector_abbr', 'tech']).sum().reset_index()
    state_total_agents = dataframe[['state_abbr', 'sector_abbr', 'tech', 'developable_customers_in_bin']].groupby(['state_abbr', 'sector_abbr', 'tech']).count().reset_index()
    # rename the final columns
    state_total_developable_customers.columns = state_total_developable_customers.columns.str.replace('developable_customers_in_bin', 'developable_customers_in_state')
    state_total_agents.columns = state_total_agents.columns.str.replace('developable_customers_in_bin','agent_count')
    # merge together
    state_denominators = pd.merge(state_total_developable_customers, state_total_agents, how = 'left', on = ['state_abbr', 'sector_abbr', 'tech'])
    
    # merge back to the main dataframe
    dataframe = pd.merge(dataframe, state_denominators, how = 'left', on = ['state_abbr', 'sector_abbr', 'tech'])
    
    # merge in the state starting capacities
    dataframe = pd.merge(dataframe, state_starting_capacities_df, how = 'left', on = ['tech', 'state_abbr', 'sector_abbr'])

    # determine the portion of initial load and systems that should be allocated to each agent
    # (when there are no developable agnets in the state, simply apportion evenly to all agents)
    dataframe['portion_of_state'] = np.where(dataframe['developable_customers_in_state'] > 0, 
                                             dataframe['developable_customers_in_bin']/dataframe['developable_customers_in_state'], 
                                             1./dataframe['agent_count'] * dataframe['systems_count'])
    # apply the agent's portion to the total to calculate starting capacity and systems                                         
    dataframe['number_of_adopters_last_year'] = np.round(dataframe['portion_of_state'] * dataframe['systems_count'], 6)
    dataframe['installed_capacity_last_year'] = np.round(dataframe['portion_of_state'] * dataframe['capacity_mw'], 6) * 1000.
    dataframe['market_share_last_year'] = np.where(dataframe['developable_customers_in_bin'] == 0, 
                                                 0, 
                                                 np.round(dataframe['portion_of_state'], 6))
    dataframe['market_value_last_year'] = dataframe['installed_costs_dollars_per_kw'] * dataframe['installed_capacity_last_year']

    # isolate the return columns
    return_cols = ['number_of_adopters_last_year', 'installed_capacity_last_year', 'market_share_last_year', 'market_value_last_year']
    out_cols = in_cols + return_cols
    dataframe = dataframe[out_cols]
    
    return dataframe

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_market_last_year(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT *
            FROM %(schema)s.output_market_last_year;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_market_last_year(dataframe, market_last_year_df):
    
    dataframe = pd.merge(dataframe, market_last_year_df, how = 'left', on = ['county_id', 'bin_id', 'tech', 'sector_abbr'])
    
    return dataframe
    
#%%
def check_agent_count():
  # TODO: add in a check that agent_core_attributes_ table has the correct number of rows
        # this should be called every time get_agents() is run
    pass