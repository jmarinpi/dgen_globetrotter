# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 11:35:14 2016

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
# functions borrowed from electricity
from agent_mutation_elec import get_depreciation_schedule, apply_depreciation_schedule, get_leasing_availability, apply_leasing_availability

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
def get_initial_agent_attributes(con, schema):
    
    inputs = locals().copy()
    sql = """SELECT *, FALSE::BOOLEAN AS new_construction
             FROM %(schema)s.agent_core_attributes_all
             WHERE year = 2012;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    agents = Agents(df)

    return agents

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_new_agent_attributes(con, schema, year):
    
    inputs = locals().copy()
    sql = """SELECT *, TRUE::BOOLEAN AS new_construction
             FROM %(schema)s.agent_core_attributes_all
             WHERE year = %(year)s;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    agents = Agents(df)

    return agents

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_psuedo_ghp_agents(con, schema):
    
    inputs = locals().copy()
    sql = """SELECT *, 
                    'ghp' as tech,
                    'com'::VARCHAR(3) as sector_abbr, 
                    10000. as buildings_in_bin,
                    FALSE::BOOLEAN as new_construction,
                    'closed vertical' as sys_config,
                    25000. as totsqft_conditioned,
                    'WY'::VARCHAR(2) as state_abbr
             FROM diffusion_geo.ghp_simulations_com;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    agents = Agents(df)

    return agents
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def size_systems_ghp(dataframe):
    
    dataframe['ghp_system_size_tons'] = dataframe['cooling_capacity_ton']
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_developable_customers_and_load(dataframe):

    dataframe['developable_customers_in_bin'] = np.where(dataframe['ghp_system_size_tons'] == 0, 
                                                                  0,
                                                                  dataframe['buildings_in_bin'])
                                                        
    dataframe['developable_load_kwh_in_bin'] = dataframe['developable_customers_in_bin'] * dataframe['baseline_source_energy_mbtu']/1000./3412.14 
                                                            
    return dataframe    
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_technology_costs_ghp(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT year, 
                    sector_abbr,
                    sys_config,
                    new_heat_exchanger_cost_dollars_per_ton as ghx_cost_dlrs_per_ton,
                    new_heat_pump_cost_dollars_per_ton as heat_pump_dlrs_per_ton,
                    fixed_om_dollars_per_sf_per_year,
                    retrofit_rest_of_system_multiplier
             FROM %(schema)s.input_ghp_cost
             WHERE year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)

    return df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_tech_costs_ghp(dataframe, tech_costs_ghp_df):    
    

    dataframe = pd.merge(dataframe, tech_costs_ghp_df, how = 'left', on = ['sector_abbr', 'sys_config'])
    # Installed Costs
    dataframe['ghx_cost_dlrs'] = dataframe['ghp_system_size_tons'] * dataframe['ghx_cost_dlrs_per_ton']
    dataframe['heat_pump_cost_dlrs'] = dataframe['ghp_system_size_tons'] * dataframe['heat_pump_dlrs_per_ton']
    dataframe['rest_of_system_cost_dlrs'] = np.where(dataframe['new_construction'] == True, 0., (dataframe['ghx_cost_dlrs'] + dataframe['heat_pump_cost_dlrs']) * dataframe['retrofit_rest_of_system_multiplier'])
    dataframe['installed_costs_dlrs'] = dataframe['ghx_cost_dlrs'] + dataframe['heat_pump_cost_dlrs'] + dataframe['rest_of_system_cost_dlrs']
    # O&M
    dataframe['fixed_om_dlrs_per_year'] = dataframe['totsqft_conditioned'] * dataframe['fixed_om_dollars_per_sf_per_year']
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_bill_savings_ghp(dataframe):
    
    dataframe['first_year_bill_with_system'] = dataframe['gshp_energy_cost']
    dataframe['first_year_bill_without_system'] = dataframe['baseline_energy_cost']
    
    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def update_year(dataframe, year):
    
    dataframe.loc[:, 'year'] = year
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_regional_energy_prices(con, schema, year):
    
    inputs = locals().copy()
    
    # need to create data for fuel type = none for apply_regional_energy_prices()
    # also need to create data for wood (assume 28 MMBTU per cord of wood and approx $200/cord)
    sql = """SELECT sector_abbr, 
                    census_division_abbr, 
                    fuel_type, 
                    dlrs_per_kwh
            FROM %(schema)s.aeo_energy_prices_to_model
            WHERE year = %(year)s
            
            UNION ALL
            
            SELECT DISTINCT sector_abbr, 
                            census_division_abbr, 
                            unnest(ARRAY['none', 'no fuel']) as fuel_type,
                            0::NUMERIC as dlrs_per_kwh
            FROM %(schema)s.aeo_energy_prices_to_model
            WHERE year = %(year)s
            
            UNION ALL
            
            SELECT DISTINCT sector_abbr, 
                            census_division_abbr, 
                            'wood' as fuel_type,
                            0.024::NUMERIC as dlrs_per_kwh
            FROM %(schema)s.aeo_energy_prices_to_model
            WHERE year = %(year)s
            
            UNION ALL
            
            SELECT DISTINCT sector_abbr, 
                            census_division_abbr, 
                            unnest(ARRAY['other', 'solar energy', 'district chilled water', 'district hot water', 'coal', 'district steam']) as fuel_type,
                            0.07::NUMERIC as dlrs_per_kwh
            FROM %(schema)s.aeo_energy_prices_to_model
            WHERE year = %(year)s            
            ;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_regional_energy_prices(dataframe, energy_prices_df):
    
    
    in_cols = list(dataframe.columns)
    
    # duplicate the energy_prices_df for each of space heating, space cooling, and water heating, renaming the price field accordingly
    space_heating_prices_df = energy_prices_df.copy()
    rename_map = {'dlrs_per_kwh' : 'space_heat_dlrs_per_kwh', 'fuel_type'    : 'space_heat_fuel'}
    space_heating_prices_df.rename(columns = rename_map, inplace = True)
    
    water_heating_prices_df = energy_prices_df.copy()
    rename_map = {'dlrs_per_kwh' : 'water_heat_dlrs_per_kwh','fuel_type'    : 'water_heat_fuel'}
    water_heating_prices_df.rename(columns = rename_map, inplace = True)

    space_cooling_prices_df = energy_prices_df.copy()
    rename_map = {'dlrs_per_kwh' : 'space_cool_dlrs_per_kwh','fuel_type'    : 'space_cool_fuel'}
    space_cooling_prices_df.rename(columns = rename_map, inplace = True)    
    
    # join dataframes together
    dataframe = pd.merge(dataframe, space_heating_prices_df, how = 'left', on = ['census_division_abbr', 'sector_abbr', 'space_heat_fuel'])
    dataframe = pd.merge(dataframe, water_heating_prices_df, how = 'left', on = ['census_division_abbr', 'sector_abbr', 'water_heat_fuel'])    
    dataframe = pd.merge(dataframe, space_cooling_prices_df, how = 'left', on = ['census_division_abbr', 'sector_abbr', 'space_cool_fuel'])

    # check for nulls; if found, raise error    
    nulls_exist = dataframe[['space_heat_dlrs_per_kwh', 'water_heat_dlrs_per_kwh', 'space_cool_dlrs_per_kwh']].isnull().any().any()
    if nulls_exist == True:
        raise ValueError("null values exist in space_heat_dlrs_per_kwh, water_heat_dlrs_per_kwh, or space_cool_dlrs_per_kwh")
    
    
    out_cols = ['space_heat_dlrs_per_kwh', 'water_heat_dlrs_per_kwh', 'space_cool_dlrs_per_kwh']
    return_cols = in_cols + out_cols
    dataframe = dataframe[return_cols]
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_end_user_costs_du(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT *
            FROM %(schema)s.input_du_cost_user
            WHERE year = %(year)s;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_end_user_costs_du(dataframe, end_user_costs_du_df):
    
    # join dataframes together
    dataframe = pd.merge(dataframe, end_user_costs_du_df, how = 'left', on = ['sector_abbr', 'year'])

    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def update_system_ages(dataframe, year):

    in_cols = list(dataframe.columns)
    
    # add in the microdata release year field for each agent (2003 for com, 2009 for recs)
    dataframe['microdata_release_year'] = np.where(dataframe['sector_abbr'] == 'res', 2009, 2003)
    
    # calculate the additional years (for new construction set = 0)
    dataframe['add_years'] = np.where(dataframe['new_construction'] == False, year - dataframe['microdata_release_year'], 0)
    
    # increment the system ages
    dataframe.loc[:, 'space_heat_system_age'] = dataframe['space_heat_system_age'] + dataframe['add_years']
    dataframe.loc[:, 'space_cool_system_age'] = dataframe['space_cool_system_age'] + dataframe['add_years']
    dataframe.loc[:, 'average_system_age'] = dataframe.loc[:, 'average_system_age'] + dataframe['add_years']
    
    # return just the input  columns
    dataframe = dataframe[in_cols]
    
    return dataframe
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def check_system_expirations(dataframe):

    in_cols = list(dataframe.columns)
    
    # add in the microdata release year field for each agent (2003 for com, 2009 for recs)
    dataframe['needs_replacement_heat_system'] = dataframe['space_heat_system_age'] > dataframe['space_heat_system_expected_lifetime']    
    dataframe['needs_replacement_cool_system'] = dataframe['space_cool_system_age'] > dataframe['space_cool_system_expected_lifetime']        
    dataframe['needs_replacement_average_system'] = dataframe['average_system_age'] > dataframe['average_system_expected_lifetime']
    
    return_cols = ['needs_replacement_heat_system', 'needs_replacement_cool_system', 'needs_replacement_average_system']
    out_cols = in_cols + return_cols
    dataframe = dataframe[out_cols]
    
    return dataframe