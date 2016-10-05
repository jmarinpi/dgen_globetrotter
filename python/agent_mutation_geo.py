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
def update_year(dataframe, year):
    
    dataframe.loc[:, 'year'] = year
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def update_system_ages(dataframe, year, is_first_year):

    in_cols = list(dataframe.columns)
    
    if is_first_year == True:
        
        # add in the microdata release year field for each agent (2003 for com, 2009 for recs)
        dataframe['microdata_release_year'] = np.where(dataframe['sector_abbr'] == 'res', 2009, 2003)
    
        # calculate the additional years (for new construction set = 0)
        dataframe['add_years'] = np.where(dataframe['new_construction'] == False, year - dataframe['microdata_release_year'], 0)
    
    else:
        dataframe['add_years'] = 2

    # increment the system ages
    dataframe.loc[:, 'space_heat_system_age'] = np.where(dataframe['new_construction'] == True, 0, dataframe['space_heat_system_age'] + dataframe['add_years'])
    dataframe.loc[:, 'space_cool_system_age'] = np.where(dataframe['new_construction'] == True, 0, dataframe['space_cool_system_age'] + dataframe['add_years'])
    dataframe.loc[:, 'average_system_age'] = np.where(dataframe['new_construction'] == True, 0, dataframe.loc[:, 'average_system_age'] + dataframe['add_years'])  

    # if system is older than expected expiration, assume it was reinstalled last year and therefore has an age of 2
    if is_first_year == False:
        # note: do not apply this in the first year, because if the system is older than the expiration in first model year, it needs to replaced this year
        dataframe.loc[dataframe['space_heat_system_age'] > dataframe['space_heat_system_expected_lifetime'], 'space_heat_system_age'] = 0 + dataframe['add_years']
        dataframe.loc[dataframe['space_cool_system_age'] > dataframe['space_cool_system_expected_lifetime'], 'space_cool_system_age'] = 0 + dataframe['add_years']
        dataframe.loc[dataframe['average_system_age'] > dataframe['average_system_expected_lifetime'], 'average_system_age'] = 0 + dataframe['add_years']


    # return just the input  columns
    dataframe = dataframe[in_cols]
    
    return dataframe
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_years_to_replacement(dataframe):

    # calculate the years remaining until the expected lifetime
    dataframe['years_to_replacement_heat'] = np.round(dataframe['space_heat_system_expected_lifetime'] - dataframe['space_heat_system_age'], 0).astype('int64')
    dataframe['years_to_replacement_cool'] = np.round(dataframe['space_cool_system_expected_lifetime'] - dataframe['space_cool_system_age'], 0).astype('int64')
    dataframe['years_to_replacement_average'] = np.round(dataframe['average_system_expected_lifetime'] - dataframe['average_system_age'], 0).astype('int64')
    # if years to replacement is negative, set to zero (this will only apply in year 1)
    dataframe.loc[dataframe['years_to_replacement_heat'] < 0, 'years_to_replacement_heat'] = 0
    dataframe.loc[dataframe['years_to_replacement_cool'] < 0, 'years_to_replacement_cool'] = 0
    dataframe.loc[dataframe['years_to_replacement_average'] < 0, 'years_to_replacement_average'] = 0
        
    return dataframe
