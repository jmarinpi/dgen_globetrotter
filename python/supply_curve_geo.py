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

def generate_resource_data(con, schema):
    
    setup_resource_data_egs()
    setup_resource_data_hydrothermal()
    combine_resource_data()
    
    return
#%%

def setup_resource_data_egs():
    
    #TODO: write this function
    pass

    return

def setup_resource_data_hydrothermal():
    
    #TODO: write this function
    pass

    return

def combine_resource_data():
    
    #TODO: write this function
    pass

    return


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_resource_data(con, schema, year):
    
    inputs = locals().copy()
        
    sql = """SELECT *
             FROM diffusion_geo.resource_data_dummy;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_resource_data(con, schema, year):