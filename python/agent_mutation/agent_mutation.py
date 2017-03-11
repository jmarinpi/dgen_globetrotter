# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 11:35:14 2016

@author: mgleason
"""

import pandas as pd
import decorators
import utility_functions as utilfunc

# load logger
logger = utilfunc.get_logger()


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
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
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_leasing_availability(con, schema, year):

    inputs = locals().copy()

    sql = '''SELECT tech, state_abbr, leasing_allowed
             FROM %(schema)s.leasing_availability_to_model
             WHERE year = %(year)s;''' % inputs
    df = pd.read_sql(sql, con)

    return df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_leasing_availability(dataframe, leasing_availability_df):

    dataframe = pd.merge(dataframe, leasing_availability_df,
                         how='left', on=['state_abbr', 'tech'])

    return dataframe
