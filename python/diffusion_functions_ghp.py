"""
Name: diffusion_functions
Purpose: Contains functions to calculate diffusion of distributed wind model

    (1) Determine maximum market size as a function of payback time;
    (2) Parameterize Bass diffusion curve with diffusion rates (p, q) set by 
        payback time;
    (3) Determine current stage (equivaluent time) of diffusion based on existing 
        market and current economics 
    (3) Calculate new market share by stepping forward on diffusion curve.


Author: bsigrin & edrury
Last Revision: 3/26/14

"""

import numpy as np
import pandas as pd
import utility_functions as utilfunc
import decorators
import psycopg2 as pg
from cStringIO import StringIO

#==============================================================================
# Load logger
logger = utilfunc.get_logger()

# configure psycopg2 to treat numeric values as floats (improves performance of pulling data from the database)
DEC2FLOAT = pg.extensions.new_type(
    pg.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
pg.extensions.register_type(DEC2FLOAT)

#==============================================================================
 

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def set_number_of_years_to_advance(dataframe, is_first_year):
    
    if is_first_year == True:
        dataframe['teq2'] = dataframe['teq'] + dataframe['teq_yr1']
    else:
        dataframe['teq2'] = dataframe['teq'] + 2 # now step forward two years from the 'new location'    

    return dataframe    


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_bass_and_diffusion_market_share(dataframe):
    
    dataframe['bass_market_share'] = dataframe['max_market_share'] * dataframe['new_adopt_fraction'] # new market adoption   
    # make sure diffusion doesn't decrease
    dataframe['diffusion_market_share'] = np.maximum(dataframe['bass_market_share'], dataframe['market_share_last_year'])    
   
    return dataframe   

   
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def bass_diffusion(df):
    ''' Calculate the fraction of population that diffuse into the max_market_share.
        Note that this is different than the fraction of population that will 
        adopt, which is the max market share

        IN: p,q - numpy arrays - Bass diffusion parameters
            t - numpy array - Number of years since diffusion began
            
            
        OUT: new_adopt_fraction - numpy array - fraction of overall population 
                                                that will adopt the technology
    '''
    df['f'] = np.e**(-1*(df['p'] + df['q']) * df['teq2']); 
    df['new_adopt_fraction'] = (1-df['f']) / (1 + (df['q']/df['p'])*df['f']); # Bass Diffusion - cumulative adoption
    return df
    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_equiv_time(df):
    ''' Calculate the "equivalent time" on the diffusion curve. This defines the
    gradient of adoption.

        IN: msly - numpy array - market share last year [at end of the previous solve] as decimal
            mms - numpy array - maximum market share as decimal
            p,q - numpy arrays - Bass diffusion parameters
            
        OUT: t_eq - numpy array - Equivalent number of years after diffusion 
                                  started on the diffusion curve
    '''
    
    df['mms_fix_zeros'] = np.where(df['max_market_share'] == 0, 1e-9, df['max_market_share'])
    df['bass_ratio'] = np.where(df['market_share_last_year'] > df['mms_fix_zeros'], 0., df['market_share_last_year']/df['mms_fix_zeros'])
   #ratio=msly/mms;  # ratio of adoption at present to adoption at terminal period
    df['teq'] = np.log( ( 1 - df['bass_ratio']) / (1 + df['bass_ratio']*(df['q']/df['p']))) / (-1*(df['p']+df['q'])); # solve for equivalent time
    
    return df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_diffusion_result_metrics(df):
    
    df['market_share'] = np.maximum(df['diffusion_market_share'], df['market_share_last_year'])
    df['new_market_share'] = df['market_share'] - df['market_share_last_year']
    # cap the new_market_share where the market share exceeds the max market share
    df['new_market_share'] = np.where(df['market_share'] > df['max_market_share'], 0, df['new_market_share'])
    # calculate new adopters, capacity and market value            
    df['new_adopters'] = df['new_market_share'] * df['bass_deployable_buildings_in_bin']
    df['new_capacity'] = df['new_adopters'] * df['ghp_system_size_tons']
    df['new_market_value'] = df['new_adopters'] * df['ghp_installed_costs_dlrs']
    # then add these values to values from last year to get cumulative values:
    df['number_of_adopters'] = df['number_of_adopters_last_year'] + df['new_adopters']
    df['installed_capacity'] = df['installed_capacity_last_year'] + df['new_capacity'] # All capacity in kW in the model
    df['market_value'] = df['market_value_last_year'] + df['new_market_value']

    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')    
def extract_market_last_year(dataframe):
    
    in_out_cols = { 'agent_id' : 'agent_id',
                    'tech' : 'tech',
                    'market_share' : 'market_share_last_year',
                    'max_market_share' : 'max_market_share_last_year',
                    'number_of_adopters' : 'number_of_adopters_last_year',
                    'installed_capacity' : 'installed_capacity_last_year',
                    'market_value' : 'market_value_last_year',
                    'initial_number_of_adopters' : 'initial_number_of_adopters',
                    'initial_capacity_tons' : 'initial_capacity_tons',
                    'initial_market_share' : 'initial_market_share',
                    'initial_market_value' : 'initial_market_value'
    }
    # extract the columns needed for next year
    market_last_year_df = dataframe[in_out_cols.keys()]
    # rename the columns
    market_last_year_df.rename(columns = in_out_cols, inplace = True)
    
    return market_last_year_df

