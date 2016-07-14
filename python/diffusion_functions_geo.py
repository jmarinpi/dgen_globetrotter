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
def get_bass_params(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT p, q, teq_yr1
            FROM %(schema)s.input_du_bass_params;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)

    return df    


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_existing_market_share(con, schema, year):
    
    inputs = locals().copy()
        
    if year == 2014:
        sql = """SELECT 0::NUMERIC as existing_market_share;"""
    else:
        sql = """SELECT existing_market_share
                 FROM %(schema)s.output_market_last_year_du;""" % inputs
        
    df = pd.read_sql(sql, con, coerce_float = False)
    existing_market_share = df['existing_market_share'][0]
    
    return existing_market_share  

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_current_mms(plant_sizes_market_df, tract_peak_demand_df):
    
    
    total_demand_mw = tract_peak_demand_df['peak_heat_demand_mw'].sum()
    total_buildable_plant_capacity_mw = plant_sizes_market_df['plant_size_market_mw'].sum()

    current_mms = total_buildable_plant_capacity_mw/total_demand_mw
    
    return current_mms
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_new_incremental_market_share(existing_market_share, current_mms, bass_params_df, year):
    
    if year == 2014:
        is_first_year = True
    
    df = bass_params_df.copy()
    df['existing_market_share'] = existing_market_share
    df['max_market_share'] = current_mms
    bass_df = calc_diffusion_market_share(df, is_first_year)
    # market share floor is based on last year's market share
    bass_df['market_share'] = np.maximum(df['diffusion_market_share'], df['existing_market_share'])
    # calculate the new incremental market share
    bass_df['new_market_share'] = bass_df['market_share'] - bass_df['existing_market_share']
    # cap the new_market_share where the market share exceeds the max market share
    bass_df['new_market_share'] = np.where(bass_df['market_share'] > bass_df['max_market_share'], 0, bass_df['new_market_share'])
    # extract out the new_market_share (this should be a one row dataframe)
    new_market_share = bass_df['new_market_share'][0]

    return new_market_share

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def select_plants_to_be_built(plant_sizes_market_df, new_incremental_market_share, seed, iterations = 10):
    
    # calculate the total buildable plant capacity (this is the max market share in terms of MW)
    total_buildable_plant_capacity_mw = plant_sizes_market_df['plant_size_market_mw'].sum()
    # multiply by the new incremental market share to find the capacity that should be added this year
    new_incremental_capacity_mw = new_incremental_market_share * total_buildable_plant_capacity_mw
    # eliminate any plants that are larger than the new_incremental_capacity_mw
    plants_filtered_df = plant_sizes_market_df[plant_sizes_market_df['plant_size_market_mw'] <= new_incremental_capacity_mw]
    # reset the index (required to perform shuffle)
    plants_filtered_df.reset_index(drop = True, inplace = True)
    np.random.seed(seed)
    new_seeds = np.random.randint(0, 10000, iterations).tolist()
    seeds_df = pd.DataFrame()
    seeds_df['seed_value'] = new_seeds
    seeds_df['seed_id'] = range(0, iterations)
    seeds_df['unfulfilled_capacity_mw'] = 1e18
    for i, row in seeds_df.iterrows():
        seed_value = int(row['seed_value'])
        seed_id = row['seed_id']
        # randomly shuffle the dataframe 
        plants_filtered_shuffled_df = plants_filtered_df.sample(frac = 1, random_state = seed_value)
        # calculate the cumulative sum of the randomly ordered plants
        plants_filtered_shuffled_df['cumulative_capacity_mw'] = plants_filtered_shuffled_df['plant_size_market_mw'].cumsum(axis = 0)
        # find how close this can get to the new_incremental_capacity_mw
        buildable_capacity_mw = plants_filtered_shuffled_df[plants_filtered_shuffled_df['cumulative_capacity_mw'] <= new_incremental_capacity_mw]['cumulative_capacity_mw'].max()
        unfulfilled_capacity_mw = new_incremental_capacity_mw - buildable_capacity_mw
        seeds_df.loc[seed_id, 'unfulfilled_capacity_mw'] = unfulfilled_capacity_mw
    # find the minimum residual
    min_unfulfilled_capacity_mw = seeds_df['unfulfilled_capacity_mw'].min()
    # extract that seed for for that row
    best_seed_value = seeds_df[seeds_df['unfulfilled_capacity_mw'] == min_unfulfilled_capacity_mw]['seed_value'][0]
    # re-run the simulation for that best seed
    # randomly shuffle the dataframe 
    plants_filtered_shuffled_df = plants_filtered_df.sample(frac = 1, random_state = best_seed_value)
    # calculate the cumulative sum of the randomly ordered plants
    plants_filtered_shuffled_df['cumulative_capacity_mw'] = plants_filtered_shuffled_df['plant_size_market_mw'].cumsum(axis = 0)
    # extract the plants that have a cumulative capacity <= the new incremental capacity
    buildable_plants_df = plants_filtered_shuffled_df[plants_filtered_shuffled_df['cumulative_capacity_mw'] <= new_incremental_capacity_mw]
    # reset the index
    buildable_plants_df.reset_index(drop = True, inplace = True)
    
    return buildable_plants_df


#=============================================================================

#  ^^^^ Calculate new diffusion in market segment ^^^^
def calc_diffusion_market_share(df, is_first_year):
    ''' Calculate the fraction of overall population that have adopted the 
        technology in the current period. Note that this does not specify the 
        actual new adoption fraction without knowing adoption in the previous period. 

        IN: payback_period - numpy array - payback in years
            max_market_share - numpy array - maximum market share as decimal
            current_market_share - numpy array - current market share as decimal
                        
        OUT: new_market_share - numpy array - fraction of overall population 
                                                that have adopted the technology
    '''
    # The relative economic attractiveness controls the p,q values in Bass diffusion
    # Current assumption is that only payback and MBS are being used, that pp is bounded [0-30] and MBS bounded [0-120]
       
    df = calc_equiv_time(df); # find the 'equivalent time' on the newly scaled diffusion curve
    if is_first_year == True:
        df['teq2'] = df['teq'] + df['teq_yr1']
    else:
        df['teq2'] = df['teq'] + 2 # now step forward two years from the 'new location'
    
    df = bass_diffusion(df); # calculate the new diffusion by stepping forward 2 years

    df['bass_market_share'] = df.max_market_share * df.new_adopt_fraction; # new market adoption    
    df['diffusion_market_share'] = np.where(df['existing_market_share'] > df['bass_market_share'], df['existing_market_share'], df['bass_market_share'])
    
    return df
#==============================================================================  
    
    
#=============================================================================
# ^^^^  Bass Diffusion Calculator  ^^^^ 
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
    
#=============================================================================

#=============================================================================
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
    df['ratio'] = np.where(df['existing_market_share'] > df['mms_fix_zeros'], 0, df['existing_market_share']/df['mms_fix_zeros'])
   #ratio=msly/mms;  # ratio of adoption at present to adoption at terminal period
    df['teq'] = np.log( ( 1 - df['ratio']) / (1 + df['ratio']*(df['q']/df['p']))) / (-1*(df['p']+df['q'])); # solve for equivalent time
    return df
    
#=============================================================================


