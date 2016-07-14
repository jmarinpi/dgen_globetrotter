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


def get_bass_params(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT p, q, teq
            FROM diffusion_template.input_du_bass_params;""" % inputs
            
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
def calculate_current_mms(plant_sizes_market_df):
    
    
    n_tracts = plant_sizes_market_df.shape[0]
    n_buildable_plants = np.sum(plant_sizes_market_df['plant_size_market_mw'] > 0)

    current_mms = n_buildable_plants/n_tracts
    
    return current_mms
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_new_incremantal_market_share(existing_market_share, current_mms, bass_params_df):
    
    df = calc_diffusion_market_share(df, cfg, con, is_first_year)
    pass
    

#=============================================================================
# ^^^^  Diffusion Calculator  ^^^^
@decorators.fn_timer(logger = logger, tab_level = 3, prefix = '')
def calc_diffusion(df, cur, con, cfg, techs, choose_tech, sectors, schema, is_first_year,
                   bass_params, override_p_value = None, override_q_value = None, override_teq_yr1_value = None):

    ''' Calculates the market share (ms) added in the solve year. Market share must be less
        than max market share (mms) except initial ms is greater than the calculated mms.
        For this circumstance, no diffusion allowed until mms > ms. Also, do not allow ms to
        decrease if economics deterioriate. Using the calculated 
        market share, relevant quantities are updated.

        IN: df - pd dataframe - Main dataframe
        
        OUT: df - pd dataframe - Main dataframe
            market_last_year - pd dataframe - market to inform diffusion in next year
    '''
    
    logger.info("\t\tCalculating Diffusion")
    
    # set p/q/teq_yr1 params    
    df  = set_bass_param(df, cfg, con, bass_params, override_p_value, override_q_value, override_teq_yr1_value)
    
    # calc diffusion market share
    df = calc_diffusion_market_share(df, cfg, con, is_first_year)
    
    # ensure no diffusion for non-selected options
    df['diffusion_market_share'] = df['diffusion_market_share'] * df['selected_option'] 
    
    # market share floor is based on last year's market share
    df['market_share'] = np.maximum(df['diffusion_market_share'], df['market_share_last_year'])

    # if in tech choice mode, ensure that total market share doesn't exceed 1   
    if choose_tech == True:
        # extract out the rows for unselected technologies
        market_share_cap = df[df['selected_option'] == False][['county_id', 'bin_id', 'sector_abbr', 'market_share']].groupby(['county_id', 'bin_id', 'sector_abbr']).sum().reset_index()
        # determine how much market share is allowable based on 1 - the MS of the unselected techs
        market_share_cap['market_share_cap'] = 1 - market_share_cap['market_share']
        # drop the market share column
        market_share_cap.drop('market_share', inplace = True, axis = 1)
        # merge to df
        df = pd.merge(df, market_share_cap, how = 'left', on = ['county_id', 'bin_id', 'sector_abbr'])
        # cap the market share (for the selected option only)
        df['market_share'] = np.where(df['selected_option'] == True, np.minimum(df['market_share'], df['market_share_cap']), df['market_share'])
        # drop the market share cap field
        df.drop('market_share_cap', inplace = True, axis = 1)
   
    # calculate the "new" market share (old - current)
    df['new_market_share'] = df['market_share'] - df['market_share_last_year']
    # cap the new_market_share where the market share exceeds the max market share
    df['new_market_share'] = np.where(df['market_share'] > df['max_market_share'], 0, df['new_market_share'])
    # calculate new adopters, capacity and market value            
    df['new_adopters'] = np.where(df['system_size_kw'] == 0, 0, df['new_market_share'] * df['developable_customers_in_bin'])
    df['new_capacity'] = df['new_adopters'] * df['system_size_kw']
    df['new_market_value'] = df['new_adopters'] * df['system_size_kw'] * df['installed_costs_dollars_per_kw']
    # then add these values to values from last year to get cumulative values:
    df['number_of_adopters'] = df['number_of_adopters_last_year'] + df['new_adopters']
    df['installed_capacity'] = df['installed_capacity_last_year'] + df['new_capacity'] # All capacity in kW in the model
    df['market_value'] = df['market_value_last_year'] + df['new_market_value']
    market_last_year = df[['county_id','bin_id', 'sector_abbr', 'tech', 'market_share', 'max_market_share','number_of_adopters', 'installed_capacity', 'market_value', 'initial_number_of_adopters', 'initial_capacity_mw', 'initial_market_share', 'initial_market_value']] # Update dataframe for next solve year
    market_last_year.columns = ['county_id', 'bin_id', 'sector_abbr', 'tech', 'market_share_last_year', 'max_market_share_last_year','number_of_adopters_last_year', 'installed_capacity_last_year', 'market_value_last_year', 'initial_number_of_adopters', 'initial_capacity_mw', 'initial_market_share', 'initial_market_value']

    return df, market_last_year


#=============================================================================

#  ^^^^ Calculate new diffusion in market segment ^^^^
def calc_diffusion_market_share(df, cfg, con, is_first_year):
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
    df['diffusion_market_share'] = np.where(df.market_share_last_year > df.bass_market_share, df.market_share_last_year, df.bass_market_share)
    
    return df
#==============================================================================  
    
#=============================================================================
def set_bass_param(df, cfg, con, bass_params, override_p_value, override_q_value, override_teq_yr1_value):
    ''' Set the p & q parameters which define the Bass diffusion curve.
    p is the coefficient of innovation, external influence or advertising effect. 
    q is the coefficient of imitation, internal influence or word-of-mouth effect.

        IN: scaled_metric_value - numpy array - scaled value of economic attractiveness [0-1]
        OUT: p,q - numpy arrays - Bass diffusion parameters
    '''
      
    # set p and q values
    if cfg.bass_method == 'sunshot':
        # set the scaled metric value
        df['scaled_metric_value'] = np.where(df.metric == 'payback_period', 1 - (df.metric_value/30), np.where(df.metric == 'percent_monthly_bill_savings', df.metric_value/2,np.nan))
        df['p'] = np.array([0.0015] * df.scaled_metric_value.size);
        df['q'] = np.where(df['scaled_metric_value'] >= 0.9, 0.5, np.where((df['scaled_metric_value'] >=0.66) & (df['scaled_metric_value'] < 0.9), 0.4, 0.3))
        df['teq_yr1'] = np.array([2] * df.scaled_metric_value.size);
        
    if cfg.bass_method == 'user_input':
        # NOTE: we haven't calibrated individual states for solar, or anything at all for wind
        df = pd.merge(df, bass_params, how = 'left', on  = ['state_abbr','sector_abbr', 'tech'])
    
    # if override values were provided for p, q, or teq_yr1, apply them to all agents
    if override_p_value is not None:
        df.loc[:, 'p'] = override_p_value

    if override_q_value is not None:
        df.loc[:, 'q'] = override_q_value
        
    if override_teq_yr1_value is not None:
        df.loc[:, 'teq_yr1'] = override_teq_yr1_value


    
    return df
    
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
    df['ratio'] = np.where(df['market_share_last_year'] > df['mms_fix_zeros'], 0, df['market_share_last_year']/df['mms_fix_zeros'])
   #ratio=msly/mms;  # ratio of adoption at present to adoption at terminal period
    df['teq'] = np.log( ( 1 - df['ratio']) / (1 + df['ratio']*(df['q']/df['p']))) / (-1*(df['p']+df['q'])); # solve for equivalent time
    return df
    
#=============================================================================


