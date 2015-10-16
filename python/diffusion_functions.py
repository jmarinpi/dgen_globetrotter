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
import data_functions as datfunc
import decorators
from config import show_times

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================

#=============================================================================
# ^^^^  Diffusion Calculator  ^^^^
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 3, prefix = '')
def calc_diffusion(df, cur, con, cfg, techs, sectors, schema, year, start_year, calibrate_mode,
                   p_scalar = 1, teq_yr1 = 2):

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
    
    
    # get market characteristics from previous year                    
    is_first_year = year == cfg.start_year                
    previous_year_results = datfunc.get_market_last_year(cur, con, is_first_year, techs, sectors, schema, calibrate_mode, df) 
    df = pd.merge(df, previous_year_results, how = 'left', on = ['county_id', 'bin_id', 'tech', 'sector_abbr'])    
    
    df = calc_diffusion_market_share(df, cfg, con, is_first_year, p_scalar, teq_yr1) 
    df['diffusion_market_share'] = df['diffusion_market_share'] * df['selected_option'] # ensure no diffusion for non-selected options
   
    df['market_share'] = np.maximum(df['diffusion_market_share'], df['market_share_last_year'])
    df['new_market_share'] = df['market_share']-df['market_share_last_year']
    df['new_market_share'] = np.where(df['market_share'] > df['max_market_share'], 0, df['new_market_share'])
            
    df['new_adopters'] = df['new_market_share'] * df['customers_in_bin']
    df['new_capacity'] = df['new_adopters'] * df['system_size_kw']
    df['new_market_value'] = df['new_adopters'] * df['system_size_kw'] * df['installed_costs_dollars_per_kw']
    # then add these values to values from last year to get cumulative values:
    df['number_of_adopters'] = df['number_of_adopters_last_year'] + df['new_adopters']
    df['installed_capacity'] = df['installed_capacity_last_year'] + df['new_capacity'] # All capacity in kW in the model
    df['market_value'] = df['market_value_last_year'] + df['new_market_value']
    market_last_year = df[['county_id','bin_id', 'sector_abbr', 'tech', 'market_share', 'max_market_share','number_of_adopters', 'installed_capacity', 'market_value']] # Update dataframe for next solve year
    market_last_year.columns = ['county_id', 'bin_id', 'sector_abbr', 'tech', 'market_share_last_year', 'max_market_share_last_year','number_of_adopters_last_year', 'installed_capacity_last_year', 'market_value_last_year' ]

    return df, market_last_year


#=============================================================================

#  ^^^^ Calculate new diffusion in market segment ^^^^
def calc_diffusion_market_share(df, cfg, con, is_first_year, p_scalar = 1, teq_yr1 = 2):
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
       
    df  = set_bass_param(df, cfg, con) 
    # scale the p values by a factor of p_scalar
    df.loc[:, 'p'] = df['p'] * p_scalar
    df = calc_equiv_time(df); # find the 'equivalent time' on the newly scaled diffusion curve
    if is_first_year == True:
        df['teq2'] = df['teq'] + teq_yr1
    else:
        df['teq2'] = df['teq'] + 2 # now step forward two years from the 'new location'
    
    df = bass_diffusion(df); # calculate the new diffusion by stepping forward 2 years

    df['bass_market_share'] = df.max_market_share * df.new_adopt_fraction; # new market adoption    
    df['diffusion_market_share'] = np.where(df.market_share_last_year > df.bass_market_share, df.market_share_last_year, df.bass_market_share)
    
    return df
#==============================================================================  
    
#=============================================================================
def set_bass_param(df, cfg, con):
    ''' Set the p & q parameters which define the Bass diffusion curve.
    p is the coefficient of innovation, external influence or advertising effect. 
    q is the coefficient of imitation, internal influence or word-of-mouth effect.

        IN: scaled_metric_value - numpy array - scaled value of economic attractiveness [0-1]
        OUT: p,q - numpy arrays - Bass diffusion parameters
    '''
    
    # get the calibrated bass parameters
    bass_params_solar = pd.read_sql('SELECT * FROM diffusion_solar.bass_pq_calibrated_params_solar', con)
   
    # set p and q values
    if cfg.bass_method == 'sunshot':
        # set the scaled metric value
        df['scaled_metric_value'] = np.where(df.metric == 'payback_period', 1 - (df.metric_value/30), np.where(df.metric == 'percent_monthly_bill_savings', df.metric_value/2,np.nan))
        df['p'] = np.array([0.0015] * df.scaled_metric_value.size);
        df['q'] = np.where(df['scaled_metric_value'] >= 0.9, 0.5, np.where((df['scaled_metric_value'] >=0.66) & (df['scaled_metric_value'] < 0.9), 0.4, 0.3))
        
    if cfg.bass_method == 'calibrated':
        # NOTE: assumes same p,q values for non-solar techs!!
        df = pd.merge(df, bass_params_solar, how = 'left', on  = ['state_abbr','sector_abbr'])
        
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


