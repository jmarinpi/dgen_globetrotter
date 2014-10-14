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
import time

#=============================================================================
# ^^^^  Diffusion Calculator  ^^^^
def calc_diffusion(df, logger, year, sector):
    ''' Brings everything together to calculate the diffusion market share 
        based on payback, max market, and market last year. Using the calculated 
        market share, relevant quantities are updated.

        IN: df - pd dataframe - Main dataframe
        
        OUT: df - pd dataframe - Main dataframe
            market_last_year - pd dataframe - market to inform diffusion in next year
    '''
    t0 = time.time()
    df['diffusion_market_share'] = calc_diffusion_market_share(df.payback_period.values,df.max_market_share.values, df.market_share_last_year.values)
    logger.info('diffunc.calc_diffusion_market_share for %s for %s sector took: %0.1fs' %(year, sector, time.time() - t0))
    
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
    market_last_year = df[['county_id','bin_id','market_share', 'number_of_adopters', 'installed_capacity', 'market_value']] # Update dataframe for next solve year
    market_last_year.columns = ['county_id', 'bin_id', 'market_share_last_year', 'number_of_adopters_last_year', 'installed_capacity_last_year', 'market_value_last_year' ]
    return df,market_last_year, logger


#=============================================================================
# ^^^^  Bass Diffusion Calculator  ^^^^
def bass_diffusion(p, q, t):
    ''' Calculate the fraction of population that diffuse into the max_market_share.
        Note that this is different than the fraction of population that have/
        will adopt.

        IN: p,q - numpy arrays - Bass diffusion parameters
            t - numpy array - Number of years since diffusion began
            
            
        OUT: new_adopt_fraction - numpy array - fraction of overall population 
                                                that will adopt the technology
    '''
    f = np.e**(-1*(p+q)*t); 
    new_adopt_fraction = (1-f) / (1 + (q/p)*f); # Bass Diffusion - cumulative adoption
    return new_adopt_fraction
    
#=============================================================================

#=============================================================================
def calc_equiv_time(msly, mms, p, q):
    ''' Calculate the "equivalent time" on the diffusion curve. This defines the
    gradient of adoption.

        IN: msly - numpy array - market share last year [at end of the previous solve] as decimal
            mms - numpy array - maximum market share as decimal
            p,q - numpy arrays - Bass diffusion parameters
            
        OUT: t_eq - numpy array - Equivalent number of years after diffusion 
                                  started on the diffusion curve
    '''
    mms = np.where(mms == 0, 1e-9, mms)
    ratio = np.where(msly > mms, 0, msly/mms)
    #ratio=msly/mms;  # ratio of adoption at present to adoption at terminal period
    t_eq = np.log( ( 1 - ratio) / (1 + ratio*(q/p))) / (-1*(p+q)); # solve for equivalent time
    return t_eq
    
#=============================================================================

#=============================================================================
def set_param_payback(payback_period,pval = 0.0015):
    ''' Set the p & q parameters which define the Bass diffusion curve.
    p is the coefficient of innovation, external influence or advertising effect. 
    q is the coefficient of imitation, internal influence or word-of-mouth effect.

        IN: payback_period - numpy array - payback in years
        OUT: p,q - numpy arrays - Bass diffusion parameters
    '''
    # set p and q values
    p = np.array([pval] * payback_period.size);
    q = np.where(payback_period <= 3, 0.5, np.where((payback_period <=10) & (payback_period > 3), 0.4, 0.3))

    return p, q
    
#=============================================================================

#==============================================================================
#  ^^^^ Calculate new diffusion in market segment ^^^^
def calc_diffusion_market_share(payback_period,max_market_share, market_share_last_year):
    ''' Calculate the fraction of overall population that have adopted the 
        technology in the current period. Note that this does not specify the 
        actual new adoption fraction without knowing adoption in the previous period. 

        IN: payback_period - numpy array - payback in years
            max_market_share - numpy array - maximum market share as decimal
            current_market_share - numpy array - current market share as decimal
                        
        OUT: new_market_share - numpy array - fraction of overall population 
                                                that have adopted the technology
    '''
    payback_period = np.maximum(np.minimum(payback_period,30),0) # Payback defined [0,30] years        
    p,q  = set_param_payback(payback_period) 
    teq = calc_equiv_time(market_share_last_year, max_market_share, p, q); # find the 'equivalent time' on the newly scaled diffusion curve
    teq2 = teq + 2; # now step forward two years from the 'new location'
    new_adopt_fraction = bass_diffusion(p, q, teq2); # calculate the new diffusion by stepping forward 2 years
    market_share = max_market_share * new_adopt_fraction; # new market adoption    
    market_share = np.where(market_share_last_year > max_market_share, market_share_last_year, market_share)
    
    return market_share
#==============================================================================  
