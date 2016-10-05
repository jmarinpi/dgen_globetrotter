# -*- coding: utf-8 -*-
"""
Created on Wed Oct  5 09:58:03 2016

@author: mgleason
"""
import numpy as np
import pandas as pd
import utility_functions as utilfunc
import decorators
import psycopg2 as pg

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================

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
