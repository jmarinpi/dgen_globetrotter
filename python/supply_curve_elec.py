# -*- coding: utf-8 -*-
"""
Created on Thu May 26 11:29:02 2016

@author: mgleason
"""
import decorators
import utility_functions as utilfunc


#%% GLOBAL SETTINGS

# load logger
logger = utilfunc.get_logger()


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def setup_resource_data(cur, con, schema, seed):
    
    # DO NOTHING -- NO SUPPLY CURVES USED IN ELEC
    return
    
    
 
#%%