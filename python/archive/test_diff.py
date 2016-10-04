# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 15:19:42 2015

@author: bsigrin
"""

#Source diffusion_functions
#Source config.py
#Create con
import config as cfg
import data_functions as datfunc
import utility_functions as utilfunc
from diffusion_functions import *
import pandas as pd

con, cur = utilfunc.make_con(cfg.pg_conn_string)

df = pd.read_csv('test_df_diff.csv')

df  = set_bass_param(df, cfg, con) 
df = calc_equiv_time(df); # find the 'equivalent time' on the newly scaled diffusion curve
df['teq2'] = df['teq'] + 2; # now step forward two years from the 'new location'
df = bass_diffusion(df); # calculate the new diffusion by stepping forward 2 years
df['bass_market_share'] = df.max_market_share * df.new_adopt_fraction; # new market adoption    
df['market_share'] = np.where(df.market_share_last_year > df.bass_market_share, df.market_share_last_year, df.bass_market_share)

df['diffusion_market_share'] = df.market_share
   
df['market_share'] = np.maximum(df['diffusion_market_share'], df['market_share_last_year'])
df['new_market_share'] = df['market_share']-df['market_share_last_year']
df['new_market_share'] = np.where(df['market_share'] > df['max_market_share'], 0, df['new_market_share'])
        
df['new_adopters'] = df['new_market_share'] * df['customers_in_bin']
df['new_capacity'] = df['new_adopters'] * df['system_size_kw']

print df['new_capacity'].sum()/1e6 # 0.61 GW nationally added

df = pd.read_csv('test_df_diff.csv').query('state_abbr == "CA"') # Now run CA only

df  = set_bass_param(df, cfg, con) 
df = calc_equiv_time(df); # find the 'equivalent time' on the newly scaled diffusion curve
df['teq2'] = df['teq'] + 2; # now step forward two years from the 'new location'
df = bass_diffusion(df); # calculate the new diffusion by stepping forward 2 years
df['bass_market_share'] = df.max_market_share * df.new_adopt_fraction; # new market adoption    
df['market_share'] = np.where(df.market_share_last_year > df.bass_market_share, df.market_share_last_year, df.bass_market_share)

df['diffusion_market_share'] = df.market_share
   
df['market_share'] = np.maximum(df['diffusion_market_share'], df['market_share_last_year'])
df['new_market_share'] = df['market_share']-df['market_share_last_year']
df['new_market_share'] = np.where(df['market_share'] > df['max_market_share'], 0, df['new_market_share'])
        
df['new_adopters'] = df['new_market_share'] * df['customers_in_bin']
df['new_capacity'] = df['new_adopters'] * df['system_size_kw']

print df['new_capacity'].sum()/1e6 # 0.75 GW is CA only WTF??