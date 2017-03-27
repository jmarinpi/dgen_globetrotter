# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 17:59:55 2017

@author: pgagnon
"""

import pandas as pd
import numpy as np

input_dir = '../unformatted_input_data/'
output_dir = '../elec_prices/'

file_names = ['elec_prices_sunShot2030_2cents.csv',
              'elec_prices_sunShot2030_2cents_battLow.csv',
              'elec_prices_sunShot2030_3cents.csv',
              'elec_prices_sunShot2030_3cents_battLow.csv',
              'elec_prices_sunShot2030_4cents.csv',
              'elec_prices_sunShot2030_4cents_battLow.csv',
              'elec_prices_sunShot2030_atbMid.csv',
              'elec_prices_sunShot2030_atbMid_battLow.csv']

for file_name in file_names:
    print file_name
    region_map = pd.read_csv('ReEDS_census_division_map.csv')
    region_map.rename(columns={'cen':'id'}, inplace=True)
    
    # Import the ReEDS file
    elec_price_df = pd.read_csv(input_dir + file_name)
    
    # Merge on census divisions and drop unnecessary columns
    elec_price_df = pd.merge(elec_price_df, region_map[['census_division_abbr', 'id']], on=['id'])
    elec_price_df.drop(['type', 'id'], axis=1, inplace=True)
    
    # Fill in the odd years with linear interpolation of even years
    missing_years = np.arange(2011, 2050, 2)
    for year in missing_years:
        elec_price_df[str(year)] = (elec_price_df[str(year-1)] + elec_price_df[str(year+1)]) / 2
        
    # Formatting for melt
    elec_price_df.set_index('census_division_abbr', inplace=True)
    elec_price_df = elec_price_df.transpose()
    elec_price_df.reset_index(inplace=True)
    elec_price_df.rename(columns={'index':'year'}, inplace=True)
    
    # Melt
    elec_price_df_melted = pd.melt(elec_price_df, id_vars='year', value_name='elec_price_res')
    
    # Change from $/MWh to $/kWh
    elec_price_df_melted['elec_price_res'] = elec_price_df_melted['elec_price_res'] / 1000.0
    
    # All sectors are the same, since ReEDS does not differentiate
    elec_price_df_melted['elec_price_com'] = elec_price_df_melted['elec_price_res']
    elec_price_df_melted['elec_price_ind'] = elec_price_df_melted['elec_price_res']
    
    # Write formatted results
    elec_price_df_melted.to_csv(output_dir + file_name, index=False)