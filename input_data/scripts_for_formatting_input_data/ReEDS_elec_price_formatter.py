# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 17:59:55 2017

@author: pgagnon
"""

import pandas as pd
import numpy as np
import os
import sys
import glob
os.chdir('C:\Users\pgagnon\Desktop\diffusion\input_data\scripts_for_formatting_input_data')

input_dir = '../unformatted_input_data/'
output_dir = '../elec_prices/'

file_name = '../unformatted_input_data/ElectricityPriceFordGen_Subset.csv'
#file_names = [s for s in glob.glob(os.path.join(os.getcwd(), input_dir) + '*.csv*') if not '~$' in s]

region_map = pd.read_csv('ReEDS_census_division_map.csv')
region_map.rename(columns={'cen':'id'}, inplace=True)
cen_div_to_state = pd.read_csv('state_to_census_regions_divs_lkup.csv')

elec_price_df = pd.read_csv(file_name)

elec_price_df = pd.merge(elec_price_df, region_map[['census_division_abbr', 'id']], on=['id'])
elec_price_df.drop(['id'], axis=1, inplace=True)

elec_price_df = elec_price_df.pivot_table(index=['scenario', 'census_division_abbr', 'type'], columns='year', values='value').reset_index(drop = False)

missing_years = np.arange(2011, 2050, 2)
for year in missing_years:
    elec_price_df[year] = (elec_price_df[(year-1)] + elec_price_df[(year+1)]) / 2
    
elec_price_df = pd.melt(elec_price_df, id_vars = ['scenario', 'census_division_abbr', 'type'], var_name = 'year')

scen_names = list(pd.unique(elec_price_df['scenario']))
price_types = ['price', 'comp_p']

for scen in scen_names:
    for price_type in price_types:
        scen_df = elec_price_df[elec_price_df.type == price_type]
        scen_df = scen_df[scen_df.scenario == scen]
        if price_type == 'price':
            scen_df['value'] = scen_df.value / 1000
            scen_df.rename(columns={'value':'elec_price_res'}, inplace=True)
            scen_df['elec_price_com'] = scen_df.elec_price_res
            scen_df['elec_price_ind'] = scen_df.elec_price_res
            scen_df.drop(['type', 'scenario'], axis=1).to_csv('../elec_prices/elec_prices_%s.csv' % scen, index=False)
        else:
            scen_df['value'] = scen_df.value / 1000
            scen_df = scen_df.merge(cen_div_to_state[['state_abbr', 'census_division_abbr']], on='census_division_abbr')
            scen_df = scen_df.pivot_table(index=['state_abbr', 'census_division_abbr', 'scenario'], columns='year', values='value').reset_index(drop = False)
            scen_df.drop(['census_division_abbr', 'scenario'], axis=1).to_csv('../wholesale_electricity_prices/wholesale_elec_prices_%s.csv' % scen, index=False)
            
#            scen_df[scen_df.type == 'comp_p'].to_csv('../wholesale_electricity_prices/ATB17_%s.csv' % scen, index=False)
    
    

        
    

#%%
for file_name in file_names:
    print file_name
    
    # Import the ReEDS file
    elec_price_df = pd.read_csv(file_name)
    
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