# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 14:33:49 2017

@author: pgagnon
"""

import pandas as pd
import numpy as np
import os

#%%
def ingest_batt_price_trajectories(model_settings):
    batt_price_traj = pd.read_csv(os.path.join(model_settings.input_data_dir, 'batt_prices', model_settings.batt_price_file_name))
    
    res_df = pd.DataFrame(batt_price_traj['year'])
    res_df = batt_price_traj[['year', 'batt_price_per_kwh_res', 'batt_price_per_kw_res', 'batt_om_per_kw_res', 'batt_om_per_kwh_res']]
    res_df.rename(columns={'batt_price_per_kwh_res':'batt_price_per_kwh', 
                           'batt_price_per_kw_res':'batt_price_per_kw',
                           'batt_om_per_kw_res':'batt_om_per_kw',
                           'batt_om_per_kwh_res':'batt_om_per_kwh'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(batt_price_traj['year'])
    com_df = batt_price_traj[['year', 'batt_price_per_kwh_nonres', 'batt_price_per_kw_nonres', 'batt_om_per_kw_nonres', 'batt_om_per_kwh_nonres']]
    com_df.rename(columns={'batt_price_per_kwh_nonres':'batt_price_per_kwh', 
                           'batt_price_per_kw_nonres':'batt_price_per_kw',
                           'batt_om_per_kw_nonres':'batt_om_per_kw',
                           'batt_om_per_kwh_nonres':'batt_om_per_kwh'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(batt_price_traj['year'])
    ind_df = batt_price_traj[['year', 'batt_price_per_kwh_nonres', 'batt_price_per_kw_nonres', 'batt_om_per_kw_nonres', 'batt_om_per_kwh_nonres']]
    ind_df.rename(columns={'batt_price_per_kwh_nonres':'batt_price_per_kwh', 
                           'batt_price_per_kw_nonres':'batt_price_per_kw',
                           'batt_om_per_kw_nonres':'batt_om_per_kw',
                           'batt_om_per_kwh_nonres':'batt_om_per_kwh'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    batt_price_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)
    
    return batt_price_traj
    
#%%
def ingest_pv_price_trajectories(model_settings):
    pv_price_traj = pd.read_csv(os.path.join(model_settings.input_data_dir, 'pv_prices', model_settings.pv_price_file_name))
    
    res_df = pd.DataFrame(pv_price_traj['year'])
    res_df = pv_price_traj[['year', 'pv_price_res', 'pv_om_res']]
    res_df.rename(columns={'pv_price_res':'pv_price_per_kw', 
                           'pv_om_res':'pv_om_per_kw'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(pv_price_traj['year'])
    com_df = pv_price_traj[['year', 'pv_price_com', 'pv_om_com']]
    com_df.rename(columns={'pv_price_com':'pv_price_per_kw', 
                           'pv_om_com':'pv_om_per_kw'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(pv_price_traj['year'])
    ind_df = pv_price_traj[['year', 'pv_price_ind', 'pv_om_ind']]
    ind_df.rename(columns={'pv_price_ind':'pv_price_per_kw', 
                           'pv_om_ind':'pv_om_per_kw'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    pv_price_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)
    
    return pv_price_traj
    
    
#%%
def ingest_pv_degradation_trajectories(model_settings):
    
    pv_deg_traj_df = pd.read_csv(os.path.join(model_settings.input_data_dir, 'pv_degradation', model_settings.pv_deg_file_name))
    
    res_df = pd.DataFrame(pv_deg_traj_df['year'])
    res_df = pv_deg_traj_df[['year', 'res']]
    res_df.rename(columns={'res':'pv_deg'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(pv_deg_traj_df['year'])
    com_df = pv_deg_traj_df[['year', 'com']]
    com_df.rename(columns={'com':'pv_deg'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(pv_deg_traj_df['year'])
    ind_df = pv_deg_traj_df[['year', 'ind']]
    ind_df.rename(columns={'ind':'pv_deg'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    pv_deg_traj_df = pd.concat([res_df, com_df, ind_df], ignore_index=True)

    return pv_deg_traj_df
    
#%%
def ingest_elec_price_trajectories(model_settings):
    
    elec_price_traj = pd.read_csv(os.path.join(model_settings.input_data_dir, 'elec_prices', model_settings.elec_price_file_name))

    base_year_prices = elec_price_traj[elec_price_traj['year']==2016]
    
    base_year_prices.rename(columns={'elec_price_res':'res_base',
                                     'elec_price_com':'com_base',
                                     'elec_price_ind':'ind_base'}, inplace=True)
    
    elec_price_change_traj = pd.merge(elec_price_traj, base_year_prices[['res_base', 'com_base', 'ind_base', 'census_division_abbr']], on='census_division_abbr')

    elec_price_change_traj['elec_price_change_res'] = elec_price_change_traj['elec_price_res'] / elec_price_change_traj['res_base']
    elec_price_change_traj['elec_price_change_com'] = elec_price_change_traj['elec_price_com'] / elec_price_change_traj['com_base']
    elec_price_change_traj['elec_price_change_ind'] = elec_price_change_traj['elec_price_ind'] / elec_price_change_traj['ind_base']

    # Melt by sector
    res_df = pd.DataFrame(elec_price_change_traj['year'])
    res_df = elec_price_change_traj[['year', 'elec_price_change_res', 'census_division_abbr']]
    res_df.rename(columns={'elec_price_change_res':'elec_price_change'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(elec_price_change_traj['year'])
    com_df = elec_price_change_traj[['year', 'elec_price_change_com', 'census_division_abbr']]
    com_df.rename(columns={'elec_price_change_com':'elec_price_change'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(elec_price_change_traj['year'])
    ind_df = elec_price_change_traj[['year', 'elec_price_change_ind', 'census_division_abbr']]
    ind_df.rename(columns={'elec_price_change_ind':'elec_price_change'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    elec_price_change_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)

    return elec_price_change_traj
    
    
#%%
def ingest_pv_power_density_trajectories(model_settings):
    
    pv_power_traj = pd.read_csv(os.path.join(model_settings.input_data_dir, 'pv_power_density', model_settings.pv_power_density_file_name))
    
    res_df = pd.DataFrame(pv_power_traj['year'])
    res_df = pv_power_traj[['year', 'res']]
    res_df.rename(columns={'res':'pv_power_density_w_per_sqft'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(pv_power_traj['year'])
    com_df = pv_power_traj[['year', 'com']]
    com_df.rename(columns={'com':'pv_power_density_w_per_sqft'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(pv_power_traj['year'])
    ind_df = pv_power_traj[['year', 'ind']]
    ind_df.rename(columns={'ind':'pv_power_density_w_per_sqft'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    pv_power_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)

    return pv_power_traj
    
#%%
def ingest_depreciation_schedules(model_settings):
    
    deprec_schedules = pd.read_csv(os.path.join(model_settings.input_data_dir, 'depreciation_schedules', model_settings.deprec_sch_file_name))
    
    deprec_schedules['deprec_sch'] = 'temp'
    
    for index in deprec_schedules.index:
        deprec_schedules.set_value(index, 'deprec_sch', np.array(deprec_schedules.loc[index, ['1','2','3','4','5','6']]))

    max_required_year = 2050
    max_input_year = np.max(deprec_schedules['year'])
    missing_years = np.arange(max_input_year+1, max_required_year+1, 1)
    last_entry = deprec_schedules[deprec_schedules['year']==max_input_year]
    
    for year in missing_years:
        last_entry['year'] = year
        deprec_schedules = deprec_schedules.append(last_entry)
        
    return deprec_schedules[['year', 'sector_abbr', 'deprec_sch']]
    
#%%
def ingest_carbon_intensities(model_settings):
    
    carbon_intensities = pd.read_csv(os.path.join(model_settings.input_data_dir, 'carbon_intensities', model_settings.carbon_file_name))
    
    years = np.arange(2014, 2051, 2)    
    years = ['2014', '2016']
    years = [str(year) for year in years]
    
    carbon_intensities_tidy = pd.melt(carbon_intensities, id_vars='state_abbr', value_vars=years, var_name='year', value_name='grid_carbon_tco2_per_kwh')

    return carbon_intensities_tidy