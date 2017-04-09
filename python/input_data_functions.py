# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 14:33:49 2017

@author: pgagnon
"""

import pandas as pd
import numpy as np
import os

#%%
def ingest_batt_tech_performance(scenario_settings):
    batt_tech_traj = pd.read_csv(os.path.join(scenario_settings.input_data_dir, 'batt_technical_performance', scenario_settings.batt_tech_file_name))
    batt_tech_traj.to_csv(scenario_settings.dir_to_write_input_data + '/batt_tech_performance.csv', index=False)
    
    res_df = pd.DataFrame(batt_tech_traj['year'])
    res_df = batt_tech_traj[['year', 'batt_eff_res', 'batt_lifetime_res']]
    res_df.rename(columns={'batt_eff_res':'batt_eff', 
                           'batt_lifetime_res':'batt_lifetime'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(batt_tech_traj['year'])
    com_df = batt_tech_traj[['year', 'batt_eff_com', 'batt_lifetime_com']]
    com_df.rename(columns={'batt_eff_com':'batt_eff', 
                           'batt_lifetime_com':'batt_lifetime'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(batt_tech_traj['year'])
    ind_df = batt_tech_traj[['year', 'batt_eff_ind', 'batt_lifetime_ind']]
    ind_df.rename(columns={'batt_eff_ind':'batt_eff', 
                           'batt_lifetime_ind':'batt_lifetime'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    batt_tech_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)
    
    return batt_tech_traj

#%%
def ingest_batt_price_trajectories(scenario_settings):
    batt_price_traj = pd.read_csv(os.path.join(scenario_settings.input_data_dir, 'batt_prices', scenario_settings.batt_price_file_name))
    batt_price_traj.to_csv(scenario_settings.dir_to_write_input_data + '/batt_prices.csv', index=False)

    
    res_df = pd.DataFrame(batt_price_traj['year'])
    res_df = batt_price_traj[['year', 'batt_price_per_kwh_res', 'batt_price_per_kw_res',
                              'batt_om_per_kw_res', 'batt_om_per_kwh_res', 'batt_replace_frac_kw', 'batt_replace_frac_kwh']]
    res_df.rename(columns={'batt_price_per_kwh_res':'batt_price_per_kwh', 
                           'batt_price_per_kw_res':'batt_price_per_kw',
                           'batt_om_per_kw_res':'batt_om_per_kw',
                           'batt_om_per_kwh_res':'batt_om_per_kwh'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(batt_price_traj['year'])
    com_df = batt_price_traj[['year', 'batt_price_per_kwh_nonres', 'batt_price_per_kw_nonres',
                              'batt_om_per_kw_nonres', 'batt_om_per_kwh_nonres', 'batt_replace_frac_kw', 'batt_replace_frac_kwh']]
    com_df.rename(columns={'batt_price_per_kwh_nonres':'batt_price_per_kwh', 
                           'batt_price_per_kw_nonres':'batt_price_per_kw',
                           'batt_om_per_kw_nonres':'batt_om_per_kw',
                           'batt_om_per_kwh_nonres':'batt_om_per_kwh'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(batt_price_traj['year'])
    ind_df = batt_price_traj[['year', 'batt_price_per_kwh_nonres', 'batt_price_per_kw_nonres',
                              'batt_om_per_kw_nonres', 'batt_om_per_kwh_nonres', 'batt_replace_frac_kw', 'batt_replace_frac_kwh']]
    ind_df.rename(columns={'batt_price_per_kwh_nonres':'batt_price_per_kwh', 
                           'batt_price_per_kw_nonres':'batt_price_per_kw',
                           'batt_om_per_kw_nonres':'batt_om_per_kw',
                           'batt_om_per_kwh_nonres':'batt_om_per_kwh'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    batt_price_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)
    
    return batt_price_traj
    
#%%
def ingest_pv_price_trajectories(scenario_settings):
    pv_price_traj = pd.read_csv(os.path.join(scenario_settings.input_data_dir, 'pv_prices', scenario_settings.pv_price_file_name))
    pv_price_traj.to_csv(scenario_settings.dir_to_write_input_data + '/pv_prices.csv', index=False)
    
    res_df = pd.DataFrame(pv_price_traj['year'])
    res_df = pv_price_traj[['year', 'pv_price_res', 'pv_om_res', 'pv_variable_om_res']]
    res_df.rename(columns={'pv_price_res':'pv_price_per_kw', 
                           'pv_om_res':'pv_om_per_kw',
                           'pv_variable_om_res':'pv_variable_om_per_kw'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(pv_price_traj['year'])
    com_df = pv_price_traj[['year', 'pv_price_com', 'pv_om_com', 'pv_variable_om_com']]
    com_df.rename(columns={'pv_price_com':'pv_price_per_kw', 
                           'pv_om_com':'pv_om_per_kw',
                           'pv_variable_om_com':'pv_variable_om_per_kw'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(pv_price_traj['year'])
    ind_df = pv_price_traj[['year', 'pv_price_ind', 'pv_om_ind', 'pv_variable_om_ind']]
    ind_df.rename(columns={'pv_price_ind':'pv_price_per_kw', 
                           'pv_om_ind':'pv_om_per_kw',
                           'pv_variable_om_ind':'pv_variable_om_per_kw'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    pv_price_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)
    
    return pv_price_traj
    
    
#%%
def ingest_pv_degradation_trajectories(scenario_settings):
    
    pv_deg_traj_df = pd.read_csv(os.path.join(scenario_settings.input_data_dir, 'pv_degradation', scenario_settings.pv_deg_file_name))
    pv_deg_traj_df.to_csv(scenario_settings.dir_to_write_input_data + '/pv_degradation.csv', index=False)
    
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
def ingest_elec_price_trajectories(scenario_settings):
    
    elec_price_traj = pd.read_csv(os.path.join(scenario_settings.input_data_dir, 'elec_prices', scenario_settings.elec_price_file_name))
    elec_price_traj.to_csv(scenario_settings.dir_to_write_input_data + '/elec_prices.csv', index=False)
    
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
    res_df.rename(columns={'elec_price_change_res':'elec_price_multiplier'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(elec_price_change_traj['year'])
    com_df = elec_price_change_traj[['year', 'elec_price_change_com', 'census_division_abbr']]
    com_df.rename(columns={'elec_price_change_com':'elec_price_multiplier'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(elec_price_change_traj['year'])
    ind_df = elec_price_change_traj[['year', 'elec_price_change_ind', 'census_division_abbr']]
    ind_df.rename(columns={'elec_price_change_ind':'elec_price_multiplier'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    elec_price_change_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)

    return elec_price_change_traj
    
    
#%%
def ingest_pv_power_density_trajectories(scenario_settings):
    
    pv_power_traj = pd.read_csv(os.path.join(scenario_settings.input_data_dir, 'pv_power_density', scenario_settings.pv_power_density_file_name))
    pv_power_traj.to_csv(scenario_settings.dir_to_write_input_data + '/pv_power_density.csv', index=False)
    
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
def ingest_depreciation_schedules(scenario_settings):
    
    deprec_schedules = pd.read_csv(os.path.join(scenario_settings.input_data_dir, 'depreciation_schedules', scenario_settings.deprec_sch_file_name))
    deprec_schedules.to_csv(scenario_settings.dir_to_write_input_data + '/depreciation_schedules.csv', index=False)
    
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
def ingest_carbon_intensities(scenario_settings):
    
    carbon_intensities = pd.read_csv(os.path.join(scenario_settings.input_data_dir, 'carbon_intensities', scenario_settings.carbon_file_name))
    carbon_intensities.to_csv(scenario_settings.dir_to_write_input_data + '/carbon_intensities.csv', index=False)
    
    years = np.arange(2014, 2051, 2)    
    years = [str(year) for year in years]
    
    carbon_intensities_tidy = pd.melt(carbon_intensities, id_vars='state_abbr', value_vars=years, var_name='year', value_name='grid_carbon_tco2_per_kwh')

    carbon_intensities_tidy['year'] = [int(year) for year in carbon_intensities_tidy['year']]

    return carbon_intensities_tidy
    
#%%
def ingest_wholesale_elec_prices(scenario_settings):
    
    wholesale_elec_prices = pd.read_csv(os.path.join(scenario_settings.input_data_dir, 'wholesale_electricity_prices', scenario_settings.wholesale_elec_file_name))
    wholesale_elec_prices.to_csv(scenario_settings.dir_to_write_input_data + '/wholesale_electricity_prices.csv', index=False)
    
    years = np.arange(2014, 2051, 2)    
    years = [str(year) for year in years]
    
    wholesale_elec_prices_tidy = pd.melt(wholesale_elec_prices, id_vars='state_abbr', value_vars=years, var_name='year', value_name='wholesale_elec_price')

    wholesale_elec_prices_tidy['year'] = [int(year) for year in wholesale_elec_prices_tidy['year']]

    return wholesale_elec_prices_tidy
    
#%%
def ingest_financing_terms(scenario_settings):
    
    financing_terms = pd.read_csv(os.path.join(scenario_settings.input_data_dir, 'financing_terms', scenario_settings.financing_file_name))
    financing_terms.to_csv(scenario_settings.dir_to_write_input_data + '/financing_terms.csv', index=False)
    
    res_df = pd.DataFrame(financing_terms['year'])
    res_df = financing_terms[['year', 'economic_lifetime', 'loan_term_res', 'loan_rate_res', 'down_payment_res', 'real_discount_res', 'tax_rate_res']]
    res_df.rename(columns={'loan_term_res':'loan_term', 
                           'loan_rate_res':'loan_rate', 
                           'down_payment_res':'down_payment', 
                           'real_discount_res':'real_discount', 
                           'tax_rate_res':'tax_rate'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(financing_terms['year'])
    com_df = financing_terms[['year', 'economic_lifetime', 'loan_term_nonres', 'loan_rate_nonres', 'down_payment_nonres', 'real_discount_nonres', 'tax_rate_nonres']]
    com_df.rename(columns={'loan_term_nonres':'loan_term', 
                           'loan_rate_nonres':'loan_rate', 
                           'down_payment_nonres':'down_payment', 
                           'real_discount_nonres':'real_discount', 
                           'tax_rate_nonres':'tax_rate'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(financing_terms['year'])
    ind_df = financing_terms[['year', 'economic_lifetime', 'loan_term_nonres', 'loan_rate_nonres', 'down_payment_nonres', 'real_discount_nonres', 'tax_rate_nonres']]
    ind_df.rename(columns={'loan_term_nonres':'loan_term', 
                           'loan_rate_nonres':'loan_rate', 
                           'down_payment_nonres':'down_payment', 
                           'real_discount_nonres':'real_discount', 
                           'tax_rate_nonres':'tax_rate'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    financing_terms = pd.concat([res_df, com_df, ind_df], ignore_index=True)

    return financing_terms