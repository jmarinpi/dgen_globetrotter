# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 14:33:49 2017

@author: pgagnon
"""

import pandas as pd
import numpy as np
import os

#%%
def ingest_storage_cost_schedules(model_settings):
    storage_cost_schedule_df = pd.read_csv(os.path.join(model_settings.input_data_dir, 'storage_cost_schedules', model_settings.storage_cost_file_name))
    
    res_df = pd.DataFrame(storage_cost_schedule_df['year'])
    res_df = storage_cost_schedule_df[['year', 'batt_cost_kwh_res', 'batt_cost_kw_res']]
    res_df.rename(columns={'batt_cost_kwh_res':'batt_cost_per_kwh', 'batt_cost_kw_res':'batt_cost_per_kw'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(storage_cost_schedule_df['year'])
    com_df = storage_cost_schedule_df[['year', 'batt_cost_kwh_nonres', 'batt_cost_kw_nonres']]
    com_df.rename(columns={'batt_cost_kwh_nonres':'batt_cost_per_kwh', 'batt_cost_kw_nonres':'batt_cost_per_kw'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(storage_cost_schedule_df['year'])
    ind_df = storage_cost_schedule_df[['year', 'batt_cost_kwh_nonres', 'batt_cost_kw_nonres']]
    ind_df.rename(columns={'batt_cost_kwh_nonres':'batt_cost_per_kwh', 'batt_cost_kw_nonres':'batt_cost_per_kw'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    storage_cost_schedule_df = pd.concat([res_df, com_df, ind_df], ignore_index=True)
    
    return storage_cost_schedule_df
    
    
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