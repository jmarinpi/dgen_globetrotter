# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 10:23:36 2016

@author: pgagnon
"""

import numpy as np
import pandas as pd

# Import from support function repo
import sys
sys.path.append('C:/users/pgagnon/desktop/support_functions/python')
import dispatch_functions as dFuncs
import tariff_functions as tFuncs
import financial_functions as fFuncs
import general_functions as gFuncs
import matplotlib.pyplot as plt




agent_df_dir = 'c:/users/pgagnon/desktop/diffusion/runs'
run_name = '/results_20170106_122800'

years = np.arange(2014,2039,2)


sectors = ['res', 'com', 'ind']
cols = ['pv_kw_cum', 'batt_kwh_cum', 'pv_kw_ann', 'batt_kwh_ann']

results_df = pd.DataFrame(index=years, columns=cols)

for year in years:
    print year
    df_year = pd.read_pickle('%s/agent_df_%s.pkl' % (agent_df_dir+run_name+'/BAU', year))
    results_df.loc[year, 'pv_kw_cum'] = np.sum(df_year['pv_kw_cum'])
    results_df.loc[year, 'batt_kwh_cum'] = np.sum(df_year['batt_kwh_cum'])
    results_df.loc[year, 'pv_kw_ann'] = np.sum(df_year['new_pv_kw'])
    results_df.loc[year, 'batt_kwh_ann'] = np.sum(df_year['new_batt_kwh'])
    results_df.loc[year, 'batt_kw_cum'] = np.sum(df_year['batt_kw_cum'])
    results_df.loc[year, 'batt_kw_ann'] = np.sum(df_year['new_batt_kw'])
    for sector in sectors:
        df_year_sector = df_year[df_year['sector_abbr']==sector]
        results_df.loc[year, 'pv_kw_cum_%s' % sector] = np.sum(df_year_sector['pv_kw_cum'])
        results_df.loc[year, 'batt_kwh_cum_%s' % sector] = np.sum(df_year_sector['batt_kwh_cum'])
        results_df.loc[year, 'pv_kw_ann_%s' % sector] = np.sum(df_year_sector['new_pv_kw'])
        results_df.loc[year, 'batt_kwh_ann_%s' % sector] = np.sum(df_year_sector['new_batt_kwh'])
        results_df.loc[year, 'batt_kw_cum_%s' % sector] = np.sum(df_year_sector['batt_kw_cum'])
        results_df.loc[year, 'batt_kw_ann_%s' % sector] = np.sum(df_year_sector['new_batt_kw'])
        
#%%
plt.figure(0)
plt.plot(results_df.index+1, results_df['pv_kw_cum']/1e6, results_df.index+1, results_df['batt_kwh_cum']/1e6)
plt.grid(True)
plt.ylabel('GWdc')
plt.legend(['PV', 'Storage'])
plt.title('pv and storage adoption')
plt.axis([2015, 2050, 0, np.max(results_df['pv_kw_cum']/1e6*1.2)])

#%%
plt.figure(1)
ratio = results_df['batt_kw_ann'] / results_df['pv_kw_ann']
plt.plot(results_df.index+1, ratio)
plt.title('annual ratio of batt_kw/pv_kw')
plt.grid(True)
#%%
plt.figure(2)
plt.plot(results_df.index+1, results_df['pv_kw_cum']/1000)
plt.plot(results_df.index+1, results_df['pv_kw_cum_res']/1000)
plt.plot(results_df.index+1, results_df['pv_kw_cum_com']/1000)
plt.plot(results_df.index+1, results_df['pv_kw_cum_ind']/1000)
plt.title('sector breakdown of MW of pv adoption')
plt.legend(['total', 'res', 'com', 'ind'])
plt.grid(True)

#%%
plt.figure(3)
plt.plot(results_df.index+1, results_df['batt_kw_cum']/1000)
plt.plot(results_df.index+1, results_df['batt_kw_cum_res']/1000)
plt.plot(results_df.index+1, results_df['batt_kw_cum_com']/1000)
plt.plot(results_df.index+1, results_df['batt_kw_cum_ind']/1000)
plt.title('sector breakdown of MW of battery adoption')
plt.legend(['total', 'res', 'com', 'ind'])
plt.grid(True)


