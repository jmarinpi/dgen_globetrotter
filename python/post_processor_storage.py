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




agent_df_dir = 'agent_df_pickles'

years = np.arange(2014,2051,2)

cols = ['pv_kw_cum', 'batt_kwh_cum', 'pv_kw_ann', 'batt_kwh_ann']
results_df = pd.DataFrame(index=years, columns=cols)


for year in years:
    df_year = pd.read_pickle('%s/df_%s.pkl' % (agent_df_dir, year))
    results_df.loc[year, 'pv_kw_cum'] = np.sum(df_year['pv_kw_cum'])
    results_df.loc[year, 'batt_kwh_cum'] = np.sum(df_year['batt_kwh_cum'])
    results_df.loc[year, 'pv_kw_ann'] = np.sum(df_year['new_pv_kw'])
    results_df.loc[year, 'batt_kwh_ann'] = np.sum(df_year['new_batt_kwh'])


plt.plot(results_df.index+1, results_df['pv_kw_cum'], results_df.index+1, results_df['batt_kwh_cum'])