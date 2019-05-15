#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 14 15:15:36 2019

@author: skoebric

Used to scale the discount rate for states based on a social recesion index, which compares the economic development
"""
#%%
import pandas as pd
import unicodedata
from sklearn.preprocessing import MinMaxScaler

irs = pd.read_csv('social_recesion_index.csv')

#function to remove accents from states and municipios
def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

irs['State'] = irs['State'].apply(remove_accents)
irs['state'] = [i.replace(' ', '') for i in irs['State']]

core_attributes = pd.read_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/mex_high_costs/agent_core_attributes_all.csv')
state_ids = set(core_attributes['state_id'])
state_names = set(core_attributes['state'])
state_id_dict = dict(zip(state_names, state_ids))

irs['state_id'] = irs['state'].map(state_id_dict)

res_scaler = MinMaxScaler((.10, .30))
irs['res_capital'] = res_scaler.fit_transform(irs['2015'].values.reshape(-1,1))

com_scaler = MinMaxScaler((.10, .20))
irs['com_capital'] = com_scaler.fit_transform(irs['2015'].values.reshape(-1,1))

ind_scaler = MinMaxScaler((.10, .20))
irs['ind_capital'] = ind_scaler.fit_transform(irs['2015'].values.reshape(-1,1))

dr_dfs = []
for i in ['res','com','ind']:
    dfloc = irs[['state_id','state',f'{i}_capital']]
    dfloc.columns = ['state_id','state','capital_cost']
    dfloc['sector_abbr'] = i
    dr_dfs.append(dfloc)

dr_df = pd.concat(dr_dfs, axis ='rows')
dr_df['inflation'] = 0.0395

def capital_inflation_to_dr(row):
    dr = (row['capital_cost'] - row['inflation']) / (row['inflation'] + 1)
    return dr

dr_df['real_discount'] = dr_df.apply(capital_inflation_to_dr, axis = 1)
print('mean discount rate', dr_df['real_discount'].mean())


dr_df_out = dr_df[['state_id','sector_abbr','real_discount']]

dr_df_out.to_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/mex_high_costs/discount_rates.csv', index=False)