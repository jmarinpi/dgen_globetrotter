#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 22 16:45:27 2019

@author: skoebric
"""
import pandas as pd
import unicodedata
from sklearn.preprocessing import MinMaxScaler
from shutil import copyfile, copytree

irs = pd.read_csv('social_recesion_index.csv')

#function to remove accents from states and municipios
def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

irs['State'] = irs['State'].apply(remove_accents)
irs['state'] = [i.replace(' ', '') for i in irs['State']]

core_attributes = pd.read_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/base/agent_core_attributes_all.csv')
state_id_dict = dict(zip(core_attributes['state'], core_attributes['state_id']))

irs['state_id'] = irs['state'].map(state_id_dict)

output_df = irs[['state','state_id']]
output_df['input_value'] = irs['2015'] #most recent irs value

res_scale_up_pct = 4
com_scale_up_pct = 4
ind_scale_up_pct = 4
scenario_name = 'august_7_19'

#scale discount rates
res_dr_scaler = MinMaxScaler((0.1, (0.1 * (1+res_scale_up_pct))))
output_df['res_discount_rate'] = res_dr_scaler.fit_transform(output_df['input_value'].values.reshape(-1,1))

com_dr_scaler = MinMaxScaler((0.1, (0.1 * (1+com_scale_up_pct))))
output_df['com_discount_rate'] = com_dr_scaler.fit_transform(output_df['input_value'].values.reshape(-1,1))

ind_dr_scaler = MinMaxScaler((0.1, (0.1 * (1+ind_scale_up_pct))))
output_df['ind_discount_rate'] = ind_dr_scaler.fit_transform(output_df['input_value'].values.reshape(-1,1))

#scale loan rates
res_loan_scaler = MinMaxScaler((0.05, (0.05 * (1+res_scale_up_pct))))
output_df['res_loan_rate'] = res_loan_scaler.fit_transform(output_df['input_value'].values.reshape(-1,1))

com_loan_scaler = MinMaxScaler((0.05, (0.05 * (1+com_scale_up_pct))))
output_df['com_loan_rate'] = com_loan_scaler.fit_transform(output_df['input_value'].values.reshape(-1,1))

ind_loan_scaler = MinMaxScaler((0.05, (0.05 * (1+ind_scale_up_pct))))
output_df['ind_loan_rate'] = ind_loan_scaler.fit_transform(output_df['input_value'].values.reshape(-1,1))

#scale down payment
res_dp_scaler = MinMaxScaler((0.2, (0.2 * (1+res_scale_up_pct))))
output_df['res_down_payment'] = res_dp_scaler.fit_transform((output_df['input_value'] * -1).values.reshape(-1,1))

com_dp_scaler = MinMaxScaler((0.2, (0.2 * (1+com_scale_up_pct))))
output_df['com_down_payment'] = com_dp_scaler.fit_transform((output_df['input_value'] * -1).values.reshape(-1,1))

ind_dp_scaler = MinMaxScaler((0.2, (0.2 * (1+ind_scale_up_pct))))
output_df['ind_down_payment'] = ind_dp_scaler.fit_transform((output_df['input_value'] * -1).values.reshape(-1,1))

#disaggregate sectors
output_dfs = []
for i in ['res','com','ind']:
    dfloc = output_df[['state_id',f'{i}_loan_rate',f'{i}_discount_rate',f'{i}_down_payment']]
    dfloc.columns = ['state_id','loan_rate','real_discount','down_payment']
    dfloc['sector_abbr'] = i
    output_dfs.append(dfloc)

output_df = pd.concat(output_dfs, axis ='rows')

#HARD CODE DOWN PAYMENT AND LOAN RATE
output_df['down_payment'] = 0.2
output_df['loan_rate'] = 0.054

#output_df['down_payment'] = 0.2
# copyfile('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/base.xlsm',f'/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/{scenario_name}.xlsm')
copytree('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/base',f'/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/{scenario_name}')
output_df.to_csv(f'/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/{scenario_name}/financing_rates.csv', index=False)