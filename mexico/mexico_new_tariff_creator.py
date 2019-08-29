#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 29 16:27:56 2019

@author: skoebric
"""

import pandas as pd
import numpy as np
import json

xl = pd.ExcelFile('/Users/skoebric/Documents/NREL-GitHub/dGen/new_mexican_tariffs.xlsx')
sheet_names = xl.sheet_names
sheet_names.pop(0)

XCHNG_RATE = 19
#%%
#CREATE TOU_DICT with 12x24s of weekday and weekend TOU rates
season_dict = {
        'spring':1,
        'summer':2,
        'fall':3,
        'winter':4}

month_season_dict = {
        'BajaCalifornia':[4,4,4,4,2,2,2,2,2,2,4,4],
        'BajaCaliforniaSur':[4,4,4,2,2,2,2,2,2,2,4,4],
        'SIN':[4,1,1,2,2,2,2,3,3,3,4,4]
        }

tou_dict = {
        'GDMTH':{},
        'DIST':{},
        'DIT':{}
        }

for s in sheet_names:
    s_split = s.split('_')
    control_region = s_split[0]
    weekday = np.array(month_season_dict[control_region])
    weekend = np.array(month_season_dict[control_region])
    if s == 'SIN_GDMTH': #the exception
        weekday = np.array(month_season_dict['BajaCaliforniaSur'])
        weekend = np.array(month_season_dict['BajaCaliforniaSur'])
    tariff = s_split[1]
    dfloc = xl.parse(s)

    df_weekday = dfloc[[c for c in dfloc.columns if 'weekday' in c]]
    df_weekend = dfloc[[c for c in dfloc.columns if 'weekend' in c]]

    df_weekday.columns = [season_dict[c.split('_')[0]] for c in df_weekday.columns]
    df_weekend.columns = [season_dict[c.split('_')[0]] for c in df_weekend.columns]

    dict_weekday = {c:df_weekday[c].values for c in df_weekday.columns}
    dict_weekend = {c:df_weekend[c].values for c in df_weekend.columns}

    weekday = np.vstack([dict_weekday[m] for m in weekday])
    weekend = np.vstack([dict_weekend[m] for m in weekend])

    tou_dict[tariff][control_region] = {'weekday':weekday,
                                        'weekend':weekend}

#%%
df = xl.parse('tariff_components')
df['unit'] = [i.lower() for i in df['unit']]
df = df.sort_values(['month'])

import unicodedata
def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

df['region'] = df['region'].apply(remove_accents)
df['region'] = [i.replace(' ', '') for i in df['region']]

rate_id_alias_count = 0

rate_dicts = []

control_reg_id_dict = {'CentroOriente':1,
                       'Sureste':2,
                       'CentroOccidente':3,
                       'Noroeste':4, 
                       'Norte':5,
                       'GolfoNorte':6,
                       'Peninsular':7,
                       'BajaCalifornia':8,
                       'BajaCaliforniaSur':9
                       }

def rates_across_12by24(input_12by24, input_rates):
    max_rate = len(input_rates)
    max_12by24 = np.max(input_12by24) + 1
    if max_rate / 12 != max_12by24:
        print('WARNING, unequal number of rates between tariff and 12by24')
        print('max_rate', max_rate)
        print('max_12by24', max_12by24)
        return 'ERROR'

    output_12by24 = []
    for i in range(0,len(input_12by24)):
        new_zero = i * max_12by24
        old = input_12by24[i]
        new = [j + new_zero for j in old]
        output_12by24.append(new)
    output_vstack = np.vstack(output_12by24)
    return output_vstack


for r in set(df['region']):
    for t in set(df['tariff']):
        e_prices = []
        d_flat_prices = []
        fixed_charges = []
        
        semi_p = True
        
        for m in set(df['month']):
            dfloc = df.loc[(df['region']==r) & (df['month']==m) & (df['tariff']==t)]
            
            if t in ['DB1','DB2','PDBT','GDBT','RABT','RAMT','APBT','APMT','GDMTO']: #nontou
                kwh = dfloc.loc[dfloc['unit']=='kwh']['value'].sum() / XCHNG_RATE
                e_prices.append(kwh)
                e_tou_exists = False
                
            elif t in ['GDMTH','DIST','DIT']: #this is slow, but I don't have time to make more efficient / vectorize
                kwh_nongen = dfloc.loc[(dfloc['component'] != 'Generación') & (dfloc['unit'] == 'kwh')]
                kwh_nongen = kwh_nongen['value'].sum() / XCHNG_RATE
                kwh_gen_df = dfloc.loc[dfloc['component'] == 'Generación']
                base_gen = float(kwh_gen_df.loc[kwh_gen_df['tou_level']=='B']['value'][0:1]) / XCHNG_RATE
                int_gen = float(kwh_gen_df.loc[kwh_gen_df['tou_level']=='I']['value'][0:1]) / XCHNG_RATE
                
                if semi_p:
                    try: #semi gen is the second highest TOU rate, it is only there for some rates, some don't have it, if it doesn't exist, set it to be the same as the highest TOU rate
                        semi_gen = float(kwh_gen_df.loc[kwh_gen_df['tou_level']=='SP']['value'][0:1]) / XCHNG_RATE
                    except TypeError as e:
                        semi_p = False
                        
                if semi_gen == 0:
                    semi_p = False
                
                punta_gen = float(kwh_gen_df.loc[kwh_gen_df['tou_level']=='P']['value'][0:1]) / XCHNG_RATE

                if semi_p:
                    kwh = [base_gen, int_gen, semi_gen, punta_gen]
                else:
                    kwh = [base_gen, int_gen, punta_gen]
                    
                kwh = [i + kwh_nongen for i in kwh]
                e_prices += kwh
                e_tou_exists = True
                
            kw = dfloc.loc[dfloc['unit']=='kw']['value'].sum() / XCHNG_RATE
            fixed = dfloc.loc[dfloc['unit']=='cliente']['value'].sum() / XCHNG_RATE
            d_flat_prices.append(kw)
            fixed_charges.append(fixed)
          
        if t in ['GDMTH','DIST','DIT']: #this is slow, but I don't have time to make more efficient / vectorize
            if r not in ['BajaCalifornia','BajaCaliforniaSur']: #lookup 12by24, some regions have 2 seasons, some have 4
                e_wkend_12by24 = rates_across_12by24(tou_dict[t]['SIN']['weekend'], e_prices)
                if e_wkend_12by24 == 'ERROR':
                    print(r, t, e_prices)
                e_wkday_12by24 = rates_across_12by24(tou_dict[t]['SIN']['weekday'], e_prices)
            else:
                e_wkend_12by24 = rates_across_12by24(tou_dict[t]['SIN']['weekend'], e_prices)

                e_wkday_12by24 = rates_across_12by24(tou_dict[t]['SIN']['weekday'], e_prices)
        elif t in ['DB1','DB2','PDBT','GDBT','RABT','RAMT','APBT','APMT','GDMTO']: #nontou
            dummy_12by24 = np.vstack([([0]*24) for i in range(0,12)])
            e_wkend_12by24 = dummy_12by24
            e_wkday_12by24 = dummy_12by24

        fixed_charge = np.mean(fixed_charges)
        e_n = len(e_prices)
        e_max_difference = max(e_prices) - min(e_prices)

        
        if sum(d_flat_prices) > 0: 
            d_flat_exists = True
        else:
            d_flat_exists = False
            
        try: #lookup control region for dict, not all distribution rates regions map exactly to control regions. 
            control_reg_id = control_reg_id_dict[r]
        except KeyError:
            control_reg_id = 'not used'
        
        rate_dict = {
                    'coincident_peak_exists':False,
                    'd_flat_exists':d_flat_exists,
                    'd_flat_levels':[[1000000000.0, 1000000000.0, 1000000000.0, 1000000000.0, 1000000000.0, 1000000000.0, 1000000000.0, 1000000000.0, 1000000000.0, 1000000000.0, 1000000000.0, 1000000000.0]],
                    'd_flat_n':12,
                    'd_flat_prices':[d_flat_prices],
                    'd_tou_8760':[0]*8760,
                    'd_tou_exists':False,
                    'd_tou_levels':[[0.0]],
                    'd_tou_n':1,
                    'd_tou_prices':[[0.0]],
                    'd_wkday_12by24':np.vstack([([0]*24) for i in range(0,12)]).tolist(),
                    'd_wkend_12by24':np.vstack([([0]*24) for i in range(0,12)]).tolist(),
                    'demand_rate_unit':'kW',
                    'e_exists':True,
                    'e_levels':[[1000000000.0]*e_n], #new mexican rates don't have subsidized consumption levels, they just boot you to the higher rate if you're over consumption limits
                    'e_max_difference':e_max_difference,
                    'e_n':e_n,
                    'e_prices':[e_prices],
                    'e_prices_no_tier':e_prices,
                    'e_tou_exists':e_tou_exists,
                    'e_wkday_12by24':e_wkday_12by24.tolist(),
                    'e_wkend_12by24':e_wkend_12by24.tolist(),
                    'energy_rate_unit':'kWh',
                    'fixed_charge':fixed_charge,
                    'kWh_useage_max':1000000000000000000000000000000000000000000000000000000000000000000, #not actually true, see note above
                    'kWh_useage_min':0,
                    'peak_kW_capacity_max':1000000000000000000000000000000000000000000000000000000000000000000, #also probablly not true
                    'peak_kW_capacity_min':0,
                    'start_day':6, #sunday
                    'rate_id_alias':rate_id_alias_count,
                    'tariff_class':t,
                    'rate_region':r,
                    'control_reg_id':control_reg_id
                    }
        rate_dicts.append(rate_dict)
        rate_id_alias_count += 1

urdb_df = pd.DataFrame(rate_dicts)
urdb_df_dropped = urdb_df.dropna(subset=['control_reg_id'])

rate_jsons = []
for r in rate_dicts:
    rate_id_alias = r['rate_id_alias']
    rate_json = json.dumps(r)
    dict_out = {'rate_id_alias':rate_id_alias,
                'rate_json':rate_json}
    rate_jsons.append(dict_out) #so that it can be packaged as rows in a dataframe

urdb_df_jsons = pd.DataFrame(rate_jsons)
urdb_df_jsons.to_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/base/urdb3_rates.csv', index = False) 


urdb_df_dropped = urdb_df.dropna(subset=['control_reg_id'])

#%%
#switch old tariff_class to new
core_in = pd.read_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/base/agent_core_attributes_all.csv')

def new_tariff_getter(row):
    #reference http://drive.cre.gob.mx/Drive/ObtenerAcuerdoAnexo/?id=111
    old_tariff = str(row['tariff_class'])
    try:    
        load = row['load_per_customer_in_bin_kwh']
    except KeyError:
        if old_tariff in ['DAC','1F','2','3','6']: #for pv_state_starting_capacities, where we don't know customer monthly load, use old rate as heuristic
            load = 151
        else:
            load = 20
        
    if old_tariff in ['1','1A','1B','1C','1D','1E','1F','DAC']:
        if load < 150:
            new_tariff = 'DB1'
        elif load >= 150:
            new_tariff = 'DB2'
    elif old_tariff in ['2','3','6']:
        if load < 25:
            new_tariff = 'PDBT'
        elif load >= 25:
            new_tariff = 'GDBT'
    elif old_tariff in ['9','9CU','9N']:
        new_tariff = 'RABT' #9CU and 9N could also be on RAMT for higher voltage!
    elif old_tariff in ['9M','7']: #not 100% sure 7 belongs here, not in the pdf
        new_tariff = 'RAMT'
    elif old_tariff in ['5','5A']: #could also be in APMT for higher voltage!
        new_tariff = 'APBT'
    elif old_tariff in ['HM','HMC','HMCF','HMF']: #could also include 6 if they want to be on TOU
        new_tariff = 'GDMTH'
    elif old_tariff in ['OM', 'OMF']:
        new_tariff = 'GDMTO'
    elif old_tariff in ['HS','HSL','HSF','HSLF']:
        new_tariff = 'DIST'
    elif old_tariff in ['HT','HTL','HTLF','HTF']:
        new_tariff = 'DIT'
    elif old_tariff in ['1','1A','1B','1C','1D','1E','1F','DAC','DB1','DB2','PDBT','GDBT','RABT','RAMT','APBT','GDMTH','GDMTO','DIST','DIT']:
        new_tariff = old_tariff
    else:
        print('ERROR, old tariff not found')
        print(old_tariff, load)
        return 'ERROR'
    
    return new_tariff
    
def new_rate_id_alias_getter(row):
    control_reg_id = row['control_reg_id']
    new_tariff = row['tariff_class']
    
    new_rate_alias = urdb_df_dropped.loc[(urdb_df_dropped['tariff_class'] == new_tariff) & (urdb_df_dropped['control_reg_id'] == control_reg_id)]
    if len(new_rate_alias) == 1: #sanity check
        new_rate_alias = int(new_rate_alias['rate_id_alias'][0:1])
        return new_rate_alias
    else:
        print('ERROR, multiple tariffs found')
        print(new_rate_alias)
        return 'ERROR'
    
core_out = core_in.copy()
core_out['tariff_class'] = core_out.apply(new_tariff_getter, axis = 1)
core_out['rate_id_alias'] = core_out.apply(new_rate_id_alias_getter, axis = 1)
core_out.to_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/base/agent_core_attributes_all.csv', index=False)
#%%
        
#Aggregate old pv starting capacity
cap_in = pd.read_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/base/pv_state_starting_capacities.csv')
cap_out = cap_in.copy()
cap_out['tariff_class'] = cap_out.apply(new_tariff_getter, axis = 1)
cap_out = cap_out.groupby(['country_abbr','control_reg_id','state_id','sector_abbr','tariff_class'], as_index=False)[['pv_capacity_mw', 'pv_systems_count']].sum()
cap_out.to_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/base/pv_state_starting_capacities.csv', index=False)





