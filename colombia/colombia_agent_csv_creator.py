#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 14:25:57 2019

@author: skoebric

This script generates most of the .csv files needed for dGen Colombia (agent_core_attributes, urdb3_rates, solar_resources etc.

Python 3!

TODO:
- train bass diffusion model using historical adoption

- wholesale prices should be in constant dollars
- rate_escalations.csv, load_growth_projections.csv could be derived from historical data
- carbon intensities needs to be localized
- can max_market_share_settings.csv be localized?
- roof area needs to be localized

"""

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~ Set-up ~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

import os
import colombia_data_functions as cdf
import pandas as pd
import swifter
import functools
import json
from rasterio.mask import mask
import rasterio
import numpy as np
import pickle

import itertools

from SAM.PySSC import PySSC

import difflib
import unicodedata

from fbprophet import Prophet
import seaborn as sns


EXCHANGE_RATE = 3500 #pesos in 1 USD

# --- Define Helper Functions ---

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~ Create Agents ~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# --- Dict of SUI Data Sources ---
sui_data = cdf.load_sui_data_from_csv()

def grouper(df, value_name):
    """Turn SUI data into longdfs. i.e. consumption, number of subscribers, average bill, etc."""
    clean_value_name = value_name.lower().replace(' ','_')
    df = df.drop(['data_type','Variable Calculada','Total Residencial','Total No Residencial', 'Otros','year'], axis = 'columns')
    df = df.rename({'Departamento':'Departamento',
                    'Municipio':'Municipio',
                    'Empresa':'empresa',
                    'Estrato 1':'estrato_1',
                    'Estrato 2':'estrato_2',
                    'Estrato 3':'estrato_3',
                    'Estrato 4':'estrato_4',
                    'Estrato 5':'estrato_5',
                    'Estrato 6':'estrato_6',
                    'Industrial':'industrial',
                    'Comercial':'comercial',
                    'Oficial':'oficial'
                    }, axis = 'columns')
    df = df.loc[df['land_type'] != 'Total']

    df = pd.melt(df, id_vars = ['Departamento','Municipio','empresa','land_type'],
                 value_vars = ['estrato_1','estrato_2','estrato_3','estrato_4','estrato_5','estrato_6','industrial',
                               'comercial','oficial'],
                 var_name = 'rate_class',
                 value_name = clean_value_name)

    df = df.loc[df[clean_value_name] > 0]
    return df

dfs = []
for k in list(sui_data.keys()):
    dfs.append(grouper(sui_data[k], k))

# --- Merge together dfs to start our agent_df---
agent_df = functools.reduce(lambda x, y: pd.merge(x, y, how='outer', on=['Departamento','Municipio','empresa','land_type','rate_class']), dfs)
agent_df = agent_df.loc[agent_df['consumo'] > 0]

# --- Clean up column names ---
agent_df = agent_df.rename({'empresa':'control_reg',
                            'consumo':'load_in_bin_kwh',
                            'rate_class':'tariff_class',
                            'suscriptores':'customers_in_bin',
                            'consumo_promedio':'load_per_customer_in_bin_kwh'}, axis = 'columns')

agent_df['state'] = agent_df['Municipio']

# --- Rename tariff classes that aren't anaylzed by dGen ---
agent_df.loc[agent_df['tariff_class'] == 'otros', 'tariff_class'] = 'comercial' #simplify oficial and otros rates
agent_df.loc[agent_df['tariff_class'] == 'oficial', 'tariff_class'] = 'comercial'

agent_df = agent_df.reset_index(drop=True)

# --- Group agents with the same geography, tariff, and land_type together ---
df = agent_df.groupby(['control_reg','state','Municipio','Departamento','land_type', 'tariff_class'], as_index = False).sum()

# --- Create dict with control_reg_ids and map it ---
control_reg_dict = {} #control regions are companies
count = 1
for c in set(agent_df['control_reg']):
    if c not in control_reg_dict.keys():
        control_reg_dict[c] = count
        count +=1
agent_df['control_reg_id'] = agent_df['control_reg'].map(control_reg_dict)

# --- Create dict with dept/muni strings (unique) and map to state_id --- 
agent_df['dept_muni_string'] = agent_df['Departamento'] + ' ' + agent_df['state']
state_dict = {} #states are municipios (with Departamentos to seperate multiple muncipios in different Departamentos)
count = 1
for s in set(agent_df['dept_muni_string']):
    if s not in state_dict.keys():
        state_dict[s] = count
        count += 1
agent_df['state_id'] = agent_df['dept_muni_string'].map(state_dict)

# --- Create dict with dept strings and map to an id ---
dept_dict = {}
count = 1
for d in set(agent_df['Departamento']):
    if d not in dept_dict.keys():
        dept_dict[d] = count
        count += 1
agent_df['dept_id'] = agent_df['Departamento']


# --- Map tariff to sector_abbr ---
sector_abbr_dict = {'estrato_1':'res',
                    'estrato_2':'res',
                    'estrato_3':'res',
                    'estrato_4':'res',
                    'estrato_5':'res',
                    'estrato_6':'res',
                    'comercial':'com',
                    'industrial':'ind',
                    'oficial':'com',
                    'otros':'comercial'}
agent_df['sector_abbr'] = agent_df['tariff_class'].map(sector_abbr_dict)

# --- Other constants ---
agent_df['avg_monthly_kwh'] = agent_df['load_per_customer_in_bin_kwh'] / 12
agent_df['owner_occupancy_status'] = 1
agent_df['cap_cost_multiplier'] = 1
agent_df['developable_buildings_pct'] = 0.6
agent_df['bldg_size_class'] = 'small'

# --- Roof sqft ---
roof_dict = {
             'res':10.7639*52.1,
             'com':10.7639*317.6,
             'ind':1.7639*688,
            }

agent_df['developable_roof_sqft'] = agent_df['sector_abbr'].map(roof_dict)


# --- Interconnection Limit ---
interconnection_limit_dict = {'res':1000,
                              'com':5000,
                              'ind':20000}
agent_df['interconnection_limit'] = agent_df['sector_abbr'].map(interconnection_limit_dict)

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~ Geography ~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# --- Merge on Muni Shape Files ---
agent_df = cdf.muni_vs_shape_merger(agent_df) #merge on muni shape files
agent_df = agent_df.dropna(subset=['load_in_bin_kwh']) #get rid of outer merge rows with only geometry
agent_df['geometry'] = agent_df['geometry'].fillna('missing') #no geometry

# --- Clean up ---
agent_df = agent_df.rename({'Departamento':'departamento'}, axis = 'columns')
agent_df = agent_df.drop(['Municipio'], axis = 'columns')

# --- Lookup Elevation for each agent ---
elev_tiff = 'reference_data/colombia_elevation.tif' #Get elevations and add them to unique_dept_set, this is used in deteriming rate e_levels
elev = rasterio.open(elev_tiff)
no_data = elev.nodata

def elevation_mask_getter(row):
    """ Find average elevation for a polygon and a .tif, on-row """
    dfloc = agent_df.loc[agent_df.index == row.name]
    if row['geometry'] != 'missing':
        out_image, out_transform = mask(elev, dfloc['geometry'], crop = True)
        flat_data = out_image.ravel()
        flat_data = np.extract(flat_data != no_data, flat_data)
        mean_data = flat_data.mean()
        return int(mean_data)
    else:
        return 'missing'

def elevation_missing_filler(row):
    """ Fill in missing values with averages for control_reg"""
    if row['elevation'] == 'missing':
        return int(empresa_series.loc[empresa_series['control_reg'] == row['control_reg']]['elevation'])
    else:
        return int(row['elevation'])

# --- Create lookup table of elevation by municipio ---
unique_muni_elev_set = agent_df[['departamento','control_reg','state','geometry']].drop_duplicates(subset = ['departamento','state','control_reg'])
unique_muni_elev_set['elevation'] = unique_muni_elev_set.swifter.apply(elevation_mask_getter, axis = 1)

# --- Create lookuptable of average elevation by control_region ---
empresa_series = unique_muni_elev_set.loc[unique_muni_elev_set['geometry'] != 'missing']
empresa_series['elevation'] = empresa_series['elevation'].astype('int')
empresa_series = pd.DataFrame(empresa_series.groupby('control_reg')['elevation'].mean()).reset_index()

# --- Use empresa_series to fill in missing values for unique_muni_emp_set ---
unique_muni_elev_set['elevation'] = unique_muni_elev_set.swifter.apply(elevation_missing_filler, axis = 1)

# --- Merge on elevation to agent_df ---
agent_df = agent_df.merge(unique_muni_elev_set[['control_reg','state','elevation']], on=['control_reg','state'])

# --- Lookup table with control region, mean load, elevation etc. ---
unique_emp_set = pd.DataFrame(agent_df.groupby('control_reg')[['control_reg_id','load_per_customer_in_bin_kwh']].mean()).reset_index()
unique_emp_set = unique_emp_set.merge(empresa_series, how='inner', on=['control_reg'])

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~ Tariffs ~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# --- Find average unsubsudized tariff ($/kwh) for each tariff ---
df_e4 = agent_df.loc[agent_df['tariff_class'] == 'estrato_4']
df_e3 = agent_df.loc[agent_df['tariff_class'] == 'estrato_3']
tariff_estrato_4 = df_e4.groupby(['control_reg'])['tarifa_media'].mean().to_dict() #lookup on what unsubsidized rate should be for each utility
tariff_estrato_3 = df_e3.groupby(['control_reg'])['tarifa_media'].mean().to_dict() #in case utility doesn't have any estrato 4 customers (sometimes in rural areas)
tariff_estrato_3 = {k:(v*1.15) for k,v in tariff_estrato_3.items() if k not in tariff_estrato_4.keys()}
df_com = agent_df.loc[agent_df['tariff_class'] == 'comercial']
df_ind = agent_df.loc[agent_df['tariff_class'] == 'industrial']
df_of = agent_df.loc[agent_df['tariff_class'] == 'oficial']
df_ot = agent_df.loc[agent_df['tariff_class'] == 'otros']
tariff_com = df_com.groupby(['control_reg'])['tarifa_media'].mean().to_dict()
tariff_ind = df_ind.groupby(['control_reg'])['tarifa_media'].mean().to_dict()
tariff_of = df_of.groupby(['control_reg'])['tarifa_media'].mean().to_dict()
tariff_ot = df_ot.groupby(['control_reg'])['tarifa_media'].mean().to_dict()
#I don't trust 'tarifa_media' at all, it definitely isn't incorporating subsidies, and in some cases it is showing that estrato_1 is paying more than estrato_6.
#I somewhat trust the estrato_4 (which should be paying the actual cost of generation/distribution) based on looking at rates on utility website, it's always very close (~3% off)

# --- create single dict of unsubsidized tarrifs, using the best available data for each geography ---
unsub_cost_dict = {}
unsub_cost_dict.update(tariff_ot) #start with things we want overwritten first, in case utilities are available in estrato_4 or estrato_3
unsub_cost_dict.update(tariff_of)
unsub_cost_dict.update(tariff_ind)
unsub_cost_dict.update(tariff_com)
unsub_cost_dict.update(tariff_estrato_3)
unsub_cost_dict.update(tariff_estrato_4)

agent_df['unsub_cost'] = agent_df['control_reg'].map(unsub_cost_dict)
agent_df['unsub_cost'] = agent_df['unsub_cost'] / EXCHANGE_RATE #peso to usd

# --- add unsubsudized costs to unique_emp_set --- 
unique_emp_set['unsub_cost'] = unique_emp_set['control_reg'].map(unsub_cost_dict)
unique_emp_set['unsub_cost'] = unique_emp_set['unsub_cost'] / EXCHANGE_RATE

# --- blank tariff_dict that's used as a model --- 
with open('reference_data/sample_tariff_dict.pkl', 'rb') as handler:
    urdb_dict = pickle.load(handler)

def urdb_dict_applier(row):
    """Create urdb3 tariff_dicts for each agent"""
    local_urdb = urdb_dict.copy()

    monthly_con = row['load_per_customer_in_bin_kwh'] / 12
    if 'estrato' in row['tariff_class']:
        estrato = int(row['tariff_class'].split('_')[1])
        if estrato < 4:
            if row['elevation'] > 1000:
                sub_limit = 173
            elif row['elevation'] < 1001:
                sub_limit = 130

        else:
            sub_limit = 0

        if monthly_con > (sub_limit * 2): #exceeding CS by 50%, and you don't get CS!!! Although, including an extra 50% here as a buffer to be conservative
           sub_limit = 0
           sub_rate = 1

        # --- Subsidies ---
        if estrato == 1:
            sub_rate = 0.45
        elif estrato == 2:
            sub_rate = 0.55
        elif estrato == 3:
            sub_rate = 0.85
        elif estrato == 4:
            sub_rate = 1
        elif estrato > 4:
            sub_rate = 1.2

    else:
        sub_limit = 0
        sub_rate = 1

    if sub_limit > 0 and sub_rate < 1:
        local_urdb['e_levels'] = [[sub_limit], [1000000000000000000000]]
        local_urdb['e_prices'] = [[row['unsub_cost'] * sub_rate], [row['unsub_cost']]]
    elif sub_limit == 0 and sub_rate > 1:
        local_urdb['e_levels'] = [[1000000000000000000000]]
        local_urdb['e_prices'] = [[row['unsub_cost'] * sub_rate]]
    elif sub_limit == 0 and sub_rate < 1:
        local_urdb['e_levels'] = [[1000000000000000000000]]
        local_urdb['e_prices'] = [[row['unsub_cost']]]
    elif sub_limit == 0 and sub_rate == 1:
        local_urdb['e_levels'] = [[1000000000000000000000]]
        local_urdb['e_prices'] = [[row['unsub_cost']]] #this should maybe be tarifa_media
    else:
        print('ERROR:')
        print(row)
        print('sub rate:',sub_rate, 'sub limit:', sub_limit,'\n')

    local_urdb['e_prices_no_tier'] = [row['unsub_cost']]

    j = json.dumps(local_urdb)
    return j


# --- For each control_reg (unique_empresa_set), construct a tariff dict for each tariff --- 
empresa_dfs = []
for r in ['estrato_1','estrato_2','estrato_3','estrato_4','estrato_5','estrato_6','comercial','industrial']:
    dfloc = unique_emp_set.copy()
    dfloc['tariff_class'] = r
    empresa_dfs.append(dfloc)

empresa_tariff_df = pd.concat(empresa_dfs, axis='rows')
empresa_tariff_df = empresa_tariff_df.reset_index(drop = True)
empresa_tariff_df['rate_json'] = empresa_tariff_df.apply(urdb_dict_applier, axis = 1)
empresa_tariff_df['rate_id_alias'] = empresa_tariff_df.index

# --- Merge on tariff_dict object to each agent ---
agent_df = agent_df.merge(empresa_tariff_df[['rate_id_alias','control_reg','tariff_class']], on = ['control_reg','tariff_class'])

# --- Save the urdb3_rates to a csv ---
empresa_tariff_df = empresa_tariff_df[['rate_json','rate_id_alias']]
empresa_tariff_df.to_json('input_scenarios/base/urdb3_rates.json')
 
#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~ Capacity Factor ~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# --- Create 8760s of capacity factor ---
weather_files_path = '/Users/skoebric/SAM Downloaded Weather Files/' #use SAM download tool to create these for each municipio
weather_files = os.listdir(weather_files_path)

not_found = [] #list of munis that didn't work
ac_list = []

def cf_8760_getter(row):
    """Use SAM to create an 8760 of Capacity Factor for each municipio"""
    dept = row['dept_string']
    dept_lower = dept.lower().replace(' ','_')
    wf_found = 'not found'
    for wf in weather_files:
        if dept_lower in wf:
            wf_found = wf
        continue
    if wf_found == 'not found':
        print(f'Municipio {dept_lower} weather file not found!')
        return 'missing'
    else:
        ssc = PySSC()
        ssc.module_exec_set_print(0)
        data = ssc.data_create()
        ssc.data_set_string( data, b'solar_resource_file', str.encode(os.path.join(weather_files_path,wf_found)))
        ssc.data_set_number( data, b'system_capacity', 1 )
        ssc.data_set_number( data, b'module_type', 0 )
        ssc.data_set_number( data, b'dc_ac_ratio', 1.2 )
        ssc.data_set_number( data, b'inv_eff', 96 )
        ssc.data_set_number( data, b'losses', 14.0756 )
        ssc.data_set_number( data, b'array_type', 0 )
        ssc.data_set_number( data, b'tilt', 20 ) #if we get better roof data, we could put that in here
        ssc.data_set_number( data, b'azimuth', 180 ) #if we get better roof data, we could put that in here
        ssc.data_set_number( data, b'gcr', 0.4 )
        ssc.data_set_number( data, b'adjust:constant', 0 )
        ssc.data_set_number( data, b'adjust:constant', 0 )
        module = ssc.module_create(b'pvwattsv5')
        ssc.module_exec_set_print( 0 )
        if ssc.module_exec(module, data) == 0:
            print ('pvwattsv5 simulation error')
            idx = 1
            msg = ssc.module_log(module, 0)
            while (msg != None):
                print ('	: ' + msg.decode("utf - 8"))
                msg = ssc.module_log(module, idx)
                idx = idx + 1
                SystemExit( "Simulation Error" );
        ssc.module_free(module)
        hourly_ac = ssc.data_get_array(data, b'ac')
        hourly_ac = [round(i,3) for i in hourly_ac]
        return hourly_ac

# --- Calculate percentage of each departamento (by kwh) in each empresa ---
emp_dept_df = agent_df.groupby(['control_reg','departamento'], as_index = False)['load_in_bin_kwh'].sum()
emp_dept_dict = {}
for e in set(emp_dept_df['control_reg']):
    dfloc = emp_dept_df.loc[emp_dept_df['control_reg'] == e]
    consum = dfloc['load_in_bin_kwh'].sum()
    dfloc['pct'] = dfloc['load_in_bin_kwh'] / consum
    dfloc = dfloc.set_index('departamento')
    dfdict = dfloc['pct'].to_dict()
    emp_dept_dict[e] = dfdict

unique_dept_set = pd.DataFrame({'departamento':list(set(agent_df['departamento']))})
unique_dept_set['dept_string'] = unique_dept_set['departamento'] + ' ' + 'Colombia'
unique_dept_set['generation_cf'] = unique_dept_set.swifter.apply(cf_8760_getter, axis = 1)

def empresa_departamento_aggregator(row):
    """
    Because empresas are the control region, and we're not using a server, we need a generation profile for each empresa, this is done by calculating
    a generation profile for each departamento using SAM, and then taking weighted mean of the departamentos in each empresa
    """
    dept_pcts = emp_dept_dict[row['control_reg']]
    output_8760s = []
    for k, v in dept_pcts.items():
        input_8760 = unique_dept_set.loc[unique_dept_set['departamento'] == k]['generation_cf'].item()
        output_8760 = [i * v for i in input_8760]
        output_8760s.append(output_8760)
    output_8760 = np.sum(output_8760s, axis = 0, dtype=np.float32())
    return output_8760

unique_emp_set['cf'] = unique_emp_set.apply(empresa_departamento_aggregator, axis = 1)

solar_resource_hourly = unique_emp_set[['control_reg','control_reg_id','cf']]
solar_resource_hourly.to_json('input_scenarios/base/solar_resource_hourly.json')

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~ Financing Rates ~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
hdi = pd.read_excel('SHDI-Database.xlsx')
hdi = hdi.loc[(hdi['iso_code'] == 'COL') & (hdi['year'] == 2017)]
hdi['region'] = hdi['region'].apply(remove_accents)
hdi['region'] = [i.replace(' ', '').upper().split('(')[0] for i in hdi['region']]

hdi_manual_replace_dict = {'VALLE':'VALLE DEL CAUCA', 'BOGOTAD.C.':'BOGOTA'}
manual_replace_dict = {'SANTAFE DE BOGOTA D.C. BOGOTA, D.C.':'BOGOTA D.C.'}
def match_string_to_code(row, thresh=0.8, str_in = 'dept_muni_string', code_dict = state_dict, manual_dict = manual_replace_dict):
    """Accents, spaces, or other differences in the long Empresa names can throw off mapping.
    This function finds the closest string (slow), and just replaces it with a 4 digit acronym used by XM
    """
    str_in = row[str_in]
    if str_in in manual_dict.keys():
        str_in = manual_dict[str_in]
        
    if str_in in code_dict.keys():
        return code_dict[str_in]
    else:
        str_matches = difflib.get_close_matches(str_in, list(code_dict.keys()), cutoff=thresh, n=1)
        
        if len(str_matches) > 0:
            print(f'Searching for {str_in}, choosing {str_matches[0]}')
            return code_dict[str_matches[0]]
        else:
            print(str_in, 'no matches!')
            return 'missing!'
        
hdi['dept_id'] = hdi.apply(match_string_to_code, axis=1, args=(0.8,'region',dept_dict, hdi_manual_replace_dict))
hdi = hdi.loc[hdi['dept_id'] != 'missing!']

# --- Convert Dept df to Empresa df (control_reg) --- 
id_to_dept_dict= {v:k for k,v in dept_dict.items()}
hdi['dept'] = hdi['dept_id'].map(id_to_dept_dict)

dept_hdi_dict = dict(zip(hdi['dept'], hdi['shdi']))

# --- Scale HDI based on percentage of departamento in each empresa --- 
control_regs = []
control_reg_hdis = []
for k, v in emp_dept_dict.items():
    control_regs.append(k)
    new_hdi = 0
    for dept, scaler in v.items():
        new_hdi += (scaler*dept_hdi_dict[dept])
    control_reg_hdis.append(new_hdi)

control_reg_hdi_df = pd.DataFrame({'control_reg':control_regs, 'hdi':control_reg_hdis})

# --- Create df with sectors and depts ---
fin_dfs = []
for s in ['res','com','ind']:
    c = control_reg_hdi_df.copy()
    c['sector_abbr'] = s
    fin_dfs.append(c)
fin_df = pd.concat(fin_dfs, axis='rows')
fin_df = fin_df.reset_index(drop=True)

hdi_max = fin_df['hdi'].max()
hdi_min = fin_df['hdi'].min()

# --- Input Desired Scale of Discount Rate and Down Payment ---
res_min_dr = 0.05
res_max_dr = 0.15
com_min_dr = 0.05
com_max_dr = 0.10
ind_min_dr = 0.05
ind_max_dr = 0.10

res_min_dp = 0.15
res_max_dp = 0.25
com_min_dp = 0.2
com_max_dp = 0.2
ind_min_dp = 0.2
ind_max_dp = 0.2

# --- Inverse Rescaling ---
fin_df.loc[fin_df['sector_abbr'] == 'res', 'discount_rate'] = ((res_max_dr - res_min_dr)/(hdi_max - hdi_min)) * (hdi_max - fin_df['hdi']) + res_min_dr
fin_df.loc[fin_df['sector_abbr'] == 'com', 'discount_rate'] = ((com_max_dr - com_min_dr)/(hdi_max - hdi_min)) * (hdi_max - fin_df['hdi']) + com_min_dr
fin_df.loc[fin_df['sector_abbr'] == 'ind', 'discount_rate'] = ((ind_max_dr - ind_min_dr)/(hdi_max - hdi_min)) * (hdi_max - fin_df['hdi']) + ind_min_dr

fin_df.loc[fin_df['sector_abbr'] == 'res', 'down_payment'] = ((res_max_dp - res_min_dp)/(hdi_max - hdi_min)) * (hdi_max - fin_df['hdi']) + res_min_dp
fin_df.loc[fin_df['sector_abbr'] == 'com', 'down_payment'] = ((com_max_dp - com_min_dp)/(hdi_max - hdi_min)) * (hdi_max - fin_df['hdi']) + com_min_dp
fin_df.loc[fin_df['sector_abbr'] == 'ind', 'down_payment'] = ((ind_max_dp - ind_min_dp)/(hdi_max - hdi_min)) * (hdi_max - fin_df['hdi']) + ind_min_dp

# Input Desired Loan Rate here ---
fin_df['loan_rate'] = 0.054

fin_df['control_reg_id'] = fin_df['control_reg'].map(control_reg_dict)

fin_df.to_csv('input_scenarios/base/financing_rates.csv', index=False)

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~ Consumption Profiles ~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# --- Fetch SUI Consumption data ---
consumo = sui_data['Consumo']
demand_melt_df = cdf.demand_8760_getter(input_codes = list(set(consumo['Empresa'])))
demand_melt_df_grouped = demand_melt_df.groupby(['Empresa','Fecha','hour'], as_index = False).sum()

# --- Create Average 8760s for each tariff based on a sampling of the utilities that have the most of that type ---
pctdf, mean_demand_df = cdf.mean_8760_getter(df = consumo, demand_melt_df = demand_melt_df)
pctdf = pctdf.drop(['Unnamed: 0', 'year'], axis = 'columns')
mean_demand_df = mean_demand_df.loc[mean_demand_df['Tariff'].isin([f"Estrato {j}" for j in range (1, 7)] + ['Comercial','Industrial','Oficial','Otros'])]

# --- Package 8760s in dict ---
demand_8760_dict = {}
for t in set(mean_demand_df['Tariff']):
    dfloc = mean_demand_df.loc[mean_demand_df['Tariff']==t]
    dfloc = dfloc.sort_values(by = ['Fecha','hour'], ascending = True)
    demand_8760_dict[t.lower().replace(' ', '_')] = list(dfloc['demand_scaled'])

def demand_8760_applier(row):
    """Scale 8760 to utility load"""
    consum = row['load_in_bin_kwh']
    raw_8760 = demand_8760_dict[row['tariff_class']]
    raw_8760_sum = sum(raw_8760)
    scaler = consum / raw_8760_sum
    scaled_8760 = [round(i * scaler, 3) for i in raw_8760]
    return scaled_8760

agent_df['demand_8760'] = agent_df.swifter.apply(demand_8760_applier, axis = 1)
agent_df['max_demand_kw'] = [max(i) for i in agent_df['demand_8760']]
agent_df = agent_df.drop(['demand_8760'], axis = 'columns')

demand_rate_class_df = pd.DataFrame({'tariff_class':list(demand_8760_dict.keys()),
                                     'kwh':list(demand_8760_dict.values())})

for index, row in demand_rate_class_df.iterrows():
    row['kwh'] = np.array([round(i,5) for i in row['kwh']], dtype=np.float32())

# --- Save Load Profiles to csv ---
demand_rate_class_df.to_json('input_scenarios/base/normalized_load.json')

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~ Agent_df Output ~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# --- Select needed columns --- 
agent_df_out = agent_df[['control_reg','control_reg_id','state_id','state','sector_abbr','tariff_class','developable_roof_sqft',
                         'customers_in_bin','load_in_bin_kwh','load_per_customer_in_bin_kwh','max_demand_kw','avg_monthly_kwh', 'interconnection_limit',
                         'owner_occupancy_status','cap_cost_multiplier','developable_buildings_pct','bldg_size_class','rate_id_alias','elevation','unsub_cost','departamento']]

# --- Create agent_id ---
agent_df_out = agent_df_out.reset_index(drop=True)
agent_df_out['agent_id'] = agent_df_out.index

agent_df_out.to_csv('input_scenarios/base/agent_core_attributes_all.csv', index = False)

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~ Load Growth ~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# --- Create Dataframe from permutations, as values are all similar ---
load_growth_df = pd.DataFrame().from_records(itertools.product(['Low','Planning','High'],range(2015,2051), ['res','com','ind'], ['COL'], range(1,43)))
load_growth_df.columns = ['scenario','year','sector_abbr','control_reg_id']

load_growth_df.loc[load_growth_df['scenario'] == 'Low', 'load_multiplier'] = 1.024
load_growth_df.loc[load_growth_df['scenario'] == 'Planning', 'load_multiplier'] = 1.034
load_growth_df.loc[load_growth_df['scenario'] == 'High', 'load_multiplier'] = 1.044

load_growth_df.to_csv('input_scenarios/base/load_growth_projections.csv', index = False)

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~ NEM Settings ~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# --- Create Dataframe from permutations, as values are all similar ---
nem_df = pd.DataFrame().from_records(itertools.product(['res','com','ind'], range(1,43), [1000], range(2015,2051)))
nem_df.columns = ['sector_abbr','control_reg_id','nem_system_size_limit_kw','year']
nem_df.loc[nem_df['sector_abbr']=='com', 'nem_system_size_limit_kw'] = 5000 #https://www.pv-magazine.com/2018/03/05/colombia-issues-regulation-for-solar-distributed-generation/
nem_df.loc[nem_df['sector_abbr']=='ind', 'nem_system_size_limit_kw'] = 20000
nem_df.to_csv('input_scenarios/base/nem_settings.csv', index = False)

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~ Bass Paramaters ~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

bass_df = unique_muni_elev_set.merge(agent_df[['control_reg','state','control_reg_id','state_id']], how = 'outer', on = ['control_reg','state'])
bass_df = bass_df.drop_duplicates(subset = ['control_reg','state'])
bass_df = bass_df[['control_reg_id','state_id']]

bass_dfs = []
for r in ['res','com','ind']:
    dfloc = bass_df.copy()
    dfloc['sector_abbr'] = r
    bass_dfs.append(dfloc)

bass_df = pd.concat(bass_dfs, axis = 'rows')
bass_df['p'] = 0.00285301
bass_df['q'] = 0.573395
bass_df['teq_yr1'] = 2
bass_df['tech'] = 'solar'

bass_df.to_csv('input_scenarios/base/pv_bass.csv', index = False)

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~ Existing Capacity ~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

existing_cap_df = pd.read_excel('/Users/skoebric/Documents/NREL-GitHub/dGen/colombia/reference_data/existing_generation.xlsx')
existing_pv = existing_cap_df.loc[existing_cap_df['Recurso'] == 'SOL']
existing_pv = existing_pv.loc[existing_pv['Capacidad MW'] < 5]
existing_pv = existing_pv.loc[existing_pv['Fecha Proyecto'] < '2017']
existing_pv['size_class'] = 'small'
existing_pv.loc[existing_pv['Capacidad MW'] > 0.99, 'size_class'] = 'large'
existing_pv['project_count'] = 1
existing_pv = existing_pv[['Departamento','Municipio','size_class','Capacidad MW', 'project_count']]
existing_group = existing_pv.groupby(['Departamento','Municipio','size_class'], as_index=False).sum()
existing_group = existing_group.rename({'Capacidad MW':'MW_pv'}, axis='columns')
existing_group['dept_muni_string'] = existing_group['Departamento'] + ' ' + existing_group['Municipio']

existing_group['dept_muni_string'] = [remove_accents(i) for i in existing_group['dept_muni_string']]

existing_group['state_id'] = existing_group.apply(match_string_to_code, axis=1)
existing_group = existing_group.loc[existing_group['state_id'] != 'missing!']

agent_df['size_class'] = 'large'
agent_df.loc[agent_df['sector_abbr'] == 'res', 'size_class'] = 'small'

def existing_muni_cap_to_rate_class_cap(row):
    state_id = row['state_id']
    size_class = row['size_class']
    customer_load = row['load_in_bin_kwh']
        
    state_cap_df = existing_group.loc[(existing_group['state_id'] == state_id) & (existing_group['size_class'] == size_class)]
    df_state_loc = agent_df.loc[(agent_df['state_id'] == state_id) & (agent_df['size_class'] == size_class)]
    total_load_in_state = df_state_loc['load_in_bin_kwh'].sum()
    total_existing_cap = state_cap_df['MW_pv'].sum()
    total_existing_projects = state_cap_df['project_count'].sum()
    
    frac = customer_load / total_load_in_state
    
    num_systems_each = total_existing_projects * frac
    if num_systems_each > 0 and num_systems_each < 1:
        num_systems_each = 1 #round fractions up to 1 system regardless of size
    else:
        num_systems_each = round(num_systems_each)
    
    MW_capacity_each = total_existing_cap * frac

    return (num_systems_each, MW_capacity_each)
    
agent_df['existing_tuple'] = agent_df.apply(existing_muni_cap_to_rate_class_cap, axis = 1)
agent_df['pv_capacity_mw'] = [i[1] for i in agent_df['existing_tuple']]
agent_df['pv_systems_count'] = [i[0] for i in agent_df['existing_tuple']]
    
ex_pv_df = agent_df[['control_reg_id','state_id','tariff_class','sector_abbr','pv_capacity_mw','pv_systems_count']]
ex_pv_df = ex_pv_df.groupby(['control_reg_id','state_id','tariff_class','sector_abbr'], as_index=False).sum()

ex_pv_df.to_csv('input_scenarios/base/pv_state_starting_capacities.csv', index = False)
#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~ Rate Escalations ~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
# --- Create Dataframe from permutations, as values are all similar ---
#this needs to be tuned more using historic data
rate_esc_df = pd.DataFrame().from_records(itertools.product(['Planning','Low','High'], range(2015, 2051), ['res','com','ind'], ['COL'], range(1,43)))
rate_esc_df.columns = ['source','year','sector_abbr','control_reg_id']

def escalation_factor_applier(row):
    multiplier = row['year'] - 2015
    if row['source'] == 'Planning':
        esc = 1 + (multiplier * .01)
    if row['source'] == 'Low':
        esc = 1 + (multiplier * .005)
    if row['source'] == 'High':
        esc = 1 + (multiplier * .02)
    return esc

rate_esc_df['escalation_factor'] = rate_esc_df.apply(escalation_factor_applier, axis = 1)
rate_esc_df.to_csv('input_scenarios/base/rate_escalations.csv', index = False)

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~ Wholesale Prices ~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
#wholesale prices
#currently, the average hourly prices for the year are averged, would be better to weight this better
#data does not disaggregate by empresa

# --- Read and Clean Historic Wholesale Prices ---
wholesale_dir = 'reference_data/wholesale_electricity_prices'
wholesale_files = os.listdir('reference_data/wholesale_electricity_prices')

good_columns = [str(i) for i in range(0, 24)]

wholesale_dfs = []
for f in wholesale_files:
    if '.xlsx' in f:
        path = wholesale_dir + '/' + f
        dfloc = pd.read_excel(path, skiprows = 2)
        dfloc = dfloc[good_columns  + ['Fecha']]
        wholesale_dfs.append(dfloc)

wholesale_df = pd.concat(wholesale_dfs, axis='rows')
wholesale_df = wholesale_df.dropna(how='any')
wholesale_df['ds'] = pd.to_datetime(wholesale_df['Fecha'])
wholesale_df['y_real'] = wholesale_df[good_columns].mean(axis=1)
wholesale_df['cap'] = wholesale_df[good_columns].max(axis=1)
wholesale_df['year'] = [i.year for i in wholesale_df.ds]
wholesale_df['month'] = [i.month for i in wholesale_df.ds]
wholesale_df = wholesale_df.groupby(['year','month'], as_index = False)['y_real'].mean()
wholesale_df['day'] = 1

# --- Add one hot column with El-Nino ---
def el_nino_applier(ds):
    date = pd.to_datetime(ds)
    if date.year in [2002, 2003, 2009, 2010, 2015, 2016, 2022, 2023, 2029, 2030, 2036, 2037, 2043, 2044] :
        return 1 #based on historical years
    else:
        return 0

wholesale_df['ds'] = pd.to_datetime(wholesale_df[['year','month','day']])
wholesale_df['el_nino'] = wholesale_df['ds'].apply(el_nino_applier)

#back of the envelope monthly inflation calculation
annual_inflation = 0.04
monthly_inflation = annual_inflation / 12
n_months = len(wholesale_df)
end_inflation = monthly_inflation*n_months
inflation_list = list(np.arange(0,end_inflation,monthly_inflation))
wholesale_df['inflation'] = inflation_list
wholesale_df['y_adj'] = wholesale_df['y_real'] * (1 - wholesale_df['inflation']) #adjust for inflation
wholesale_df['y'] = wholesale_df['y_adj'].rolling(12).mean()

# --- Run Prophet model ---
wholesale_prophet = wholesale_df[['ds','y','el_nino']]
wholesale_prophet['y'] = wholesale_prophet['y'] / EXCHANGE_RATE

m = Prophet(seasonality_mode = 'multiplicative', interval_width=0.5, yearly_seasonality=True)
m.add_regressor('el_nino')
m.fit(wholesale_prophet)

future = m.make_future_dataframe(periods=34, freq='Y')
future['el_nino'] = future['ds'].apply(el_nino_applier)

forecast = m.predict(future)

forecast['year'] = [i.year for i in forecast['ds']]
forecast_out = forecast.groupby('year')['yhat'].mean()
forecast_out = round(forecast_out,3)

prophet_out = pd.DataFrame([forecast_out], index = [i+1 for i in empresa_series.index], columns = forecast_out.index)
prophet_out['control_reg_id'] = prophet_out.index

years_include = [2014,2016,2018,2020,2022,2024,2026,2028,2030,2032,2034,2036,2038,2040,2042,2044,2046,2048,2050]

prophet_out = prophet_out[['control_reg_id'] + years_include]
prophet_out.to_csv('input_scenarios/base/wholesale_rates.csv', index = False)

# m.plot(forecast, xlabel = 'Date', ylabel = 'Cost')