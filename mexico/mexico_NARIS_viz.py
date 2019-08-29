#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 23 09:20:10 2019

@author: skoebric
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import os
import unicodedata

folder = '/Users/skoebric/Documents/dGen/agent_outputs_sens'
files = os.listdir(folder)
dfs = []
for f in files:
    dfloc = pd.read_csv(os.path.join(folder, f))
    dfloc['scenario'] = f.split('_')[0]
    dfloc['sensitivity'] = f.split('_')[1].split('.')[0]
    dfs.append(dfloc)

df = pd.concat(dfs, axis = 'rows')
df['sen'] = df['scenario'] + ' / ' + df['sensitivity']

scen_sen_map = {'20 Percent Down / 9X Discount Scaling' : '20% DP / 9X DR',
                 '50 Percent Down / 9X Discount Scaling': '50% DP / 9X DR',
                 '20 Percent Down / 6X Discount Scaling' : '20% DP / 6X DR',
                 '50 Percent Down / 6X Discount Scaling': '50% DP / 6X DR',
                 '20 Percent Down / 3X Discount Scaling' : '20% DP / 3X DR',
                 '50 Percent Down / 3X Discount Scaling': '50% DP / 3X DR',
                 '50 Percent Down / Fixed Discount': '50% DP / DR Fixed'}

sen_map = {'3X Discount Scaling': '3X DR Scaling',
         '6X Discount Scaling': '6X DR Scaling',
         '9X Discount Scaling': '9X DR Scaling',
         'Fixed Discount': '0 DR Scaling'}

df['sen'] = df['sen'].map(scen_sen_map)
df['sensitivity'] = df['sensitivity'].map(sen_map)

#drop big columns
mem = df.memory_usage(deep=True, index=False).to_dict()
keep_columns = []
for k, v in mem.items():
    if v < 3000000:
        keep_columns.append(k)
df = df[keep_columns]

#join on IRS social index
irs = pd.read_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/reference_data/financing_terms/social_recesion_index.csv')

#function to remove accents from states and municipios
def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

irs['State'] = irs['State'].apply(remove_accents)
irs['state'] = [i.replace(' ', '') for i in irs['State']]

core_attributes = pd.read_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/input_scenarios/mex_high_costs/agent_core_attributes_all.csv')
state_id_dict = dict(zip(core_attributes['state'], core_attributes['state_id']))

irs['state_id'] = irs['state'].map(state_id_dict)
irs['social_index'] = irs['2015']

df = pd.merge(df, irs[['state_id','social_index']], on= 'state_id')

avg_irs_cr_dict = {}
for cr in set(df['control_reg_id']):
    dfloc = df.loc[df['control_reg_id'] == cr]
    avg_irs_cr_dict[cr] = dfloc['social_index'].mean()

df['avg_social_index'] = df['control_reg_id'].map(avg_irs_cr_dict)


#%%
keep_columns = {
                'control_reg_id':'first',
                'max_demand_kw':'sum',
                'customers_in_bin':'sum',
                'number_of_adopters':'sum',
                'load_in_bin_kwh':'sum',
                'loan_rate':'mean',
                'real_discount':'mean',
                'down_payment':'mean',
                'metric_value':'mean',
                'wholesale_elec_usd_per_kwh':'sum',
                'npv':'sum',
                'cf':'mean',
                'new_adopters':'sum',
                'new_pv_kw':'sum',
                'number_of_adopters':'sum',
                'new_market_value':'sum',
                'total_gen_twh':'sum',
                'new_pv_kw':'sum',
                'pv_kw':'sum',
                'pv_kw_cum':'sum',
                'number_of_adopters':'sum',
                'new_market_share':'mean',
                'market_share':'mean',
                'max_market_share':'mean',
                'social_index':'mean',
                'avg_social_index':'first'
                }

df_long = df[list(keep_columns.keys())]

df_long = df.groupby(['control_reg','year','scenario','sensitivity']).agg(keep_columns).reset_index()

df_long['kw_pv_per_customer'] = df_long['pv_kw_cum'] / df_long['customers_in_bin']

df_long = df_long.sort_values(['avg_social_index','sensitivity'], ascending = True)

sns.set(style="ticks", rc={"lines.linewidth": 3})

g = sns.FacetGrid(df_long, col = 'control_reg', row='sensitivity', margin_titles=True, height=2, aspect=1)

def line_plotter_style(x, y, **kwags):
    data = kwags.pop("data")
    ax = plt.gca()
    sns.lineplot(x, y, style = 'scenario', legend = 'brief', ci=None, alpha=0.7, data=data, ax = ax)

    irs_avg = data['social_index'].mean()
    loan_rate_avg = data['loan_rate'].mean()
    discount_rate_avg = data['real_discount'].mean()
    down_payment_avg = data['down_payment'].mean()

    s = f"""
    avg irs: {round(irs_avg, 3)}, loan: {round(loan_rate_avg, 3)},
    disc. rate: {round(discount_rate_avg, 3)}
    """
#    plt.subplots_adjust(bottom=0.2)
    # plt.text(x=2014.5, y=200000, s=s, size = 8)

g = g.map_dataframe(line_plotter_style, "year", "kw_pv_per_customer").add_legend()

#%%

import geopandas as gpd
import pandas as pd
import unicodedata
import folium

low = pd.read_csv('/Users/skoebric/Documents/dGen/agent_outputs_low_costs.csv')
mid = pd.read_csv('/Users/skoebric/Documents/dGen/agent_outputs_mid_costs.csv')
high = pd.read_csv('/Users/skoebric/Documents/dGen/agent_outputs_high_costs.csv')

low_group = low.groupby(['control_reg_id','control_reg','state','state_id','year'], as_index=False)[['pv_kw_cum']].sum()
mid_group = mid.groupby(['control_reg_id','control_reg','state','state_id','year'], as_index=False)[['pv_kw_cum']].sum()
high_group = high.groupby(['control_reg_id','control_reg','state','state_id','year'], as_index=False)[['pv_kw_cum']].sum()

data = low_group.copy()
data = data.rename({'pv_kw_cum':'Low Cost Scenario'}, axis='columns')
data['Mid Cost Scenario'] = mid_group['pv_kw_cum']
data['High Cost Scenario'] = high_group['pv_kw_cum']

state_id_map = dict(zip(data['state'], data['state_id']))

countries = gpd.read_file('/Users/skoebric/Downloads/ne_10m_admin_1_states_provinces/ne_10m_admin_1_states_provinces.shp')
mex_shp = countries.loc[countries['admin'] == 'Mexico']
mex_shp = mex_shp.dropna(subset=['name'])

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

mex_shp['state'] = mex_shp['name'].apply(remove_accents)
mex_shp['state'] = [i.title().replace(' ','').replace('De','de').replace('La','la').replace('QueretaroDeArteaga','Queretaro').replace('Coahuila','CoahuiladeZaragoza').replace('Veracruz','VeracruzdeIgnaciodelaLlave').replace('Michoacan','MichoacandeOcampo') for i in mex_shp['state']]
mex_shp = mex_shp[['state','geometry']]
mex_shp['state_id'] = mex_shp['state'].map(state_id_map)

data = pd.merge(mex_shp, data, how = 'left', on = ['state','state_id'])
data = gpd.GeoDataFrame(data)

#%%
m = folium.Map()
fg = folium.FeatureGroup(name='Mexico Low Adoption Scenario')
for index, row in low_group.iterrows():
    geojson_ = folium.GeoJson(low_group.iloc[index:index+1],
           style_function = lambda feature: {
               'fillColor': feature['properties']['color'],
               'fillOpacity' : 0.7,
               'color': '#000000',
               'weight':0.2
               })

