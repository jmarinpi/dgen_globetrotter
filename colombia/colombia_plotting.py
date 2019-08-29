#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 15 19:29:10 2019

@author: skoebric
"""

import pandas as pd

keep_columns = {'control_reg':'first',
                'control_reg_id':'first',
                'state':'first',
                'state_id':'first',
                'departamento':'first',
                'sector_abbr':'first',
                'max_demand_kw':'sum',
                'unsub_cost':'mean',
                'year':'first',
                'customers_in_bin':'sum',
                'load_in_bin_kwh':'sum',
                'max_pv_size':'sum',
                'wholesale_elec_usd_per_kwh':'sum',
                'pv_kw':'sum',
                'npv':'sum',
                'cf':'mean',
                'new_adopters':'sum',
                'new_pv_kw':'sum',
                'number_of_adopters':'sum',
                'new_market_value':'sum',
                'total_gen_twh':'sum',
                'pv_kw_cum':'sum',
                'pv_MW_cum':'sum',
                'market_share':'mean'
                }

keep_columns_muni = {k:v for k,v in keep_columns.items() if k not in ['state_id','state','departamento','sector_abbr']}
keep_columns_dept = {k:v for k,v in keep_columns.items() if k not in ['departamento','sector_abbr']}

numeric = ['max_demand_kw','unsub_cost','customers_in_bin','load_in_bin_kwh','max_pv_size',
           'wholesale_elec_usd_per_kwh','pv_kw','npv','cf','new_adopters','new_pv_kw','number_of_adopters',
           'new_market_value','total_gen_twh','pv_kw_cum','pv_MW_cum','market_share']
#new_adopt_fraction

agent_dfs = {}
for y in [2020, 2030, 2040]:
    agent_df = pd.read_pickle(f'/Users/skoebric/Documents/Colombia/5-16 run/agent_df_{y}.pkl')
    agent_df['pv_MW_cum'] = agent_df['pv_kw_cum'] / 1000
    for c in numeric:
        agent_df[c] = agent_df[c].astype('float')
    all_columns = agent_df.columns
    agent_df = agent_df[list(keep_columns.keys())]
    agent_dfs[y]=agent_df
#agent_df = pd.concat(agent_dfs, axis = 'rows')
#agent_df = agent_df[[']]

#%%
municipio_df = agent_dfs[2040].groupby(['state_id','state','departamento','sector_abbr']).agg(keep_columns_muni).reset_index()

municipio_df_no_sector = municipio_df.groupby(['state_id','state','departamento']).agg(keep_columns_muni).reset_index()

departamento_df = agent_dfs[2040].groupby(['departamento','sector_abbr']).agg(keep_columns_dept).reset_index()

departamento_df_no_sector = agent_dfs[2040].groupby(['departamento']).agg(keep_columns_dept).reset_index()

#

#%%
import geopandas as gpd
import unicodedata
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable

departamento_gdf = gpd.read_file('/Users/skoebric/Dropbox/GitHub/colombia_eda/colombia-departamento.json')
municipio_gdf = gpd.read_file('/Users/skoebric/Dropbox/GitHub/colombia_eda/colombia-municipios.geojson')

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

muni_shape_in = gpd.read_file('reference_data/colombia-municipios.geojson', encoding = 'latin')
muni_shape_in.rename({'NOMBRE_DPT':'departamento','NOMBRE_MPI':'state'}, axis = 'columns', inplace = True)
muni_shape_in['departamento'] = [remove_accents(i) for i in list(muni_shape_in['departamento'])]
muni_shape_in['departamento'] = muni_shape_in['departamento'].replace('SANTAFE DE BOGOTA D.C', 'BOGOTA')
muni_shape_in['state'] = muni_shape_in['state'].replace('SANTAFE DE BOGOTA D.C.', ' D.C.')
muni_shape_in['state'] = [remove_accents(i) for i in list(muni_shape_in['state'])]
muni_shape_in.crs = {'init':'epsg:4326'}

dept_shape_in = gpd.read_file('reference_data/colombia-departamento-simplified.json', encoding = 'latin')
dept_shape_in.rename({'NOMBRE_DPT':'departamento','NOMBRE_MPI':'state'}, axis = 'columns', inplace = True)
dept_shape_in['departamento'] = dept_shape_in['departamento'].replace('SANTAFE DE BOGOTA D.C', 'BOGOTA')
dept_shape_in.crs = {'init':'epsg:4326'}

departamento_gdf['departamento'] = municipio_gdf['NOMBRE_DPT'].apply(remove_accents)

municipio_gdf_no_sector = pd.merge(muni_shape_in[['departamento','state','geometry']], municipio_df_no_sector, on=['departamento','state'])

departamento_gdf_no_sector = pd.merge(dept_shape_in[['departamento','geometry']], departamento_df_no_sector, on=['departamento'])



def colombia_chorpleth(column, level, gdf = None):
    fig, ax = plt.subplots()

    if gdf == None:
        if level == 'muni':
            gdf = municipio_gdf_no_sector
        elif level == 'dept':
            gdf = departamento_gdf_no_sector

    gdf.plot(column=column, cmap='OrRd', scheme='fisher_jenks', k=7, ax=ax, legend = True, legend_kwds={'fontsize':8})
    if level == 'muni':
        muni_shape_in.plot(ax=ax, facecolor='none', edgecolor ='k', linewidth=0.1, alpha=0.8)
        dept_shape_in.plot(ax=ax, facecolor='none', edgecolor ='k', linewidth=0.18, alpha=0.8)
    elif level == 'dept':
        dept_shape_in.plot(ax=ax, facecolor='none', edgecolor ='k', linewidth=0.18, alpha=0.8)

    ax.axis('off')

    fig.tight_layout()

    leg = ax.get_legend()
    leg.set_bbox_to_anchor((1, 0.7, 0.2, 0.2))
    plt.savefig(f"/Users/skoebric/Documents/Colombia/viz/{level}_{column}.png", dpi = 300)


colombia_chorpleth('market_share', 'dept')

#%%
import seaborn as sns

sns.set_style('darkgrid')

capacity_df_in = pd.read_csv('/Users/skoebric/Documents/Colombia/5-16 run/dpv_MW_by_ba_and_year.csv')

core_attributes = agent_df.copy()

control_reg_lookup = dict(zip(set(core_attributes['control_reg_id']), set(core_attributes['control_reg'])))

cap_df = capacity_df_in.diff(axis=1)

cap_df = cap_df.drop(['control_reg_id', '2016'], axis = 'columns')

cap_df['Control Region'] = cap_df.index.map(control_reg_lookup)
cap_df = cap_df.dropna(subset=['Control Region'])
cap_df = cap_df.set_index('Control Region', drop = True)

fig, ax = plt.subplots(figsize = (10,7))
cap_df[cap_df.columns[0:-1]].T.plot(ax=ax)
ax.legend(ncol=3, fontsize = 9, title = 'Control Region').draggable()
ax.set_xlabel('Year', fontsize = 14)
ax.set_xticks(range(0,12))
ax.set_xticklabels(cap_df.columns[0:-1])
ax.set_ylabel('Annual New Capacity (MW)', fontsize = 14)
plt.tight_layout()
plt.savefig(f"/Users/skoebric/Documents/Colombia/viz/incremental_growth.png", dpi = 300)

#%%
long_df = agent_dfs[2040].copy()
long_df['adoption_percent'] = (long_df['pv_MW_cum'] * 1000) / long_df['max_pv_size']

fig, ax = plt.subplots(figsize=(10,7))
sns.scatterplot('adoption_percent','market_share', hue='sector_abbr', size='adoption_percent', data=long_df)

#%%
# long_df = agent_dfs[2040].copy()
# long_df = long_df.groupby(['control_reg','sector_abbr','state']).agg(keep_columns_muni).reset_index()
sns.swarmplot('sector_abbr','unsub_cost', hue='market_share',data=long_df)
