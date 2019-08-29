#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 13:18:13 2019

@author: skoebric
"""
import numpy as np

pg_params = {	"dbname": "dgen_db_fy18q1_naristrc3",
             	"host": "atlas.nrel.gov",
            	"port": "5432",
            	"user": "skoebric",
            	"password": "skoebric"}



import psycopg2 as pg

pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s port=%(port)s' % pg_params

conn = pg.connect(pg_conn_string)

#%%

cur = conn.cursor()

#%%
import pandas as pd

agent_df = pd.read_sql("SELECT * FROM diffusion_mexico.import_agent_core_attributes_all_mex", conn)
pca_mex = pd.read_sql("SELECT * FROM diffusion_mexico.reeds_pca_control_region_lookup", conn)
regions_mex = pd.read_sql("SELECT DISTINCT control_reg, control_reg_id FROM diffusion_mexico.regions", conn)
pca_ca = pd.read_sql("SELECT * FROM diffusion_canada.reeds_pca_province_lookup", conn)

#%%

pca_mex = pca_mex.merge(regions_mex, on='control_reg', how='left')
pca_ca = pca_ca.rename({'province':'control_reg'}, axis='columns')

wholesale_df = pd.read_csv('/Users/skoebric/Documents/NREL-GitHub/dGen/naris_mx/reference_data/wholesale_electricity_prices/ElectricityPriceFordGen.csv')
wholesale_df = wholesale_df.rename({'id':'reeds_pca'}, axis='columns')
wholesale_df = wholesale_df.loc[wholesale_df['scenario'] == 'BAU']
wholesale_df = wholesale_df.loc[wholesale_df['type'] == 'price']
wholesale_df = wholesale_df.loc[wholesale_df['year'] > 2012]
wholesale_df['value'] = wholesale_df['value'] / 1000

wholesale_mex = wholesale_df.loc[wholesale_df['reeds_pca'].isin(list(pca_mex['reeds_pca']))]
wholesale_mex = wholesale_mex.merge(pca_mex[['reeds_pca','control_reg','control_reg_id','total_population']], on='reeds_pca', how='left')

wholesale_ca = wholesale_df.loc[wholesale_df['reeds_pca'].isin(list(pca_ca['reeds_pca']))]
wholesale_ca = wholesale_ca.merge(pca_ca[['reeds_pca','control_reg','control_reg_id']], on='reeds_pca', how='left')

wholesale_us = wholesale_df.loc[~wholesale_df['reeds_pca'].isin(list(pca_ca['reeds_pca']) + list(pca_mex['reeds_pca']))]
#%%
weighted_mean_mex = lambda x: np.average(x, weights=wholesale_mex.loc[x.index, 'total_population'])
weighted_mean_ca = lambda x: np.average(x, weights=wholesale_ca.loc[x.index, 'total_population'])
first_columns = ['control_reg','control_reg_id','country']

mex_wide = wholesale_mex[['control_reg','control_reg_id','year','value']]
mex_wide = mex_wide.groupby(['control_reg','control_reg_id','year'], as_index=False)['value'].mean()
mex_wide = mex_wide.pivot(index='control_reg',columns='year',values='value')
mex_wide = mex_wide.merge(pca_mex[['control_reg','control_reg_id']], on = 'control_reg', how='left')
mex_wide['country'] = 'MEX'
mex_wide = pd.concat([mex_wide[first_columns], mex_wide.drop(first_columns, axis='columns')], axis='columns')
mex_wide = mex_wide.drop_duplicates(subset=['control_reg'], keep='first')
mex_wide.to_csv('/Users/skoebric/Desktop/NARIS_wholesale/wholesale_rates_MX.csv')

# mex_wide_weighted = mex_wide.groupby(['control_reg','control_reg_id','year'], as_index=False).agg({'value':weighted_mean_mex})
# mex_wide_weighted = mex_wide_weighted.pivot(index='control_reg',columns='year',values='value')
# mex_wide_weighted = mex_wide_weighted / 1000
# mex_wide_weighted.to_csv('/Users/skoebric/Desktop/NARIS_wholesale/wholesale_rates_MX.csv')

ca_wide = wholesale_ca[['control_reg','control_reg_id','year','value']]
ca_wide = ca_wide.groupby(['control_reg','control_reg_id','year'], as_index=False)['value'].mean()
ca_wide = ca_wide.pivot(index='control_reg',columns='year',values='value')
ca_wide = ca_wide.merge(pca_ca[['control_reg','control_reg_id']], on = 'control_reg', how='left')
ca_wide['country'] = 'CAN'
ca_wide = pd.concat([ca_wide[first_columns], ca_wide.drop(first_columns, axis='columns')], axis='columns')
ca_wide = ca_wide.drop_duplicates(subset=['control_reg'], keep='first')
ca_wide.to_csv('/Users/skoebric/Desktop/NARIS_wholesale/wholesale_rates_CA.csv')


us_wide = wholesale_us[['reeds_pca','year','value']]
us_wide = us_wide.pivot(index='reeds_pca', columns='year', values='value')
us_wide = us_wide.reset_index(drop=False)
us_wide = us_wide.rename({'reeds_pca':'control_reg'}, axis='columns')
us_wide['country'] = 'USA'
us_wide = pd.concat([us_wide[['country','control_reg']], us_wide.drop(['country','control_reg'], axis='columns')], axis='columns')
us_wide = us_wide.sort_values('control_reg')
us_wide.to_csv('/Users/skoebric/Desktop/NARIS_wholesale/wholesale_rates_US.csv')
