# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 11:16:53 2016

@author: bsigrin
"""

import urllib2
import pandas as pd
import json
import itertools


sectors = ['RES','IDAL','COMM']
regions = ['NEENGL','MDATL','ENC','WNC','SOATL','ESC','WSC','MTN','PCF']
scenarios = ['REF2015','HIGHMACRO','LOWMACRO','LOWPRICE','HIGHRESOURCE','HIGHPRICE']
energy = ['ELC', 'DFO', 'NG', 'PROP']


params_list = [dict(zip(['sector', 'region', 'scenario', 'energy'], t)) for t in itertools.product(sectors, regions, scenarios, energy)]

# Iterate over sector, region, and scenario to pull the table via the API
df1 = pd.DataFrame()
for params in params_list:
     
            url = 'http://api.eia.gov/series/?api_key=7D4BEDC1881B2AAC518E832AC04FF8AA&series_id=AEO.2015.%(scenario)s.PRCE_REAL_%(sector)s_NA_%(energy)s_NA_%(region)s_Y13DLRPMMBTU.A' % params
            
            raw = urllib2.urlopen(url).read()
            jso = json.loads(raw)
            data_series = jso['series'][0]['data']            
            temp_df = pd.DataFrame(data_series)
            temp_df.columns = ['year','dlrs_per_mmbtu']
            for k, v in params.iteritems():
                temp_df[k] = v
                
            df1 = df1.append(temp_df, ignore_index = True)
                   
# The 2015 AEO doesn't have a low resource scenario, so pull that one in a separate call
df2 = pd.DataFrame()
sectors = ['RES','IDAL','CMM']
scenarios = ['LOWRESOURCE']
params_list = [dict(zip(['sector', 'region', 'scenario', 'energy'], t)) for t in itertools.product(sectors, regions, scenarios, energy)]

for params in params_list:
    url = 'http://api.eia.gov/series/?api_key=7D4BEDC1881B2AAC518E832AC04FF8AA&series_id=AEO.2014.%(scenario)s.PRCE_ENE_%(sector)s_NA_%(energy)s_NA_%(region)s_Y12DLRPMMBTU.A' % params
    
    raw = urllib2.urlopen(url).read()
    jso = json.loads(raw)
    data_series = jso['series'][0]['data']            
    temp_df = pd.DataFrame(data_series)
    temp_df.columns = ['year','dlrs_per_mmbtu']
    for k, v in params.iteritems():
        temp_df[k] = v
        
    df2 = df2.append(temp_df, ignore_index = True)

df = df1.append(df2, ignore_index = True)            

#Replace strings with codes we use
df['scenario'] = df['scenario'].replace({'REF2015': 'AEO2015 Reference'}, regex=True)
df['scenario'] = df['scenario'].replace({'HIGHMACRO': 'AEO2015 High Growth'}, regex=True)
df['scenario'] = df['scenario'].replace({'LOWMACRO': 'AEO2015 Low Growth'}, regex=True)
df['scenario'] = df['scenario'].replace({'LOWPRICE': 'AEO2015 Low Prices'}, regex=True)
df['scenario'] = df['scenario'].replace({'HIGHRESOURCE': 'AEO2015 High Resource'}, regex=True)
df['scenario'] = df['scenario'].replace({'HIGHPRICE': 'AEO2015 High Prices'}, regex=True)
df['scenario'] = df['scenario'].replace({'LOWRESOURCE': 'AEO2015 Low Resource'}, regex=True)
          
df['sector'] = df['sector'].replace({'RES': 'res'}, regex=True)
df['sector'] = df['sector'].replace({'IDAL': 'ind'}, regex=True)
df['sector'] = df['sector'].replace({'COMM': 'com'}, regex=True)
df['sector'] = df['sector'].replace({'CMM': 'com'}, regex=True)

df['region'] = df['region'].replace({'NEENGL': 'NE'}, regex=True)
df['region'] = df['region'].replace({'MDATL': 'MA'}, regex=True)
df['region'] = df['region'].replace({'SOATL': 'SA'}, regex=True)
df['region'] = df['region'].replace({'PCF': 'PAC'}, regex=True)

df['energy'] = df['energy'].replace({'ELC': 'electricity'}, regex=True)
df['energy'] = df['energy'].replace({'DFO': 'distallate fuel oil'}, regex=True)
df['energy'] = df['energy'].replace({'NG': 'natural gas'}, regex=True)
df['energy'] = df['energy'].replace({'PROP': 'propane'}, regex=True)



#Two more transformations: Normalize to the 2014 value and extend forecast to 2080 (keep price constant in real terms based on 2040 values)

df['year'] = df['year'].astype(int)
df_2040 = df[df['year'] == 2040]

final_df = df.copy()
for year in range(2041, 2081):
    new_rows = df_2040.copy()
    new_rows['year'] = year
    
    final_df = final_df.append(new_rows, ignore_index = False)
    
final_df.to_csv('/Users/mgleason/NREL_Projects/github/diffusion/sql/data_prep/1_load_and_prep_diffusion_shared_data/2b_eia_state_avg_costs/aeo_data.csv', indices = False)
#sectors = ['res','ind','com']
#regions = ['NE','MA','ENC','WNC','SA','ESC','WSC','MTN','PAC']
#scenarios = ['AEO2015 Reference','AEO2015 High Growth','AEO2015 Low Growth','AEO2015 Low Prices','AEO2015 High Resource','AEO2015 High Prices']
#
#df.year = df.year.astype(float) # just in case
## Subset each unique forecast and then perform operations
#final_df = pd.DataFrame()
#for sector in sectors:
#    for region in regions:
#        for scenario in scenarios:            
#            temp_df = df.query('sector == "%s" and region == "%s" and scenario == "%s"' %(sector,region,scenario))
#            
#            
#            # Find the 2040 value
#            val = float(temp_df.query('year == 2040')['dlrs_per_mmbtu'])
#            temp_df2 = pd.DataFrame({'year':range(2041,2081)})
#            temp_df2['dlrs_per_mmbtu'] = val
#            temp_df2['scenario'] = scenario
#            temp_df2['region'] = region
#            temp_df2['sector'] = sector
#            
#            temp_df3 = temp_df.append(temp_df2, ignore_index = True)
#            final_df = final_df.append(temp_df3, ignore_index = True)
#            