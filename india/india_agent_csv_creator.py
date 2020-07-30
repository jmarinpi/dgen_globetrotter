#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 14:25:57 2019

@author: skoebric
"""

"""
TODO:
    - confirm how net metering is read in
    - create agent csv with data we already have
    - move agent creation into dgen_model based on params in config? 
"""

# --- Python Battery Imports ---
import os
import itertools
import json
from distutils.dir_util import copy_tree

# --- External Library Imports ---
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.ops import nearest_points
from shapely.geometry import Point, shape
import shapely

# --- Module Imports ---
import agent_config as config
import helper
import agent_sampling as samp

pd.options.mode.chained_assignment = None

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~ Functions ~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# --- read lookups ---
state_id_lookup = pd.read_csv(os.path.join('reference_data', 'india_census','state_id_lookup.csv'))
state_id_lookup = dict(zip(state_id_lookup['state_name'], state_id_lookup['state_id']))

def wholesale_rates():
    """
    Net Billing avoided cost. Creates 'wholesale_elec_usd_per_kwh'. 
    Used if 'compensation_style' == 'Net Billing (Wholesale)'
        

    Columns
    -------
    state_id (int) : lookup from census data
    2014-2050 (float) : annual value
    """
    
    reeds = pd.read_csv(os.path.join('reference_data','reeds_output_margcost_state.csv'))
    reeds.columns = ['state_name','year','scenario','variable','cost']
    reeds = reeds.loc[reeds['scenario'] == 'Base']
    reeds = reeds.loc[reeds['variable'] == 'mc.total']
    
    # --- pivot to wide ---
    pivoted = reeds.pivot_table(index=['state_name'], columns=['year'], values='cost')
    pivoted = pivoted.reset_index(drop=False)
    pivoted['state_name'] = pivoted['state_name'].apply(helper.sanitize_string)
    pivoted['state_name'] = pivoted['state_name'].replace('delhi', 'nct_of_delhi')
    pivoted.loc[pivoted['state_name'] != 'telangana']
    
    # --- add in earlier years ---
    annual_diff = (pivoted[2018] - pivoted[2017]) / pivoted[2018]
    for y_index, y in enumerate([2014,2015,2016]):
        pivoted[y] = pivoted[2017] * (1 - annual_diff)**(3-y_index)
        
    # --- add in later years ---
    annual_diff = (pivoted[2047] - pivoted[2046]) / pivoted[2047]
    for y_index, y in enumerate([2048,2049,2050]):
        pivoted[y] = pivoted[2047] * (1 + annual_diff)**(y_index + 1)
    
    # --- add state_id ---
    pivoted['state_id'] = pivoted['state_name'].map(state_id_lookup)
    
    # --- currency conversion ---
    pivoted[list(range(2014,2051))] = pivoted[list(range(2014,2051))] / config.RUPPES_TO_USD
    
    # --- reorder columns ---
    wholesale_rates = pivoted[['state_id'] + list(range(2014,2051))]
    wholesale_rates.to_csv(os.path.join('india_base','wholesale_rates.csv'), index=False)

def financing_rates(agent_df):
    """
    Create .csv with discount rates by sector/geography, scaled by a social indicator score.

    Columns
    -------
        state_id (int) : integer representation of state
        sector_abbr (str) : the sector of the agent
        loan_rate (float) : the annual interest rate on a loan. 
        real_discount (float) : the discount rate of the state/sector in percent
        down_payment (float) : percent of downpayment towards system (typically 0.2 to compare apples to apples in WACC)
    """
    # --- Take dict of controlregions by social indicator ---
    state_social_df = agent_df[['state_id','social_indicator']].drop_duplicates()

    # --- Permute by sector ---
    social_dfs = []
    for s in ['res','com','ind']:
        _state_social_df = state_social_df.copy()
        _state_social_df['sector_abbr'] = s
        social_dfs.append(_state_social_df)
    finance_df = pd.concat(social_dfs, axis='rows')
    finance_df = finance_df.reset_index(drop=True)

    social_max = finance_df['social_indicator'].max()
    social_min = finance_df['social_indicator'].min()

    #TODO: I'm guessing this will break when MAX_DP and MIN_DP are the same because of division by zero
    # --- inverse normalization of discount rate (i.e. lower social indicator has higher discount rate) ---
    finance_df.loc[finance_df['sector_abbr'] == 'res', 'discount_rate'] = ((config.RES_MAX_DR - config.RES_MIN_DR)/(social_max - social_min)) * (social_max - finance_df['social_indicator']) + config.RES_MIN_DR
    finance_df.loc[finance_df['sector_abbr'] == 'com', 'discount_rate'] = ((config.COM_MAX_DR - config.COM_MIN_DR)/(social_max - social_min)) * (social_max - finance_df['social_indicator']) + config.COM_MIN_DR
    finance_df.loc[finance_df['sector_abbr'] == 'ind', 'discount_rate'] = ((config.IND_MAX_DR - config.IND_MIN_DR)/(social_max - social_min)) * (social_max - finance_df['social_indicator']) + config.IND_MIN_DR

    # --- inverse normalization of loan rate(i.e. lower social indicator has higher loan rate ---
    finance_df.loc[finance_df['sector_abbr'] == 'res', 'loan_rate'] = ((config.RES_MAX_LR - config.RES_MIN_LR)/(social_max - social_min)) * (finance_df['social_indicator'] - social_max) + config.RES_MIN_LR
    finance_df.loc[finance_df['sector_abbr'] == 'com', 'loan_rate'] = ((config.COM_MAX_LR - config.COM_MIN_LR)/(social_max - social_min)) * (finance_df['social_indicator'] - social_max) + config.COM_MIN_LR
    finance_df.loc[finance_df['sector_abbr'] == 'ind', 'loan_rate'] = ((config.IND_MAX_LR - config.IND_MIN_LR)/(social_max - social_min)) * (finance_df['social_indicator'] - social_max) + config.IND_MIN_LR

    # --- normalization of down payment (i.e. lower social indicator has lower down payment ---
    finance_df.loc[finance_df['sector_abbr'] == 'res', 'down_payment'] = ((config.RES_MAX_DP - config.RES_MIN_DP)/(social_max - social_min)) * (finance_df['social_indicator'] - social_max) + config.RES_MIN_DP
    finance_df.loc[finance_df['sector_abbr'] == 'com', 'down_payment'] = ((config.COM_MAX_DP - config.COM_MIN_DP)/(social_max - social_min)) * (finance_df['social_indicator'] - social_max) + config.COM_MIN_DP
    finance_df.loc[finance_df['sector_abbr'] == 'ind', 'down_payment'] = ((config.IND_MAX_DP - config.IND_MIN_DP)/(social_max - social_min)) * (finance_df['social_indicator'] - social_max) + config.IND_MIN_DP

    # --- Write to csv ---
    finance_df.to_csv(os.path.join('india_base','financing_rates.csv'), index=False)


def load_growth(agent_df):
    """
    Create csv with annual load growth pct by geography.

    Columns
    -------
    scenario (str) : matches the string from the input sheet
    year (int) : year of load growth relative to 2014
    sector_abbr (str) : the sector of the agent
    load_multiplier (float) : load growth relative to 2014

    Methodology
    -----------
    Take ReEDS Load Time Slice Hourly Load by State, average by year
        
    Assumptions 
    -----------
    Currently assumes that all sectors have the same load growth. Could use 'CEA_historic_consumption_by_sector.csv' to normalize this by sector.
    """

    reeds_load = pd.read_csv(os.path.join('reference_data','ReEDS_load.csv'), names=['state','hour','year','value'])

    # --- Group by year ---
    reeds_load = reeds_load.groupby(['state','year'], as_index=False)['value'].mean()

    # --- Pivot Wide ---
    reeds_load = pd.pivot_table(reeds_load, index='state', columns='year', values='value')

    # --- Convert to pct diff ---
    reeds_load = reeds_load.pct_change(axis=1)

    # --- Add missing years ---
    for y in range(2014,2018,1): # not in df
        reeds_load[y] = np.nan

    for y in range(2048,2051,1): # not in df
        reeds_load[y] = np.nan
        
    reeds_load.sort_index(axis=1, inplace=True)

    # --- Add Previous Years --
    reeds_load = reeds_load.fillna(method='bfill', axis=1).fillna(method='ffill', axis=1)

    long_dfs = []
    # --- Calculate cumulative product ---
    for scenario, multiplier in zip(['Low','Planning','High'],[config.LOW_LOAD_GROWTH, config.PLANNING_LOAD_GROWTH, config.HIGH_LOAD_GROWTH]):
        reeds_load += 1
        reeds_load *= multiplier
        reeds_load = reeds_load.cumprod(axis=1)

        # --- Convert back to long_df ---
        load_growth = reeds_load.copy()
        load_growth.reset_index(inplace=True)
        load_growth = load_growth.melt(id_vars=['state'], var_name=['year'], value_name='new_load_growth')

        for c in ['residential', 'commercial', 'industrial', 'agriculture']:
            load_growth[c] = load_growth['new_load_growth'] #TODO: implement sector specific load growth
        load_growth.drop('new_load_growth', axis='columns', inplace=True)

        load_growth = load_growth.melt(id_vars=['state','year'], var_name='sector_abbr', value_name='load_multiplier')
        load_growth['scenario'] = scenario
        long_dfs.append(load_growth)

    load_growth = pd.concat(long_dfs, axis='rows')

    # --- map state_id ---
    load_growth['state'] = load_growth['state'].apply(helper.sanitize_string)
    load_growth['state_id'] = load_growth['state'].map(state_id_lookup)
    load_growth.drop(['state'], axis='columns')
    load_growth['sector_abbr'] = load_growth['sector_abbr'].map({'residential':'res','commercial':'com', 'industrial':'ind', 'agricutlure':'agg'})

    load_growth.to_csv(os.path.join('india_base','load_growth_projections.csv'), index=False)


def nem_settings(agent_df):
    """
    Create nem_settings.csv based on config variables. 

    Columns
    -------
        sector_abbr (str) : the sector of the agent
        year (int) : year for policy details
        nem_system_size_limit_kw (int) : size limit for individual agent system size (kW)
        year_end_excess_sell_rate_usd_per_kwh (float) : payment for excess genration at end of year TODO how is this used? 
    """

    # --- Create Dataframe from permutations, as values are all similar ---
    nem_df = pd.DataFrame().from_records(itertools.product(['res','com','ind'], list(set(agent_df['state_id'])), range(2015,2051)))
    nem_df.columns = ['sector_abbr','state_id','year']
    nem_df.loc[nem_df['sector_abbr']=='res', 'nem_system_size_limit_kw'] = config.RES_NEM_KW_LIMIT
    nem_df.loc[nem_df['sector_abbr']=='com', 'nem_system_size_limit_kw'] = config.COM_NEM_KW_LIMIT
    nem_df.loc[nem_df['sector_abbr']=='ind', 'nem_system_size_limit_kw'] = config.IND_NEM_KW_LIMIT

    # --- Define Compensation Style for each State --- #TODO: update this based on cstep data
    nem_df['compensation_style'] = 'Net Metering'

    nem_df.to_csv(os.path.join('india_base','nem_settings.csv'), index = False)
    
def rate_escalations(agent_df):
    """
    Create rate_escalations.csv based on compound increase of config values. 

    Columns
    -------
        source (str) : rate growth planning scenario, from input sheet
        state_id (int) : integer representation of state
        sector_abbr (str) : the sector of the agent
        year (int) : year of rate escalation relative to 2014
        escalation_factor (float) : multiplier of rate escalation relative to 2014
    """

    # --- Create Dataframe from permutations, as values are all similar ---
    rate_esc_df = pd.DataFrame().from_records(itertools.product(['Planning','Low','High'], range(2015, 2051), ['res','com','ind'], list(set(agent_df['state_id']))))
    rate_esc_df.columns = ['source','year','sector_abbr','state_id']

    def escalation_factor_applier(row):
        multiplier = row['year'] - 2015
        if row['source'] == 'Planning':
            esc = 1 + (multiplier * config.PLANNING_RATE_ESCALATION)
        if row['source'] == 'Low':
            esc = 1 + (multiplier * config.LOW_RATE_ESCALATION)
        if row['source'] == 'High':
            esc = 1 + (multiplier * config.HIGH_RATE_ESCALATION)
        return esc

    rate_esc_df['escalation_factor'] = rate_esc_df.apply(escalation_factor_applier, axis = 1)
    rate_esc_df.to_csv(os.path.join('india_base','rate_escalations.csv'), index = False)

def pv_state_starting_capacities():
    """
    Columns
    -------
        state_id (int) : integer representation of state
        state_id (int) : integer representation of state
        sector_abbr (str) : the sector of the agent
        tariff_id (str) : the tariff class (particularly relevant in countries with crosssubsidization)
        pv_capacity_mw (int) : existing PV capacity in the state/tariff
        pv_systems_count (int) : existing number of PV systems
    """
    df_sec_natl_cumulative_q3 = pd.read_csv(os.path.join('reference_data','Historical India PV Install (National).csv')).loc[:4].set_index('Cumulative Capacity').drop('Source',axis=1) #SOURCE: BNEF
    df_sec_natl_cumulative_q3.columns = df_sec_natl_cumulative_q3.columns.astype(int)
    df_sec_natl_cumulative_share_q3 = df_sec_natl_cumulative_q3.loc[['Commercial Q3','Residential Q3','Industrial Q3','Total Rooftop Q3']].astype(float)/df_sec_natl_cumulative_q3.loc['Total Installed Q3'].astype(float).sum(axis=0)
    
    df_sec_natl_cumulative_q4 = pd.read_csv(os.path.join('reference_data','Historical India PV Install (National).csv')).loc[5:9].set_index('Cumulative Capacity').drop('Source',axis=1) #SOURCE: BNEF
    df_sec_natl_cumulative_q4.columns = df_sec_natl_cumulative_q4.columns.astype(int)
    df_sec_natl_cumulative_share_q4 = df_sec_natl_cumulative_q4.loc[['Commercial Q4','Residential Q4','Industrial Q4','Total Rooftop Q4']].astype(float)/df_sec_natl_cumulative_q4.loc['Total Installed Q4'].astype(float).sum(axis=0)

    df_natl_cumulative_q3 = pd.read_csv(os.path.join('reference_data','Historical India PV Install (National).csv'), index_col=0).loc['Total Installed Q3'].drop('Source')
    df_natl_cumulative_q3.index = df_natl_cumulative_q3.index.astype(int)

    df_natl_cumulative_q4 = pd.read_csv(os.path.join('reference_data','Historical India PV Install (National).csv'), index_col=0).loc['Average Total Installed'].drop('Source')
    df_natl_cumulative_q4.index = df_natl_cumulative_q4.index.astype(int)

    df_region_cumulative_q4 = pd.read_csv(os.path.join('reference_data','Cumulative Installed Capacity by State.csv')).set_index('State') #SOURCE: MNRE Q4
    region_cumulative_totals_q4 = df_region_cumulative_q4.sum(axis=0)
    df_region_cumulative_q4.columns = df_region_cumulative_q4.columns.astype(int)
    df_region_cumulative_share_q4 = df_region_cumulative_q4/region_cumulative_totals_q4.sum()

    india_regions = df_region_cumulative_q4.index.tolist()
    df_region_cumulative_q3 = pd.DataFrame(index=india_regions)
    for year in range(2017,2020):
        df_region_cumulative_q3[year] = df_region_cumulative_share_q4[year]*df_natl_cumulative_q3[year]

    northeast_states = ['Manipur', 'Assam', 'Meghalaya','Tripura', 'Mizoram', 'Nagaland', 'Sikkim', 'Arunachal Pradesh']
    territories = ['Andaman and Nicobar Islands','Chandigarh','Dadra Nagar Haveli','Daman Diu','Delhi','Jammu and Kashmir','Puducherry']
    states = ['Andhra Pradesh','Arunachal Pradesh','Assam','Bihar','Chhattisgarh','Goa','Gujarat','Haryana','Himachal Pradesh','Jharkhand',
              'Karnataka','Kerala','Madhya Pradesh','Maharashtra','Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Punjab','Rajasthan',
              'Sikkim','Tamil Nadu','Telangana','Tripura','Uttarakhand','Uttar Pradesh','West Bengal']
    sectors = ['Residential','Commercial','Industrial']

    df_starting_capacities = pd.DataFrame()
    
    df_cumulative_northeast_q3 = df_region_cumulative_q3.loc[northeast_states] #q3
    df_cumulative_northeast_share_q3 = df_cumulative_northeast_q3/df_cumulative_northeast_q3.sum(axis=0)
    
    for sector in sectors:
        df_sec_region_q3 =  pd.read_excel(os.path.join('reference_data','Residential and Commercial Installed Capacity by State.xlsx'), sheet_name="%s" % sector).set_index('Unnamed: 0') #SOURCE: Bridge to India Q3
        df_sec_region_q3.columns = df_sec_region_q3.columns.astype(int)
        
        sec_northeast_total_q3 = df_sec_region_q3.loc['North East']
        df_sec_region_northeast_q3 = df_sec_region_q3.loc[northeast_states]  

        for year in range(2017,2020):
            df_sec_region_northeast_q3[year] = (df_cumulative_northeast_share_q3[year]*sec_northeast_total_q3[year]).round(1)
       
        df_sec_region_q3.loc[(df_sec_region_q3.index.isin(df_sec_region_northeast_q3.index))] = df_sec_region_northeast_q3
        df_sec_region_q3 = df_sec_region_q3.drop('North East', axis=0)     
        
        df_sec_nans = df_sec_region_q3.loc[(df_sec_region_q3[year].isnull())]

        for year in range(2017,2020):
            for state in df_sec_nans.index:
                df_sec_nans.loc[state,year] = df_sec_natl_cumulative_q3.loc['%s Q3' % (sector),year]*df_region_cumulative_share_q4.loc[state, year]
  
        df_sec_region_q3.loc[(df_sec_region_q3.index.isin(df_sec_nans.index))] = df_sec_nans
        df_sec_region_share = df_sec_region_q3/df_sec_region_q3.sum()
        
        df_sec_region_q4 = pd.DataFrame(index=df_sec_region_q3.index)
         
        for year in range(2014,2020):
            if year<2015:
                df_sec_natl_cumulative_q4.loc['%s Q4' % (sector),year] = df_sec_natl_cumulative_share_q4.loc['%s Q4' % (sector),2015]*df_natl_cumulative_q4[year]
                df_sec_region_q4[year] = df_sec_region_share[2017]*df_sec_natl_cumulative_q4.loc['%s Q4' % (sector), year]
            elif year<2017:
                df_sec_region_q4[year] = df_sec_region_share[2017]*df_sec_natl_cumulative_q4.loc['%s Q4' % (sector), year]
            else:
                df_sec_region_q4[year] = df_sec_region_share[year]*df_sec_natl_cumulative_q4.loc['%s Q4' % (sector), year]
   
        df_sec_by_region = df_sec_region_q4
        
        df_sec_starting_capacities = pd.DataFrame(index=india_regions,columns=['state_id','state_id','sector_abbr','tariff_id','pv_capacity_mw','pv_systems_count'])
        df_sec_starting_capacities['sector_abbr']=sector.lower()[:3]
        
        for reg in india_regions:
            df_sec_starting_capacities.loc[reg, 'pv_capacity_mw'] = df_sec_by_region.loc[reg,2014]
            
        df_starting_capacities_all = pd.DataFrame()
        df_sec_by_region = df_sec_by_region.reset_index().rename(columns={'Unnamed: 0':'state_name'})
        
        df_starting_capacities = df_sec_by_region[['state_name',2014]]
        df_starting_capacities = df_starting_capacities.rename(columns={2014:'pv_capacity_mw'})
        df_starting_capacities['sector_abbr']=sector.lower()[:3]
         
        df_starting_capacities['state_name'] = df_starting_capacities['state_name'].apply(helper.sanitize_string)
        df_starting_capacities['state_id'] = df_starting_capacities['state_name'].map(state_id_lookup)
        df_starting_capacities['state_id'] = df_starting_capacities['state_id']
        
        if sector=='Residential':
            avg_system_size=10
        elif sector=='Commercial':
            avg_system_size=100
        else:
            avg_system_size=200
        
        df_starting_capacities['pv_systems_count']=df_starting_capacities['pv_capacity_mw']*1e6/avg_system_size
        df_starting_capacities = df_starting_capacities[['state_id','sector_abbr','pv_capacity_mw','pv_systems_count']]
        df_starting_capacities_all=pd.concat([df_starting_capacities_all,df_starting_capacities],axis=0)

    df_starting_capacities_all.to_csv(os.path.join('india_base','pv_state_starting_capacities.csv'), index = False)


def solar_resource_profiles(agents):
    """
    Columns
    -------
        state_id (int) : integer representation of state
        cf (set/list) : 8760 of cf normalized between TODO what is this normalized between?
    """
    # --- convert all h5 files to pickles ---
    # with h5py.File("pv_2014.h5", 'r+') as h5:
    #     tables = list(h5.keys())
    
    #     for table in tables:
    #         if 'profile' in table:
    #             profile = pd.DataFrame(h5[table][:])
    #             profile.to_pickle(os.path.join('reference_data', 'solar_resource_pickles',f'{table}.pkl'))

    # --- load resource meta table ---
    meta = pd.read_pickle(os.path.join('reference_data', 'solar_resource_pickles','meta.pkl'))
    
    # --- find tilt ---
    def find_tilt(lat, array=[15,25,35,45,55]):
        array = np.asarray(array)
        idx = (np.abs(array - lat)).argmin()
        return array[idx]
    meta['tilt'] = meta['latitude'].apply(find_tilt) #convert tilt to file terms
    meta['azimuth'] = 180
    
    # --- create centroid of each available resource file ---
    meta['geometry'] = [Point(row['longitude'], row['latitude']) for _, row in meta.iterrows()]
    meta = gpd.GeoDataFrame(meta)
    meta.index.name = 'resource_id'
    meta.reset_index(drop=False, inplace=True)
    resource_points = meta.unary_union
    
    # --- create list of centroids for each district ---
    resource = agents.drop_duplicates(subset=['district_id'], keep='first')
    resource['geometry'] = resource['centroid']
    resource = resource[['geometry','district_id']]

    try: #if column is text
        resource['geometry'] = resource['geometry'].apply(lambda x: shapely.wkt.loads(x))
    except Exception as e:
        pass
    
    resource = gpd.GeoDataFrame(resource)
    
    # --- find nearest resource region for each district ---
    def nearest_point_worker(point, points_lookup=resource_points, thresh=2):
        
        _, match = nearest_points(point, points_lookup)
        dist = point.distance(match)
        
        if dist < thresh:
            return match
        
        else:
            print('nearest point worker failed on', point.x, point.y)
            return np.nan
    
    
    resource['geometry'] = resource['geometry'].apply(nearest_point_worker)
    
    # --- merge district with meta ---
    resource = resource.merge(meta[['resource_id','tilt','azimuth','geometry']], on='geometry')
    
    # --- load resource data for each district ---
    def load_resource_data(row):
        fp = os.path.join('reference_data', 'solar_resource_pickles',f"pv_a{row['azimuth']}_t{row['tilt']}_cf_profile.pkl")
        cf = pd.read_pickle(fp)
        profile = np.array(cf[row['resource_id']], dtype='int16')
        return profile
    
    resource['cf'] = resource.apply(load_resource_data, axis=1)
    
    resource = resource[['district_id','resource_id','tilt','azimuth','cf']]
    resource.to_json(os.path.join('india_base','solar_resource_hourly.json'))

def urdb_tariff(agents):
    """
    Currently using a sample tariff based on the sector, with zero scaling by utility. 
    
    Columns
    -------
        tariff_id (int) : integer representation of rate_id
        rate_json (json) : json representation of rate
    """
    
    # --- initialize tariff df ---
    urdb_df = pd.DataFrame()
    
    # --- load sample tariff --- 
    sample_tariff = pd.read_pickle(os.path.join('reference_data','sample_tariff_dict.pkl'))
        
    for rate_id in agents['tariff_id'].unique():
        
        sector, geo = rate_id.split('#')
        
        # --- differentiate tariff data --- #TODO: differentiate based on geography
        if sector == 'res':
            e_levels = [[100], [1e20]]
            e_prices = [[0.04], [0.05]]
        if sector == 'com':
            e_levels = [[100], [1e20]]
            e_prices = [[0.04], [0.05]]
        if sector == 'ind':
            e_levels = [[100], [1e20]]
            e_prices = [[0.04], [0.05]]
        if sector == 'agg':
            e_levels = [[100], [1e20]]
            e_prices = [[0.04], [0.05]]
            
        tariff = sample_tariff.copy()
        tariff['e_levels'] = e_levels
        tariff['e_prices'] = e_prices
        
        # --- jsonify tariff and write to df ---
        tariff = json.dumps(tariff)
        tariff_row = pd.DataFrame({'tariff_id':[rate_id], 'rate_json':[tariff]})
        urdb_df = urdb_df.append(tariff_row)

    # --- Save the df to a json ---
    urdb_df.reset_index(inplace=True, drop=True)
    urdb_df.to_json(os.path.join('india_base','urdb3_rates.json'))
    

def normalized_load():
    """
    Dummy data using Colombia 8760s, with ind copy for agg. Normalized to 300000 kWh per year.
    
    Columns
    -------
        state_id (int) : integer representation of state
        kwh (set/list) : 8760 of load
    
    """
    sample_load = pd.read_json(os.path.join(os.pardir, 'input_scenarios','col_test','normalized_load.json'))
    sample_load = sample_load.rename({'tariff_class':'tariff_id'}, axis='columns')
    sample_load = sample_load.loc[sample_load['tariff_id'].isin(['estrato_3', 'comercial','industrial'])]
    sample_load.columns = ['sector_abbr','kwh']
    sample_load['sector_abbr'] = ['res','com','ind']
    sample_load = sample_load.append(pd.DataFrame({'sector_abbr':['agg'], 'kwh':sample_load.loc[sample_load['sector_abbr'] == 'ind', 'kwh']}))
    sample_load.reset_index(inplace=True, drop=True)
    sample_load.to_json(os.path.join('india_base','normalized_load.json'))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~ Create Agents ~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print('.....creating agents')
# --- Load Files ---
print('........loading consumption data')
con = samp.load_con()
print('........loading census data')
census = samp.load_census()
    
# --- Make Distributions ---
print('........counting load')
load_count = samp.make_load_count(con)
print('........sampling sectors')
sector_dist_load = samp.make_sector_dist_load(con)
print('........sampling agent count')
agent_count = samp.make_agent_count(sector_dist_load)
print('........sampling household count')
hh_count = samp.make_hh_count(census)
print('........sampling district count')
district_dist = samp.make_district_dist(census, agent_count)
print('........sampling roof size')
roof_dist = samp.make_roof_dist(census, agent_count, samp.developable_sqft_mu, samp.developable_sqft_sigma)
print('........sampling load')
customers_in_bin_dist, load_per_customer_in_bin_dist = samp.make_load_dist(census, agent_count,
                                                                      hh_count, load_count,
                                                                      samp.customers_per_hh_by_sector,
                                                                      samp.all_geo_sigma_load)
# --- Initialize Agents ---
print('........initializing agents')
agents = samp.initialize_agents(agent_count)

# --- Apply Distributions ---
print('........creating district distribution')
agents = samp.assign_distribution(agents, district_dist, 'district_name')
print('........creating rooftop distribution')
agents = samp.assign_distribution(agents, roof_dist, 'developable_roof_sqft')
print('........creating load distribution')
agents = samp.assign_distribution(agents, load_per_customer_in_bin_dist, 'load_per_customer_in_bin_kwh')
print('........creating customers distribution')
agents = samp.assign_distribution(agents, customers_in_bin_dist, 'customers_in_bin')


# --- Clean up ---
print('........mapping distributions')
agents = samp.map_geo_ids(agents)
agents = samp.map_tariff_ids(agents)
agents = samp.map_hdi(agents)
print('........merging geographies')
agents = samp.merge_district_geometry(agents)
print('........cleaning up agents')
agents = samp.clean_agents(agents)

# --- Save agents ---
print('........saving agents as csv')
agents.to_csv(os.path.join('india_base','agent_core_attributes.csv'), index=False)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~ Create CSVs ~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print('....creating scenario csvs')
print('........wholesale rates')
wholesale_rates()
print('........financing rates')
financing_rates(agents)
print('........load growth')
load_growth(agents)
print('........nem settings')
nem_settings(agents)
print('........rate escalations')
rate_escalations(agents)
print('........pv starting capacities')
pv_state_starting_capacities()
print('........solar resource profiles')
solar_resource_profiles(agents)
print('........urdb tariffs')
urdb_tariff(agents)
print('........normalized load')
normalized_load()

# --- copy files to input_scenarios in pdir ---
print('....copying csvs to input_scenarios')
copy_tree('india_base/', os.path.join(os.pardir, 'input_scenarios','india_base/'))

#%%


