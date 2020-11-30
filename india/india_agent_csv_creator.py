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

def wholesale_rates(agent_df):
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
    wholesale_rates = reeds.pivot_table(index=['state_name'], columns=['year'], values='cost')
    wholesale_rates = wholesale_rates.reset_index(drop=False)
    wholesale_rates['state_name'] = wholesale_rates['state_name'].replace('delhi', 'nct_of_delhi')
    wholesale_rates.loc[wholesale_rates['state_name'] != 'telangana']
    
    # --- add in earlier years ---
    annual_diff = (wholesale_rates[2018] - wholesale_rates[2017]) / wholesale_rates[2018]
    for y_index, y in enumerate([2014,2015,2016]):
        wholesale_rates[y] = wholesale_rates[2017] * (1 - annual_diff)**(3-y_index)
        
    # --- add in later years ---
    annual_diff = (wholesale_rates[2047] - wholesale_rates[2046]) / wholesale_rates[2047]
    for y_index, y in enumerate([2048,2049,2050]):
        wholesale_rates[y] = wholesale_rates[2047] * (1 + annual_diff)**(y_index + 1)

    # --- fuzzy string matching ---
    clean_list = list(agent_df['state_name'].unique())
    wholesale_rates['state_name'] = wholesale_rates['state_name'].apply(helper.sanitize_string)
    wholesale_rates['state_name'] = helper.fuzzy_address_matcher(wholesale_rates['state_name'], clean_list)
    
    # --- any missing states ---
    avg_wholesale_rates = wholesale_rates[list(range(2014,2051))].mean()
    for state in clean_list:
        if state not in set(wholesale_rates['state_name']):
            state_wholesale_rates = avg_wholesale_rates.copy()
            state_wholesale_rates['state_name'] = state
            wholesale_rates = wholesale_rates.append(state_wholesale_rates, ignore_index=True)

    # --- drop any duplicates ---
    wholesale_rates = wholesale_rates.drop_duplicates(subset=['state_name'])
    
    # --- map state id ---
    wholesale_rates['state_id'] = wholesale_rates['state_name'].map(state_id_lookup)
    wholesale_rates.drop(['state_name'], axis='columns', inplace=True)
    
    # --- currency conversion ---
    wholesale_rates[list(range(2014,2051))] = wholesale_rates[list(range(2014,2051))] / config.RUPPES_TO_USD
    
    # --- reorder columns ---
    wholesale_rates = wholesale_rates[['state_id'] + list(range(2014,2051))]
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

    reeds = pd.read_excel(
        os.path.join('reference_data','NREL_2020_load_forecast_2016-2037_KA.xlsx'),
        sheet_name='Growth_NREL_Baseline')

    reeds = reeds.loc[reeds['metric'] == 'Annual Energy']
    reeds.drop('metric', inplace=True, axis='columns')
    reeds.set_index('BA', inplace=True)

    # --- scale back load before 2019 ---
    reeds_before = reeds[[2017,2018]]
    for y in [2014,2015,2016]: # add in previous years
        reeds_before[y] = np.nan

    reeds_before = reeds_before[[2014,2015,2016,2017,2018]]
    reeds_before = reeds_before.fillna(method='bfill', axis=1).fillna(method='ffill', axis=1)
    reeds_before = 1 - reeds_before #load expressed as multiplier from next year
    reeds_before = reeds_before[list(reeds_before.columns)[::-1]] #reverse list
    reeds_before = reeds_before.cumprod(axis=1)
    reeds_before = reeds_before[list(reeds_before.columns)[::-1]] #reverse back to chronological
    reeds_before[2019] = 1 #2019 EPS is baseline year

    reeds_after = reeds[[2021,2022,2023,2024,2025,2026,2031,2036]]
    missing_years = [2020]+list(range(2027,2031))+list(range(2032,2036))+list(range(2037,2051))
    for y in missing_years:
        reeds_after[y] = np.nan
    reeds_after += 1 #express as percent increase from previous year

    reverse_cagr = lambda x: (x)**(1/5) #convert 5 year compund growth rate to annual
    reeds_after[2031] = reeds_after[2031].apply(reverse_cagr)
    reeds_after[2036] = reeds_after[2036].apply(reverse_cagr)
        
    reeds_after = reeds_after[list(range(2020,2051))]
    reeds_after = reeds_after.fillna(method='bfill', axis=1).fillna(method='ffill', axis=1)
    reeds_after = reeds_after.cumprod(axis=1)

    load_growth = pd.concat([reeds_before, reeds_after], axis='columns')

    load_growth.index.name = 'state_name'
    load_growth.reset_index(drop=False, inplace=True)

    load_growth = load_growth.melt(
        id_vars=['state_name'],
        var_name='year',
        value_name='load_multiplier'
    )

    # --- fuzzy string matching ---
    clean_list = list(agent_df['state_name'].unique())
    load_growth['state_name'] = load_growth['state_name'].apply(helper.sanitize_string)
    load_growth['state_name'] = helper.fuzzy_address_matcher(load_growth['state_name'], clean_list)

    # --- any missing states ---
    avg_load_growth = load_growth.groupby(['year'], as_index=False)['load_multiplier'].mean()
    for state in clean_list:
        if state not in set(load_growth['state_name']):
            state_load_growth = avg_load_growth.copy()
            state_load_growth['state_name'] = state
            load_growth = load_growth.append(state_load_growth)
            
    # --- map state id ---
    load_growth['state_id'] = load_growth['state_name'].map(state_id_lookup)
    load_growth.drop(['state_name'], axis='columns', inplace=True)

    # --- duplicate for sectors ---
    load_growths = []
    for s in ['res','com','ind','agg']:
        df = load_growth.copy()
        df['sector_abbr'] = s
        load_growths.append(df)
    load_growth = pd.concat(load_growths, axis='rows')

    load_growth['scenario'] = 'Planning'
    load_growth = load_growth.drop_duplicates(subset=['state_id','sector_abbr','year'])
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

    # --- Define Compensation Style for each State ---
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

def avoided_costs(agent_df):
    avoided_df = pd.DataFrame().from_records(itertools.product(list(set(agent_df['state_id'])), range(2015,2051)))
    avoided_df.columns = ['state_id','year']
    avoided_df['value'] = 0.03
    avoided_df = avoided_df.pivot(values='value', index='state_id', columns='year')
    avoided_df.to_csv(os.path.join('india_base','avoided_cost_rates.csv'))


def pv_state_starting_capacities(agent_df):
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


    # --- Read in 2019 adoption by sector and state, excluding some states ---
    res = pd.read_excel(os.path.join(
        'reference_data', 'Residential and Commercial Installed Capacity by State.xlsx'), sheet_name="Residential")
    com = pd.read_excel(os.path.join(
        'reference_data', 'Residential and Commercial Installed Capacity by State.xlsx'), sheet_name="Commercial")
    ind = pd.read_excel(os.path.join(
        'reference_data', 'Residential and Commercial Installed Capacity by State.xlsx'), sheet_name="Industrial")
    res['sector_abbr'] = 'res'
    com['sector_abbr'] = 'com'
    ind['sector_abbr'] = 'ind'
    by_sector = pd.concat([res, com, ind], axis='rows')
    # by_sector[[2018, 2019]] = by_sector[[2017, 2018, 2019]].diff(axis=1)[[2018, 2019]] # convert to annual installations
    by_sector = by_sector.fillna(0)
    by_sector.columns = ['state_name', 2017, 2018, 2019, 'sector_abbr']
    by_sector = by_sector.melt(id_vars=['state_name', 'sector_abbr'], var_name=[
                            'year'], value_name='cum_mw')

    replace_dict = {'Delhi': 'nct_of_delhi', 'Telangana': 'andhra_pradesh'}
    by_sector['state_name'] = by_sector['state_name'].replace(replace_dict)

    # --- fuzzy string matching ---
    clean_list = list(agent_df['state_name'].unique())
    by_sector['state_name'] = by_sector['state_name'].apply(helper.sanitize_string)
    by_sector['state_name'] = helper.fuzzy_address_matcher(
        by_sector['state_name'], clean_list)
    by_sector['state_id'] = by_sector['state_name'].map(state_id_lookup)
    by_sector['state_name'].unique()

    # --- group by duplicates ---
    by_sector = by_sector.groupby(
        ['state_name', 'state_id', 'sector_abbr', 'year'], as_index=False)['cum_mw'].sum()

    all_india = pd.read_csv(os.path.join(
        'reference_data', 'ieefa_india_national_mw.csv'))


    # --- Scale state/sector to meet annual estimates ---
    g = by_sector.groupby(['year'])['cum_mw'].sum()
    for y in set(by_sector['year']):
        old_mw = g[y]
        new_mw = all_india.loc[all_india['year'] == y, 'ieefa_total_mw'].values[0]
        multiplier = 1 + ((new_mw - old_mw) / old_mw)
        by_sector.loc[by_sector['year'] == y, 'cum_mw'] *= multiplier

    # --- Extrapolate state level data to previous years with national sums ---
    years = [i for i in set(all_india['year']) if i < by_sector['year'].min()]
    years.sort()
    years.reverse()
    for y in years:
        next_year = by_sector.loc[by_sector['year'] == y+1]
        old_mw = next_year['cum_mw'].sum()
        new_mw = all_india.loc[all_india['year'] == y, 'ieefa_total_mw'].values[0]
        multiplier = 1 + ((new_mw - old_mw) / old_mw)
        next_year['cum_mw'] *= multiplier
        next_year['year'] = y
        by_sector = pd.concat([by_sector, next_year], axis='rows')

    # --- Extrapolate projected capacity growth forward ---
    years = [i for i in set(all_india['year']) if i > by_sector['year'].max()]
    years.sort()
    for y in years:
        last_year = by_sector.loc[by_sector['year'] == y-1]
        old_mw = last_year['cum_mw'].sum()
        new_mw = all_india.loc[all_india['year'] == y, 'ieefa_total_mw'].values[0]
        multiplier = 1 + ((new_mw - old_mw) / old_mw)
        last_year['cum_mw'] *= multiplier
        last_year['year'] = y
        by_sector = pd.concat([by_sector, last_year], axis='rows')


    # --- identify states with no capacity ---
    for state in agent_df['state_name'].unique():
        for sector in agent_df['sector_abbr'].unique():
            if sector != 'agg':
                state_mask = (by_sector['state_name'] == state)
                sector_mask = (by_sector['sector_abbr'] == sector)
                sub_df = by_sector.loc[state_mask & sector_mask]
                assert len(sub_df) != 0

    # --- Calculate number of adopters from cumulative capacity ---
    system_size_dict = {'res': 4, 'com': 16,'ind': 100}  # based on avg rootop size
    by_sector['cum_installed_count'] = by_sector['cum_mw'] / \
        by_sector['sector_abbr'].map(system_size_dict) * 1000

    # --- create annual installtions ---
    by_sector.sort_values(['state_name', 'sector_abbr', 'year'], inplace=True)
    by_sector['annual_installed_mw'] = by_sector.groupby(
        ['state_name', 'state_id', 'sector_abbr'])['cum_mw'].transform(lambda x: x.diff(1))
    by_sector['annual_installed_count'] = by_sector.groupby(['state_name', 'state_id', 'sector_abbr'])[
        'cum_installed_count'].transform(lambda x: x.diff(1))
    by_sector[['cum_mw', 'cum_mw', 'annual_installed_mw', 'annual_installed_count']] = by_sector[[
        'cum_mw', 'cum_mw', 'annual_installed_mw', 'annual_installed_count']].fillna(0).clip(0)

    # --- Output annual installations for bass fitting ---
    by_sector.to_csv(os.path.join('reference_data',
                                'clean_pv_installed_all_years.csv'), index=False)

    # --- Make cumulative for pv_state_starting_capacities ---
    out = by_sector.loc[by_sector['year'] == config.START_YEAR]
    out = out[['state_id', 'sector_abbr', 'cum_installed_count', 'cum_mw']]
    out.rename({'cum_installed_count': 'pv_systems_count',
                'cum_mw': 'pv_capacity_mw'}, axis='columns', inplace=True)
    out.to_csv(os.path.join('india_base', 'pv_state_starting_capacities.csv'), index=False)


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
    sample_tariff = pd.read_pickle(os.path.join('reference_data', 'sample_tariff_dict.pkl'))

    # --- Load tariff .csv ---
    tariffs = pd.read_csv(os.path.join('reference_data', 'clean_CSTEP_india_tariffs.csv'))
    tariffs = tariffs.dropna(subset=['rate_rs_per_kwh'])
    tariffs['per_unit'] = tariffs['per_unit'].fillna('kwh')
    tariffs['rate_rs_per_kwh'] = tariffs['rate_rs_per_kwh'] / config.RUPPES_TO_USD

    for rate_id in agents['tariff_id'].unique():

        sector, geo = rate_id.split('#')

        # --- Lookup best tariff ---
        sector_mask = (tariffs['sector_abbr'] == sector)
        geo_mask = (tariffs['state'] == geo)
        units_mask = (tariffs['per_unit'] == 'kwh') #ignore demand charge rates for now
        t = tariffs[sector_mask & geo_mask & units_mask]
        t.sort_values('min_kwh', ascending=True)

        if sector in ['res', 'com']:
            if 'LT' in t['tension'].tolist():
                t = t.loc[t['tension'] == 'LT']
            else:
                t = t.loc[t['tension'] == 'HT']
        elif sector in ['ind']:
            if 'HT' in t['tension'].tolist():
                t = t.loc[t['tension'] == 'HT']
            else:
                t = t.loc[t['tension'] == 'LT']
        elif sector in ['agg']:
            t = {'min_kwh':pd.Series([1e10]), 'rate_rs_per_kwh':pd.Series([0])} #free electricity for agg

        assert len(t) > 0, f"No tariffs for {geo}, {sector}"
            
        tariff = sample_tariff.copy()
        if len(t) > 1:
            tariff['e_levels'] = t['min_kwh'].tolist()[1:]
            tariff['e_levels'].append(1e10)
        else:
            tariff['e_levels'] = [1e10]

        tariff['e_prices'] = t['rate_rs_per_kwh'].tolist()
        assert len(tariff['e_prices']) == len(tariff['e_levels'])

        tariff['e_levels'] = [[i] for i in tariff['e_levels']]
        tariff['e_prices'] = [[i] for i in tariff['e_prices']]

        # --- jsonify tariff and write to agents ---
        tariff = json.dumps(tariff)
        tariff_row = pd.DataFrame({'tariff_id':[rate_id], 'rate_json':[tariff]})
        urdb_df = urdb_df.append(tariff_row)

    # --- Save the agents to a json ---
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

def apply_itc(agents):
    special_states = [
        'uttarakhand',
        'sikkim',
        'himachal_pradesh',
        'jammu_kashmir',
        'lakshadweep'
    ]
    
    incentives = []
    for index, row in agents.iterrows():
        if row['state_name'] in special_states:
            incentives.append(0.7)
        else:
            incentives.append(0.3)
    agents['investment_incentive_pct'] = incentives
        
    agents['investment_incentive_year_cutoff'] = 2022
    
    return agents

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
print('........creating district distribution', 'agents shape:', agents.shape)
agents = samp.assign_distribution(agents, district_dist, 'district_name')
print('........creating rooftop distribution', 'agents shape:', agents.shape)
agents = samp.assign_distribution(agents, roof_dist, 'developable_roof_sqft')
print('........creating load distribution', 'agents shape:', agents.shape)
agents = samp.assign_distribution(agents, load_per_customer_in_bin_dist, 'load_per_customer_in_bin_kwh')
print('........creating customers distribution', 'agents shape:', agents.shape)
agents = samp.assign_distribution(agents, customers_in_bin_dist, 'customers_in_bin')
print('........applying available incentives', 'agents shape:', agents.shape)
agents = apply_itc(agents)

# --- Clean up ---
print('........mapping distributions', 'agents shape:', agents.shape)
agents = samp.map_geo_ids(agents)
agents = samp.map_tariff_ids(agents)
agents = samp.map_hdi(agents)
print('........merging geographies', 'agents shape:', agents.shape)
agents = samp.merge_district_geometry(agents)
print('........cleaning up agents', 'agents shape:', agents.shape)
agents = samp.clean_agents(agents)

# --- Save agents ---
print('........saving agents as csv', 'agents shape:', agents.shape)
agents['compensation_style'] = 'Net Metering'
agents['tech'] = 'solar'
agents.to_csv(os.path.join('india_base','agent_core_attributes.csv'), index=False)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~ Create CSVs ~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print('....creating scenario csvs')
print('........wholesale rates')
wholesale_rates(agents)
print('........financing rates')
financing_rates(agents)
print('........avoided cost rates')
avoided_costs(agents)
print('........load growth')
load_growth(agents)
print('........nem settings')
nem_settings(agents)
print('........rate escalations')
rate_escalations(agents)
print('........pv starting capacities')
pv_state_starting_capacities(agents)
print('........solar resource profiles')
solar_resource_profiles(agents)
print('........urdb tariffs')
urdb_tariff(agents)
print('........normalized load')
normalized_load()

# --- copy files to input_scenarios in pdir ---
print('....copying csvs to input_scenarios')
copy_tree('india_base/', os.path.join(os.pardir, 'input_scenarios','india_base/'))

#IMPORTANT NOTE! After running this script, run 'india_bass_estimation.ipynb' to recalibrate the bass diffusion model to the new agents
# This is important when changing the number of agents per state represeted (i.e. because the historic adoption sampled will be allocated to different districts potenitally)
#%%


