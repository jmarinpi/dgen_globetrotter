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
import unicodedata

# --- External Library Imports ---
import pandas as pd

# --- Module Imports ---
#import config

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~ Set-up ~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# --- Define Helper Functions ---

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~ Create Agents ~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

"""
--- agent_core_attributes_all.csv ---

Columns
-------
    control_reg (str) : Usually the utility or balancing authority
    control_reg_id (int) : integer representation of control_reg
    state (str) : Usually the sub-federal political geography, sometimes a sub-sub feder (i.e. county)
    state_id (int) : integer representation of state
    sector_abbr (str) : the sector of the agent
    tariff_class (str) : the tariff class (particularly relevant in countries with crosssubsidization)
    rate_id_alias (int) : integer representation of rate in urdb format
    customers_in_bin (int) : customers represented by agent
    load_in_bin_kwh (int) : annual kWh represented by agent
    load_per_customer_in_bin_kwh (int) : load_in_bin_kwh / customers_in_bin
    avg_monthly_kwh (int) : load_in_bin_kwh / 12
    max_demand_kw (float) : peak demand (kW) from agent
    owner_occupancy_status (bool 1/0) : rental, or owner occupied
    cap_cost_multiplier (float) : multiplier on installation cost for geography/agent
    developable_buildings_pct (float) : pct of customers represented by agent that can install solar
    bldg_size_class	(str) : 'small' how is this used? TODO
    developable_roof_sqft (int) : total installable rooftop area TODO move this to a seperate csv? 
    social_indicator (float) : a linear scaler indicating social development within a given region
        for instance, see 'hdi' for Mexico
"""

"""
--- avoided_cost_rates.csv ---

Columns
-------
    2014-2050 (float) : TODO what is this?
"""

"""
--- carbon_intensities_grid.csv ---

Columns
-------
    2014-2050 (float) : TODO how is this used? 
"""

"""
--- carbon_intensities_ng.csv ---

Columns
-------
    2014-2050 (float) : TODO how is this used? 
"""

"""
--- wholesale_rates.csv ---

Columns
-------
    2014-2050 (float) : TODO how is this used? 
"""

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~ Financing Rates ~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

"""
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
    social_dfs.append(state_social_df)
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
finance_df.to_csv(os.path.join('input_scenarios','india_base','financing_rates.csv'), index=False)

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~ Load Growth ~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

"""
Columns
-------
    scenario (str) : matches the string from the input sheet
    year (int) : year of load growth relative to 2014
    sector_abbr (str) : the sector of the agent
    control_reg_id (int) : integer representation of control_reg
    load_multiplier (float) : load growth relative to 2014

Methodology
-----------
    Take ReEDS Load Time Slice Hourly Load by State, average by year
    
Assumptions 
-----------
    Currently assumes that all sectors have the same load growth. Could use 'CEA_historic_consumption_by_sector.csv' to normalize this by sector.
"""

import pandas as pd
import numpy as np

reeds_load = pd.read_csv('ReEDS_load.csv', names=['state','hour','year','value'])

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

# --- Calculate cumulative product ---
reeds_load += 1
reeds_load = reeds_load.cumprod(axis=1)

# --- Convert back to long_df ---
load_growth = reeds_load.copy()
load_growth.reset_index(inplace=True)
load_growth = load_growth.melt(id_vars=['state'], var_name=['year'], value_name='new_load_growth')

for c in ['residential', 'commercial', 'industrial', 'agriculture']:
    load_growth[c] = load_growth['new_load_growth']
load_growth.drop('new_load_growth', axis='columns', inplace=True)

load_growth = load_growth.melt(id_vars=['state','year'], var_name='sector_abbr', value_name='load_growth')

load_growth.to_csv(os.path.join('input_scenarios','india_base','financing_rates.csv'), index=False)

#%%
"""
--- nem_settings.csv ---

Columns
-------
    control_reg_id (int) : integer representation of control_reg
    sector_abbr (str) : the sector of the agent
    year (int) : year for policy details
    nem_system_size_limit_kw (int) : size limit for individual agent system size (kW)
    year_end_excess_sell_rate_usd_per_kwh (float) : payment for excess genration at end of year TODO how is this used? 
"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~ NEM Settings ~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# --- Create Dataframe from permutations, as values are all similar ---
nem_df = pd.DataFrame().from_records(itertools.product(['res','com','ind'], list(set(agent_df['control_reg_id'])), range(2015,2051)))
nem_df.columns = ['sector_abbr','control_reg_id','year']
nem_df.loc[nem_df['sector_abbr']=='res', 'nem_system_size_limit_kw'] = config.RES_NEM_KW_LIMIT
nem_df.loc[nem_df['sector_abbr']=='com', 'nem_system_size_limit_kw'] = config.COM_NEM_KW_LIMIT
nem_df.loc[nem_df['sector_abbr']=='ind', 'nem_system_size_limit_kw'] = config.IND_NEM_KW_LIMIT
nem_df.to_csv(os.path.join('input_scenarios','base','nem_settings.csv'), index = False)

#%%

"""
--- pv_bass.csv ---

Columns
-------
    control_reg_id (int) : integer representation of control_reg
    state_id (int) : integer representation of state
    sector_abbr (str) : the sector of the agent
    p (float) : bass innovator parameter
    q (float) : bass immitator parameter
    teq_yr1 (int) : number of years since technology adoption at year 1
    tech (str) : the technology
"""

"""
--- pv_state_starting_capacities.csv ---

Columns
-------
    control_reg_id (int) : integer representation of control_reg
    state_id (int) : integer representation of state
    sector_abbr (str) : the sector of the agent
    tariff_class (str) : the tariff class (particularly relevant in countries with crosssubsidization)
    pv_capacity_mw (int) : existing PV capacity in the state/tariff
    pv_systems_count (int) : existing number of PV systems
"""

#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~ Rate Escalations ~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""
Columns
-------
    source (str) : rate growth planning scenario, from input sheet
    control_reg_id (int) : integer representation of control_reg
    sector_abbr (str) : the sector of the agent
    year (int) : year of rate escalation relative to 2014
    escalation_factor (float) : multiplier of rate escalation relative to 2014
"""

# --- Create Dataframe from permutations, as values are all similar ---
rate_esc_df = pd.DataFrame().from_records(itertools.product(['Planning','Low','High'], range(2015, 2051), ['res','com','ind'], list(set(agent_df['control_reg_id']))))
rate_esc_df.columns = ['source','year','sector_abbr','control_reg_id']

def escalation_factor_applier(row):
    multiplier = row['year'] - 2015
    if row['source'] == 'Planning':
        esc = 1 + (multiplier * config.PLANNING_RATE_ESCALTION)
    if row['source'] == 'Low':
        esc = 1 + (multiplier * config.LOW_RATE_ESCALTION)
    if row['source'] == 'High':
        esc = 1 + (multiplier * config.HIGH_RATE_ESCALATION)
    return esc

rate_esc_df['escalation_factor'] = rate_esc_df.apply(escalation_factor_applier, axis = 1)
rate_esc_df.to_csv('input_scenarios/base/rate_escalations.csv', index = False)

#%%
"""
--- solar_resource_hourly.csv OR .json ---

Columns
-------
    control_reg_id (int) : integer representation of control_reg
    state_id (int) : integer representation of state
    cf (set/list) : 8760 of cf normalized between TODO what is this normalized between?
"""

"""
--- urdb3_rates.csv OR .json ---

Columns
-------
    rate_id_alias (int) : integer representation of rate_id
    rate_json (json) : json representation of rate
"""

"""
--- normalized_load.csv OR .json ---

Columns
-------
    control_reg_id (int) : integer representation of control_reg
    kwh (set/list) : 8760 of load normalized between TODO what is this normalized between?
"""