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


#%%
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~ Set-up ~~~~~~~~~~~~~~~~"""
"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

EXCHANGE_RATE = 70 #rupee in 1 USD

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

"""
--- discount_rates.csv ---

Columns
-------
    state_id (int) : integer representation of state
    sector_abbr (str) : the sector of the agent
    real_discount (float) : the discount rate of the state/sector in percent TODO: how is this different than financing_rates.csv?
"""

"""
--- financing_rates.csv ---

Columns
-------
    state_id (int) : integer representation of state
    sector_abbr (str) : the sector of the agent
    loan_rate (float) : the annual interest rate on a loan. 
    real_discount (float) : the discount rate of the state/sector in percent
    down_payment (float) : percent of downpayment towards system (typically 0.2 to compare apples to apples in WACC)
"""

"""
--- load_growth_projection.csv ---

Columns
-------
    scenario (str) : matches the string from the input sheet
    year (int) : year of load growth relative to 2014
    sector_abbr (str) : the sector of the agent
    control_reg_id (int) : integer representation of control_reg
    load_multiplier (float) : load growth relative to 2014
"""

"""
--- max_market_share_settings.csv ---

Columns
-------
    metric (str) : i.e. 'payback period'
    metric_value (float) : i.e 'payback period'
    max_market_share (float) : survey results estimating maximum market share based on metric_value
    source (str) : source of survey info
    business model (str) : how the solar is financed
    sector_abbr (str) : the sector of the agent
"""

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

"""
--- normalized_load.csv OR .json ---

Columns
-------
    control_reg_id (int) : integer representation of control_reg
    kwh (set/list) : 8760 of load normalized between TODO what is this normalized between?
"""

"""
--- normalized_load.csv OR .json ---

Columns
-------
    control_reg_id (int) : integer representation of control_reg
    kwh (set/list) : 8760 of load normalized between TODO what is this normalized between?
"""

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

"""
--- rate_escalations.csv ---

Columns
-------
    source (str) : rate growth planning scenario, from input sheet
    control_reg_id (int) : integer representation of control_reg
    sector_abbr (str) : the sector of the agent
    year (int) : year of rate escalation relative to 2014
    escalation_factor (float) : multiplier of rate escalation relative to 2014
"""

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