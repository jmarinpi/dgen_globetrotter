# -*- coding: utf-8 -*-
"""
This module contains variables that can be changed, but are not exposed to non-expert users.
"""
import os
import multiprocessing

#==============================================================================

#==============================================================================
SCENARIOS = ['col_test']
SECTORS = ['res','com','ind']
SECTOR_NAMES = {'res':'Residential','com':'Commercial','ind':'Industrial'}
TECHS = [['solar']]
TECH_MODES = ['elec']

#==============================================================================
#   get the path of the current file
#==============================================================================
MODEL_PATH = os.path.dirname(os.path.abspath(__file__))

#==============================================================================
#   model start year 
#==============================================================================
START_YEAR = 2016

#==============================================================================
#   local cores
#==============================================================================
# LOCAL_CORES = int(multiprocessing.cpu_count() * 0.4)
LOCAL_CORES = 1

#==============================================================================
#   silence some output
#==============================================================================
VERBOSE = False

#==============================================================================
#   run a smaller agent_df for debugging
#==============================================================================
SAMPLE_PCT = 1

EXCHANGE_RATE = 70 #rupee in 1 USD

# --- Input Desired Scale of Discount Rate ---
RES_MIN_DR = 0.05
RES_MAX_DR = 0.15
COM_MIN_DR= 0.05
COM_MAX_DR = 0.10
IND_MIN_DR = 0.05
IND_MAX_DR = 0.10

# --- Downpayment ---
RES_MIN_DP = 0.2
RES_MAX_DP = 0.2
COM_MIN_DP= 0.2
COM_MAX_DP = 0.2
IND_MIN_DP = 0.2
IND_MAX_DP = 0.2

# --- Loan Rate ---
RES_MIN_LR = 0.054
RES_MAX_LR = 0.054
COM_MIN_LR = 0.054
COM_MAX_LR = 0.054
IND_MIN_LR = 0.054
IND_MAX_LR = 0.054

# --- Load Growth Scenarios ---
LOW_LOAD_GROWTH = 1.024
PLANNING_LOAD_GROWTH = 1.034
HIGH_LOAD_GROWTH = 1.044

# --- NEM interconnection limits ---
RES_NEM_KW_LIMIT = 1000
COM_NEM_KW_LIMIT = 5000
IND_NEM_KW_LIMIT = 10000

# --- Rate escalation ---
LOW_RATE_ESCALATION = 0.005
PLANNING_RATE_ESCALATION = 0.01
HIGH_RATE_ESCALATION = 0.02

#==============================================================================
#  Runtime Tests
#==============================================================================
NULL_COLUMN_EXCEPTIONS = ['state_incentives', 'pct_state_incentives', 'batt_dispatch_profile', 'export_tariff_results','carbon_price_cents_per_kwh']
                        # 'market_share_last_year', 'max_market_share_last_year', 'adopters_cum_last_year', 'market_value_last_year', 'initial_number_of_adopters', 'initial_pv_kw', 'initial_market_share', 'initial_market_value', 'system_kw_cum_last_year', 'new_system_kw', 'batt_kw_cum_last_year', 'batt_kwh_cum_last_year',
CHANGED_DTYPES_EXCEPTIONS = []
MISSING_COLUMN_EXCEPTIONS = []