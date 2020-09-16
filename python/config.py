# -*- coding: utf-8 -*-
"""
This module contains variables that can be changed, but are not exposed to non-expert users.
"""
import os
import multiprocessing

#==============================================================================

#==============================================================================
SCENARIOS = ['india_base']
SECTORS = ['res','com','ind']
SECTOR_NAMES = {'res':'Residential','com':'Commercial','ind':'Industrial'}
TECHS = [['solar']]
TECH_MODES = ['elec']
BA_COLUMN = 'state_id' #geo id column that data is available at such as control_reg_id, state_id, district_id etc. 

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
LOCAL_CORES = int(multiprocessing.cpu_count() - 3)

#==============================================================================
#   silence some output
#==============================================================================
VERBOSE = False

#==============================================================================
#   run a smaller agent_df for debugging
#==============================================================================
SAMPLE_PCT = 1

#==============================================================================
#  Runtime Tests
#==============================================================================
NULL_COLUMN_EXCEPTIONS = ['state_incentives', 'pct_state_incentives', 'batt_dispatch_profile', 'export_tariff_results','carbon_price_cents_per_kwh']
                        # 'market_share_last_year', 'max_market_share_last_year', 'adopters_cum_last_year', 'market_value_last_year', 'initial_number_of_adopters', 'initial_pv_kw', 'initial_market_share', 'initial_market_value', 'system_kw_cum_last_year', 'new_system_kw', 'batt_kw_cum_last_year', 'batt_kwh_cum_last_year',
CHANGED_DTYPES_EXCEPTIONS = []
MISSING_COLUMN_EXCEPTIONS = []