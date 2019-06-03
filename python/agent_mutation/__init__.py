# -*- coding: utf-8 -*-
"""
Edited Monday Nov 5, 218
@author: tkwasnik
"""

import elec

def init_solar_agents(scenario_settings):
    '''initiate solar agents
        
        Loads and merges state DPV capacity in 2015, load profiles, hourly solar resource, and electric rates with core agent attributes.

        Author: Ted Kwasnik

        Inputs:
        -scenario_settings - object - 
        
        Outputs:
        -agents_df - pandas dataframe - core initial agent attributes
    '''

    # =========================================================================
    # LOAD CORE AGENT ATTRIBUTES
    # =========================================================================
    
    scenario_settings.load_core_agent_attributes()

    scenario_settings.load_financing_rates()
    
    # =========================================================================
    # GET NORMALIZED LOAD PROFILES
    # =========================================================================
    
    scenario_settings.load_normalized_load_profiles()
    scenario_settings.load_normalized_hourly_resource_solar()
    scenario_settings.load_electric_rates_json()
    
    agents_df = scenario_settings.core_agent_attributes

    #==============================================================================
    # Set initial year columns. Initial columns do not change, whereas non-initial are adjusted each year
    # note that some of the above operations rely on non-initial name, which should be cleaned up when agent initialization is rebuilt
    #==============================================================================    
    agents_df.rename(columns={'customers_in_bin':'customers_in_bin_initial', 
                               'load_per_customer_in_bin_kwh':'load_per_customer_in_bin_kwh_initial',
                               'load_in_bin_kwh':'load_in_bin_kwh_initial',
                               'consumption_hourly':'consumption_hourly_initial'}, inplace=True)
    
    #only include agents matching the sector provided in the excel input
    agents_df = agents_df.loc[agents_df['sector_abbr'].isin(scenario_settings.sectors)]

    return agents_df
