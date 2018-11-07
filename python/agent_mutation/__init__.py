# -*- coding: utf-8 -*-
"""
Edited Monday Nov 5, 218
@author: tkwasnik
"""
import elec

def init_solar_agents(scenario_settings):
    # Create core agent attributes
    scenario_settings.load_core_agent_attributes()
    
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
                               'load_kwh_per_customer_in_bin':'load_kwh_per_customer_in_bin_initial',
                               'load_kwh_in_bin':'load_kwh_in_bin_initial',
                               'consumption_hourly':'consumption_hourly_initial'}, inplace=True)

    return agents_df
