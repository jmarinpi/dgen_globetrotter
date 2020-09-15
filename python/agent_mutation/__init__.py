# -*- coding: utf-8 -*-
"""
Edited Monday Nov 5, 218
@author: tkwasnik
"""

from . import elec

import utility_functions as utilfunc
logger = utilfunc.get_logger()

def init_solar_agents(scenario_settings):
    """initiate solar agents
        
        Loads and merges state DPV capacity in 2015, load profiles, hourly solar resource, and electric rates with core agent attributes.

        Author: Ted Kwasnik

        Inputs:
        -scenario_settings - object - 
        
        Outputs:
        -agents_df - pandas dataframe - core initial agent attributes
    """

    scenario_settings.load_core_agent_attributes()
    logger.info(f"........loaded core agent attributes, agents shape: {scenario_settings.core_agent_attributes.shape}")
    scenario_settings.load_starting_capacities()
    logger.info(f"........loaded starting capacities, agents shape: {scenario_settings.core_agent_attributes.shape}")
    scenario_settings.load_financing_rates()
    logger.info(f"........loaded financing rates, agents shape: {scenario_settings.core_agent_attributes.shape}")
    scenario_settings.load_normalized_load_profiles()
    logger.info(f"........loaded load profiles, agents shape: {scenario_settings.core_agent_attributes.shape}")
    scenario_settings.load_normalized_hourly_resource_solar()
    logger.info(f"........loaded solar profiles, agents shape: {scenario_settings.core_agent_attributes.shape}")
    scenario_settings.load_electric_rates_json()
    logger.info(f"........loaded electric tariffs, agents shape: {scenario_settings.core_agent_attributes.shape}")

    agents_df = scenario_settings.core_agent_attributes

    agents_df.rename(columns={'customers_in_bin':'customers_in_bin_initial', 
                               'load_per_customer_in_bin_kwh':'load_per_customer_in_bin_kwh_initial',
                               'load_in_bin_kwh':'load_in_bin_kwh_initial',
                               'consumption_hourly':'consumption_hourly_initial'}, inplace=True)
    
    #only include agents matching the sector provided in the excel input
    agents_df = agents_df.loc[agents_df['sector_abbr'].isin(scenario_settings.sectors)]

    return agents_df
