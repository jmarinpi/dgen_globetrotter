# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 11:35:14 2016

@author: mgleason
"""

import psycopg2 as pg
import numpy as np
import pandas as pd
import decorators
import utility_functions as utilfunc
import multiprocessing
import traceback
import data_functions as datfunc
from agent import Agent, Agents, AgentsAlgorithm
from cStringIO import StringIO
import pssc_mp
# functions borrowed from electricity
from agent_mutation_elec import get_depreciation_schedule, apply_depreciation_schedule, get_leasing_availability, apply_leasing_availability

#%% GLOBAL SETTINGS

# load logger
logger = utilfunc.get_logger()

# configure psycopg2 to treat numeric values as floats (improves performance of pulling data from the database)
DEC2FLOAT = pg.extensions.new_type(
    pg.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
pg.extensions.register_type(DEC2FLOAT)



#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_initial_agent_attributes(con, schema):
    
    inputs = locals().copy()
    sql = """SELECT *, FALSE::BOOLEAN AS new_construction
             FROM %(schema)s.agent_core_attributes_all
             WHERE year = 2012;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    agents = Agents(df)

    return agents

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_new_agent_attributes(con, schema, year):
    
    inputs = locals().copy()
    sql = """SELECT *, TRUE::BOOLEAN AS new_construction
             FROM %(schema)s.agent_core_attributes_all
             WHERE year = %(year)s;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    agents = Agents(df)

    return agents


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def replicate_agents_by_factor(dataframe, new_column_name, factor_list):
    
    df_list = []
    for factor in factor_list:
        temp_df = dataframe.copy()
        temp_df[new_column_name] = factor
        df_list.append(temp_df)
    
    dataframe = pd.concat(df_list, axis = 0, ignore_index = True)
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_technology_performance_improvements_ghp(con, schema, year):
    
    inputs = locals().copy()
    sql = """SELECT heat_pump_lifetime_yrs as ghp_heat_pump_lifetime_yrs, 
                    efficiency_improvement_factor as ghp_efficiency_improvement_factor,
                    sys_config
             FROM %(schema)s.input_ghp_performance_improvements
             WHERE year = %(year)s;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_technology_performance_ghp(dataframe, tech_performance_df):
    
    # join on sys_config
    dataframe = pd.merge(dataframe, tech_performance_df, how = 'left', on = 'sys_config')
    
    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_system_degradataion_ghp(con, schema, year):
    
    inputs = locals().copy()
    sql = """SELECT iecc_temperature_zone, 
                    sys_config,
                    annual_degradation_pct as ghp_ann_system_degradation
             FROM %(schema)s.input_ghp_system_degradation
             WHERE year = %(year)s;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_system_degradation_ghp(dataframe, system_degradation_df):
    
    # join on sys_config and iecc_temperature_zone
    dataframe = pd.merge(dataframe, system_degradation_df, how = 'left', on = ['sys_config', 'iecc_temperature_zone'])
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_technology_performance_improvements_and_degradation_baseline(con, schema, year):
    
    inputs = locals().copy()
    sql = """SELECT sector_abbr, 
                    baseline_system_type, 
                    efficiency_improvement_factor as baseline_efficiency_improvement_factor, 
                    system_lifetime_yrs as baseline_system_lifetime_yrs, 
                    annual_degradation_pct as baseline_ann_system_degradation
             FROM %(schema)s.input_baseline_performance_hvac
             WHERE year = %(year)s
             
             UNION ALL

             SELECT unnest(ARRAY['res', 'com']) as sector_abbr,
                    -1::INTEGER as baseline_system_type,
                    NULL::NUMERIC as baseline_efficiency_improvement_factor,
                    NULL::NUMERIC as baseline_system_lifetime_yrs, 
                    NULL::NUMERIC as baseline_ann_system_degradation;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_technology_performance_improvements_and_degradation_baseline(dataframe, tech_performance_df):
    
    # join on sector and baseline type
    dataframe = pd.merge(dataframe, tech_performance_df, how = 'left', on = ['sector_abbr', 'baseline_system_type'])
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_crb_ghp_simulations(con, schema):
    
    inputs = locals().copy()
    sql = """SELECT crb_model, 
                	 iecc_climate_zone, 
                	 gtc_btu_per_hftf, 
                	 savings_pct_electricity_consumption, 
                   savings_pct_natural_gas_consumption, 
                   crb_ghx_length_ft, 
                   crb_cooling_capacity_ton, 
                   crb_totsqft,
                   cooling_ton_per_sqft, 
                   ghx_length_ft_per_cooling_ton
          FROM diffusion_geo.ghp_simulations_dummy;"""
    
    df = pd.read_sql(sql, con, coerce_float = False)

    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_crb_ghp_simulations(dataframe, crb_ghp_df):
    
    # join on crb_model, iecc_cliamte_zone, and gtc value
    # TODO: this will change based on feedback from xiaobing about how to extrapolate from crbs to cbecs/recs (issue #)
    dataframe = pd.merge(dataframe, crb_ghp_df, how = 'left', on = ['crb_model', 'iecc_climate_zone', 'gtc_btu_per_hftf'])
    # change some random set to have no mapping (this will happen with the real mapping data, so we need to simulate it here to write downstream code)
    cols = ['savings_pct_electricity_consumption',
            'savings_pct_natural_gas_consumption',
            'crb_ghx_length_ft',
            'crb_cooling_capacity_ton',
            'crb_totsqft',
            'cooling_ton_per_sqft',
            'ghx_length_ft_per_cooling_ton']
    np.random.seed(1)
    dataframe.loc[np.random.randint(0, dataframe.shape[0], 20), cols] = np.nan
   
    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_baseline_system_types(con, schema):
    
    # TODO: revise this to map to baseline systems based on the GHP CRB simulation
    inputs = locals().copy()
    sql = """SELECT crb_model, 
                	 iecc_climate_zone, 
                	 gtc_btu_per_hftf, 
                   1::INTEGER as baseline_system_type
          FROM diffusion_geo.ghp_simulations_dummy;"""
    
    df = pd.read_sql(sql, con, coerce_float = False)

    return df   

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_baseline_system_types(dataframe, baseline_systems_df):
    
    # join on crb_model, iecc_cliamte_zone, and gtc value
    # TODO: this will change based on feedback from xiaobing about how to extrapolate from crbs to cbecs/recs (issue #)
    dataframe = pd.merge(dataframe, baseline_systems_df, how = 'left', on = ['crb_model', 'iecc_climate_zone', 'gtc_btu_per_hftf'])
    # change the rows with no CRB mapping to have no baseline type (this will happen with the real mapping data, so we need to simulate it here to write downstream code)
    dataframe.loc[dataframe['cooling_ton_per_sqft'].isnull(), 'baseline_system_type'] = -1
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def mark_unmodellable_agents(dataframe):
    
    dataframe['modellable'] = np.where(dataframe['baseline_system_type'] == -1, False, True)
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_siting_constraints_ghp(con, schema, year):
    
    inputs = locals().copy()
    sql = """SELECT area_per_well_sqft_vertical, 
                    max_well_depth_ft,
                    area_per_pipe_length_sqft_per_foot_horizontal
             FROM %(schema)s.input_ghp_siting;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)
    # add year field (only to facilitate downstream joins)
    df['year'] = year

    return df

    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def size_systems_ghp(dataframe):
    
    # output should be both ghx_length_ft and cooling_capacity_ton

    # first calculate required system capacity
    dataframe['ghp_system_size_tons'] = np.where(dataframe['modellable'] == True, dataframe['cooling_ton_per_sqft'] * dataframe['totsqft'], np.nan)
    # add system size kw (for compatibility with downstream code)
    dataframe['system_size_kw'] = np.where(dataframe['modellable'] == True, dataframe['ghp_system_size_tons'] * 3.5168525, np.nan)

    # next, calculate the ghx length required to provide that capacity
    dataframe['ghx_length_ft'] = np.where(dataframe['modellable'] == True, dataframe['ghp_system_size_tons'] * dataframe['ghx_length_ft_per_cooling_ton'], np.nan)

    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_siting_constraints_ghp(dataframe, siting_constraints_df):
    
    in_cols = dataframe.columns.tolist()
    
    dataframe = pd.merge(dataframe, siting_constraints_df, how = 'left', on = 'year')
    dataframe['parcel_size_sqft'] = dataframe['acres_per_bldg'] * 43560.
    # find the number of wells and total length of vertical loop that could be installed
    dataframe['n_installable_wells_vertical'] = dataframe['parcel_size_sqft'] / dataframe['area_per_well_sqft_vertical']
    dataframe['length_installable_vertical_ft'] = dataframe['n_installable_wells_vertical'] * dataframe['max_well_depth_ft']
    # find the length of horizontal loop that could be isntalled
    dataframe['length_installable_horizontal_ft'] = dataframe['parcel_size_sqft'] / dataframe['area_per_pipe_length_sqft_per_foot_horizontal']
    # determine whether each option is viable
    dataframe['length_installable_ft'] = np.where(dataframe['sys_config'] == 'vertical', dataframe['length_installable_vertical_ft'], dataframe['length_installable_horizontal_ft'])
    dataframe['viable_sys_config'] = np.where(dataframe['modellable'] == True, dataframe['length_installable_ft'] >= dataframe['ghx_length_ft'], False)
    
    out_cols = ['area_per_well_sqft_vertical',
                'max_well_depth_ft',
                'area_per_pipe_length_sqft_per_foot_horizontal',
                'n_installable_wells_vertical', 
                'length_installable_ft',
                'viable_sys_config'
                ]
    return_cols = in_cols + out_cols
    
    dataframe = dataframe[return_cols]
    
    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def identify_bass_deployable_agents(dataframe, sunk_costs):

    # deployable customers are those: 
    # (1) with a system that can be sited on the property, 
    # (2) that are modellable (i.e., we have a CRB model to use), and 
    # (3) need a replacement system NOW (ONLY if sunk_costs == False) 

    if sunk_costs == True:
        dataframe['bass_deployable'] = (dataframe['viable_sys_config'] == True) & (dataframe['modellable'] == True)        
    elif sunk_costs == False:
        dataframe['bass_deployable'] = (dataframe['viable_sys_config'] == True) & (dataframe['modellable'] == True) & (dataframe['needs_average_system'] == True)        
    else:
        raise ValueError('sunk_costs must be one of: True/False')

    dataframe['bass_deployable_buildings_in_bin'] = np.where(dataframe['bass_deployable'] == True, dataframe['buildings_in_bin'], 0.)   
   
    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def identify_market_eligible_agents(dataframe):

    dataframe['market_eligible'] = (dataframe['viable_sys_config'] == True) & (dataframe['modellable'] == True)
    dataframe['market_eligible_buildings_in_bin'] = np.where(dataframe['market_eligible'] == True, dataframe['buildings_in_bin'], 0.)
    
    return dataframe
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_technology_costs_ghp(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT sector_abbr,
                    sys_config,
                    heat_exchanger_cost_dollars_per_ft,
                    heat_pump_cost_dollars_per_cooling_ton,
                    new_rest_of_system_costs_dollars_per_cooling_ton as ghp_new_rest_of_system_costs_dollars_per_cooling_ton,
                    fixed_om_dollars_per_sf_per_year as ghp_fixed_om_dollars_per_sf_per_year,
                    retrofit_rest_of_system_multiplier as ghp_retrofit_rest_of_system_multiplier
             FROM %(schema)s.input_ghp_cost
             WHERE year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)

    return df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_tech_costs_ghp(dataframe, tech_costs_ghp_df):    
    
    dataframe = pd.merge(dataframe, tech_costs_ghp_df, how = 'left', on = ['sector_abbr', 'sys_config'])
    # Installed Costs
    dataframe['ghx_cost_dlrs'] = dataframe['ghx_length_ft'] * dataframe['heat_exchanger_cost_dollars_per_ft']
    dataframe['ghp_heat_pump_cost_dlrs'] = dataframe['ghp_system_size_tons'] * dataframe['heat_pump_cost_dollars_per_cooling_ton']
    dataframe['ghp_rest_of_system_cost_dlrs'] = np.where(dataframe['new_construction'] == True, 
                                                     dataframe['ghp_new_rest_of_system_costs_dollars_per_cooling_ton'] * dataframe['ghp_system_size_tons'], 
                                                     dataframe['ghp_new_rest_of_system_costs_dollars_per_cooling_ton'] * dataframe['ghp_system_size_tons'] * dataframe['ghp_retrofit_rest_of_system_multiplier'])
    dataframe['ghp_installed_costs_dlrs'] = dataframe['ghx_cost_dlrs'] + dataframe['ghp_heat_pump_cost_dlrs'] + dataframe['ghp_rest_of_system_cost_dlrs']
    dataframe['ghp_fixed_om_dlrs_per_year'] = dataframe['ghp_fixed_om_dollars_per_sf_per_year'] * dataframe['totsqft']
     # reset values to NA where the system isn't modellable
    out_cols = ['ghx_cost_dlrs', 
                'ghp_heat_pump_cost_dlrs', 
                'ghp_rest_of_system_cost_dlrs', 
                'ghp_installed_costs_dlrs', 
                'ghp_fixed_om_dlrs_per_year']
    dataframe.loc[dataframe['modellable'] == False, out_cols] = np.nan   
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_technology_costs_baseline(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT sector_abbr,
                    baseline_system_type,
                    hvac_equipment_cost_dollars_per_cooling_ton,
                    new_rest_of_system_costs_dollars_per_cooling_ton as baseline_new_rest_of_system_costs_dollars_per_cooling_ton,
                    retrofit_rest_of_system_multiplier as baseline_retrofit_rest_of_system_multiplier,
                    fixed_om_dollars_per_sf_per_year as baseline_fixed_om_dollars_per_sf_per_year
             FROM %(schema)s.input_baseline_costs_hvac
             WHERE year = %(year)s
             
             UNION ALL

             SELECT unnest(ARRAY['res', 'com']) as sector_abbr,
                    -1::INTEGER as baseline_system_type,
                    NULL::NUMERIC as hvac_equipment_cost_dollars_per_cooling_ton,
                    NULL::NUMERIC as baseline_new_rest_of_system_costs_dollars_per_cooling_ton,
                    NULL::NUMERIC as baseline_retrofit_rest_of_system_multiplier,
                    NULL::NUMERIC as baseline_fixed_om_dollars_per_sf_per_year;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)

    return df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_tech_costs_baseline(dataframe, tech_costs_baseline_df, sunk_costs):    
    

    dataframe = pd.merge(dataframe, tech_costs_baseline_df, how = 'left', on = ['sector_abbr', 'baseline_system_type'])
    # Installed Costs
    if sunk_costs == True:
        # installation costs will be zero, except for new construction
        # O&M are normal
        dataframe['baseline_equipment_costs_dlrs'] = np.where(dataframe['new_construction'] == True, dataframe['ghp_system_size_tons'] * dataframe['hvac_equipment_cost_dollars_per_cooling_ton'], 0.)
        dataframe['baseline_rest_of_system_cost_dlrs'] = np.where(dataframe['new_construction'] == True, dataframe['baseline_new_rest_of_system_costs_dollars_per_cooling_ton'] * dataframe['ghp_system_size_tons'], 0.)
        dataframe['baseline_installed_costs_dlrs'] = dataframe['baseline_equipment_costs_dlrs'] + dataframe['baseline_rest_of_system_cost_dlrs']
        dataframe['baseline_fixed_om_dlrs_per_year'] = dataframe['baseline_fixed_om_dollars_per_sf_per_year'] * dataframe['totsqft']     
    elif sunk_costs == False:
        dataframe['baseline_equipment_costs_dlrs'] = dataframe['ghp_system_size_tons'] * dataframe['hvac_equipment_cost_dollars_per_cooling_ton']
        dataframe['baseline_rest_of_system_cost_dlrs'] = np.where(dataframe['new_construction'] == True, 
                                                         dataframe['baseline_new_rest_of_system_costs_dollars_per_cooling_ton'] * dataframe['ghp_system_size_tons'], 
                                                         dataframe['baseline_new_rest_of_system_costs_dollars_per_cooling_ton'] * dataframe['ghp_system_size_tons'] * dataframe['baseline_retrofit_rest_of_system_multiplier'])
        dataframe['baseline_installed_costs_dlrs'] = dataframe['baseline_equipment_costs_dlrs'] + dataframe['baseline_rest_of_system_cost_dlrs']
        dataframe['baseline_fixed_om_dlrs_per_year'] = dataframe['baseline_fixed_om_dollars_per_sf_per_year'] * dataframe['totsqft']     
    else:
        raise ValueError('sunk_costs must be one of: True/False')

    # reset values to NA where the system isn't modellable
    out_cols = ['baseline_equipment_costs_dlrs', 'baseline_rest_of_system_cost_dlrs', 'baseline_installed_costs_dlrs', 'baseline_fixed_om_dlrs_per_year']    
    dataframe.loc[dataframe['modellable'] == False, out_cols] = np.nan       
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_site_energy_consumption_ghp(dataframe):
    
    # determine the total amount of natural gas and elec used for space heating and cooling by BASELINE HVAC
    dataframe['baseline_site_natgas_per_building_kwh'] = (dataframe['site_space_heat_per_building_in_bin_kwh'] * (dataframe['space_heat_fuel'] == 'natural gas') + 
                                                      dataframe['site_space_cool_per_building_in_bin_kwh'] * (dataframe['space_cool_fuel'] == 'natural gas'))
    dataframe['baseline_site_elec_per_building_kwh'] = (dataframe['site_space_heat_per_building_in_bin_kwh'] * (dataframe['space_heat_fuel'] == 'electricity') + 
                                                    dataframe['site_space_cool_per_building_in_bin_kwh'] * (dataframe['space_cool_fuel'] == 'electricity'))
    # determine the total amount of natural gas and elec used for space heating and cooling by GHP
    # (account for energy savings from CRBS)
    dataframe['ghp_site_natgas_per_building_kwh'] = np.where(dataframe['modellable'] == True, dataframe['baseline_site_natgas_per_building_kwh'] * (1. - dataframe['savings_pct_natural_gas_consumption']), np.nan)
    dataframe['ghp_site_elec_per_building_kwh'] = np.where(dataframe['modellable'] == True, dataframe['baseline_site_elec_per_building_kwh'] * (1. - dataframe['savings_pct_electricity_consumption']), np.nan)
    
    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def update_year(dataframe, year):
    
    dataframe.loc[:, 'year'] = year
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_regional_energy_prices(con, schema, year):
    
    inputs = locals().copy()
    
    # need to create data for fuel type = none for apply_regional_energy_prices()
    # also need to create data for wood (assume 28 MMBTU per cord of wood and approx $200/cord)
    sql = """SELECT sector_abbr, 
                    census_division_abbr, 
                    fuel_type, 
                    dlrs_per_kwh
            FROM %(schema)s.aeo_energy_prices_to_model
            WHERE year = %(year)s
            
            UNION ALL
            
            SELECT DISTINCT sector_abbr, 
                            census_division_abbr, 
                            unnest(ARRAY['none', 'no fuel']) as fuel_type,
                            0::NUMERIC as dlrs_per_kwh
            FROM %(schema)s.aeo_energy_prices_to_model
            WHERE year = %(year)s
            
            UNION ALL
            
            SELECT DISTINCT sector_abbr, 
                            census_division_abbr, 
                            'wood' as fuel_type,
                            0.024::NUMERIC as dlrs_per_kwh
            FROM %(schema)s.aeo_energy_prices_to_model
            WHERE year = %(year)s
            
            UNION ALL
            
            SELECT DISTINCT sector_abbr, 
                            census_division_abbr, 
                            unnest(ARRAY['other', 'solar energy', 'district chilled water', 'district hot water', 'coal', 'district steam']) as fuel_type,
                            0.07::NUMERIC as dlrs_per_kwh
            FROM %(schema)s.aeo_energy_prices_to_model
            WHERE year = %(year)s            
            ;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_regional_energy_prices(dataframe, energy_prices_df):
    
    
    in_cols = list(dataframe.columns)
    
    # duplicate the energy_prices_df for each of space heating, space cooling, and water heating, renaming the price field accordingly
    space_heating_prices_df = energy_prices_df.copy()
    rename_map = {'dlrs_per_kwh' : 'space_heat_dlrs_per_kwh', 'fuel_type'    : 'space_heat_fuel'}
    space_heating_prices_df.rename(columns = rename_map, inplace = True)
    
    water_heating_prices_df = energy_prices_df.copy()
    rename_map = {'dlrs_per_kwh' : 'water_heat_dlrs_per_kwh','fuel_type'    : 'water_heat_fuel'}
    water_heating_prices_df.rename(columns = rename_map, inplace = True)

    space_cooling_prices_df = energy_prices_df.copy()
    rename_map = {'dlrs_per_kwh' : 'space_cool_dlrs_per_kwh','fuel_type'    : 'space_cool_fuel'}
    space_cooling_prices_df.rename(columns = rename_map, inplace = True)    
    
    # join dataframes together
    dataframe = pd.merge(dataframe, space_heating_prices_df, how = 'left', on = ['census_division_abbr', 'sector_abbr', 'space_heat_fuel'])
    dataframe = pd.merge(dataframe, water_heating_prices_df, how = 'left', on = ['census_division_abbr', 'sector_abbr', 'water_heat_fuel'])    
    dataframe = pd.merge(dataframe, space_cooling_prices_df, how = 'left', on = ['census_division_abbr', 'sector_abbr', 'space_cool_fuel'])

    # check for nulls; if found, raise error    
    nulls_exist = dataframe[['space_heat_dlrs_per_kwh', 'water_heat_dlrs_per_kwh', 'space_cool_dlrs_per_kwh']].isnull().any().any()
    if nulls_exist == True:
        raise ValueError("null values exist in space_heat_dlrs_per_kwh, water_heat_dlrs_per_kwh, or space_cool_dlrs_per_kwh")    
    
    out_cols = ['space_heat_dlrs_per_kwh', 'water_heat_dlrs_per_kwh', 'space_cool_dlrs_per_kwh']
    return_cols = in_cols + out_cols
    dataframe = dataframe[return_cols]
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_end_user_costs_du(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT *
            FROM %(schema)s.input_du_cost_user
            WHERE year = %(year)s;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_end_user_costs_du(dataframe, end_user_costs_du_df):
    
    # join dataframes together
    dataframe = pd.merge(dataframe, end_user_costs_du_df, how = 'left', on = ['sector_abbr', 'year'])

    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def update_system_ages(dataframe, year, is_first_year, sunk_costs):

    in_cols = list(dataframe.columns)
    
    if is_first_year == True:
        
        # add in the microdata release year field for each agent (2003 for com, 2009 for recs)
        dataframe['microdata_release_year'] = np.where(dataframe['sector_abbr'] == 'res', 2009, 2003)
    
        # calculate the additional years (for new construction set = 0)
        dataframe['add_years'] = np.where(dataframe['new_construction'] == False, year - dataframe['microdata_release_year'], 0)
    
    else:
        dataframe['add_years'] = 2

    # increment the system ages
    if sunk_costs == True:
        dataframe.loc[:, 'space_heat_system_age'] = np.nan
        dataframe.loc[:, 'space_cool_system_age'] = np.nan
        dataframe.loc[:, 'average_system_age'] = np.nan
    elif sunk_costs == False:
        dataframe.loc[:, 'space_heat_system_age'] = np.where(dataframe['new_construction'] == True, 0, dataframe['space_heat_system_age'] + dataframe['add_years'])
        dataframe.loc[:, 'space_cool_system_age'] = np.where(dataframe['new_construction'] == True, 0, dataframe['space_cool_system_age'] + dataframe['add_years'])
        dataframe.loc[:, 'average_system_age'] = np.where(dataframe['new_construction'] == True, 0, dataframe.loc[:, 'average_system_age'] + dataframe['add_years'])  
    else:
        raise ValueError('sunk_costs must be one of: True/False')

    # return just the input  columns
    dataframe = dataframe[in_cols]
    
    return dataframe
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def identify_agents_requiring_new_systems(dataframe, sunk_costs):

    in_cols = list(dataframe.columns)

    if sunk_costs == True:
        # use syntax below to assign np.nan but keep the dtype compatible with boolean True/False operators
        dataframe['needs_heat_system'] = pd.Series(np.nan, dtype = 'object')
        dataframe['needs_cool_system'] = pd.Series(np.nan, dtype = 'object')
        dataframe['needs_average_system'] = pd.Series(np.nan, dtype = 'object')
    elif sunk_costs == False:
        dataframe['needs_heat_system'] = np.where(dataframe['new_construction'] == True, True, dataframe['space_heat_system_age'] > dataframe['space_heat_system_expected_lifetime'])
        dataframe['needs_cool_system'] = np.where(dataframe['new_construction'] == True, True, dataframe['space_cool_system_age'] > dataframe['space_cool_system_expected_lifetime'])
        dataframe['needs_average_system'] = np.where(dataframe['new_construction'] == True, True, dataframe['average_system_age'] > dataframe['average_system_expected_lifetime'])   
    else:
        raise ValueError('sunk_costs must be one of: True/False')    
    # add in the microdata release year field for each agent (2003 for com, 2009 for recs)

    return_cols = ['needs_heat_system', 'needs_cool_system', 'needs_average_system']
    out_cols = in_cols + return_cols
    dataframe = dataframe[out_cols]
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def add_metric_field(dataframe):
    
    dataframe['metric'] = np.where(dataframe['business_model'] == 'tpo', 'percent_monthly_bill_savings', 'payback_period')

    return dataframe
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_financial_parameters(dataframe, financial_parameters_df):
    
    dataframe = pd.merge(dataframe, financial_parameters_df, how = 'left', on = ['sector_abbr', 'business_model', 'tech', 'year'])
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_expected_rate_escalations(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT sector_abbr, 
                   census_division_abbr, 
                   fuel_type, 
                   array_agg(dlrs_per_kwh order by year) as dlrs_per_kwh
            FROM %(schema)s.aeo_energy_prices_to_model
            WHERE year BETWEEN %(year)s and %(year)s + 29
                AND fuel_type IN ('natural gas', 'electricity')
            GROUP BY sector_abbr, census_division_abbr, fuel_type;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_expected_rate_escalations(dataframe, rate_escalations_df):
 
    in_cols = list(dataframe.columns)
    
    # merge in costs of natural gas and electricity specifically (for GHP)
    ng_prices_df = rate_escalations_df[rate_escalations_df['fuel_type'] == 'natural gas']
    # rename price column
    rename_map = {'dlrs_per_kwh' : 'dlrs_per_kwh_natgas'}
    ng_prices_df.rename(columns = rename_map, inplace = True)   
    # drop fuel_type column
    ng_prices_df.drop('fuel_type', axis = 1, inplace = True)
    # join to main dataframe
    join_keys = ['census_division_abbr', 'sector_abbr']
    dataframe = pd.merge(dataframe, ng_prices_df, how = 'left', on = join_keys)   
    
    elec_prices_df = rate_escalations_df[rate_escalations_df['fuel_type'] == 'electricity']
    # rename price column
    rename_map = {'dlrs_per_kwh' : 'dlrs_per_kwh_elec'}
    # drop fuel_type column
    elec_prices_df.rename(columns = rename_map, inplace = True)
    # join to main dataframe
    join_keys = ['census_division_abbr', 'sector_abbr']
    dataframe = pd.merge(dataframe, elec_prices_df, how = 'left', on = join_keys)    

    
    out_cols = ['dlrs_per_kwh_natgas', 'dlrs_per_kwh_elec']
    return_cols = in_cols + out_cols
    dataframe = dataframe[return_cols]
    
    return dataframe
 
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_state_incentives(dataframe, state_incentives_df):        

    #TODO: need to actually write this code (issue #517)
    fill_vals = {'value_of_increment' : 0,
                'value_of_pbi_fit' : 0,
                'value_of_ptc' : 0,
                'pbi_fit_length' : 0,
                'ptc_length' : 0,
                'value_of_rebate' : 0,
                'value_of_tax_credit_or_deduction' : 0}    
    for col, val in fill_vals.iteritems():
        dataframe[col] = np.where(dataframe['modellable'] == True, val, np.nan)
        
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_value_of_itc(df, itc_options, year):
        
    in_cols = df.columns.tolist()        
        
    # create duplicates of the itc data for each business model
    # host-owend
    itc_ho = itc_options.copy() 
    # set the business model
    itc_ho['business_model'] = 'host_owned'
    
    # tpo
    itc_tpo_nonres = itc_options[itc_options['sector_abbr'] <> 'res'].copy() 
    itc_tpo_res = itc_options[itc_options['sector_abbr'] == 'com'].copy() 
    # reset the sector_abbr to res
    itc_tpo_res.loc[:, 'sector_abbr'] = 'res'
    # combine the data
    itc_tpo = pd.concat([itc_tpo_nonres, itc_tpo_res], axis = 0, ignore_index = True)
    # set the business model
    itc_tpo['business_model'] = 'tpo'    
    
    # concatente the business models
    itc_all = pd.concat([itc_ho, itc_tpo], axis = 0, ignore_index = True)

    row_count = df.shape[0]   
    # merge to df
    df = pd.merge(df, itc_all, how = 'left', on = ['sector_abbr', 'year', 'business_model', 'tech'])
    # drop the rows that are outside of the allowable system sizes
    df = df[(df['system_size_kw'] > df['min_size_kw']) & (df['system_size_kw'] <= df['max_size_kw']) | df['system_size_kw'].isnull()]
    # confirm shape hasn't changed
    if df.shape[0] <> row_count:
        raise ValueError('Row count of dataframe changed during merge')
        
    # Calculate the value of ITC (accounting for reduced costs from state/local incentives)
    df['applicable_ic'] = df['ghp_installed_costs_dlrs'] - (df['value_of_tax_credit_or_deduction'] + df['value_of_rebate'] + df['value_of_increment'])
    df['value_of_itc'] =  np.where(df['modellable'] == True, df['applicable_ic'] * df['itc_fraction'], np.nan)
                          
    df = df.drop(['applicable_ic', 'itc_fraction'], axis = 1)
    
    out_cols = ['value_of_itc']
    return_cols = in_cols + out_cols
    
    df = df[return_cols]
    
    return df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def add_bin_id(dataframe):
    
    # add bin_id field -- only purpose is for compatiblity with tech choice function
    dataframe['bin_id'] = dataframe.groupby(['county_id'])['agent_id'].rank(ascending = True, method = 'first')

    return dataframe
    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_incentives_cap(dataframe, incentives_cap_df):
    
    join_keys = ['tech']
    dataframe = pd.merge(dataframe, incentives_cap_df, how = 'left', on = join_keys)
    
    return dataframe



#%%
def apply_nan_to_unmodellable_agents(dataframe, lkup_table, join_keys):
    
    # NOTE: only works for simple, single-value columns. compound (i.e., list or array) columns do not work
    # figure out waht the newly added columns are
    new_cols = list(set(lkup_table.columns.tolist()) - set(join_keys))
    dataframe.loc[dataframe['modellable'] == False, new_cols] = np.nan
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_state_starting_capacities_ghp(con, schema):

    inputs = locals().copy()    
    
    sql = '''SELECT sector_abbr,
                    state_abbr,
                    capacity_tons as cumulative_market_share_tons,
                    'ghp'::text as tech
             FROM diffusion_geo.starting_capacities_2012_ghp;''' % inputs
    df = pd.read_sql(sql, con)
    
    return df    
    

    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def estimate_initial_market_shares(dataframe, state_starting_capacities_df, year):
  
    # calculate the total capacity of systems for market eligible in each state and sector
    dataframe['market_eligible_capacity_tons_in_bin'] = dataframe['market_eligible_buildings_in_bin'] * dataframe['ghp_system_size_tons']
    state_total_market_eligible_capacity = dataframe[['state_abbr', 'sector_abbr', 'tech', 'market_eligible_capacity_tons_in_bin']].groupby(['state_abbr', 'sector_abbr', 'tech']).sum().reset_index()
    # rename column
    state_total_market_eligible_capacity.rename(columns = {'market_eligible_capacity_tons_in_bin' : 'market_eligible_capacity_tons_in_state'}, inplace = True)
    
    # combine with state starting capacities
    state_df = pd.merge(state_total_market_eligible_capacity, state_starting_capacities_df, how = 'left', on = ['tech', 'state_abbr', 'sector_abbr'])
    state_df['cumulative_market_share_pct'] = state_df['cumulative_market_share_tons'] / state_df['market_eligible_capacity_tons_in_state']
    # add dummy columns for incremental market share
    state_df['new_incremental_market_share_pct'] = np.nan
    state_df['new_incremental_capacity_tons'] = np.nan
    state_df['year'] = year - 2
    
    return state_df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def write_cumulative_market_share(con, cur, cumulative_market_share_df, schema):
    
    inputs = locals().copy()    
    
    inputs['out_table'] = '%(schema)s.output_market_summary_ghp'  % inputs

    # open an in memory stringIO file (like an in memory csv)
    s = StringIO()
    # write the data to the stringIO
    out_cols = ['year',
                'state_abbr',
                'sector_abbr',
                'cumulative_market_share_pct', 
                'cumulative_market_share_tons', 
                'new_incremental_market_share_pct', 
                'new_incremental_capacity_tons']
    cumulative_market_share_df[out_cols].to_csv(s, index = False, header = False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    cur.copy_expert('COPY %(out_table)s FROM STDOUT WITH CSV' % inputs, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()    
    s.close()


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_market_last_year(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT state_abbr, 
                    sector_abbr, 
                    cumulative_market_share_pct as existing_state_market_share_pct,
                    cumulative_market_share_tons as existing_state_market_share_tons
            FROM %(schema)s.output_market_summary_ghp
            WHERE year = %(year)s - 2;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_market_last_year(dataframe, market_last_year_df):
    
    dataframe = pd.merge(dataframe, market_last_year_df, how = 'left', on = ['state_abbr', 'sector_abbr'])
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_bass_ratio(dataframe, market_last_year_df):

    # record input columns
    in_cols = list(dataframe.columns)

    # calculate bass ratio for each state and sector as: bass ratio = existing market share in tons / max market share in tons

    # calculate the total capacity that is available based on the max market share for each bin
    dataframe['max_market_share_tons'] = dataframe['max_market_share'] * dataframe['ghp_system_size_tons'] * dataframe['market_eligible_buildings_in_bin']
    # sum to state level
    state_max_market_share_tons = dataframe[['state_abbr', 'sector_abbr', 'max_market_share_tons']].groupby(['state_abbr', 'sector_abbr']).sum().reset_index()
    # merge to the market last year
    state_df = pd.merge(state_max_market_share_tons, market_last_year_df, how = 'left', on = ['state_abbr', 'sector_abbr'])
    # calculate the ratio of market share to max market share tons
    state_df['bass_ratio'] = np.where(state_df['existing_state_market_share_tons'] > state_df['max_market_share_tons'], 0., state_df['existing_state_market_share_tons'] / state_df['max_market_share_tons'])

    # merge to the agents dataframe
    dataframe = pd.merge(dataframe, state_df, how = 'left', on = ['state_abbr', 'sector_abbr'])    
    
    out_cols = ['bass_ratio']
    return_cols = in_cols + out_cols
    dataframe = dataframe[return_cols]
    
    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_existing_market_share_pct(dataframe, is_first_year):

    # record input columns
    in_cols = list(dataframe.columns)

    if is_first_year == True:
        dataframe['existing_market_share_pct'] = dataframe['bass_ratio'] * dataframe['max_market_share']    
    elif is_first_year == False:
        dataframe['existing_market_share_pct'] = np.where(dataframe['new_construction'] == True, 0., dataframe['market_share_last_year'])
    else:
        raise ValueError('is_first_year must be one of: True/False')        
    
    out_cols = ['existing_market_share_pct']
    return_cols = in_cols + out_cols
    dataframe = dataframe[return_cols]
    
    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_bass_params(dataframe, bass_params_df):
    
    dataframe = pd.merge(dataframe, bass_params_df, how = 'left', on = ['state_abbr', 'sector_abbr', 'tech'])
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_diffusion_result_metrics(dataframe):
    
    # ensure no diffusion for non-bass deployable agents
    dataframe.loc[:, 'diffusion_market_share'] = dataframe['diffusion_market_share'] * dataframe['bass_deployable'] 
    # market sahre is equal to the diffusion_market_share (which has already been capped to ensure it isn't lower than the existing market share pct)
    dataframe['market_share'] = np.maximum(dataframe['diffusion_market_share'], dataframe['market_share_last_year'])
    # calculate the new market share (market_share - existing_market_share)
    dataframe['new_market_share'] = dataframe['market_share'] - dataframe['market_share_last_year']
    # cap the new_market_share where the market share exceeds the max market share
    dataframe.loc[:, 'new_market_share'] = np.where(dataframe['market_share'] > dataframe['max_market_share'], 0, dataframe['new_market_share'])

    # calculate new adopters, capacity and market value
    dataframe['new_adopters'] = dataframe['new_market_share'] * dataframe['bass_deployable_buildings_in_bin']
    dataframe['new_capacity'] = dataframe['new_adopters'] * dataframe['ghp_system_size_tons']
    dataframe['new_market_value'] = dataframe['new_adopters'] * dataframe['ghp_installed_costs_dlrs']
    
    # then add these values to values from last year to get cumulative values:
    dataframe['number_of_adopters'] = dataframe['number_of_adopters_last_year'] + dataframe['new_adopters']
    dataframe['installed_capacity'] = dataframe['installed_capacity_last_year'] + dataframe['new_capacity'] # All capacity in tons in the model
    dataframe['market_value'] = dataframe['market_value_last_year'] + dataframe['new_market_value']

    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def summarize_state_deployment(dataframe, year):
    
    # calculate the total eligible capacity in bin
    dataframe['market_eligible_capacity_in_bin'] = dataframe['ghp_system_size_tons'] * dataframe['market_eligible_buildings_in_bin']
    state_df = dataframe[['state_abbr', 'sector_abbr', 'new_capacity', 'installed_capacity', 'market_eligible_capacity_in_bin']].groupby(['state_abbr', 'sector_abbr']).sum().reset_index()

    # rename columns
    rename_map = {'new_capacity' : 'new_incremental_capacity_tons',
                   'installed_capacity' : 'cumulative_market_share_tons',
                   'market_eligible_capacity_in_bin' : 'market_eligible_capacity_in_state'}
    state_df.rename(columns = rename_map, inplace = True)
    
    state_df['new_incremental_market_share_pct'] = state_df['new_incremental_capacity_tons'] / state_df['market_eligible_capacity_in_state']
    state_df['cumulative_market_share_pct'] = state_df['cumulative_market_share_tons'] / state_df['market_eligible_capacity_in_state']
    
    state_df['year'] = year
    
    return state_df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def summarize_results_for_next_year(dataframe, sunk_costs):
    
    dataframe['market_value_last_year'] = dataframe['market_value']
    dataframe['market_share_last_year'] = dataframe['market_share']
    dataframe['installed_capacity_last_year'] = dataframe['installed_capacity']
    dataframe['number_of_adopters_last_year'] = dataframe['number_of_adopters']
    
    # if sunk_costs == True, the system ages don't really matter, so set to np.nan
    if sunk_costs == True:
        dataframe['space_heat_system_age_last_year'] = np.nan
        dataframe['space_cool_system_age_last_year'] = np.nan
        dataframe['average_system_age_last_year'] = np.nan
    elif sunk_costs == False:
        dataframe['space_heat_system_age_last_year'] = np.where(dataframe['bass_deployable'] == True, 0., dataframe['space_heat_system_age'])
        dataframe['space_cool_system_age_last_year'] = np.where(dataframe['bass_deployable'] == True, 0., dataframe['space_cool_system_age'])
        dataframe['average_system_age_last_year'] = np.where(dataframe['bass_deployable'] == True, 0., dataframe['average_system_age'])
    else:
        raise ValueError('sunk_costs must be one of: True/False')

    
    out_cols = ['agent_id',
                'market_share_last_year',
                'market_value_last_year',
                'installed_capacity_last_year',
                'number_of_adopters_last_year',
                'space_heat_system_age_last_year',
                'space_cool_system_age_last_year',
                'average_system_age_last_year'
    ]
    dataframe = dataframe[out_cols]
    
    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def append_previous_year_results(dataframe, agents_last_year_df):
    
    in_cols = dataframe.columns.tolist()
    
    new_cols = { 'market_share_last_year' : 0.,
                 'market_value_last_year' : 0.,
                'installed_capacity_last_year' : 0.,
                'number_of_adopters_last_year' : 0.
                }    
    
    if agents_last_year_df is not None:
        dataframe = pd.merge(dataframe, agents_last_year_df, how = 'left', on = 'agent_id')
        # update the system ages 
        dataframe.loc[:, 'space_heat_system_age'] = dataframe['space_heat_system_age_last_year']
        dataframe.loc[:, 'space_cool_system_age'] = dataframe['space_cool_system_age_last_year']
        dataframe.loc[:, 'average_system_age'] = dataframe['average_system_age_last_year']  
        ## initialize values for new contruction to zero
        for col, val in new_cols.iteritems():
            dataframe.loc[dataframe['new_construction'] == True, col] = val
    else:
        # initialize values for all rows to zero
        for col, val in new_cols.iteritems():
            dataframe[col] = val
            

    out_cols = ['market_share_last_year',
                'market_value_last_year',
                'installed_capacity_last_year',
                'number_of_adopters_last_year']
    return_cols = in_cols + out_cols
    dataframe = dataframe[return_cols]
    
    return dataframe
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def create_new_id_column(dataframe, columns, id_col_name):
    
    unique_combos = dataframe.groupby(columns).size().reset_index()
    # drop the new columns
    new_column_i = len(unique_combos.columns.tolist()) - 1
    new_column = unique_combos.columns[new_column_i]
    unique_combos.drop(new_column, axis = 1, inplace = True)
    unique_combos[id_col_name] = unique_combos.index
    
    dataframe = pd.merge(dataframe, unique_combos, how = 'left', on = columns)

    return dataframe

    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def mark_excluded_options(dataframe):
    
    dataframe['excluded_option'] = (dataframe['market_eligible'] == False) | ((dataframe['business_model'] == 'tpo') & (dataframe['leasing_allowed'] == False))
    
    return dataframe    


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def sanitize_decision_col(dataframe, decision_col, new_col):
    
    dataframe[new_col] = np.where((dataframe[decision_col] < 0) | (dataframe[decision_col].isnull()), 0, dataframe[decision_col])
    
    return dataframe   
