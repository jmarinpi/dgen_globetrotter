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
             WHERE year = %(year)s;""" % inputs
    
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
    dataframe['ghp_system_size_tons'] = dataframe['cooling_ton_per_sqft'] * dataframe['totsqft']
    # add system size kw (for compatibility with downstream code)
    dataframe['system_size_kw'] = dataframe['ghp_system_size_tons'] * 3.5168525

    # next, calculate the ghx length required to provide that capacity
    dataframe['ghx_length_ft'] = dataframe['ghp_system_size_tons'] * dataframe['ghx_length_ft_per_cooling_ton']

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
    dataframe['viable_sys_config'] = dataframe['length_installable_ft'] >= dataframe['ghx_length_ft']
    
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
def identify_developable_agents(dataframe):

    # TODO: also account for the fact that some microdata can't be represented by CRBs
    dataframe['developable'] = (dataframe['viable_sys_config'] == True) & (dataframe['needs_replacement_average_system'] == True)
   
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
             WHERE year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)

    return df

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_tech_costs_baseline(dataframe, tech_costs_baseline_df):    
    

    dataframe = pd.merge(dataframe, tech_costs_baseline_df, how = 'left', on = ['sector_abbr', 'baseline_system_type'])
    # Installed Costs
    dataframe['baseline_equipment_cost_dollars'] = dataframe['ghp_system_size_tons'] * dataframe['hvac_equipment_cost_dollars_per_cooling_ton']
    dataframe['baseline_rest_of_system_cost_dlrs'] = np.where(dataframe['new_construction'] == True, 
                                                     dataframe['baseline_new_rest_of_system_costs_dollars_per_cooling_ton'] * dataframe['ghp_system_size_tons'], 
                                                     dataframe['baseline_new_rest_of_system_costs_dollars_per_cooling_ton'] * dataframe['ghp_system_size_tons'] * dataframe['baseline_retrofit_rest_of_system_multiplier'])
    dataframe['baseline_installed_costs_dlrs'] = dataframe['baseline_equipment_cost_dollars'] + dataframe['baseline_rest_of_system_cost_dlrs']
    
    return dataframe



#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_site_energy_consumption_ghp(dataframe):
    
    # determine the total amount of natural gas and elec used for space heating and cooling by BASELINE HVAC
    dataframe['site_hvac_natgas_per_building_kwh'] = (dataframe['site_space_heat_per_building_in_bin_kwh'] * (dataframe['space_heat_fuel'] == 'natural gas') + 
                                                      dataframe['site_space_cool_per_building_in_bin_kwh'] * (dataframe['space_cool_fuel'] == 'natural gas'))
    dataframe['site_hvac_elec_per_building_kwh'] = (dataframe['site_space_heat_per_building_in_bin_kwh'] * (dataframe['space_heat_fuel'] == 'electricity') + 
                                                    dataframe['site_space_cool_per_building_in_bin_kwh'] * (dataframe['space_cool_fuel'] == 'electricity'))
    # determine the total amount of natural gas and elec used for space heating and cooling by GHP
    # (account for energy savings from CRBS)
    dataframe['site_ghp_natgas_per_building_kwh'] = dataframe['site_hvac_natgas_per_building_kwh'] * (1. - dataframe['savings_pct_natural_gas_consumption'])
    dataframe['site_ghp_elec_per_building_kwh'] = dataframe['site_hvac_elec_per_building_kwh'] * (1. - dataframe['savings_pct_electricity_consumption'])        
    
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
def update_system_ages(dataframe, year):

    in_cols = list(dataframe.columns)
    
    # add in the microdata release year field for each agent (2003 for com, 2009 for recs)
    dataframe['microdata_release_year'] = np.where(dataframe['sector_abbr'] == 'res', 2009, 2003)
    
    # calculate the additional years (for new construction set = 0)
    dataframe['add_years'] = np.where(dataframe['new_construction'] == False, year - dataframe['microdata_release_year'], 0)
    
    # increment the system ages
    dataframe.loc[:, 'space_heat_system_age'] = dataframe['space_heat_system_age'] + dataframe['add_years']
    dataframe.loc[:, 'space_cool_system_age'] = dataframe['space_cool_system_age'] + dataframe['add_years']
    dataframe.loc[:, 'average_system_age'] = dataframe.loc[:, 'average_system_age'] + dataframe['add_years']
    
    # return just the input  columns
    dataframe = dataframe[in_cols]
    
    return dataframe
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def check_system_expirations(dataframe):

    in_cols = list(dataframe.columns)
    
    # add in the microdata release year field for each agent (2003 for com, 2009 for recs)
    dataframe['needs_replacement_heat_system'] = dataframe['space_heat_system_age'] > dataframe['space_heat_system_expected_lifetime']    
    dataframe['needs_replacement_cool_system'] = dataframe['space_cool_system_age'] > dataframe['space_cool_system_expected_lifetime']        
    dataframe['needs_replacement_average_system'] = dataframe['average_system_age'] > dataframe['average_system_expected_lifetime']
    
    return_cols = ['needs_replacement_heat_system', 'needs_replacement_cool_system', 'needs_replacement_average_system']
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
    dataframe = pd.merge(dataframe, ng_prices_df, how = 'left', on = ['census_division_abbr', 'sector_abbr'])
    
    elec_prices_df = rate_escalations_df[rate_escalations_df['fuel_type'] == 'electricity']
    # rename price column
    rename_map = {'dlrs_per_kwh' : 'dlrs_per_kwh_elec'}
    # drop fuel_type column
    elec_prices_df.rename(columns = rename_map, inplace = True)
    # join to main dataframe
    dataframe = pd.merge(dataframe, elec_prices_df, how = 'left', on = ['census_division_abbr', 'sector_abbr'])    
    
    
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
        dataframe[col] = val
        
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
    keep_rows = np.where(df['tech'] == 'ghp', (df['ghp_system_size_tons'] > df['min_size_kw_or_tons']) & (df['ghp_system_size_tons'] <= df['max_size_kw_or_tons']), (df['system_size_kw'] > df['min_size_kw_or_tons']) & (df['system_size_kw'] <= df['max_size_kw_or_tons']))
    df = df[keep_rows]
    # confirm shape hasn't changed
    if df.shape[0] <> row_count:
        raise ValueError('Row count of dataframe changed during merge')
        
    # Calculate the value of ITC (accounting for reduced costs from state/local incentives)
    df['applicable_ic'] = df['ghp_installed_costs_dlrs'] - (df['value_of_tax_credit_or_deduction'] + df['value_of_rebate'] + df['value_of_increment'])
    df['value_of_itc'] =  (
                            df['applicable_ic'] *
                            df['itc_fraction'] 
                          )
                          
    df = df.drop(['applicable_ic', 'itc_fraction'], axis = 1)
    
    out_cols = ['value_of_itc']
    return_cols = in_cols + out_cols
    
    df = df[return_cols]
    
    return df    