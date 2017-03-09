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
from agent_mutation import get_depreciation_schedule, apply_depreciation_schedule, get_leasing_availability, apply_leasing_availability

# GLOBAL SETTINGS

# load logger
logger = utilfunc.get_logger()

# configure psycopg2 to treat numeric values as floats (improves
# performance of pulling data from the database)
DEC2FLOAT = pg.extensions.new_type(
    pg.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
pg.extensions.register_type(DEC2FLOAT)


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def replicate_agents_by_factor(dataframe, new_column_name, factor_list):

    df_list = []
    for factor in factor_list:
        temp_df = dataframe.copy()
        temp_df[new_column_name] = factor
        df_list.append(temp_df)

    dataframe = pd.concat(df_list, axis=0, ignore_index=True)

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_technology_performance_improvements_ghp(con, schema, year):

    inputs = locals().copy()
    sql = """SELECT year,
                    ghp_heat_pump_lifetime_yrs,
                    ghp_efficiency_improvement_pct
             FROM %(schema)s.input_ghp_performance_improvements
             WHERE year = %(year)s;""" % inputs

    df = pd.read_sql(sql, con, coerce_float=False)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_technology_performance_ghp(dataframe, tech_performance_df):

    # join on sys_config
    dataframe = pd.merge(dataframe, tech_performance_df, how='left', on='year')

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_technology_performance_improvements_baseline(con, schema, year):

    inputs = locals().copy()
    sql = """SELECT sector_abbr,
                    system_lifetime_yrs as baseline_system_lifetime_yrs,
                    efficiency_improvement_pct as baseline_efficiency_improvement_pct
             FROM %(schema)s.input_baseline_system_performance
             WHERE year = %(year)s;""" % inputs

    df = pd.read_sql(sql, con, coerce_float=False)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_technology_performance_baseline(dataframe, tech_performance_df):

    # join on sector and baseline type
    dataframe = pd.merge(dataframe, tech_performance_df,
                         how='left', on=['sector_abbr'])
    # fill nas for unmodellable agents
    out_cols = ['baseline_system_lifetime_yrs',
                'baseline_efficiency_improvement_pct']
    dataframe.loc[dataframe['modellable'] == False, out_cols] = np.nan

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_ghp_baseline_type_lkup(con, schema):

    inputs = locals().copy()
    sql = """SELECT baseline_type,
                    sector_abbr,
                    pba_or_typehuq,
                    pba_or_typehuq_desc,
                    space_heat_equip,
                    space_heat_fuel,
                    space_cool_equip,
                    space_cool_fuel
              FROM diffusion_geo.eia_buildings_to_ornl_baseline_lkup
              WHERE provided = TRUE;"""

    df = pd.read_sql(sql, con, coerce_float=False)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_ghp_baseline_simulations(con, schema):

    inputs = locals().copy()
    sql = """SELECT baseline_type,
                	 iecc_climate_zone,
                	 savings_pct_electricity_consumption,
                   savings_pct_natural_gas_consumption,
                   crb_ghx_length_ft,
                   crb_cooling_capacity_ton,
                   crb_totsqft,
                   cooling_ton_per_sqft
          FROM diffusion_geo.ornl_ghp_simulations;"""

    df = pd.read_sql(sql, con, coerce_float=False)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def map_agents_to_ghp_baseline_types(dataframe, baseline_lkup_df):

    in_cols = dataframe.columns.tolist()

    join_cols = ['sector_abbr',
                 'pba_or_typehuq',
                 'space_heat_equip',
                 'space_heat_fuel',
                 'space_cool_equip',
                 'space_cool_fuel']

    dataframe = pd.merge(dataframe, baseline_lkup_df, how='left', on=join_cols)
    # mark NAs (those with no mapping) with a value of -1
    dataframe.loc[:, 'baseline_type'] = dataframe['baseline_type'].fillna(-1)
    # set as ingeteger
    dataframe.loc[:, 'baseline_type'] = dataframe[
        'baseline_type'].astype('int64')

    out_cols = ['baseline_type']
    return_cols = in_cols + out_cols
    dataframe = dataframe[return_cols]

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def join_crb_ghp_simulations(dataframe, baseline_ghp_sims_df):

    # join on baseline_type and geographic variables (= iecc_cliamte_zone and
    # gtc value)
    dataframe = pd.merge(dataframe, baseline_ghp_sims_df, how='left', on=[
                         'baseline_type', 'iecc_climate_zone'])

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def mark_unmodellable_agents(dataframe):

    dataframe['modellable'] = np.where((dataframe['baseline_type'] == -1) | (
        dataframe['savings_pct_electricity_consumption'].isnull()), False, True)

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_siting_constraints_ghp(con, schema, year):

    inputs = locals().copy()
    sql = """SELECT area_per_well_sqft_vertical,
                    max_well_depth_ft,
                    area_per_pipe_length_sqft_per_foot_horizontal
             FROM %(schema)s.input_ghp_siting;""" % inputs

    df = pd.read_sql(sql, con, coerce_float=False)
    # add year field (only to facilitate downstream joins)
    df['year'] = year

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def size_systems_ghp(dataframe):

    # output should be both ghx_length_ft and cooling_capacity_ton

    # first calculate required system capacity
    dataframe['ghp_system_size_tons'] = np.where(dataframe['modellable'] == True, dataframe[
                                                 'cooling_ton_per_sqft'] * dataframe['totsqft'], np.nan)
    # add system size kw (for compatibility with downstream code)
    dataframe['system_size_kw'] = np.where(dataframe['modellable'] == True, dataframe[
                                           'ghp_system_size_tons'] * 3.5168525, np.nan)

    # next, calculate the ghx length required to provide that capacity
    dataframe['ghx_length_ft'] = np.where(dataframe['modellable'] == True, dataframe[
                                          'ghp_system_size_tons'] * gtc_to_ghx_length(dataframe['gtc_btu_per_hftf']), np.nan)

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def gtc_to_ghx_length(gtc_val):

    # TODO: modify this relationship once provided by Xiaobing.
    # ghx_length_ft_per_cooling_ton = gtc_val * ...

    # output constant = average value from ORNL sims
    ghx_length_ft_per_cooling_ton = 329.9464559542199422

    return ghx_length_ft_per_cooling_ton


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_siting_constraints_ghp(dataframe, siting_constraints_df):

    in_cols = dataframe.columns.tolist()

    dataframe = pd.merge(dataframe, siting_constraints_df,
                         how='left', on='year')
    dataframe['parcel_size_sqft'] = dataframe['acres_per_bldg'] * 43560.
    # find the number of wells and total length of vertical loop that could be
    # installed
    dataframe['n_installable_wells_vertical'] = dataframe[
        'parcel_size_sqft'] / dataframe['area_per_well_sqft_vertical']
    dataframe['length_installable_vertical_ft'] = dataframe[
        'n_installable_wells_vertical'] * dataframe['max_well_depth_ft']
    # find the length of horizontal loop that could be isntalled
    dataframe['length_installable_horizontal_ft'] = dataframe[
        'parcel_size_sqft'] / dataframe['area_per_pipe_length_sqft_per_foot_horizontal']
    # determine whether each option is viable
    dataframe['length_installable_ft'] = np.where(dataframe['sys_config'] == 'vertical', dataframe[
                                                  'length_installable_vertical_ft'], dataframe['length_installable_horizontal_ft'])
    dataframe['viable_sys_config'] = np.where(dataframe['modellable'] == True, dataframe[
                                              'length_installable_ft'] >= dataframe['ghx_length_ft'], False)

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


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def identify_bass_deployable_agents(dataframe):

    # deployable customers are those:
    # (1) with a system that can be sited on the property,
    # (2) that are modellable (i.e., we have a CRB model to use), and

    dataframe['bass_deployable'] = (dataframe['viable_sys_config'] == True) & (
        dataframe['modellable'] == True)
    dataframe['bass_deployable_buildings_in_bin'] = np.where(
        dataframe['bass_deployable'] == True, dataframe['buildings_in_bin'], 0.)

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def identify_market_eligible_agents(dataframe):

    dataframe['market_eligible'] = (dataframe['viable_sys_config'] == True) & (
        dataframe['modellable'] == True)
    dataframe['market_eligible_buildings_in_bin'] = np.where(
        dataframe['market_eligible'] == True, dataframe['buildings_in_bin'], 0.)

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_technology_costs_ghp(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT sector_abbr,
                    sys_config,
                    heat_exchanger_cost_dollars_per_ft,
                    ghp_cost_improvement_pct,
                    fixed_om_dollars_per_sf_per_year as ghp_fixed_om_dollars_per_sf_per_year
             FROM %(schema)s.input_ghp_costs
             WHERE year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float=False)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_tech_costs_ghp(dataframe, tech_costs_ghp_df):

    dataframe = pd.merge(dataframe, tech_costs_ghp_df,
                         how='left', on=['sector_abbr', 'sys_config'])
    # Installed Costs
    dataframe['ghx_cost_dlrs'] = dataframe['ghx_length_ft'] * \
        dataframe['heat_exchanger_cost_dollars_per_ft']
    dataframe['ghp_heat_pump_cost_dlrs'] = calc_heat_pump_costs(
        dataframe['ghp_system_size_tons']) * (1. - dataframe['ghp_cost_improvement_pct'])
    dataframe['ghp_installed_costs_dlrs'] = dataframe[
        'ghx_cost_dlrs'] + dataframe['ghp_heat_pump_cost_dlrs']
    dataframe['ghp_fixed_om_dlrs_per_year'] = dataframe[
        'ghp_fixed_om_dollars_per_sf_per_year'] * dataframe['totsqft']
    # reset values to NA where the system isn't modellable
    out_cols = ['ghx_cost_dlrs',
                'ghp_heat_pump_cost_dlrs',
                'ghp_installed_costs_dlrs',
                'ghp_fixed_om_dlrs_per_year']
    dataframe.loc[dataframe['modellable'] == False, out_cols] = np.nan

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def calc_heat_pump_costs(ghp_system_size_tons):

    # TODO: update function with cost correlation provided by Xiaobing
    ghp_heat_pump_costs = 1600.

    return ghp_heat_pump_costs


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_technology_costs_baseline(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT sector_abbr,
                    hvac_equipment_cost_improvement_pct,
                    fixed_om_dollars_per_sf_per_year as baseline_fixed_om_dollars_per_sf_per_year
             FROM %(schema)s.input_baseline_costs_hvac
             WHERE year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float=False)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_tech_costs_baseline(dataframe, tech_costs_baseline_df):

    dataframe = pd.merge(dataframe, tech_costs_baseline_df,
                         how='left', on=['sector_abbr'])

    # TODO: change heat capacity input in calc_baseline_equipment_costs from None based on Xiaobing's regression
#    dataframe['baseline_equipment_costs_dlrs'] = calc_baseline_equipment_costs(dataframe['ghp_system_size_tons'], dataframe['heating_capacity']) * (1. - dataframe['hvac_equipment_cost_improvement_pct'])
    dataframe['baseline_equipment_costs_dlrs'] = calc_baseline_equipment_costs(
        dataframe['ghp_system_size_tons'], None) * (1. - dataframe['hvac_equipment_cost_improvement_pct'])

    dataframe['baseline_installed_costs_dlrs'] = dataframe[
        'baseline_equipment_costs_dlrs']
    dataframe['baseline_fixed_om_dlrs_per_year'] = dataframe[
        'baseline_fixed_om_dollars_per_sf_per_year'] * dataframe['totsqft']

    # reset values to NA where the system isn't modellable
    out_cols = ['hvac_equipment_cost_improvement_pct',
                'baseline_fixed_om_dollars_per_sf_per_year',
                'baseline_equipment_costs_dlrs',
                'baseline_installed_costs_dlrs',
                'baseline_fixed_om_dlrs_per_year']
    dataframe.loc[dataframe['modellable'] == False, out_cols] = np.nan

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def calc_baseline_equipment_costs(system_size_tons, heating_capacity):

    # TODO: update function with cost correlation provided by Xiaobing
    baseline_equipment_costs = 3000.

    return baseline_equipment_costs


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def calculate_site_energy_consumption_ghp(dataframe):

    # determine the total consumption for space heating and cooling by fuel -
    # BASELINE HVAC
    dataframe['baseline_site_natgas_per_building_kwh'] = dataframe['site_space_heat_per_building_in_bin_kwh'] * \
        (dataframe['space_heat_fuel'] == 'natural gas') * \
        (1. - dataframe['baseline_efficiency_improvement_pct'])
    dataframe['baseline_site_elec_per_building_kwh'] = (dataframe['site_space_heat_per_building_in_bin_kwh'] * (dataframe['space_heat_fuel'] == 'electricity') +
                                                        dataframe['site_space_cool_per_building_in_bin_kwh'] * (dataframe['space_cool_fuel'] == 'electricity')) * (1. - dataframe['baseline_efficiency_improvement_pct'])
    dataframe['baseline_site_propane_per_building_kwh'] = dataframe['site_space_heat_per_building_in_bin_kwh'] * \
        (dataframe['space_heat_fuel'] == 'propane') * \
        (1. - dataframe['baseline_efficiency_improvement_pct'])
    dataframe['baseline_site_fuel_oil_per_building_kwh'] = dataframe['site_space_heat_per_building_in_bin_kwh'] * \
        (dataframe['space_heat_fuel'] == 'distallate fuel oil') * \
        (1. - dataframe['baseline_efficiency_improvement_pct'])
    dataframe['baseline_site_dist_hot_water_per_building_kwh'] = dataframe['site_space_heat_per_building_in_bin_kwh'] * \
        (dataframe['space_heat_fuel'] == 'district hot water') * \
        (1. - dataframe['baseline_efficiency_improvement_pct'])
    dataframe['baseline_site_dist_steam_per_building_kwh'] = dataframe['site_space_heat_per_building_in_bin_kwh'] * \
        (dataframe['space_heat_fuel'] == 'district steam') * \
        (1. - dataframe['baseline_efficiency_improvement_pct'])
    dataframe['baseline_site_dist_chil_water_per_building_kwh'] = dataframe['site_space_cool_per_building_in_bin_kwh'] * \
        (dataframe['space_cool_fuel'] == 'district chilled water') * \
        (1. - dataframe['baseline_efficiency_improvement_pct'])

    # determine the total consumption for space heating and cooling by fuel - GHP
    # (account for energy savings from CRBS)
    dataframe['ghp_site_natgas_per_building_kwh'] = np.where(dataframe['modellable'] == True, dataframe['baseline_site_natgas_per_building_kwh'] * (
        1. - dataframe['savings_pct_natural_gas_consumption'] * (1. + dataframe['ghp_efficiency_improvement_pct'])), np.nan)
    dataframe['ghp_site_elec_per_building_kwh'] = np.where(dataframe['modellable'] == True, dataframe['baseline_site_elec_per_building_kwh'] * (
        1. - dataframe['savings_pct_electricity_consumption'] * (1. + dataframe['ghp_efficiency_improvement_pct'])), np.nan)
    # natural gas savings used to model savings of other fossil fuels
    # (propane, fuel oil) and district heating fuels
    dataframe['ghp_site_propane_per_building_kwh'] = np.where(dataframe['modellable'] == True, dataframe['baseline_site_propane_per_building_kwh'] * (
        1. - dataframe['savings_pct_natural_gas_consumption'] * (1. + dataframe['ghp_efficiency_improvement_pct'])), np.nan)
    dataframe['ghp_site_fuel_oil_per_building_kwh'] = np.where(dataframe['modellable'] == True, dataframe['baseline_site_fuel_oil_per_building_kwh'] * (
        1. - dataframe['savings_pct_natural_gas_consumption'] * (1. + dataframe['ghp_efficiency_improvement_pct'])), np.nan)
    dataframe['ghp_site_dist_hot_water_per_building_kwh'] = np.where(dataframe['modellable'] == True, dataframe['baseline_site_dist_hot_water_per_building_kwh'] * (
        1. - dataframe['savings_pct_natural_gas_consumption'] * (1. + dataframe['ghp_efficiency_improvement_pct'])), np.nan)
    dataframe['ghp_site_dist_steam_per_building_kwh'] = np.where(dataframe['modellable'] == True, dataframe['baseline_site_dist_steam_per_building_kwh'] * (
        1. - dataframe['savings_pct_natural_gas_consumption'] * (1. + dataframe['ghp_efficiency_improvement_pct'])), np.nan)
    # electricity savings used to model district chilled water savings
    dataframe['ghp_site_dist_chil_water_per_building_kwh'] = np.where(dataframe['modellable'] == True, dataframe['baseline_site_dist_chil_water_per_building_kwh'] * (
        1. - dataframe['savings_pct_electricity_consumption'] * (1. + dataframe['ghp_efficiency_improvement_pct'])), np.nan)

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def add_metric_field(dataframe):

    dataframe['metric'] = np.where(dataframe[
                                   'business_model'] == 'tpo', 'percent_monthly_bill_savings', 'payback_period')

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_financial_parameters(dataframe, financial_parameters_df):

    dataframe = pd.merge(dataframe, financial_parameters_df, how='left', on=[
                         'sector_abbr', 'business_model', 'tech', 'year'])

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_expected_rate_escalations(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT sector_abbr,
                   census_division_abbr,
                   fuel_type,
                   array_agg(dlrs_per_kwh order by year)::DOUBLE PRECISION[] as dlrs_per_kwh
            FROM %(schema)s.aeo_energy_prices_to_model
            WHERE year BETWEEN %(year)s and %(year)s + 29
            GROUP BY sector_abbr, census_division_abbr, fuel_type;""" % inputs

    df = pd.read_sql(sql, con, coerce_float=False)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_expected_rate_escalations(dataframe, rate_escalations_df):

    in_cols = list(dataframe.columns)

    # merge in costs of all fuels (natgas, electricity, propane, fuel oil) for
    # GHP
    ng_prices_df = rate_escalations_df[
        rate_escalations_df['fuel_type'] == 'natural gas']
    # rename price column
    rename_map = {'dlrs_per_kwh': 'dlrs_per_kwh_natgas'}
    ng_prices_df.rename(columns=rename_map, inplace=True)
    # drop fuel_type column
    ng_prices_df.drop('fuel_type', axis=1, inplace=True)
    # join to main dataframe
    join_keys = ['census_division_abbr', 'sector_abbr']
    dataframe = pd.merge(dataframe, ng_prices_df, how='left', on=join_keys)

    elec_prices_df = rate_escalations_df[
        rate_escalations_df['fuel_type'] == 'electricity']
    # rename price column
    rename_map = {'dlrs_per_kwh': 'dlrs_per_kwh_elec'}
    # drop fuel_type column
    elec_prices_df.rename(columns=rename_map, inplace=True)
    # join to main dataframe
    join_keys = ['census_division_abbr', 'sector_abbr']
    dataframe = pd.merge(dataframe, elec_prices_df, how='left', on=join_keys)

    propane_prices_df = rate_escalations_df[
        rate_escalations_df['fuel_type'] == 'propane']
    # rename price column
    rename_map = {'dlrs_per_kwh': 'dlrs_per_kwh_propane'}
    # drop fuel_type column
    propane_prices_df.rename(columns=rename_map, inplace=True)
    # join to main dataframe
    join_keys = ['census_division_abbr', 'sector_abbr']
    dataframe = pd.merge(dataframe, propane_prices_df,
                         how='left', on=join_keys)

    fuel_oil_prices_df = rate_escalations_df[
        rate_escalations_df['fuel_type'] == 'distallate fuel oil']
    # rename price column
    rename_map = {'dlrs_per_kwh': 'dlrs_per_kwh_fuel_oil'}
    # drop fuel_type column
    fuel_oil_prices_df.rename(columns=rename_map, inplace=True)
    # join to main dataframe
    join_keys = ['census_division_abbr', 'sector_abbr']
    dataframe = pd.merge(dataframe, fuel_oil_prices_df,
                         how='left', on=join_keys)

    out_cols = ['dlrs_per_kwh_natgas', 'dlrs_per_kwh_elec',
                'dlrs_per_kwh_propane', 'dlrs_per_kwh_fuel_oil']
    return_cols = in_cols + out_cols
    dataframe = dataframe[return_cols]

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def calc_state_incentives(dataframe, state_incentives_df):

    # TODO: need to actually write this code (issue #517)
    fill_vals = {'value_of_increment': 0,
                 'value_of_pbi_fit': 0,
                 'value_of_ptc': 0,
                 'pbi_fit_length': 0,
                 'ptc_length': 0,
                 'value_of_rebate': 0,
                 'value_of_tax_credit_or_deduction': 0}
    for col, val in fill_vals.iteritems():
        dataframe[col] = np.where(dataframe['modellable'] == True, val, np.nan)

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
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
    itc_tpo = pd.concat([itc_tpo_nonres, itc_tpo_res],
                        axis=0, ignore_index=True)
    # set the business model
    itc_tpo['business_model'] = 'tpo'

    # concatente the business models
    itc_all = pd.concat([itc_ho, itc_tpo], axis=0, ignore_index=True)

    row_count = df.shape[0]
    # merge to df
    df = pd.merge(df, itc_all, how='left', on=[
                  'sector_abbr', 'year', 'business_model', 'tech'])
    # drop the rows that are outside of the allowable system sizes
    df = df[(df['system_size_kw'] > df['min_size_kw']) & (
        df['system_size_kw'] <= df['max_size_kw']) | df['system_size_kw'].isnull()]
    # confirm shape hasn't changed
    if df.shape[0] <> row_count:
        raise ValueError('Row count of dataframe changed during merge')

    # Calculate the value of ITC (accounting for reduced costs from
    # state/local incentives)
    df['applicable_ic'] = df['ghp_installed_costs_dlrs'] - \
        (df['value_of_tax_credit_or_deduction'] +
         df['value_of_rebate'] + df['value_of_increment'])
    df['value_of_itc'] = np.where(df['modellable'] == True, df[
                                  'applicable_ic'] * df['itc_fraction'], np.nan)

    df = df.drop(['applicable_ic', 'itc_fraction'], axis=1)

    out_cols = ['value_of_itc']
    return_cols = in_cols + out_cols

    df = df[return_cols]

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def add_bin_id(dataframe):

    # add bin_id field -- only purpose is for compatiblity with tech choice
    # function
    dataframe['bin_id'] = dataframe.groupby(
        ['county_id'])['agent_id'].rank(ascending=True, method='first')

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_incentives_cap(dataframe, incentives_cap_df):

    join_keys = ['tech']
    dataframe = pd.merge(dataframe, incentives_cap_df,
                         how='left', on=join_keys)

    return dataframe


def apply_nan_to_unmodellable_agents(dataframe, lkup_table, join_keys):

    # NOTE: only works for simple, single-value columns. compound (i.e., list or array) columns do not work
    # figure out waht the newly added columns are
    new_cols = list(set(lkup_table.columns.tolist()) - set(join_keys))
    dataframe.loc[dataframe['modellable'] == False, new_cols] = np.nan

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_bass_params(dataframe, bass_params_df):

    dataframe = pd.merge(dataframe, bass_params_df, how='left', on=[
                         'state_abbr', 'sector_abbr', 'tech'])

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_state_starting_capacities_ghp(con, schema):

    inputs = locals().copy()

    sql = '''SELECT sector_abbr,
                    state_abbr,
                    capacity_tons,
                    'ghp'::text as tech
             FROM diffusion_geo.starting_capacities_2012_ghp;''' % inputs
    df = pd.read_sql(sql, con)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def estimate_initial_market_shares(dataframe, state_starting_capacities_df):

    # record input columns
    in_cols = list(dataframe.columns)

    # find the total market eligible ghp capacity in each state (by technology
    # and sector)
    dataframe['market_eligible_capacity_in_bin'] = dataframe[
        'market_eligible_buildings_in_bin'] * dataframe['ghp_system_size_tons']
    state_total_market_eligible_capacity = dataframe[['state_abbr', 'sector_abbr', 'tech', 'market_eligible_capacity_in_bin']].groupby(
        ['state_abbr', 'sector_abbr', 'tech']).sum().reset_index()
    state_total_agents = dataframe[['state_abbr', 'sector_abbr', 'tech', 'market_eligible_capacity_in_bin']].groupby(
        ['state_abbr', 'sector_abbr', 'tech']).count().reset_index()
    # rename the final columns
    state_total_market_eligible_capacity.columns = state_total_market_eligible_capacity.columns.str.replace(
        'market_eligible_capacity_in_bin', 'market_eligible_capacity_in_state')
    state_total_agents.columns = state_total_agents.columns.str.replace(
        'market_eligible_capacity_in_bin', 'agent_count_in_state')
    # merge together
    state_denominators = pd.merge(state_total_market_eligible_capacity, state_total_agents, how='left', on=[
                                  'state_abbr', 'sector_abbr', 'tech'])

    # merge back to the main dataframe
    dataframe = pd.merge(dataframe, state_denominators, how='left', on=[
                         'state_abbr', 'sector_abbr', 'tech'])

    # merge in the state starting capacities
    dataframe = pd.merge(dataframe, state_starting_capacities_df, how='left', on=[
                         'tech', 'state_abbr', 'sector_abbr'])

    # determine the portion of initial load and systems that should be allocated to each agent
    # (when there are no developable agnets in the state, simply apportion evenly to all agents)
    dataframe['portion_of_state'] = np.where(dataframe['market_eligible_capacity_in_state'] > 0,
                                             dataframe[
                                                 'market_eligible_capacity_in_bin'] / dataframe['market_eligible_capacity_in_state'],
                                             1. / dataframe['agent_count_in_state'])
    # apply the agent's portion to the total to calculate starting capacity
    # and systems
    dataframe['installed_capacity_last_year'] = np.round(
        dataframe['portion_of_state'] * dataframe['capacity_tons'], 6)
    dataframe['number_of_adopters_last_year'] = np.round(
        dataframe['installed_capacity_last_year'] / dataframe['ghp_system_size_tons'], 6)
    dataframe['market_share_last_year'] = np.where(dataframe['market_eligible_capacity_in_bin'] == 0,
                                                   0,
                                                   np.round(dataframe['installed_capacity_last_year'] / dataframe['market_eligible_capacity_in_bin'], 6))
    dataframe['market_value_last_year'] = dataframe[
        'ghp_installed_costs_dlrs'] * dataframe['number_of_adopters_last_year']

    # reproduce these columns as "initial" columns too
    dataframe['initial_number_of_adopters'] = dataframe[
        'number_of_adopters_last_year']
    dataframe['initial_capacity_tons'] = dataframe[
        'installed_capacity_last_year']
    dataframe['initial_market_share'] = dataframe['market_share_last_year']
    dataframe['initial_market_value'] = dataframe['market_value_last_year']

    # isolate the return columns
    return_cols = ['initial_number_of_adopters',
                   'initial_capacity_tons',
                   'initial_market_share',
                   'initial_market_value',
                   'number_of_adopters_last_year',
                   'installed_capacity_last_year',
                   'market_share_last_year',
                   'market_value_last_year']
    out_cols = in_cols + return_cols
    dataframe = dataframe[out_cols]

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_market_last_year(con, schema):

    inputs = locals().copy()

    sql = """SELECT agent_id,
                    tech,
                    market_share_last_year,
                    max_market_share_last_year,
                    number_of_adopters_last_year,
                    installed_capacity_last_year,
                    market_value_last_year,
                    initial_number_of_adopters,
                    initial_capacity_tons,
                    initial_market_share,
                    initial_market_value
            FROM %(schema)s.output_market_last_year_ghp;""" % inputs
    df = pd.read_sql(sql, con, coerce_float=False)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_market_last_year(dataframe, market_last_year_df):

    dataframe = pd.merge(dataframe, market_last_year_df,
                         how='left', on=['agent_id', 'tech'])
    # fill in values with zero for new construction
    new_cols = ['market_share_last_year',
                'max_market_share_last_year',
                'number_of_adopters_last_year',
                'installed_capacity_last_year',
                'market_value_last_year',
                'initial_number_of_adopters',
                'initial_capacity_tons',
                'initial_market_share',
                'initial_market_value']
    dataframe.loc[dataframe['new_construction'] == True, new_cols] = 0.

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def write_last_year(con, cur, market_last_year, schema):

    inputs = locals().copy()

    inputs['out_table'] = '%(schema)s.output_market_last_year_ghp' % inputs

    sql = """DELETE FROM %(out_table)s;"""  % inputs
    cur.execute(sql)
    con.commit()

    # open an in memory stringIO file (like an in memory csv)
    s = StringIO()
    # write the data to the stringIO
    out_cols = ['agent_id',
                'tech',
                'market_share_last_year',
                'max_market_share_last_year',
                'number_of_adopters_last_year',
                'installed_capacity_last_year',
                'market_value_last_year',
                'initial_number_of_adopters',
                'initial_capacity_tons',
                'initial_market_share',
                'initial_market_value'
                ]
    market_last_year[out_cols].to_csv(s, index=False, header=False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    cur.copy_expert('COPY %(out_table)s FROM STDOUT WITH CSV' % inputs, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()
    s.close()


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def create_new_id_column(dataframe, columns, id_col_name):

    unique_combos = dataframe.groupby(columns).size().reset_index()
    # drop the new columns
    new_column_i = len(unique_combos.columns.tolist()) - 1
    new_column = unique_combos.columns[new_column_i]
    unique_combos.drop(new_column, axis=1, inplace=True)
    unique_combos[id_col_name] = unique_combos.index

    dataframe = pd.merge(dataframe, unique_combos, how='left', on=columns)

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def mark_excluded_options(dataframe):

    dataframe['excluded_option'] = (dataframe['market_eligible'] == False) | (
        (dataframe['business_model'] == 'tpo') & (dataframe['leasing_allowed'] == False))

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def sanitize_decision_col(dataframe, decision_col, new_col):

    dataframe[new_col] = np.where((dataframe[decision_col] < 0) | (
        dataframe[decision_col].isnull()), 0, dataframe[decision_col])

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def write_agent_outputs_ghp(con, cur, schema, dataframe):

    inputs = locals().copy()

    # set fields to write
    fields = ['agent_id',
              'year',
              'pgid',
              'county_id',
              'state_abbr',
              'state_fips',
              'county_fips',
              'tract_fips',
              'tract_id_alias',
              'old_county_id',
              'census_division_abbr',
              'census_region',
              'reportable_domain',
              'pca_reg',
              'reeds_reg',
              'acres_per_bldg',
              'hdf_load_index',
              'iecc_temperature_zone',
              'iecc_climate_zone',
              'hazus_bldg_type',
              'buildings_in_bin',
              'site_space_heat_in_bin_kwh',
              'site_space_cool_in_bin_kwh',
              'site_water_heat_in_bin_kwh',
              'site_total_heat_in_bin_kwh',
              'site_space_heat_per_building_in_bin_kwh',
              'site_space_cool_per_building_in_bin_kwh',
              'site_water_heat_per_building_in_bin_kwh',
              'site_total_heat_per_building_in_bin_kwh',
              'demand_space_heat_in_bin_kwh',
              'demand_space_cool_in_bin_kwh',
              'demand_water_heat_in_bin_kwh',
              'demand_total_heat_in_bin_kwh',
              'demand_space_heat_per_building_in_bin_kwh',
              'demand_space_cool_per_building_in_bin_kwh',
              'demand_water_heat_per_building_in_bin_kwh',
              'demand_total_heat_per_building_in_bin_kwh',
              'space_heat_system_age',
              'space_cool_system_age',
              'average_system_age',
              'space_heat_system_expected_lifetime',
              'space_cool_system_expected_lifetime',
              'average_system_expected_lifetime',
              'eia_bldg_id',
              'eia_bldg_weight',
              'climate_zone',
              'pba',
              'pbaplus',
              'typehuq',
              'owner_occupied',
              'year_built',
              'single_family_res',
              'num_tenants',
              'num_floors',
              'space_heat_equip',
              'space_heat_fuel',
              'water_heat_equip',
              'water_heat_fuel',
              'space_cool_equip',
              'space_cool_fuel',
              'space_heat_efficiency',
              'space_cool_efficiency',
              'water_heat_efficiency',
              'totsqft',
              'totsqft_heat',
              'totsqft_cool',
              'crb_model',
              'gtc_btu_per_hftf',
              'sector_abbr',
              'sector',
              'tech',
              'new_construction',
              'years_to_replacement_heat',
              'years_to_replacement_cool',
              'years_to_replacement_average',
              'savings_pct_electricity_consumption',
              'savings_pct_natural_gas_consumption',
              'cooling_ton_per_sqft',
              'baseline_type',
              'modellable',
              'ghp_system_size_tons',
              'system_size_kw',
              'ghx_length_ft',
              'sys_config',
              'area_per_well_sqft_vertical',
              'max_well_depth_ft',
              'area_per_pipe_length_sqft_per_foot_horizontal',
              'n_installable_wells_vertical',
              'length_installable_ft',
              'viable_sys_config',
              'market_eligible',
              'market_eligible_buildings_in_bin',
              'bass_deployable',
              'bass_deployable_buildings_in_bin',
              'heat_exchanger_cost_dollars_per_ft',
              'ghp_cost_improvement_pct',
              'ghp_fixed_om_dollars_per_sf_per_year',
              'ghx_cost_dlrs',
              'ghp_heat_pump_cost_dlrs',
              'ghp_installed_costs_dlrs',
              'ghp_fixed_om_dlrs_per_year',
              'hvac_equipment_cost_improvement_pct',
              'baseline_fixed_om_dollars_per_sf_per_year',
              'baseline_equipment_costs_dlrs',
              'baseline_installed_costs_dlrs',
              'baseline_fixed_om_dlrs_per_year',
              'ghp_heat_pump_lifetime_yrs',
              'ghp_efficiency_improvement_pct',
              'baseline_system_lifetime_yrs',
              'baseline_efficiency_improvement_pct',
              'baseline_site_natgas_per_building_kwh',
              'baseline_site_elec_per_building_kwh',
              'baseline_site_propane_per_building_kwh',
              'baseline_site_fuel_oil_per_building_kwh',
              'baseline_site_dist_hot_water_per_building_kwh',
              'baseline_site_dist_steam_per_building_kwh',
              'baseline_site_dist_chil_water_per_building_kwh',
              'ghp_site_natgas_per_building_kwh',
              'ghp_site_elec_per_building_kwh',
              'ghp_site_propane_per_building_kwh',
              'ghp_site_fuel_oil_per_building_kwh',
              'ghp_site_dist_hot_water_per_building_kwh',
              'ghp_site_dist_steam_per_building_kwh',
              'ghp_site_dist_chil_water_per_building_kwh',
              'leasing_allowed',
              'business_model',
              'metric',
              'loan_term_yrs',
              'loan_rate',
              'down_payment',
              'discount_rate',
              'tax_rate',
              'length_of_irr_analysis_yrs',
              'value_of_pbi_fit',
              'value_of_tax_credit_or_deduction',
              'pbi_fit_length',
              'value_of_increment',
              'value_of_rebate',
              'ptc_length',
              'value_of_ptc',
              'value_of_itc',
              'max_incentive_fraction',
              'ghp_total_value_of_incentives',
              'ghp_avg_annual_energy_costs_dlrs',
              'baseline_avg_annual_energy_costs_dlrs',
              'avg_annual_net_cashflow_tpo',
              'monthly_bill_savings',
              'percent_monthly_bill_savings',
              'payback_period',
              'ttd',
              'metric_value_precise',
              'npv4',
              'npv_agent',
              'npv4_per_ton',
              'npv_agent_per_ton',
              'inflation_rate',
              'lcoe',
              'metric_value_bounded',
              'metric_value_as_factor',
              'metric_value',
              'max_market_share',
              'initial_number_of_adopters',
              'initial_capacity_tons',
              'initial_market_share',
              'initial_market_value',
              'number_of_adopters_last_year',
              'installed_capacity_last_year',
              'market_share_last_year',
              'market_value_last_year',
              'p',
              'q',
              'teq_yr1',
              'mms_fix_zeros',
              'bass_ratio',
              'teq',
              'teq2',
              'f',
              'new_adopt_fraction',
              'bass_market_share',
              'diffusion_market_share',
              'market_share',
              'new_market_share',
              'new_adopters',
              'new_capacity',
              'new_market_value',
              'number_of_adopters',
              'installed_capacity',
              'market_value'
              ]

    # convert formatting of fields list
    inputs['fields_str'] = utilfunc.pylist_2_pglist(fields).replace("'", "")
    # open an in memory stringIO file (like an in memory csv)
    s = StringIO()
    # write the data to the stringIO
    dataframe.loc[:, fields].to_csv(s, index=False, header=False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    sql = 'COPY %(schema)s.agent_outputs_ghp (%(fields_str)s) FROM STDOUT WITH CSV' % inputs
    cur.copy_expert(sql, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()
    s.close()
