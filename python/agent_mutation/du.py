# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 11:35:14 2016

@author: mgleason
"""

import psycopg2 as pg
import pandas as pd
import decorators
import utility_functions as utilfunc


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
def get_regional_energy_prices(con, schema, year):

    inputs = locals().copy()

    # need to create data for fuel type = none for apply_regional_energy_prices()
    # also need to create data for wood (assume 28 MMBTU per cord of wood and
    # approx $200/cord)
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

    df = pd.read_sql(sql, con, coerce_float=False)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_regional_energy_prices(dataframe, energy_prices_df):

    in_cols = list(dataframe.columns)

    # duplicate the energy_prices_df for each of space heating, space cooling,
    # and water heating, renaming the price field accordingly
    space_heating_prices_df = energy_prices_df.copy()
    rename_map = {'dlrs_per_kwh': 'space_heat_dlrs_per_kwh',
                  'fuel_type': 'space_heat_fuel'}
    space_heating_prices_df.rename(columns=rename_map, inplace=True)

    water_heating_prices_df = energy_prices_df.copy()
    rename_map = {'dlrs_per_kwh': 'water_heat_dlrs_per_kwh',
                  'fuel_type': 'water_heat_fuel'}
    water_heating_prices_df.rename(columns=rename_map, inplace=True)

    space_cooling_prices_df = energy_prices_df.copy()
    rename_map = {'dlrs_per_kwh': 'space_cool_dlrs_per_kwh',
                  'fuel_type': 'space_cool_fuel'}
    space_cooling_prices_df.rename(columns=rename_map, inplace=True)

    # join dataframes together
    dataframe = pd.merge(dataframe, space_heating_prices_df, how='left', on=[
                         'census_division_abbr', 'sector_abbr', 'space_heat_fuel'])
    dataframe = pd.merge(dataframe, water_heating_prices_df, how='left', on=[
                         'census_division_abbr', 'sector_abbr', 'water_heat_fuel'])
    dataframe = pd.merge(dataframe, space_cooling_prices_df, how='left', on=[
                         'census_division_abbr', 'sector_abbr', 'space_cool_fuel'])

    # check for nulls; if found, raise error
    nulls_exist = dataframe[['space_heat_dlrs_per_kwh',
                             'water_heat_dlrs_per_kwh', 'space_cool_dlrs_per_kwh']].isnull().any().any()
    if nulls_exist == True:
        raise ValueError(
            "null values exist in space_heat_dlrs_per_kwh, water_heat_dlrs_per_kwh, or space_cool_dlrs_per_kwh")

    out_cols = ['space_heat_dlrs_per_kwh',
                'water_heat_dlrs_per_kwh', 'space_cool_dlrs_per_kwh']
    return_cols = in_cols + out_cols
    dataframe = dataframe[return_cols]

    return dataframe


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_end_user_costs_du(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT *
            FROM %(schema)s.input_du_cost_user
            WHERE year = %(year)s;""" % inputs

    df = pd.read_sql(sql, con, coerce_float=False)

    return df


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_end_user_costs_du(dataframe, end_user_costs_du_df):

    # join dataframes together
    dataframe = pd.merge(dataframe, end_user_costs_du_df,
                         how='left', on=['sector_abbr', 'year'])

    return dataframe
