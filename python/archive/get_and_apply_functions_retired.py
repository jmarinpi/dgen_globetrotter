# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 19:24:58 2017

@author: pgagnon
"""

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_technology_costs_solar(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT 'solar'::VARCHAR(5) as tech,
                    a.sector_abbr,
                    a.installed_costs_dollars_per_kw,
                    a.fixed_om_dollars_per_kw_per_yr,
                    a.variable_om_dollars_per_kwh,
                    a.inverter_cost_dollars_per_kw,
                    b.size_adjustment_factor as pv_size_adjustment_factor,
                    b.base_size_kw as pv_base_size_kw,
                    b.new_construction_multiplier as pv_new_construction_multiplier
            FROM %(schema)s.input_solar_cost_projections_to_model a
            LEFT JOIN %(schema)s.input_solar_cost_multipliers b
                ON a.sector_abbr = b.sector_abbr
            WHERE a.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float=False)

    return df
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_storage_costs(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT a.year,
            a.sector_abbr,
            a.scenario,
            a.batt_kwh_cost,
            a.batt_kw_cost
            FROM %(schema)s.input_storage_cost_projections_to_model a
            WHERE a.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)

    return df
    
    


    
#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_system_degradation(con, schema):
    '''Return the annual system degradation rate as float.
        '''

    inputs = locals().copy()

    sql = '''SELECT tech, ann_system_degradation
             FROM %(schema)s.input_performance_annual_system_degradation;''' % inputs
    system_degradation_df = pd.read_sql(sql, con)

    return system_degradation_df
    
    
#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_market_last_year(con, schema):

    inputs = locals().copy()

    sql = """SELECT *
            FROM %(schema)s.output_market_last_year;""" % inputs
    df = pd.read_sql(sql, con, coerce_float=False)

    return df
    
    
#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_technology_performance_solar(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT 'solar'::VARCHAR(5) as tech,
                    efficiency_improvement_factor as pv_efficiency_improvement_factor,
                    density_w_per_sqft as pv_density_w_per_sqft,
                    inverter_lifetime_yrs
             FROM %(schema)s.input_solar_performance_improvements
             WHERE year = %(year)s;""" % inputs

    df = pd.read_sql(sql, con, coerce_float=False)

    return df