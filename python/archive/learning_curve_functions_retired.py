# -*- coding: utf-8 -*-
"""
Created on Fri Mar 17 12:47:25 2017

@author: pgagnon
"""


def write_costs(con, cur, schema, learning_curves_mode, year, end_year):

    inputs = locals().copy()
    inputs['prev_year'] = year - 2
    inputs['next_year'] = year + 2

    # do not run if in the final model year
    if year == end_year:
        return

    for i, row in learning_curves_mode.iterrows():
        lc_enabled = row['enabled']
        tech = row['tech']
        if lc_enabled == True:
            if tech == 'solar':
                sql = """INSERT INTO %(schema)s.yearly_technology_costs_solar
                        WITH a AS
                        (
                            SELECT a.year, a.sector_abbr,
                                	((c.cumulative_installed_capacity/d.cumulative_installed_capacity)/b.frac_of_global_mkt)^(ln(1-b.learning_rate)/ln(2)) as cost_scalar,
                                a.inverter_cost_dollars_per_kw,
                                a.installed_costs_dollars_per_kw,
                                e.fixed_om_dollars_per_kw_per_yr,
                                e.variable_om_dollars_per_kwh
                            FROM %(schema)s.yearly_technology_costs_solar a
                            LEFT JOIN %(schema)s.input_solar_cost_learning_rates b
                                ON b.year = %(year)s
                            LEFT JOIN %(schema)s.cumulative_installed_capacity_solar c
                                ON c.year = %(year)s
                            LEFT JOIN %(schema)s.cumulative_installed_capacity_solar d
                                ON d.year = %(prev_year)s
                            LEFT JOIN %(schema)s.input_solar_cost_projections_to_model e
                                ON e.year = %(next_year)s
                                AND e.sector = a.sector_abbr
                            WHERE a.year = %(year)s
                        )
                        SELECT year + 2 as year, sector_abbr,
                            installed_costs_dollars_per_kw * cost_scalar as installed_costs_dollars_per_kw,
                            inverter_cost_dollars_per_kw * cost_scalar as inverter_cost_dollars_per_kw,
                            fixed_om_dollars_per_kw_per_yr,
                            variable_om_dollars_per_kwh
                        FROM a""" % inputs
            elif tech == 'wind':
                sql = """INSERT INTO %(schema)s.yearly_technology_costs_wind
                        WITH a AS
                        (
                            SELECT a.year, a.turbine_size_kw, a.turbine_height_m,
                                	((c.cumulative_installed_capacity/d.cumulative_installed_capacity)/f.frac_of_global_mkt)^(ln(1-b.learning_rate)/ln(2)) as cost_scalar,
                                a.installed_costs_dollars_per_kw,
                                e.fixed_om_dollars_per_kw_per_yr,
                                e.variable_om_dollars_per_kwh
                            FROM %(schema)s.yearly_technology_costs_wind a
                            LEFT JOIN %(schema)s.input_wind_cost_learning_rates b
                                ON b.year = %(year)s
                                AND a.turbine_size_kw = b.turbine_size_kw
                            LEFT JOIN %(schema)s.input_wind_cost_global_fraction f
                                ON f.year = %(year)s
                            LEFT JOIN %(schema)s.cumulative_installed_capacity_wind c
                                ON c.year = %(year)s
                                AND a.turbine_size_kw = c.turbine_size_kw
                            LEFT JOIN %(schema)s.cumulative_installed_capacity_wind d
                                ON d.year = %(prev_year)s
                                AND a.turbine_size_kw = d.turbine_size_kw
                            LEFT JOIN %(schema)s.turbine_costs_per_size_and_year e
                                ON e.year = %(next_year)s
                                AND a.turbine_size_kw = e.turbine_size_kw
                                AND a.turbine_height_m = e.turbine_height_m
                            WHERE a.year = %(year)s
                        )
                        SELECT year + 2 as year, turbine_size_kw, turbine_height_m,
                            installed_costs_dollars_per_kw * cost_scalar as installed_costs_dollars_per_kw,
                            fixed_om_dollars_per_kw_per_yr,
                            variable_om_dollars_per_kwh
                        FROM a""" % inputs
        else:
            if tech == 'solar':
                sql = """INSERT INTO %(schema)s.yearly_technology_costs_solar
                         SELECT a.year, a.sector as sector_abbr,
                                a.installed_costs_dollars_per_kw,
                                a.inverter_cost_dollars_per_kw,
                                a.fixed_om_dollars_per_kw_per_yr,
                                a.variable_om_dollars_per_kwh
                            FROM %(schema)s.input_solar_cost_projections_to_model a
                            WHERE a.year = %(next_year)s
                        """ % inputs
            elif tech == 'wind':
                sql = """INSERT INTO %(schema)s.yearly_technology_costs_wind
                         SELECT a.year,
                                a.turbine_size_kw,
                                a.turbine_height_m,
                                a.installed_costs_dollars_per_kw,
                                a.fixed_om_dollars_per_kw_per_yr,
                                a.variable_om_dollars_per_kwh
                            FROM %(schema)s.turbine_costs_per_size_and_year a
                            WHERE a.year = %(next_year)s
                        """ % inputs
        cur.execute(sql)
        con.commit()
        
        
#%%
def get_learning_curves_mode(con, schema):

    inputs = locals().copy()

    sql = """SELECT 'wind'::TEXT as tech, enabled
            FROM %(schema)s.input_cost_learning_curves_enabled_wind

            UNION ALL

            SELECT 'solar'::TEXT as tech, enabled
            FROM %(schema)s.input_cost_learning_curves_enabled_solar""" % inputs

    learning_curves_mode = pd.read_sql(sql, con)

    return learning_curves_mode