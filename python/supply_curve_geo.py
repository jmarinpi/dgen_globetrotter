# -*- coding: utf-8 -*-
"""
Created on Thu May 26 11:29:02 2016

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
def setup_resource_data(cur, con, schema, seed):
    
    setup_resource_data_egs_hdr(cur, con, schema, seed)
    setup_resource_data_hydrothermal(cur, con, schema, seed)
    
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def setup_resource_data_egs_hdr(cur, con, schema, seed):
    
    inputs = locals().copy()
    
    sql = """DROP TABLE IF EXISTS %(schema)s.resources_egs_hdr;
             CREATE UNLOGGED TABLE %(schema)s.resources_egs_hdr AS
             WITH a AS
            (
                	SELECT unnest(array[1,2]) as tract_id_alias, -- todo: this should come from the lkup table
                         a.gid, 
                         a.area_sqkm, -- todo: replace with correct area_sqkm from lkup table
                         b.depth_km, 
                         b.thickness_km,
                		diffusion_shared.r_rnorm_rlnorm(b.t_deg_c_mean, 
                                                        b.t_deg_c_sd, 
                                                        'normal'::TEXT, 
                                                        1 * %(seed)s) as t_deg_c_est -- todo: replace 1 with tract_id_alias * seed
                	FROM dgeo.smu_t35km_2016 a -- todo: change this to the intersected lkup table from Meghan
                	LEFT JOIN diffusion_geo.egs_hdr_temperature_at_depth b
                	ON a.gid = b.gid
                  WHERE a.gid = 1 -- todo: remove this -- but, we should have some sort of other filter that defines the tracts to consider
            ),
            b as
            (
                	SELECT tract_id_alias, gid, area_sqkm,
                		depth_km, thickness_km,
                		case when t_deg_c_est > 150 or t_deg_c_est < 30 then 0 -- bound temps between 30 and 150
                		     else t_deg_c_est
                		end as res_temp_deg_c,
                		area_sqkm * thickness_km as volume_km3
                	FROM a
            ),
            c as
            (
                	SELECT c.year,
                         b.tract_id_alias,
                         b.gid,
                         b.depth_km,
                         ROUND(b.area_sqkm/c.area_per_wellset_sqkm,0)::INTEGER as n_wells_in_tract,
                	 	diffusion_geo.extractable_resource_joules_recovery_factor(b.volume_km3, 
            									    b.res_temp_deg_c, 
            									    c.resource_recovery_factor)/3.6e+9 as extractable_resource_mwh
                 FROM b
                 CROSS JOIN %(schema)s.input_du_egs_reservoir_factors c
            )
            SELECT c.year,
                	c.tract_id_alias,
                 c.gid as resource_id,
                 'egs'::TEXT as resource_type,
                 'hdr'::TEXT as system_type,
                 c.depth_km * 1000 as depth_m,
                 c.n_wells_in_tract,
                	CASE WHEN extractable_resource_mwh < 0 THEN 0 -- prevent negative values
                	ELSE c.extractable_resource_mwh/c.n_wells_in_tract
                	END as extractable_resource_per_well_in_tract_mwh
            FROM c;""" % inputs
    cur.execute(sql)
    con.commit()
    # TODO: add some mechanism for only compiling data for tracts in states to model
    # TODO: set this up to use p_run?
    


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def setup_resource_data_hydrothermal(cur, con, schema, seed):
    
    inputs = locals().copy()

    sql = """DROP TABLE IF EXISTS %(schema)s.resources_hydrothermal;
             CREATE UNLOGGED TABLE %(schema)s.resources_hydrothermal AS
             SELECT a.tract_id_alias,
                    a.resource_id,
                    a.resource_type,
                    a.system_type,
                	  round(
                		diffusion_shared.r_runif(a.min_depth_m, 
                				  a.max_depth_m, 
                				 1, 
                				 %(seed)s * a.tract_id_alias),
                		0)::INTEGER as depth_m,
                   n_wells_in_tract,
                   extractable_resource_per_well_in_tract_mwh
             FROM diffusion_geo.hydrothermal_resource_data_dummy a -- TODO: replace with actual resource data from meghan -- may need to merge pts and polys;""" % inputs
    cur.execute(sql)
    con.commit()
    # TODO: add some mechanism for only compiling data for tracts in states to model
    # TODO: set this up to use p_run?


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_resource_data(con, schema, year):
    
    inputs = locals().copy()
        
    sql = """SELECT a.tract_id_alias,
                    a.resource_id,
                    a.resource_type,
                    a.system_type,
                    a.depth_m,
                    a.n_wells_in_tract,
                    a.extractable_resource_per_well_in_tract_mwh as resource_per_well_mwh
             FROM %(schema)s.resources_hydrothermal a
             
             UNION ALL
             
             SELECT b.tract_id_alias,
                    b.resource_id::TEXT as resource_id,
                    b.resource_type,
                    b.system_type,
                    b.depth_m,
                    b.n_wells_in_tract,
                    b.extractable_resource_per_well_in_tract_mwh as resource_per_well_mwh
             FROM %(schema)s.resources_egs_hdr b
             WHERE b.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_plant_cost_data(con, schema, year):
    
    inputs = locals().copy()
    sql = """SELECT a.year, 
                    a.wells_per_wellset,
                	  b.peaking_boilers_pct_of_peak_demand,
                	  b.max_acceptable_drawdown_pct_of_initial_capacity,
                	  b.plant_lifetime_yrs,
                	  b.hydro_expected_drawdown_pct_per_yr,
                	  b.egs_expected_drawdown_pct_per_yr,
                	  b.res_enduse_efficiency_factor,
                	  b.com_enduse_efficiency_factor,
                	  b.ind_enduse_efficiency_factor,
                	  c.plant_installation_costs_dollars_per_kw, 
                	  c.fixed_om_costs_pct_cap_costs, 
                	  c.distribution_network_construction_costs_dollars_per_m, 
                	  c.operating_costs_reservoir_pumping_costs_dollars_per_gal, 
                	  c.operating_costs_pumping_costs_dollars_per_gal_mile, 
                	  c.natural_gas_peaking_boilers_dollars_per_kw, 
                	  c.construction_period_yrs, 
                	  d.future_drilling_cost_improvements_pct_current_costs, 
                	  d.reservoir_stimulation_costs_dollars_per_well_set, 
                	  d.exploration_and_discovery_costs_pct_cap_costs
             FROM %(schema)s.input_du_egs_reservoir_factors a
             LEFT JOIN %(schema)s.input_du_performance_projections b
                 ON a.year = b.year
             LEFT JOIN %(schema)s.input_du_cost_plant_surface c
                 ON a.year = c.year
             LEFT JOIN %(schema)s.input_du_cost_plant_subsurface d
                 ON a.year = d.year
             WHERE a.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_plant_cost_data(resource_df, costs_df):
    
    inputs = locals().copy()

    
    return 