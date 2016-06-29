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
def generate_resource_data(cur, con, schema, seed):
    
    setup_resource_data_egs(cur, con, schema, seed)
    setup_resource_data_hydrothermal(cur, con, schema, seed)
    combine_resource_data()
    
    return
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def setup_resource_data_egs(cur, con, schema, seed):
    
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
    
    return


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
             FROM diffusion_geo.hydrothermal_resource_data_dummy a -- TODO: replace with actual resource data from meghan;""" % inputs
    cur.execute(sql)
    con.commit()
    # TODO: add some mechanism for only compiling data for tracts in states to model
    # TODO: set this up to use p_run?
    
    return

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def combine_resource_data():
    
    #TODO: write this function
    pass

    return


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def get_resource_data(con, schema, year):
    
    inputs = locals().copy()
        
    sql = """SELECT *
             FROM diffusion_geo.resource_data_dummy;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df
    
#%%