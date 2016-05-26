# -*- coding: utf-8 -*-
"""
Created on Thu May 26 11:29:02 2016

@author: mgleason
"""
import psycopg2 as pg
import numpy as np
import pandas as pd
import logging
reload(logging)
import decorators
from config import show_times
import utility_functions as utilfunc
from multiprocessing import Process, JoinableQueue

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
def p_execute(pg_conn_string, sql):
    try:
        # create cursor and connection
        con, cur = utilfunc.make_con(pg_conn_string)  
        # execute query
        cur.execute(sql)
        # commit changes
        con.commit()
        # close cursor and connection
        con.close()
        cur.close()
    except Exception, e:
        print 'Error: %s' % e


#%%
def p_run(pg_conn_string, sql, county_chunks, npar):
    
    jobs = []
    for i in range(npar):
        place_holders = {'i': i, 'county_ids': utilfunc.pylist_2_pglist(county_chunks[i])}
        isql = sql % place_holders
        proc = Process(target = p_execute, args = (pg_conn_string, isql))
        jobs.append(proc)
        proc.start()
    for job in jobs:
        job.join()   


#%%
def split_counties(cur, schema, npar):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()      
    
    # get list of counties
    sql =   """SELECT a.county_id 
               FROM diffusion_blocks.county_geoms a
               INNER JOIN %(schema)s.states_to_model b
                   ON a.state_abbr = b.state_abbr
               ORDER BY a.county_id;""" % inputs
    cur.execute(sql)
    counties = [row['county_id'] for row in cur.fetchall()]
    
    if len(counties) > npar:
        county_chunks = map(list,np.array_split(counties, npar))
    else:
        county_chunks = [counties]
        npar = 1
    
    return county_chunks, npar
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 1, prefix = '')
def generate_core_agent_characteristics(cur, con, techs, schema, n_bins, sectors,
                                            npar, pg_conn_string, scenario_opts):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'

    seed = scenario_opts['random_generator_seed']

    # break counties into subsets for parallel processing
    county_chunks, npar = split_counties(cur, schema, npar)        
    
    # all in postgres
    for sector_abbr, sector in sectors.iteritems():
        with utilfunc.Timer() as t:
            logger.info("Creating Agents for %s Sector" % sector)
                
            #==============================================================================
            #     sample from blocks and building microdata, convolve samples, and estimate 
            #     max demand for each agent
            #==============================================================================
            # NOTE: each of these functions is dependent on the last, so changes from one must be cascaded to the others
            sample_blocks(schema, sector_abbr, county_chunks, n_bins, seed, npar, pg_conn_string)
            sample_building_microdata(schema, sector_abbr, county_chunks, n_bins, seed, npar, pg_conn_string)
            convolve_block_and_building_samples(schema, sector_abbr, county_chunks, n_bins, seed, npar, pg_conn_string)
            calculate_max_demand(schema, sector_abbr, county_chunks, n_bins, seed, npar, pg_conn_string)

            #==============================================================================
            #     impose agent level siting  attributes (i.e., "tech potential")
            #==============================================================================
            # SOLAR
            simulate_roof_characteristics(county_chunks, npar, pg_conn_string, con, schema, sector_abbr, seed)
            
            # WIND
            determine_allowable_turbine_heights(county_chunks, npar, pg_conn_string, schema, sector_abbr)
            find_potential_turbine_sizes(county_chunks, npar, pg_conn_string, schema, sector_abbr)




#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def sample_blocks(schema, sector_abbr, county_chunks, n_bins, seed, npar, pg_conn_string):

    msg = '\tSampling from Blocks for Each County'
    logger.info(msg)
    
    inputs_dict = locals().copy()
    inputs_dict['i_place_holder'] = '%(i)s'
    inputs_dict['chunk_place_holder'] = '%(county_ids)s'
        
    #==============================================================================
    #     randomly sample  N blocks from each county 
    #==============================================================================    
    # (note: [this may not be true any longer...] some counties will have fewer than N points, in which case, all are returned) 
    sql = """DROP TABLE IF EXISTS %(schema)s.block_%(sector_abbr)s_sample_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.block_%(sector_abbr)s_sample_%(i_place_holder)s AS
            WITH b as 
            (
                SELECT unnest(diffusion_shared.sample(array_agg(a.pgid ORDER BY a.pgid), %(n_bins)s ,%(seed)s, True, array_agg(a.sample_weight ORDER BY a.pgid))) as pgid
                FROM %(schema)s.block_microdata_%(sector_abbr)s_joined a
                WHERE a.county_id IN (%(chunk_place_holder)s)
                GROUP BY a.county_id
            )
                
            SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a.county_id ORDER BY a.county_id, a.pgid) as bin_id
            FROM %(schema)s.block_microdata_%(sector_abbr)s_joined a
            INNER JOIN b
            ON a.pgid = b.pgid
            WHERE a.county_id IN (%(chunk_place_holder)s);""" % inputs_dict

    p_run(pg_conn_string, sql, county_chunks, npar)


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def sample_building_microdata(schema, sector_abbr, county_chunks, n_bins, seed, npar, pg_conn_string):

    msg = "\tSampling from Load Microdata"
    logger.info(msg)
    
    
    inputs_dict = locals().copy()    
    inputs_dict['i_place_holder'] = '%(i)s'
    inputs_dict['chunk_place_holder'] = '%(county_ids)s'
    inputs_dict['load_where'] = " AND '%s' = b.sector_abbr" % sector_abbr
    if sector_abbr == 'res':
        inputs_dict['load_region'] = 'reportable_domain'
    else:
        inputs_dict['load_region'] = 'census_division_abbr'


    #==============================================================================
    #    create lookup table with random values for each load bin 
    #==============================================================================
    sql =  """DROP TABLE IF EXISTS %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s;
         CREATE UNLOGGED TABLE %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s AS
         WITH all_bins AS
         (
             SELECT a.county_id, 
                     b.load_id, b.weight, b.ann_cons_kwh, b.crb_model, b.roof_style, b.roof_sqft, b.ownocc8
             FROM diffusion_blocks.county_geoms a
             INNER JOIN %(schema)s.states_to_model c
                   ON a.state_abbr = c.state_abbr
             LEFT JOIN diffusion_shared.cbecs_recs_combined b
                 ON a.%(load_region)s = b.%(load_region)s
             WHERE a.county_id in  (%(chunk_place_holder)s)
                   %(load_where)s
        ),
        sampled_bins AS 
        (
            SELECT a.county_id, 
                    unnest(diffusion_shared.sample(array_agg(a.load_id ORDER BY a.load_id), %(n_bins)s, %(seed)s * a.county_id, True, array_agg(a.weight ORDER BY a.load_id))) as load_id
            FROM all_bins a
            GROUP BY a.county_id
        ), 
        numbered_samples AS
        (
            SELECT a.county_id, a.load_id,
                   ROW_NUMBER() OVER (PARTITION BY a.county_id ORDER BY a.county_id, a.load_id) as bin_id 
            FROM sampled_bins a
        )
        SELECT  a.county_id, a.bin_id,
                    b.load_id, b.weight, b.ann_cons_kwh, b.crb_model, b.roof_style, b.roof_sqft, b.ownocc8
        FROM numbered_samples a
        LEFT JOIN diffusion_shared.cbecs_recs_combined b
            ON a.load_id = b.load_id
        %(load_where)s ;""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    # add an index on county id and row_number
    sql = """CREATE INDEX county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s_join_fields_btree 
            ON %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s USING BTREE(county_id, bin_id);
            
            CREATE INDEX county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s_crb_model_btree 
            ON %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s USING BTREE(crb_model);""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def convolve_block_and_building_samples(schema, sector_abbr, county_chunks, n_bins, seed, npar, pg_conn_string):

    msg = '\tConvolving Block and Building Samples'    
    logger.info(msg)
    
    
    inputs_dict = locals().copy()    
    inputs_dict['i_place_holder'] = '%(i)s'
    inputs_dict['chunk_place_holder'] = '%(county_ids)s'      
    # for commercial customers, due to multi-tenant occupancy, use buildings rather than customers as the unit for agents
    if sector_abbr == 'com':
        inputs_dict['county_customer_count'] = 'county_bldg_count_2012'
    else:
        inputs_dict['county_customer_count'] = 'county_total_customers_2011'
        
   
    #==============================================================================
    #     link each block sample to a building sample
    #==============================================================================
    sql =  """DROP TABLE IF EXISTS %(schema)s.block_%(sector_abbr)s_sample_load_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.block_%(sector_abbr)s_sample_load_%(i_place_holder)s AS
            WITH binned as
            (
                SELECT a.*, b.crb_model, b.ann_cons_kwh, b.weight as eia_weight, 
                       CASE WHEN b.roof_sqft < 5000 THEN 'small'::character varying(6)
                            WHEN b.roof_sqft >= 5000 and b.roof_sqft < 25000 THEN 'medium'::character varying(6)
                            WHEN b.roof_sqft >= 25000 THEN 'large'::character varying(6)
                        END as bldg_size_class,
                        b.roof_sqft, b.roof_style, b.ownocc8,
                    	a.%(county_customer_count)s * b.weight/sum(b.weight) OVER (PARTITION BY a.county_id) as customers_in_bin, 
                    	a.county_total_load_mwh_2011 * 1000 * (b.ann_cons_kwh*b.weight)/sum(b.ann_cons_kwh*b.weight) OVER (PARTITION BY a.county_id) as load_kwh_in_bin
                FROM %(schema)s.block_%(sector_abbr)s_sample_%(i_place_holder)s a
                LEFT JOIN %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s b
                    ON a.county_id = b.county_id
                    AND a.bin_id = b.bin_id
                WHERE county_total_load_mwh_2011 > 0
            )
            SELECT a.*,
            	CASE  WHEN a.customers_in_bin > 0 THEN ROUND(a.load_kwh_in_bin/a.customers_in_bin, 0)::BIGINT
                	ELSE 0::BIGINT
                  END AS load_kwh_per_customer_in_bin
            FROM binned a;""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)

    # add indices
    sql = """CREATE INDEX block_%(sector_abbr)s_sample_load_%(i_place_holder)s_join_fields_btree 
            ON %(schema)s.block_%(sector_abbr)s_sample_load_%(i_place_holder)s 
            USING BTREE(hdf_load_index, crb_model);
            """ % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def calculate_max_demand(schema, sector_abbr, county_chunks, n_bins, seed, npar, pg_conn_string):

    msg = '\tCalculating Maximum Electricity Demand for Each Agent'    
    logger.info(msg)
    
    
    inputs_dict = locals().copy()    
    inputs_dict['i_place_holder'] = '%(i)s'
    inputs_dict['chunk_place_holder'] = '%(county_ids)s'
    # lookup table for finding the normalized max demand
    inputs_dict['load_demand_lkup'] = 'diffusion_load_profiles.energy_plus_max_normalized_demand'
          
    #==============================================================================
    #     find the max demand for each agent based on the applicable energy plus building model
    #==============================================================================
    sql = """DROP TABLE IF EXISTS %(schema)s.block_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.block_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s AS
            SELECT a.*, ROUND(b.normalized_max_demand_kw_per_kw * a.load_kwh_per_customer_in_bin, 0)::INTEGER AS max_demand_kw
            FROM %(schema)s.block_%(sector_abbr)s_sample_load_%(i_place_holder)s a
            LEFT JOIN %(load_demand_lkup)s b
                ON a.crb_model = b.crb_model
                AND a.hdf_load_index = b.hdf_index;""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
           

    # add indices
    sql = """CREATE INDEX block_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s_pkey_btree 
            ON %(schema)s.block_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s 
            USING BTREE(county_id, bin_id);
            
            CREATE INDEX block_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s_max_demand_kw_btree 
            ON %(schema)s.block_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s 
            USING BTREE(max_demand_kw);
            
            CREATE INDEX block_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s_state_abbr_btree 
            ON %(schema)s.block_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s 
            USING BTREE(state_abbr);
            """ % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def simulate_roof_characteristics(inputs_dict, county_chunks, npar, pg_conn_string, con, schema, sector_abbr, seed):
     
    msg = "Simulating rooftop characteristics for each agent"
    logger.info(msg)
    
    
    inputs = locals().copy()
    if inputs['sector_abbr'] == 'res':
        inputs['zone'] = 'residential'
    else:
        inputs['zone'] = 'com_ind'
    # get the rooftop source
    sql = """SELECT * 
             FROM %(schema)s.input_solar_rooftop_source;""" % inputs
    rooftop_source_df = pd.read_sql(sql, con)
    rooftop_source = rooftop_source_df['rooftop_source'].iloc[0]
    inputs['rooftop_source'] = rooftop_source

        
    # find the most appropriate city to sample from for each agent
    sql = """DROP TABLE IF EXISTS %(schema)s.block_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.block_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s AS
            with a as
            (
                	SELECT a.*, b.city_id, b.rank as city_rank
                	FROM %(schema)s.block_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s a
                	LEFT JOIN diffusion_solar.rooftop_city_ranks_by_county_and_ulocale_%(sector_abbr)s b
                		ON a.county_id = b.county_id
                		and a.ulocale = b.ulocale
                	INNER JOIN diffusion_solar.rooftop_city_ulocale_zone_size_class_lkup c
                		ON a.ulocale = c.ulocale
                		AND c.zone = '%(zone)s' 
                		AND a.bldg_size_class = c.size_class
                		AND b.city_id = c.city_id
            ), 
            b as
            (
                	SELECT  a.*, row_number() OVER (PARTITION BY county_id, bin_id ORDER BY city_rank asc) as rank
                	FROM a
            )
            SELECT *
            FROM b
            WHERE rank = 1;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    # add indices on join keys
    sql =  """CREATE INDEX block_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s_join_keys_btree 
              ON %(schema)s.block_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s 
              USING BTREE(city_id, bldg_size_class, ulocale);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    # sample from the lidar bins for that city
    sql = """DROP TABLE IF EXISTS %(schema)s.block_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.block_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s AS
            WITH b as
            (
            	SELECT a.county_id, a.bin_id,
            		unnest(diffusion_shared.sample(array_agg(b.pid ORDER BY b.pid), 1, 
            		%(seed)s * a.bin_id * a.county_id, FALSE, 
                    array_agg(b.count ORDER BY b.pid))) as pid
            	FROM %(schema)s.block_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s a
            	LEFT JOIN diffusion_solar.rooftop_orientation_frequencies_%(rooftop_source)s b
            		ON a.city_id = b.city_id
            		AND  b.zone = '%(zone)s'
            		AND a.ulocale = b.ulocale
            		AND a.bldg_size_class = b.size_class
            	GROUP BY a.county_id, a.bin_id
            )
            SELECT a.*, c.tilt, c.azimuth, e.pct_developable,
                  	c.slopearea_m2_bin * 10.7639 * d.gcr as available_roof_sqft,
                    d.gcr as ground_cover_ratio                 
            FROM %(schema)s.block_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s a
            INNER JOIN b
            	ON a.county_id = b.county_id
            	and a.bin_id = b.bin_id
            INNER JOIN diffusion_solar.rooftop_orientation_frequencies_%(rooftop_source)s c
            	ON b.pid = c.pid
            INNER JOIN diffusion_solar.rooftop_ground_cover_ratios d
            	on c.flat_roof = d.flat_roof
            INNER JOIN diffusion_solar.rooftop_percent_developable_buildings_by_state e
            	ON a.state_abbr = e.state_abbr
            	AND a.bldg_size_class = e.size_class;""" % inputs   
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    
    # query for indices creation    
    sql =  """CREATE INDEX block_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s_census_division_abbr_btree 
              ON %(schema)s.block_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s 
              USING BTREE(census_division_abbr);
              
              CREATE INDEX block_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s_resource_key_btree 
              ON %(schema)s.block_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s 
              USING BTREE(solar_re_9809_gid, tilt, azimuth);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def determine_allowable_turbine_heights(county_chunks, npar, pg_conn_string, schema, sector_abbr):
    
    
    msg = "Determining allowable turbine heights for each agent"
    logger.info(msg)

    
    inputs = locals().copy()
    
    #==============================================================================
    #     Find the allowable range of turbine heights for each agent
    #==============================================================================      
    sql = """DROP TABLE IF EXISTS %(schema)s.block_%(sector_abbr)s_sample_allowable_turbine_heights_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.block_%(sector_abbr)s_sample_allowable_turbine_heights_%(i_place_holder)s AS
             SELECT a.*, b.*,
                     CASE WHEN a.canopy_pct >= b.canopy_pct_requiring_clearance * 100 THEN a.canopy_ht_m + b.canopy_clearance_static_adder_m
                         ELSE 0
                     END as min_allowable_blade_height_m,
                     CASE WHEN a.acres_per_bldg <= b.required_parcel_size_cap_acres THEN sqrt(a.acres_per_bldg * 4046.86)/(2 * b.blade_height_setback_factor)
                         ELSE 'Infinity'::double precision
                     END as max_allowable_blade_height_m
                	FROM  %(schema)s.block_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s a
                	CROSS JOIN %(schema)s.input_wind_siting_settings_all b;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def find_potential_turbine_sizes(county_chunks, npar, pg_conn_string, schema, sector_abbr):

    msg = "Finding potential turbine sizes for each agent based on allowable height range"
    logger.info(msg)


    inputs = locals().copy()

                   
    #==============================================================================
    #     Create a lookup table of the allowable turbine heights and sizes for
    #     each agent
    #==============================================================================   
    sql = """DROP TABLE IF EXISTS %(schema)s.block_%(sector_abbr)s_sample_allowable_turbines_lkup_%(i_place_holder)s;
            CREATE TABLE %(schema)s.block_%(sector_abbr)s_sample_allowable_turbines_lkup_%(i_place_holder)s AS
            WITH turbine_sizes as
            (
                	SELECT a.turbine_size_kw, a.rotor_radius_m,
                		b.turbine_height_m,
                		b.turbine_height_m - a.rotor_radius_m * c.canopy_clearance_rotor_factor as effective_min_blade_height_m,
                		b.turbine_height_m + a.rotor_radius_m as effective_max_blade_height_m
                	FROM diffusion_wind.turbine_size_to_rotor_radius_lkup a
                	LEFT JOIN %(schema)s.input_wind_performance_allowable_turbine_sizes b
                		ON a.turbine_size_kw = b.turbine_size_kw
                	CROSS JOIN %(schema)s.input_wind_siting_settings_all c
            ),
            SELECT a.pgid, 
                	COALESCE(b.turbine_height_m, 0) AS turbine_height_m,
                	COALESCE(b.turbine_size_kw, 0) as turbine_size_kw 
            FROM %(schema)s.block_%(sector_abbr)s_sample_allowable_turbine_heights_%(i_place_holder)s a
            LEFT JOIN turbine_sizes b
                	ON b.effective_min_blade_height_m >= a.min_allowable_blade_height_m 
                	AND b.effective_max_blade_height_m <= a.max_allowable_blade_height_m;
             """ % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
       
    
    # create indices        
    sql =  """CREATE INDEX block_%(sector_abbr)s_sample_allowable_turbines_lkup_%(i_place_holder)s_pgid_btree 
              ON %(schema)s.block_%(sector_abbr)s_sample_allowable_turbines_lkup_%(i_place_holder)s 
              USING BTREE(pgid);
    
              CREATE INDEX block_%(sector_abbr)s_sample_allowable_turbines_lkup_%(i_place_holder)s_turbine_height_m_btree 
              ON %(schema)s.block_%(sector_abbr)s_sample_allowable_turbines_lkup_%(i_place_holder)s 
              USING BTREE(turbine_height_m);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)              



#%%
def select_rate_tariffs():
    
    # all in postgres
    pass

#%%
def assemble_resource_data():
    
    # all in postgres
    pass

#%%
def apply_temporal_factors():
    
    # all in python??
    pass

#%%
def get_agents():
    
    pass

#%% TODO LIST

# IMMEDIATE
# TODO: refactor wind siting to produce a lookup table of available turibne heights and sizes for each agent (this will be used later in assembling resource data?)
# TODO: figure out how to validate sql before running and catch errors immediately (stopping the code!)
# TODO: rename intermediate tables created at various steps and make sure they link to the next step
# TODO: test the progression of base_characteristics generation in isolation from the model

# LONG TERM
# TODO: Refactor sql code so that we arent carrying all attributes through all steps??? then join at the end?
# TODO: strip cap cost multipliers from agent locations and move to somewhere downstream
# TODO: Remove RECS/CBECS as option for rooftop characteristics from input sheet and database