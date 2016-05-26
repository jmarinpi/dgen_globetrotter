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
import multiprocessing
import traceback

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
        
        return (0, None)
        
    except Exception, e:       
        return (1, e.__str__())


#%%
def p_run(pg_conn_string, sql, county_chunks, pool):
           
    num_workers = pool._processes
    result_list = []
    for i in xrange(num_workers):

        place_holders = {'i': i, 'county_ids': utilfunc.pylist_2_pglist(county_chunks[i])}
        isql = sql % place_holders
        
        res = pool.apply_async(p_execute, args = (pg_conn_string, isql))
        result_list.append(res)    
    
    # get results as they are returned
    result_returns = []
    for i, result in enumerate(result_list):        
        result_return = result.get()     
        result_returns.append(result_return)    
    
    results_df = pd.DataFrame(result_returns, columns = ['status_code', 'msg'])
    # find whether there are any errors
    errors_df = results_df[results_df['status_code'] == 1]
    if errors_df.shape[0] > 0:
        # errors = '\n\n'.join(errors_df['msg']) # if you'd rather print all messages, but usually they will be redundant
        first_error = errors_df['msg'][0]
        pool.close() 
        raise Exception('One or more SQL errors occurred.\n\nFirst error was:\n\n%s' % first_error)
    else:
        return


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 1, prefix = '')
def generate_core_agent_attributes(cur, con, techs, schema, agents_per_region, sectors,
                                            pg_procs, pg_conn_string, scenario_opts):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'

    seed = scenario_opts['random_generator_seed']
    
    # break counties into subsets for parallel processing
    county_chunks, pg_procs = split_counties(cur, schema, pg_procs)        

    # create the pool of multiprocessing workers
    # (note: do this after splitting counties because, for small states, split_counties will adjust the number of pg_procs)
    pool = multiprocessing.Pool(processes = pg_procs) 
    
    try:
        # all in postgres
        for sector_abbr, sector in sectors.iteritems():
            with utilfunc.Timer() as t:
                logger.info("Creating Agents for %s Sector" % sector)
                    
                #==============================================================================
                #     sample from blocks and building microdata, convolve samples, and estimate 
                #     max demand for each agent
                #==============================================================================
                # NOTE: each of these functions is dependent on the last, so changes from one must be cascaded to the others
                sample_blocks(schema, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string)
                sample_building_microdata(schema, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string)
                convolve_block_and_building_samples(schema, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string)
                calculate_max_demand(schema, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string)
    
                #==============================================================================
                #     impose agent level siting  attributes (i.e., "tech potential")
                #==============================================================================
                # SOLAR
                simulate_roof_characteristics(county_chunks, pool, pg_conn_string, con, schema, sector_abbr, seed)
                
                # WIND
                determine_allowable_turbine_heights(county_chunks, pool, pg_conn_string, schema, sector_abbr)
                find_potential_turbine_sizes(county_chunks, pool, pg_conn_string, schema, sector_abbr)
    
                #==============================================================================
                #     combine all pieces into a single table
                #==============================================================================
                combine_all_attributes(county_chunks, pool, cur, con, pg_conn_string, schema, sector_abbr)
    

    finally:
        # close the multiprocessing pool
        pool.close() 



#%%
def split_counties(cur, schema, pg_procs):
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
    
    if len(counties) > pg_procs:
        county_chunks = map(list,np.array_split(counties, pg_procs))
    else:
        county_chunks = [counties]
        pg_procs = 1
    
    return county_chunks, pg_procs

    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def sample_blocks(schema, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string):

    msg = '\tSampling from Blocks for Each County'
    logger.info(msg)
    
    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'
        
    #==============================================================================
    #     randomly sample  N blocks from each county 
    #==============================================================================    
    # (note: [this may not be true any longer...] some counties will have fewer than N points, in which case, all are returned) 
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s AS
            WITH a as 
            (
                SELECT a.county_id, 
                        unnest(diffusion_shared.sample(array_agg(a.pgid ORDER BY a.pgid), 
                                                       %(agents_per_region)s, 
                                                       %(seed)s, 
                                                       True, 
                                                       array_agg(a.sample_weight ORDER BY a.pgid))
                                                       ) as pgid
                FROM %(schema)s.block_microdata_%(sector_abbr)s_joined a
                WHERE a.county_id IN (%(chunk_place_holder)s)
                GROUP BY a.county_id
            )
                
            SELECT a.pgid, a.county_id, ROW_NUMBER() OVER (PARTITION BY a.county_id ORDER BY a.county_id, a.pgid) as bin_id
            FROM a;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
    

    # add indices
    sql = """CREATE INDEX agent_blocks_%(sector_abbr)s_%(i_place_holder)s_pgid_btree 
            ON %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(pgid);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def sample_building_microdata(schema, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string):

    msg = "\tSampling from Building Microdata"
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'
    inputs['load_where'] = " AND '%s' = b.sector_abbr" % sector_abbr
    if sector_abbr == 'res':
        inputs['load_region'] = 'reportable_domain'
    else:
        inputs['load_region'] = 'census_division_abbr'


    #==============================================================================
    #    create lookup table with random values for each load bin 
    #==============================================================================
    sql =  """DROP TABLE IF EXISTS %(schema)s.agent_bldgs_%(sector_abbr)s_%(i_place_holder)s;
         CREATE UNLOGGED TABLE %(schema)s.agent_bldgs_%(sector_abbr)s_%(i_place_holder)s AS
         WITH all_bldgs AS
         (
             SELECT a.county_id, 
                     b.bldg_id, b.weight, b.ann_cons_kwh, 
                     b.crb_model, b.roof_style, b.roof_sqft, 
                     b.ownocc8
             FROM diffusion_blocks.county_geoms a
             INNER JOIN %(schema)s.states_to_model c
                   ON a.state_abbr = c.state_abbr
             LEFT JOIN diffusion_shared.cbecs_recs_combined b
                 ON a.%(load_region)s = b.%(load_region)s
             WHERE a.county_id in  (%(chunk_place_holder)s)
                   %(load_where)s
        ),
        sampled_bldgs AS 
        (
            SELECT a.county_id, 
                    unnest(diffusion_shared.sample(array_agg(a.bldg_id ORDER BY a.bldg_id), 
                                                   %(agents_per_region)s, 
                                                   %(seed)s * a.county_id, 
                                                   True, 
                                                   array_agg(a.weight ORDER BY a.bldg_id))
                                                   ) as bldg_id
            FROM all_bldgs a
            GROUP BY a.county_id
        ), 
        numbered_samples AS
        (
            SELECT a.county_id, a.bldg_id,
                   ROW_NUMBER() OVER (PARTITION BY a.county_id ORDER BY a.county_id, a.bldg_id) as bin_id 
            FROM sampled_bldgs a
        )
        SELECT  a.county_id, a.bin_id, a.bldg_id,
                    b.weight, b.ann_cons_kwh, b.crb_model, b.roof_style, b.roof_sqft, b.ownocc8
        FROM numbered_samples a
        LEFT JOIN diffusion_shared.cbecs_recs_combined b
            ON a.bldg_id = b.bldg_id
        %(load_where)s ;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
    

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_bldgs_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
    
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def convolve_block_and_building_samples(schema, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string, step = 3):

    msg = '\tConvolving Block and Building Samples'    
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'      
    # for commercial customers, due to multi-tenant occupancy, use buildings rather than customers as the unit for agents
    if sector_abbr == 'com':
        inputs['county_customer_count'] = 'county_bldg_count_2012'
    else:
        inputs['county_customer_count'] = 'county_total_customers_2011'
        
   
    #==============================================================================
    #     link each block sample to a building sample
    #==============================================================================
    sql =  """DROP TABLE IF EXISTS %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s AS
            WITH a as
            (
                SELECT a.pgid, a.county_id, a.bin_id, 
                        b.crb_model, b.ann_cons_kwh, 
                        b.weight as eia_weight, 
                       CASE WHEN b.roof_sqft < 5000 THEN 'small'::character varying(6)
                            WHEN b.roof_sqft >= 5000 and b.roof_sqft < 25000 THEN 'medium'::character varying(6)
                            WHEN b.roof_sqft >= 25000 THEN 'large'::character varying(6)
                        END as bldg_size_class,
                        b.roof_sqft, b.roof_style, b.ownocc8,
                    	c.%(county_customer_count)s * b.weight/sum(b.weight) OVER (PARTITION BY b.county_id) as customers_in_bin, 
                    	c.county_total_load_mwh_2011 * 1000 * (b.ann_cons_kwh * b.weight)/sum(b.ann_cons_kwh * b.weight) 
                             OVER (PARTITION BY b.county_id) as load_kwh_in_bin,
                        c.hdf_load_index as hdf_index
                FROM %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s a
                LEFT JOIN %(schema)s.agent_bldgs_%(sector_abbr)s_%(i_place_holder)s b
                    ON a.county_id = b.county_id
                    AND a.bin_id = b.bin_id
                LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined c
                    ON a.pgid = c.pgid
                WHERE c.county_total_load_mwh_2011 > 0
            )
            SELECT a.*,
            	CASE  WHEN a.customers_in_bin > 0 THEN ROUND(a.load_kwh_in_bin/a.customers_in_bin, 0)::BIGINT
                	ELSE 0::BIGINT
                  END AS load_kwh_per_customer_in_bin
            FROM a;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
    

    # add indices
    sql = """CREATE INDEX agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s_join_btree 
            ON %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(crb_model, hdf_index);
            
            
            CREATE INDEX agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s_pgid_btree 
            ON %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(pgid);            
            """ % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
    
    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def calculate_max_demand(schema, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string):

    msg = '\tCalculating Maximum Electricity Demand for Each Agent'    
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

          
    #==============================================================================
    #     find the max demand for each agent based on the applicable energy plus building model
    #==============================================================================
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_max_demand_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.agent_max_demand_%(sector_abbr)s_%(i_place_holder)s AS
            SELECT a.county_id, a.bin_id, 
                    ROUND(b.normalized_max_demand_kw_per_kw * a.load_kwh_per_customer_in_bin, 0)::INTEGER AS max_demand_kw
            FROM %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s a
            LEFT JOIN diffusion_load_profiles.energy_plus_max_normalized_demand b
                ON a.crb_model = b.crb_model
                AND a.hdf_index = b.hdf_index;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
           
    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_max_demand_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def simulate_roof_characteristics(county_chunks, pool, pg_conn_string, con, schema, sector_abbr, seed):
     
    msg = "\tSimulating Rooftop Characteristics For Each Agent"
    logger.info(msg)

    
    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'
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
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s AS
            with a as
            (
                	SELECT a.county_id, a.bin_id, a.bldg_size_class,
                         b.ulocale, b.state_abbr,
                         c.city_id, c.rank as city_rank
                	FROM %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s a
                  LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                        ON a.pgid = b.pgid
                	LEFT JOIN diffusion_solar.rooftop_city_ranks_by_county_and_ulocale_%(sector_abbr)s c
                		ON a.county_id = c.county_id
                		AND b.ulocale = c.ulocale
                	INNER JOIN diffusion_solar.rooftop_city_ulocale_zone_size_class_lkup d
                		ON b.ulocale = d.ulocale
                		AND d.zone = '%(zone)s' 
                		AND a.bldg_size_class = d.size_class
                		AND c.city_id = d.city_id
            ), 
            b as
            (
                	SELECT  a.*, row_number() OVER (PARTITION BY county_id, bin_id ORDER BY city_rank asc) as rank
                	FROM a
            )
            SELECT *
            FROM b
            WHERE rank = 1;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
    
    # add indices on join keys
    sql =  """CREATE INDEX agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s_city_id_btree 
              ON %(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s 
              USING BTREE(city_id);
              
              CREATE INDEX agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s_bldg_size_class_btree 
              ON %(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s 
              USING BTREE(bldg_size_class);
              
              CREATE INDEX agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s_ulocale_btree 
              ON %(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s 
              USING BTREE(ulocale);
              
              CREATE INDEX agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s_state_abbr_btree 
              ON %(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s 
              USING BTREE(state_abbr);
              
              
              
              CREATE INDEX agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s_id_btree 
              ON %(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s 
              USING BTREE(county_id, bin_id);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
    
    
    # sample from the lidar bins for that city
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_rooftops_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.agent_rooftops_%(sector_abbr)s_%(i_place_holder)s AS
            WITH b as
            (
                	SELECT a.county_id, a.bin_id,
                		unnest(diffusion_shared.sample(array_agg(b.pid ORDER BY b.pid), 1, 
                                                     %(seed)s * a.bin_id * a.county_id, 
                                                     FALSE, 
                                                     array_agg(b.count ORDER BY b.pid))
                                                     ) as pid
                	FROM %(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s a
                	LEFT JOIN diffusion_solar.rooftop_orientation_frequencies_%(rooftop_source)s b
                		ON a.city_id = b.city_id
                		AND  b.zone = '%(zone)s'
                		AND a.ulocale = b.ulocale
                		AND a.bldg_size_class = b.size_class
                	GROUP BY a.county_id, a.bin_id
            )
            SELECT a.county_id, a.bin_id, 
                    c.tilt, c.azimuth, e.pct_developable,
                    c.slopearea_m2_bin * 10.7639 * d.gcr as developable_roof_sqft,
                    d.gcr as ground_cover_ratio                 
            FROM %(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s a
            INNER JOIN b
                	ON a.county_id = b.county_id
                	AND a.bin_id = b.bin_id
            INNER JOIN diffusion_solar.rooftop_orientation_frequencies_%(rooftop_source)s c
                	ON b.pid = c.pid
            INNER JOIN diffusion_solar.rooftop_ground_cover_ratios d
                	on c.flat_roof = d.flat_roof
            INNER JOIN diffusion_solar.rooftop_percent_developable_buildings_by_state e
                	ON a.state_abbr = e.state_abbr
                  AND a.bldg_size_class = e.size_class;""" % inputs   
    p_run(pg_conn_string, sql, county_chunks, pool)
    
    
    # add primary key 
    sql =  """ALTER TABLE %(schema)s.agent_rooftops_%(sector_abbr)s_%(i_place_holder)s
              ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def determine_allowable_turbine_heights(county_chunks, pool, pg_conn_string, schema, sector_abbr):
    
    
    msg = "\tDetermining Allowable Turbine Heights for Each Agent"
    logger.info(msg)

    
    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'
    
    #==============================================================================
    #     Find the allowable range of turbine heights for each agent
    #==============================================================================      
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_turbine_height_constraints_%(sector_abbr)s_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.agent_turbine_height_constraints_%(sector_abbr)s_%(i_place_holder)s AS
             SELECT a.county_id, a.bin_id,
                     
                     CASE WHEN b.canopy_pct >= c.canopy_pct_requiring_clearance * 100 THEN 
                               b.canopy_ht_m + c.canopy_clearance_static_adder_m
                         ELSE 0
                     END as min_allowable_blade_height_m,

                     CASE WHEN b.acres_per_bldg <= c.required_parcel_size_cap_acres THEN 
                               sqrt(b.acres_per_bldg * 4046.86)/(2 * c.blade_height_setback_factor)
                         ELSE 'Infinity'::double precision
                     END as max_allowable_blade_height_m

                	FROM %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s a
                  LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                        ON a.pgid = b.pgid
                	CROSS JOIN %(schema)s.input_wind_siting_settings_all c;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
    
    # add primary key 
    sql =  """ALTER TABLE %(schema)s.agent_turbine_height_constraints_%(sector_abbr)s_%(i_place_holder)s
              ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def find_potential_turbine_sizes(county_chunks, pool, pg_conn_string, schema, sector_abbr):

    msg = "\tIdentifying Potential Turbine Sizes for Each Agent"
    logger.info(msg)


    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'
    
                   
    #==============================================================================
    #     Create a lookup table of the allowable turbine heights and sizes for
    #     each agent
    #==============================================================================   
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s;
            CREATE TABLE %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s AS
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
            )
            SELECT a.county_id, a.bin_id,
                	COALESCE(b.turbine_height_m, 0) AS turbine_height_m,
                	COALESCE(b.turbine_size_kw, 0) as turbine_size_kw 
            FROM %(schema)s.agent_turbine_height_constraints_%(sector_abbr)s_%(i_place_holder)s a
            LEFT JOIN turbine_sizes b
                	ON b.effective_min_blade_height_m >= a.min_allowable_blade_height_m 
                	AND b.effective_max_blade_height_m <= a.max_allowable_blade_height_m;
             """ % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
       
    
    # create indices        
    sql =  """CREATE INDEX agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s_id_btree 
              ON %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s
              USING BTREE(county_id, bin_id);
    
              CREATE INDEX agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s_turbine_height_m_btree 
              ON %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s
              USING BTREE(turbine_height_m);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)              

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def combine_all_attributes(county_chunks, pool, cur, con, pg_conn_string, schema, sector_abbr):

    msg = "\tCombining All Core Agent Attributes"
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    
    sql_part = """SELECT -- block location dependent properties
                    b.*,
                    
                    -- building microdata dependent properties 
                    a.bin_id,
                    a.crb_model,
                    a.ann_cons_kwh,
                    a.eia_weight,
                    a.bldg_size_class,
                    a.roof_sqft as eia_roof_sqft,
                    a.roof_style,
                    a.ownocc8,
                    a.customers_in_bin,
                    a.load_kwh_in_bin,
                    a.load_kwh_in_bin/customers_in_bin as load_kwh_per_customer_in_bin,
                    
                    -- load profile
                    c.max_demand_kw,
    
                    -- solar siting constraints
                    d.tilt,
                    d.azimuth,
                    d.pct_developable as pct_of_bldgs_developable,
                    d.developable_roof_sqft,
                    d.ground_cover_ratio,    

                    -- wind siting constraints                    
                    e.min_allowable_blade_height_m,
                    e.max_allowable_blade_height_m
    
             FROM %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s a
             LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                 ON a.pgid = b.pgid
             LEFT JOIN %(schema)s.agent_max_demand_%(sector_abbr)s_%(i_place_holder)s c
                 ON a.county_id = c.county_id
                 AND a.bin_id = c.bin_id
             LEFT JOIN %(schema)s.agent_rooftops_%(sector_abbr)s_%(i_place_holder)s d
                 ON a.county_id = d.county_id
                 AND a.bin_id = d.bin_id
            LEFT JOIN %(schema)s.agent_turbine_height_constraints_%(sector_abbr)s_%(i_place_holder)s e
                 ON a.county_id = e.county_id
                 AND a.bin_id = e.bin_id""" % inputs
    
    # create the template table
    template_inputs = inputs.copy()
    template_inputs['i'] = 0
    template_inputs['sql_body'] = sql_part % template_inputs
    sql_template = """DROP TABLE IF EXISTS %(schema)s.agent_core_attributes_%(sector_abbr)s;
                      CREATE TABLE %(schema)s.agent_core_attributes_%(sector_abbr)s AS
                      %(sql_body)s
                      LIMIT 0;""" % template_inputs
    cur.execute(sql_template)
    con.commit()
    
    # reconfigure sql into an insert statement
    inputs['sql_body'] = sql_part
    sql = """INSERT INTO %(schema)s.agent_core_attributes_%(sector_abbr)s
            %(sql_body)s;""" % inputs
    # run the insert statement
    p_run(pg_conn_string, sql, county_chunks, pool)
    
    # add primary key 
    sql =  """ALTER TABLE %(schema)s.agent_core_attributes_%(sector_abbr)s
              ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    cur.execute(sql)
    con.commit()

    # create indices
    # TODO: add indices that are neeeded in subsequent steps

    
    
    

#%%
def select_rate_tariffs():
    
    # all in postgres
    pass

#%%
def assemble_resource_data():
    
    # all in postgres
    # for wind, need to use %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s
    pass
    # for solar, not changes to columns:
        # available_roof_sqft --> developable_roof_sqft
        # pct_developable --> pct_of_bldgs_developable

#%%
def apply_temporal_factors():
    
    # all in python??
    pass

#%%
def get_agents():
    
    pass

#%% TODO LIST

# IMMEDIATE


# UP NEXT
# TODO: Look into why determine_allowable_turbine_heights is slow for residential and commercial
    #  (may just be table size)
# TODO: do a little more inspection of the agent_core_attributes to make sure outputs look good
# TODO: add code to drop intermediate tables from core attributes generation
# TODO: add indices to agent_core_attributes for subsequent steps
# TODO: add in a check that agent_core_attributes_ table has the correct number of rows
# TODO: continue with the rest of the functions outlined above, 
        # to reproduce the functionality of the old generate customer bins 
        # in a more modular form

# LONG TERM
# TODO: strip cap cost multipliers from agent locations and move to somewhere downstream
# TODO: Remove RECS/CBECS as option for rooftop characteristics from input sheet and database