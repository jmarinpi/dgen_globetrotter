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
import data_functions as datfunc
from agent import Agent, Agents, AgentsAlgorithm
from cStringIO import StringIO

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
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 0, prefix = '')
def generate_core_agent_attributes(cur, con, techs, schema, agents_per_region, sectors,
                                            pg_procs, pg_conn_string, seed):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    
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
                find_potential_turbine_sizes(county_chunks, cur, con, pool, pg_conn_string, schema, sector_abbr)
    
                #==============================================================================
                #     combine all pieces into a single table
                #==============================================================================
                combine_all_attributes(county_chunks, pool, cur, con, pg_conn_string, schema, sector_abbr)
    
        #==============================================================================
        #     create a view that combines all sectors and techs
        #==============================================================================
        merge_all_core_agents(cur, con, schema, sectors, techs)

        #==============================================================================
        #    drop the intermediate tables
        #==============================================================================
        cleanup_intermediate_tables(schema, sectors, county_chunks, pg_conn_string, cur, con, pool)
        
    except:
        # roll back any transactions
        con.rollback()
        # drop the output schema
        datfunc.drop_output_schema(pg_conn_string, schema, True)
        # re-raise the exception
        raise
        
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
                     b.bldg_id, b.weight
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
                    b.weight, b.ann_cons_kwh, b.crb_model, b.roof_style, b.roof_sqft,
                    b.ownocc8 as owner_occupancy_status
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
                        b.roof_sqft, b.roof_style, b.owner_occupancy_status,
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
                    c.tilt, c.azimuth, e.pct_developable as pct_of_bldgs_developable,
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
def find_potential_turbine_sizes(county_chunks, cur, con, pool, pg_conn_string, schema, sector_abbr):

    msg = "\tIdentifying Potential Turbine Sizes for Each Agent"
    logger.info(msg)


    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'
    
                   
    #==============================================================================
    #     Create a lookup table of the allowable turbine heights and sizes for
    #     each agent
    #============================================================================== 
    # create the output table
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s;
              CREATE TABLE %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s
              (
                county_id integer,
                bin_id integer,
                turbine_height_m integer,
                turbine_size_kw numeric
              );""" % inputs                   
    cur.execute(sql)
    con.commit()
                   
    sql = """INSERT INTO %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s
            SELECT a.county_id, a.bin_id,
                	COALESCE(b.turbine_height_m, 0) AS turbine_height_m,
                	COALESCE(b.turbine_size_kw, 0) as turbine_size_kw 
            FROM %(schema)s.agent_turbine_height_constraints_%(sector_abbr)s_%(i_place_holder)s a
            LEFT JOIN %(schema)s.input_wind_siting_turbine_sizes b
                	ON b.effective_min_blade_height_m >= a.min_allowable_blade_height_m 
                	AND b.effective_max_blade_height_m <= a.max_allowable_blade_height_m;
             """ % inputs
    p_run(pg_conn_string, sql, county_chunks, pool)
       
    
    # create indices        
    sql =  """CREATE INDEX agent_allowable_turbines_lkup_%(sector_abbr)s_id_btree 
              ON %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s
              USING BTREE(county_id, bin_id);
    
              CREATE INDEX agent_allowable_turbines_lkup_%(sector_abbr)s_turbine_height_m_btree 
              ON %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s
              USING BTREE(turbine_height_m);""" % inputs
    cur.execute(sql)
    con.commit()



#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def combine_all_attributes(county_chunks, pool, cur, con, pg_conn_string, schema, sector_abbr):

    msg = "\tCombining All Core Agent Attributes"
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    
    sql_part = """SELECT 
                    -- block location dependent properties
                    b.*,
                    
                    -- building microdata dependent properties 
                    a.bin_id,
                    a.crb_model,
                    a.ann_cons_kwh,
                    a.eia_weight,
                    a.bldg_size_class,
                    a.roof_sqft as eia_roof_sqft,
                    a.roof_style,
                    a.owner_occupancy_status,
                    a.customers_in_bin,
                    a.load_kwh_in_bin,
                    a.load_kwh_in_bin/customers_in_bin as load_kwh_per_customer_in_bin,
                    
                    -- load profile
                    c.max_demand_kw,
    
                    -- solar siting constraints
                    d.tilt,
                    d.azimuth,
                    d.pct_of_bldgs_developable,
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
    # TODO: add other indices that are neeeded in subsequent steps?
    sql = """CREATE INDEX agent_core_attributes_%(sector_abbr)s_btree_wind_resource
            ON  %(schema)s.agent_core_attributes_%(sector_abbr)s
            USING BTREE(i, j, cf_bin);
            
            CREATE INDEX agent_core_attributes_%(sector_abbr)s_btree_solar_resource
            ON  %(schema)s.agent_core_attributes_%(sector_abbr)s
            USING BTREE(solar_re_9809_gid, azimuth, tilt);""" % inputs
    cur.execute(sql)
    con.commit()


    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def cleanup_intermediate_tables(schema, sectors, county_chunks, pg_conn_string, cur, con, pool):
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    
    #==============================================================================
    #   clean up intermediate tables
    #==============================================================================
    msg = "\tCleaning Up Intermediate Tables..."
    logger.info(msg)
    intermediate_tables = [ 
                            '%(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_bldgs_%(sector_abbr)s_%(i_place_holder)s',    
                            '%(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_max_demand_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_rooftops_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_turbine_height_constraints_%(sector_abbr)s_%(i_place_holder)s'         
                            ]
    
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = 'DROP TABLE IF EXISTS %s;'
        for intermediate_table in intermediate_tables:
            table_name = intermediate_table % inputs
            isql = sql % table_name
            if '%(i)s' in table_name:
                p_run(pg_conn_string, isql, county_chunks, pool)
            else:
                cur.execute(isql)
                con.commit()       


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def merge_all_core_agents(cur, con, schema, sectors, techs):
    
    inputs = locals().copy()    
    
    msg = "Merging All Agents into a Single Table View"
    logger.info(msg)    
    
    sql_list = []
    for sector_abbr, sector in sectors.iteritems():
        for tech in techs:
            inputs['sector_abbr'] = sector_abbr
            inputs['tech'] = tech
            sql = """SELECT a.pgid, 
                            a.county_id, 
                            a.bin_id, 
                            a.state_abbr, 
                            a.census_division_abbr, 
                            a.pca_reg, 
                            a.reeds_reg, 
                            a.customers_in_bin, 
                            a.load_kwh_per_customer_in_bin, 
                            a.load_kwh_in_bin,
                            a.max_demand_kw,
                            a.hdf_load_index,
                            a.owner_occupancy_status,
                            -- capital cost regional multiplier
                            a.cap_cost_multiplier_%(tech)s as cap_cost_multiplier,
                            -- solar
                            a.solar_re_9809_gid,
                            a.tilt, 
                            a.azimuth, 
                            a.developable_roof_sqft, 
                            a.pct_of_bldgs_developable,
                            -- wind
                            a.i,
                            a.j,
                            a.cf_bin,
                            -- replicate for each sector and tech
                            '%(sector_abbr)s'::CHARACTER VARYING(3) as sector_abbr,
                            '%(tech)s'::varchar(5) as tech
                    FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a """ % inputs
            sql_list.append(sql)
    
    inputs['sql_body'] = ' UNION ALL '.join(sql_list)
    sql = """DROP VIEW IF EXISTS %(schema)s.agent_core_attributes_all;
             CREATE VIEW %(schema)s.agent_core_attributes_all AS
             %(sql_body)s;""" % inputs
    cur.execute(sql)
    con.commit()
    
    
#%%
def get_load_growth(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT sector_abbr, census_division_abbr, load_multiplier
            FROM %(schema)s.load_growth_to_model
            WHERE year = %(year)s;""" % inputs
            
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df    

#%%
def apply_load_growth(dataframe, load_growth_df):
    
        dataframe = pd.merge(dataframe, load_growth_df, how = 'left', on = ['sector_abbr', 'census_division_abbr'])
        dataframe['customers_in_bin'] = dataframe['customers_in_bin'] * dataframe['load_multiplier']
        dataframe['load_kwh_in_bin'] = dataframe['load_kwh_in_bin'] * dataframe['load_multiplier']
        
        return dataframe

#%%
def calculate_developable_customers_and_load(dataframe):

    dataframe['developable_customers_in_bin'] = np.where(dataframe['tech'] == 'solar', dataframe['pct_of_bldgs_developable'] * dataframe['customers_in_bin'], 
                                                            dataframe['customers_in_bin'])                                 
    dataframe['developable_load_kwh_in_bin'] = np.where(dataframe['tech'] == 'solar', dataframe['pct_of_bldgs_developable'] * dataframe['load_kwh_in_bin'], 
                                                            dataframe['load_kwh_in_bin'])     
                                                            
    return dataframe
    

#%%




#%%
#def get_temporal_data(con, schema, year, tech):
#    
#    inputs = locals().copy()
#    
#    sql = """SELECT *
#            FROM %(schema)s.temporal_factors_%(tech)s
#            WHERE year = %(year)s;""" % inputs
#            
#    df = pd.read_sql(sql, con, coerce_float = False)
#    
#    return df
#
#
#@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 1, prefix = '')
#def combine_temporal_data(cur, con, schema, techs, start_year, end_year, sector_abbrs):
#
#    logger.info("Combining Temporal Factors")
#
#    if 'wind' in techs:
#        combine_temporal_data_wind(cur, con, schema, start_year, end_year, sector_abbrs)
#    
#    if 'solar' in techs:
#        combine_temporal_data_solar(cur, con, schema, start_year, end_year, sector_abbrs)
#    
#
#def combine_temporal_data_wind(cur, con, schema, start_year, end_year, sector_abbrs):
#    # create a dictionary out of the input arguments -- this is used through sql queries    
#    inputs = locals().copy()       
#    
#    #==============================================================================
#    #     create table defining power curve partial transitions
#    #==============================================================================  
#    create_pc_transitions(cur, con, schema)        
#    
#    # combine the temporal data (this only needs to be done once for all sectors)
#    
#    # combined temporal data for technology specific factors
#    sql = """DROP TABLE IF EXISTS %(schema)s.temporal_factors_wind;
#            CREATE UNLOGGED TABLE %(schema)s.temporal_factors_wind as
#            SELECT      a.year, 
#                    	a.turbine_size_kw, 
#                         a.power_curve_1,
#                         a.power_curve_2,
#                    	a.interp_factor,
#                    	b.turbine_height_m,
#                    	d.derate_factor,
#                         'wind'::VARCHAR(5) as tech
#            FROM %(schema)s.input_wind_performance_power_curve_transitions a
#            LEFT JOIN %(schema)s.input_wind_performance_allowable_turbine_sizes b
#                	ON a.turbine_size_kw = b.turbine_size_kw
#            LEFT JOIN %(schema)s.input_wind_performance_gen_derate_factors d
#                	ON a.year = d.year
#                 AND  a.turbine_size_kw = d.turbine_size_kw
#            WHERE a.year BETWEEN %(start_year)s AND %(end_year)s
#            
#            UNION ALL
#            
#            SELECT GENERATE_SERIES(%(start_year)s, %(end_year)s, 2) as year,
#                	0 as turbine_size_kw,
#                	0 as power_curve_1,
#                  0 as power_curve_2,
#                  0 as interp_factor,
#                	0 as turbine_height_m,
#                	0 as derate_factor;""" % inputs
#    cur.execute(sql)
#    con.commit()
#    
#    
#    # create indices for subsequent joins
#    sql =  """CREATE INDEX temporal_factors_technology_join_fields_btree 
#              ON %(schema)s.temporal_factors_wind
#              USING BTREE(turbine_height_m, turbine_size_kw, power_curve_1, power_curve_2);
#              
#              CREATE INDEX temporal_factors_technology_year_btree 
#              ON %(schema)s.temporal_factors_wind
#              USING BTREE(year);""" % inputs
#    cur.execute(sql)
#    con.commit()             

#%%

def get_rate_structures(con, schema):
    
    inputs = locals().copy()
    
    sql = """
            	SELECT 'res' as sector_abbr, res_rate_structure as rate_structure
        	FROM %(schema)s.input_main_scenario_options
        	UNION
        	SELECT 'com' as sector_abbr, com_rate_structure as rate_structure
        	FROM %(schema)s.input_main_scenario_options
        	UNION
        	SELECT 'ind' as sector_abbr, ind_rate_structure as rate_structure
        	FROM %(schema)s.input_main_scenario_options;""" % inputs
    
    rate_structures_df = pd.read_sql(sql, con)
    rate_structures = dict(zip(rate_structures_df['sector_abbr'], rate_structures_df['rate_structure']))
    
    return rate_structures    

  
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_electric_rates(cur, con, schema, sectors, seed, pg_conn_string):

    # NOTE: This function creates a lookup table for the agents in each sector, providing
    #       the county_id and bin_id for each agent, along with the rate_id_alias and rate_source.
    #       This information is used in "get_electric_rate_tariffs" to load in the actual rate tariff for each agent.

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'
    excluded_rates = pd.read_csv('./excluded_rates_ids.csv', header=None)
    inputs['excluded_rate_ids'] = '(' + ', '.join([str(i[0]) for i in excluded_rates.values]) + ')'


    msg = "\tGenerating Electric Rate Tariff Lookup Table for Agents"
    logger.info(msg)
    
    # determine which type of rate to use for each sector
    rate_structures = get_rate_structures(con, schema)
    
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        rate_structure = rate_structures[sector_abbr]

        if rate_structure.lower() == 'complex rates':
    
            sql = """DROP TABLE IF EXISTS %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s;
                    CREATE UNLOGGED TABLE %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s AS
                    WITH a AS
                    (
                        	SELECT a.county_id, a.bin_id, a.state_abbr, a.max_demand_kw,
                        		unnest(b.rate_ids) as rate_id_alias,
                        		unnest(b.rate_ranks) as rate_rank
                        	FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                        	LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                                	ON a.pgid = b.pgid
                    ),
                    b AS
                    (
                            SELECT a.*,
                                    b.rate_type,
                                    b.pct_of_customers
                            FROM a 
                            LEFT JOIN diffusion_shared.urdb_rates_by_state_%(sector_abbr)s b
                                    ON a.rate_id_alias = b.rate_id_alias
                                    AND a.state_abbr = b.state_abbr
                            WHERE a.max_demand_kw <= b.urdb_demand_max
                                  AND a.max_demand_kw >= b.urdb_demand_min
                                  AND b.rate_id_alias NOT IN %(excluded_rate_ids)s
                    ),
                    c as
                    (
                        	SELECT *, rank() OVER (PARTITION BY county_id, bin_id ORDER BY rate_rank ASC) as rank
                        	FROM b
                    ), 
                    d as
                    (
                        SELECT c.*, COALESCE(c.pct_of_customers, d.%(sector_abbr)s_weight) as rate_type_weight
                        FROM c 
                        LEFT JOIN %(schema)s.input_main_market_rate_type_weights d
                        ON c.rate_type = d.rate_type
                        WHERE c.rank = 1
                    )
                    SELECT d.county_id, d.bin_id,
                            unnest(diffusion_shared.sample(
                                            array_agg(d.rate_id_alias ORDER BY d.rate_id_alias), 
                                            1, 
                                          (%(seed)s * d.county_id * d.bin_id), 
                                          False,
                                          array_agg(d.rate_type_weight ORDER BY d.rate_id_alias))) as rate_id_alias,
                            'urdb3'::CHARACTER VARYING(5) as rate_source
                    FROM d
                    GROUP BY d.county_id, d.bin_id;""" % inputs
            cur.execute(sql)
            con.commit()
    
        elif rate_structure.lower() == 'flat (annual average)':
            # flat annual average rate ids are already stored in the demandmax table as county_id
            # we simply need to duplicate and rename that field to rate_id_alias and specify the rate_source
            sql = """DROP TABLE IF EXISTS %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s;
                     CREATE UNLOGGED TABLE %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s AS
                     SELECT a.county_id, a.bin_id 
                            a.old_county_id as rate_id_alias, 
                          'aa%(sector_abbr)s'::CHARACTER VARYING(5) as rate_source
                    FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                    WHERE a.county_id in (%(chunk_place_holder)s);""" % inputs
            cur.execute(sql)
            con.commit()
                         
        elif rate_structure.lower() == 'flat (user-defined)':
            # user-defined rates are id'ed based on the state_fips, which is already stored in the demandmax table
            # we simply need to duplicate and rename that field to rate_id_alias and specify the rate_source
            sql = """DROP TABLE IF EXISTS %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s;
                    CREATE UNLOGGED TABLE %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s AS
                    SELECT a.county_id, a.bin_id 
                            a.state_fips::INTEGER as rate_id_alias, 
                            'ud%(sector_abbr)s'::CHARACTER VARYING(5) as rate_source
                    FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                    WHERE a.county_id in (%(chunk_place_holder)s);""" % inputs
            cur.execute(sql)
            con.commit()
    
        # add primary key to rates lkup table
        sql = """ALTER TABLE %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s
                 ADD PRIMARY KEY (county_id, bin_id);""" % inputs
        cur.execute(sql)
        con.commit()
        
        # get the rates
        sql = """SELECT a.county_id, a.bin_id, '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        b.sam_json as rate_json
               FROM  %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s a
               LEFT JOIN %(schema)s.all_rate_jsons b 
                   ON a.rate_id_alias = b.rate_id_alias
                   AND a.rate_source = b.rate_source;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    # combine the dfs
    df = pd.concat(df_list, axis = 0, ignore_index = True)

    return df

#%%
def get_net_metering_settings(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT state_abbr,
                    sector_abbr,
                    system_size_limit_kw as nem_system_size_limit_kw,
                    year_end_excess_sell_rate_dlrs_per_kwh as ur_nm_yearend_sell_rate,
                    hourly_excess_sell_rate_dlrs_per_kwh as ur_flat_sell_rate
             FROM %(schema)s.input_main_nem_scenario
             WHERE year = %(year)s;""" % inputs
             
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df

#%%
def select_electric_rates(dataframe, rates_df, net_metering_df):
    
    dataframe = pd.merge(dataframe, rates_df, how = 'left', on = ['county_id', 'bin_id', 'sector_abbr'])
    dataframe = pd.merge(dataframe, net_metering_df, how = 'left',  on = ['state_abbr', 'sector_abbr'])
                                                            
    return dataframe

#%%
def update_net_metering_fields(dataframe):
    
    dataframe = dataframe.apply(datfunc.update_rate_json_w_nem_fields, axis = 1)    
    
    return dataframe

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def assemble_resource_data():
    
    # all in postgres
    # for wind, need to use %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s
    pass
    # for solar, not changes to columns:
        # available_roof_sqft --> developable_roof_sqft
        # pct_developable --> pct_of_bldgs_developable

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_core_agent_attributes(con, schema):
    
    inputs = locals().copy()
    sql = """SELECT *
             FROM %(schema)s.agent_core_attributes_all;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)

    agents = Agents(df)

    return agents

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_system_sizing_targets(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT 'solar'::VARCHAR(5) as tech, 
                     sector_abbr,
                     sys_size_target_nem,
                     sys_size_target_no_nem,
                     NULL::NUMERIC AS sys_oversize_limit_nem,
                     NULL::NUMERIC AS sys_oversize_limit_no_nem
             FROM %(schema)s.input_solar_performance_system_sizing_factors 
             
             UNION ALL
             
             SELECT 'wind'::VARCHAR(5) as tech, 
                     sector_abbr,
                     sys_size_target_nem,
                     sys_size_target_no_nem,
                     sys_oversize_limit_nem,
                     sys_oversize_limit_no_nem
             FROM %(schema)s.input_wind_performance_system_sizing_factors;""" % inputs

    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_technology_performance_solar(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT 'solar'::VARCHAR(5) as tech,
                    efficiency_improvement_factor as pv_efficiency_improvement_factor,
                    density_w_per_sqft as pv_density_w_per_sqft,
                    inverter_lifetime_yrs as pv_inverter_lifetime_yrs
             FROM %(schema)s.input_solar_performance_improvements
             WHERE year = %(year)s;""" % inputs
    
    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_technology_performance_wind(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT 'wind'::VARCHAR(5) as tech,
                    a.turbine_size_kw, 
                    a.derate_factor as wind_derate_factor
            FROM %(schema)s.input_wind_performance_gen_derate_factors a
            WHERE a.year = %(year)s
            
            UNION ALL
            
            SELECT 'wind'::VARCHAR(5) as tech,
                    0::NUMERIC as turbine_size_kw,
                    0::NUMERIC as wind_derate_factor;""" % inputs

    df = pd.read_sql(sql, con, coerce_float = False)
    
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_annual_resource_wind(con, schema, year, sectors):
    
    inputs = locals().copy()
    
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT 'wind'::VARCHAR(5) as tech,
                        '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        a.county_id, a.bin_id, 
                    	COALESCE(b.turbine_height_m, 0) as turbine_height_m,
                    	COALESCE(b.turbine_size_kw, 0) as turbine_size_kw,
                    	coalesce(c.interp_factor, 0) as power_curve_interp_factor,
                    	COALESCE(c.power_curve_1, -1) as power_curve_1,
                    	COALESCE(c.power_curve_2, -1) as power_curve_2,
                    	COALESCE(d.aep, 0) as naep_1,
                    	COALESCE(e.aep, 0) as naep_2
                FROM  %(schema)s.agent_core_attributes_%(sector_abbr)s a
                LEFT JOIN %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s b
                    	ON a.county_id = b.county_id
                    	and a.bin_id = b.bin_id
                LEFT JOIN %(schema)s.input_wind_performance_power_curve_transitions c
                    	ON b.turbine_size_kw = c.turbine_size_kw
                         AND c.year = %(year)s
                LEFT JOIN diffusion_resource_wind.wind_resource_annual d
                    	ON a.i = d.i
                    	AND a.j = d.j
                    	AND a.cf_bin = d.cf_bin
                    	AND b.turbine_height_m = d.height
                    	AND c.power_curve_1 = d.turbine_id
                LEFT JOIN diffusion_resource_wind.wind_resource_annual e
                    	ON a.i = e.i
                    	AND a.j = e.j
                    	AND a.cf_bin = e.cf_bin
                    	AND b.turbine_height_m = e.height
                    	AND c.power_curve_2 = e.turbine_id;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    df = pd.concat(df_list, axis = 0, ignore_index = True)
    
    return df

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_annual_resource_solar(con, schema, sectors):
    
    inputs = locals().copy()
    
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT 'solar'::VARCHAR(5) as tech,
                '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                a.county_id, a.bin_id,
                b.naep 
                FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                LEFT JOIN diffusion_resource_solar.solar_resource_annual b
                    ON a.solar_re_9809_gid = b.solar_re_9809_gid
                    AND a.tilt = b.tilt
                    AND a.azimuth = b.azimuth;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    df = pd.concat(df_list, axis = 0, ignore_index = True)
        
    return df
    
    
#%%
def apply_technology_performance_solar(resource_solar_df, tech_performance_solar_df):
    
    resource_solar_df = pd.merge(resource_solar_df, tech_performance_solar_df, how = 'left', on = ['tech'])
    resource_solar_df['naep'] = resource_solar_df['naep'] * resource_solar_df['pv_efficiency_improvement_factor']
    
    return resource_solar_df

#%%
def apply_technology_performance_wind(resource_wind_df, tech_performance_wind_df):
    
    resource_wind_df = pd.merge(resource_wind_df, tech_performance_wind_df, how = 'left', on = ['tech', 'turbine_size_kw'])
    resource_wind_df['naep_1'] = resource_wind_df['naep_1'] * resource_wind_df['wind_derate_factor']
    resource_wind_df['naep_2'] = resource_wind_df['naep_2'] * resource_wind_df['wind_derate_factor']
    resource_wind_df['naep'] = resource_wind_df['power_curve_interp_factor'] * (resource_wind_df['naep_2'] - resource_wind_df['naep_1']) + resource_wind_df['naep_1']
    
    return resource_wind_df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def size_systems_wind(dataframe, system_sizing_targets_df, resource_df):
    
    in_cols = list(dataframe.columns)
       
    # TODO: this will be inefficient during parallelization
    wind_df = dataframe[dataframe['tech'] == 'wind']
    nonwind_df = dataframe[dataframe['tech'] <> 'wind']
    
    # join in system sizing targets df
    wind_df = pd.merge(wind_df, system_sizing_targets_df, how = 'left', on = ['sector_abbr', 'tech'])    
    
    # determine whether NEM is available in the state and sector
    wind_df['ur_enable_net_metering'] = wind_df['nem_system_size_limit_kw'] == 0
    
    # set the target kwh according to NEM availability
    wind_df['target_kwh'] = np.where(wind_df['ur_enable_net_metering'] == True, 
                                       wind_df['load_kwh_per_customer_in_bin'] * wind_df['sys_size_target_no_nem'],
                                       wind_df['load_kwh_per_customer_in_bin'] * wind_df['sys_size_target_nem'])
    # also set the oversize limit according to NEM availability
    wind_df['oversize_limit_kwh'] = np.where(wind_df['ur_enable_net_metering'] == True, 
                                       wind_df['load_kwh_per_customer_in_bin'] * wind_df['sys_oversize_limit_no_nem'],
                                       wind_df['load_kwh_per_customer_in_bin'] * wind_df['sys_oversize_limit_nem'])

    # join in the resource data
    wind_df = pd.merge(wind_df, resource_df, how = 'left', on = ['tech', 'sector_abbr', 'county_id', 'bin_id'])

    # calculate the system generation from naep and turbine_size_kw    
    wind_df['aep_kwh'] = wind_df['turbine_size_kw'] * wind_df['naep']

    # initialize values for score and nturb
    wind_df['score'] = np.absolute(wind_df['aep_kwh'] - wind_df['target_kwh'])
    wind_df['nturb'] = 1.
    
    # Handle Special Cases
    
    # Buildings requiring more electricity than can be generated by the largest turbine (1.5 MW)
    # Return very low rank score and the optimal continuous number of turbines
    big_projects = (wind_df['turbine_size_kw'] == 1500) & (wind_df['aep_kwh'] < wind_df['target_kwh'])
    wind_df.loc[big_projects, 'score'] = 0
    wind_df.loc[big_projects, 'nturb'] = np.minimum(4, wind_df['target_kwh'] / wind_df['aep_kwh']) 


    # identify oversized projects
    oversized_turbines = wind_df['aep_kwh'] > wind_df['oversize_limit_kwh']
    # also identify zero production turbines
    no_kwh = wind_df['aep_kwh'] == 0
    # where either condition is true, set a high score and zero turbines
    wind_df.loc[oversized_turbines | no_kwh, 'score'] = np.array([1e8]) + wind_df['turbine_size_kw'] * 100 + wind_df['turbine_height_m']
    wind_df.loc[oversized_turbines | no_kwh, 'nturb'] = 0.0
    # also disable net metering
    wind_df.loc[oversized_turbines | no_kwh, 'ur_enable_net_metering'] = False
    
    # check that the system is within the net metering size limit
    over_nem_limit = wind_df['turbine_size_kw'] > wind_df['nem_system_size_limit_kw']
    wind_df.loc[over_nem_limit, 'score'] = wind_df['score'] * 2
    wind_df.loc[over_nem_limit, 'ur_enable_net_metering'] = False

    # for each agent, find the optimal turbine
    wind_df['rank'] = wind_df.groupby(['county_id', 'bin_id', 'sector_abbr'])['score'].rank(ascending = True, method = 'first')
    wind_df_sized = wind_df[wind_df['rank'] == 1]
    # add in the system_size_kw field
    wind_df_sized['system_size_kw'] = wind_df_sized['turbine_size_kw'] * wind_df_sized['nturb']
    # recalculate the aep based on the system size (instead of plain turbine size)
    wind_df_sized['aep'] = wind_df_sized['system_size_kw'] * wind_df_sized['naep']
    
    # note: ur_enable_net_metering is automatically returned because it is an input column
    return_cols = ['ur_enable_net_metering', 'score', 'nturb', 'aep', 'system_size_kw', 'turbine_height_m', 
                   'turbine_size_kw', 'power_curve_1', 'power_curve_2', 'power_curve_interp_factor', 'wind_derate_factor']
    out_cols = list(pd.unique(in_cols + return_cols))
    
    out_df = pd.concat([wind_df_sized[out_cols], nonwind_df], axis = 0, ignore_index = True)

    return out_df

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def size_systems_solar(dataframe, system_sizing_targets_df, resource_df, default_panel_size_sqft = 17.5):
    
    in_cols = list(dataframe.columns)
       
    # TODO: this will be inefficient during parallelization
    solar_df = dataframe[dataframe['tech'] == 'solar']
    nonsolar_df = dataframe[dataframe['tech'] <> 'solar']    
    
    # join in system sizing targets df
    solar_df = pd.merge(solar_df, system_sizing_targets_df, how = 'left', on = ['sector_abbr', 'tech'])     
    
    # join in the resource data
    solar_df = pd.merge(solar_df, resource_df, how = 'left', on = ['tech', 'sector_abbr', 'county_id', 'bin_id'])
 
    solar_df['max_buildable_system_kw'] =  0.001 * solar_df['developable_roof_sqft'] * solar_df['pv_density_w_per_sqft']

    # initialize the system size targets
    solar_df['ideal_system_size_kw_no_nem'] = solar_df['load_kwh_per_customer_in_bin'] * solar_df['sys_size_target_no_nem']/solar_df['naep']
    solar_df['ideal_system_size_kw_nem'] = solar_df['load_kwh_per_customer_in_bin'] * solar_df['sys_size_target_nem']/solar_df['naep'] 
    
    # deal with special cases: no net metering, unlimited NEM, limited NEM
    no_net_metering = solar_df['nem_system_size_limit_kw'] == 0
    unlimited_net_metering = solar_df['nem_system_size_limit_kw'] == float('inf')
    solar_df['ideal_system_size_kw'] = np.where(no_net_metering, 
                                                solar_df['ideal_system_size_kw_no_nem'],
                                                np.where(unlimited_net_metering, 
                                                         solar_df['ideal_system_size_kw_nem'],
                                                         np.minimum(solar_df['ideal_system_size_kw_nem'], solar_df['nem_system_size_limit_kw']) # if limited NEM, maximize size up to the NEM limit
                                                         )
                                                )
    # change NEM enabled accordingly
    solar_df['ur_enable_net_metering'] = np.where(no_net_metering, False, True)
                                             
    # calculate the system size based on the target size and the availabile roof space
    solar_df['system_size_kw'] = np.round(np.minimum(solar_df['max_buildable_system_kw'], solar_df['ideal_system_size_kw']), 2)                      
    # derive the number of panels
    solar_df['npanels'] = solar_df['system_size_kw']/(0.001 * solar_df['pv_density_w_per_sqft'] * default_panel_size_sqft) # Denom is kW of a panel
    # calculate aep
    solar_df['aep'] = solar_df['system_size_kw'] * solar_df['naep']    

    out_cols = list(pd.unique(in_cols + ['ur_enable_net_metering', 'npanels', 'aep', 'system_size_kw']))
    
    out_df = pd.concat([solar_df[out_cols], nonsolar_df], axis = 0, ignore_index = True)

    return out_df   


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_normalized_load_profiles(con, schema, sectors):
    
    inputs = locals().copy()
    
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        a.county_id, a.bin_id, 
                        b.nkwh as consumption_hourly,
                        1e8 as scale_offset
                 FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                 LEFT JOIN diffusion_load_profiles.energy_plus_normalized_load_%(sector_abbr)s b
                     ON a.crb_model = b.crb_model
                     AND a.hdf_load_index = b.hdf_index;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    df = pd.concat(df_list, axis = 0, ignore_index = True)
            
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def scale_normalized_load_profiles(dataframe, load_df):
    
    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)
    # join the dataframe and load_df
    dataframe = pd.merge(dataframe, load_df, how  = 'left', on = ['county_id', 'bin_id', 'sector_abbr'])
    # apply the scale offset to convert values to float with correct precision
    dataframe = dataframe.apply(datfunc.scale_array_precision, axis = 1, args = ('consumption_hourly', 'scale_offset'))
    # scale the normalized profile to sum to the total load
    dataframe = dataframe.apply(datfunc.scale_array_sum, axis = 1, args = ('consumption_hourly', 'load_kwh_per_customer_in_bin'))    
    
    # subset to only the desired output columns
    out_cols = in_cols + ['consumption_hourly']
    
    dataframe = dataframe[out_cols]
    
    return dataframe
    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_normalized_hourly_resource_solar(con, schema, sectors):
    
    inputs = locals().copy()
    
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT 'solar'::VARCHAR(5) as tech,
                        '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        a.county_id, a.bin_id,
                        b.cf as generation_hourly,
                        1e6 as scale_offset
                FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                LEFT JOIN diffusion_resource_solar.solar_resource_hourly b
                    ON a.solar_re_9809_gid = b.solar_re_9809_gid
                    AND a.tilt = b.tilt
                    AND a.azimuth = b.azimuth;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    df = pd.concat(df_list, axis = 0, ignore_index = True)
        
    return df


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_normalized_hourly_resource_wind(con, schema, sectors, cur, agents):
    
    inputs = locals().copy()
    
    # isolate the information from agents regarding the power curves and hub heights for each agent
    system_sizes_df = agents.dataframe[agents.dataframe['tech'] == 'wind'][['sector_abbr', 'county_id', 'bin_id', 'i', 'j', 'cf_bin', 'turbine_height_m', 'power_curve_1', 'power_curve_2']]
    system_sizes_df['turbine_height_m'] = system_sizes_df['turbine_height_m'].astype(np.int64)   
    system_sizes_df['power_curve_1'] = system_sizes_df['power_curve_1'].astype(np.int64)   
    system_sizes_df['power_curve_2'] = system_sizes_df['power_curve_2'].astype(np.int64)   
        
    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        # write the power curve(s) and turbine heights for each agent to postgres
        sql = """DROP TABLE IF EXISTS %(schema)s.agent_selected_turbines_%(sector_abbr)s;
                CREATE UNLOGGED TABLE %(schema)s.agent_selected_turbines_%(sector_abbr)s
                (
                    county_id integer,
                    bin_id integer,
                    i integer,
                    j integer,
                    cf_bin integer,
                    turbine_height_m integer,
                    power_curve_1 integer,
                    power_curve_2 integer
                );""" % inputs   
        cur.execute(sql)
        con.commit()
        
        system_sizes_sector_df = system_sizes_df[system_sizes_df['sector_abbr'] == sector_abbr][['county_id', 'bin_id', 'i', 'j', 'cf_bin', 'turbine_height_m', 'power_curve_1', 'power_curve_2']]
        system_sizes_sector_df['turbine_height_m'] = system_sizes_sector_df['turbine_height_m'].astype(np.int64)
        
        s = StringIO()
        # write the data to the stringIO
        system_sizes_sector_df.to_csv(s, index = False, header = False)
        # seek back to the beginning of the stringIO file
        s.seek(0)
        # copy the data from the stringio file to the postgres table
        cur.copy_expert('COPY %(schema)s.agent_selected_turbines_%(sector_abbr)s FROM STDOUT WITH CSV' % inputs, s)
        # commit the additions and close the stringio file (clears memory)
        con.commit()    
        s.close()
        
        # add primary key
        sql = """ALTER TABLE %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 ADD PRIMARY KEY (county_id, bin_id);""" % inputs        
        cur.execute(sql)
        con.commit()
        
        # add indices
        sql = """CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_i
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(i);
                 
                 CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_j
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(j);
                 
                 CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_cf_bin
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(cf_bin);
                 
                 CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_turbine_height_m
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(turbine_height_m);
                 
                 CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_power_curve_1
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(power_curve_1);
                 
                 CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_power_curve_2
                 ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                 USING BTREE(power_curve_2);""" % inputs
        cur.execute(sql)
        con.commit()

        sql = """SELECT 'wind'::VARCHAR(5) as tech,
                        '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        a.county_id, a.bin_id,
                        COALESCE(b.cf, array_fill(1, array[8760])) as generation_hourly_1,
                        COALESCE(c.cf, array_fill(1, array[8760])) as generation_hourly_2,
                        1e3 as scale_offset
                FROM %(schema)s.agent_selected_turbines_%(sector_abbr)s a
                LEFT JOIN diffusion_resource_wind.wind_resource_hourly b
                    ON a.i = b.i
                    	AND a.j = b.j
                    	AND a.cf_bin = b.cf_bin
                    	AND a.turbine_height_m = b.height
                    	AND a.power_curve_1 = b.turbine_id
                LEFT JOIN diffusion_resource_wind.wind_resource_hourly c
                    ON a.i = c.i
                    	AND a.j = c.j
                    	AND a.cf_bin = c.cf_bin
                    	AND a.turbine_height_m = c.height
                    	AND a.power_curve_1 = c.turbine_id;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float = False)
        df_list.append(df_sector)
        
    df = pd.concat(df_list, axis = 0, ignore_index = True)
        
    return df    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_normalized_hourly_resource_solar(dataframe, hourly_resource_df):
    
    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)     
    
    # subdivide wind and other techs
    solar_df = dataframe[dataframe['tech'] == 'solar']
    nonsolar_df = dataframe[dataframe['tech'] <> 'solar']
    
    # join resource data to dataframe
    nonsolar_df = pd.merge(nonsolar_df, hourly_resource_df, how = 'left', on = ['sector_abbr', 'tech', 'county_id', 'bin_id'])
    # apply the scale offset to convert values to float with correct precision
    nonsolar_df = nonsolar_df.apply(datfunc.scale_array_precision, axis = 1, args = ('generation_hourly', 'scale_offset'))
    # scale the normalized profile by the system size
    nonsolar_df = nonsolar_df.apply(datfunc.scale_array_sum, axis = 1, args = ('generation_hourly', 'aep'))    
    # subset to only the desired output columns
    out_cols = in_cols + ['generation_hourly']
    nonsolar_df = nonsolar_df[out_cols]    
    
    # concatenate with other techs
    dataframe = pd.concat([solar_df, nonsolar_df], axis = 0, ignore_index = False)

    return dataframe


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_normalized_hourly_resource_wind(dataframe, hourly_resource_df):
    
    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)     
    
    # subdivide wind and other techs
    wind_df = dataframe[dataframe['tech'] == 'wind']
    nonwind_df = dataframe[dataframe['tech'] <> 'wind']
    
    # join resource data to dataframe
    wind_df = pd.merge(wind_df, hourly_resource_df, how = 'left', on = ['sector_abbr', 'tech', 'county_id', 'bin_id'])
    # apply the scale offset to convert values to float with correct precision
    wind_df = wind_df.apply(datfunc.scale_array_precision, axis = 1, args = ('generation_hourly_1', 'scale_offset'))
    wind_df = wind_df.apply(datfunc.scale_array_precision, axis = 1, args = ('generation_hourly_2', 'scale_offset'))    
    # interpolate power curves
    wind_df = wind_df.apply(datfunc.interpolate_array, axis = 1, args = ('generation_hourly_1', 'generation_hourly_2', 'power_curve_interp_factor', 'generation_hourly'))
    # scale the normalized profile by the system size
    wind_df = wind_df.apply(datfunc.scale_array_sum, axis = 1, args = ('generation_hourly', 'aep'))    
    # subset to only the desired output columns
    out_cols = in_cols + ['generation_hourly']
    wind_df = wind_df[out_cols]  
    
    # make sure column order matches between two dataframes
    nonwind_df = nonwind_df[out_cols]
    
    # concatenate with other techs
    dataframe = pd.concat([wind_df, nonwind_df], axis = 0, ignore_index = False)

    return dataframe
    
    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
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
    df = pd.read_sql(sql, con, coerce_float = False)

    return df
    

#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def get_technology_costs_wind(con, schema, year):
    
    inputs = locals().copy()
    
    sql = """SELECT 'wind'::VARCHAR(5) as tech,
                    a.turbine_size_kw,
                    a.turbine_height_m,
                    a.installed_costs_dollars_per_kw,
                    a.fixed_om_dollars_per_kw_per_yr,
                    a.variable_om_dollars_per_kwh
                FROM %(schema)s.turbine_costs_per_size_and_year a       
            WHERE a.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float = False)

    return df
    
    
#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_tech_costs_solar(dataframe, tech_costs_df):    
    
    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)
    # join the data
    dataframe = pd.merge(dataframe, tech_costs_df, how = 'left', on = ['tech', 'sector_abbr'])
    # apply the capital cost multipliers and size adjustment factor
    dataframe['inverter_cost_dollars_per_kw'] = (dataframe['inverter_cost_dollars_per_kw'] * dataframe['cap_cost_multiplier'] * 
                                                    (1 - (dataframe['pv_size_adjustment_factor'] * dataframe['pv_base_size_kw'] - dataframe['system_size_kw'])))
    dataframe['installed_costs_dollars_per_kw'] = (dataframe['installed_costs_dollars_per_kw'] * dataframe['cap_cost_multiplier'] * 
                                                    (1 - (dataframe['pv_size_adjustment_factor'] * dataframe['pv_base_size_kw'] - dataframe['system_size_kw'])))                                                    
    # identify the new columns to return
    return_cols = ['inverter_cost_dollars_per_kw', 'installed_costs_dollars_per_kw', 'fixed_om_dollars_per_kw_per_yr', 'variable_om_dollars_per_kwh']    
    out_cols = in_cols + return_cols

    dataframe = dataframe[out_cols]

    return dataframe                                                


#%%
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def apply_tech_costs_wind(dataframe, tech_costs_df):    

    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)    
    # join the data
    dataframe = pd.merge(dataframe, tech_costs_df, how = 'left', on = ['tech', 'turbine_size_kw', 'turbine_height_m'])  
    # fill nas (these occur where system size is zero)
    dataframe['installed_costs_dollars_per_kw'] = dataframe['installed_costs_dollars_per_kw'].fillna(0)
    dataframe['fixed_om_dollars_per_kw_per_yr'] = dataframe['fixed_om_dollars_per_kw_per_yr'].fillna(0)
    dataframe['variable_om_dollars_per_kwh'] = dataframe['variable_om_dollars_per_kwh'].fillna(0)
    # apply the capital cost multipliers to the installed costs
    dataframe['installed_costs_dollars_per_kw'] = dataframe['installed_costs_dollars_per_kw'] * dataframe['cap_cost_multiplier']
    
    # add an empty column for the inteverter costs (for compatibility with solar)
    dataframe['inverter_cost_dollars_per_kw'] = np.nan
    
    # identify the new columns to return
    return_cols = ['inverter_cost_dollars_per_kw', 'installed_costs_dollars_per_kw', 'fixed_om_dollars_per_kw_per_yr', 'variable_om_dollars_per_kwh']    
    out_cols = in_cols + return_cols

    dataframe = dataframe[out_cols]

    return dataframe
        
#%%
def check_agent_count():
  # TODO: add in a check that agent_core_attributes_ table has the correct number of rows
        # this should be called every time get_agents() is run
    pass

#%% TODO LIST

# ~~~IMMEDIATE~~~


# ~~~UP NEXT~~~
# TODO: add indices to agent_core_attributes for subsequent steps
    # county_id
    # pgid
# TODO: continue with the rest of the functions outlined above, 
        # to reproduce the functionality of the old generate customer bins 
        # in a more modular form


# ~~~LONG TERM~~~
# TODO: Figure out how to make collection of hourly array data (resource and consumption) more efficient
# TODO: get logger working on both data_functions and agent_preparation
# TODO: strip cap cost multipliers from agent locations and move to somewhere downstream
# TODO: Remove RECS/CBECS as option for rooftop characteristics from input sheet and database