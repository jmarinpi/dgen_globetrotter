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
def p_run(pg_conn_string, sql, chunks, pool):
           
    num_workers = pool._processes
    result_list = []
    for i in xrange(num_workers):

        place_holders = {'i': i, 'ids': utilfunc.pylist_2_pglist(chunks[i])}
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
@decorators.fn_timer(logger = logger, tab_level = 0, prefix = '')
def generate_core_agent_attributes(cur, con, techs, schema, sample_pct, min_agents, agents_per_region, sectors,
                                            pg_procs, pg_conn_string, seed):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    
    # break counties into subsets for parallel processing
    chunks, pg_procs = split_tracts(cur, schema, pg_procs)        

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
                calculate_number_of_agents_by_tract(schema, sector_abbr, chunks, sample_pct, min_agents, seed, pool, pg_conn_string)                    
                sample_blocks(schema, sector_abbr, chunks, seed, pool, pg_conn_string, con, cur)
                sample_building_type(schema, sector_abbr, chunks, seed, pool, pg_conn_string)
                # TODO: add in selection of heating fuel type for res sector
                sample_building_microdata(schema, sector_abbr, chunks, seed, pool, pg_conn_string)
                estimate_agent_thermal_loads(schema, sector_abbr, chunks, pool, pg_conn_string)
                estimate_system_ages(schema, sector_abbr, chunks, seed, pool, pg_conn_string)
                estimate_system_lifetimes(schema, sector_abbr, chunks, seed, pool, pg_conn_string)
                map_to_generic_baseline_system(schema, sector_abbr, chunks, pool, pg_conn_string)
                                        
                #==============================================================================
                #     impose agent level siting  attributes (i.e., "tech potential")
                #==============================================================================
                # TODO: add this for ghp, similar to wind

    
                #==============================================================================
                #     combine all pieces into a single table
                #==============================================================================
                combine_all_attributes(chunks, pool, cur, con, pg_conn_string, schema, sector_abbr)
    
        #==============================================================================
        #     create a view that combines all sectors and techs
        #==============================================================================
        merge_all_core_agents(cur, con, schema, sectors, techs)

        #==============================================================================
        #    drop the intermediate tables
        #==============================================================================
        # TODO: update the list of tables to delete
        cleanup_intermediate_tables(schema, sectors, chunks, pg_conn_string, cur, con, pool)

    except:
        # roll back any transactions
        con.rollback()
        # re-raise the exception
        raise
        
    finally:
        # close the multiprocessing pool
        pool.close() 


#%%
def split_tracts(cur, schema, pg_procs):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()      
    
    # get list of counties
    sql =   """SELECT a.tract_id_alias
               FROM diffusion_blocks.tract_ids a
               INNER JOIN %(schema)s.states_to_model b
                   ON a.state_fips = b.state_fips
               ORDER BY a.tract_id_alias;""" % inputs
    cur.execute(sql)
    tracts = [row['tract_id_alias'] for row in cur.fetchall()]
    
    if len(tracts) > pg_procs:
        chunks = map(list,np.array_split(tracts, pg_procs))
    else:
        chunks = [tracts]
        pg_procs = 1
    
    return chunks, pg_procs


#%%
def calculate_number_of_agents_by_tract(schema, sector_abbr, chunks, sample_pct, min_agents, seed, pool, pg_conn_string):

    msg = '\tDetermining Number of Agents in Each Tract'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'
    inputs['sample_pct'] = sample_pct
    inputs['min_agents'] = min_agents
    inputs['sector_abbr'] = sector_abbr   
    
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s AS
            	SELECT a.tract_id_alias, a.bldg_count_%(sector_abbr)s as tract_bldg_count,
            		 CASE WHEN ROUND(a.bldg_count_%(sector_abbr)s * %(sample_pct)s, 0)::INTEGER < %(min_agents)s
                               THEN %(min_agents)s
                          ELSE ROUND(a.bldg_count_%(sector_abbr)s * %(sample_pct)s, 0)::INTEGER
                      END AS n_agents
            	FROM diffusion_blocks.tract_building_count_by_sector a
            	WHERE tract_id_alias in (%(chunk_place_holder)s);""" % inputs
    
    p_run(pg_conn_string, sql, chunks, pool)
    
    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (tract_id_alias);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)             
  
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def sample_blocks(schema, sector_abbr, chunks, seed, pool, pg_conn_string, con, cur):

    msg = '\tSampling from Blocks for Each Tract'
    logger.info(msg)
    
    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'
    inputs['sector_abbr'] = sector_abbr    
    
    #==============================================================================
    #     randomly sample  N blocks from each county 
    #==============================================================================    
    # create a sequence that will be used to populate a new primary key across all table partitions
    # using a sequence ensure ids will be unique across all partitioned tables
    sql = """DROP SEQUENCE IF EXISTS %(schema)s.agent_id_%(sector_abbr)s_sequence;
            CREATE SEQUENCE %(schema)s.agent_id_%(sector_abbr)s_sequence
            INCREMENT 1
            START 1;""" % inputs
    cur.execute(sql)
    con.commit()
    
    # (note: [this may not be true any longer...] some counties will have fewer than N points, in which case, all are returned) 
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s AS
             WITH a as
             (
                 SELECT a.tract_id_alias,
                         array_agg(a.pgid ORDER BY a.pgid) as pgids,
                         array_agg(a.sample_weight ORDER BY a.pgid) as block_weights
                 FROM %(schema)s.block_microdata_%(sector_abbr)s_joined a
                 WHERE a.tract_id_alias in (%(chunk_place_holder)s)
                 GROUP BY a.tract_id_alias

             )
             SELECT nextval('%(schema)s.agent_id_%(sector_abbr)s_sequence') as agent_id, 
                     a.tract_id_alias, 
                        unnest(diffusion_shared.sample(a.pgids, 
                                                       b.n_agents, 
                                                       %(seed)s, 
                                                       True, 
                                                       a.block_weights)
                                                       ) as pgid
            FROM a
            LEFT JOIN %(schema)s.agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s b
                ON a.tract_id_alias = b.tract_id_alias;""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)
    
    # add indices
    sql = """CREATE INDEX agent_blocks_%(sector_abbr)s_%(i_place_holder)s_pgid_btree 
            ON %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(pgid);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def sample_building_type(schema, sector_abbr, chunks, seed, pool, pg_conn_string):

    msg = '\tSampling Building Types from Blocks for Each Tract'
    logger.info(msg)
    
    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'
    inputs['sector_abbr'] = sector_abbr    
    
    #==============================================================================
    #     randomly sample  N blocks from each county 
    #==============================================================================    
    # (note: [this may not be true any longer...] some counties will have fewer than N points, in which case, all are returned) 
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s AS
             SELECT a.agent_id, a.tract_id_alias, b.census_division_abbr, b.reportable_domain, -- need these two fields for subsequent microdata step
                        unnest(diffusion_shared.sample(c.bldg_types, 
                                                       1, 
                                                       a.agent_id * %(seed)s,  -- ensure unique sample for each block
                                                       True, 
                                                       b.bldg_probs_%(sector_abbr)s)
                                                       ) as bldg_type,
                        unnest(diffusion_shared.sample(b.bldg_probs_%(sector_abbr)s, 
                                                       1, 
                                                       a.agent_id * %(seed)s,  -- ensure unique sample for each block
                                                       True, 
                                                       b.bldg_probs_%(sector_abbr)s)
                                                       ) as block_bldgs_weight
            FROM %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s a
            LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                ON a.pgid = b.pgid
            lEFT JOIN diffusion_blocks.bldg_type_arrays c
                ON c.sector_abbr = '%(sector_abbr)s';""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)
    
    # add indices
    sql = """CREATE INDEX agent_building_types_%(sector_abbr)s_%(i_place_holder)s_bldg_type_btree 
            ON %(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(bldg_type);
            
            CREATE INDEX agent_building_types_%(sector_abbr)s_%(i_place_holder)s_reportable_domain_btree 
            ON %(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(reportable_domain);

            CREATE INDEX agent_building_types_%(sector_abbr)s_%(i_place_holder)s_census_division_abbr_btree 
            ON %(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(census_division_abbr);            
            
            CREATE INDEX agent_building_types_%(sector_abbr)s_%(i_place_holder)s_tract_id_alias_btree 
            ON %(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(tract_id_alias);      
            """ % inputs
    p_run(pg_conn_string, sql, chunks, pool)
    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def sample_building_microdata(schema, sector_abbr, chunks, seed, pool, pg_conn_string):

    msg = "\tSampling from Building Microdata"
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['sector_abbr'] = sector_abbr
    if sector_abbr == 'res':
        inputs['eia_join_clause'] = """ b.eia_type = c.typehuq
                                    AND b.min_tenants <= c.num_tenants
                                    AND b.max_tenants >= c.num_tenants 
                                    AND a.reportable_domain = c.reportable_domain
                                    AND c.sector_abbr = '%(sector_abbr)s' """ % inputs

    else:
        inputs['eia_join_clause'] = """ b.eia_type = c.pbaplus
                                    AND a.census_division_abbr = c.census_division_abbr 
                                    AND c.sector_abbr = '%(sector_abbr)s' """  % inputs


    sql =  """DROP TABLE IF EXISTS %(schema)s.agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s;
         CREATE UNLOGGED TABLE %(schema)s.agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s AS
         WITH all_bldgs AS
         (
             SELECT a.agent_id, 
                     c.building_id as eia_bldg_id, c.sample_wt::NUMERIC as eia_bldg_weight
             FROM %(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s a
             LEFT JOIN diffusion_shared.cdms_to_eia_lkup b
                 ON a.bldg_type = b.cdms
             lEFT JOIN diffusion_shared.cbecs_recs_expanded_combined c
                	ON %(eia_join_clause)s
        ),
        sampled_bldgs AS 
        (
            SELECT a.agent_id, 
                    unnest(diffusion_shared.sample(array_agg(a.eia_bldg_id ORDER BY a.eia_bldg_id), 
                                                   1, 
                                                   %(seed)s * a.agent_id, -- ensures unique sample for each agent 
                                                   True, 
                                                   array_agg(a.eia_bldg_weight ORDER BY a.eia_bldg_id))
                                                   ) as eia_bldg_id
            FROM all_bldgs a
            GROUP BY a.agent_id
        )
        SELECT  a.agent_id, a.eia_bldg_id,
                b.sample_wt as eia_bldg_weight,
                b.climate_zone, b.pba, b.pbaplus, b.typehuq, b.roof_material, b.owner_occupied, b.kwh, b.year_built, b.single_family_res,
                b.num_tenants, b.num_floors, b.space_heat_equip, b.space_heat_fuel, b.space_heat_age_min, b.space_heat_age_max, 
                b.water_heat_equip, b.water_heat_fuel, b.water_heat_age_min, b.water_heat_age_max, b.space_cool_equip, b.space_cool_fuel,
                b.space_cool_age_min, b.space_cool_age_max, b.ducts, b.totsqft, b.totsqft_heat, b.totsqft_cool, b.kbtu_space_heat,
                b.kbtu_space_cool, b.kbtu_water_heat, b.crb_model, b.roof_style, b.roof_sqft
        FROM sampled_bldgs a
        LEFT JOIN diffusion_shared.cbecs_recs_expanded_combined b
            ON a.eia_bldg_id = b.building_id
            AND b.sector_abbr = '%(sector_abbr)s';""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)
    

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)
    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def estimate_agent_thermal_loads(schema, sector_abbr, chunks, pool, pg_conn_string):

    msg = '\tEstimating Agent Thermal Loads'    
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['sector_abbr'] = sector_abbr  



    sql = """DROP TABLE IF EXISTS %(schema)s.agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s AS
            WITH b as
            (
                SELECT  a.agent_id, 
                        a.block_bldgs_weight::NUMERIC/sum(a.block_bldgs_weight) OVER (PARTITION BY a.tract_id_alias) * b.tract_bldg_count as buildings_in_bin
                FROM %(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s a
                LEFT JOIN %(schema)s.agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s b
			ON a.tract_id_alias = b.tract_id_alias
            ),
            c as
            (
    
                SELECT a.agent_id,
                           b.buildings_in_bin,
                           (b.buildings_in_bin * a.kbtu_space_heat)/sum(b.buildings_in_bin * a.kbtu_space_heat) OVER (PARTITION BY d.old_county_id) 
                               * e.space_heating_thermal_load_mmbtu * 1000. as space_heat_kbtu_in_bin,
                           (b.buildings_in_bin * a.kbtu_space_cool)/sum(b.buildings_in_bin * a.kbtu_space_cool) OVER (PARTITION BY d.old_county_id) 
                               * e.space_cooling_thermal_load_mmbtu * 1000. as space_cool_kbtu_in_bin,
                           (b.buildings_in_bin * a.kbtu_water_heat)/sum(b.buildings_in_bin * a.kbtu_water_heat) OVER (PARTITION BY d.old_county_id) 
                               * e.water_heating_thermal_load_mmbtu * 1000. as water_heat_kbtu_in_bin,
                            a.totsqft

                 FROM %(schema)s.agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s a
                 LEFT JOIN b
                     ON a.agent_id = b.agent_id         
                 LEFT JOIN  %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s c
                     ON a.agent_id = c.agent_id
                 LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined d
                     ON c.pgid = d.pgid
                 LEFT JOIN diffusion_shared.county_thermal_demand_%(sector_abbr)s e
                     ON d.old_county_id = e.county_id
            )
            SELECT agent_id, buildings_in_bin, totsqft,
                   space_heat_kbtu_in_bin, space_cool_kbtu_in_bin, water_heat_kbtu_in_bin,
                   space_heat_kbtu_in_bin/buildings_in_bin as space_heat_kbtu_per_building_in_bin,
                   space_cool_kbtu_in_bin/buildings_in_bin as space_cool_kbtu_per_building_in_bin,
                   water_heat_kbtu_in_bin/buildings_in_bin as water_heat_kbtu_per_building_in_bin
            FROM c;""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)
    
    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)    
       


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def estimate_system_ages(schema, sector_abbr, chunks, seed, pool, pg_conn_string):

    msg = '\tEstimating Agent HVAC System Agents'    
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['sector_abbr'] = sector_abbr  

    sql = """DROP TABLE IF EXISTS %(schema)s.agent_system_ages_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.agent_system_ages_%(sector_abbr)s_%(i_place_holder)s AS
            WITH a as
            (
                SELECT agent_id,             
                    CASE WHEN a.space_heat_age_min IS NULL OR a.space_heat_age_max IS NULL THEN NULL::INTEGER
                    ELSE ROUND(diffusion_shared.r_runif(a.space_heat_age_min, a.space_heat_age_max, 1, %(seed)s * agent_id), 0)::INTEGER
                    END as space_heat_system_age, 
    
                    CASE WHEN a.space_cool_age_min IS NULL OR a.space_cool_age_max IS NULL THEN NULL::INTEGER
                    ELSE ROUND(diffusion_shared.r_runif(a.space_cool_age_min, a.space_cool_age_max, 1, %(seed)s * agent_id), 0)::INTEGER
                    END as space_cool_system_age
            
                FROM %(schema)s.agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s a
            )
            SELECT agent_id, space_heat_system_age, space_cool_system_age,
                    r_median(ARRAY[space_heat_system_age, space_cool_system_age]) as average_system_age
            FROM a;""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)    

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_system_ages_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)    

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def estimate_system_lifetimes(schema, sector_abbr, chunks, seed, pool, pg_conn_string):

    msg = '\tEstimating Agent HVAC System Expected Lifetimes'    
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['sector_abbr'] = sector_abbr  

    sql = """DROP TABLE IF EXISTS %(schema)s.agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s AS
            WITH a as
            (
            SELECT a.agent_id,     
                CASE WHEN space_heat_equip = 'none' THEN NULL::INTEGER
                ELSE ROUND(diffusion_shared.r_rnorm_rlnorm(b.mean, b.std, b.dist_type, %(seed)s * agent_id), 0)::INTEGER
                END as space_heat_system_expected_lifetime,

                CASE WHEN space_cool_equip = 'none' THEN NULL::INTEGER
                ELSE ROUND(diffusion_shared.r_rnorm_rlnorm(b.mean, b.std, b.dist_type, %(seed)s * agent_id), 0)::INTEGER
                END as space_cool_system_expected_lifetime
                
            FROM  %(schema)s.agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s a
            CROSS JOIN distributions b
            ),
            heat_join as
            (
                SELECT a.agent_id, a.space_heat_equip, a.space_heat_fuel, a.space_heat_system_age,
                        b.mean as space_heat_system_age_mean, b.std as space_heat_system_age_std,
                        b.dist_type as space_heat_system_age_dist_type,
                        r_median(ARRAY[a.space_heat_system_age, a.space_cool_system_age]) as average_system_age
                FROM a
                LEFT JOIN diffusion_geo.hvac_life_expectancy b
                ON a.space_heat_equip = b.space_equip and a.space_heat_fuel = b.space_fuel
                WHERE b.sector_abbr = %(sector)s
            ),
            cool_join as
            (
                SELECT a.agent_id, a.space_cool_equip, a.space_cool_system_age, a.space_cool_fuel,
                        b.mean as space_cool_system_age_mean, b.std as space_cool_system_age_std,
                        b.dist_type as space_cool_system_age_dist_type
                FROM a
                LEFT JOIN diffusion_geo.hvac_life_expectancy b
                ON a.space_cool_equip = b.space_equip and a.space_cool_fuel = b.space_fuel
                WHERE b.sector_abbr = %(sector)s
            ),
            SELECT a.agent_id, a.space_heat_equip, a.space_heat_fuel, a.space_heat_system_age,
                   a.space_heat_system_age_mean, a.space_heat_system_age_std,
                   a.dist_type as space_heat_system_age_dist_type,
                   b.space_cool_equip, b.space_cool_system_age, b.space_cool_fuel,
                   b.space_cool_system_age_mean, b.space_cool_system_age_std,
                   b.space_cool_system_age_dist_type,
                   a.average_system_age
            FROM heat_join a
            LEFT JOIN cool_join b
            ON a.agent_id = b.agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)    

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)    
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def map_to_generic_baseline_system(schema, sector_abbr, chunks, pool, pg_conn_string):
    
    msg = '\tMapping HVAC Systems to Generic Baseline Types'    
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['sector_abbr'] = sector_abbr  
    
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_system_baseline_types_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.agent_system_baseline_types_%(sector_abbr)s_%(i_place_holder)s AS
            SELECT a.agent_id, 'type 1'::VARCHAR(6) as baseline_system_type
            FROM %(schema)s.agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s a;""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)
    
    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_system_baseline_types_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)    
    


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def combine_all_attributes(chunks, pool, cur, con, pg_conn_string, schema, sector_abbr):

    msg = "\tCombining All Core Agent Attributes"
    logger.info(msg)
    
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'

    
    sql_part = """SELECT a.agent_id,
                        -- block attributes
                    	b.pgid,
                    	b.county_id,
                    	b.state_abbr,
                    	b.state_fips,
                    	b.county_fips,
                    	b.tract_fips,
                    	b.tract_id_alias,
                    	b.old_county_id,
                    	b.census_division_abbr,
                    	b.census_region,
                    	b.reportable_domain,
                    	b.pca_reg,
                    	b.reeds_reg,
                    	b.acres_per_bldg,
                    	c.bldg_type as hazus_bldg_type,
                     
                         -- thermal load
                    	d.buildings_in_bin,
                    	ROUND(d.space_heat_kbtu_in_bin::NUMERIC, 0) as space_heat_kbtu_in_bin,
                    	ROUND(d.space_cool_kbtu_in_bin::NUMERIC, 0) as space_cool_kbtu_in_bin,
                    	ROUND(d.water_heat_kbtu_in_bin::NUMERIC, 0) as water_heat_kbtu_in_bin,
                    	ROUND(d.space_heat_kbtu_per_building_in_bin::NUMERIC, 0) as space_heat_kbtu_per_building_in_bin,
                    	ROUND(d.space_cool_kbtu_per_building_in_bin::NUMERIC, 0) as space_cool_kbtu_per_building_in_bin,
                    	ROUND(d.water_heat_kbtu_per_building_in_bin::NUMERIC, 0) as water_heat_kbtu_per_building_in_bin,
                     
                         -- system ages
                    	e.space_heat_system_age,
                    	e.space_cool_system_age,
                    	e.average_system_age,
                     
                         -- system lifetimes
                    	f.space_heat_system_expected_lifetime,
                    	f.space_cool_system_expected_lifetime,
                    	f.average_system_expected_lifetime,
                     
                         -- baseline system type
                    	g.baseline_system_type,
                     
                         -- eia microdata attributes
                    	h.eia_bldg_id,
                    	h.eia_bldg_weight,
                    	h.climate_zone,
                    	h.pba,
                    	h.pbaplus,
                    	h.typehuq,
                    	h.owner_occupied,
                    	h.year_built,
                    	h.single_family_res,
                    	h.num_tenants,
                    	h.num_floors,
                    	h.space_heat_equip,
                    	h.space_heat_fuel,
                    	h.water_heat_equip,
                    	h.water_heat_fuel,
                    	h.space_cool_equip,
                    	h.space_cool_fuel,
                    	h.totsqft,
                    	h.totsqft_heat,
                    	h.totsqft_cool
                    FROM %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s a
                    LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                    ON a.pgid = b.pgid
                    LEFT JOIN %(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s c
                    ON a.agent_id = c.agent_id
                    LEFT JOIN %(schema)s.agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s d
                    ON a.agent_id = d.agent_id
                    LEFT JOIN %(schema)s.agent_system_ages_%(sector_abbr)s_%(i_place_holder)s e
                    ON a.agent_id = e.agent_id
                    LEFT JOIN %(schema)s.agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s f
                    ON a.agent_id = f.agent_id
                    LEFT JOIN %(schema)s.agent_system_baseline_types_%(sector_abbr)s_%(i_place_holder)s g
                    ON a.agent_id = g.agent_id 
                    LEFT JOIN %(schema)s.agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s h
                    ON a.agent_id = h.agent_id """ % inputs
    
    # create the template table
    template_inputs = inputs.copy()
    template_inputs['i'] = 0
    template_inputs['sql_body'] = sql_part % template_inputs
    sql_template = """DROP TABLE IF EXISTS %(schema)s.agent_core_attributes_%(sector_abbr)s;
                      CREATE UNLOGGED TABLE %(schema)s.agent_core_attributes_%(sector_abbr)s AS
                      %(sql_body)s
                      LIMIT 0;""" % template_inputs
    cur.execute(sql_template)
    con.commit()
    
    # reconfigure sql into an insert statement
    inputs['sql_body'] = sql_part
    sql = """INSERT INTO %(schema)s.agent_core_attributes_%(sector_abbr)s
            %(sql_body)s;""" % inputs
    # run the insert statement
    p_run(pg_conn_string, sql, chunks, pool)
    
    # add primary key 
    sql =  """ALTER TABLE %(schema)s.agent_core_attributes_%(sector_abbr)s
              ADD PRIMARY KEY (agent_id);""" % inputs
    cur.execute(sql)
    con.commit()

    # create indices
    # TODO: add indices that are neeeded in subsequent steps?


    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def cleanup_intermediate_tables(schema, sectors, county_chunks, pg_conn_string, cur, con, pool):
    
    inputs = locals().copy()    
    inputs['i_place_holder'] = '%(i)s'
    
    #==============================================================================
    #   clean up intermediate tables
    #==============================================================================
    msg = "\tCleaning Up Intermediate Tables..."
    logger.info(msg)
    intermediate_tables = [ 
                            '%(schema)s.agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_building_types_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_system_ages_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s',
                            '%(schema)s.agent_system_baseline_types_%(sector_abbr)s_%(i_place_holder)s'
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
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def merge_all_core_agents(cur, con, schema, sectors, techs):
    
    inputs = locals().copy()    
    
    msg = "Merging All Agents into a Single Table View"
    logger.info(msg)    
    
    sql_list = []
    for sector_abbr, sector in sectors.iteritems():
        for tech in techs:
            inputs['sector_abbr'] = sector_abbr
            inputs['sector'] = sector
            inputs['tech'] = tech
            sql = """SELECT a.*,
                            -- replicate for each sector and tech
                            '%(sector_abbr)s'::CHARACTER VARYING(3) as sector_abbr,
                            '%(sector)s'::TEXT as sector,
                            '%(tech)s'::varchar(5) as tech
                    FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a """ % inputs
            sql_list.append(sql)
    
    inputs['sql_body'] = ' UNION ALL '.join(sql_list)
    sql = """DROP VIEW IF EXISTS %(schema)s.agent_core_attributes_all;
             CREATE VIEW %(schema)s.agent_core_attributes_all AS
             %(sql_body)s;""" % inputs
    cur.execute(sql)
    con.commit()
    
    




