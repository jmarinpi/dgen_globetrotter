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
from agent_preparation import p_run, create_agent_id_sequence

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


#%%
@decorators.fn_timer(logger=logger, tab_level=0, prefix='')
def generate_core_agent_attributes(cur, con, techs, schema, role, sample_pct, min_agents, agents_per_region, sectors,
                                   pg_procs, pg_conn_string, seed, end_year):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'

    # break counties into subsets for parallel processing
    county_chunks, pg_procs = split_counties(cur, schema, pg_procs)

    # create the pool of multiprocessing workers
    # (note: do this after splitting counties because, for small states, split_counties will adjust the number of pg_procs)
    pool = multiprocessing.Pool(processes=pg_procs)

    try:
        create_agent_id_sequence(schema, con, cur)
        # all in postgres
        for sector_abbr, sector in sectors.iteritems():
            with utilfunc.Timer() as t:
                logger.info("Creating Agents for %s Sector" % sector)

                #==============================================================
                #     sample from blocks and building microdata, convolve samples, and estimate
                #     max demand for each agent
                #==============================================================
                # NOTE: each of these functions is dependent on the last, so
                # changes from one must be cascaded to the others
                sample_blocks(schema,role, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string)
                add_agent_ids(schema,role, sector_abbr, 'initial', county_chunks, pool, pg_conn_string, con, cur)
                sample_building_microdata(schema,role, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string)
                convolve_block_and_building_samples(schema, role,sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string)
                sample_agent_utility_type(schema,role, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string)
                calculate_max_demand(schema, role,sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string)

                #==============================================================
                #     impose agent level siting  attributes (i.e., "tech potential")
                #==============================================================
                # SOLAR
                simulate_roof_characteristics(county_chunks, pool, pg_conn_string, con, schema, role,sector_abbr, seed)

                # WIND
                determine_allowable_turbine_heights(county_chunks, pool, pg_conn_string, schema,role, sector_abbr)
                find_potential_turbine_sizes(county_chunks, cur, con, pool, pg_conn_string, schema,role, sector_abbr)
                simulate_turbine_configurations(county_chunks, pool, cur, con, pg_conn_string, schema, role, sector_abbr)
                
                # combine attributes
                combine_all_attributes(county_chunks, pool, cur, con, pg_conn_string, schema, role, sector_abbr)
        #======================================================================
        #     create a view that combines all sectors and techs
        #======================================================================
        merge_all_core_agents(cur, con, schema, role,sectors, techs)

        #======================================================================
        #    drop the intermediate tables
        #======================================================================
        cleanup_intermediate_tables(schema,role, sectors, county_chunks, pg_conn_string, cur, con, pool)

    except:
        # roll back any transactions
        con.rollback()
        # re-raise the exception
        raise

    finally:
        # close the multiprocessing pool
        pool.close()


#%%
def split_counties(cur, schema, pg_procs):
    # create a dictionary out of the input arguments -- this is used through
    # sql queries
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
        county_chunks = map(list, np.array_split(counties, pg_procs))
    else:
        county_chunks = [counties]
        pg_procs = 1

    return county_chunks, pg_procs


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def sample_blocks(schema, role, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string):

    msg = '\tSampling from Blocks for Each County'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    #=========================================================================
    #     randomly sample N blocks from each county
    #=========================================================================
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

            SELECT  NULL::INTEGER agent_id,
                a.pgid,
                b.tract_id_alias,
                a.county_id,
                ROW_NUMBER() OVER (PARTITION BY a.county_id ORDER BY a.county_id, a.pgid) as bin_id
            FROM a
            LEFT JOIN diffusion_blocks.block_tract_id_alias b
            ON a.pgid = b.pgid
            ORDER BY a.county_id, a.pgid;""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # add indices
    sql = """CREATE INDEX agent_blocks_%(sector_abbr)s_%(i_place_holder)s_pgid_btree
            ON %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(pgid);""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)


#%%
# TODO -- make sure that this agent_id gets filtered through
@decorators.fn_timer(logger=logger, tab_level=3, prefix='')
def add_agent_ids(schema, role, sector_abbr, initial_or_new, chunks, pool, pg_conn_string, con, cur):
    inputs = locals().copy()

    # need to do this sequentially to ensure conssitent application of agent_ids
    # (if run in parallel, there is no guarantee the order in which each table will be hitting the sequence)
    for i in range(0, len(chunks)):
        inputs['i_place_holder'] = i
        sql = """UPDATE %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s
                 SET agent_id = nextval('%(schema)s.agent_id_sequence');""" % inputs
        cur.execute(sql)
        con.commit()

    # add primary key
    # (can do this part in parallel)

    # # reset i_place_holder to normal value
    # inputs['i_place_holder'] = '%(i)s'
    # # run query
    # sql = """ALTER TABLE %(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s
    #          ADD PRIMARY KEY (agent_id);""" % inputs
    # p_run(pg_conn_string, role, sql, chunks, pool)


#%%
# an agent belongs to
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def sample_building_microdata(schema, role, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string):

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

    #=========================================================================
    #    create lookup table with random values for each load bin
    #=========================================================================
    sql =  """DROP TABLE IF EXISTS %(schema)s.agent_bldgs_%(sector_abbr)s_%(i_place_holder)s;
         CREATE UNLOGGED TABLE %(schema)s.agent_bldgs_%(sector_abbr)s_%(i_place_holder)s AS
         WITH all_bldgs AS
         (
             SELECT  a.county_id,
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
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_bldgs_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def convolve_block_and_building_samples(schema, role, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string, step=3):

    msg = '\tConvolving Block and Building Samples'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'
    # for commercial customers, due to multi-tenant occupancy, use buildings
    # rather than customers as the unit for agents
    if sector_abbr == 'com':
        inputs['county_customer_count'] = 'county_bldg_count_2012'
    else:
        inputs['county_customer_count'] = 'county_total_customers_2011'

    #=========================================================================
    #     link each block sample to a building sample
    #=========================================================================
    sql =  """DROP TABLE IF EXISTS %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s AS
            WITH a as
            (
                SELECT a.agent_id, a.pgid, a.tract_id_alias, a.county_id, a.bin_id,
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
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # add indices
    sql = """CREATE INDEX agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s_join_btree
            ON %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(crb_model, hdf_index);


            CREATE INDEX agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s_pgid_btree
            ON %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(pgid);
            """ % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def sample_agent_utility_type(schema, role, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string):
    # NOTE: This function uses a random weighted sampling process to determine the agent's utility type.
    #       The utility type will be used to determine the agent's rates later on in
    #       agent_mutation_elec.get_electric_rates.

    msg = "\tSampling Utility Type for Each Agent"
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    #=========================================================================
    #     Assign each agent to a utility type
    #=========================================================================
    sql =  """DROP TABLE IF EXISTS %(schema)s.agent_utility_type_%(sector_abbr)s_%(i_place_holder)s;
              CREATE  TABLE %(schema)s.agent_utility_type_%(sector_abbr)s_%(i_place_holder)s AS
              WITH utility_type_options AS
                 (
                    SELECT a.agent_id,
                           array_agg(b.utility_id ORDER BY b.utility_id) as utility_ids,
                           array_agg(b.util_type_weight ORDER BY b.utility_id)  as util_type_weights
                    FROM %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s a
                    LEFT JOIN diffusion_shared.cnty_util_type_weights_%(sector_abbr)s b
                        ON a.county_id = b.county_id
                        AND b.util_type_weight > 0 --NOTE: these should actually be removed from the lookup table   **
                    GROUP BY a.agent_id
                ),

                fix_nulls as
                (
                     SELECT a.agent_id,

                            CASE WHEN utility_ids = ARRAY[NULL::INTEGER] THEN ARRAY[1]
                            ELSE utility_ids
                            END AS utility_ids,

                            CASE WHEN utility_ids = ARRAY[NULL::INTEGER] THEN ARRAY[1]
                            ELSE util_type_weights
                            END AS util_type_weights

                    FROM  utility_type_options a
                ),

                sample as
                (
                    SELECT a.agent_id,
                             unnest(diffusion_shared.sample(
                                a.utility_ids,
                                1,
                                %(seed)s * a.agent_id,
                                True,
                                util_type_weights)) as util_id
                    FROM fix_nulls a
                )

                SELECT a.agent_id,
                       CASE WHEN util_id = 1 THEN 'Investor Owned'
                            WHEN util_id = 2 THEN 'Cooperative'
                            WHEN util_id = 3 THEN 'Municipal'
                            WHEN util_id = 4 THEN 'Other'
                       END as utility_type
                FROM sample a;""" % inputs
    # TODO--fix nulls
    # TODO -change utility names to shorthand
    p_run(pg_conn_string, role,  sql, county_chunks, pool)

    # Add primary key
    sql = """ALTER TABLE %(schema)s.agent_utility_type_%(sector_abbr)s_%(i_place_holder)s
            ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # add constraint to confirm no null utility types
    sql = """ALTER TABLE %(schema)s.agent_utility_type_%(sector_abbr)s_%(i_place_holder)s
             ALTER COLUMN utility_type SET NOT NULL;""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def calculate_max_demand(schema, role, sector_abbr, county_chunks, agents_per_region, seed, pool, pg_conn_string):

    msg = '\tCalculating Maximum Electricity Demand for Each Agent'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    #=========================================================================
    #     find the max demand for each agent based on the applicable energy plus building model
    #=========================================================================
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_max_demand_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.agent_max_demand_%(sector_abbr)s_%(i_place_holder)s AS
            SELECT a.county_id, a.bin_id,
                    ROUND(b.normalized_max_demand_kw_per_kw * a.load_kwh_per_customer_in_bin, 0)::INTEGER AS
                    max_demand_kw,
                    ROUND(a.load_kwh_per_customer_in_bin/12.0, 0)::INTEGER AS avg_monthly_kwh
            FROM %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s a
            LEFT JOIN diffusion_load_profiles.energy_plus_max_normalized_demand b
                ON a.crb_model = b.crb_model
                AND a.hdf_index = b.hdf_index;""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.agent_max_demand_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def simulate_roof_characteristics(county_chunks, pool, pg_conn_string, con, schema, role, sector_abbr, seed):

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
    p_run(pg_conn_string,role, sql, county_chunks, pool)

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
    p_run(pg_conn_string,role, sql, county_chunks, pool)

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
    p_run(pg_conn_string,role, sql, county_chunks, pool)

    # add primary key
    sql =  """ALTER TABLE %(schema)s.agent_rooftops_%(sector_abbr)s_%(i_place_holder)s
              ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def determine_allowable_turbine_heights(county_chunks, pool, pg_conn_string, schema, role, sector_abbr):

    msg = "\tDetermining Allowable Turbine Heights for Each Agent"
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    #=========================================================================
    #     Find the allowable range of turbine heights for each agent
    #=========================================================================
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
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # add primary key
    sql =  """ALTER TABLE %(schema)s.agent_turbine_height_constraints_%(sector_abbr)s_%(i_place_holder)s
              ADD PRIMARY KEY (county_id, bin_id);""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def find_potential_turbine_sizes(county_chunks, cur, con, pool, pg_conn_string, schema, role, sector_abbr):

    msg = "\tIdentifying Potential Turbine Sizes for Each Agent"
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    #=========================================================================
    #     Create a lookup table of the allowable turbine heights and sizes for
    #     each agent
    #=========================================================================
    sql = """DROP TABLE IF EXISTS %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s;
             CREATE TABLE %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s AS
             SELECT a.county_id, a.bin_id,
                 COALESCE(b.turbine_height_m, 0) AS turbine_height_m,
                 COALESCE(b.turbine_size_kw, 0) as turbine_size_kw
             FROM %(schema)s.agent_turbine_height_constraints_%(sector_abbr)s_%(i_place_holder)s a
             LEFT JOIN %(schema)s.input_wind_siting_turbine_sizes b
                 ON b.effective_min_blade_height_m >= a.min_allowable_blade_height_m
                 AND b.effective_max_blade_height_m <= a.max_allowable_blade_height_m;
             """ % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # create indices
    sql =  """CREATE INDEX agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s_id_btree
              ON %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s
              USING BTREE(county_id, bin_id);

              CREATE INDEX agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s_turbine_height_m_btree
              ON %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s
              USING BTREE(turbine_height_m);""" % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def simulate_turbine_configurations(county_chunks, pool, cur, con, pg_conn_string, schema, role, sector_abbr):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    sql = """DROP TABLE IF EXISTS %(schema)s.agent_allowable_turbines_simulated_%(sector_abbr)s_%(i_place_holder)s;
    CREATE TABLE %(schema)s.agent_allowable_turbines_simulated_%(sector_abbr)s_%(i_place_holder)s AS
    WITH nem_state_by_sector AS(
        SELECT 
            state_abbr,
            sector_abbr,
            COALESCE(pv_kw_limit, float8 '+infinity') as system_size_limit_nem
        FROM %(schema)s.input_main_nem_state_by_sector_2017
        WHERE scenario = (SELECT val FROM %(schema)s.input_main_nem_selected_scenario)
    ),
    all_turbines AS
    (
         SELECT 
             b.county_id,
             a.bin_id,
             b.i, b.j, b.cf_bin,
             a.load_kwh_per_customer_in_bin,
             
             c.turbine_height_m,
             c.turbine_size_kw,
             
             d.system_size_limit_nem,
             
             e.sys_size_target_nem,
             e.sys_oversize_limit_nem,
             e.sys_size_target_no_nem,
             e.sys_oversize_limit_no_nem
             
             FROM %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s a
             LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                 ON a.pgid = b.pgid
             RIGHT JOIN %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s c
                 ON a.county_id = c.county_id
                 AND a.bin_id = c.bin_id
             LEFT JOIN nem_state_by_sector d
                 ON b.state_abbr = d.state_abbr
                 AND d.sector_abbr = '%(sector_abbr)s'
             LEFT JOIN %(schema)s.input_wind_performance_system_sizing_factors e
                 ON e.sector_abbr = '%(sector_abbr)s'
    ), 
    simulated AS
    (
         SELECT 
             b.year,
             a.county_id,
             a.bin_id,
             a.turbine_height_m,
             a.turbine_size_kw,
             COALESCE(b.interp_factor * (w2.aep-w1.aep) + w1.aep, 0) * b.derate_factor as naep,
             diffusion_wind.scoe_2(a.load_kwh_per_customer_in_bin,
                                   COALESCE(b.interp_factor * (w2.aep-w1.aep) + w1.aep, 0) * b.derate_factor,
                                   a.turbine_size_kw,
                                   a.system_size_limit_nem,
                                   a.sys_size_target_nem,
                                   a.sys_oversize_limit_nem,
                                   a.sys_size_target_no_nem,
                                   a.sys_oversize_limit_no_nem) as scoe_return
         FROM all_turbines a
         INNER JOIN %(schema)s.temporal_factors_wind b
             ON a.turbine_height_m = b.turbine_height_m
             AND a.turbine_size_kw = b.turbine_size_kw
         LEFT JOIN diffusion_resource_wind.wind_resource_annual w1
             ON a.i = w1.i
             AND a.j = w1.j
             AND a.cf_bin = w1.cf_bin
             AND a.turbine_height_m = w1.height
             AND b.power_curve_1 = w1.turbine_id
         LEFT JOIN diffusion_resource_wind.wind_resource_annual w2
             ON a.i = w2.i
             AND a.j = w2.j
             AND a.cf_bin = w2.cf_bin
             AND a.turbine_height_m = w2.height
             AND b.power_curve_2 = w2.turbine_id
    )
    SELECT
        year,
        county_id,
        bin_id,

        CASE WHEN (scoe_return).scoe = 'Inf'::DOUBLE PRECISION THEN 0
        ELSE turbine_size_kw
        END AS turbine_size_kw,
        CASE WHEN (scoe_return).scoe = 'Inf'::DOUBLE PRECISION THEN 0
        ELSE turbine_height_m
        END AS turbine_height_m,
        
        CASE WHEN (scoe_return).scoe = 'Inf'::DOUBLE PRECISION THEN 0
        ELSE naep
        END AS naep,
        
        naep * (scoe_return).nturb * turbine_size_kw as aep,
        (scoe_return).nturb * turbine_size_kw as system_size_kw,
        (scoe_return).nturb as nturb,
        
        (scoe_return).scoe AS scoe
    FROM simulated """ % inputs
    p_run(pg_conn_string, role, sql, county_chunks, pool)
    
    
    #==============================================================================
    #    Find the Most Cost-Effective Wind Turbine Configuration for Each Customer Bin
    #==============================================================================    
    sql_part = """SELECT DISTINCT ON (a.county_id, a.bin_id, a.year) a.*
             FROM %(schema)s.agent_allowable_turbines_simulated_%(sector_abbr)s_%(i_place_holder)s a
             ORDER BY a.county_id ASC, a.bin_id ASC, a.year ASC, a.scoe ASC,
                      a.system_size_kw ASC, a.turbine_height_m ASC""" % inputs
    
    # create the template table
    template_inputs = inputs.copy()
    template_inputs['i'] = 0
    template_inputs['sql_body'] = sql_part % template_inputs
    sql_template = """DROP TABLE IF EXISTS %(schema)s.agent_best_option_wind_%(sector_abbr)s;
                      CREATE TABLE %(schema)s.agent_best_option_wind_%(sector_abbr)s AS
                      %(sql_body)s
                      LIMIT 0;""" % template_inputs
    cur.execute(sql_template)
    con.commit()

    # reconfigure sql into an insert statement
    inputs['sql_body'] = sql_part
    sql = """INSERT INTO %(schema)s.agent_best_option_wind_%(sector_abbr)s
            %(sql_body)s;""" % inputs
    # run the insert statement
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # create indices
    sql = """CREATE INDEX agent_best_option_wind_%(sector_abbr)s_btree_join_fields
            ON  %(schema)s.agent_best_option_wind_%(sector_abbr)s
            USING BTREE(year, county_id, bin_id, turbine_height_m, turbine_size_kw);""" % inputs
    cur.execute(sql)
    con.commit()    
        

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def combine_all_attributes(county_chunks, pool, cur, con, pg_conn_string, schema, role, sector_abbr):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    sql_part = """SELECT
                    -- agent id
                    a.agent_id,

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
                    a.load_kwh_per_customer_in_bin,

                    -- rate utility_type
                    f.utility_type as util_type,

                    -- load profile
                    c.max_demand_kw,
                    c.avg_monthly_kwh,

                    -- solar siting constraints
                    d.tilt,
                    d.azimuth,
                    d.pct_of_bldgs_developable,
                    d.developable_roof_sqft,
                    d.ground_cover_ratio,

                    -- wind siting constraints
                    e.turbine_height_m,
                    e.turbine_size_kw

             FROM %(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s a
             LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                 ON a.pgid = b.pgid
             LEFT JOIN %(schema)s.agent_max_demand_%(sector_abbr)s_%(i_place_holder)s c
                 ON a.county_id = c.county_id
                 AND a.bin_id = c.bin_id
             LEFT JOIN %(schema)s.agent_rooftops_%(sector_abbr)s_%(i_place_holder)s d
                 ON a.county_id = d.county_id
                 AND a.bin_id = d.bin_id
            RIGHT JOIN %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s e
                 ON a.county_id = e.county_id
                 AND a.bin_id = e.bin_id
            LEFT JOIN %(schema)s.agent_utility_type_%(sector_abbr)s_%(i_place_holder)s f
                on a.agent_id = f.agent_id """ % inputs

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
    p_run(pg_conn_string, role, sql, county_chunks, pool)

#    # add primary key
#    # TODO: move addition of primary key to year loop?
#    sql =  """ALTER TABLE %(schema)s.agent_core_attributes_%(sector_abbr)s
#              ADD PRIMARY KEY (agent_id);""" % inputs
#    cur.execute(sql)
#    con.commit()

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
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def combine_all_attributes_solar(county_chunks, pool, cur, con, pg_conn_string, schema, role, sector_abbr):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    sql_part = """SELECT
                    -- agent id
                    a.agent_id,

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
                    a.load_kwh_per_customer_in_bin,

                    -- rate utility_type
                    f.utility_type as util_type,

                    -- load profile
                    c.max_demand_kw,
                    c.avg_monthly_kwh,

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
                 AND a.bin_id = e.bin_id
            LEFT JOIN %(schema)s.agent_utility_type_%(sector_abbr)s_%(i_place_holder)s f
                on a.agent_id = f.agent_id """ % inputs

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
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # add primary key
    sql =  """ALTER TABLE %(schema)s.agent_core_attributes_%(sector_abbr)s
              ADD PRIMARY KEY (agent_id);""" % inputs
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
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def combine_all_attributes_wind(county_chunks, pool, cur, con, pg_conn_string, schema, role, sector_abbr):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    sql_part = """SELECT
                    -- agent id
                    a.agent_id,

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
                    a.load_kwh_per_customer_in_bin,

                    -- rate utility_type
                    f.utility_type as util_type,

                    -- load profile
                    c.max_demand_kw,
                    c.avg_monthly_kwh,

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
                 AND a.bin_id = e.bin_id
            LEFT JOIN %(schema)s.agent_utility_type_%(sector_abbr)s_%(i_place_holder)s f
                on a.agent_id = f.agent_id """ % inputs

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
    p_run(pg_conn_string, role, sql, county_chunks, pool)

    # add primary key
    sql =  """ALTER TABLE %(schema)s.agent_core_attributes_%(sector_abbr)s
              ADD PRIMARY KEY (agent_id);""" % inputs
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
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def cleanup_intermediate_tables(schema, role, sectors, county_chunks, pg_conn_string, cur, con, pool):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'

    #=========================================================================
    #   clean up intermediate tables
    #=========================================================================
    msg = "\tCleaning Up Intermediate Tables..."
    logger.info(msg)
    intermediate_tables = [
        '%(schema)s.agent_blocks_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.agent_bldgs_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.agent_blocks_and_bldgs_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.agent_utility_type_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.agent_max_demand_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.agent_rooftop_cities_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.agent_rooftops_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.agent_turbine_height_constraints_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.agent_allowable_turbines_simulated_%(sector_abbr)s_%(i_place_holder)s'
    ]

    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = 'DROP TABLE IF EXISTS %s;'
        for intermediate_table in intermediate_tables:
            table_name = intermediate_table % inputs
            isql = sql % table_name
            if '%(i)s' in table_name:
                p_run(pg_conn_string, role,isql, county_chunks, pool)
            else:
                cur.execute(isql)
                con.commit()


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def merge_all_core_agents(cur, con, schema, role, sectors, techs):

    inputs = locals().copy()

    msg = "Merging All Agents into a Single Table View"
    logger.info(msg)

    sql_list = []
    for sector_abbr, sector in sectors.iteritems():
        for tech in techs:
            inputs['sector_abbr'] = sector_abbr
            inputs['sector'] = sector
            inputs['tech'] = tech
            sql = """SELECT a.agent_id,
                            a.pgid,
                            a.tract_id_alias,
                            a.county_id,
                            a.bin_id,
                            a.state_abbr,
                            a.census_division_abbr,
                            a.pca_reg,
                            a.reeds_reg,
                            a.util_type,
                            a.customers_in_bin,
                            a.load_kwh_per_customer_in_bin,
                            a.load_kwh_in_bin,
                            a.max_demand_kw,
                            a.avg_monthly_kwh,
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
                            a.bldg_size_class,
                            -- wind
                            a.i,
                            a.j,
                            a.cf_bin,
                            a.turbine_height_m,
                            a.turbine_size_kw,
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


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def combine_temporal_data_wind(cur, con, schema, start_year, end_year):
    
    inputs = locals().copy()
    
    # combined temporal data for technology specific factors
    sql = """DROP TABLE IF EXISTS %(schema)s.temporal_factors_wind;
            CREATE UNLOGGED TABLE %(schema)s.temporal_factors_wind as
            SELECT      a.year, 
                    	a.turbine_size_kw, 
                         a.power_curve_1,
                         a.power_curve_2,
                    	a.interp_factor,
                    	b.turbine_height_m,
                    	d.derate_factor
            FROM %(schema)s.input_wind_performance_power_curve_transitions a
            LEFT JOIN %(schema)s.input_wind_performance_allowable_turbine_sizes b
                	ON a.turbine_size_kw = b.turbine_size_kw
            LEFT JOIN %(schema)s.input_wind_performance_gen_derate_factors d
                	ON a.year = d.year
                 AND  a.turbine_size_kw = d.turbine_size_kw
            WHERE a.year BETWEEN %(start_year)s AND %(end_year)s
            
            UNION ALL
            
            SELECT GENERATE_SERIES(%(start_year)s, %(end_year)s, 2) as year,
                	0 as turbine_size_kw,
                	0 as power_curve_1,
                  0 as power_curve_2,
                  0 as interp_factor,
                	0 as turbine_height_m,
                	0 as derate_factor;""" % inputs
    cur.execute(sql)
    con.commit()
    
    
    # create indices for subsequent joins
    sql =  """CREATE INDEX temporal_factors_technology_join_fields_btree 
              ON %(schema)s.temporal_factors_wind
              USING BTREE(turbine_height_m, turbine_size_kw, power_curve_1, power_curve_2);
              
              CREATE INDEX temporal_factors_technology_year_btree 
              ON %(schema)s.temporal_factors_wind
              USING BTREE(year);""" % inputs
    cur.execute(sql)
    con.commit()  
    