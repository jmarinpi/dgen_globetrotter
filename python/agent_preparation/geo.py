# -*- coding: utf-8 -*-
"""
Created on Thu May 26 11:29:02 2016

@author: mgleason
"""
import psycopg2 as pg
import numpy as np
import decorators
import utility_functions as utilfunc
import multiprocessing
from agents_preparation import p_run, create_agent_id_sequence

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


@decorators.fn_timer(logger=logger, tab_level=0, prefix='')
def generate_core_agent_attributes(cur, con, techs, schema, sample_pct,
                                   min_agents, agents_per_region, sectors,
                                   pg_procs, pg_conn_string, seed, end_year):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'

    # break counties into subsets for parallel processing
    chunks, pg_procs = split_tracts(cur, schema, pg_procs)

    # create the pool of multiprocessing workers
    # (note: do this after splitting counties because, for small states,
    # split_counties will adjust the number of pg_procs)
    pool = multiprocessing.Pool(processes=pg_procs)

    try:
        # create sequence to produce unique agent ids
        create_agent_id_sequence(schema, con, cur)
        for sector_abbr, sector in sectors.iteritems():
            with utilfunc.Timer() as t:
                logger.info("Creating Agents for %s Sector" % sector)

                # =============================================================
                #     sample from blocks and building microdata, convolve
                # samples, and estimate
                #     max demand for each agent
                # =============================================================

                # INITIAL AGENTS TO REPRESENT STARTING BUILDING STOCK (2012)
                logger.info("\tWorking on Initial Building Stock (2012)")
                # NOTE: each of these functions is dependent on the last, so
                # changes from one must be cascaded to the others
                calculate_initial_number_of_agents_by_tract(schema,
                                                            sector_abbr,
                                                            chunks,
                                                            sample_pct,
                                                            min_agents, seed,
                                                            pool,
                                                            pg_conn_string)
                sample_blocks(schema, sector_abbr, 'initial',
                              chunks, seed, pool, pg_conn_string, con, cur)
                add_agent_ids(schema, sector_abbr, 'initial',
                              chunks, pool, pg_conn_string, con, cur)
                sample_building_type(schema, sector_abbr, 'initial', chunks,
                                     seed, pool, pg_conn_string)
                # TODO: add in selection of heating fuel type for res sector
                sample_building_microdata(schema, sector_abbr, 'initial',
                                          chunks, seed, pool, pg_conn_string)
                estimate_agent_thermal_loads(schema, sector_abbr, 'initial',
                                             chunks, pool, pg_conn_string)
                estimate_system_ages(schema, sector_abbr, 'initial', chunks,
                                     seed, pool, pg_conn_string)
                estimate_system_lifetimes(schema, sector_abbr, 'initial',
                                          chunks, seed, pool, pg_conn_string)
                # get GHP resource (thermal conductivity)
                sample_ground_thermal_conductivity(schema, sector_abbr,
                                                   'initial', chunks, pool,
                                                   pg_conn_string, seed)

                # NEW AGENTS TO REPRESENT NEW CONSTRUCTION (2014 - 2050)
                # calculate the agents required to represent new construction
                logger.info(
                    "\tWorking on New Construction (2014 - %s)" % end_year)
                calculate_new_construction_number_of_agents_by_tract(
                    schema, sector_abbr, chunks, sample_pct, seed, pool,
                    pg_conn_string, end_year)
                sample_blocks(schema, sector_abbr, 'new', chunks,
                              seed, pool, pg_conn_string, con, cur)
                add_agent_ids(schema, sector_abbr, 'new', chunks,
                              pool, pg_conn_string, con, cur)
                sample_building_type(schema, sector_abbr,
                                     'new', chunks, seed, pool, pg_conn_string)
                sample_building_microdata(schema, sector_abbr, 'new', chunks,
                                          seed, pool, pg_conn_string)
                estimate_agent_thermal_loads(
                    schema, sector_abbr, 'new', chunks, pool, pg_conn_string)
                estimate_system_ages(schema, sector_abbr,
                                     'new', chunks, seed, pool, pg_conn_string)
                estimate_system_lifetimes(schema, sector_abbr, 'new', chunks,
                                          seed, pool, pg_conn_string)
                # get GHP resource (thermal conductivity)
                sample_ground_thermal_conductivity(schema, sector_abbr, 'new',
                                                   chunks, pool,
                                                   pg_conn_string, seed)

                # =============================================================
                #     combine all pieces into a single table
                # =============================================================
                combine_all_attributes(chunks, pool, cur, con, pg_conn_string,
                                       schema, sector_abbr)

        # =====================================================================
        #     create a view that combines all sectors and techs
        # =====================================================================
        merge_all_core_agents(cur, con, schema, sectors, techs)

        # =====================================================================
        #    drop the intermediate tables
        # =====================================================================
        # TODO: update the list of tables to delete
        cleanup_intermediate_tables(schema, sectors, chunks, pg_conn_string,
                                    cur, con, pool)

    except:
        # roll back any transactions
        con.rollback()
        # re-raise the exception
        raise

    finally:
        # close the multiprocessing pool
        pool.close()


def split_tracts(cur, schema, pg_procs):
    # create a dictionary out of the input arguments -- this is used through
    # sql queries
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
        chunks = map(list, np.array_split(tracts, pg_procs))
    else:
        chunks = [tracts]
        pg_procs = 1

    return chunks, pg_procs


def calculate_initial_number_of_agents_by_tract(schema, sector_abbr, chunks,
                                                sample_pct, min_agents, seed,
                                                pool, pg_conn_string):

    msg = '\t\tDetermining Initial Number of Agents in Each Tract'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'

    sql = """DROP TABLE IF EXISTS %(schema)s.initial_agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.initial_agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s AS
            	SELECT a.tract_id_alias,
                     2012::INTEGER as year,
                     a.bldg_count_%(sector_abbr)s as tract_bldg_count,
            		 CASE WHEN ROUND(a.bldg_count_%(sector_abbr)s * %(sample_pct)s, 0)::INTEGER < %(min_agents)s
                               THEN %(min_agents)s
                          ELSE ROUND(a.bldg_count_%(sector_abbr)s * %(sample_pct)s, 0)::INTEGER
                      END AS n_agents
            	FROM diffusion_blocks.tract_building_count_by_sector a
            	WHERE tract_id_alias in (%(chunk_place_holder)s)
            AND a.bldg_count_%(sector_abbr)s > 0;""" % inputs

    p_run(pg_conn_string, sql, chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.initial_agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (tract_id_alias, year);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)


def calculate_new_construction_number_of_agents_by_tract(schema, sector_abbr,
                                                         chunks, sample_pct,
                                                         seed, pool,
                                                         pg_conn_string,
                                                         end_year):

    msg = '\t\tDetermining Number of New Construction Agents in Each Tract by Year'
    logger.info(msg)

    inputs = locals().copy()

    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'
    inputs['min_agents'] = 1

    sql = """DROP TABLE IF EXISTS %(schema)s.new_agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.new_agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s AS
            WITH a as
            (
                SELECT a.tract_id_alias,
                       a.year,
                       a.new_bldgs_%(sector_abbr)s as tract_bldg_count,
                       a.new_bldgs_%(sector_abbr)s * %(sample_pct)s as raw_sample_count
                	FROM %(schema)s.new_building_growth_to_model a
                	WHERE tract_id_alias in (%(chunk_place_holder)s)
                  AND a.year <= %(end_year)s
            )
            SELECT a.tract_id_alias,
                    a.year,
                    a.tract_bldg_count,
                    CASE WHEN a.raw_sample_count = 0 THEN 0::INTEGER
                         WHEN a.raw_sample_count > 0 and a.raw_sample_count < %(min_agents)s THEN %(min_agents)s
                    ELSE ROUND(a.raw_sample_count, 0)::INTEGER
                    END AS n_agents
            FROM a;""" % inputs

    p_run(pg_conn_string, sql, chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.new_agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (tract_id_alias, year);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)


@decorators.fn_timer(logger=logger, tab_level=3, prefix='')
def sample_blocks(schema, sector_abbr, initial_or_new, chunks, seed, pool,
                  pg_conn_string, con, cur):

    msg = '\t\tSampling from Blocks for Each Tract'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'
    inputs['sector_abbr'] = sector_abbr
    inputs['initial_or_new'] = initial_or_new

    # ========================================================================
    #     randomly sample  N blocks from each county
    # ========================================================================
    # (note: [this may not be true any longer...] some counties will have
    # fewer than N points, in which case, all are returned)
    sql = """DROP TABLE IF EXISTS %(schema)s.%(initial_or_new)s_agent_blocks_%(sector_abbr)s_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.%(initial_or_new)s_agent_blocks_%(sector_abbr)s_%(i_place_holder)s AS
             WITH a as
             (
                 SELECT a.tract_id_alias,
                         array_agg(a.pgid ORDER BY a.pgid) as pgids,
                         array_agg(a.sample_weight_geo ORDER BY a.pgid) as block_weights
                 FROM %(schema)s.block_microdata_%(sector_abbr)s_joined a
                 WHERE a.tract_id_alias in (%(chunk_place_holder)s)
                 AND a.bldg_count_%(sector_abbr)s > 0
                 GROUP BY a.tract_id_alias

             )
             SELECT NULL::INTEGER agent_id,
                     a.tract_id_alias,
                     b.year,
                        unnest(diffusion_shared.sample(a.pgids,
                                                       b.n_agents,
                                                       %(seed)s * b.year,
                                                       True,
                                                       a.block_weights)
                                                       ) as pgid
            FROM a
            LEFT JOIN %(schema)s.%(initial_or_new)s_agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s b
                ON a.tract_id_alias = b.tract_id_alias
            ORDER BY b.year, a.tract_id_alias, pgid;""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)

    # add indices
    sql = """CREATE INDEX %(initial_or_new)s_agent_blocks_%(sector_abbr)s_%(i_place_holder)s_pgid_btree
            ON %(schema)s.%(initial_or_new)s_agent_blocks_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(pgid);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)


@decorators.fn_timer(logger=logger, tab_level=3, prefix='')
def add_agent_ids(schema, sector_abbr, initial_or_new, chunks, pool,
                  pg_conn_string, con, cur):

    inputs = locals().copy()

    # need to do this sequentially to ensure conssitent application of
    # agent_ids
    # (if run in parallel, there is no guarantee the order in which each table
    # will be hitting the sequence)
    for i in range(0, len(chunks)):
        inputs['i_place_holder'] = i
        sql = """UPDATE %(schema)s.%(initial_or_new)s_agent_blocks_%(sector_abbr)s_%(i_place_holder)s
                 SET agent_id = nextval('%(schema)s.agent_id_sequence');""" % inputs
        cur.execute(sql)
        con.commit()

    # add primary key
    # (can do this part in parallel)

    # reset i_place_holder to normal value
    inputs['i_place_holder'] = '%(i)s'
    # run query
    sql = """ALTER TABLE %(schema)s.%(initial_or_new)s_agent_blocks_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)


@decorators.fn_timer(logger=logger, tab_level=3, prefix='')
def sample_building_type(schema, sector_abbr, initial_or_new, chunks, seed,
                         pool, pg_conn_string):

    msg = '\t\tSampling Building Types from Blocks for Each Tract'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'
    inputs['sector_abbr'] = sector_abbr
    inputs['initial_or_new'] = initial_or_new

    sql = """DROP TABLE IF EXISTS %(schema)s.%(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.%(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s AS
             SELECT a.agent_id, a.tract_id_alias, a.year,
                         b.census_division_abbr, b.reportable_domain, b.climate_zone_recs, b.climate_zone_cbecs, -- need these two fields for subsequent microdata step
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
            FROM %(schema)s.%(initial_or_new)s_agent_blocks_%(sector_abbr)s_%(i_place_holder)s a
            LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                ON a.pgid = b.pgid
            lEFT JOIN diffusion_blocks.bldg_type_arrays c
                ON c.sector_abbr = '%(sector_abbr)s';""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.%(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)

    # add indices
    sql = """CREATE INDEX %(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s_bldg_type_btree
            ON %(schema)s.%(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(bldg_type);

            CREATE INDEX %(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s_reportable_domain_btree
            ON %(schema)s.%(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(reportable_domain);

            CREATE INDEX %(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s_census_division_abbr_btree
            ON %(schema)s.%(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(census_division_abbr);

            CREATE INDEX %(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s_tract_id_alias_btree
            ON %(schema)s.%(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s
            USING BTREE(tract_id_alias);
            """ % inputs
    p_run(pg_conn_string, sql, chunks, pool)


@decorators.fn_timer(logger=logger, tab_level=3, prefix='')
def sample_building_microdata(schema, sector_abbr, initial_or_new, chunks,
                              seed, pool, pg_conn_string):

    msg = "\t\tSampling from Building Microdata"
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['sector_abbr'] = sector_abbr
    inputs['initial_or_new'] = initial_or_new

    if sector_abbr == 'res':
        if initial_or_new == 'initial':
            inputs['eia_join_clause'] = """ b.eia_type = c.typehuq
                                        AND b.min_tenants <= c.num_tenants
                                        AND b.max_tenants >= c.num_tenants
                                        AND a.reportable_domain = c.reportable_domain
                                        AND c.sector_abbr = '%(sector_abbr)s' """ % inputs
        elif initial_or_new == 'new':
            # NOTE: have to relax location and building type clauses to allow
            # for subsetting on year
            inputs['eia_join_clause'] = """ b.eia_type = c.typehuq
                                            AND a.climate_zone_recs = c.climate_zone
                                            AND c.year_built >= 2005
                                            AND c.sector_abbr = '%(sector_abbr)s' """ % inputs

    else:
        if initial_or_new == 'initial':
            inputs['eia_join_clause'] = """ b.eia_type = c.pbaplus
                                        AND a.census_division_abbr = c.census_division_abbr
                                        AND c.sector_abbr = '%(sector_abbr)s' """  % inputs
        elif initial_or_new == 'new':
            # NOTE: have to relax location and building type clauses to allow
            # for subsetting on year
            inputs['eia_join_clause'] = """a.census_division_abbr = c.census_division_abbr
                                        AND c.year_built >= 2000
                                        AND c.sector_abbr = '%(sector_abbr)s' """  % inputs

    sql =  """DROP TABLE IF EXISTS %(schema)s.%(initial_or_new)s_agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s;
         CREATE UNLOGGED TABLE %(schema)s.%(initial_or_new)s_agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s AS
         WITH all_bldgs AS
         (
             SELECT a.agent_id,
                     c.building_id as eia_bldg_id, c.sample_wt::NUMERIC as eia_bldg_weight
             FROM %(schema)s.%(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s a
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
        SELECT  a.agent_id,
                a.eia_bldg_id,
                b.sample_wt as eia_bldg_weight,
                b.climate_zone,
                b.pba,
                b.pbaplus,
                b.typehuq,
                b.pba_or_typehuq,
                b.roof_material,
                b.owner_occupied,
                b.kwh,
                b.year_built,
                b.single_family_res,
                b.num_tenants,
                b.num_floors,
                b.space_heat_equip,
                b.space_heat_fuel,
                b.space_heat_age_min,
                b.space_heat_age_max,
                b.water_heat_equip,
                b.water_heat_fuel,
                b.water_heat_age_min,
                b.water_heat_age_max,
                b.space_cool_equip,
                b.space_cool_fuel,
                b.space_cool_age_min,
                b.space_cool_age_max,
                b.ducts,
                b.totsqft,
                b.totsqft_heat,
                b.totsqft_cool,
                b.kbtu_space_heat as site_space_heat_kbtu,
                b.kbtu_space_cool as site_space_cool_kbtu,
                b.kbtu_water_heat as site_water_heat_kbtu,
                b.crb_model,
                b.roof_style,
                b.roof_sqft,

                c.efficiency as space_heat_efficiency,
                d.efficiency as space_cool_efficiency,
                e.efficiency as water_heat_efficiency

        FROM sampled_bldgs a

        LEFT JOIN diffusion_shared.cbecs_recs_expanded_combined b
            ON a.eia_bldg_id = b.building_id
            AND b.sector_abbr = '%(sector_abbr)s'

        LEFT JOIN diffusion_geo.cbecs_recs_thermal_efficiency_factors c
            ON c.sector_abbr = '%(sector_abbr)s'
            AND b.space_heat_equip = c.equipment_type
            and b.space_heat_fuel = c.fuel
            AND c.end_use = 'space_heat'

        LEFT JOIN diffusion_geo.cbecs_recs_thermal_efficiency_factors d
            ON d.sector_abbr = '%(sector_abbr)s'
            AND b.space_cool_equip = d.equipment_type
            and b.space_cool_fuel = d.fuel
            AND d.end_use = 'space_cool'

        LEFT JOIN diffusion_geo.cbecs_recs_thermal_efficiency_factors e
            ON e.sector_abbr = '%(sector_abbr)s'
            AND b.water_heat_equip = e.equipment_type
            and b.water_heat_fuel = e.fuel
            AND e.end_use = 'water_heat';""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.%(initial_or_new)s_agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)


@decorators.fn_timer(logger=logger, tab_level=3, prefix='')
def estimate_agent_thermal_loads(schema, sector_abbr, initial_or_new, chunks, pool, pg_conn_string):

    msg = '\t\tEstimating Agent Thermal Loads'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['sector_abbr'] = sector_abbr
    inputs['initial_or_new'] = initial_or_new

    sql = """DROP TABLE IF EXISTS %(schema)s.%(initial_or_new)s_agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.%(initial_or_new)s_agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s AS
            WITH b as
            (
                SELECT  a.agent_id,
                        a.block_bldgs_weight::NUMERIC/sum(a.block_bldgs_weight) OVER (PARTITION BY a.tract_id_alias) * b.tract_bldg_count as buildings_in_bin
                FROM %(schema)s.%(initial_or_new)s_agent_building_types_%(sector_abbr)s_%(i_place_holder)s a
                LEFT JOIN %(schema)s.%(initial_or_new)s_agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s b
			ON a.tract_id_alias = b.tract_id_alias
                   AND a.year = b.year
            ),
            c as
            (

                SELECT a.agent_id,
                           b.buildings_in_bin,
                           CASE WHEN '%(initial_or_new)s' = 'initial' THEN
                                       (b.buildings_in_bin * a.site_space_heat_kbtu)/sum(b.buildings_in_bin * NULLIF(a.site_space_heat_kbtu, 0)) OVER (PARTITION BY d.old_county_id)
                                       * e.space_heating_thermal_load_mmbtu * 1000.
                                WHEN '%(initial_or_new)s' = 'new' THEN b.buildings_in_bin * a.site_space_heat_kbtu
                           END AS site_space_heat_in_bin_kbtu,

                           CASE WHEN '%(initial_or_new)s' = 'initial' THEN
                                       (b.buildings_in_bin * a.site_space_cool_kbtu)/sum(b.buildings_in_bin * NULLIF(a.site_space_cool_kbtu, 0)) OVER (PARTITION BY d.old_county_id)
                                       * e.space_cooling_thermal_load_mmbtu * 1000.
                                WHEN '%(initial_or_new)s' = 'new' THEN b.buildings_in_bin * a.site_space_cool_kbtu
                           END AS site_space_cool_in_bin_kbtu,

                           CASE WHEN '%(initial_or_new)s' = 'initial' THEN
                                       (b.buildings_in_bin * a.site_water_heat_kbtu)/sum(b.buildings_in_bin * NULLIF(a.site_water_heat_kbtu, 0)) OVER (PARTITION BY d.old_county_id)
                                       * e.water_heating_thermal_load_mmbtu * 1000.
                                WHEN '%(initial_or_new)s' = 'new' THEN b.buildings_in_bin * a.site_water_heat_kbtu
                           END AS site_water_heat_in_bin_kbtu,
                            a.space_heat_efficiency,
                            a.space_cool_efficiency,
                            a.water_heat_efficiency,
                            a.totsqft
                 FROM %(schema)s.%(initial_or_new)s_agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s a
                 LEFT JOIN b
                     ON a.agent_id = b.agent_id
                 LEFT JOIN  %(schema)s.%(initial_or_new)s_agent_blocks_%(sector_abbr)s_%(i_place_holder)s c
                     ON a.agent_id = c.agent_id
                 LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined d
                     ON c.pgid = d.pgid
                 LEFT JOIN diffusion_shared.county_thermal_demand_%(sector_abbr)s e
                     ON d.old_county_id = e.county_id
            ),
            d AS
            (
                SELECT agent_id,
                        buildings_in_bin,
                        totsqft,
                        space_heat_efficiency,
                        space_cool_efficiency,
                        water_heat_efficiency,
                       ROUND(COALESCE(site_space_heat_in_bin_kbtu, 0) * 1000 / 3412.14) as site_space_heat_in_bin_kwh,
                       ROUND(COALESCE(site_space_cool_in_bin_kbtu, 0) * 1000 / 3412.14) as site_space_cool_in_bin_kwh,
                       ROUND(COALESCE(site_water_heat_in_bin_kbtu, 0) * 1000 / 3412.14) as site_water_heat_in_bin_kwh,
                       ROUND(COALESCE(site_space_heat_in_bin_kbtu / buildings_in_bin, 0) * 1000 / 3412.14) as site_space_heat_per_building_in_bin_kwh,
                       ROUND(COALESCE(site_space_cool_in_bin_kbtu / buildings_in_bin, 0) * 1000 / 3412.14) as site_space_cool_per_building_in_bin_kwh,
                       ROUND(COALESCE(site_water_heat_in_bin_kbtu / buildings_in_bin, 0) * 1000 / 3412.14) as site_water_heat_per_building_in_bin_kwh
                FROM c
            )
            SELECT agent_id,
                    buildings_in_bin,
                    totsqft,
                    site_space_heat_in_bin_kwh,
                    site_space_cool_in_bin_kwh,
                    site_water_heat_in_bin_kwh,
                    site_space_heat_in_bin_kwh + site_water_heat_in_bin_kwh as site_total_heat_in_bin_kwh,

                    site_space_heat_per_building_in_bin_kwh,
                    site_space_cool_per_building_in_bin_kwh,
                    site_water_heat_per_building_in_bin_kwh,
                    site_space_heat_per_building_in_bin_kwh + site_water_heat_per_building_in_bin_kwh as site_total_heat_per_building_in_bin_kwh,

                    site_space_heat_in_bin_kwh * space_heat_efficiency as demand_space_heat_in_bin_kwh,
                    site_space_cool_in_bin_kwh * space_cool_efficiency as demand_space_cool_in_bin_kwh,
                    site_water_heat_in_bin_kwh * water_heat_efficiency as demand_water_heat_in_bin_kwh,
                    site_space_heat_in_bin_kwh * space_heat_efficiency + site_water_heat_in_bin_kwh * water_heat_efficiency as demand_total_heat_in_bin_kwh,

                    site_space_heat_per_building_in_bin_kwh * space_heat_efficiency as demand_space_heat_per_building_in_bin_kwh,
                    site_space_cool_per_building_in_bin_kwh * space_cool_efficiency as demand_space_cool_per_building_in_bin_kwh,
                    site_water_heat_per_building_in_bin_kwh * water_heat_efficiency as demand_water_heat_per_building_in_bin_kwh,
                    site_space_heat_per_building_in_bin_kwh * space_heat_efficiency + site_water_heat_per_building_in_bin_kwh * water_heat_efficiency as demand_total_heat_per_building_in_bin_kwh

            FROM d;""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.%(initial_or_new)s_agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)


@decorators.fn_timer(logger=logger, tab_level=3, prefix='')
def estimate_system_ages(schema, sector_abbr, initial_or_new, chunks, seed, pool, pg_conn_string):

    msg = '\t\tEstimating Agent HVAC System Ages'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['sector_abbr'] = sector_abbr
    inputs['initial_or_new'] = initial_or_new

    sql = """DROP TABLE IF EXISTS %(schema)s.%(initial_or_new)s_agent_system_ages_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.%(initial_or_new)s_agent_system_ages_%(sector_abbr)s_%(i_place_holder)s AS
            WITH a as
            (
                SELECT agent_id,
                    CASE
                        WHEN '%(initial_or_new)s' = 'initial' THEN
                            CASE
                                WHEN a.space_heat_age_min IS NULL OR a.space_heat_age_max IS NULL THEN ROUND(diffusion_shared.r_runif(0, 25, 1, %(seed)s * agent_id), 0)::INTEGER
                                ELSE ROUND(diffusion_shared.r_runif(a.space_heat_age_min, a.space_heat_age_max, 1, %(seed)s * agent_id), 0)::INTEGER
                            END
                        WHEN '%(initial_or_new)s' = 'new' THEN -1::INTEGER
                    END as space_heat_system_age,

                     CASE
                        WHEN '%(initial_or_new)s' = 'initial' THEN
                            CASE
                                WHEN a.space_cool_age_min IS NULL OR a.space_cool_age_max IS NULL THEN ROUND(diffusion_shared.r_runif(0, 25, 1, %(seed)s * agent_id), 0)::INTEGER
                                ELSE ROUND(diffusion_shared.r_runif(a.space_cool_age_min, a.space_cool_age_max, 1, %(seed)s * agent_id), 0)::INTEGER
                            END
                        WHEN '%(initial_or_new)s' = 'new' THEN -1::INTEGER
                    END as space_cool_system_age

                FROM %(schema)s.%(initial_or_new)s_agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s a
            )
            SELECT agent_id, space_heat_system_age, space_cool_system_age,
                    diffusion_shared.r_median(ARRAY[space_heat_system_age, space_cool_system_age]) as average_system_age
            FROM a;""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.%(initial_or_new)s_agent_system_ages_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)


@decorators.fn_timer(logger=logger, tab_level=3, prefix='')
def estimate_system_lifetimes(schema, sector_abbr, initial_or_new, chunks, seed, pool, pg_conn_string):

    msg = '\t\tEstimating Agent HVAC System Expected Lifetimes'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['sector_abbr'] = sector_abbr
    inputs['initial_or_new'] = initial_or_new

    sql = """DROP TABLE IF EXISTS %(schema)s.%(initial_or_new)s_agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.%(initial_or_new)s_agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s AS
            WITH a as
            (
                SELECT a.agent_id,
                        CASE WHEN a.space_heat_equip = 'none' THEN -1::INTEGER
                        ELSE ROUND(diffusion_shared.r_rnorm_rlnorm(b.mean, b.std, b.dist_type, %(seed)s * a.agent_id), 0)::INTEGER
                        END as space_heat_system_expected_lifetime,

                        CASE WHEN a.space_cool_equip = 'none' THEN -1::INTEGER
                        ELSE ROUND(diffusion_shared.r_rnorm_rlnorm(c.mean, c.std, c.dist_type, %(seed)s * a.agent_id), 0)::INTEGER
                        END as space_cool_system_expected_lifetime
                FROM %(schema)s.%(initial_or_new)s_agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s a
                LEFT JOIN diffusion_geo.hvac_life_expectancy b
                    ON a.space_heat_equip = b.space_equip
                    AND a.space_heat_fuel = b.space_fuel
                    AND b.space_type = 'heat'
                    AND b.sector_abbr = '%(sector_abbr)s'
                LEFT JOIN diffusion_geo.hvac_life_expectancy c
                    ON a.space_cool_equip = c.space_equip
                    AND a.space_cool_fuel = c.space_fuel
                    AND c.space_type = 'cool'
                    AND c.sector_abbr = '%(sector_abbr)s'
            )
            SELECT agent_id, space_heat_system_expected_lifetime, space_cool_system_expected_lifetime,
                    diffusion_shared.r_median(ARRAY[space_heat_system_expected_lifetime, space_cool_system_expected_lifetime]) as average_system_expected_lifetime
            FROM a;""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)

    # add primary key
    sql = """ALTER TABLE %(schema)s.%(initial_or_new)s_agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)


@decorators.fn_timer(logger=logger, tab_level=3, prefix='')
def sample_ground_thermal_conductivity(schema, sector_abbr, initial_or_new, chunks, pool, pg_conn_string, seed):

    msg = '\t\tSimulating Ground Thermal Conductivity for Buildings'
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['sector_abbr'] = sector_abbr
    inputs['initial_or_new'] = initial_or_new

    sql = """DROP TABLE IF EXISTS %(schema)s.%(initial_or_new)s_agent_gtc_%(sector_abbr)s_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.%(initial_or_new)s_agent_gtc_%(sector_abbr)s_%(i_place_holder)s AS
            SELECT a.agent_id,
                  (diffusion_shared.sample(array[c.q25, c.q50, c.q75],
                                           1,
                                           %(seed)s * a.agent_id,
                                           True,
                                           array[.375, .25, .375]))[1] as gtc_btu_per_hftf
            FROM %(schema)s.%(initial_or_new)s_agent_blocks_%(sector_abbr)s_%(i_place_holder)s a
            LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                     ON a.pgid = b.pgid
            LEFT JOIN diffusion_geo.thermal_conductivity_summary_by_climate_zone_ornl c
                ON b.iecc_climate_zone = c.iecc_climate_zone;""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)
    # TODO: should be using
    # diffusion_geo.thermal_conductivity_summary_by_climate_zone, but not sure
    # what ORNL used so have to use theirs for now

    # add primary key
    sql = """ALTER TABLE %(schema)s.%(initial_or_new)s_agent_gtc_%(sector_abbr)s_%(i_place_holder)s
             ADD PRIMARY KEY (agent_id);""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)

    # make sure no nulls
    sql = """ALTER TABLE %(schema)s.%(initial_or_new)s_agent_gtc_%(sector_abbr)s_%(i_place_holder)s
             ALTER COLUMN gtc_btu_per_hftf
             SET NOT NULL;""" % inputs
    p_run(pg_conn_string, sql, chunks, pool)


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def combine_all_attributes(chunks, pool, cur, con, pg_conn_string, schema, sector_abbr):

    msg = "\tCombining All Core Agent Attributes"
    logger.info(msg)

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(ids)s'

    sql_part = """SELECT a.agent_id,
                        a.year,
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
                         b.hdf_load_index,
                         b.iecc_temperature_zone,
                         b.iecc_climate_zone,
                    	c.bldg_type as hazus_bldg_type,

                         -- thermal load
                    	d.buildings_in_bin,
                        d.site_space_heat_in_bin_kwh,
                        d.site_space_cool_in_bin_kwh,
                        d.site_water_heat_in_bin_kwh,
                        d.site_total_heat_in_bin_kwh,
                        d.site_space_heat_per_building_in_bin_kwh,
                        d.site_space_cool_per_building_in_bin_kwh,
                        d.site_water_heat_per_building_in_bin_kwh,
                        d.site_total_heat_per_building_in_bin_kwh,
                        d.demand_space_heat_in_bin_kwh,
                        d.demand_space_cool_in_bin_kwh,
                        d.demand_water_heat_in_bin_kwh,
                        d.demand_total_heat_in_bin_kwh,
                        d.demand_space_heat_per_building_in_bin_kwh,
                        d.demand_space_cool_per_building_in_bin_kwh,
                        d.demand_water_heat_per_building_in_bin_kwh,
                        d.demand_total_heat_per_building_in_bin_kwh,

                         -- system ages
                    	e.space_heat_system_age,
                    	e.space_cool_system_age,
                    	e.average_system_age,

                         -- system lifetimes
                    	f.space_heat_system_expected_lifetime,
                    	f.space_cool_system_expected_lifetime,
                    	f.average_system_expected_lifetime,

                         -- eia microdata attributes
                    	h.eia_bldg_id,
                    	h.eia_bldg_weight,
                    	h.climate_zone,
                    	h.pba,
                    	h.pbaplus,
                    	h.typehuq,
                        h.pba_or_typehuq,
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
                        h.space_heat_efficiency,
                        h.space_cool_efficiency,
                        h.water_heat_efficiency,
                    	h.totsqft,
                    	h.totsqft_heat,
                    	h.totsqft_cool,
                        h.crb_model,

                        -- resource
                        i.gtc_btu_per_hftf

                    FROM %(schema)s.initial_agent_blocks_%(sector_abbr)s_%(i_place_holder)s a
                    LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                        ON a.pgid = b.pgid
                    LEFT JOIN %(schema)s.initial_agent_building_types_%(sector_abbr)s_%(i_place_holder)s c
                        ON a.agent_id = c.agent_id
                    LEFT JOIN %(schema)s.initial_agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s d
                        ON a.agent_id = d.agent_id
                    LEFT JOIN %(schema)s.initial_agent_system_ages_%(sector_abbr)s_%(i_place_holder)s e
                        ON a.agent_id = e.agent_id
                    LEFT JOIN %(schema)s.initial_agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s f
                        ON a.agent_id = f.agent_id
                    LEFT JOIN %(schema)s.initial_agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s h
                        ON a.agent_id = h.agent_id
                    LEFT JOIN %(schema)s.initial_agent_gtc_%(sector_abbr)s_%(i_place_holder)s i
                        ON a.agent_id = i.agent_id

            UNION ALL

                   SELECT a.agent_id,
                        a.year,
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
                         b.hdf_load_index,
                         b.iecc_temperature_zone,
                         b.iecc_climate_zone,
                    	c.bldg_type as hazus_bldg_type,

                         -- thermal load
                    	d.buildings_in_bin,
                        d.site_space_heat_in_bin_kwh,
                        d.site_space_cool_in_bin_kwh,
                        d.site_water_heat_in_bin_kwh,
                        d.site_total_heat_in_bin_kwh,
                        d.site_space_heat_per_building_in_bin_kwh,
                        d.site_space_cool_per_building_in_bin_kwh,
                        d.site_water_heat_per_building_in_bin_kwh,
                        d.site_total_heat_per_building_in_bin_kwh,
                        d.demand_space_heat_in_bin_kwh,
                        d.demand_space_cool_in_bin_kwh,
                        d.demand_water_heat_in_bin_kwh,
                        d.demand_total_heat_in_bin_kwh,
                        d.demand_space_heat_per_building_in_bin_kwh,
                        d.demand_space_cool_per_building_in_bin_kwh,
                        d.demand_water_heat_per_building_in_bin_kwh,
                        d.demand_total_heat_per_building_in_bin_kwh,

                         -- system ages
                    	e.space_heat_system_age,
                    	e.space_cool_system_age,
                    	e.average_system_age,

                         -- system lifetimes
                    	f.space_heat_system_expected_lifetime,
                    	f.space_cool_system_expected_lifetime,
                    	f.average_system_expected_lifetime,

                         -- eia microdata attributes
                    	h.eia_bldg_id,
                    	h.eia_bldg_weight,
                    	h.climate_zone,
                    	h.pba,
                    	h.pbaplus,
                    	h.typehuq,
                        h.pba_or_typehuq,
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
                        h.space_heat_efficiency,
                        h.space_cool_efficiency,
                        h.water_heat_efficiency,
                    	h.totsqft,
                    	h.totsqft_heat,
                    	h.totsqft_cool,
                        h.crb_model,

                        -- resource
                        i.gtc_btu_per_hftf

                    FROM %(schema)s.new_agent_blocks_%(sector_abbr)s_%(i_place_holder)s a
                    LEFT JOIN %(schema)s.block_microdata_%(sector_abbr)s_joined b
                        ON a.pgid = b.pgid
                    LEFT JOIN %(schema)s.new_agent_building_types_%(sector_abbr)s_%(i_place_holder)s c
                        ON a.agent_id = c.agent_id
                    LEFT JOIN %(schema)s.new_agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s d
                        ON a.agent_id = d.agent_id
                    LEFT JOIN %(schema)s.new_agent_system_ages_%(sector_abbr)s_%(i_place_holder)s e
                        ON a.agent_id = e.agent_id
                    LEFT JOIN %(schema)s.new_agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s f
                        ON a.agent_id = f.agent_id
                    LEFT JOIN %(schema)s.new_agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s h
                        ON a.agent_id = h.agent_id
                    LEFT JOIN %(schema)s.new_agent_gtc_%(sector_abbr)s_%(i_place_holder)s i
                        ON a.agent_id = i.agent_id""" % inputs

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
    sql = """CREATE INDEX agent_core_attributes_%(sector_abbr)s_btree_crb_model
             ON %(schema)s.agent_core_attributes_%(sector_abbr)s
             USING BTREE(crb_model);

             CREATE INDEX agent_core_attributes_%(sector_abbr)s_btree_hdf_load_index
             ON %(schema)s.agent_core_attributes_%(sector_abbr)s
             USING BTREE(hdf_load_index);

             CREATE INDEX agent_core_attributes_%(sector_abbr)s_btree_year
             ON %(schema)s.agent_core_attributes_%(sector_abbr)s
             USING BTREE(year);
             """ % inputs
    cur.execute(sql)
    con.commit()


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def cleanup_intermediate_tables(schema, sectors, county_chunks,
                                pg_conn_string, cur, con, pool):

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'

    # ========================================================================
    #   clean up intermediate tables
    # ========================================================================
    msg = "\tCleaning Up Intermediate Tables..."
    logger.info(msg)
    intermediate_tables = [
        '%(schema)s.initial_agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.initial_agent_blocks_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.initial_agent_building_types_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.initial_agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.initial_agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.initial_agent_system_ages_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.initial_agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.initial_agent_gtc_%(sector_abbr)s_%(i_place_holder)s',

        '%(schema)s.new_agent_count_by_tract_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.new_agent_blocks_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.new_agent_building_types_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.new_agent_eia_bldgs_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.new_agent_thermal_loads_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.new_agent_system_ages_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.new_agent_system_expected_lifetimes_%(sector_abbr)s_%(i_place_holder)s',
        '%(schema)s.new_agent_gtc_%(sector_abbr)s_%(i_place_holder)s'
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


@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
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
