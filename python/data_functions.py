# -*- coding: utf-8 -*-
"""
Functions for pulling data
Created on Mon Mar 24 08:59:44 2014
@author: bsigrin
"""
import psycopg2 as pg
import psycopg2.extras as pgx
import psycopg2.extensions as pgxt
import pandas.io.sql as sqlio
import time   
import numpy as np
from scipy.interpolate import interp1d as interp1d
import pandas as pd
import datetime
from multiprocessing import Process, Queue, JoinableQueue
import select
from cStringIO import StringIO
import logging
reload(logging)
# note: need to install using pip install git+https://github.com/borntyping/python-colorlog.git#egg=colorlog
import colorlog
import colorama
import gzip
import subprocess
import os
import sys, getopt

def load_resume_vars(cfg, resume_year):
    # Load the variables necessary to resume the model
    if resume_year == 2014:
        cfg.init_model = True
        out_dir = None
        input_scenarios = None
        market_last_year = None
    else:
        cfg.init_model = False
        # Load files here
        market_last_year = pd.read_pickle("market_last_year.pkl")   
        with open('saved_vars.pickle', 'rb') as handle:
            saved_vars = pickle.load(handle)
        out_dir = saved_vars['out_dir']
        input_scenarios = saved_vars['input_scenarios']
    return cfg.init_model, out_dir, input_scenarios, market_last_year

def prep_model(cfg):               
    # Make output folder
    cdate = time.strftime('%Y%m%d_%H%M%S')    
    out_dir = '%s/runs/results_%s' %(os.path.dirname(os.getcwd()),cdate)        
    os.makedirs(out_dir)
    
    # check that random generator seed is in the acceptable range
    if cfg.random_generator_seed < 0 or cfg.random_generator_seed > 1:
        raise ValueError("""random_generator_seed in config.py is not in the range of acceptable values. Change to a value in the range >= 0 and <= 1.""")                           
        # check that number of customer bins is in the acceptable range
    if cfg.customer_bins not in (10,50,100,500):
        raise ValueError("""Error: customer_bins in config.py is not in the range of acceptable values. Change to a value in the set (10,50,100,500).""") 
    model_init = time.time()
    return out_dir, model_init
def parse_command_args(argv):
    ''' Function to parse the command line arguments
    IN:
    
    -h : help 'dg_model.py -i <Initiate Model?> -y <year>'
    -i : Initiate model for 2010 and quit
    -y: or year= : Resume model solve in passed year
    
    OUT:
    
    init_model - Boolean - Should model initiate?
    resume_year - Float - year model should resume
    '''
    
    resume_year = None
    init_model = False
    
    try:
        opts, args = getopt.getopt(argv,"hiy:",["year="])
    except getopt.GetoptError:
        print 'Command line argument not recognized, please use: dg_model.py -i -y <year>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'dg_model.py -i <Initiate Model?> -y <year>'
            sys.exit()
        elif opt in ("-i"):
            init_model = True
        elif opt in ("-y", "year="):
            resume_year = arg
    return init_model, resume_year 

def init_log(log_file_path):
    
    colorama.init()
    logging.basicConfig(filename = log_file_path, filemode = 'w', format='%(levelname)-8s:%(message)s', level = logging.DEBUG)   
    logger = logging.getLogger(__name__)
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(levelname)-8s:%(reset)s %(white)s%(message)s",
        datefmt=None,
        reset=True
        )     
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)
    logger.addHandler(console)
    
    return logger

def shutdown_log(logger):
    logging.shutdown()
    for handler in logger.handlers:
        handler.flush()
        handler.close()
        logger.removeHandler(handler)

def wait(conn):
    while 1:
        state = conn.poll()
        if state == pg.extensions.POLL_OK:
            break
        elif state == pg.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == pg.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise pg.OperationalError("poll() returned %s" % state)


def pylist_2_pglist(l):
    return str(l)[1:-1]

def make_con(connection_string, async = False):    
    con = pg.connect(connection_string, async = async)
    if async:
        wait(con)
    # create cursor object
    cur = con.cursor(cursor_factory=pgx.RealDictCursor)
    # set role (this should avoid permissions issues)
    cur.execute('SET ROLE "wind_ds-writers";')    
    if async:
        wait(con)
    else:
        con.commit()
    
    return con, cur


def combine_temporal_data(cur, con, start_year, end_year, sectors, preprocess, logger):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()       

    msg = "Combining Temporal Factors"    
    logger.info(msg)

    t0 = time.time()    
    if preprocess:
        return 1
    
    # combine all of the temporal data (this only needs to be done once for all sectors)
    sql = """DROP TABLE IF EXISTS diffusion_wind.temporal_factors;
            CREATE TABLE diffusion_wind.temporal_factors as 
            SELECT a.year, a.turbine_size_kw, a.power_curve_id,
            	b.turbine_height_m,
            	c.fixed_om_dollars_per_kw_per_yr, 
            	c.variable_om_dollars_per_kwh,
            	c.installed_costs_dollars_per_kw,
            	d.census_division_abbr,
            	d.sector,
            	d.escalation_factor as rate_escalation_factor,
            	d.source as rate_escalation_source,
            	e.scenario as load_growth_scenario,
            	e.load_multiplier,
            f.carbon_dollars_per_ton,
            g.derate_factor
            FROM diffusion_wind.wind_performance_improvements a
            LEFT JOIN diffusion_wind.allowable_turbine_sizes b
            ON a.turbine_size_kw = b.turbine_size_kw
            LEFT JOIN diffusion_wind.turbine_costs_per_size_and_year c
            ON a.turbine_size_kw = c.turbine_size_kw
            AND a.year = c.year
            LEFT JOIN diffusion_wind.rate_escalations_to_model d
            ON a.year = d.year
            LEFT JOIN diffusion_shared.aeo_load_growth_projections e
            ON d.census_division_abbr = e.census_division_abbr
            AND a.year = e.year
            LEFT JOIN diffusion_wind.market_projections f
            ON a.year = f.year
            LEFT JOIN diffusion_wind.wind_generation_derate_factors g
            ON a.year = g.year
            AND  a.turbine_size_kw = g.turbine_size_kw
            WHERE a.year BETWEEN %(start_year)s AND %(end_year)s
            AND d.sector in (%(sectors)s);""" % inputs
    cur.execute(sql)
    con.commit()
    
    # create indices for subsequent joins
    sql =  """CREATE INDEX temporal_factors_turbine_height_m_btree 
              ON diffusion_wind.temporal_factors 
              USING BTREE(turbine_height_m);
              
              CREATE INDEX temporal_factors_sector_btree 
              ON diffusion_wind.temporal_factors 
              USING BTREE(sector);
              
              CREATE INDEX temporal_factors_load_growth_scenario_btree 
              ON diffusion_wind.temporal_factors 
              USING BTREE(load_growth_scenario);
              
              CREATE INDEX temporal_factors_rate_escalation_source_btree 
              ON diffusion_wind.temporal_factors 
              USING BTREE(rate_escalation_source);
              
              CREATE INDEX temporal_factors_census_division_abbr_btree 
              ON diffusion_wind.temporal_factors 
              USING BTREE(census_division_abbr);
              
              CREATE INDEX temporal_factors_join_fields_btree 
              ON diffusion_wind.temporal_factors 
              USING BTREE(turbine_height_m, census_division_abbr, power_curve_id);"""
    cur.execute(sql)
    con.commit()  
    
    return 1
    
def clear_outputs(con,cur):
    """Delete all rows from the res, com, and ind output tables"""
    
    sql = """DELETE FROM diffusion_wind.outputs_res;
            DELETE FROM diffusion_wind.outputs_com;
            DELETE FROM diffusion_wind.outputs_ind;""" 
    cur.execute(sql)
    con.commit()
    
#def clear_outputs(con,cur, sector_abbr):
#    """Delete all rows from the output table"""
#    
#    sql = """DELETE FROM diffusion_wind.outputs_%s""" % sector_abbr
#    cur.execute(sql)
#    con.commit()


def write_outputs(con, cur, outputs_df, sector_abbr):
    
    # set fields to write
    fields = ['gid',
            'year',
            'customer_expec_elec_rates',
            'ownership_model',
            'loan_term_yrs',
            'loan_rate',
            'down_payment',
            'discount_rate',
            'tax_rate',
            'length_of_irr_analysis_yrs',
            'market_share_last_year',
            'number_of_adopters_last_year',
            'installed_capacity_last_year',
            'market_value_last_year',
            'value_of_increment',
            'value_of_pbi_fit',
            'value_of_ptc',
            'pbi_fit_length',
            'ptc_length',
            'value_of_rebate',
            'value_of_tax_credit_or_deduction',
            'ic',
            'payback_period',
            'lcoe',
            'payback_key',
            'max_market_share',
            'diffusion_market_share',
            'new_market_share',
            'new_adopters',
            'new_capacity',
            'new_market_value',
            'market_share',
            'number_of_adopters',
            'installed_capacity',
            'market_value']    
    # convert formatting of fields list
    fields_str = pylist_2_pglist(fields).replace("'","")       
    # open an in memory stringIO file (like an in memory csv)
    s = StringIO()
    # write the data to the stringIO
    outputs_df[fields].to_csv(s, index = False, header = False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    cur.copy_expert('COPY diffusion_wind.outputs_%s (%s) FROM STDOUT WITH CSV' % (sector_abbr,fields_str), s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()    
    s.close()
    

    
    
def p_execute(pg_conn_string, sql):
    # create cursor and connection
    con, cur = make_con(pg_conn_string)  
    # execute query
    cur.execute(sql)
    # commit changes
    con.commit()
    # close cursor and connection
    con.close()
    cur.close()

    
def p_run(pg_conn_string, sql, county_chunks, npar):
    
    jobs = []
    for i in range(npar):
        place_holders = {'i': i, 'county_ids': pylist_2_pglist(county_chunks[i])}
        isql = sql % place_holders
        proc = Process(target = p_execute, args = (pg_conn_string, isql))
        jobs.append(proc)
        proc.start()
    for job in jobs:
        job.join()   


def copy_outputs_to_csv(out_path, sectors, cur, con):
    
    sql = '''DROP TABLE IF EXISTS diffusion_wind.outputs_all;
            CREATE TABLE diffusion_wind.outputs_all AS  '''    
    
    for i, sector_abbr in enumerate(sectors.keys()):
        sector = sectors[sector_abbr].lower()
        if i > 0:
            union = 'UNION ALL '
        else:
            union = ''
        
        sub_sql = '''%s 
                    SELECT '%s'::text as sector, 

                    a.gid, a.year, a.customer_expec_elec_rates, a.ownership_model, a.loan_term_yrs, 
                    a.loan_rate, a.down_payment, a.discount_rate, a.tax_rate, a.length_of_irr_analysis_yrs, 
                    a.market_share_last_year, a.number_of_adopters_last_year, a.installed_capacity_last_year, 
                    a.market_value_last_year, a.value_of_increment, a.value_of_pbi_fit, 
                    a.value_of_ptc, a.pbi_fit_length, a.ptc_length, a.value_of_rebate, a.value_of_tax_credit_or_deduction, 
                    a.ic, a.payback_period, a.lcoe, a.payback_key, a.max_market_share, 
                    a.diffusion_market_share, a.new_market_share, a.new_adopters, a.new_capacity, 
                    a.new_market_value, a.market_share, a.number_of_adopters, a.installed_capacity, 
                    a.market_value,
                    
                    b.county_id, b.state_abbr, b.census_division_abbr, b.utility_type, 
                    b.census_region, b.pca_reg, b.reeds_reg, b.row_number, b.max_height, b.elec_rate_cents_per_kwh, 
                    b.carbon_price_cents_per_kwh, b.cap_cost_multiplier, b.fixed_om_dollars_per_kw_per_yr, 
                    b.variable_om_dollars_per_kwh, b.installed_costs_dollars_per_kw, 
                    b.ann_cons_kwh, b.prob, b.weight, b.customers_in_bin, b.initial_customers_in_bin, 
                    b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
                    b.nem_system_limit_kw, b.excess_generation_factor, b.i, b.j, b.cf_bin, 
                    b.aep_scale_factor, b.derate_factor, b.naep, b.aep, b.system_size_kw,
                    b.nturb, b.turbine_size_kw, 
                    b.power_curve_id, b.turbine_height_m, b.scoe,
                    
                    c.initial_market_share, c.initial_number_of_adopters,
                    c.initial_capacity_mw
                    
                    FROM diffusion_wind.outputs_%s a
                    
                    LEFT JOIN diffusion_wind.pt_%s_best_option_each_year b
                    ON a.gid = b.gid
                    and a.year = b.year
                    
                    LEFT JOIN diffusion_wind.pt_%s_initial_market_shares c
                    ON a.gid = c.gid
                    ''' % (union, sector, sector_abbr, sector_abbr, sector_abbr)
        sql += sub_sql
    
    sql += ';'
    cur.execute(sql)
    con.commit()

    # create indices that will be needed for various aggregations in R visualization script
    sql = '''CREATE INDEX outputs_all_year_btree ON diffusion_wind.outputs_all USING BTREE(year);
             CREATE INDEX outputs_all_state_abbr_btree ON diffusion_wind.outputs_all USING BTREE(state_abbr);
             CREATE INDEX outputs_all_sector_btree ON diffusion_wind.outputs_all USING BTREE(sector);
             CREATE INDEX outputs_all_turbine_size_kw_btree ON diffusion_wind.outputs_all USING BTREE(turbine_size_kw);
             CREATE INDEX outputs_all_turbine_height_m_btree ON diffusion_wind.outputs_all USING BTREE(turbine_height_m);'''
    cur.execute(sql)
    con.commit()

    # copy data to csv
    f = gzip.open(out_path+'/outputs.csv.gz','w')
    cur.copy_expert('COPY diffusion_wind.outputs_all TO STDOUT WITH CSV HEADER;', f)
    f.close()
    
    # write the scenario optoins to csv as well
    f2 = open(out_path+'/scenario_options_summary.csv','w')
    cur.copy_expert('COPY diffusion_wind.scenario_options TO STDOUT WITH CSV HEADER;',f2)
    f2.close()

def create_scenario_report(scen_name, out_path, cur, con, Rscript_path, logger = None):
           
    # path to the plot_outputs R script        
    plot_outputs_path = '%s/r/graphics/plot_outputs.R' % os.path.dirname(os.getcwd())        
    
    #command = ("%s --vanilla ../r/graphics/plot_outputs.R %s" %(Rscript_path, runpath))
    # for linux and mac, this needs to be formatted as a list of args passed to subprocess
    command = [Rscript_path,'--vanilla',plot_outputs_path,out_path,scen_name]
    msg = 'Creating outputs report'
    if logger is not None:            
        logger.info(msg)
    else:
        print msg
    proc = subprocess.Popen(command,stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    messages = proc.communicate()
    if 'error' in messages[1].lower():
        if logger is not None:
            logger.error(messages[1])
        else:
            print "Error: %s" % messages[1]
    if 'warning' in messages[1].lower():
        if logger is not None:
            logger.warning(messages[1])
        else:
            print "Warning: %s" % messages[1]
    returncode = proc.returncode    

    

def generate_customer_bins(cur, con, seed, n_bins, sector_abbr, sector, start_year, end_year, 
                           rate_escalation_source, load_growth_scenario, exclusion_type, oversize_turbine_factor,undersize_turbine_factor,
                           preprocess, npar, pg_conn_string, nem_availability, logger):

    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()  
    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'
    inputs['seed_str'] = str(seed).replace('.','p')
    
    msg = "Setting up %(sector)s Customer Profiles by County for Scenario Run" % inputs
    logger.info(msg)
     
    if preprocess == True:
        table_name_dict = {'res': 'diffusion_wind.pt_res_best_option_each_year', 'com' : 'diffusion_wind.pt_com_best_option_each_year', 'ind' : 'diffusion_wind.pt_ind_best_option_each_year'}
        return table_name_dict[sector_abbr]
    
    #==============================================================================
    #     break counties into subsets for parallel processing
    #==============================================================================
    # get list of counties
    sql =   """SELECT county_id 
               FROM diffusion_wind.counties_to_model
               ORDER BY county_id;"""
    cur.execute(sql)
    counties = [row['county_id'] for row in cur.fetchall()]
    county_chunks = map(list,np.array_split(counties, npar))
    

    #==============================================================================
    #     check whether seed has been used before -- if not, create a new random lookup table
    #==============================================================================
    sql = """SELECT seed 
             FROM diffusion_wind.prior_seeds_%(sector_abbr)s;""" % inputs
    cur.execute(sql)
    prior_seeds = [float(row['seed']) for row in cur.fetchall()]
    inputs['random_lookup_table'] = 'random_lookup_%(sector_abbr)s_%(seed_str)s' % inputs
    
    if seed not in prior_seeds:
        msg = "New Seed: Generating Random Values for Sampling"
        logger.info(msg)
        # generate the random lookup table
        sql = """CREATE TABLE diffusion_wind.%(random_lookup_table)s AS
                 WITH 
                     s as (SELECT setseed(%(seed)s)),
                     p as (SELECT a.gid FROM diffusion_shared.pt_grid_us_%(sector_abbr)s a 
                       ORDER BY a.gid)
                 SELECT p.gid, random() as random
                 FROM p, s;""" % inputs
        cur.execute(sql)
        con.commit()
        
        # add a primary key lookup
        sql = """ALTER TABLE diffusion_wind.%(random_lookup_table)s
                 ADD PRIMARY KEY (gid);""" % inputs
        cur.execute(sql)
        con.commit()        
        
        # add seed to the prior seeds table
        sql = """INSERT INTO diffusion_wind.prior_seeds_%(sector_abbr)s (seed) VALUES (%(seed)s);""" % inputs
        cur.execute(sql)
        con.commit()
        
        # vacuum analyze the lookup table
        sql = "VACUUM ANALYZE diffusion_wind.%(random_lookup_table)s;" % inputs
        con.autocommit = True
        cur.execute(sql)
        con.autocommit = False
        
    else:
        msg = "This seed has been used previously. Skipping generation of random values"
        logger.warning(msg)

    
    #==============================================================================
    #     randomly sample  N points from each county 
    #==============================================================================    
    # (note: some counties will have fewer than N points, in which case, all are returned) 
    msg = 'Sampling Customer Bins from Each County'
    logger.info(msg)
    t0 = time.time() 
    sql = """DROP TABLE IF EXISTS diffusion_wind.pt_%(sector_abbr)s_sample_%(i_place_holder)s;
             CREATE TABLE diffusion_wind.pt_%(sector_abbr)s_sample_%(i_place_holder)s AS
             WITH a as (
            	SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a.county_id order by b.random, a.gid) as row_number
            	FROM diffusion_wind.pt_grid_us_%(sector_abbr)s_joined a
              LEFT JOIN diffusion_wind.%(random_lookup_table)s b
              ON a.gid = b.gid
              WHERE a.county_id IN (%(chunk_place_holder)s))
            SELECT *
            FROM a
            WHERE row_number <= %(n_bins)s;""" % inputs    

    p_run(pg_conn_string, sql, county_chunks, npar)

    #==============================================================================
    #    create lookup table with random values for each load bin 
    #==============================================================================
    msg = "Setting up randomized load bins"
    logger.info(msg)
    t0 = time.time()
    sql =  """DROP TABLE IF EXISTS diffusion_wind.county_load_bins_random_lookup_%(sector_abbr)s;
             CREATE TABLE diffusion_wind.county_load_bins_random_lookup_%(sector_abbr)s AS
             WITH s as (SELECT setseed(%(seed)s))
                	SELECT a.county_id, 
                         row_number() OVER (PARTITION BY a.county_id ORDER BY random() * b.prob) as row_number, 
                         b.*
                	FROM s, diffusion_wind.counties_to_model a
                	LEFT JOIN diffusion_shared.binned_annual_load_kwh_%(n_bins)s_bins b
                	ON a.census_region = b.census_region
                	AND b.sector = lower('%(sector)s');""" % inputs
    cur.execute(sql)
    con.commit()
    
    # add an index on county id and row number
    sql = """CREATE INDEX county_load_bins_random_lookup_%(sector_abbr)s_join_fields_btree 
            ON diffusion_wind.county_load_bins_random_lookup_%(sector_abbr)s USING BTREE(county_id, row_number);""" % inputs
    cur.execute(sql)
    con.commit()
   
    #==============================================================================
    #     link each point to a load bin
    #==============================================================================
    # use random weighted sampling on the load bins to ensure that countyies with <N points
    # have a representative sample of load bins 
    msg = 'Associating Customer Bins with Load and Customer Count'    
    logger.info(msg)
    sql =  """DROP TABLE IF EXISTS diffusion_wind.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s;
            CREATE TABLE diffusion_wind.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s AS
            WITH binned as(
            SELECT a.*, b.ann_cons_kwh, b.prob, b.weight,
            	a.county_total_customers_2011 * b.weight/sum(weight) OVER (PARTITION BY a.county_id) as customers_in_bin, 
            	a.county_total_load_mwh_2011 * 1000 * (b.ann_cons_kwh*b.weight)/sum(b.ann_cons_kwh*b.weight) OVER (PARTITION BY a.county_id) as load_kwh_in_bin
            FROM diffusion_wind.pt_%(sector_abbr)s_sample_%(i_place_holder)s a
            LEFT JOIN diffusion_wind.county_load_bins_random_lookup_%(sector_abbr)s b
            ON a.county_id = b.county_id
            and a.row_number = b.row_number
            where county_total_load_mwh_2011 > 0)
            SELECT a.*,
            	CASE WHEN a.customers_in_bin > 0 THEN a.load_kwh_in_bin/a.customers_in_bin 
            	ELSE 0
            	END AS load_kwh_per_customer_in_bin
            FROM binned a;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)

    # query for indices creation
    sql =  """ALTER TABLE diffusion_wind.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s 
              ADD PRIMARY Key (gid);
              
              CREATE INDEX pt_%(sector_abbr)s_sample_load_%(i_place_holder)s_census_division_abbr_btree 
              ON diffusion_wind.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s 
              USING BTREE(census_division_abbr);
              
              CREATE INDEX pt_%(sector_abbr)s_sample_load_%(i_place_holder)s_i_j_cf_bin 
              ON diffusion_wind.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s 
              USING BTREE(i,j,cf_bin);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)


    # add index for exclusions (if they apply)
    if exclusion_type is not None:
        sql =  """CREATE INDEX pt_%(sector_abbr)s_sample_load_%(i_place_holder)s_%(exclusion_type)s_btree 
                  ON diffusion_wind.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s 
                  USING BTREE(%(exclusion_type)s)
                  WHERE %(exclusion_type)s > 0;""" % inputs
        p_run(pg_conn_string, sql, county_chunks, npar)
    
    #==============================================================================
    #     Find All Combinations of Points and Wind Resource
    #==============================================================================  
    msg = "Finding All Wind Resource Combinations for Each Customer Bin"
    logger.info(msg)
    sql =  """DROP TABLE IF EXISTS diffusion_wind.pt_%(sector_abbr)s_sample_load_and_wind_%(i_place_holder)s;
                CREATE TABLE diffusion_wind.pt_%(sector_abbr)s_sample_load_and_wind_%(i_place_holder)s AS
                SELECT a.*,
                c.aep*a.aep_scale_factor as naep_no_derate,
                c.turbine_id as power_curve_id, 
                c.height as turbine_height_m,
                c.excess_gen_factor as excess_generation_factor
                FROM diffusion_wind.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s a
                LEFT JOIN diffusion_wind.wind_resource_annual c
                ON a.i = c.i
                AND a.j = c.j
                AND a.cf_bin = c.cf_bin""" % inputs
    if exclusion_type is not None:
        sql += """ AND a.%(exclusion_type)s >= c.height
                WHERE a.%(exclusion_type)s > 0;""" % inputs 
    else:
        sql += ';'
    p_run(pg_conn_string, sql, county_chunks, npar)

    # create indices for subsequent joins
    sql =  """CREATE INDEX pt_%(sector_abbr)s_sample_load_and_wind_%(i_place_holder)s_join_fields_btree 
              ON diffusion_wind.pt_%(sector_abbr)s_sample_load_and_wind_%(i_place_holder)s 
              USING BTREE(turbine_height_m, census_division_abbr, power_curve_id);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    #==============================================================================
    #     Find All Combinations of Costs and Resource for Each Customer Bin
    #==============================================================================
    msg = "Finding All Combinations of Cost and Resource for Each Customer Bin and Year"
    logger.info(msg)
    if exclusion_type is not None:
        inputs['exclusions_insert'] = "a.%(exclusion_type)s as max_height," % inputs
    else:
        inputs['exclusions_insert'] = ""
        
    sql =  """DROP TABLE IF EXISTS diffusion_wind.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s;
            CREATE TABLE diffusion_wind.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s AS
            WITH combined AS
            (
                SELECT
                 	a.gid, b.year, a.county_id, a.state_abbr, a.census_division_abbr, 
                      a.utility_type, a.census_region, a.pca_reg, a.reeds_reg, a.row_number, 
                      %(exclusions_insert)s
                	(a.elec_rate_cents_per_kwh * b.rate_escalation_factor) + (b.carbon_dollars_per_ton * 100 * a.carbon_intensity_t_per_kwh) as elec_rate_cents_per_kwh, 
                b.carbon_dollars_per_ton * 100 * a.carbon_intensity_t_per_kwh as  carbon_price_cents_per_kwh,
                	a.cap_cost_multiplier,
                	b.fixed_om_dollars_per_kw_per_yr, 
                	b.variable_om_dollars_per_kwh,
                	b.installed_costs_dollars_per_kw * a.cap_cost_multiplier::numeric as installed_costs_dollars_per_kw,
                	a.ann_cons_kwh, a.prob, a.weight,
                	b.load_multiplier * a.customers_in_bin as customers_in_bin, 
                	a.customers_in_bin as initial_customers_in_bin, 
                	b.load_multiplier * a.load_kwh_in_bin AS load_kwh_in_bin,
                	a.load_kwh_in_bin AS initial_load_kwh_in_bin,
                	a.load_kwh_per_customer_in_bin,
                a.nem_system_limit_kw,
                a.excess_generation_factor,
                	a.i, a.j, a.cf_bin, a.aep_scale_factor, b.derate_factor,
                	a.naep_no_derate * b.derate_factor as naep,
                	b.turbine_size_kw,
                	a.power_curve_id, 
                	a.turbine_height_m,
                	diffusion_wind.scoe(b.installed_costs_dollars_per_kw * a.cap_cost_multiplier::numeric, b.fixed_om_dollars_per_kw_per_yr, 
                              b.variable_om_dollars_per_kwh, a.naep_no_derate * b.derate_factor, b.turbine_size_kw , 
                              a.load_kwh_per_customer_in_bin , a.nem_system_limit_kw, a.excess_generation_factor, 
                              '%(nem_availability)s', %(oversize_turbine_factor)s, %(undersize_turbine_factor)s) as scoe_return
                FROM diffusion_wind.pt_%(sector_abbr)s_sample_load_and_wind_%(i_place_holder)s a
                INNER JOIN diffusion_wind.temporal_factors b
                ON a.turbine_height_m = b.turbine_height_m
                AND a.power_curve_id = b.power_curve_id
                AND a.census_division_abbr = b.census_division_abbr
                WHERE b.sector = '%(sector)s'
                AND b.rate_escalation_source = '%(rate_escalation_source)s'
                AND b.load_growth_scenario = '%(load_growth_scenario)s'
            )
            SELECT gid, year, county_id, state_abbr, census_division_abbr, utility_type, 
               census_region, pca_reg, reeds_reg, row_number, max_height, elec_rate_cents_per_kwh, 
               carbon_price_cents_per_kwh, cap_cost_multiplier, 
        
               fixed_om_dollars_per_kw_per_yr, 
               variable_om_dollars_per_kwh, 
               installed_costs_dollars_per_kw, 
        
               ann_cons_kwh, prob, weight, customers_in_bin, initial_customers_in_bin, 
               load_kwh_in_bin, initial_load_kwh_in_bin, load_kwh_per_customer_in_bin, 
               nem_system_limit_kw, excess_generation_factor, i, j, cf_bin, 
               aep_scale_factor, derate_factor, 
        
               naep, 

               naep*(scoe_return).nturb*turbine_size_kw as aep,
               (scoe_return).nturb*turbine_size_kw as system_size_kw,
               (scoe_return).nturb as nturb,

               turbine_size_kw, 
               power_curve_id, 
               turbine_height_m, 
               (scoe_return).scoe as scoe
          FROM combined;""" % inputs

        
    p_run(pg_conn_string, sql, county_chunks, npar)

    # NOTE: not worth creating indices for this one -- it wil only slow down the processing    

    #==============================================================================
    #    Find the Most Cost-Effective Wind Turbine Configuration for Each Customer Bin
    #==============================================================================
    msg = "Selecting the most cost-effective wind turbine configuration for each customer bin and year"
    logger.info(msg)
    # create empty table
    sql = """DROP TABLE IF EXISTS diffusion_wind.pt_%(sector_abbr)s_best_option_each_year;
            CREATE TABLE diffusion_wind.pt_%(sector_abbr)s_best_option_each_year AS
            SELECT *
            FROM diffusion_wind.pt_%(sector_abbr)s_sample_all_combinations_0
            LIMIT 0;""" % inputs    
    cur.execute(sql)
    con.commit()
    
    sql =  """INSERT INTO diffusion_wind.pt_%(sector_abbr)s_best_option_each_year
              SELECT distinct on (a.gid, a.year) a.*
              FROM  diffusion_wind.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s a
              ORDER BY a.gid, a.year, a.scoe ASC;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    # create index on gid and year
    sql = """CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_gid_btree 
             ON diffusion_wind.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(gid);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_year_btree 
             ON diffusion_wind.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(year);
            """ % inputs
    cur.execute(sql)
    con.commit()
    
    #==============================================================================
    #   clean up intermediate tables
    #==============================================================================
    msg = "Cleaning up intermediate tables"
    logger.info(msg)
    intermediate_tables = ['diffusion_wind.pt_%(sector_abbr)s_sample_%(i_place_holder)s' % inputs,
                       'diffusion_wind.county_load_bins_random_lookup_%(sector_abbr)s' % inputs,
                       'diffusion_wind.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s' % inputs,
                       'diffusion_wind.pt_%(sector_abbr)s_sample_load_and_wind_%(i_place_holder)s' % inputs,
                       'diffusion_wind.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s' % inputs]
        
         
    sql = 'DROP TABLE IF EXISTS %s;'
    for intermediate_table in intermediate_tables:
        isql = sql % intermediate_table
        if '%(i)s' in intermediate_table:
            p_run(pg_conn_string, isql, county_chunks, npar)
        else:
            cur.execute(isql)
            con.commit()    

    #==============================================================================
    #     return name of final table
    #==============================================================================
    final_table = 'diffusion_wind.pt_%(sector_abbr)s_best_option_each_year' % inputs
    return final_table

def get_sectors(cur):
    '''Return the sectors to model from table view in postgres.
        Returned as a dictionary.
        '''    
    
    sql = 'SELECT sectors FROM diffusion_wind.sectors_to_model;'
    cur.execute(sql)
    sectors = cur.fetchone()['sectors']
    return sectors
    
    
def get_exclusions(cur):
    '''Return the sectors to model from table view in postgres.
        Returned as a dictionary.
        '''    
    
    sql = 'SELECT * FROM diffusion_wind.exclusions_to_model;'
    cur.execute(sql)
    exclusions = cur.fetchone()['exclusions']
    return exclusions
    
def get_depreciation_schedule(con, type = 'all'):
    ''' Pull depreciation schedule from dB
    
        IN: type - string - [all, macrs, standard] 
        OUT: df  - pd dataframe - year, depreciation schedule:

    '''
    if not con:
        close_con = True
        con = make_con()
    else:
        close_con = False    
    if type.lower() == 'macrs':
        sql = 'SELECT macrs FROM diffusion_wind.depreciation_schedule'
    elif type.lower() == 'standard':
        sql = 'SELECT standard FROM diffusion_wind.depreciation_schedule'
    else:
        sql = 'SELECT * FROM diffusion_wind.depreciation_schedule'
    df = sqlio.read_frame(sql, con)
    return df
    
def get_scenario_options(cur):
    ''' Pull scenario options from dB
    
        IN: none
        OUT: scenario_options - pandas data frame:
                    'region', 
                    'end_year', 
                    'markets', 
                    'cust_exp_elec_rates', 
                    'res_rate_structure', 
                    'res_rate_escalation', 
                    'res_max_market_curve', 
                    'com_rate_structure', 
                    'com_rate_escalation', 
                    'com_max_market_curve', 
                    'ind_rate_structure', 
                    'ind_rate_escalation', 
                    'ind_max_market_curve', 
                    'net_metering_availability', 
                    'carbon_price', 
                    'height_exclusions', 
                    'ann_inflation', 
                    'scenario_name', 
                    'overwrite_exist_inc', 
                    'starting_year', 
                    'utility_type_iou', 
                    'utility_type_muni', 
                    'utility_type_coop', 
                    'utility_type_allother'
        
    '''
    sql = "SELECT * FROM diffusion_wind.scenario_options"
    cur.execute(sql)
    results = cur.fetchall()[0]
    return results

def get_dsire_incentives(cur, con, sector_abbr, preprocess, npar, pg_conn_string, logger):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()
    inputs['chunk_place_holder'] = '%(county_ids)s'

    msg = "Identifying initial incentives for customer bins from DSIRE Database"
    logger.info(msg)
    if not preprocess:
        # adjust the name of the sector for incentives table (ind doesn't exist in dsire -- use com)
        if sector_abbr == 'ind':
            inputs['incentives_sector'] = 'com'
        else:
            inputs['incentives_sector'] = sector_abbr           
        
        #==============================================================================
        #     break counties into subsets for parallel processing
        #==============================================================================
        # get list of counties
        sql =   """SELECT county_id 
                   FROM diffusion_wind.counties_to_model
                   ORDER BY county_id;"""
        cur.execute(sql)
        counties = [row['county_id'] for row in cur.fetchall()]
        county_chunks = map(list,np.array_split(counties, npar))        
        
        # initialize the output table
        t0 = time.time()  
        sql = """DROP TABLE IF EXISTS diffusion_wind.pt_%(sector_abbr)s_incentives;
                CREATE TABLE diffusion_wind.pt_%(sector_abbr)s_incentives
                    (
                      gid integer,
                      uid integer,
                      incentive_id integer,
                      increment_1_capacity_kw numeric,
                      increment_2_capacity_kw numeric,
                      increment_3_capacity_kw numeric,
                      pbi_fit_duration_years numeric,
                      pbi_fit_end_date date,
                      pbi_fit_max_size_kw numeric,
                      pbi_fit_min_output_kwh_yr numeric,
                      pbi_fit_min_size_kw numeric,
                      ptc_duration_years numeric,
                      ptc_end_date date,
                      rating_basis_ac_dc text,
                      fit_dlrs_kwh numeric,
                      pbi_dlrs_kwh numeric,
                      pbi_fit_dlrs_kwh numeric,
                      increment_1_rebate_dlrs_kw numeric,
                      increment_2_rebate_dlrs_kw numeric,
                      increment_3_rebate_dlrs_kw numeric,
                      max_dlrs_yr numeric,
                      max_tax_credit_dlrs numeric,
                      max_tax_deduction_dlrs numeric,
                      pbi_fit_max_dlrs numeric,
                      pbi_fit_pcnt_cost_max numeric,
                      ptc_dlrs_kwh numeric,
                      rebate_dlrs_kw numeric,
                      rebate_max_dlrs numeric,
                      rebate_max_size_kw numeric,
                      rebate_min_size_kw numeric,
                      rebate_pcnt_cost_max numeric,
                      tax_credit_pcnt_cost numeric,
                      tax_deduction_pcnt_cost numeric,
                      tax_credit_max_size_kw numeric,
                      tax_credit_min_size_kw numeric,
                      sector text)""" % inputs    
        cur.execute(sql)
        con.commit()
        
        # set up sql statement to insert data into the table in chunks
        sql =  """INSERT INTO diffusion_wind.pt_%(sector_abbr)s_incentives
                    SELECT a.gid, c.*
                    FROM diffusion_wind.pt_%(sector_abbr)s_best_option_each_year a
                    LEFT JOIN diffusion_wind.dsire_incentives_lookup_%(sector_abbr)s b
                    ON a.gid = b.pt_gid
                    LEFT JOIN diffusion_wind.incentives c
                    ON b.wind_incentives_uid = c.uid
                    WHERE lower(c.sector) = '%(incentives_sector)s'
                    AND a.county_id IN (%(chunk_place_holder)s)
                    AND a.year = 2014
                    ORDER by a.gid;""" % inputs  
        # run in parallel
        p_run(pg_conn_string, sql, county_chunks, npar) 
    
    sql =  """SELECT * FROM 
            diffusion_wind.pt_%(sector_abbr)s_incentives;""" % inputs
    df = sqlio.read_frame(sql, con)
    return df


def get_initial_market_shares(cur, con, sector_abbr, sector):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()     
    
    sql = """DROP TABLE IF EXISTS diffusion_wind.pt_%(sector_abbr)s_initial_market_shares;
           CREATE TABLE diffusion_wind.pt_%(sector_abbr)s_initial_market_shares AS
            SELECT a.county_id, a.gid, 
                	(a.customers_in_bin/sum(a.customers_in_bin) OVER (PARTITION BY a.county_id)) * b.systems_count_%(sector)s AS initial_number_of_adopters,
                	(a.customers_in_bin/sum(a.customers_in_bin) OVER (PARTITION BY a.county_id)) * b.capacity_mw_%(sector)s AS initial_capacity_mw,
                	b.systems_count_%(sector)s/sum(a.customers_in_bin) OVER (PARTITION BY a.county_id) AS initial_market_share
            FROM diffusion_wind.pt_%(sector_abbr)s_best_option_each_year a
            LEFT JOIN diffusion_wind.starting_wind_capacities_mw_2014_us b
            ON a.county_id = b.county_id
            where a.year = 2014;""" % inputs          
    cur.execute(sql)
    con.commit()
    
    sql = """CREATE INDEX pt_%(sector_abbr)s_initial_market_shares_gid_btree ON diffusion_wind.pt_%(sector_abbr)s_initial_market_shares USING BTREE(gid);""" % inputs
    cur.execute(sql)
    con.commit()

    # BOS - installed capacity is stored as MW in the database, but to be consisent with calculations should be in kW
    sql = """SELECT gid, 
            initial_market_share AS market_share_last_year,
            initial_number_of_adopters AS number_of_adopters_last_year,
            1000 * initial_capacity_mw AS installed_capacity_last_year 
            FROM diffusion_wind.pt_%(sector_abbr)s_initial_market_shares;""" % inputs
    df = sqlio.read_frame(sql, con)
    return df  


def get_main_dataframe(con, main_table, year):
    ''' Pull main pre-processed dataframe from dB
    
        IN: con - pg con object - connection object
        OUT: df  - pd dataframe - pre-processed resource,bins, rates, etc. for all years:

    '''
    if not con:
        close_con = True
        con = make_con()
    else:
        close_con = False
    sql = 'SELECT * FROM %s WHERE year = %s' % (main_table,year)
    df = sqlio.read_frame(sql, con)
    return df
    
def get_financial_parameters(con, res_model = 'Existing Home', com_model = 'Host Owned', ind_model = 'Host Owned'):
    ''' Pull financial parameters dataframe from dB. Use passed parameters to subset for new/existing home/leasing/host-owned
    
        IN: con - pg con object - connection object
            res - string - which residential ownership structure to use (assume 100%)
            com - string - which commercial ownership structure to use (assume 100%)
            ind - string - which industrial ownership structure to use (assume 100%)
            
        OUT: fin_param  - pd dataframe - pre-processed resource,bins, rates, etc. for all years:
    '''
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()   
    
    # Get data, filtering based on ownership models selected
    sql = """SELECT lower(sector) as sector, ownership_model, loan_term_yrs, loan_rate, down_payment, 
           discount_rate, tax_rate, length_of_irr_analysis_yrs
           FROM diffusion_wind.financial_parameters
           WHERE (lower(sector) = 'residential' AND ownership_model = '%(res_model)s')
           OR (lower(sector) = 'commercial' AND ownership_model = '%(com_model)s')
           OR (lower(sector) = 'industrial' AND ownership_model = '%(ind_model)s');""" % inputs
    df = sqlio.read_frame(sql, con)
    
    return df
 
#==============================================================================
   
def get_max_market_share(con, sectors, scenario_opts, residential_type = 'retrofit', commercial_type = 'retrofit', industrial_type = 'retrofit'):
    ''' Pull max market share from dB, select curve based on scenario_options, and interpolate to tenth of a year. 
        Use passed parameters to determine ownership type
    
        IN: con - pg con object - connection object
            residential_type - string - which residential ownership structure to use (new or retrofit)
            commercial_type - string - which commercial ownership structure to use (new or retrofit)
            industrial_type - string - which industrial ownership structure to use (new or retrofit)
            
        OUT: max_market_share  - pd dataframe - dataframe to join on main df to determine max share 
                                                keys are sector & payback period 
    '''
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()       

    # the max market curves need to be interpolated to a finer temporal resolution of 1/10ths of years
    # initialize a list for time steps at that inverval for a max 30 year payback period
    yrs = np.linspace(0,30,301)
    
    # initialize a data frame to hold all of the interpolated max market curves (1 for each sector)
    max_market_share = pd.DataFrame()
    # loop through sectors
    for sector in sectors:
        # define the ownership type based on the current sector
        ownership_type = inputs['%s_type' % sector.lower()]
        short_sector = sector[:3].lower()        
        
        # Whether to use default or user fit max market share curves
        if scenario_opts[short_sector + '_max_market_curve'] == 'User Fit':
            sql = """SELECT * 
            FROM diffusion_wind.user_defined_max_market_share
            WHERE lower(sector) = '%s';""" % sector.lower()
            mm = sqlio.read_frame(sql, con)
        else:
            # get the data for this sector from postgres (this will handle all of the selection based on scenario inputs)
            sql = """SELECT *
                     FROM diffusion_wind.max_market_curves_to_model
                     WHERE lower(sector) = '%s';""" % sector.lower()
            mm = sqlio.read_frame(sql, con)
        # create an interpolation function to interpolate max market share (for either retrofit or new) based on the year
        interp_func = interp1d(mm['year'], mm[ownership_type]);
        # create a data frame of max market values for yrs using this interpolation function
        interpolated_mm = pd.DataFrame({'max_market_share': interp_func(yrs),'payback_key': np.arange(301)})
        # add in the sector to the data frame
        interpolated_mm['sector'] = sector.lower()
        # append to the main data frame
        max_market_share = max_market_share.append(interpolated_mm, ignore_index = True)
    return max_market_share
    

def get_market_projections(con):
    ''' Pull market projections table from dB
    
        IN: con - pg con object - connection object
        OUT: market_projections - numpy array - table containing various market projections
    '''
    return sqlio.read_frame('SELECT * FROM diffusion_wind.market_projections', con)
    
def get_manual_incentives(con):
    ''' Pull manual incentives from input sheet
    
        IN: con - pg con object - connection object
        OUT: inc - pd dataframe - dataframe of manual incentives
    '''
    sql = 'SELECT * FROM diffusion_wind.manual_incentives'
    df = sqlio.read_frame(sql, con)
    df['sector'] = df['sector'].str.lower()
    return df
 
def calc_manual_incentives(df,con,cur_year):
    ''' Calculate the value in first year and length for incentives manually 
    entered in input sheet. 

        IN: df - pandas DataFrame - main dataframe
            cur - SQL cursor 
                        
        OUT: manual_incentives_value - pandas DataFrame - value of rebate, tax incentives, and PBI
    '''
    # Join manual incentives with main df   
    inc = get_manual_incentives(con)
    d = pd.merge(df,inc,left_on = ['state_abbr','sector','utility_type'], right_on = ['region','sector','utility_type'])
        
    # Calculate value of incentive and rebate, and value and length of PBI
    d['value_of_tax_credit_or_deduction'] = d['incentive'] * d['installed_costs_dollars_per_kw'] * d['system_size_kw'] * (cur_year <= d['expire'])
    d['value_of_tax_credit_or_deduction'] = d['value_of_tax_credit_or_deduction'].astype(float)
    d['value_of_pbi_fit'] = 0.01 * d['incentives_c_per_kwh'] * d['aep'] * (cur_year <= d['expire']) # First year value  
    d['value_of_rebate'] = np.minimum(1000 * d['dol_per_kw'] * d['system_size_kw'] * (cur_year <= d['expire']), d['system_size_kw'])
    d['pbi_fit_length'] = d['no_years']
    
    # These values are not used, but necessary for cashflow calculations later
    # Convert dtype to float s.t. columns are included in groupby calculation.
    d['value_of_increment'] = 0
    d['value_of_ptc'] = 0
    d['ptc_length'] = 0
    
    d['value_of_tax_credit_or_deduction'] = d['value_of_tax_credit_or_deduction'].astype(float)
    d['value_of_pbi_fit'] = d['value_of_pbi_fit'].astype(float)
    d['value_of_rebate'] = d['value_of_rebate'].astype(float)
    d['pbi_fit_length'] = d['pbi_fit_length'].astype(float)
    '''
    Because a system could potentially qualify for several incentives, the left 
    join above could join on multiple rows. Thus, groupby by gid 
    to sum over incentives and condense back to unique gid values
    '''
    
    return d[['value_of_increment', 'value_of_pbi_fit', 'value_of_ptc', 'pbi_fit_length', 'ptc_length', 'value_of_rebate', 'value_of_tax_credit_or_deduction']].groupby(d['gid']).sum().reset_index()
    
def calc_dsire_incentives(inc, cur_year, default_exp_yr = 2016, assumed_duration = 10):
    '''
    Calculate the value of incentives based on DSIRE database. The main dataframe 
    is joined by gid to the dsire incentives.There may be many incentives per gid, so the value is calculated for each row (incentives)
    and then groupedby gid, summing over incentives value. For multiyear incentives (ptc/pbi/fit), this requires
    assumption that incentives are disbursed over 10 years.
    
    IN: inc - pandas dataframe (df) - main df joined by dsire_incentives
        cur_year - scalar - current model year
        default_exp_yr - scalar - assumed expiry year if none given
        assumed duration - scalar - assumed duration of multiyear incentives if none given
    OUT: value_of_incentives - pandas df - Values of incentives by type. For 
                                        mutiyear incentves, the (undiscounted) lifetime value is given 
    '''  
    # Shorten names
    ic = inc['installed_costs_dollars_per_kw'] * inc['system_size_kw']
    
    cur_date = np.array([datetime.date(cur_year, 1, 1)]*len(inc))
    default_exp_date = np.array([datetime.date(default_exp_yr, 1, 1)]*len(inc))
    
    ## Coerce incentives to following types:
    inc.increment_1_capacity_kw = inc.increment_1_capacity_kw.astype(float)
    inc.increment_2_capacity_kw = inc.increment_2_capacity_kw.astype(float)
    inc.increment_3_capacity_kw = inc.increment_3_capacity_kw.astype(float)
    inc.increment_1_rebate_dlrs_kw = inc.increment_1_rebate_dlrs_kw.astype(float)
    inc.increment_2_rebate_dlrs_kw = inc.increment_2_rebate_dlrs_kw.astype(float)
    inc.pbi_fit_duration_years = inc.pbi_fit_duration_years.astype(float)
    inc.pbi_fit_max_size_kw = inc.pbi_fit_max_size_kw.astype(float)
    inc.pbi_fit_min_output_kwh_yr = inc.pbi_fit_min_output_kwh_yr.astype(float)
    inc.pbi_fit_min_size_kw = inc.pbi_fit_min_size_kw.astype(float)
    inc.pbi_dlrs_kwh = inc.pbi_dlrs_kwh.astype(float)
    inc.fit_dlrs_kwh = inc.fit_dlrs_kwh.astype(float)
    inc.pbi_fit_max_dlrs = inc.pbi_fit_max_dlrs.astype(float)
    inc.pbi_fit_pcnt_cost_max = inc.pbi_fit_pcnt_cost_max.astype(float)
    inc.ptc_duration_years = inc.ptc_duration_years.astype(float)
    inc.ptc_dlrs_kwh = inc.ptc_dlrs_kwh.astype(float)
    inc.max_dlrs_yr = inc.max_dlrs_yr.astype(float)
    inc.rebate_dlrs_kw = inc.rebate_dlrs_kw.astype(float)
    inc.rebate_max_dlrs = inc.rebate_max_dlrs.astype(float)
    inc.rebate_max_size_kw = inc.rebate_max_size_kw.astype(float)
    inc.rebate_min_size_kw = inc.rebate_min_size_kw.astype(float)
    inc.rebate_pcnt_cost_max = inc.rebate_pcnt_cost_max.astype(float)
    inc.max_tax_credit_dlrs = inc.max_tax_credit_dlrs.astype(float)
    inc.max_tax_deduction_dlrs = inc.max_tax_deduction_dlrs.astype(float)
    inc.tax_credit_pcnt_cost = inc.tax_credit_pcnt_cost.astype(float)
    inc.tax_deduction_pcnt_cost = inc.tax_deduction_pcnt_cost.astype(float)
    inc.tax_credit_max_size_kw = inc.tax_credit_max_size_kw.astype(float)
    inc.tax_credit_min_size_kw = inc.tax_credit_min_size_kw.astype(float)
    inc.max_dlrs_yr[inc.max_dlrs_yr.isnull()] = 1e9
    inc.tax_credit_max_size_kw[inc.tax_credit_max_size_kw.isnull()] = 10000
    
    # 1. # Calculate Value of Increment Incentive
    cap_1 =  np.minimum(inc.increment_1_capacity_kw * 1000, inc['system_size_kw'])
    cap_2 =  (inc.increment_1_capacity_kw > 0) * np.maximum(inc['system_size_kw'] - inc.increment_1_capacity_kw * 1000,0)
    
    value_of_increment = cap_1 * inc.increment_1_rebate_dlrs_kw + cap_2 * inc.increment_2_rebate_dlrs_kw
    value_of_increment[np.isnan(value_of_increment)] = 0
    inc['value_of_increment'] = value_of_increment
    
    # 2. # Calculate lifetime value of PBI & FIT
    inc.pbi_fit_end_date[inc.pbi_fit_end_date.isnull()] = datetime.date(default_exp_yr, 1, 1) # Assign expiry if no date
    pbi_fit_still_exists = cur_date <= inc.pbi_fit_end_date # Is the incentive still valid
    
    pbi_fit_cap = np.where(inc['system_size_kw'] < inc.pbi_fit_min_size_kw, 0, inc['system_size_kw'])
    pbi_fit_cap = np.where(pbi_fit_cap > inc.pbi_fit_max_size_kw, inc.pbi_fit_max_size_kw, pbi_fit_cap)
    pbi_fit_aep = np.where(inc['aep'] < inc.pbi_fit_min_output_kwh_yr, 0, inc['aep'])
    
    # If exists pbi_fit_kwh > 0 but no duration, assume duration
    inc.pbi_fit_duration_years = np.where((inc.pbi_fit_dlrs_kwh > 0) & (inc.pbi_fit_duration_years.isnull()), assumed_duration, inc.pbi_fit_duration_years)
    
    value_of_pbi_fit = pbi_fit_still_exists * np.minimum(inc.pbi_fit_dlrs_kwh, inc.max_dlrs_yr) * pbi_fit_aep
    inc.pbi_fit_max_dlrs[inc.pbi_fit_max_dlrs.isnull()] = 1e9
    value_of_pbi_fit = np.minimum(value_of_pbi_fit,inc.pbi_fit_max_dlrs)
    value_of_pbi_fit[np.isnan(value_of_pbi_fit)] = 0
    length_of_pbi_fit = inc.pbi_fit_duration_years
    length_of_pbi_fit[np.isnan(length_of_pbi_fit)] = 0
    
    # 3. # Lifetime value of the pbi/fit. Assume all pbi/fits are disbursed over 10 years. 
    # This will get the undiscounted sum of incentive correct, present value may have small error
    inc['lifetime_value_of_pbi_fit'] = length_of_pbi_fit * value_of_pbi_fit
    
    ## Calculate first year value and length of PTC
    inc.ptc_end_date[inc.ptc_end_date.isnull()] = datetime.date(default_exp_yr, 1, 1) # Assign expiry if no date
    ptc_still_exists = cur_date <= inc.ptc_end_date # Is the incentive still valid
    ptc_max_size = np.minimum(inc['system_size_kw'], inc.tax_credit_max_size_kw)
    inc.max_tax_credit_dlrs = np.where(inc.max_tax_credit_dlrs.isnull(), 1e9, inc.max_tax_credit_dlrs)
    inc.ptc_duration_years = np.where((inc.ptc_dlrs_kwh > 0) & (inc.ptc_duration_years.isnull()), assumed_duration, inc.ptc_duration_years)
    value_of_ptc =  ptc_still_exists * np.minimum(inc.ptc_dlrs_kwh * inc.naep * ptc_max_size, inc.max_dlrs_yr)
    value_of_ptc[np.isnan(value_of_ptc)] = 0
    value_of_ptc = np.where(value_of_ptc < inc.max_tax_credit_dlrs, value_of_ptc,inc.max_tax_credit_dlrs)
    length_of_ptc = inc.ptc_duration_years
    length_of_ptc[np.isnan(length_of_ptc)] = 0
    
    # Lifetime value of the ptc. Assume all ptcs are disbursed over 10 years
    # This will get the undiscounted sum of incentive correct, present value may have small error
    inc['lifetime_value_of_ptc'] = length_of_ptc * value_of_ptc

    # 4. #Calculate Value of Rebate
    rebate_cap = np.where(inc['system_size_kw'] < inc.rebate_min_size_kw, 0, inc['system_size_kw'])
    rebate_cap = np.where(rebate_cap > inc.rebate_max_size_kw, inc.rebate_max_size_kw, rebate_cap)
    value_of_rebate = inc.rebate_dlrs_kw * rebate_cap
    value_of_rebate = np.minimum(inc.rebate_max_dlrs, value_of_rebate)
    value_of_rebate = np.minimum(inc.rebate_pcnt_cost_max * ic, value_of_rebate)
    value_of_rebate[np.isnan(value_of_rebate)] = 0
    
    inc['value_of_rebate'] = value_of_rebate
    
    # 5. # Calculate Value of Tax Credit
    # Assume able to fully monetize tax credits
    inc.tax_credit_pcnt_cost = np.where(inc.tax_credit_pcnt_cost.isnull(), 0, inc.tax_credit_pcnt_cost)
    inc.tax_credit_pcnt_cost = np.where(inc.tax_credit_pcnt_cost >= 1, 0.01 * inc.tax_credit_pcnt_cost, inc.tax_credit_pcnt_cost)
    inc.tax_deduction_pcnt_cost = np.where(inc.tax_deduction_pcnt_cost.isnull(), 0, inc.tax_deduction_pcnt_cost)
    inc.tax_deduction_pcnt_cost = np.where(inc.tax_deduction_pcnt_cost >= 1, 0.01 * inc.tax_deduction_pcnt_cost, inc.tax_deduction_pcnt_cost)    
    tax_pcnt_cost = inc.tax_credit_pcnt_cost + inc.tax_deduction_pcnt_cost
    
    inc.max_tax_credit_dlrs = np.where(inc.max_tax_credit_dlrs.isnull(), 1e9, inc.max_tax_credit_dlrs)
    inc.max_tax_deduction_dlrs = np.where(inc.max_tax_deduction_dlrs.isnull(), 1e9, inc.max_tax_deduction_dlrs)
    max_tax_credit_or_deduction_value = np.maximum(inc.max_tax_credit_dlrs,inc.max_tax_deduction_dlrs)
    
    value_of_tax_credit_or_deduction = tax_pcnt_cost * ic
    value_of_tax_credit_or_deduction = np.minimum(max_tax_credit_or_deduction_value, value_of_tax_credit_or_deduction)
    value_of_tax_credit_or_deduction = np.where(inc.tax_credit_max_size_kw > inc['system_size_kw'], tax_pcnt_cost * inc.tax_credit_max_size_kw, value_of_tax_credit_or_deduction)
    value_of_tax_credit_or_deduction[np.isnan(value_of_tax_credit_or_deduction)] = 0
    
    inc['value_of_tax_credit_or_deduction'] = value_of_tax_credit_or_deduction
    inc = inc[['gid', 'value_of_increment', 'lifetime_value_of_pbi_fit', 'lifetime_value_of_ptc', 'value_of_rebate', 'value_of_tax_credit_or_deduction']].groupby(['gid']).sum().reset_index() 
    inc['value_of_pbi_fit'] = inc['lifetime_value_of_pbi_fit'] / assumed_duration
    inc['pbi_fit_length'] = assumed_duration
    
    inc['value_of_ptc'] = inc['lifetime_value_of_ptc'] / assumed_duration
    inc['ptc_length'] = assumed_duration
    
    return inc[['gid', 'value_of_increment', 'value_of_pbi_fit', 'value_of_ptc', 'pbi_fit_length', 'ptc_length', 'value_of_rebate', 'value_of_tax_credit_or_deduction']]

def get_rate_escalations(con):
    '''
    Get rate escalation multipliers from database. Escalations are filtered and applied in calc_economics,
    resulting in an average real compounding rate growth. This rate is then used to calculate cash flows
    
    IN: con - connection to server
    OUT: DataFrame with census_division_abbr, sector, year, escalation_factor, and source as columns
    '''  
    rate_escalations = sqlio.read_frame('SELECT * FROM diffusion_shared.rate_escalations', con)
    rate_escalations['sector'] = rate_escalations['sector'].str.lower()
    return rate_escalations
    
def calc_expected_rate_escal(df,rate_escalations, year): 
    '''
    Append the expected rate escalation to the main dataframe.
    Get rate escalation multipliers from database. Escalations are filtered and applied in calc_economics,
    resulting in an average real compounding rate growth. This rate is then used to calculate cash flows
    
    IN: con - connection to server
    OUT: DataFrame with census_division_abbr, sector, year, escalation_factor, and source as columns
    '''  
    def avg_pct_change(x):
        x = x.sort('year')
        return x['escalation_factor'].pct_change().mean()
    
    # Only use the escalation multiplier over the next 30 years
    projected_rate_escalations = rate_escalations[(rate_escalations['year'] < year + 30) & (rate_escalations['year'] >=  year)]
    
    avg_rate_esc = projected_rate_escalations.groupby(['census_division_abbr','sector'])
    
    # Next we find the average percent change in rate projected over next 30 years    
    avg_rate_esc = avg_rate_esc.apply(avg_pct_change)
    
    # Finally, join dataframes to add customer_expec_elec_rate field
    avg_rate_esc = pd.DataFrame({'customer_expec_elec_rates': avg_rate_esc})
    df = pd.merge(df,avg_rate_esc, how = 'left', left_on = ['census_division_abbr', 'sector'], right_index = True)
    return df

def code_profiler(out_dir):
    lines = [ line for line in open(out_dir + '/dg_wind_model.log') if 'took:' in line]
    
    process = [line.split('took:')[-2] for line in lines]
    process = [line.split(':')[-1] for line in process]
    
    time = [line.split('took:')[-1] for line in lines]
    time = [line.split('s')[0] for line in time]
    time = [float(x) for x in time]
    
    
    profile = pd.DataFrame({'process': process, 'time':time})
    profile = profile.sort('time', ascending = False)
    profile.to_csv(out_dir + '/code_profiler.csv') 