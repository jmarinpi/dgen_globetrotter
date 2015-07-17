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
import pandas as pd
import datetime
from multiprocessing import Process, Queue, JoinableQueue, Pool
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
import sys
import getopt
import psutil


# configure psycopg2 to treat numeric values as floats (improves performance of pulling data from the database)
DEC2FLOAT = pg.extensions.new_type(
    pg.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
pg.extensions.register_type(DEC2FLOAT)


def set_source_pt_microdata(con, cur, schema, tech):
    
    inputs = locals().copy()
    sector_abbrs = ['res', 'com', 'ind']


    for sector_abbr in sector_abbrs:
        inputs['sector_abbr'] = sector_abbr
        
        sql = '''DROP VIEW IF EXISTS %(schema)s.point_microdata_%(sector_abbr)s_us_joined;
                 CREATE VIEW %(schema)s.point_microdata_%(sector_abbr)s_us_joined AS
                 SELECT *
                 FROM %(schema)s.point_microdata_%(sector_abbr)s_us_joined_%(tech)s;''' % inputs
        cur.execute(sql)
        con.commit()

        sql = '''DROP VIEW IF EXISTS %(schema)s.point_microdata_%(sector_abbr)s_us;
                 CREATE VIEW %(schema)s.point_microdata_%(sector_abbr)s_us AS
                 SELECT *
                 FROM diffusion_%(tech)s.point_microdata_%(sector_abbr)s_us;''' % inputs
        cur.execute(sql)
        con.commit()        

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


def make_con(connection_string, role = 'diffusion-writers', async = False):    
    con = pg.connect(connection_string, async = async)
    if async:
        wait(con)
    # create cursor object
    cur = con.cursor(cursor_factory=pgx.RealDictCursor)
    # set role (this should avoid permissions issues)
    cur.execute('SET ROLE "%s";' % role)    
    if async:
        wait(con)
    else:
        con.commit()
    
    return con, cur


def current_datetime(format = '%Y_%m_%d_%Hh%Mm%Ss'):
    
    dt = datetime.datetime.strftime(datetime.datetime.now(), format)
    
    return dt


def create_output_schema(pg_conn_string, source_schema = 'diffusion_template'):
    
    inputs = locals().copy()
    con, cur = make_con(pg_conn_string, role = "diffusion-schema-writers")

    cdt = current_datetime()
    dest_schema = 'diffusion_results_%s' % cdt
    inputs['dest_schema'] = dest_schema
    
    sql = '''SELECT clone_schema('%(source_schema)s', '%(dest_schema)s', 'diffusion-writers');''' % inputs
    cur.execute(sql)        
    con.commit()
    
    return dest_schema
    
def drop_output_schema(pg_conn_string, schema):

    inputs = locals().copy()

    con, cur = make_con(pg_conn_string, role = "diffusion-schema-writers")
    sql = '''DROP SCHEMA IF EXISTS %(schema)s CASCADE;''' % inputs
    cur.execute(sql)
    con.commit()
    

def combine_temporal_data(cur, con, schema, technology, start_year, end_year, sector_abbrs, preprocess, logger):

    msg = "Combining Temporal Factors"    
    logger.info(msg)

    if preprocess:
        return 1
        
    if technology == 'wind':
        combine_temporal_data_wind(cur, con, schema, start_year, end_year, sector_abbrs, preprocess, logger)
    elif technology == 'solar':
        combine_temporal_data_solar(cur, con, schema, start_year, end_year, sector_abbrs, preprocess, logger)
    

def combine_temporal_data_solar(cur, con, schema, start_year, end_year, sector_abbrs, preprocess, logger):
    
     # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()       

    # combine all of the temporal data (this only needs to be done once for all sectors)        
    sql = """
            DROP TABLE IF EXISTS %(schema)s.temporal_factors;
            CREATE UNLOGGED TABLE %(schema)s.temporal_factors as 
            SELECT a.year, 
                a.efficiency_improvement_factor,
                a.density_w_per_sqft,
                a.inverter_lifetime_yrs,
                b.capital_cost_dollars_per_kw, 
                b.inverter_cost_dollars_per_kw, 
                b.fixed_om_dollars_per_kw_per_yr, 
                b.variable_om_dollars_per_kwh, 
                b.sector,
                b.source as cost_projection_source,
                c.census_division_abbr,
                c.escalation_factor as rate_escalation_factor,
                c.source as rate_escalation_source,
                d.scenario as load_growth_scenario,
                d.load_multiplier,
                e.carbon_dollars_per_ton
            FROM %(schema)s.input_solar_performance_improvements a
            LEFT JOIN %(schema)s.input_solar_cost_projections_to_model b
                ON a.year = b.year
            LEFT JOIN %(schema)s.rate_escalations_to_model c
                ON a.year = c.year
                AND b.sector = c.sector
            LEFT JOIN diffusion_shared.aeo_load_growth_projections_2014 d
                ON c.census_division_abbr = d.census_division_abbr
                AND a.year = d.year
                AND b.sector = d.sector_abbr
            LEFT JOIN %(schema)s.input_main_market_projections e
                ON a.year = e.year
            WHERE a.year BETWEEN %(start_year)s AND %(end_year)s
                AND c.sector in (%(sector_abbrs)s);""" % inputs
    cur.execute(sql)
    con.commit()
    
    # create indices for subsequent joins
    sql =  """CREATE INDEX temporal_factors_sector_btree 
              ON %(schema)s.temporal_factors 
              USING BTREE(sector);
              
              CREATE INDEX temporal_factors_load_growth_scenario_btree 
              ON %(schema)s.temporal_factors 
              USING BTREE(load_growth_scenario);
              
              CREATE INDEX temporal_factors_rate_escalation_source_btree 
              ON %(schema)s.temporal_factors 
              USING BTREE(rate_escalation_source);
              
              CREATE INDEX temporal_factors_census_division_abbr_btree 
              ON %(schema)s.temporal_factors 
              USING BTREE(census_division_abbr);""" % inputs
    cur.execute(sql)
    con.commit()  
    
    return 1

def combine_temporal_data_wind(cur, con, schema, start_year, end_year, sector_abbrs, preprocess, logger):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()       
    
    # combine the temporal data (this only needs to be done once for all sectors)
    
    # combined temporal data for technology specific factors
    sql = """DROP TABLE IF EXISTS %(schema)s.temporal_factors_technology;
            CREATE UNLOGGED TABLE %(schema)s.temporal_factors_technology as
            SELECT      a.year, 
                    	a.turbine_size_kw, 
                    	a.power_curve_id,
                    	c.turbine_height_m,
                    	c.fixed_om_dollars_per_kw_per_yr, 
                    	c.variable_om_dollars_per_kwh,
                    	c.installed_costs_dollars_per_kw,
                    	d.derate_factor
            FROM %(schema)s.input_wind_performance_improvements a
            LEFT JOIN diffusion_wind.allowable_turbine_sizes b
                	ON a.turbine_size_kw = b.turbine_size_kw
            LEFT JOIN %(schema)s.turbine_costs_per_size_and_year c
                	ON a.turbine_size_kw = c.turbine_size_kw
                 AND a.year = c.year
                 AND b.turbine_height_m = c.turbine_height_m
            LEFT JOIN %(schema)s.input_wind_performance_gen_derate_factors d
                	ON a.year = d.year
                 AND  a.turbine_size_kw = d.turbine_size_kw
            WHERE a.year BETWEEN %(start_year)s AND %(end_year)s
            
            UNION ALL
            
            SELECT GENERATE_SERIES(%(start_year)s, %(end_year)s, 2) as year,
                	0 as turbine_size_kw,
                	0 as power_curve_id,
                	0 as turbine_height_m,
                	0 as fixed_om_dollars_per_kw_per_yr, 
                	0 as variable_om_dollars_per_kwh,
                	0 as installed_costs_dollars_per_kw,
                	0 as derate_factor;""" % inputs
    cur.execute(sql)
    con.commit()
    
    # combine temporal data for market specific factors
    sql = """DROP TABLE IF EXISTS %(schema)s.temporal_factors_market;
            CREATE UNLOGGED TABLE %(schema)s.temporal_factors_market as
            SELECT      a.year, 	
                    	a.census_division_abbr,
                    	a.sector as sector_abbr,
                    	a.escalation_factor as rate_escalation_factor,
                    	a.source as rate_escalation_source,
                    	b.scenario as load_growth_scenario,
                    	b.load_multiplier,
                    	c.carbon_dollars_per_ton
            FROM %(schema)s.rate_escalations_to_model a
            LEFT JOIN diffusion_shared.aeo_load_growth_projections_2014 b
                	ON a.census_division_abbr = b.census_division_abbr
                 AND a.year = b.year
                 AND a.sector = b.sector_abbr               
            LEFT JOIN %(schema)s.input_main_market_projections c
                	ON a.year = c.year
            WHERE a.year BETWEEN %(start_year)s AND %(end_year)s
                            AND a.sector in (%(sector_abbrs)s);""" % inputs
    cur.execute(sql)
    con.commit()    
    
    # create indices for subsequent joins
    sql =  """CREATE INDEX temporal_factors_technology_turbine_height_m_btree 
              ON %(schema)s.temporal_factors_technology
              USING BTREE(turbine_height_m);
              
              CREATE INDEX temporal_factors_technology_power_curve_id_btree 
              ON %(schema)s.temporal_factors_technology
              USING BTREE(power_curve_id);
              
              CREATE INDEX temporal_factors_technology_year_btree 
              ON %(schema)s.temporal_factors_technology
              USING BTREE(year);""" % inputs
    cur.execute(sql)
    con.commit()                
              
    sql =  """CREATE INDEX temporal_factors_market_sector_abbr_btree 
              ON %(schema)s.temporal_factors_market
              USING BTREE(sector_abbr);
              
              CREATE INDEX temporal_factors_market_load_growth_scenario_btree 
              ON %(schema)s.temporal_factors_market 
              USING BTREE(load_growth_scenario);
              
              CREATE INDEX temporal_factors_market_rate_escalation_source_btree 
              ON %(schema)s.temporal_factors_market 
              USING BTREE(rate_escalation_source);
              
              CREATE INDEX temporal_factors_market_census_division_abbr_btree 
              ON %(schema)s.temporal_factors_market 
              USING BTREE(census_division_abbr);
              
              CREATE INDEX temporal_factors_market_year_btree 
              ON %(schema)s.temporal_factors_market
              USING BTREE(year);""" % inputs
    cur.execute(sql)
    con.commit()  
    
    return 1
    
def clear_outputs(con, cur, schema):
    """Delete all rows from the res, com, and ind output tables"""
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()      
    
    sql = """DELETE FROM %(schema)s.outputs_res;
            DELETE FROM %(schema)s.outputs_com;
            DELETE FROM %(schema)s.outputs_ind;""" % inputs
    cur.execute(sql)
    con.commit()


def write_outputs(con, cur, outputs_df, sector_abbr, schema):
    
    inputs = locals().copy()    
    
    # set fields to write
    fields = [  'micro_id',
                'county_id',
                'bin_id',          
                'year',
                'business_model',
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
                'metric',
                'metric_value',
                'lcoe',
                'max_market_share',
                'diffusion_market_share',
                'new_market_share',
                'new_adopters',
                'new_capacity',
                'new_market_value',
                'market_share',
                'number_of_adopters',
                'installed_capacity',
                'market_value',
                'first_year_bill_with_system',
                'first_year_bill_without_system',
                'npv4',
                'excess_generation_percent']    
    # convert formatting of fields list
    inputs['fields_str'] = pylist_2_pglist(fields).replace("'","")       
    # open an in memory stringIO file (like an in memory csv)
    s = StringIO()
    # write the data to the stringIO
    outputs_df[fields].to_csv(s, index = False, header = False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    cur.copy_expert('COPY %(schema)s.outputs_%(sector_abbr)s (%(fields_str)s) FROM STDOUT WITH CSV' % inputs, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()    
    s.close()
     
def p_execute(pg_conn_string, sql):
    try:
        # create cursor and connection
        con, cur = make_con(pg_conn_string)  
        # execute query
        cur.execute(sql)
        # commit changes
        con.commit()
        # close cursor and connection
        con.close()
        cur.close()
    except Exception, e:
        print 'Error: %s' % e
        print sql

    
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

def combine_outputs_wind(schema, sectors, cur, con):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()   

    sql = '''DROP TABLE IF EXISTS %(schema)s.outputs_all;
            CREATE UNLOGGED TABLE %(schema)s.outputs_all AS  ''' % inputs  
    
    for i, sector_abbr in enumerate(sectors.keys()):
        inputs['sector'] = sectors[sector_abbr].lower()
        inputs['sector_abbr'] = sector_abbr
        if i > 0:
            inputs['union'] = 'UNION ALL '
        else:
            inputs['union'] = ''
        
        sub_sql = '''%(union)s 
                    SELECT '%(sector)s'::text as sector, 

                    a.micro_id, a.county_id, a.bin_id, a.year, a.business_model, a.loan_term_yrs, 
                    a.loan_rate, a.down_payment, a.discount_rate, a.tax_rate, a.length_of_irr_analysis_yrs, 
                    a.market_share_last_year, a.number_of_adopters_last_year, a.installed_capacity_last_year, 
                    a.market_value_last_year, a.value_of_increment, a.value_of_pbi_fit, 
                    a.value_of_ptc, a.pbi_fit_length, a.ptc_length, a.value_of_rebate, a.value_of_tax_credit_or_deduction, 
                    a.ic, a.metric, a.metric_value, a.lcoe, a.max_market_share, 
                    a.diffusion_market_share, a.new_market_share, a.new_adopters, a.new_capacity, 
                    a.new_market_value, a.market_share, a.number_of_adopters, a.installed_capacity, 
                    a.market_value, a.first_year_bill_with_system, a.first_year_bill_without_system, 
                    a.npv4, a.excess_generation_percent,

                    b.state_abbr, b.census_division_abbr, b.utility_type, b.hdf_load_index,
                    b.pca_reg, b.reeds_reg, b.incentive_array_id, b.ranked_rate_array_id,
                    b.carbon_price_cents_per_kwh, 
                    b.fixed_om_dollars_per_kw_per_yr, 
                    b.variable_om_dollars_per_kwh, b.installed_costs_dollars_per_kw, 
                    b.ann_cons_kwh, 
                    b.customers_in_bin, b.initial_customers_in_bin, 
                    b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
                    b.crb_model, b.max_demand_kw, b.rate_id_alias, b.rate_source, 
                    b.ur_enable_net_metering, b.nem_system_size_limit_kw,
                    b.ur_nm_yearend_sell_rate, b.ur_flat_sell_rate,
                    b.naep, b.aep, b.system_size_kw,
                    CASE WHEN b.turbine_size_kw = 1500 AND b.nturb > 1 THEN '1500+'::TEXT 
                    ELSE b.turbine_size_kw::TEXT 
                    END as system_size_factors,
                    b.turbine_id,
                    b.i, b.j, b.cf_bin,
                    b.nturb, b.turbine_size_kw, 
                    b.turbine_height_m, b.scoe,
                    b.rate_escalation_factor,
                    
                    (b.rate_escalation_factor * a.first_year_bill_without_system)/b.load_kwh_per_customer_in_bin as cost_of_elec_dols_per_kwh,
                    
                    c.initial_market_share, c.initial_number_of_adopters,
                    c.initial_capacity_mw
                                        
                    
                    FROM %(schema)s.outputs_%(sector_abbr)s a
                    
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_best_option_each_year b
                    ON a.county_id = b.county_id
                    AND a.bin_id = b.bin_id
                    and a.year = b.year
                    
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_initial_market_shares c
                    ON a.county_id = c.county_id
                    AND a.bin_id = c.bin_id
                    ''' % inputs
        sql += sub_sql
    
    sql += ';'
    cur.execute(sql)
    con.commit()

    # create indices that will be needed for various aggregations in R visualization script
    sql = '''CREATE INDEX outputs_all_year_btree ON %(schema)s.outputs_all USING BTREE(year);
             CREATE INDEX outputs_all_state_abbr_btree ON %(schema)s.outputs_all USING BTREE(state_abbr);
             CREATE INDEX outputs_all_sector_btree ON %(schema)s.outputs_all USING BTREE(sector);
             CREATE INDEX outputs_all_business_model_btree ON %(schema)s.outputs_all USING BTREE(business_model);
             CREATE INDEX outputs_all_system_size_factors_btree ON %(schema)s.outputs_all USING BTREE(system_size_factors);                          
             CREATE INDEX outputs_all_metric_btree ON %(schema)s.outputs_all USING BTREE(metric);             
             CREATE INDEX outputs_all_turbine_height_m_btree ON %(schema)s.outputs_all USING BTREE(turbine_height_m);''' % inputs
    cur.execute(sql)
    con.commit()


def combine_outputs_solar(schema, sectors, cur, con):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()   

    sql = '''DROP TABLE IF EXISTS %(schema)s.outputs_all;
            CREATE UNLOGGED TABLE %(schema)s.outputs_all AS  ''' % inputs  
    
    for i, sector_abbr in enumerate(sectors.keys()):
        inputs['sector'] = sectors[sector_abbr].lower()
        inputs['sector_abbr'] = sector_abbr
        if i > 0:
            inputs['union'] = 'UNION ALL '
        else:
            inputs['union'] = ''
        
        sub_sql = '''%(union)s 
                    SELECT '%(sector)s'::text as sector, 

                    a.micro_id, a.county_id, a.bin_id, a.year, 
                    
                    a.business_model, a.loan_term_yrs, 
                    a.loan_rate, a.down_payment, a.discount_rate, a.tax_rate, a.length_of_irr_analysis_yrs, 
                    a.market_share_last_year, a.number_of_adopters_last_year, a.installed_capacity_last_year, 
                    a.market_value_last_year, a.value_of_increment, a.value_of_pbi_fit, 
                    a.value_of_ptc, a.pbi_fit_length, a.ptc_length, a.value_of_rebate, a.value_of_tax_credit_or_deduction, 
                    a.ic, a.metric, a.metric_value, a.lcoe, a.max_market_share, 
                    a.diffusion_market_share, a.new_market_share, a.new_adopters, a.new_capacity, 
                    a.new_market_value, a.market_share, a.number_of_adopters, a.installed_capacity, 
                    a.market_value, a.first_year_bill_with_system, a.first_year_bill_without_system, 
                    a.npv4, a.excess_generation_percent,

                    
                    b.state_abbr, b.census_division_abbr, b.utility_type, b.hdf_load_index,
                    b.pca_reg, b.reeds_reg, b.incentive_array_id, b.ranked_rate_array_id,
                    b.carbon_price_cents_per_kwh, 
                    b.fixed_om_dollars_per_kw_per_yr, 
                    b.variable_om_dollars_per_kwh, b.installed_costs_dollars_per_kw, 
                    b.inverter_cost_dollars_per_kw, 
                    b.ann_cons_kwh, 
                    b.customers_in_bin, b.initial_customers_in_bin, 
                    b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
                    b.crb_model, b.max_demand_kw, b.rate_id_alias, b.rate_source, 
                    b.ur_enable_net_metering, b.nem_system_size_limit_kw,
                    b.ur_nm_yearend_sell_rate, b.ur_flat_sell_rate,   
                    b.naep, b.aep, b.system_size_kw, 
                    r_cut(b.system_size_kw, ARRAY[0,2.5,5.0,10.0,20.0,50.0,100.0,250.0,500.0,750.0,1000.0,1500.0]) 
                        as system_size_factors,
                    b.npanels, 
                    b.tilt, b.azimuth,
                    b.pct_shaded, b.solar_re_9809_gid, 
                    b.density_w_per_sqft, b.inverter_lifetime_yrs, 
                    b.roof_sqft, b.roof_style, b.roof_planes, b.rooftop_portion,
                    b.slope_area_multiplier, b.unshaded_multiplier, b.available_roof_sqft,
                                        
                    b.rate_escalation_factor,
                    
                    (b.rate_escalation_factor * a.first_year_bill_without_system)/b.load_kwh_per_customer_in_bin as cost_of_elec_dols_per_kwh,
                    
                    c.initial_market_share, c.initial_number_of_adopters,
                    c.initial_capacity_mw
                    
                    FROM %(schema)s.outputs_%(sector_abbr)s a
                    
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_best_option_each_year b
                    ON a.county_id = b.county_id
                    AND a.bin_id = b.bin_id
                    and a.year = b.year
                    
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_initial_market_shares c
                    ON a.county_id = c.county_id
                    AND a.bin_id = c.bin_id
                    ''' % inputs
        sql += sub_sql
    
    sql += ';'
    cur.execute(sql)
    con.commit()

    # create indices that will be needed for various aggregations in R visualization script
    sql = '''CREATE INDEX outputs_all_year_btree ON %(schema)s.outputs_all USING BTREE(year);
             CREATE INDEX outputs_all_state_abbr_btree ON %(schema)s.outputs_all USING BTREE(state_abbr);
             CREATE INDEX outputs_all_sector_btree ON %(schema)s.outputs_all USING BTREE(sector);
             CREATE INDEX outputs_all_business_model_btree ON %(schema)s.outputs_all USING BTREE(business_model);
             CREATE INDEX outputs_all_system_size_factors_btree ON %(schema)s.outputs_all USING BTREE(system_size_factors);                          
             CREATE INDEX outputs_all_metric_btree ON %(schema)s.outputs_all USING BTREE(metric);''' % inputs
    cur.execute(sql)
    con.commit()

def combine_outputs_reeds(schema, sectors, cur, con):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()   

    sql = '''DROP TABLE IF EXISTS %(schema)s.reeds_outputs;
            CREATE UNLOGGED TABLE %(schema)s.reeds_outputs AS  ''' % inputs  
    
    for i, sector_abbr in enumerate(sectors.keys()):
        inputs['sector'] = sectors[sector_abbr].lower()
        inputs['sector_abbr'] = sector_abbr
        if i > 0:
            inputs['union'] = 'UNION ALL '
        else:
            inputs['union'] = ''
        
        sub_sql = '''%(union)s 
                    SELECT '%(sector)s'::text as sector, 

                    a.micro_id, a.county_id, a.bin_id, a.year, a.new_capacity, a.installed_capacity, 
                    b.azimuth,b.tilt,b.customers_in_bin,
                    b.state_abbr, b.pca_reg, b.reeds_reg,
                    (b.rate_escalation_factor * a.first_year_bill_without_system)/b.load_kwh_per_customer_in_bin as cost_of_elec_dols_per_kwh,
                    a.excess_generation_percent
                                        
                    FROM %(schema)s.outputs_%(sector_abbr)s a
                    
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_best_option_each_year b
                    ON a.county_id = b.county_id
                    AND a.bin_id = b.bin_id
                    and a.year = b.year

                    ''' % inputs
        sql += sub_sql
    sql += ';'
    cur.execute(sql)
    con.commit()
    sql2 = 'SELECT * FROM %(schema)s.reeds_outputs' % inputs
    return sqlio.read_frame(sql2,con)

def copy_outputs_to_csv(technology, schema, out_path, sectors, cur, con):
    
    if technology == 'wind':
        combine_outputs_wind(schema, sectors, cur, con)
    elif technology == 'solar':
        combine_outputs_solar(schema, sectors, cur, con)        

    # copy data to csv
    f = gzip.open(out_path+'/outputs.csv.gz','w',1)
    cur.copy_expert('COPY %s.outputs_all TO STDOUT WITH CSV HEADER;' % schema, f)
    f.close()
    
    # write the scenario optoins to csv as well
    f2 = open(out_path+'/scenario_options_summary.csv','w')
    cur.copy_expert('COPY %s.input_main_scenario_options TO STDOUT WITH CSV HEADER;' % schema, f2)
    f2.close()
    

def create_scenario_report(technology, schema, scen_name, out_path, cur, con, Rscript_path, logger = None):
           
    # path to the plot_outputs R script        
    plot_outputs_path = '%s/r/graphics/plot_outputs.R' % os.path.dirname(os.getcwd())        
    
    #command = ("%s --vanilla ../r/graphics/plot_outputs.R %s" %(Rscript_path, runpath))
    # for linux and mac, this needs to be formatted as a list of args passed to subprocess
    command = [Rscript_path,'--vanilla', plot_outputs_path, out_path, scen_name, technology, schema]
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

def generate_customer_bins(cur, con, technology, schema, seed, n_bins, sector_abbr, sector, start_year, end_year, 
                           rate_escalation_source, load_growth_scenario, oversize_system_factor, undersize_system_factor,
                           preprocess, npar, pg_conn_string, rate_structure, logger):
                               
                               
    if technology == 'wind':
        resource_key = 'i,j,cf_bin'
        final_table = generate_customer_bins_wind(cur, con, technology, schema, seed, n_bins, sector_abbr, sector, start_year, end_year, 
                           rate_escalation_source, load_growth_scenario, resource_key, oversize_system_factor, undersize_system_factor,
                           preprocess, npar, pg_conn_string, rate_structure, logger)
    elif technology == 'solar':
        resource_key = 'solar_re_9809_gid'
        final_table = generate_customer_bins_solar(cur, con, technology, schema, seed, n_bins, sector_abbr, sector, start_year, end_year, 
                           rate_escalation_source, load_growth_scenario, resource_key, oversize_system_factor, undersize_system_factor,
                           preprocess, npar, pg_conn_string, rate_structure, logger)  

    return final_table

########################################################################################################################
########################################################################################################################
########################################################################################################################
def split_counties(cur, schema, npar):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()      
    
    # get list of counties
    sql =   """SELECT county_id 
               FROM %(schema)s.counties_to_model
               ORDER BY county_id;""" % inputs
    cur.execute(sql)
    counties = [row['county_id'] for row in cur.fetchall()]
    county_chunks = map(list,np.array_split(counties, npar))    
    
    return county_chunks

def sample_customers_and_load(inputs_dict, county_chunks, npar, pg_conn_string, logger, sector_abbr):


    inputs_dict['chunk_place_holder'] = '%(county_ids)s'
    inputs_dict['load_where'] = " AND '%s' = b.sector_abbr" % sector_abbr
    # lookup table for finding the normalized max demand
    inputs_dict['load_demand_lkup'] = 'diffusion_shared.energy_plus_max_normalized_demand'
    if sector_abbr == 'res':
        inputs_dict['load_region'] = 'reportable_domain'
        # note: climate zone is not currently used. see issue #363
#        inputs_dict['load_climate_zone'] = 'climate_zone_building_america'
    else:
        inputs_dict['load_region'] = 'census_division_abbr'
#        inputs_dict['load_climate_zone'] = 'climate_zone_cbecs_2003'
    #==============================================================================
    #     randomly sample  N points from each county 
    #==============================================================================    
    # (note: some counties will have fewer than N points, in which case, all are returned) 
    msg = 'Sampling Customer Bins from Each County'
    logger.info(msg)
    t0 = time.time() 
    sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_%(i_place_holder)s AS
            WITH b as 
            (
                SELECT unnest(sample(array_agg(a.micro_id ORDER BY a.micro_id),%(n_bins)s,%(seed)s,True,array_agg(a.point_weight ORDER BY a.micro_id))) as micro_id
                FROM %(schema)s.point_microdata_%(sector_abbr)s_us a
                WHERE a.county_id IN (%(chunk_place_holder)s)
                GROUP BY a.county_id
            )
                
            SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a.county_id ORDER BY a.county_id, a.micro_id) as bin_id
            FROM %(schema)s.point_microdata_%(sector_abbr)s_us_joined a
            INNER JOIN b
            ON a.micro_id = b.micro_id
            WHERE a.county_id IN (%(chunk_place_holder)s);""" % inputs_dict

    p_run(pg_conn_string, sql, county_chunks, npar)
    print time.time()-t0

    #==============================================================================
    #    create lookup table with random values for each load bin 
    #==============================================================================
    msg = "Setting up randomized load bins"
    logger.info(msg)
    t0 = time.time()
    
    
    sql =  """DROP TABLE IF EXISTS %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s;
         CREATE UNLOGGED TABLE %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s AS
         WITH all_bins AS
         (
             SELECT a.county_id, 
                     b.load_id, b.weight, b.ann_cons_kwh, b.crb_model, b.roof_style, b.roof_sqft, b.ownocc8
             FROM %(schema)s.counties_to_model a
             LEFT JOIN diffusion_shared.cbecs_recs_combined b
                 ON a.%(load_region)s = b.%(load_region)s
             WHERE a.county_id in  (%(chunk_place_holder)s)
                   %(load_where)s
        ),
        sampled_bins AS 
        (
            SELECT a.county_id, 
                    unnest(sample(array_agg(a.load_id ORDER BY a.load_id),%(n_bins)s,%(seed)s * a.county_id,True,array_agg(a.weight ORDER BY a.load_id))) as load_id
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
    print time.time()-t0
    
    # add an index on county id and row_number
    sql = """CREATE INDEX county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s_join_fields_btree 
            ON %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s USING BTREE(county_id, bin_id);
            CREATE INDEX county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s_crb_model_btree 
            ON %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s USING BTREE(crb_model);""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
   
    #==============================================================================
    #     link each point to a load bin
    #==============================================================================
    # use random weighted sampling on the load bins to ensure that countyies with <N points
    # have a representative sample of load bins 
    msg = 'Associating Customer Bins with Load and Customer Count'    
    logger.info(msg)
    sql =  """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s AS
            WITH binned as
            (
                SELECT a.*, b.crb_model, b.ann_cons_kwh, b.weight, b.roof_sqft, b.roof_style, b.ownocc8,
                    	a.county_total_customers_2011 * b.weight/sum(b.weight) OVER (PARTITION BY a.county_id) as customers_in_bin, 
                    	a.county_total_load_mwh_2011 * 1000 * (b.ann_cons_kwh*b.weight)/sum(b.ann_cons_kwh*b.weight) OVER (PARTITION BY a.county_id) as load_kwh_in_bin
                FROM %(schema)s.pt_%(sector_abbr)s_sample_%(i_place_holder)s a
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
    print time.time()-t0

    # **** ADD INDICES ****
    sql = """CREATE INDEX pt_%(sector_abbr)s_sample_load_%(i_place_holder)s_join_fields_btree 
            ON %(schema)s.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s USING BTREE(hdf_load_index, crb_model);""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    #==============================================================================
    #     find the max demand for each bin based on the applicable energy plus building model
    #==============================================================================
    sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s AS
            SELECT a.*, ROUND(b.normalized_max_demand_kw_per_kw * a.load_kwh_per_customer_in_bin, 0)::INTEGER AS max_demand_kw
            FROM %(schema)s.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s a
            LEFT JOIN %(load_demand_lkup)s b
            ON a.crb_model = b.crb_model
            AND a.hdf_load_index = b.hdf_index;""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
    print time.time()-t0       

    # add indices on: max_demand_kw, state_abbr, ranked_rate_array_id
    sql = """CREATE INDEX pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s_pkey_btree 
            ON %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s USING BTREE(county_id, bin_id);
            
            CREATE INDEX pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s_max_demand_kw_btree 
            ON %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s USING BTREE(max_demand_kw);
            
            CREATE INDEX pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s_state_abbr_btree 
            ON %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s USING BTREE(state_abbr);
            
            CREATE INDEX pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s_ranked_rate_array_id_btree 
            ON %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s USING BTREE(ranked_rate_array_id);
            """ % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)


def find_rates(inputs_dict, county_chunks, npar, pg_conn_string, rate_structure, logger):


    if rate_structure.lower() == 'complex rates':
        # find the highest ranked applicable rate for each point (based on max demand kw and state)
        # (note: this may return multiple rates for a single point)
        sql =   """
                DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_applicable_rates_%(i_place_holder)s;
                CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_applicable_rates_%(i_place_holder)s AS
                WITH a AS
                (
                    	SELECT a.county_id, a.bin_id, 
                    		b.rate_id_alias,
                              b.rate_type,
                    		c.rank as rate_rank
                    	FROM %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s a
                    	LEFT JOIN diffusion_shared.urdb_rates_by_state_%(sector_abbr)s b
                                	ON a.max_demand_kw <= b.urdb_demand_max
                                	AND a.max_demand_kw >= b.urdb_demand_min
                                	AND a.state_abbr = b.state_abbr
                    	LEFT JOIN diffusion_shared.ranked_rate_array_lkup_%(sector_abbr)s c
                    	ON a.ranked_rate_array_id = c.ranked_rate_array_id
                    	AND b.rate_id_alias = c.rate_id_alias
                    ),
                b as
                (
                    	SELECT *, rank() OVER (PARTITION BY county_id, bin_id ORDER BY rate_rank ASC) as rank
                    	FROM a
                )
                SELECT b.*, c.%(sector_abbr)s_weight as rate_type_weight
                FROM b 
                LEFT JOIN %(schema)s.input_main_market_rate_type_weights c
                ON b.rate_type = c.rate_type
                WHERE b.rank = 1;""" % inputs_dict
        p_run(pg_conn_string, sql, county_chunks, npar)
        
        # add indices on county id, bin id
        sql = """
                CREATE INDEX pt_%(sector_abbr)s_sample_load_applicable_rates_%(i_place_holder)s_pkey_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_applicable_rates_%(i_place_holder)s USING BTREE(county_id, bin_id);
              """ % inputs_dict
        p_run(pg_conn_string, sql, county_chunks, npar)
        
        # deal with multiple equally ranked rates for a single point
        # (randomly select for now -- in the future, we will randomly select with weights based on rate type)
        sql =   """
                DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s;
                CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s AS
                WITH a AS
                (
                    SELECT a.county_id, a.bin_id,
                            unnest(sample(array_agg(a.rate_id_alias ORDER BY a.rate_id_alias), 1, 
                                          (%(seed)s * a.county_id * a.bin_id), False,
                                          array_agg(a.rate_type_weight ORDER BY a.rate_id_alias))) as rate_id_alias
                    FROM %(schema)s.pt_%(sector_abbr)s_sample_load_applicable_rates_%(i_place_holder)s a
                    GROUP BY a.county_id, a.bin_id
                )
                SELECT b.*, a.rate_id_alias, 'urdb3'::CHARACTER VARYING(5) as rate_source
                FROM %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s b
                LEFT JOIN a
                ON a.county_id = b.county_id
                AND a.bin_id = b.bin_id;""" % inputs_dict
        p_run(pg_conn_string, sql, county_chunks, npar)

    elif rate_structure.lower() == 'flat (annual average)':
        # flat annual average rate ids are already stored in the demandmax table as county_id
        # we simply need to duplicate and rename that field to rate_id_alias and specify the rate_source
        sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s;
                CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s AS
                SELECT b.*, b.county_id as rate_id_alias, 'aa%(sector_abbr)s'::CHARACTER VARYING(5) as rate_source
                FROM %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s b;""" % inputs_dict
        p_run(pg_conn_string, sql, county_chunks, npar)
                     
    elif rate_structure.lower() == 'flat (user-defined)':
        # user-defined rates are id'ed based on the state_fips, which is already stored in the demandmax table
        # we simply need to duplicate and rename that field to rate_id_alias and specify the rate_source
        sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s;
                CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s AS
                SELECT b.*, b.state_fips as rate_id_alias, 'ud%(sector_abbr)s'::CHARACTER VARYING(5) as rate_source
                FROM %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s b;""" % inputs_dict
        p_run(pg_conn_string, sql, county_chunks, npar)

    
    ###############################################################################################
    # regardless of the rate structure, the output table needs indices added for subsequent queries
    if inputs_dict['technology'] == 'wind':    
        # add index for exclusions (if they apply)
        sql =  """  CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_acres_per_hu_btree 
                    ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s 
                    USING btree(acres_per_hu);
                    
                    CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_hi_dev_pct_btree 
                    ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s 
                    USING btree(hi_dev_pct);
                    
                    CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_canopy_pct_hi_btree 
                    ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s 
                    USING btree(canopy_pct_hi);
                    
                    CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_canopy_ht_m_btree 
                    ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s 
                    USING btree(canopy_ht_m);""" % inputs_dict                                  
        p_run(pg_conn_string, sql, county_chunks, npar)
        
    elif inputs_dict['technology'] == 'solar':
        # add an index on county id and row_number
        sql = """CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_join_fields_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s USING BTREE(county_id, bin_id);
                
                CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_roof_style_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s USING BTREE(roof_style);
                
                CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_state_abbr_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s USING BTREE(state_abbr);""" % inputs_dict
        p_run(pg_conn_string, sql, county_chunks, npar)


def assign_roof_characteristics(inputs_dict, county_chunks, npar, pg_conn_string, logger):
    
    msg = "Assigning rooftop characteristics"
    logger.info(msg)
    
    #=============================================================================================================
    #     link each point to a rooftop orientation based on roof_style and prob weights in rooftop_characteristics
    #=============================================================================================================
    t0 = time.time()
    sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s AS
            WITH all_roof_options AS
            (
                	SELECT a.county_id, a.bin_id, 
                         b.uid as roof_char_uid, b.prob_weight
                	FROM %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s a
                	LEFT JOIN diffusion_solar.rooftop_characteristics b
                	ON b.sector_abbr = '%(sector_abbr)s'
                  AND a.roof_style = b.roof_style
            ),
            selected_roof_options AS 
            (
                	select county_id, bin_id, 
                		 unnest(sample(array_agg(roof_char_uid ORDER BY roof_char_uid),
                			1, -- sample size
                			%(seed)s * county_id * bin_id, -- random generator seed
                			False, -- sample w/o replacement
                			array_agg(prob_weight ORDER BY roof_char_uid))) as roof_char_uid
                	FROM all_roof_options
                	GROUP BY county_id, bin_id
            )
            SELECT a.*,
                	c.tilt, c.azimuth, d.pct_shaded,
                  c.roof_planes, c.rooftop_portion, c.slope_area_multiplier, c.unshaded_multiplier,
                	a.roof_sqft * c.rooftop_portion * c.slope_area_multiplier * c.unshaded_multiplier as available_roof_sqft
            FROM %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s a
            LEFT JOIN selected_roof_options b
                    ON a.county_id = b.county_id 
                    AND a.bin_id = b.bin_id
            LEFT JOIN diffusion_solar.rooftop_characteristics c
                    ON b.roof_char_uid = c.uid
            LEFT JOIN diffusion_solar.solar_ds_regional_shading_assumptions d
                    ON a.state_abbr = d.state_abbr;""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)

    # query for indices creation    
    sql =  """CREATE INDEX pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s_census_division_abbr_btree 
              ON %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s 
              USING BTREE(census_division_abbr);
              
              CREATE INDEX pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s_resource_key_btree 
              ON %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s 
              USING BTREE(solar_re_9809_gid, tilt, azimuth);""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    print time.time()-t0


def generate_customer_bins_solar(cur, con, technology, schema, seed, n_bins, sector_abbr, sector, start_year, end_year, 
                           rate_escalation_source, load_growth_scenario, resource_key, 
                           oversize_system_factor, undersize_system_factor,
                           preprocess, npar, pg_conn_string, rate_structure, logger):

    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()  
    inputs['i_place_holder'] = '%(i)s'
    inputs['seed_str'] = str(seed).replace('.','p')
        
    msg = "Setting up %(sector)s Customer Profiles by County for Scenario Run" % inputs
    logger.info(msg)
     
    if preprocess == True:
        table_name_dict = {'res': '%(schema)s.pt_res_best_option_each_year' % inputs, 
                           'com' : '%(schema)s.pt_com_best_option_each_year' % inputs, 
                           'ind' : '%(schema)s.pt_ind_best_option_each_year' % inputs}
        return table_name_dict[sector_abbr]
    
    #==============================================================================
    #     break counties into subsets for parallel processing
    #==============================================================================
    # get list of counties
    county_chunks = split_counties(cur, schema, npar)

    #==============================================================================
    #     sample customer locations and load. and link together    
    #==============================================================================
    sample_customers_and_load(inputs, county_chunks, npar, pg_conn_string, logger, sector_abbr)

    #==============================================================================
    #     get rate for each customer bin
    #==============================================================================
    find_rates(inputs, county_chunks, npar, pg_conn_string, rate_structure, logger)

    #==============================================================================
    #     Assign rooftop characterisics
    #==============================================================================  
    assign_roof_characteristics(inputs, county_chunks, npar, pg_conn_string, logger)

    #==============================================================================
    #     Join to Resource
    #==============================================================================
    msg = "Finding Resource for Each Customer Bin"
    logger.info(msg)
    sql =  """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_and_resource_%(i_place_holder)s;
                CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_and_resource_%(i_place_holder)s AS
                SELECT a.*,
                        b.naep
                FROM %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s a
                LEFT JOIN diffusion_solar.solar_resource_annual b
                    ON a.solar_re_9809_gid = b.solar_re_9809_gid
                    AND a.tilt = b.tilt
                    AND a.azimuth = b.azimuth""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)

    # create indices for subsequent joins
    sql =  """CREATE INDEX pt_%(sector_abbr)s_sample_load_and_resource_%(i_place_holder)s_census_division_abbr_btree 
              ON %(schema)s.pt_%(sector_abbr)s_sample_load_and_resource_%(i_place_holder)s 
              USING BTREE(census_division_abbr);
              
              CREATE INDEX pt_%(sector_abbr)s_sample_load_and_resource_%(i_place_holder)s_nem_join_fields_btree 
              ON %(schema)s.pt_%(sector_abbr)s_sample_load_and_resource_%(i_place_holder)s 
              USING BTREE(state_abbr, utility_type);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)

    #==============================================================================
    #     Find All Combinations of Costs and Resource for Each Customer Bin
    #==============================================================================
    msg = "Combining Cost, Resource, and System Sizing for Each Customer Bin and Year"
    t0 = time.time()
    logger.info(msg)
    
    sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_best_option_each_year;
                CREATE UNLOGGED TABLE  %(schema)s.pt_%(sector_abbr)s_best_option_each_year
                (
                  micro_id integer,
                  county_id integer,
                  bin_id bigint,
                  year integer,
                  state_abbr character varying(2),
                  census_division_abbr text,
                  utility_type character varying(9),
                  hdf_load_index integer,
                  pca_reg text,
                  reeds_reg integer,
                  rate_escalation_factor numeric,
                  incentive_array_id integer,
                  ranked_rate_array_id integer,
                  carbon_price_cents_per_kwh numeric,
                  fixed_om_dollars_per_kw_per_yr numeric,
                  variable_om_dollars_per_kwh numeric,
                  installed_costs_dollars_per_kw numeric, -- *** THIS MAY NOT BE CORRECT-- CHECK WITH BEN ***
                  inverter_cost_dollars_per_kw numeric,
                  ann_cons_kwh numeric,
                  customers_in_bin double precision,
                  initial_customers_in_bin double precision,
                  load_kwh_in_bin double precision,
                  initial_load_kwh_in_bin double precision,
                  load_kwh_per_customer_in_bin BIGINT,
                  crb_model text,
                  max_demand_kw integer,
                  rate_id_alias integer,
                  rate_source CHARACTER VARYING(5),
                  naep numeric,
                  aep numeric,
                  system_size_kw numeric,
                  npanels numeric,
                  ur_enable_net_metering boolean,
                  nem_system_size_limit_kw double precision,
                  ur_nm_yearend_sell_rate numeric,
                  ur_flat_sell_rate numeric,                  
                  tilt integer,
                  azimuth text,
                  pct_shaded double precision,
                  solar_re_9809_gid integer,
                  density_w_per_sqft numeric,
                  inverter_lifetime_yrs integer,
                  roof_sqft integer,
                  roof_style text,
                  roof_planes integer,
                  rooftop_portion numeric,
                  slope_area_multiplier numeric,
                  unshaded_multiplier numeric,
                  available_roof_sqft integer,
                  owner_occupancy_status integer
                );""" % inputs
    cur.execute(sql)
    con.commit()
    
    sql =  """INSERT INTO %(schema)s.pt_%(sector_abbr)s_best_option_each_year
            WITH combined AS
            (
                SELECT
                 	a.micro_id, a.county_id, a.bin_id, 
                  b.year, 
                  a.state_abbr, 
                  a.census_division_abbr, 
                  a.utility_type, 
                  a.hdf_load_index,
                  a.pca_reg, a.reeds_reg,
                  b.rate_escalation_factor,
                  a.incentive_array_id,
                  a.ranked_rate_array_id,
                  b.carbon_dollars_per_ton * 100 * a.carbon_intensity_t_per_kwh as  carbon_price_cents_per_kwh,
                	b.fixed_om_dollars_per_kw_per_yr, 
                	b.variable_om_dollars_per_kwh,
                	b.capital_cost_dollars_per_kw * a.cap_cost_multiplier::NUMERIC as capital_cost_dollars_per_kw,
                  b.inverter_cost_dollars_per_kw * a.cap_cost_multiplier::NUMERIC as inverter_cost_dollars_per_kw,
                	a.ann_cons_kwh, 
                	b.load_multiplier * a.customers_in_bin * (1-a.pct_shaded) as customers_in_bin, 
                	a.customers_in_bin * (1-a.pct_shaded) as initial_customers_in_bin, 
                	b.load_multiplier * a.load_kwh_in_bin * (1-a.pct_shaded) AS load_kwh_in_bin,
                	a.load_kwh_in_bin * (1-a.pct_shaded) AS initial_load_kwh_in_bin,
                	a.load_kwh_per_customer_in_bin,
                  a.crb_model,                  
                  a.max_demand_kw,
                  a.rate_id_alias,
                  a.rate_source,
                	a.naep * b.efficiency_improvement_factor as naep,
                  a.tilt,
                  a.azimuth,
                  a.pct_shaded,
                  a.solar_re_9809_gid,
                  b.density_w_per_sqft, 
                  b.inverter_lifetime_yrs,
                  c.system_size_limit_kw as nem_system_size_limit_kw,
                  c.year_end_excess_sell_rate_dlrs_per_kwh as ur_nm_yearend_sell_rate,
                  c.hourly_excess_sell_rate_dlrs_per_kwh as ur_flat_sell_rate,
                  a.roof_sqft,
                  a.roof_style,
                  a.roof_planes,
                  a.rooftop_portion,
                  a.slope_area_multiplier,
                  a.unshaded_multiplier,
                  a.available_roof_sqft,
                  a.ownocc8,
                  --OPTIMAL SIZING ALGORITHM THAT RETURNS A SYSTEM SIZE AND NUMBER OF PANELS:
                  diffusion_solar.system_sizing(a.load_kwh_per_customer_in_bin,
                                                a.naep * b.efficiency_improvement_factor,
                                                a.available_roof_sqft,
                                                b.density_w_per_sqft,
                                                c.system_size_limit_kw,
                                                d.sys_size_target_nem,
                                                d.sys_size_target_no_nem) as system_sizing_return
                FROM %(schema)s.pt_%(sector_abbr)s_sample_load_and_resource_%(i_place_holder)s a
                INNER JOIN %(schema)s.temporal_factors b
                    ON a.census_division_abbr = b.census_division_abbr
                LEFT JOIN %(schema)s.input_main_nem_scenario c
                    ON c.state_abbr = a.state_abbr
                    AND c.utility_type = a.utility_type
                    AND c.year = b.year
                    AND c.sector_abbr = '%(sector_abbr)s'
                LEFT JOIN %(schema)s.input_solar_performance_system_sizing_factors d
                    ON d.sector_abbr = '%(sector_abbr)s'
                WHERE b.sector = '%(sector_abbr)s'
                    AND b.rate_escalation_source = '%(rate_escalation_source)s'
                    AND b.load_growth_scenario = '%(load_growth_scenario)s'
            )
                SELECT micro_id, county_id, bin_id, year, state_abbr, census_division_abbr, utility_type, hdf_load_index,
                   pca_reg, reeds_reg, rate_escalation_factor, incentive_array_id, ranked_rate_array_id,
                   carbon_price_cents_per_kwh, 
            
                   fixed_om_dollars_per_kw_per_yr, 
                   variable_om_dollars_per_kwh, 
                   capital_cost_dollars_per_kw as installed_costs_dollars_per_kw, -- *** THIS MAY NOT BE CORRECT -- CHECK WITH BEN ***
                   inverter_cost_dollars_per_kw,
            
                   ann_cons_kwh, 
                   customers_in_bin, initial_customers_in_bin, 
                   load_kwh_in_bin, initial_load_kwh_in_bin, load_kwh_per_customer_in_bin, 
                   crb_model, max_demand_kw, rate_id_alias, rate_source,
    
                   naep,
                   naep * (system_sizing_return).system_size_kw as aep,
                   (system_sizing_return).system_size_kw as system_size_kw,
                   (system_sizing_return).npanels as npanels,
                   
                   (system_sizing_return).nem_available as ur_enable_net_metering,
                   nem_system_size_limit_kw,
                   ur_nm_yearend_sell_rate,
                   ur_flat_sell_rate,
    
                   tilt,
                   azimuth,
                   pct_shaded,
                   solar_re_9809_gid,
                   density_w_per_sqft,
                   inverter_lifetime_yrs,
                   roof_sqft,
                   roof_style,
                   roof_planes,
                   rooftop_portion,
                   slope_area_multiplier,
                   unshaded_multiplier,
                   available_roof_sqft,
                   ownocc8 as owner_occupancy_state
          FROM combined;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)
    print time.time() - t0
    
    # create indices
    sql = """CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_join_fields_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(county_id,bin_id);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_year_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(year);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_incentive_array_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(incentive_array_id);    
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_re_9809_gid_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(solar_re_9809_gid);            
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_tilt_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(tilt);     

             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_azimuth_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(azimuth);        
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_system_size_kw_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(system_size_kw);  

             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_rate_id_alias_source_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(rate_id_alias, rate_source);         
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_hdf_load_index_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(hdf_load_index);  
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_crb_model_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(crb_model);  
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_load_kwh_per_customer_in_bin_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(load_kwh_per_customer_in_bin);    
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_nem_fields_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate);
            """ % inputs
    cur.execute(sql)
    con.commit()
    
    print time.time() - t0

    #==============================================================================
    #   clean up intermediate tables
    #==============================================================================
    msg = "Cleaning up intermediate tables"
    logger.info(msg)
    intermediate_tables = [ '%(schema)s.county_rooftop_availability_samples_%(sector_abbr)s_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_and_resource_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_%(i_place_holder)s' % inputs,
                            '%(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_applicable_rates_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s' % inputs                            
                            ]

        
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
    final_table = '%(schema)s.pt_%(sector_abbr)s_best_option_each_year' % inputs

    return final_table


def apply_siting_restrictions(inputs_dict, county_chunks, npar, pg_conn_string, logger):
    
    #==============================================================================
    #     Find the allowable turbine heights and sizes (kw) for each customer bin
    #==============================================================================    
    # (note: some counties will have fewer than N points, in which case, all are returned) 
    msg = 'Applying Turbine Siting Restrictions'
    logger.info(msg)
    t0 = time.time() 
    
    sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_rate_allowable_turbines_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_rate_allowable_turbines_%(i_place_holder)s AS
             
                WITH restrictions AS
                (
                	SELECT a.turbine_height_m, 
                         a.turbine_size_kw,
                         b.min_acres_per_hu,
                         c.max_hi_dev_pct,
                         d.required_clearance_m
                	FROM diffusion_wind.allowable_turbine_sizes a
                	-- min. acres per housing unit
                	LEFT JOIN %(schema)s.input_wind_siting_parcel_size b
                		ON a.turbine_height_m = b.turbine_height_m
                	-- max high development percent
                	LEFT JOIN %(schema)s.input_wind_siting_hi_dev c
                		ON a.turbine_height_m = c.turbine_height_m
                	-- required canopy clearance
                	LEFT JOIN %(schema)s.input_wind_siting_canopy_clearance d
                		ON a.turbine_size_kw = d.turbine_size_kw
                )
                SELECT  a.*, 
                    	COALESCE(b.turbine_height_m, 0) AS turbine_height_m, 
                    	COALESCE(b.turbine_size_kw, 0) AS turbine_size_kw
                FROM  %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s a
                LEFT JOIN restrictions b
                	ON a.hi_dev_pct <= b.max_hi_dev_pct
                	and a.acres_per_hu >= b.min_acres_per_hu
                	and (   a.canopy_pct_hi = false 
                		   OR
                	       (b.turbine_height_m >= (a.canopy_ht_m + b.required_clearance_m))
                	    );
             """ % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
       
    
    # create indices for next joins          
    sql =  """CREATE INDEX pt_%(sector_abbr)s_sample_load_rate_allowable_turbines_%(i_place_holder)s_turbine_height_m_btree 
              ON %(schema)s.pt_%(sector_abbr)s_sample_load_rate_allowable_turbines_%(i_place_holder)s 
              USING BTREE(turbine_height_m);
              
              CREATE INDEX pt_%(sector_abbr)s_sample_load_rate_allowable_turbines_%(i_place_holder)s_resource_key_btree 
              ON %(schema)s.pt_%(sector_abbr)s_sample_load_rate_allowable_turbines_%(i_place_holder)s
              USING BTREE(%(resource_key)s);""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)              
    print time.time()-t0

########################################################################################################################
########################################################################################################################
########################################################################################################################
def generate_customer_bins_wind(cur, con, technology, schema, seed, n_bins, sector_abbr, sector, start_year, end_year, 
                           rate_escalation_source, load_growth_scenario, resource_key,
                           oversize_system_factor, undersize_system_factor,
                           preprocess, npar, pg_conn_string, rate_structure, logger):

    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['seed_str'] = str(seed).replace('.','p')
        
    msg = "Setting up %(sector)s Customer Profiles by County for Scenario Run" % inputs
    logger.info(msg)
     
    if preprocess == True:
        table_name_dict = {'res': '%(schema)s.pt_res_best_option_each_year' % inputs, 
                           'com' : '%(schema)s.pt_com_best_option_each_year' % inputs, 
                           'ind' : '%(schema)s.pt_ind_best_option_each_year' % inputs}
        return table_name_dict[sector_abbr]
    
    #==============================================================================
    #     break counties into subsets for parallel processing
    #==============================================================================
    # get list of counties
    county_chunks = split_counties(cur, schema, npar)
    

    #==============================================================================
    #     sample customer locations and load. and link together    
    #==============================================================================
    sample_customers_and_load(inputs, county_chunks, npar, pg_conn_string, logger, sector_abbr)
    
    #==============================================================================
    #     get rate for each cusomter bin
    #==============================================================================
    find_rates(inputs, county_chunks, npar, pg_conn_string, rate_structure, logger)    

    #==============================================================================
    #     apply turbine siting restrictions
    #==============================================================================   
    apply_siting_restrictions(inputs, county_chunks, npar, pg_conn_string, logger)
    
    #==============================================================================
    #     Find All Combinations of Points and Wind Resource
    #==============================================================================  
    msg = "Finding All Wind Resource Combinations for Each Customer Bin"
    logger.info(msg)
    sql =  """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_rate_turbine_resource_%(i_place_holder)s;
                CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_rate_turbine_resource_%(i_place_holder)s AS
                SELECT a.*,
                    COALESCE(b.aep, 0) as naep_no_derate,
                    COALESCE(b.turbine_id, 0) as power_curve_id
                FROM %(schema)s.pt_%(sector_abbr)s_sample_load_rate_allowable_turbines_%(i_place_holder)s a
                LEFT JOIN diffusion_wind.wind_resource_annual b
                    ON a.i = b.i
                    AND a.j = b.j
                    AND a.cf_bin = b.cf_bin
                    AND a.turbine_height_m = b.height;
                    """ % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    # create indices for subsequent joins
    sql =  """CREATE INDEX pt_%(sector_abbr)s_sample_load_rate_turbine_resource_%(i_place_holder)s_temporal_join_fields_btree 
              ON %(schema)s.pt_%(sector_abbr)s_sample_load_rate_turbine_resource_%(i_place_holder)s 
              USING BTREE(turbine_height_m, turbine_size_kw, census_division_abbr, power_curve_id);
              
              CREATE INDEX pt_%(sector_abbr)s_sample_load_rate_turbine_resource_%(i_place_holder)s_nem_join_fields_btree 
              ON %(schema)s.pt_%(sector_abbr)s_sample_load_rate_turbine_resource_%(i_place_holder)s 
              USING BTREE(state_abbr, utility_type);""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)


    #==============================================================================
    #     Find All Combinations of Costs and Resource for Each Customer Bin
    #==============================================================================
    msg = "Finding All Combinations of Cost and Resource for Each Customer Bin and Year"
    t0 = time.time()
    logger.info(msg)       
    sql =  """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s AS
            WITH combined AS
            (
                SELECT
                 	a.micro_id, a.county_id, a.bin_id, b.year, a.state_abbr, a.census_division_abbr,
                      a.utility_type, a.hdf_load_index,
                      a.pca_reg, a.reeds_reg,
                      b.rate_escalation_factor,
                      a.incentive_array_id,
                      a.ranked_rate_array_id,
                      a.ownocc8,
                b.carbon_dollars_per_ton * 100 * a.carbon_intensity_t_per_kwh as  carbon_price_cents_per_kwh,
                	e.fixed_om_dollars_per_kw_per_yr, 
                	e.variable_om_dollars_per_kwh,
                	e.installed_costs_dollars_per_kw * a.cap_cost_multiplier::numeric as installed_costs_dollars_per_kw,
                	a.ann_cons_kwh, 
                	b.load_multiplier * a.customers_in_bin as customers_in_bin, 
                	a.customers_in_bin as initial_customers_in_bin, 
                	b.load_multiplier * a.load_kwh_in_bin AS load_kwh_in_bin,
                	a.load_kwh_in_bin AS initial_load_kwh_in_bin,
                	a.load_kwh_per_customer_in_bin,
                  a.crb_model,
                  a.max_demand_kw,
                  a.rate_id_alias,
                  a.rate_source,
                	a.naep_no_derate * e.derate_factor as naep,
                  a.power_curve_id as turbine_id,
                  a.i, a.j, a.cf_bin,
                	e.turbine_size_kw,
                	a.turbine_height_m,
                  c.system_size_limit_kw as nem_system_size_limit_kw,
                  c.year_end_excess_sell_rate_dlrs_per_kwh as ur_nm_yearend_sell_rate,
                  c.hourly_excess_sell_rate_dlrs_per_kwh as ur_flat_sell_rate,
                	diffusion_wind.scoe(a.load_kwh_per_customer_in_bin,
                                  a.naep_no_derate * e.derate_factor, 
                                  e.turbine_size_kw,
                                  c.system_size_limit_kw,
                                  d.sys_size_target_nem,
                                  d.sys_oversize_limit_nem,
                                  d.sys_size_target_no_nem,
                                  d.sys_oversize_limit_no_nem) as scoe_return
                FROM %(schema)s.pt_%(sector_abbr)s_sample_load_rate_turbine_resource_%(i_place_holder)s a
                
                INNER JOIN %(schema)s.temporal_factors_market b
                    ON a.census_division_abbr = b.census_division_abbr

                INNER JOIN %(schema)s.temporal_factors_technology e
                    ON a.turbine_height_m = e.turbine_height_m
                    AND a.turbine_size_kw = e.turbine_size_kw
                    AND a.power_curve_id = e.power_curve_id
                    AND b.year = e.year
                   
                    
                LEFT JOIN %(schema)s.input_main_nem_scenario c
                    ON c.state_abbr = a.state_abbr
                    AND c.utility_type = a.utility_type
                    AND c.year = b.year
                    AND c.sector_abbr = '%(sector_abbr)s'
                
                LEFT JOIN %(schema)s.input_wind_performance_system_sizing_factors d
                    ON d.sector_abbr = '%(sector_abbr)s'

                WHERE b.sector_abbr = '%(sector_abbr)s'
                    AND b.rate_escalation_source = '%(rate_escalation_source)s'
                    AND b.load_growth_scenario = '%(load_growth_scenario)s'
            )
                SELECT micro_id, county_id, bin_id, year, state_abbr, census_division_abbr, utility_type, hdf_load_index,
                   pca_reg, reeds_reg, rate_escalation_factor, incentive_array_id, ranked_rate_array_id, 
                   carbon_price_cents_per_kwh, 
            
                   fixed_om_dollars_per_kw_per_yr, 
                   variable_om_dollars_per_kwh, 
                   installed_costs_dollars_per_kw, 
            
                   ann_cons_kwh, 
                   customers_in_bin, initial_customers_in_bin, 
                   load_kwh_in_bin, initial_load_kwh_in_bin, load_kwh_per_customer_in_bin, 
                   crb_model, max_demand_kw, rate_id_alias, rate_source,
                   (scoe_return).nem_available as ur_enable_net_metering,
                   nem_system_size_limit_kw,
                   ur_nm_yearend_sell_rate,
                   ur_flat_sell_rate,

                   naep,
                   naep*(scoe_return).nturb*turbine_size_kw as aep,
                   (scoe_return).nturb*turbine_size_kw as system_size_kw,
                   (scoe_return).nturb as nturb,
                   turbine_id,
                   i, j, cf_bin,
                   turbine_size_kw, 
                   turbine_height_m, 
                   (round((scoe_return).scoe,4)*1000)::BIGINT as scoe,
                   ownocc8 as owner_occupancy_status
          FROM combined;
          
          CREATE INDEX pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s_sort_fields_btree
             ON %(schema)s.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s
             USING BTREE(county_id ASC, bin_id ASC, year ASC, scoe ASC, system_size_kw ASC, turbine_height_m ASC);           
          """ % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)
    print time.time() - t0

    #==============================================================================
    #    Find the Most Cost-Effective Wind Turbine Configuration for Each Customer Bin
    #==============================================================================
    msg = "Selecting the most cost-effective wind turbine configuration for each customer bin and year"
    t0 = time.time()
    logger.info(msg)
    # create empty table
    sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_best_option_each_year;
            CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_best_option_each_year AS
            SELECT *
            FROM %(schema)s.pt_%(sector_abbr)s_sample_all_combinations_0
            LIMIT 0;""" % inputs    
    cur.execute(sql)
    con.commit()
    
    sql =  """INSERT INTO %(schema)s.pt_%(sector_abbr)s_best_option_each_year
              SELECT distinct on (a.county_id, a.bin_id, a.year) a.*
              FROM  %(schema)s.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s a
              ORDER BY a.county_id ASC, a.bin_id ASC, a.year ASC, a.scoe ASC,
                       a.system_size_kw ASC, a.turbine_height_m ASC;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    # create indices
    sql = """CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_join_fields_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(county_id,bin_id);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_year_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(year);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_incentive_array_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(incentive_array_id);              
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_i_j_cf_bin_height_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(i, j, cf_bin, turbine_height_m);     
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_turbine_id_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(turbine_id);   
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_system_size_kw_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(system_size_kw);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_rate_id_alias_source_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(rate_id_alias, rate_source);             
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_hdf_load_index_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(hdf_load_index);  
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_crb_model_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(crb_model);  

             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_load_kwh_per_customer_in_bin_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(load_kwh_per_customer_in_bin);      
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_nem_fields_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year
             USING BTREE(ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate);                  
             
             """ % inputs
    cur.execute(sql)
    con.commit()
    
    print time.time() - t0

    #==============================================================================
    #   clean up intermediate tables
    #==============================================================================
    msg = "Cleaning up intermediate tables"
    logger.info(msg)
    intermediate_tables = ['%(schema)s.pt_%(sector_abbr)s_sample_%(i_place_holder)s' % inputs,
                           '%(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s' % inputs,
                           '%(schema)s.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s' % inputs,
                           '%(schema)s.pt_%(sector_abbr)s_sample_load_rate_allowable_turbines_%(i_place_holder)s' % inputs,
                           '%(schema)s.pt_%(sector_abbr)s_sample_load_rate_turbine_resource_%(i_place_holder)s' % inputs,
                           '%(schema)s.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s' % inputs,
                           '%(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s' % inputs,
                           '%(schema)s.pt_%(sector_abbr)s_sample_load_applicable_rates_%(i_place_holder)s' % inputs,
                           '%(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s' % inputs    ]

      
         
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
    final_table = '%(schema)s.pt_%(sector_abbr)s_best_option_each_year' % inputs

    return final_table

def get_unique_parameters_for_urdb3(cur, con, technology, schema, sectors):
    
    
    inputs_dict = locals().copy()     
       
    if technology == 'wind':
        inputs_dict['resource_keys'] = 'i, j, cf_bin, turbine_height_m, turbine_id'
    elif technology == 'solar':
        inputs_dict['resource_keys'] = 'solar_re_9809_gid, tilt, azimuth'


    sqls = []
    for sector_abbr, sector in sectors.iteritems():
        inputs_dict['sector'] = sector
        inputs_dict['sector_abbr'] = sector_abbr
        sql = """SELECT  rate_id_alias, rate_source,
                    	hdf_load_index, crb_model, load_kwh_per_customer_in_bin,
                        %(resource_keys)s, system_size_kw, 
                        ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate
                FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year
                GROUP BY  rate_id_alias, rate_source,
                    	 hdf_load_index, crb_model, load_kwh_per_customer_in_bin,
                    	 %(resource_keys)s, system_size_kw,
                         ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate""" % inputs_dict
        sqls.append(sql)      
    
    
    inputs_dict['sql'] = ' UNION '.join(sqls)    
    sql = """DROP TABLE IF EXISTS %(schema)s.unique_rate_gen_load_combinations;
             CREATE UNLOGGED TABLE %(schema)s.unique_rate_gen_load_combinations AS
             %(sql)s;""" % inputs_dict
    cur.execute(sql)
    con.commit()
    
    
    # create indices on: rate_id_alias, hdf_load_index, crb_model, resource keys
    sql = """CREATE INDEX unique_rate_gen_load_combinations_rate_id_alias_btree
             ON %(schema)s.unique_rate_gen_load_combinations
             USING BTREE(rate_id_alias);
             
             CREATE INDEX unique_rate_gen_load_combinations_rate_source_btree
             ON %(schema)s.unique_rate_gen_load_combinations
             USING BTREE(rate_source);
            
             CREATE INDEX unique_rate_gen_load_combinations_hdf_load_index_btree
             ON %(schema)s.unique_rate_gen_load_combinations
             USING BTREE(hdf_load_index);
             
             CREATE INDEX unique_rate_gen_load_combinations_crb_model_btree
             ON %(schema)s.unique_rate_gen_load_combinations
             USING BTREE(crb_model);

             CREATE INDEX unique_rate_gen_load_combinations_load_kwh_btree
             ON %(schema)s.unique_rate_gen_load_combinations
             USING BTREE(load_kwh_per_customer_in_bin);
             
             CREATE INDEX unique_rate_gen_load_combinations_system_size_kw_btree
             ON %(schema)s.unique_rate_gen_load_combinations
             USING BTREE(system_size_kw);
             
             CREATE INDEX unique_rate_gen_load_combinations_resource_keys_btree
             ON %(schema)s.unique_rate_gen_load_combinations
             USING BTREE(%(resource_keys)s);
             
             CREATE INDEX unique_rate_gen_load_combinations_nem_fields_btree
             ON %(schema)s.unique_rate_gen_load_combinations
             USING BTREE(ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate);
             """ % inputs_dict
             
    cur.execute(sql)
    con.commit()
    
    # add a unique id/primary key
    sql = """ALTER TABLE %(schema)s.unique_rate_gen_load_combinations
             ADD COLUMN uid serial PRIMARY KEY;""" % inputs_dict
    cur.execute(sql)
    con.commit()
    
def get_max_row_count_for_utilityrate3():
    
    # find the total size of memory on the system
    mem = psutil.virtual_memory()
    free_mem = mem.available
    # target to fill up only an eighth of the total memory 
    # (this gives a buffer)
    target_mem = int(free_mem/16)
    
    # previous runs suggest that the size of each row in bytes will generally be less than:
    row_mem = 300000
    
    # how many rows can be stored in the target mem?
    total_rows = target_mem/row_mem
    
    return total_rows
    

def split_utilityrate3_inputs(row_count_limit, cur, con, schema):

    inputs_dict = locals().copy()    
    
   # find the set of uids     
    sql =   """SELECT uid 
               FROM %(schema)s.unique_rate_gen_load_combinations
               ORDER BY uid;""" % inputs_dict
    cur.execute(sql)
    uids = [row['uid'] for row in cur.fetchall()]
    # find how many total uids there are
    total_row_count = len(uids)
    # determine the approximate chunk size
    num_chunks = np.ceil(float(total_row_count)/row_count_limit)
    
    # split the uids into npar chunks
    uid_chunks = np.array_split(uids, num_chunks)
    
    return uid_chunks

    
    

def get_utilityrate3_inputs(uids, cur, con, technology, schema, npar, pg_conn_string):
    
    
    inputs_dict = locals().copy()     
       
    inputs_dict['load_scale_offset'] = 1e8
    if technology == 'wind':
        inputs_dict['gen_join_clause'] = """a.i = d.i
                                            AND a.j = d.j
                                            AND a.cf_bin = d.cf_bin
                                            AND a.turbine_height_m = d.height
                                            AND a.turbine_id = d.turbine_id"""
        inputs_dict['gen_scale_offset'] = 1e3
    elif technology == 'solar':
        inputs_dict['gen_join_clause'] = """a.solar_re_9809_gid = d.solar_re_9809_gid
                                            AND a.tilt = d.tilt
                                            AND a.azimuth = d.azimuth"""
        inputs_dict['gen_scale_offset'] = 1e6

    # split the uids up into chunks for parallel processing        
    uid_chunks = map(list, np.array_split(uids, npar))    
    inputs_dict['chunk_place_holder'] = '%(uids)s'        

    # build out the sql query that will be used to collect the data
    sql = """
            -- COMBINE LOAD DATA FOR RES AND COM INTO SINGLE TABLE
            WITH eplus as 
            (
                	SELECT hdf_index, crb_model, nkwh
                	FROM diffusion_shared.energy_plus_normalized_load_res
                	WHERE crb_model = 'reference'
                	UNION ALL
                	SELECT hdf_index, crb_model, nkwh
                	FROM diffusion_shared.energy_plus_normalized_load_com
            )
                   
            SELECT 	a.uid, 
                    	b.sam_json as rate_json, 
                        a.load_kwh_per_customer_in_bin, c.nkwh as consumption_hourly,
                        a.system_size_kw,
                        COALESCE(d.cf,  array_fill(1, array[8760])) as generation_hourly, -- fill in for customers with no matching wind resource (values don't matter because they will be zeroed out)
                        a.ur_enable_net_metering, a.ur_nm_yearend_sell_rate, a.ur_flat_sell_rate
            	
            FROM %(schema)s.unique_rate_gen_load_combinations a
            
            -- JOIN THE RATE DATA
            LEFT JOIN %(schema)s.all_rate_jsons b 
                    ON a.rate_id_alias = b.rate_id_alias
                    AND a.rate_source = b.rate_source
            
            -- JOIN THE LOAD DATA
            LEFT JOIN eplus c
                    ON a.crb_model = c.crb_model
                    AND a.hdf_load_index = c.hdf_index
            
            -- JOIN THE RESOURCE DATA
            LEFT JOIN diffusion_%(technology)s.%(technology)s_resource_hourly d
                    ON %(gen_join_clause)s
            
            WHERE a.uid IN (%(chunk_place_holder)s);""" % inputs_dict

    results = JoinableQueue()    
    jobs = []
 
    for i in range(npar):
        place_holders = {'uids': pylist_2_pglist(uid_chunks[i])}
        isql = sql % place_holders
        proc = Process(target = p_get_utilityrate3_inputs, args = (inputs_dict, pg_conn_string, isql, results))
        jobs.append(proc)
        proc.start()
    
    # get the results from the parallel processes (this method avoids deadlocks)
    results_list = []
    for i in range(0, npar):
        result = results.get()
        results_list.append(result)

    # concatenate all of the dataframes into a single data frame
    results_df = pd.concat(results_list)
    # reindex the dataframe
    results_df.reset_index(drop = True, inplace = True)
    
    return results_df

def update_rate_json_w_nem_fields(row):
    
    nem_fields = ['ur_enable_net_metering', 'ur_nm_yearend_sell_rate', 'ur_flat_sell_rate']
    nem_dict = dict((k, row[k]) for k in nem_fields)
    row['rate_json'].update(nem_dict)
    
    return row


def scale_array(row, array_col, scale_col, prec_offset_value):
    
    row[array_col] = (np.array(row[array_col], dtype = 'int64') * np.float(row[scale_col]))/prec_offset_value
    
    return row

def p_get_utilityrate3_inputs(inputs_dict, pg_conn_string, sql, queue):
    try:
        # create cursor and connection
        con, cur = make_con(pg_conn_string)  
        # get the data from postgres
        df = pd.read_sql(sql, con, coerce_float = False)
        # close cursor and connection
        con.close()
        cur.close()
        
        # scale the normalized hourly load based on the annual load and scale offset factor
        df = df.apply(scale_array, axis = 1, args = ('consumption_hourly','load_kwh_per_customer_in_bin', inputs_dict['load_scale_offset']))
        
        # scale the hourly cfs into hourly kw using the system size
        df = df.apply(scale_array, axis = 1, args = ('generation_hourly','system_size_kw', inputs_dict['gen_scale_offset']))
        
        # update the net metering fields in the rate_json
        df = df.apply(update_rate_json_w_nem_fields, axis = 1)
               
        # add the results to the queue
        queue.put(df[['uid','rate_json','consumption_hourly','generation_hourly']])
        
    except Exception, e:
        print 'Error: %s' % e
        print sql
    

def run_utilityrate3(df, logger):
    # NOTE: This method is slower than pssc_mp.pssc_mp()
    # unless there is only one core available, in which case
    # this method will run faster due to no overhead of setting
    # up multiprocessing
    from pssc import utilityrate3
    results = []
    for i in range(0, df.shape[0]):
        uid = df['uid'][i]
        generation_hourly = df['generation_hourly'][i]
        consumption_hourly = df['consumption_hourly'][i]
        rate_json = df['rate_json'][i]
        sam_out = utilityrate3(generation_hourly, consumption_hourly, rate_json, analysis_period=1., inflation_rate=0., degradation=(0.,),
                 return_values=('elec_cost_with_system_year1', 'elec_cost_without_system_year1'), logger = logger)
        sam_out['uid'] = uid
        results.append(sam_out)
    
    results_df = pd.DataFrame.from_dict(results)
    # round costs to 2 decimal places (i.e., pennies)
    results_df['elec_cost_with_system_year1'] = results_df['elec_cost_with_system_year1'].round(2)
    results_df['elec_cost_without_system_year1'] = results_df['elec_cost_without_system_year1'].round(2)
    
    return results_df
    

def write_utilityrate3_to_pg(cur, con, sam_results_list, schema, sectors, technology):
    
    inputs_dict = locals().copy()  

    # concatenate all of the dataframes into a single data frame
    sam_results_df = pd.concat(sam_results_list)
    # reindex the dataframe
    sam_results_df.reset_index(drop = True, inplace = True)     
    
    # set the join clauses depending on the technology
    if technology == 'wind':
        inputs_dict['resource_join_clause'] = """a.i = b.i
                                            AND a.j = b.j
                                            AND a.cf_bin = b.cf_bin
                                            AND a.turbine_height_m = b.turbine_height_m
                                            AND a.turbine_id = b.turbine_id """
    elif technology == 'solar':
        inputs_dict['resource_join_clause'] = """a.solar_re_9809_gid = b.solar_re_9809_gid
                                            AND a.tilt = b.tilt
                                            AND a.azimuth = b.azimuth """
    
      
    #==============================================================================
    #     CREATE TABLE TO HOLD RESULTS
    #==============================================================================
    sql = """DROP TABLE IF EXISTS %(schema)s.utilityrate3_results;
             CREATE UNLOGGED TABLE %(schema)s.utilityrate3_results
             (
                uid integer,
                elec_cost_with_system_year1 NUMERIC,
                elec_cost_without_system_year1 NUMERIC,
                excess_generation_percent NUMERIC
                
             );
             """ % inputs_dict
    cur.execute(sql)
    con.commit()
    
    # open an in memory stringIO file (like an in memory csv)
    s = StringIO()
    # write the data to the stringIO
    sam_results_df[['uid','elec_cost_with_system_year1','elec_cost_without_system_year1','excess_generation_percent']].to_csv(s, index = False, header = False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    cur.copy_expert('COPY %(schema)s.utilityrate3_results FROM STDOUT WITH CSV' % inputs_dict, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()    
    s.close()
    
    # add primary key constraint to uid field
    sql = """ALTER TABLE %(schema)s.utilityrate3_results ADD PRIMARY KEY (uid);""" % inputs_dict
    cur.execute(sql)
    con.commit()
    
    
    #==============================================================================
    #     APPEND THE RESULTS TO CUSTOMER BINS
    #==============================================================================
    for sector_abbr, sector in sectors.iteritems():
        inputs_dict['sector_abbr'] = sector_abbr
        inputs_dict['sector'] = sector
        sql = """   DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_elec_costs;
                    CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_elec_costs AS
                    
                    SELECT a.county_id, a.bin_id, a.year, 
                        c.elec_cost_with_system_year1 as first_year_bill_with_system, 
                        c.elec_cost_without_system_year1 as first_year_bill_without_system,
                        c.excess_generation_percent as excess_generation_percent
                    FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year a
                
                    LEFT JOIN %(schema)s.unique_rate_gen_load_combinations b
                        ON a.rate_id_alias = b.rate_id_alias
                        AND a.rate_source = b.rate_source
                        AND a.hdf_load_index = b.hdf_load_index
                        AND a.crb_model = b.crb_model
                        AND a.load_kwh_per_customer_in_bin = b.load_kwh_per_customer_in_bin
                        AND a.system_size_kw = b.system_size_kw
                        AND %(resource_join_clause)s
                        AND a.ur_enable_net_metering = b.ur_enable_net_metering
                        AND a.ur_nm_yearend_sell_rate = b.ur_nm_yearend_sell_rate
                        AND a.ur_flat_sell_rate = b.ur_flat_sell_rate
                        
                    LEFT JOIN %(schema)s.utilityrate3_results c
                        ON b.uid = c.uid
            
        """ % inputs_dict
        
        cur.execute(sql)
        con.commit()
    
        # add indices on: county_id, bin_id, year
        sql = """CREATE INDEX pt_%(sector_abbr)s_elec_costs_join_fields_btree 
                 ON %(schema)s.pt_%(sector_abbr)s_elec_costs
                 USING BTREE(county_id,bin_id);
             
                 CREATE INDEX pt_%(sector_abbr)s_elec_costs_year_btree 
                 ON %(schema)s.pt_%(sector_abbr)s_elec_costs
                 USING BTREE(year);""" % inputs_dict
        cur.execute(sql)
        con.commit()

def get_sectors(cur, schema):
    '''Return the sectors to model from table view in postgres.
        Returned as a dictionary.
        '''    
    
    sql = '''SELECT sectors 
              FROM %s.sectors_to_model;''' % schema
    cur.execute(sql)
    sectors = cur.fetchone()['sectors']
    return sectors
    
def get_system_degradation(cur, schema):
    '''Return the annual system degradation rate as float.
        '''    
    sql = '''SELECT ann_system_degradation 
             FROM %s.input_solar_performance_annual_system_degradation;''' % schema
    cur.execute(sql)
    ann_system_degradation = cur.fetchone()['ann_system_degradation']
    return ann_system_degradation    
    
        
def get_depreciation_schedule(con, schema, type = 'macrs, standard'):
    ''' Pull depreciation schedule from dB
    
        IN: type - string - [all, macrs, standard] 
        OUT: df  - pd dataframe - year, depreciation schedule:

    '''
    inputs = locals().copy()    
            
    inputs['field'] = type.lower()
    sql = '''SELECT %(field)s 
             FROM %(schema)s.input_solar_finances_depreciation_schedule;''' % inputs
    df = sqlio.read_frame(sql, con)
    return df
    
def get_scenario_options(cur, schema):
    ''' Pull scenario options from dB
    
    '''
    sql = '''SELECT * 
             FROM %s.input_main_scenario_options;''' % schema
    cur.execute(sql)
    results = cur.fetchall()[0]
    return results


def get_dsire_incentives(cur, con, schema, tech, sector_abbr, preprocess, npar, pg_conn_string, logger):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()

    msg = "Identifying initial incentives for customer bins from DSIRE Database"
    logger.info(msg)
    
    if sector_abbr == 'ind':
        inputs['incentives_sector'] = 'com'
    else:
        inputs['incentives_sector'] = sector_abbr    
    
    sql =   """
                WITH a AS
                (
                	SELECT DISTINCT incentive_array_id as incentive_array_id
                	FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year
                	WHERE year = 2014
                )
                SELECT a.incentive_array_id, c.*
                FROM a
                LEFT JOIN diffusion_%(tech)s.dsire_incentives_simplified_lkup_%(sector_abbr)s b
                    ON a.incentive_array_id = b.incentive_array_id
                LEFT JOIN diffusion_%(tech)s.incentives c
                    ON b.incentives_uid = c.uid
                WHERE lower(c.sector) = '%(incentives_sector)s'
                ORDER BY a.incentive_array_id
            """ % inputs
    df = sqlio.read_frame(sql, con, coerce_float = False)
    return df


def get_initial_market_shares(cur, con, tech, sector_abbr, sector, schema, technology):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()     
    # add the starting capacity table to the inputs dict
    inputs['cap_table'] = 'starting_capacities_mw_2012_q4_us'

    sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_initial_market_shares;
             CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_initial_market_shares AS
             WITH a as
             (
			SELECT county_id, bin_id, state_abbr,
				CASE  WHEN system_size_kw = 0 then 0
					ELSE customers_in_bin
				END AS customers_in_bin
			FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year	
			WHERE year = 2014			
             ),
             b as
             (
                	SELECT a.county_id, a.bin_id,
                		(a.customers_in_bin/sum(a.customers_in_bin) OVER (PARTITION BY a.state_abbr)) * b.systems_count_%(sector)s AS initial_number_of_adopters,
                		(a.customers_in_bin/sum(a.customers_in_bin) OVER (PARTITION BY a.state_abbr)) * b.capacity_mw_%(sector)s AS initial_capacity_mw,
                		a.customers_in_bin
                	FROM a
                	LEFT JOIN diffusion_%(tech)s.starting_capacities_mw_2012_q4_us b
                		ON a.state_abbr = b.state_abbr
            ) 
            SELECT b.county_id, b.bin_id,
                 ROUND(COALESCE(b.initial_number_of_adopters, 0)::NUMERIC, 6) as initial_number_of_adopters,
                 ROUND(COALESCE(b.initial_capacity_mw, 0)::NUMERIC, 6) as initial_capacity_mw,
        	     CASE  WHEN customers_in_bin = 0 then 0
                       ELSE ROUND(COALESCE(b.initial_number_of_adopters/b.customers_in_bin, 0)::NUMERIC, 6) 
                 END AS initial_market_share
            FROM b;""" % inputs
    cur.execute(sql)
    con.commit()    
    
    
    sql = """CREATE INDEX pt_%(sector_abbr)s_initial_market_shares_join_fields_btree 
             ON %(schema)s.pt_%(sector_abbr)s_initial_market_shares 
             USING BTREE(county_id,bin_id);""" % inputs
    cur.execute(sql)
    con.commit()

    # BOS - installed capacity is stored as MW in the database, but to be consisent with calculations should be in kW
    sql = """SELECT county_id, bin_id, 
                    initial_market_share AS market_share_last_year,
                    initial_number_of_adopters AS number_of_adopters_last_year,
                    1000 * initial_capacity_mw AS installed_capacity_last_year 
            FROM %(schema)s.pt_%(sector_abbr)s_initial_market_shares;""" % inputs
    df = sqlio.read_frame(sql, con)
    return df  


def get_main_dataframe(con, sector_abbr, schema, year):
    ''' Pull main pre-processed dataframe from dB
    
        IN: con - pg con object - connection object
        OUT: df  - pd dataframe - pre-processed resource,bins, rates, etc. for all years:

    '''
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs_dict = locals().copy()     
    
    sql = """SELECT a.*, b.first_year_bill_with_system, b.first_year_bill_without_system, b.excess_generation_percent
            FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year a
            LEFT JOIN %(schema)s.pt_%(sector_abbr)s_elec_costs b
                    ON a.county_id = b.county_id
                    AND a.bin_id = b.bin_id
                    AND a.year = b.year
            WHERE a.year = %(year)s""" % inputs_dict
    df = sqlio.read_frame(sql, con, coerce_float = False)

    return df
    
def get_financial_parameters(con, schema, tech):
    ''' Pull financial parameters dataframe from dB. We used to filter by business model here, but with leasing we will join
    on sector and business_model later in calc_economics.
    
        IN: con - pg con object - connection object
            schema - string - schema for technology i.e. diffusion_solar
            
        OUT: fin_param  - pd dataframe - pre-processed resource,bins, rates, etc. for all years:
    '''
    inputs = locals().copy()
    
    sql = '''SELECT * 
             FROM %(schema)s.input_%(tech)s_finances;''' % inputs
    df = sqlio.read_frame(sql, con)
    
    # minor formatting for table joins later on
    df.sector = df.sector.str.lower()
    df['business_model'] = df.ownership_model.str.lower().str.replace(" ", "_").str.replace("leased","tpo")
    df = df.drop('ownership_model', axis = 1)
    
    return df
 
#==============================================================================
   
def get_max_market_share(con, schema):
    ''' Pull max market share from dB, select curve based on scenario_options, and interpolate to tenth of a year. 
        Use passed parameters to determine ownership type
    
        IN: con - pg con object - connection object
            schema - string - schema for technology i.e. diffusion_solar

            
        OUT: max_market_share  - pd dataframe - dataframe to join on main df to determine max share 
                                                keys are sector & payback period 
    '''

    sql = '''SELECT * 
             FROM %s.max_market_curves_to_model;'''  % schema
    max_market_share = sqlio.read_frame(sql, con)
   
    return max_market_share
    

def get_market_projections(con, schema):
    ''' Pull market projections table from dB
    
        IN: con - pg con object - connection object
        OUT: market_projections - numpy array - table containing various market projections
    '''
    sql = '''SELECT * 
             FROM %s.input_main_market_projections;''' % schema
    return sqlio.read_frame(sql , con)


def get_manual_incentive_options(con, schema, tech):
    
    inputs = locals().copy()
    
    sql = '''SELECT overwrite_exist_inc, incentive_start_year
             FROM %(schema)s.input_%(tech)s_incentive_options;''' % inputs
    df = sqlio.read_frame(sql, con)
    
    return df            

    
def get_manual_incentives(con, schema, tech):
    ''' Pull manual incentives from input sheet
    
        IN: con - pg con object - connection object
        OUT: inc - pd dataframe - dataframe of manual incentives
    '''
    inputs = locals().copy()    
    
    sql = '''SELECT * 
             FROM %(schema)s.input_%(tech)s_incentives;''' % inputs
    df = sqlio.read_frame(sql, con)
    df['sector'] = df['sector'].str.lower()
    return df
 
def calc_manual_incentives(df, con, cur_year, schema, tech):
    ''' Calculate the value in first year and length for incentives manually 
    entered in input sheet. 

        IN: df - pandas DataFrame - main dataframe
            cur - SQL cursor 
                        
        OUT: manual_incentives_value - pandas DataFrame - value of rebate, tax incentives, and PBI
    '''
    # Join manual incentives with main df   
    inc = get_manual_incentives(con, schema, tech)
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
    join above could join on multiple rows. Thus, groupby by county_id & bin_id 
    to sum over incentives and condense back to unique county_id/bin_id/business_model combinations
    '''

    value_of_incentives = d[['county_id', 'bin_id', 'business_model','value_of_increment', 'value_of_pbi_fit', 'value_of_ptc', 'pbi_fit_length', 'ptc_length', 'value_of_rebate', 'value_of_tax_credit_or_deduction']].groupby(['county_id','bin_id','business_model']).sum().reset_index() 
    
    return value_of_incentives
    
def calc_dsire_incentives(inc, cur_year, default_exp_yr = 2016, assumed_duration = 10):
    '''
    Calculate the value of incentives based on DSIRE database. There may be many incentives per each customer bin (county_id+bin_id),
    so the value is calculated for each row (incentives)
    and then groupedby county_id & bin_id, summing over incentives value. For multiyear incentives (ptc/pbi/fit), this requires
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
    default_exp_date = np.array([datetime.date(default_exp_yr, 12, 31)]*len(inc))
    
    # Column names differ btw the wind and solar tables. 
    # Adding this exception handling so they have common set of columns
    
    for col in ['increment_4_capacity_kw','increment_4_rebate_dlrs_kw',
    'pbi_fit_max_size_for_dlrs_calc_kw','tax_credit_dlrs_kw',
    'pbi_fit_min_output_kwh_yr','increment_3_rebate_dlrs_kw',
    'increment_4_rebate_dlrs_kw']:
        if col not in inc.columns:
            inc[col] = None
    
    ## Coerce incentives to following types:
    # Don't loop over column names, because some are strings e.g. don't coerce all to floats
    inc.increment_1_capacity_kw = inc.increment_1_capacity_kw.astype(float)
    inc.increment_2_capacity_kw = inc.increment_2_capacity_kw.astype(float)
    inc.increment_3_capacity_kw = inc.increment_3_capacity_kw.astype(float)
    inc.increment_4_capacity_kw = inc.increment_4_capacity_kw.astype(float)
    inc.increment_1_rebate_dlrs_kw = inc.increment_1_rebate_dlrs_kw.astype(float)
    inc.increment_2_rebate_dlrs_kw = inc.increment_2_rebate_dlrs_kw.astype(float)
    inc.increment_3_rebate_dlrs_kw = inc.increment_3_rebate_dlrs_kw.astype(float)
    inc.increment_4_rebate_dlrs_kw = inc.increment_4_rebate_dlrs_kw.astype(float)
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

    increment_vars = ['increment_1_capacity_kw','increment_2_capacity_kw','increment_3_capacity_kw','increment_4_capacity_kw', 'increment_1_rebate_dlrs_kw','increment_2_rebate_dlrs_kw','increment_3_rebate_dlrs_kw','increment_4_rebate_dlrs_kw']    
    inc[increment_vars] = inc[increment_vars].fillna(0)

    # The amount of capacity that qualifies for the increment
    cap_1 = np.minimum(inc.increment_1_capacity_kw, inc['system_size_kw'])
    cap_2 = np.maximum(inc['system_size_kw'] - inc.increment_1_capacity_kw,0)
    cap_3 = np.maximum(inc['system_size_kw'] - inc.increment_2_capacity_kw,0)
    cap_4 = np.maximum(inc['system_size_kw'] - inc.increment_3_capacity_kw,0)
    
    value_of_increment = cap_1 * inc.increment_1_rebate_dlrs_kw + cap_2 * inc.increment_2_rebate_dlrs_kw + cap_3 * inc.increment_3_rebate_dlrs_kw + cap_4 * inc.increment_4_rebate_dlrs_kw
    value_of_increment[np.isnan(value_of_increment)] = 0
    inc['value_of_increment'] = value_of_increment
    # Don't let increment exceed 20% of project cost
    inc['value_of_increment'] = np.where(inc['value_of_increment'] > 0.2 * inc['installed_costs_dollars_per_kw'] * inc['system_size_kw'],  0.2 * inc['installed_costs_dollars_per_kw'] * inc['system_size_kw'], inc['value_of_increment'])
    
    # 2. # Calculate lifetime value of PBI & FIT
    inc['pbi_fit_min_output_kwh_yr'] = inc['pbi_fit_min_output_kwh_yr'].fillna(0)    
    inc.pbi_fit_end_date[inc.pbi_fit_end_date.isnull()] = datetime.date(default_exp_yr, 12, 31) # Assign expiry if no date
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
    value_of_ptc =  ptc_still_exists * np.minimum(inc.ptc_dlrs_kwh * inc.aep * (ptc_max_size/inc.system_size_kw), inc.max_dlrs_yr)
    value_of_ptc[np.isnan(value_of_ptc)] = 0
    value_of_ptc = np.where(value_of_ptc < inc.max_tax_credit_dlrs, value_of_ptc,inc.max_tax_credit_dlrs)
    length_of_ptc = inc.ptc_duration_years
    length_of_ptc[np.isnan(length_of_ptc)] = 0
    
    # Lifetime value of the ptc. Assume all ptcs are disbursed over 10 years
    # This will get the undiscounted sum of incentive correct, present value may have small error
    inc['lifetime_value_of_ptc'] = length_of_ptc * value_of_ptc

    # 4. #Calculate Value of Rebate

    # check whether the credits are still active (this can be applied universally because DSIRE does not provide specific info 
    # about expirations for each tax credit or deduction). 
    #Assume that expiration date is inclusive e.g. consumer receives incentive in 2016 if expiration date of 2016 (or greater)
    if datetime.date(cur_year, 1, 1) >= datetime.date(default_exp_yr, 12, 31):
        value_of_rebate = 0.0
    else:
        rebate_cap = np.where(inc['system_size_kw'] < inc.rebate_min_size_kw, 0, inc['system_size_kw'])
        rebate_cap = np.where(rebate_cap > inc.rebate_max_size_kw, inc.rebate_max_size_kw, rebate_cap)
        value_of_rebate = inc.rebate_dlrs_kw * rebate_cap
        value_of_rebate = np.minimum(inc.rebate_max_dlrs, value_of_rebate)
        value_of_rebate = np.minimum(inc.rebate_pcnt_cost_max * ic, value_of_rebate)
        value_of_rebate[np.isnan(value_of_rebate)] = 0
    
    inc['value_of_rebate'] = value_of_rebate
    
    # 5. # Calculate Value of Tax Credit
    # Assume able to fully monetize tax credits
    
    # check whether the credits are still active (this can be applied universally because DSIRE does not provide specific info 
    # about expirations for each tax credit or deduction). 
    #Assume that expiration date is inclusive e.g. consumer receives incentive in 2016 if expiration date of 2016 (or greater)
    if datetime.date(cur_year, 1, 1) >= datetime.date(default_exp_yr, 12, 31):
        inc['value_of_tax_credit_or_deduction'] = 0.0
    else:
        inc.tax_credit_pcnt_cost = np.where(inc.tax_credit_pcnt_cost.isnull(), 0, inc.tax_credit_pcnt_cost)
        inc.tax_credit_pcnt_cost = np.where(inc.tax_credit_pcnt_cost >= 1, 0.01 * inc.tax_credit_pcnt_cost, inc.tax_credit_pcnt_cost)
        inc.tax_deduction_pcnt_cost = np.where(inc.tax_deduction_pcnt_cost.isnull(), 0, inc.tax_deduction_pcnt_cost)
        inc.tax_deduction_pcnt_cost = np.where(inc.tax_deduction_pcnt_cost >= 1, 0.01 * inc.tax_deduction_pcnt_cost, inc.tax_deduction_pcnt_cost)    
        tax_pcnt_cost = inc.tax_credit_pcnt_cost + inc.tax_deduction_pcnt_cost
        
        inc.max_tax_credit_dlrs = np.where(inc.max_tax_credit_dlrs.isnull(), 1e9, inc.max_tax_credit_dlrs)
        inc.max_tax_deduction_dlrs = np.where(inc.max_tax_deduction_dlrs.isnull(), 1e9, inc.max_tax_deduction_dlrs)
        max_tax_credit_or_deduction_value = np.maximum(inc.max_tax_credit_dlrs,inc.max_tax_deduction_dlrs)
        
        inc['tax_credit_dlrs_kw'] = inc['tax_credit_dlrs_kw'].fillna(0)
        
        value_of_tax_credit_or_deduction = tax_pcnt_cost * ic + inc['tax_credit_dlrs_kw'] * inc['system_size_kw']
        value_of_tax_credit_or_deduction = np.minimum(max_tax_credit_or_deduction_value, value_of_tax_credit_or_deduction)
        value_of_tax_credit_or_deduction = np.where(inc.tax_credit_max_size_kw < inc['system_size_kw'], tax_pcnt_cost * inc.tax_credit_max_size_kw * inc.installed_costs_dollars_per_kw, value_of_tax_credit_or_deduction)
        value_of_tax_credit_or_deduction = pd.Series(value_of_tax_credit_or_deduction).fillna(0)        
        #value_of_tax_credit_or_deduction[np.isnan(value_of_tax_credit_or_deduction)] = 0
        inc['value_of_tax_credit_or_deduction'] = value_of_tax_credit_or_deduction.astype(float)
    
    # sum results to customer bins
    inc = inc[['county_id', 'bin_id', 'business_model', 'value_of_increment', 'lifetime_value_of_pbi_fit', 'lifetime_value_of_ptc', 'value_of_rebate', 'value_of_tax_credit_or_deduction']].groupby(['county_id','bin_id','business_model']).sum().reset_index() 
    
    inc['value_of_pbi_fit'] = inc['lifetime_value_of_pbi_fit'] / assumed_duration
    inc['pbi_fit_length'] = assumed_duration
    
    inc['value_of_ptc'] = inc['lifetime_value_of_ptc'] / assumed_duration
    inc['ptc_length'] = assumed_duration
    
    return inc[['county_id','bin_id', 'business_model','value_of_increment', 'value_of_pbi_fit', 'value_of_ptc', 'pbi_fit_length', 'ptc_length', 'value_of_rebate', 'value_of_tax_credit_or_deduction']]

def get_rate_escalations(con, schema):
    '''
    Get rate escalation multipliers from database. Escalations are filtered and applied in calc_economics,
    resulting in an average real compounding rate growth. This rate is then used to calculate cash flows
    
    IN: con - connection to server
    OUT: DataFrame with census_division_abbr, sector, year, escalation_factor, and source as columns
    '''  
    sql = """SELECT census_division_abbr, year, 
                lower(sector) as sector, escalation_factor
                FROM %s.rate_escalations_to_model;""" % schema
    rate_escalations = sqlio.read_frame(sql, con)
    return rate_escalations

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
    
    rate_structures_df = sqlio.read_frame(sql, con)
    rate_structures = dict(zip(rate_structures_df['sector_abbr'], rate_structures_df['rate_structure']))
    
    return rate_structures    
  
def get_lease_availability(con, schema, tech):
    '''
    Get leasing availability by state and year, based on options selected in input sheet
    
    IN: con - connection to server
    OUT: DataFrame with state, year, and availability (True/False) as columns
    '''  
    inputs = locals().copy()    
    
    sql = '''SELECT state_abbr, year, leasing_allowed
                FROM %(schema)s.input_%(tech)s_leasing_availability;''' % inputs
    df = sqlio.read_frame(sql, con)
    return df
    
def calc_expected_rate_escal(df, rate_escalations, year, sector_abbr,tech_lifetime): 
    '''
    Append the expected rate escalation to the main dataframe.
    Get rate escalation multipliers from database. Escalations are filtered and applied in calc_economics,
    resulting in an average real compounding rate growth. This rate is then used to calculate cash flows
    
    IN: con - connection to server
    OUT: DataFrame with census_division_abbr, sector, year, escalation_factor, and source as columns
    '''  
    
    # Only use the escalation multiplier over the next 30 years
    projected_rate_escalations = rate_escalations[(rate_escalations['year'] < (year + tech_lifetime)) & (rate_escalations['year'] >=  year) & (rate_escalations['sector'] == sector_abbr.lower())]
    
    rate_pivot = projected_rate_escalations.pivot(index = 'census_division_abbr',columns = 'year', values = 'escalation_factor')    
    rate_pivot['census_division_abbr'] = rate_pivot.index
    
    # Need to join expected escalations on df without sorting, thus remerge with original frame
    # see: http://stackoverflow.com/questions/20206615/how-can-a-pandas-merge-preserve-order
    temp_df = df[['county_id','bin_id','business_model','census_division_abbr']]
    customer_expected_escalations = temp_df.merge(temp_df.merge(rate_pivot, how = 'left', on = 'census_division_abbr', sort = False))
    
    if (df[['county_id','bin_id','business_model']] == customer_expected_escalations[['county_id','bin_id','business_model']]).all().all():
        return customer_expected_escalations.ix[:,4:].values
    else:
        raise Exception("rate_escalations_have been reordered!")

def fill_jagged_array(vals,lens, cols):
    '''
    Create a 'jagged' array filling each row with a value of variable length.
    vals and lens must be equal length; cols gives the number of columns of the
    output ndarray
    
    IN: 
        vals - np array containing values to fill
        lens - np array containing lengths of values to fill
        cols - integer of number of columns in output array
    
    OUT:
        
        jagged numpy array
    '''
    
    rows = vals.shape[0]
    # create a 1d array of zeros, same size as array b
    z = np.zeros((rows,),dtype = int)
    
    # combine a and b within a 1d array in an alternating manner
    az = np.vstack((vals,z)).ravel(1)    
    # calculate the number of repeats necessary for the zeros, then combine with b in a 1d array in an alternating manner
    bz = np.vstack((lens,cols-lens)).ravel(1)
    # use the repeate function to repeate elements in az by the factors in bz, then reshape to the final array size and shape
    r = np.repeat(az,bz).reshape((rows,cols))
    return r
            
def assign_business_model(df, prng, method = 'prob', alpha = 2):
    
    if method == 'prob':
        
        # The method here is to calculate a probability of leasing based on the relative
        # trade-off of market market shares. Then we draw a random number to determine if
        # the customer leases (# < prob of leasing). A ranking method is used as a mask to
        # identify which rows to drop 
        
        # sort the dataframe (may not be necessry)
#        df = df.sort(['county_id', 'bin_id', 'business_model'])
        
        # Calculate the logit value and sum of logit values for the bin id
        df['mkt_exp'] = df['max_market_share']**alpha
        gb = df.groupby(['county_id','bin_id'])
        gb = pd.DataFrame({'mkt_sum': gb['mkt_exp'].sum()})
        
        # Draw a random number for both business models in the bin
        gb['rnd'] = prng.rand(len(gb)) 
        df = df.merge(gb, left_on=['county_id','bin_id'],right_index = True)
        
        # Determine the probability of leasing
        df['prob_of_leasing'] = df['mkt_exp']/df['mkt_sum']
        df.loc[(df['business_model'] == 'tpo') & ~(df['leasing_allowed']),'prob_of_leasing'] = 0 #Restrict leasing if not allowed by state
        
        # Both business models are still in the df, so we use a ranking algorithm after the random draw
        # To determine whether to buy or lease 
        df['rank'] = 0
        df.loc[(df['business_model'] == 'host_owned'),'rank'] = 1
        df.loc[(df['business_model'] == 'tpo') & (df['rnd']< df['prob_of_leasing']),'rank'] = 2
        
        
        gb = df.groupby(['county_id','bin_id'])
        rb = gb['rank'].rank(ascending = False)
        df['econ_rank'] = rb    
        df = df[df.econ_rank == 1]
        
        df = df.drop(['mkt_exp','mkt_sum','rnd','rank','econ_rank'],axis = 1)
        #df[['county_id','bin_id','business_model','max_market_share','rnd','prob_of_leasing','econ_rank']].head(10)
        
    if method == 'rank':
        
        # just pick the business model with a higher max market share
        df['mms'] = df['max_market_share']
        df.loc[(df['business_model'] == 'tpo') & ~(df['leasing_allowed']),'mms'] = 0
        gb = df.groupby(['county_id','bin_id'])
       
        rb = gb['mms'].rank(ascending = False)
        df['econ_rank'] = rb    
        df = df[df.econ_rank == 1]
        df = df.drop(['econ_rank','mms'], axis = 1)
        
    return df

def summarise_solar_resource_by_ts_and_pca_reg(df, con):
    '''
    Outputs for ReEDS linkage the solar capacity factor by time slice and PCA 
    weighted by the existing azimuth/tilts deployed. Summary is based
    on a pre-processing step which finds the mean CF by timeslice by averaging
    over the point-level resource (solar_re_9808_gid) within a PCA
    
    IN: 
        con
        df
    
    OUT:
        
        pandas dataframe [pca_reg, ts, cf]
    '''
    
    # Query the solar resource by pca, tilt, azimuth, & timeslice and rename columns e.g. at this point resource has already been averaged over solar_re_9809_gid 
    resource = pd.read_sql("SELECT * FROM diffusion_solar.solar_resource_by_pca_summary;", con)
    resource.drop('npoints', axis =1, inplace = True)
    resource['pca_reg'] = 'p' + resource.pca_reg.map(str)
    resource.columns = ['pca_reg','tilt','azimuth','H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16','H17']

    # Determine the percentage of adopters that have selected a given azimuth/tilt combination in the pca
    d = df[['installed_capacity','pca_reg', 'azimuth', 'tilt']].groupby(['pca_reg', 'azimuth', 'tilt']).sum()    
    d = d.groupby(level=0).apply(lambda x: x/float(x.sum())).reset_index()
    
    # Join the resource to get the capacity factor by time slice, azimuth, tilt & pca
    d = pd.merge(d, resource, how = 'left', on = ['pca_reg','azimuth','tilt'])
    
    # Pivot to tall format
    d = d.set_index(['pca_reg','azimuth','tilt','installed_capacity']).stack().reset_index()
    d.columns = ['pca_reg','azimuth','tilt','installed_capacity','ts','cf']
    
    # Finally, calculate weighted mean CF by timeslice using number_of_adopters as the weight 
    d['cf'] = d['cf'] * d['installed_capacity']
    d = d.groupby(['pca_reg','ts']).sum().reset_index()
    d.drop(['tilt','installed_capacity'], axis = 1, inplace = True)
    
    return d

def excess_generation_percent(row, con, gen):
    ''' Function to calculate percent of excess generation given 8760-lists of 
    consumption and generation. Currently function is configured to work only with
    the rate_input_df to avoid pulling generation and consumption profiles
    '''

    annual_generation = sum(row[gen])
    
    if annual_generation == 0:
        row['excess_generation_percent'] = 0
    else:
        # Determine the annual amount of generation (kWh) that exceeds consumption,
        # and must be sold to the grid to receive value
        annual_excess_gen = sum(np.maximum(row[gen] - row[con],0))
        row['excess_generation_percent'] = annual_excess_gen / annual_generation # unitless (kWh/kWh)

    return row
    
def code_profiler(out_dir):
    lines = [ line for line in open(out_dir + '/dg_model.log') if 'took:' in line]
    
    process = [line.split('took:')[-2] for line in lines]
    process = [line.split(':')[-1] for line in process]
    
    time = [line.split('took:')[-1] for line in lines]
    time = [line.split('s')[0] for line in time]
    time = [float(x) for x in time]
    
    
    profile = pd.DataFrame({'process': process, 'time':time})
    profile = profile.sort('time', ascending = False)
    profile.to_csv(out_dir + '/code_profiler.csv') 
    
