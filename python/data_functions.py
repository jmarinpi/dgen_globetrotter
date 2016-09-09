# -*- coding: utf-8 -*-
"""
Functions for pulling data
Created on Mon Mar 24 08:59:44 2014
@author: mgleason and bsigrin
"""
import psycopg2 as pg
import time   
import numpy as np
import pandas as pd
import datetime
from multiprocessing import Process, JoinableQueue
from cStringIO import StringIO
import logging
reload(logging)
import gzip
import subprocess
import os
import psutil
import decorators
import utility_functions as utilfunc
import shutil
import pssc_mp

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================


#==============================================================================
# configure psycopg2 to treat numeric values as floats (improves performance of pulling data from the database)
DEC2FLOAT = pg.extensions.new_type(
    pg.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
pg.extensions.register_type(DEC2FLOAT)
#==============================================================================


def create_tech_subfolders(out_scen_path, techs, out_subfolders, choose_tech):
    
    for tech in techs:
        # set output subfolders  
        out_tech_path = os.path.join(out_scen_path, tech)
        os.makedirs(out_tech_path)
        out_subfolders[tech].append(out_tech_path)
    
    if choose_tech == True:
        out_tech_choice_path = os.path.join(out_scen_path, 'tech_choice')
        os.makedirs(out_tech_choice_path)
    
    return out_subfolders

def create_scenario_results_folder(input_scenario, scen_name, scenario_names, out_dir, dup_n = 0):
    
    if scen_name in scenario_names:
        logger.warning("Warning: Scenario name %s is a duplicate. Renaming to %s_%s" % (scen_name, scen_name, dup_n))
        scen_name = "%s_%s" % (scen_name, dup_n)
        dup_n += 1
    scenario_names.append(scen_name)
    out_scen_path = os.path.join(out_dir, scen_name)
    os.makedirs(out_scen_path)
    # copy the input scenario spreadsheet
    if input_scenario is not None:
        shutil.copy(input_scenario, out_scen_path)
    
    return out_scen_path, scenario_names, dup_n


@decorators.fn_timer(logger = logger, tab_level = 1, prefix = '')
def create_output_schema(pg_conn_string, source_schema = 'diffusion_template', include_data = False):
    
    inputs = locals().copy()
    
    logger.info('Creating output schema based on %(source_schema)s' % inputs)
    
    con, cur = utilfunc.make_con(pg_conn_string, role = "diffusion-schema-writers")

    # check that the source schema exists
    sql = """SELECT count(*)
            FROM pg_catalog.pg_namespace
            WHERE nspname = '%(source_schema)s';""" % inputs
    check = pd.read_sql(sql, con)
    if check['count'][0] <> 1:
        msg = "Specified source_schema (%(source_schema)s) does not exist." % inputs
        raise ValueError(msg)

    cdt = utilfunc.current_datetime()
    dest_schema = 'diffusion_results_%s' % cdt
    inputs['dest_schema'] = dest_schema
    
    sql = '''SELECT diffusion_shared.clone_schema('%(source_schema)s', '%(dest_schema)s', 'diffusion-writers', %(include_data)s);''' % inputs
    cur.execute(sql)        
    con.commit()

    logger.info('\tOutput schema is: %s' % dest_schema)
   
    return dest_schema

@decorators.fn_timer(logger = logger, tab_level = 1, prefix = '')
def drop_output_schema(pg_conn_string, schema, delete_output_schema):
    
    inputs = locals().copy()    

    if delete_output_schema == True:
        logger.info('Dropping the Output Schema (%s) from Database' % schema)
    
        con, cur = utilfunc.make_con(pg_conn_string, role = "diffusion-schema-writers")
        sql = '''DROP SCHEMA IF EXISTS %(schema)s CASCADE;''' % inputs
        cur.execute(sql)
        con.commit()
    else:
        logger.warning("The output schema  (%(schema)s) has not been deleted. Please delete manually when you are finished analyzing outputs." % inputs)
    

    
def clear_outputs(con, cur, schema):
    """Delete all rows from the res, com, and ind output tables"""
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()      
    
    sql = """DELETE FROM %(schema)s.outputs_res;
            DELETE FROM %(schema)s.outputs_com;
            DELETE FROM %(schema)s.outputs_ind;
            DELETE FROM %(schema)s.cumulative_installed_capacity_solar;            
            DELETE FROM %(schema)s.cumulative_installed_capacity_wind;            
            DELETE FROM %(schema)s.yearly_technology_costs_solar;
            DELETE FROM %(schema)s.yearly_technology_costs_wind;
            """ % inputs
    cur.execute(sql)
    con.commit()


def write_outputs(con, cur, outputs_df, sectors, schema):
    
    inputs = locals().copy()    
    
    # set fields to write
    fields = [  'selected_option',
                'tech',
                'pgid',
                'county_id',
                'bin_id',
                'year',
                'sector_abbr',
                'sector',
                'state_abbr',
                'census_division_abbr',
                'pca_reg',
                'reeds_reg',
                'customers_in_bin',
                'load_kwh_per_customer_in_bin',
                'load_kwh_in_bin',
                'max_demand_kw',
                'hdf_load_index',
                'owner_occupancy_status',
                'pct_of_bldgs_developable',
                'developable_customers_in_bin',
                'developable_load_kwh_in_bin',
                'solar_re_9809_gid',
                'tilt',
                'azimuth',
                'developable_roof_sqft',
                'inverter_lifetime_yrs',
                'ann_system_degradation',
                'i',
                'j',
                'cf_bin',
                'power_curve_1',
                'power_curve_2',
                'power_curve_interp_factor',
                'wind_derate_factor',
                'turbine_height_m',
                'turbine_size_kw',
                'aep',
                'naep',
                'cf',
                'system_size_kw',
                'system_size_factors',
                'n_units',
                'total_gen_twh',
                'rate_id_alias',
                'rate_source',
                'nem_system_size_limit_kw',
                'ur_nm_yearend_sell_rate',
                'ur_flat_sell_rate',
                'flat_rate_excess_gen_kwh',
                'ur_enable_net_metering',
                'full_net_metering',
                'excess_generation_percent',
                'first_year_bill_with_system',
                'first_year_bill_without_system',
                'net_fit_credit_dollars',
                'monthly_bill_savings',
                'percent_monthly_bill_savings',
                'cost_of_elec_dols_per_kwh',
                'cap_cost_multiplier',
                'inverter_cost_dollars_per_kw',
                'installed_costs_dollars_per_kw',
                'fixed_om_dollars_per_kw_per_yr',
                'variable_om_dollars_per_kwh',
                'carbon_price_cents_per_kwh',
                'curtailment_rate',
                'reeds_elec_price_mult',
                'business_model',
                'leasing_allowed',
                'loan_term_yrs',
                'loan_rate',
                'down_payment',
                'discount_rate',
                'tax_rate',
                'length_of_irr_analysis_yrs',
                'value_of_increment',
                'value_of_pbi_fit',
                'value_of_ptc',
                'pbi_fit_length',
                'ptc_length',
                'value_of_rebate',
                'value_of_tax_credit_or_deduction',
                'value_of_itc',
                'total_value_of_incentives',
                'lcoe',
                'npv4',
                'npv_agent',
                'metric',
                'metric_value',
                'max_market_share',
                'initial_number_of_adopters',
                'initial_capacity_mw',
                'initial_market_share',
                'initial_market_value',
                'number_of_adopters_last_year',
                'installed_capacity_last_year',
                'market_share_last_year',
                'market_value_last_year',
                'new_adopters',
                'new_capacity',
                'new_market_share',
                'new_market_value',                
                'number_of_adopters',
                'installed_capacity',
                'market_share',
                'market_value'
            ]    

    # convert formatting of fields list
    inputs['fields_str'] = utilfunc.pylist_2_pglist(fields).replace("'","")       
    # open an in memory stringIO file (like an in memory csv)
    s = StringIO()
    # write the data to the stringIO
    outputs_df.loc[:, fields].to_csv(s, index = False, header = False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    sql = 'COPY %(schema)s.agent_outputs (%(fields_str)s) FROM STDOUT WITH CSV' % inputs
    cur.copy_expert(sql, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()    
    s.close()


@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def index_output_table(con, cur, schema):
    
    inputs = locals().copy()
    
    # create indices that will be needed for various aggregations in R visualization script
    sql = '''CREATE INDEX agent_outputs_year_btree ON %(schema)s.agent_outputs USING BTREE(year);
             CREATE INDEX agent_outputs_state_abbr_btree ON %(schema)s.agent_outputs USING BTREE(state_abbr);
             CREATE INDEX agent_outputs_sector_btree ON %(schema)s.agent_outputs USING BTREE(sector);
             CREATE INDEX agent_outputs_business_model_btree ON %(schema)s.agent_outputs USING BTREE(business_model);
             CREATE INDEX agent_outputs_system_size_factors_btree ON %(schema)s.agent_outputs USING BTREE(system_size_factors);                          
             CREATE INDEX agent_outputs_metric_btree ON %(schema)s.agent_outputs USING BTREE(metric);             
             CREATE INDEX agent_outputs_turbine_height_m_btree ON %(schema)s.agent_outputs USING BTREE(turbine_height_m);
             CREATE INDEX agent_outputs_tech_btree ON %(schema)s.agent_outputs USING BTREE(tech);
             ''' % inputs
    cur.execute(sql)
    con.commit()


@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def copy_outputs_to_csv(techs, schema, out_scen_path, cur, con, file_suffix = ''):
    
    logger.info('\tExporting Results from Database')

    inputs = locals().copy()

    # copy data to csv
    for tech in techs:
        inputs['tech'] = tech
        out_file = os.path.join(out_scen_path, tech, 'outputs_%s%s.csv.gz' % (tech, file_suffix))
        f = gzip.open(out_file,'w',1)
        sql = """COPY 
                    (
                        SELECT * 
                        FROM %(schema)s.agent_outputs 
                        WHERE tech = '%(tech)s'
                    ) 
                TO STDOUT WITH CSV HEADER;""" % inputs
        cur.copy_expert(sql, f)
        f.close()
    
    # write the scenario optoins to csv as well
    f2 = open(os.path.join(out_scen_path, 'scenario_options_summary.csv'),'w')
    cur.copy_expert('COPY %s.input_main_scenario_options TO STDOUT WITH CSV HEADER;' % schema, f2)
    f2.close()
    
    
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def create_scenario_report(techs, schema, scen_name, out_scen_path, cur, con, Rscriblock_path, pg_params_file, file_suffix = ''):
    
    if len(techs) > 1:
        logger.info('\tCompiling Output Reports')
    else:
        logger.info('\tCompiling Output Report')
    
    # path to the plot_outputs R script        
    plot_outputs_path = '%s/r/graphics/plot_outputs.R' % os.path.dirname(os.getcwd())        
    
    
    for tech in techs:
        out_tech_path = os.path.join(out_scen_path, tech)
        #command = ("%s --vanilla ../r/graphics/plot_outputs.R %s" %(Rscriblock_path, runpath))
        # for linux and mac, this needs to be formatted as a list of args passed to subprocess
        command = [Rscriblock_path,'--vanilla', plot_outputs_path, out_tech_path, scen_name, tech, schema, pg_params_file, file_suffix]
        proc = subprocess.Popen(command,stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        messages = proc.communicate()
        if 'error' in messages[1].lower():
            logger.error(messages[1])
        if 'warning' in messages[1].lower():
            logger.warning(messages[1])

@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def create_tech_choice_report(choose_tech, schema, scen_name, out_scen_path, cur, con, Rscriblock_path, pg_params_file, file_suffix = ''):
    
    if choose_tech == True:
        logger.info('\tCompiling Technology Choice Report')        
    
        # path to the plot_outputs R script        
        plot_outputs_path = '%s/r/graphics/tech_choice_report.R' % os.path.dirname(os.getcwd())        
            
        out_path = os.path.join(out_scen_path, 'tech_choice')
        #command = ("%s --vanilla ../r/graphics/plot_outputs.R %s" %(Rscriblock_path, runpath))
        # for linux and mac, this needs to be formatted as a list of args passed to subprocess
        command = [Rscriblock_path,'--vanilla', plot_outputs_path, out_path, scen_name, schema, pg_params_file, file_suffix]
        proc = subprocess.Popen(command,stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        messages = proc.communicate()
        if 'error' in messages[1].lower():
            logger.error(messages[1])
        if 'warning' in messages[1].lower():
            logger.warning(messages[1])
    else:
        logger.info("\tSkipping Creation of Technology Choice Report (Not Applicable)")

@decorators.fn_timer(logger = logger, tab_level = 1, prefix = '')
def create_deployment_summary_table(cur, con, schema):
    
    inputs = locals().copy()
    
    logger.info("Creating Deployment Summary Table")
    
    sql = """DROP TABLE IF EXISTS %(schema)s.deployment_summary;
             CREATE TABLE %(schema)s.deployment_summary
             (
                tech text,
                year integer,
                sector text,
                installed_capacity_gw numeric,
                number_of_adopters numeric,
                p numeric,
                q numeric,
                teq_yr1 numeric
             );""" % inputs
    cur.execute(sql)
    con.commit()

def create_economics_results_table(cur, con, schema):
    
    inputs = locals().copy()    
    
    #==============================================================================
    #     CREATE TABLE TO HOLD RESULTS
    #==============================================================================
    sql = """DROP TABLE IF EXISTS %(schema)s.economic_results;
             CREATE UNLOGGED TABLE %(schema)s.economic_results
             (
                    pgid integer,
                    county_id integer,
                    bin_id integer,
                    year integer,
                    state_abbr text,
                    census_division_abbr text,
                    pca_reg text,
                    reeds_reg integer,
                    carbon_price_cents_per_kwh numeric,
                    fixed_om_dollars_per_kw_per_yr numeric,
                    variable_om_dollars_per_kwh numeric,
                    installed_costs_dollars_per_kw numeric,
                    customers_in_bin numeric,
                    load_kwh_per_customer_in_bin integer,
                    system_size_kw numeric,
                    aep numeric,
                    owner_occupancy_status numeric,
                    tilt integer,
                    azimuth text,
                    available_roof_sqft integer,
                    inverter_cost_dollars_per_kw numeric,
                    inverter_lifetime_yrs integer,
                    tech text,
                    first_year_bill_with_system numeric,
                    first_year_bill_without_system numeric,
                    excess_generation_percent numeric,
                    leasing_allowed boolean,
                    sector_abbr text,
                    curtailment_rate integer,
                    ReEDS_elec_price_mult integer,
                    business_model text,
                    metric text,
                    loan_term_yrs integer,
                    loan_rate numeric,
                    down_payment numeric,
                    discount_rate numeric,
                    tax_rate numeric,
                    length_of_irr_analysis_yrs integer,
                    ann_system_degradation numeric,
                    deprec text,
                    overwrite_exist_inc boolean,
                    incentive_start_year integer,
                    rate_escalations text,
                    value_of_increment numeric,
                    value_of_pbi_fit numeric,
                    value_of_ptc numeric,
                    pbi_fit_length integer,
                    ptc_length integer,
                    value_of_rebate numeric,
                    value_of_tax_credit_or_deduction numeric,
                    value_of_itc numeric,
                    ic numeric,
                    total_value_of_incentive numeric,
                    first_year_energy_savings numeric,
                    monthly_bill_savings numeric,
                    percent_monthly_bill_savings numeric,
                    total_value_of_incentives numeric,
                    metric_value_precise numeric,
                    lcoe numeric,
                    npv4 numeric,
                    metric_value_bounded numeric,
                    metric_value_as_factor integer,
                    metric_value numeric,
                    sector text,
                    max_market_share numeric,
                    source text,
                    selected_option boolean
             );
             """ % inputs
    cur.execute(sql)
    con.commit()

@decorators.fn_timer(logger = logger, tab_level = 3, prefix = '')
def write_economics_df_to_csv(cur, con, schema, df):
    
    inputs = locals().copy()
    
    logger.info("\t\tWriting economics results to database")
    
    # open an in memory stringIO file (like an in memory csv)
    s = StringIO()
    # write the data to the stringIO
    df.to_csv(s, index = False, header = False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    cur.copy_expert('COPY %(schema)s.economic_results FROM STDOUT WITH CSV' % inputs, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()    
    s.close()


@decorators.fn_timer(logger = logger, tab_level = 3, prefix = '')
def get_economics_df(con, schema, year):

    inputs = locals().copy()
    
    logger.info("\t\tLoading economics results from database")

    sql = """SELECT * FROM 
            %(schema)s.economic_results
            WHERE year = %(year)s;""" % inputs

    df = pd.read_sql(sql, con)
    
    return df

@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def summarize_deployment(cur, con, schema, p, q, teq_yr1):
    
    inputs = locals().copy()
    
    logger.info("\tSummarizing Deployment from p/teq_yr1 combination")    
    
    sql = """INSERT INTO %(schema)s.deployment_summary
             SELECT tech, year, sector, 
                SUM(installed_capacity)/1e6 as installed_capacity_gw, 
                SUM(number_of_adopters) as number_of_adopters,
                %(p)s::NUMERIC as p,
                %(q)s::NUMERIC as q,
                %(teq_yr1)s::NUMERIC as teq_yr1
            FROM %(schema)s.outputs_all
            GROUP BY tech, year, sector;""" % inputs
    cur.execute(sql)
    con.commit()


@decorators.fn_timer(logger = logger, tab_level = 1, prefix = '')
def copy_deployment_summary_to_csv(schema, out_scen_path, cur, con):
    
    logger.info('Exporting Deployment Summary from Database')

    # copy data to csv
    out_file = os.path.join(out_scen_path, 'deployment_summary.csv.gz')
    f = gzip.open(out_file,'w',1)
    cur.copy_expert('COPY %s.deployment_summary TO STDOUT WITH CSV HEADER;' % schema, f)
    f.close()



def check_tech_potential_limits(cur, con, schema, techs, sectors, out_dir):
    
    inputs = locals().copy()    
    
    logger.info('Checking Agent Tech Potential Against State Tech Potential Limits')    
    
    for tech in techs:
        inputs['tech'] = tech
        if tech == 'wind':            
            # summarize the tech potential for all agents (by state) in terms of:
                # capacity, generation, and systems count
            sql_list = []
            for sector_abbr, sector in sectors.iteritems():
                inputs['sector_abbr'] = sector_abbr
                sql = """SELECT state_abbr,
                                sum(aep * initial_customers_in_bin)/1e6 as gen_gwh,
                                sum(initial_customers_in_bin) as systems_count,
                                sum(system_size_kw * initial_customers_in_bin)/1e6 as cap_gw
                       FROM %(schema)s.block_%(sector_abbr)s_best_option_each_year_wind
                       WHERE year = 2014
                       AND system_size_kw <> 0
                       GROUP BY state_abbr""" % inputs
                sql_list.append(sql)
            inputs['sql_all'] = ' UNION ALL '.join(sql_list)
            sql = """DROP TABLE IF EXISTS %(schema)s.agent_tech_potential_by_state_wind;
                     CREATE UNLOGGED TABLE %(schema)s.agent_tech_potential_by_state_wind AS
                     WITH a as
                     (%(sql_all)s)
                     SELECT state_abbr, 
                            sum(gen_gwh) as gen_gwh,
                            sum(systems_count) as systems_count,
                            sum(cap_gw) as cap_gw
                     FROM a
                     GROUP BY state_abbr;""" % inputs
            cur.execute(sql)
            con.commit()            
            
            # compare to known tech potential limits
            sql = """DROP TABLE IF EXISTS %(schema)s.tech_potential_ratios_wind;
                     CREATE TABLE %(schema)s.tech_potential_ratios_wind AS
                    SELECT a.state_abbr,
                            a.cap_gw/b.cap_gw as pct_of_tech_potential_capacity,
                            a.gen_gwh/b.gen_gwh as pct_of_tech_potential_generation,
                            a.systems_count/b.systems_count as pct_of_tech_potential_systems_count
                     FROM %(schema)s.agent_tech_potential_by_state_wind a
                     LEFT JOIN diffusion_wind.tech_potential_by_state  b
                         ON a.state_abbr = b.state_abbr""" % inputs
            cur.execute(sql)
            con.commit()
                         
            # find overages
            sql = """SELECT *
                     FROM %(schema)s.tech_potential_ratios_wind
                         WHERE pct_of_tech_potential_capacity > 1
                               OR pct_of_tech_potential_generation > 1
                               OR pct_of_tech_potential_systems_count > 1;""" % inputs
            overage = pd.read_sql(sql, con)
            
            # report overages, if any
            if overage.shape[0] > 0:
                inputs['out_overage_csv'] = os.path.join(out_dir, 'tech_potential_overages_wind.csv')
                logger.warning('\tModel WIND tech potential exceeds actual %(tech)s tech potential for some states. See: %(out_overage_csv)s for details.' % inputs)                
                overage.to_csv(inputs['out_overage_csv'], index = False, header = True)
            else:
                inputs['out_ratios_csv'] = os.path.join(out_dir, 'tech_potential_ratios_wind.csv')
                logger.info('\tModel WIND tech potential is within state %(tech)s tech potential limits. See: %(out_ratios_csv)s for details.' % inputs)
                sql = """SELECT *
                     FROM %(schema)s.tech_potential_ratios_wind""" % inputs
                ratios = pd.read_sql(sql, con)
                ratios.to_csv(inputs['out_ratios_csv'], index = False, header = True)
            
            
        elif tech == 'solar':
            
            # summarize the tech potential for all agents (by state and bldg size class) in terms of:
                # capacity, generation, and roof area
            sql_list = []
            for sector_abbr, sector in sectors.iteritems():
                inputs['sector_abbr'] = sector_abbr
                sql = """SELECT state_abbr, bldg_size_class, 
                                sum(aep * initial_customers_in_bin)/1e6 as gen_gwh,
                                sum(available_roof_sqft * initial_customers_in_bin)/10.7639 as area_m2,
                                sum(system_size_kw * initial_customers_in_bin)/1e6 as cap_gw
                       FROM %(schema)s.block_%(sector_abbr)s_best_option_each_year_solar
                       WHERE year = 2014
                       GROUP BY state_abbr, bldg_size_class""" % inputs
                sql_list.append(sql)
            inputs['sql_all'] = ' UNION ALL '.join(sql_list)
            sql = """DROP TABLE IF EXISTS %(schema)s.agent_tech_potential_by_state_solar;
                     CREATE UNLOGGED TABLE %(schema)s.agent_tech_potential_by_state_solar AS
                     WITH a as
                     (%(sql_all)s)
                     SELECT state_abbr, bldg_size_class, 
                            sum(gen_gwh) as gen_gwh,
                            sum(area_m2) as area_m2,
                            sum(cap_gw) as cap_gw
                     FROM a
                     GROUP BY state_abbr, bldg_size_class;""" % inputs
            cur.execute(sql)
            con.commit()
            
            # compare to known tech potential limits
            sql = """DROP TABLE IF EXISTS %(schema)s.tech_potential_ratios_solar;
                     CREATE TABLE %(schema)s.tech_potential_ratios_solar AS
                    SELECT a.state_abbr, a.bldg_size_class,
                            a.cap_gw/b.cap_gw as pct_of_tech_potential_capacity,
                            a.gen_gwh/b.gen_gwh as pct_of_tech_potential_generation,
                            a.area_m2/b.area_m2 as pct_of_tech_potential_area
                     FROM %(schema)s.agent_tech_potential_by_state_solar a
                     LEFT JOIN diffusion_solar.rooftop_tech_potential_limits_by_state  b
                         ON a.state_abbr = b.state_abbr
                         AND a.bldg_size_class = b.size_class""" % inputs
            cur.execute(sql)
            con.commit()
                         
            # find overages
            sql = """SELECT *
                     FROM %(schema)s.tech_potential_ratios_solar
                         WHERE pct_of_tech_potential_capacity > 1
                               OR pct_of_tech_potential_generation > 1
                               OR pct_of_tech_potential_area > 1;""" % inputs
            overage = pd.read_sql(sql, con)
            
            # report overages, if any
            if overage.shape[0] > 0:
                inputs['out_overage_csv'] = os.path.join(out_dir, 'tech_potential_overages_solar.csv')
                logger.warning('\tModel SOLAR tech potential exceeds actual %(tech)s tech potential for some states. See: %(out_overage_csv)s for details.' % inputs)                
                overage.to_csv(inputs['out_overage_csv'], index = False, header = True)
            else:
                inputs['out_ratios_csv'] = os.path.join(out_dir, 'tech_potential_ratios_solar.csv')
                logger.info('\tModel SOLAR tech potential is within state %(tech)s tech potential limits. See: %(out_ratios_csv)s for details.' % inputs)
                sql = """SELECT *
                     FROM %(schema)s.tech_potential_ratios_solar""" % inputs
                ratios = pd.read_sql(sql, con)
                ratios.to_csv(inputs['out_ratios_csv'], index = False, header = True)
                

#%%

def get_sectors(cur, schema):
    '''Return the sectors to model from table view in postgres.
        Returned as a dictionary.
        '''    
    
    sql = '''SELECT sectors 
              FROM %s.sectors_to_model;''' % schema
    cur.execute(sql)
    sectors = cur.fetchone()['sectors']
    return sectors


def get_technologies(con, schema):
    
    
    sql = '''with a as
            (
                	select unnest(array['wind', 'solar', 'du', 'ghp']) as tech, 
                         unnest(array[run_wind, run_solar, run_du, run_ghp]) as enabled
                  FROM %s.input_main_scenario_options
            )
            SELECT tech
            FROM a
            WHERE enabled = TRUE;''' % schema
    
    # get the data
    df = pd.read_sql(sql, con)
    # convert to a simple list    
    techs = df.tech.tolist()
    
    if len(techs) == 0:
        raise ValueError("No technologies were selected to be run in the input sheet.")  
    
    return techs
    
    
def get_scenario_options(cur, schema):
    ''' Pull scenario options from dB
    
    '''
    sql = '''SELECT * 
             FROM %s.input_main_scenario_options;''' % schema
    cur.execute(sql)
    results = cur.fetchall()[0]
    return results


def cleanup_incentives(df, dsire_opts):
    
    # add in columns that may be missing
    for col in ['increment_4_capacity_kw','increment_4_rebate_dlrs_kw',
                'pbi_fit_max_size_for_dlrs_calc_kw','tax_credit_dlrs_kw',
                'pbi_fit_min_output_kwh_yr','increment_3_rebate_dlrs_kw',
                'increment_4_rebate_dlrs_kw']:
        if col not in df.columns:
            df[col] = np.nan
    
    # isolate the output columns
    out_cols = df.columns
    
    # merge the dsire options dataframe
    df = pd.merge(df, dsire_opts, how = 'left', on = ['tech'])    
    
    # fix data types for float columns (may come in as type 'O' due to all nulls)
    float_cols = ['increment_1_capacity_kw', 
                    'increment_2_capacity_kw', 
                    'increment_3_capacity_kw', 
                    'increment_4_capacity_kw', 
                    'increment_1_rebate_dlrs_kw', 
                    'increment_2_rebate_dlrs_kw', 
                    'increment_3_rebate_dlrs_kw', 
                    'increment_4_rebate_dlrs_kw', 
                    'pbi_fit_duration_years', 
                    'pbi_fit_max_size_kw', 
                    'pbi_fit_min_output_kwh_yr', 
                    'pbi_fit_min_size_kw', 
                    'pbi_dlrs_kwh', 
                    'fit_dlrs_kwh', 
                    'pbi_fit_max_dlrs', 
                    'pbi_fit_pcnt_cost_max', 
                    'ptc_duration_years', 
                    'ptc_dlrs_kwh', 
                    'max_dlrs_yr', 
                    'rebate_dlrs_kw', 
                    'rebate_max_dlrs', 
                    'rebate_max_size_kw', 
                    'rebate_min_size_kw', 
                    'rebate_pcnt_cost_max', 
                    'max_tax_credit_dlrs', 
                    'max_tax_deduction_dlrs', 
                    'tax_credit_pcnt_cost', 
                    'tax_deduction_pcnt_cost', 
                    'tax_credit_max_size_kw', 
                    'tax_credit_min_size_kw']
    
    df.loc[:, float_cols] = df[float_cols].astype(float)


    # replace null values with defaults
    max_dlrs = 1e9
    dlrs_per_kwh = 0
    dlrs_per_kw = 0
    max_size_kw = 10000
    min_size_kw = 0
    min_output_kwh_yr = 0
    increment_incentive_kw = 0
    pcnt_cost_max = 100
    # percent cost max
    df.loc[:, 'rebate_pcnt_cost_max'] = df.rebate_pcnt_cost_max.fillna(pcnt_cost_max)
    # expiration date
    df.loc[:, 'ptc_end_date'] = df.ptc_end_date.astype('O').fillna(df['dsire_default_exp_date'])
    df.loc[:, 'pbi_fit_end_date'] = df.pbi_fit_end_date.astype('O').fillna(df['dsire_default_exp_date']) # Assign expiry if no date    
    # max dollars
    df.loc[:, 'max_dlrs_yr'] = df.max_dlrs_yr.fillna(max_dlrs)
    df.loc[:, 'pbi_fit_max_dlrs'] = df.pbi_fit_max_dlrs.fillna(max_dlrs)
    df.loc[:, 'max_tax_credit_dlrs'] = df.max_tax_credit_dlrs.fillna(max_dlrs)
    df.loc[:, 'rebate_max_dlrs'] = df.rebate_max_dlrs.fillna(max_dlrs)     
    # dollars per kwh
    df.loc[:, 'ptc_dlrs_kwh'] = df.ptc_dlrs_kwh.fillna(dlrs_per_kwh)
    # dollars per kw
    df.loc[:, 'rebate_dlrs_kw'] = df.rebate_dlrs_kw.fillna(dlrs_per_kw)
    # max size
    df.loc[:, 'tax_credit_max_size_kw'] = df.tax_credit_max_size_kw.fillna(max_size_kw)
    df.loc[:, 'pbi_fit_max_size_kw' ] = df.pbi_fit_min_size_kw.fillna(max_size_kw)
    df.loc[:, 'rebate_max_size_kw'] = df.rebate_min_size_kw.fillna(max_size_kw)
    # min size
    df.loc[:, 'pbi_fit_min_size_kw' ] = df.pbi_fit_min_size_kw.fillna(min_size_kw)
    df.loc[:, 'rebate_min_size_kw'] = df.rebate_min_size_kw.fillna(min_size_kw)
    # minimum output kwh
    df.loc[:, 'pbi_fit_min_output_kwh_yr'] = df['pbi_fit_min_output_kwh_yr'].fillna(min_output_kwh_yr)    
    # increment incentives
    increment_vars = ['increment_1_capacity_kw','increment_2_capacity_kw','increment_3_capacity_kw','increment_4_capacity_kw', 'increment_1_rebate_dlrs_kw','increment_2_rebate_dlrs_kw','increment_3_rebate_dlrs_kw','increment_4_rebate_dlrs_kw']    
    df.loc[:, increment_vars] = df[increment_vars].fillna(increment_incentive_kw)
    
    return df[out_cols]


def get_dsire_settings(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT *
             FROM %(schema)s.input_main_dsire_incentive_options;""" % inputs
    
    dsire_opts = pd.read_sql(sql, con, coerce_float = True)
    
    return dsire_opts
    

def get_incentives_cap(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT *
             FROM %(schema)s.input_main_incentives_cap;""" % inputs
    
    incentives_cap = pd.read_sql(sql, con, coerce_float = True)
    
    return incentives_cap    



def get_bass_params(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT state_abbr, 
                    p, 
                    q, 
                    teq_yr1, 
                    sector_abbr, 
                    tech
             FROM %(schema)s.input_solar_bass_params

             UNION ALL

             SELECT state_abbr, 
                    p, 
                    q, 
                    teq_yr1, 
                    sector_abbr, 
                    tech
             FROM %(schema)s.input_wind_bass_params

             UNION ALL

             SELECT state_abbr, 
                    p, 
                    q, 
                    teq_yr1, 
                    sector_abbr, 
                    tech
             FROM %(schema)s.input_ghp_bass_params;""" % inputs
    
    bass_df = pd.read_sql(sql, con, coerce_float = True)
    
    return bass_df

def get_itc_incentives(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT year, substring(lower(sector), 1, 3) as sector_abbr, 
                    itc_fraction, tech, min_size_kw, max_size_kw
             FROM %(schema)s.input_main_itc_options;""" % inputs
    itc_options = pd.read_sql(sql, con) 
    
    return itc_options

def get_dsire_incentives(cur, con, schema, techs, sectors, pg_conn_string, dsire_opts):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()
    
    if 'solar' in techs:
        sql = """SELECT c.*, 'solar'::TEXT as tech
                    FROM diffusion_solar.incentives c;"""
    else:
        sql = """SELECT c.*, 'solar'::TEXT as tech
                    FROM diffusion_solar.incentives c
                    LIMIT 0;"""
                    
    # get the data
    df = pd.read_sql(sql, con, coerce_float = True)
    # clean it up
    df = cleanup_incentives(df, dsire_opts)

    return df



def get_state_dsire_incentives(cur, con, schema, techs, dsire_opts):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()
    
    sql_list = []
    for tech in techs:
        inputs['tech'] = tech
        sql =   """SELECT *
                   FROM diffusion_%(tech)s.state_dsire_incentives
                """ % inputs
        sql_list.append(sql)
    
    sql = ' UNION ALL '.join(sql_list)
    # get the data
    df = pd.read_sql(sql, con, coerce_float = True)
    
    # isolate the output columns
    out_cols = df.columns
    
    # fill in expiration dates with default from input sheet if missing
    # merge dsire opts
    df = pd.merge(df, dsire_opts, how = 'left', on = ['tech'])  
    # fill in missing values
    df.loc[:, 'exp_date'] = df.exp_date.astype('O').fillna(df['dsire_default_exp_date'])
    
    # convert exp_date to datetime
    df['exp_date'] = pd.to_datetime(df['exp_date'])

    return df[out_cols]  


def calc_state_dsire_incentives(df, state_dsire_df, year):
    
    # convert current year into a datetime object (assume current date is the first day of the 2 year period ending in YEAR)
    df['cur_date'] = pd.to_datetime((df['year']-2).apply(str))    
    
    # calculate installed costs
    df['ic'] = df['installed_costs_dollars_per_kw'] * df['system_size_kw']    

    # join data frames
    inc = pd.merge(df, state_dsire_df, how = 'left', on = ['state_abbr', 'sector_abbr', 'tech'])    

    # drop rows that don't fit within the correct ranges for system size
    inc = inc[(inc['system_size_kw'] >= inc['min_size_kw']) & (inc['system_size_kw'] < inc['max_size_kw'])]
    # drop rows that don't fit within correct aep range
    inc = inc[(inc['aep'] >= inc['min_aep_kwh']) & (inc['aep'] < inc['max_aep_kwh'])]
    # drop rows that don't fit within the correct date
    inc = inc[inc['cur_date'] <= inc['exp_date']]

    
    # calculate ITC
    inc['value_of_itc'] = 0.0
    inc.loc[inc['incentive_type'] == 'ITC', 
                'value_of_itc'] = np.minimum(
                                            inc['val_pct_cost'] * inc['ic'] * 
                                            (inc['system_size_kw'] >= inc['min_size_kw']) * 
                                            (inc['system_size_kw'] < inc['max_size_kw']) * 
                                            (inc['cur_date'] <= inc['exp_date']), 
                                        inc['cap_dlrs']
                                    )
                                    
    # calculate PTC
    inc['value_of_ptc'] = 0.0
    inc.loc[inc['incentive_type'] == 'PTC', 
                'value_of_ptc'] = np.minimum(
                                            inc['dlrs_per_kwh'] * inc['aep'] *
                                            (inc['system_size_kw'] >= inc['min_size_kw']) * 
                                            (inc['system_size_kw'] < inc['max_size_kw']) *
                                            (inc['aep'] >= inc['min_aep_kwh']) * 
                                            (inc['aep'] < inc['max_aep_kwh']) *
                                            (inc['cur_date'] <= inc['exp_date']),
                                        np.minimum(
                                                inc['cap_dlrs'], 
                                                inc['cap_pct_cost'] * inc['ic']
                                            )
                                    )
    inc['ptc_length'] = 0.0
    inc.loc[inc['incentive_type'] == 'PTC', 
                'ptc_length'] = inc['duration_years']


    # calculate capacity based rebates
    inc['value_of_cap_rebate'] = 0.0
    inc.loc[inc['incentive_type'] == 'capacity_based_rebate', 
            'value_of_cap_rebate'] = np.minimum(
                                                (inc['dlrs_per_kw'] * (inc['system_size_kw']-inc['fixed_kw']) + inc['fixed_dlrs']) *
                                                (inc['system_size_kw'] >= inc['min_size_kw']) * 
                                                (inc['system_size_kw'] < inc['max_size_kw']) *                                             
                                                (inc['cur_date'] <= inc['exp_date']),
                                            np.minimum(
                                                inc['cap_dlrs'], 
                                                inc['cap_pct_cost'] * inc['ic']
                                            )
                                    )
    # calculate production based rebates
    inc['value_of_prod_rebate'] = 0.0
    inc.loc[inc['incentive_type'] == 'production_based_rebate', 
            'value_of_prod_rebate'] = np.minimum(
                                                (inc['dlrs_per_kwh'] * (inc['aep']-inc['fixed_kwh']) + inc['fixed_dlrs']) *
                                                (inc['system_size_kw'] >= inc['min_size_kw']) * 
                                                (inc['system_size_kw'] < inc['max_size_kw']) *
                                                (inc['aep'] >= inc['min_aep_kwh']) * 
                                                (inc['aep'] < inc['max_aep_kwh']) *
                                                (inc['cur_date'] <= inc['exp_date']),
                                            np.minimum(
                                                inc['cap_dlrs'], 
                                                inc['cap_pct_cost'] * inc['ic']
                                            )
                                    )


    # calculate FIT
    inc['value_of_pbi_fit'] = 0.0
    inc.loc[inc['incentive_type'] == 'PBI', 
            'value_of_pbi_fit'] = np.minimum(
                                            np.minimum(
                                                        inc['dlrs_per_kwh'] * inc['aep'] + inc['fixed_dlrs'],
                                                        inc['cap_dlrs_yr']
                                                        ) * 
                                            (inc['system_size_kw'] >= inc['min_size_kw']) * 
                                            (inc['system_size_kw'] < inc['max_size_kw']) *
                                            (inc['aep'] >= inc['min_aep_kwh']) * 
                                            (inc['aep'] < inc['max_aep_kwh']) *
                                            (inc['cur_date'] <= inc['exp_date']),
                                        inc['cap_pct_cost'] * inc['ic']
                                    )
    inc['pbi_fit_length'] = 0.0
    inc.loc[inc['incentive_type'] == 'PBI', 
                'pbi_fit_length'] = inc['duration_years']
    
    # calculate ITD
    inc['value_of_itd'] = 0.0
    inc.loc[inc['incentive_type'] == 'ITD', 
            'value_of_itd'] = np.minimum(
                                            inc['val_pct_cost'] * inc['ic'] * 
                                            (inc['system_size_kw'] >= inc['min_size_kw']) * 
                                            (inc['system_size_kw'] < inc['max_size_kw']) * 
                                            (inc['cur_date'] <= inc['exp_date']), 
                                        inc['cap_dlrs']
                                    )

    # combine tax credits and deductions
    inc['value_of_tax_credit_or_deduction'] = inc['value_of_itc'] + inc['value_of_itc']  
    # combine cap and prod rebates
    inc['value_of_rebate'] = inc['value_of_cap_rebate'] + inc['value_of_prod_rebate']
    # add "value of increment" for backwards compatbility with old dsire and manual incentives (note: this is already built into rebates)
    inc['value_of_increment'] = 0.0 
    
    # sum results to customer bins
    out_cols = ['tech',
                 'sector_abbr',
                 'county_id',
                 'bin_id',
                 'business_model',
                 'value_of_increment',
                 'value_of_pbi_fit',
                 'value_of_ptc',
                 'pbi_fit_length',
                 'ptc_length',
                 'value_of_rebate',
                 'value_of_tax_credit_or_deduction']
    sum_cols = [ 'value_of_increment',
                 'value_of_pbi_fit',
                 'value_of_ptc',
                 'value_of_rebate',
                 'value_of_tax_credit_or_deduction']
    max_cols = [ 'pbi_fit_length',
                 'ptc_length'] # there should never be multiples of either of these, so taking the max is the correct choice
    group_cols = ['tech', 
                    'sector_abbr', 
                    'county_id',
                    'bin_id',
                    'business_model']
    if inc.shape[0] > 0:
        inc_summed = inc[group_cols + sum_cols].groupby(group_cols).sum().reset_index() 
        inc_max = inc[group_cols + max_cols].groupby(group_cols).max().reset_index() 
        inc_combined = pd.merge(inc_summed, inc_max, how = 'left', on = group_cols)
    else:    
        inc_combined = inc[out_cols]

    return inc_combined[out_cols]



def get_srecs(cur, con, schema, techs, pg_conn_string, dsire_opts):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()
    
    sql_list = []
    for tech in techs:
        inputs['tech'] = tech
        sql =   """SELECT *, '%(tech)s'::TEXT as tech
                    FROM diffusion_%(tech)s.srecs
                """ % inputs
        sql_list.append(sql)
    
    sql = ' UNION ALL '.join(sql_list)
    # get the data
    df = pd.read_sql(sql, con, coerce_float = True)
    # clean it up
    df = cleanup_incentives(df, dsire_opts)
    
    return df
    

def write_last_year(con, cur, market_last_year, schema):
    
    inputs = locals().copy()    
    
    inputs['out_table'] = '%(schema)s.output_market_last_year'  % inputs
    
    sql = """DELETE FROM %(out_table)s;"""  % inputs
    cur.execute(sql)
    con.commit()

    # open an in memory stringIO file (like an in memory csv)
    s = StringIO()
    # write the data to the stringIO
    out_cols = ['county_id', 'bin_id', 'tech', 'sector_abbr',
                'market_share_last_year', 'max_market_share_last_year', 'number_of_adopters_last_year', 'installed_capacity_last_year', 'market_value_last_year', 
                'initial_number_of_adopters', 'initial_capacity_mw', 'initial_market_share', 'initial_market_value'
                ]
    market_last_year[out_cols].to_csv(s, index = False, header = False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    cur.copy_expert('COPY %(out_table)s FROM STDOUT WITH CSV' % inputs, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()    
    s.close()


def write_cumulative_deployment(con, cur, df, schema, techs, year, start_year):
    
    inputs = locals().copy()    
    
    dfs = {}
    if 'wind' in techs:
        wind_df = df[df['tech'] == 'wind'][['year', 'turbine_size_kw', 'installed_capacity']].groupby(['year', 'turbine_size_kw']).sum().reset_index()
        dfs['wind'] = wind_df
    
    if 'solar' in techs:
        solar_df = df[df['tech'] == 'solar'][['year', 'installed_capacity']].groupby(['year']).sum().reset_index()
        dfs['solar'] = solar_df
  
    
    for tech, tech_df in dfs.iteritems():
        inputs['tech'] = tech
        # open an in memory stringIO file (like an in memory csv)
        s = StringIO()
        # write the data to the stringIO
        tech_df.to_csv(s, index = False, header = False)
        # seek back to the beginning of the stringIO file
        s.seek(0)
        # copy the data from the stringio file to the postgres table
        sql = 'COPY %(schema)s.cumulative_installed_capacity_%(tech)s FROM STDOUT WITH CSV' % inputs
        cur.copy_expert(sql, s)
        # commit the additions and close the stringio file (clears memory)
        con.commit()    
        s.close()     
        
    if year == start_year:
        dfs = {}
        if 'wind' in techs:
            wind_df = df[df['tech'] == 'wind'][['year', 'turbine_size_kw', 'installed_capacity_last_year']].groupby(['year', 'turbine_size_kw']).sum().reset_index()
            wind_df['year'] = start_year - 2
            dfs['wind'] = wind_df
        
        if 'solar' in techs:
            solar_df = df[df['tech'] == 'solar'][['year', 'installed_capacity_last_year']].groupby(['year']).sum().reset_index()
            solar_df['year'] = start_year - 2
            dfs['solar'] = solar_df
            
        for tech, tech_df in dfs.iteritems():
            inputs['tech'] = tech
            # open an in memory stringIO file (like an in memory csv)
            s = StringIO()
            # write the data to the stringIO
            tech_df.to_csv(s, index = False, header = False)
            # seek back to the beginning of the stringIO file
            s.seek(0)
            # copy the data from the stringio file to the postgres table
            sql = 'COPY %(schema)s.cumulative_installed_capacity_%(tech)s FROM STDOUT WITH CSV' % inputs
            cur.copy_expert(sql, s)
            # commit the additions and close the stringio file (clears memory)
            con.commit()    
            s.close()     
            

def get_learning_curves_mode(con, schema):
    
    inputs = locals().copy()    
    
    sql = """SELECT 'wind'::TEXT as tech, enabled
            FROM %(schema)s.input_cost_learning_curves_enabled_wind
            
            UNION ALL
            
            SELECT 'solar'::TEXT as tech, enabled
            FROM %(schema)s.input_cost_learning_curves_enabled_solar""" % inputs
            
    learning_curves_mode = pd.read_sql(sql, con)

    return learning_curves_mode

def write_first_year_costs(con, cur, schema, start_year):
    
    inputs = locals().copy()
    
    # solar
    sql = """INSERT INTO %(schema)s.yearly_technology_costs_solar
             SELECT a.year, a.sector_abbr, 
                    a.installed_costs_dollars_per_kw,
                    a.inverter_cost_dollars_per_kw,
                    a.fixed_om_dollars_per_kw_per_yr,
                    a.variable_om_dollars_per_kwh
                FROM %(schema)s.input_solar_cost_projections_to_model a
                WHERE a.year = %(start_year)s;""" % inputs
    cur.execute(sql)
    con.commit()
    
    # wind
    sql = """INSERT INTO %(schema)s.yearly_technology_costs_wind
             SELECT a.year, 
                    a.turbine_size_kw,
                    a.turbine_height_m,
                    a.installed_costs_dollars_per_kw,
                    a.fixed_om_dollars_per_kw_per_yr,
                    a.variable_om_dollars_per_kwh
                FROM %(schema)s.turbine_costs_per_size_and_year a
                WHERE a.year = %(start_year)s""" % inputs
    cur.execute(sql)
    con.commit()    

def write_costs(con, cur, schema, learning_curves_mode, year, end_year):
    
    inputs = locals().copy()
    inputs['prev_year'] = year - 2    
    inputs['next_year'] = year + 2
    
    # do not run if in the final model year
    if year == end_year:
        return
    
    for i, row in learning_curves_mode.iterrows():
        lc_enabled = row['enabled']
        tech = row['tech']
        if lc_enabled == True:
            if tech == 'solar':
                sql = """INSERT INTO %(schema)s.yearly_technology_costs_solar
                        WITH a AS
                        (
                            SELECT a.year, a.sector_abbr, 
                                	((c.cumulative_installed_capacity/d.cumulative_installed_capacity)/b.frac_of_global_mkt)^(ln(1-b.learning_rate)/ln(2)) as cost_scalar,
                                a.inverter_cost_dollars_per_kw,
                                a.installed_costs_dollars_per_kw,
                                e.fixed_om_dollars_per_kw_per_yr,
                                e.variable_om_dollars_per_kwh
                            FROM %(schema)s.yearly_technology_costs_solar a
                            LEFT JOIN %(schema)s.input_solar_cost_learning_rates b
                                ON b.year = %(year)s
                            LEFT JOIN %(schema)s.cumulative_installed_capacity_solar c
                                ON c.year = %(year)s
                            LEFT JOIN %(schema)s.cumulative_installed_capacity_solar d
                                ON d.year = %(prev_year)s
                            LEFT JOIN %(schema)s.input_solar_cost_projections_to_model e
                                ON e.year = %(next_year)s
                                AND e.sector = a.sector_abbr
                            WHERE a.year = %(year)s
                        )
                        SELECT year + 2 as year, sector_abbr,
                            installed_costs_dollars_per_kw * cost_scalar as installed_costs_dollars_per_kw,
                            inverter_cost_dollars_per_kw * cost_scalar as inverter_cost_dollars_per_kw,
                            fixed_om_dollars_per_kw_per_yr,
                            variable_om_dollars_per_kwh
                        FROM a""" % inputs
            elif tech == 'wind':
                sql = """INSERT INTO %(schema)s.yearly_technology_costs_wind
                        WITH a AS
                        (
                            SELECT a.year, a.turbine_size_kw, a.turbine_height_m, 
                                	((c.cumulative_installed_capacity/d.cumulative_installed_capacity)/f.frac_of_global_mkt)^(ln(1-b.learning_rate)/ln(2)) as cost_scalar,
                                a.installed_costs_dollars_per_kw,
                                e.fixed_om_dollars_per_kw_per_yr,
                                e.variable_om_dollars_per_kwh
                            FROM %(schema)s.yearly_technology_costs_wind a
                            LEFT JOIN %(schema)s.input_wind_cost_learning_rates b
                                ON b.year = %(year)s
                                AND a.turbine_size_kw = b.turbine_size_kw
                            LEFT JOIN %(schema)s.input_wind_cost_global_fraction f
                                ON f.year = %(year)s
                            LEFT JOIN %(schema)s.cumulative_installed_capacity_wind c
                                ON c.year = %(year)s
                                AND a.turbine_size_kw = c.turbine_size_kw
                            LEFT JOIN %(schema)s.cumulative_installed_capacity_wind d
                                ON d.year = %(prev_year)s
                                AND a.turbine_size_kw = d.turbine_size_kw
                            LEFT JOIN %(schema)s.turbine_costs_per_size_and_year e
                                ON e.year = %(next_year)s
                                AND a.turbine_size_kw = e.turbine_size_kw
                                AND a.turbine_height_m = e.turbine_height_m
                            WHERE a.year = %(year)s
                        )
                        SELECT year + 2 as year, turbine_size_kw, turbine_height_m,
                            installed_costs_dollars_per_kw * cost_scalar as installed_costs_dollars_per_kw,
                            fixed_om_dollars_per_kw_per_yr,
                            variable_om_dollars_per_kwh
                        FROM a""" % inputs
        else:
            if tech == 'solar':
                sql = """INSERT INTO %(schema)s.yearly_technology_costs_solar
                         SELECT a.year, a.sector as sector_abbr, 
                                a.installed_costs_dollars_per_kw,
                                a.inverter_cost_dollars_per_kw,
                                a.fixed_om_dollars_per_kw_per_yr,
                                a.variable_om_dollars_per_kwh
                            FROM %(schema)s.input_solar_cost_projections_to_model a
                            WHERE a.year = %(next_year)s
                        """ % inputs
            elif tech == 'wind':
                sql = """INSERT INTO %(schema)s.yearly_technology_costs_wind
                         SELECT a.year, 
                                a.turbine_size_kw,
                                a.turbine_height_m,
                                a.installed_costs_dollars_per_kw,
                                a.fixed_om_dollars_per_kw_per_yr,
                                a.variable_om_dollars_per_kwh
                            FROM %(schema)s.turbine_costs_per_size_and_year a
                            WHERE a.year = %(next_year)s
                        """ % inputs     
        cur.execute(sql)
        con.commit()
        

def get_financial_parameters(con, schema):
    ''' Pull financial parameters dataframe from dB. We used to filter by business model here, but with leasing we will join
    on sector and business_model later in calc_economics.
    
        IN: con - pg con object - connection object
            schema - string - schema for technology i.e. diffusion_solar
            
        OUT: fin_param  - pd dataframe - pre-processed resource,bins, rates, etc. for all years:
    '''
    inputs = locals().copy()
    
    sql = '''SELECT * 
             FROM %(schema)s.input_financial_parameters;''' % inputs
    df = pd.read_sql(sql, con)
    
    return df
 
   
def get_max_market_share(con, schema):
    ''' Pull max market share from dB, select curve based on scenario_options, and interpolate to tenth of a year. 
        Use passed parameters to determine ownership type
    
        IN: con - pg con object - connection object
            schema - string - schema for technology i.e. diffusion_solar

            
        OUT: max_market_share  - pd dataframe - dataframe to join on main df to determine max share 
                                                keys are sector & payback period 
    '''

    sql = '''SELECT metric_value, 
                    sector_abbr, 
                    max_market_share, 
                    metric, 
                    source, 
                    business_model
             FROM %s.max_market_curves_to_model
             
             UNION ALL
             
            SELECT 30.1 as metric_value, 
                    sector_abbr, 
                    0::NUMERIC as max_market_share, 
                    metric, 
                    source, 
                    business_model
            FROM %s.max_market_curves_to_model
            WHERE metric_value = 30 
            AND metric = 'payback_period' 
            AND business_model = 'host_owned';'''  % (schema, schema)
    max_market_share = pd.read_sql(sql, con)
   
    return max_market_share
    

def get_market_projections(con, schema):
    ''' Pull market projections table from dB
    
        IN: con - pg con object - connection object
        OUT: market_projections - numpy array - table containing various market projections
    '''
    sql = '''SELECT * 
             FROM %s.input_main_market_projections;''' % schema
    return pd.read_sql(sql , con)



def calc_dsire_incentives(df, dsire_incentives, srecs, cur_year, dsire_opts, assumed_duration = 10):
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
    
    dsire_df = pd.merge(df, dsire_incentives, how = 'left', on = ['state_abbr', 'sector_abbr', 'tech'])    
    srecs_df = pd.merge(df, srecs, how = 'left', on = ['state_abbr', 'sector_abbr', 'tech'])

    # combine sr and inc
    inc = pd.concat([dsire_df, srecs_df], axis = 0, ignore_index = True) 
    # merge dsire opts
    inc = pd.merge(inc, dsire_opts, how = 'left', on = 'tech')

    # Shorten names
    inc['ic'] = inc['installed_costs_dollars_per_kw'] * inc['system_size_kw']
    
    cur_date = np.array([datetime.date(cur_year, 1, 1)]*len(inc))
    
    # 1. # Calculate Value of Increment Incentive
    # The amount of capacity that qualifies for the increment
    inc['cap_1'] = np.minimum(inc.increment_1_capacity_kw, inc['system_size_kw'])
    inc['cap_2'] = np.maximum(inc['system_size_kw'] - inc.increment_1_capacity_kw, 0)
    inc['cap_3'] = np.maximum(inc['system_size_kw'] - inc.increment_2_capacity_kw, 0)
    inc['cap_4'] = np.maximum(inc['system_size_kw'] - inc.increment_3_capacity_kw, 0)
    
    inc['est_value_of_increment'] = inc['cap_1'] * inc.increment_1_rebate_dlrs_kw + inc['cap_2'] * inc.increment_2_rebate_dlrs_kw + inc['cap_3'] * inc.increment_3_rebate_dlrs_kw + inc['cap_4'] * inc.increment_4_rebate_dlrs_kw
    inc.loc[:, 'est_value_of_increment'] = inc['est_value_of_increment'].fillna(0)
    inc['value_of_increment'] = np.minimum(inc['est_value_of_increment'], 0.2 * inc['installed_costs_dollars_per_kw'] * inc['system_size_kw'])
     
    # 2. # Calculate lifetime value of PBI & FIT
    inc['pbi_fit_still_exists'] = cur_date <= inc.pbi_fit_end_date # Is the incentive still valid
    # suppress errors where pbi_fit_min or max _size_kw is nan -- this will only occur for rows with no incentives
    with np.errstate(invalid = 'ignore'):   
        inc['pbi_fit_cap'] = np.where(inc['system_size_kw'] < inc.pbi_fit_min_size_kw, 0, inc['system_size_kw'])
        inc.loc[:, 'pbi_fit_cap'] = np.where(inc['pbi_fit_cap'] > inc.pbi_fit_max_size_kw, inc.pbi_fit_max_size_kw, inc['pbi_fit_cap'])
    inc['pbi_fit_aep'] = np.where(inc['aep'] < inc.pbi_fit_min_output_kwh_yr, 0, inc['aep'])
    
    # If exists pbi_fit_kwh > 0 but no duration, assume duration
    inc.loc[(inc.pbi_fit_dlrs_kwh > 0) & inc.pbi_fit_duration_years.isnull(), 'pbi_fit_duration_years']  = assumed_duration
    inc['value_of_pbi_fit'] = (inc['pbi_fit_still_exists'] * np.minimum(inc.pbi_fit_dlrs_kwh, inc.max_dlrs_yr) * inc['pbi_fit_aep']).astype('float64')
    inc.loc[:, 'value_of_pbi_fit'] = np.minimum(inc['value_of_pbi_fit'], inc.pbi_fit_max_dlrs)
    inc.loc[:, 'value_of_pbi_fit'] = inc.value_of_pbi_fit.fillna(0)
    inc['length_of_pbi_fit'] = inc.pbi_fit_duration_years.fillna(0)
    
    # 3. # Lifetime value of the pbi/fit. Assume all pbi/fits are disbursed over 10 years. 
    # This will get the undiscounted sum of incentive correct, present value may have small error
    inc['lifetime_value_of_pbi_fit'] = inc['length_of_pbi_fit'] * inc['value_of_pbi_fit']
    
    ## Calculate first year value and length of PTC
    inc['ptc_still_exists'] = cur_date <= inc.ptc_end_date # Is the incentive still valid
    inc['ptc_max_size'] = np.minimum(inc['system_size_kw'], inc.tax_credit_max_size_kw)
    inc.loc[(inc.ptc_dlrs_kwh > 0) & (inc.ptc_duration_years.isnull()), 'ptc_duration_years'] = assumed_duration
    with np.errstate(invalid = 'ignore'):
        inc['value_of_ptc'] =  np.where(inc['ptc_still_exists'] & inc.system_size_kw > 0, np.minimum(inc.ptc_dlrs_kwh * inc.aep * (inc['ptc_max_size']/inc.system_size_kw), inc.max_dlrs_yr), 0)
    inc.loc[:, 'value_of_ptc'] = inc.value_of_ptc.fillna(0)    
    inc.loc[:, 'value_of_ptc'] = np.where(inc['value_of_ptc'] < inc.max_tax_credit_dlrs, inc['value_of_ptc'], inc.max_tax_credit_dlrs)
    inc['length_of_ptc'] = inc.ptc_duration_years.fillna(0)
    
    # Lifetime value of the ptc. Assume all ptcs are disbursed over 10 years
    # This will get the undiscounted sum of incentive correct, present value may have small error
    inc['lifetime_value_of_ptc'] = inc['length_of_ptc'] * inc['value_of_ptc']

    # 4. #Calculate Value of Rebate
    inc['rebate_cap'] = np.where(inc['system_size_kw'] < inc.rebate_min_size_kw, 0, inc['system_size_kw'])
    inc.loc[:, 'rebate_cap'] = np.where(inc['rebate_cap'] > inc.rebate_max_size_kw, inc.rebate_max_size_kw, inc['rebate_cap'])
    inc['value_of_rebate'] = inc.rebate_dlrs_kw * inc['rebate_cap']
    inc.loc[:, 'value_of_rebate'] = np.minimum(inc.rebate_max_dlrs, inc['value_of_rebate'])
    inc.loc[:, 'value_of_rebate'] = np.minimum(inc.rebate_pcnt_cost_max * inc['ic'], inc['value_of_rebate'])
    inc.loc[:, 'value_of_rebate'] = inc.value_of_rebate.fillna(0)    
    # overwrite these values with zero where the incentive has expired
    inc.loc[:, 'value_of_rebate'] = np.where(np.array(datetime.date(cur_year, 1, 1)) >= np.array(inc['dsire_default_exp_date']), 0.0, inc['value_of_rebate'])
    

    # 5. # Calculate Value of Tax Credit
    # Assume able to fully monetize tax credits
    
    # check whether the credits are still active (this can be applied universally because DSIRE does not provide specific info 
    # about expirations for each tax credit or deduction). 
    #Assume that expiration date is inclusive e.g. consumer receives incentive in 2016 if expiration date of 2016 (or greater)
    inc.loc[inc.tax_credit_pcnt_cost.isnull(), 'tax_credit_pcnt_cost'] = 0
    inc.loc[inc.tax_credit_pcnt_cost >= 1, 'tax_credit_pcnt_cost'] = 0.01 * inc.tax_credit_pcnt_cost
    inc.loc[inc.tax_deduction_pcnt_cost.isnull(), 'tax_deduction_pcnt_cost'] = 0
    inc.loc[inc.tax_deduction_pcnt_cost >= 1, 'tax_deduction_pcnt_cost'] =  0.01 * inc.tax_deduction_pcnt_cost
    inc['tax_pcnt_cost'] = inc.tax_credit_pcnt_cost + inc.tax_deduction_pcnt_cost
    
    inc.max_tax_credit_dlrs = np.where(inc.max_tax_credit_dlrs.isnull(), 1e9, inc.max_tax_credit_dlrs)
    inc.max_tax_deduction_dlrs = np.where(inc.max_tax_deduction_dlrs.isnull(), 1e9, inc.max_tax_deduction_dlrs)
    inc['max_tax_credit_or_deduction_value'] = np.maximum(inc.max_tax_credit_dlrs,inc.max_tax_deduction_dlrs)
    
    inc['tax_credit_dlrs_kw'] = inc['tax_credit_dlrs_kw'].fillna(0)
    
    inc['value_of_tax_credit_or_deduction'] = inc['tax_pcnt_cost'] * inc['ic'] + inc['tax_credit_dlrs_kw'] * inc['system_size_kw']
    inc.loc[:, 'value_of_tax_credit_or_deduction'] = np.minimum(inc['max_tax_credit_or_deduction_value'], inc['value_of_tax_credit_or_deduction'])
    inc.loc[:, 'value_of_tax_credit_or_deduction'] = np.where(inc.tax_credit_max_size_kw < inc['system_size_kw'], inc['tax_pcnt_cost'] * inc.tax_credit_max_size_kw * inc.installed_costs_dollars_per_kw, inc['value_of_tax_credit_or_deduction'])
    inc.loc[:, 'value_of_tax_credit_or_deduction'] = pd.Series(inc['value_of_tax_credit_or_deduction']).fillna(0)        
    #value_of_tax_credit_or_deduction[np.isnan(value_of_tax_credit_or_deduction)] = 0
    inc.loc[:, 'value_of_tax_credit_or_deduction'] = inc['value_of_tax_credit_or_deduction'].astype(float)
    # overwrite these values with zero where the incentive has expired
    inc.loc[:, 'value_of_tax_credit_or_deduction'] = np.where(np.array(datetime.date(cur_year, 1, 1)) >= np.array(inc['dsire_default_exp_date']), 0.0, inc['value_of_tax_credit_or_deduction'])
        
        
    # sum results to customer bins
    if inc.shape[0] > 0:
        inc_summed = inc[['tech', 'sector_abbr', 'county_id', 'bin_id', 'business_model', 'value_of_increment', 'lifetime_value_of_pbi_fit', 'lifetime_value_of_ptc', 'value_of_rebate', 'value_of_tax_credit_or_deduction']].groupby(['tech', 'sector_abbr', 'county_id','bin_id','business_model']).sum().reset_index() 
    else:
        inc_summed = inc[['tech', 'sector_abbr', 'county_id', 'bin_id', 'business_model', 'value_of_increment', 'lifetime_value_of_pbi_fit', 'lifetime_value_of_ptc', 'value_of_rebate', 'value_of_tax_credit_or_deduction']]

    inc_summed.loc[:, 'value_of_pbi_fit'] = inc_summed['lifetime_value_of_pbi_fit'] / assumed_duration
    inc_summed['pbi_fit_length'] = assumed_duration
    
    inc_summed.loc[:, 'value_of_ptc'] = inc_summed['lifetime_value_of_ptc'] / assumed_duration
    inc_summed['ptc_length'] = assumed_duration
    
    return inc_summed[['tech', 'sector_abbr', 'county_id','bin_id', 'business_model','value_of_increment', 'value_of_pbi_fit', 'value_of_ptc', 'pbi_fit_length', 'ptc_length', 'value_of_rebate', 'value_of_tax_credit_or_deduction']]

def get_rate_escalations(con, schema):
    '''
    Get rate escalation multipliers from database. Escalations are filtered and applied in calc_economics,
    resulting in an average real compounding rate growth. This rate is then used to calculate cash flows
    
    IN: con - connection to server
    OUT: DataFrame with census_division_abbr, sector, year, escalation_factor, and source as columns
    '''  
    inputs = locals().copy()
    
    sql = """SELECT census_division_abbr, lower(sector) as sector_abbr, 
                    array_agg(escalation_factor order by year asc) as rate_escalations
            FROM %(schema)s.rate_escalations_to_model
            GROUP BY census_division_abbr, sector""" % inputs
    rate_escalations = pd.read_sql(sql, con, coerce_float = False)
    
    return rate_escalations


def get_lease_availability(con, schema, tech):
    '''
    Get leasing availability by state and year, based on options selected in input sheet
    
    IN: con - connection to server
    OUT: DataFrame with state, year, and availability (True/False) as columns
    '''  
    inputs = locals().copy()    
    
    sql = '''SELECT state_abbr, year, leasing_allowed
                FROM %(schema)s.input_%(tech)s_leasing_availability;''' % inputs
    df = pd.read_sql(sql, con)
    return df

def get_annual_inflation(con,schema):
    '''
    Get inflation rate (constant for all years & sectors)
    
    IN: con - connection to server, schema
    OUT: Float value of inflation rate
    '''  
    inputs = locals().copy() 
    sql = '''SELECT *
             FROM %(schema)s.input_main_market_inflation;''' % inputs
    df = pd.read_sql(sql, con)
    return df.values[0][0] # Just want the inflation as a float (for now)

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
        # if the mkt_sum == 0, there is no probability of adoption at all, regardless of the business model
        # but the division below will fail for the prob of leasing
        # therefore, just set prob of leasing to zero
        with np.errstate(invalid = 'ignore'):
            df['prob_of_leasing'] = np.where(df['mkt_sum'] == 0, 0, df['mkt_exp']/df['mkt_sum'])
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



def calc_value_of_itc(df, itc_options, year):
    
    # create duplicates of the itc data for each business model
    # host-owend
    itc_ho = itc_options.copy() 
    # set the business model
    itc_ho['business_model'] = 'host_owned'
    
    # tpo
    itc_tpo_nonres = itc_options[itc_options['sector_abbr'] <> 'res'].copy() 
    itc_tpo_res = itc_options[itc_options['sector_abbr'] == 'com'].copy() 
    # reset the sector_abbr to res
    itc_tpo_res.loc[:, 'sector_abbr'] = 'res'
    # combine the data
    itc_tpo = pd.concat([itc_tpo_nonres, itc_tpo_res], axis = 0, ignore_index = True)
    # set the business model
    itc_tpo['business_model'] = 'tpo'    
    
    # concatente the business models
    itc_all = pd.concat([itc_ho, itc_tpo], axis = 0, ignore_index = True)

    row_count = df.shape[0]   
    # merge to df
    df = pd.merge(df, itc_all, how = 'left', on = ['sector_abbr', 'year', 'business_model', 'tech'])
    # drop the rows that are outside of the allowable system sizes
    df = df[(df['system_size_kw'] > df['min_size_kw']) & (df['system_size_kw'] <= df['max_size_kw'])]
    # confirm shape hasn't changed
    if df.shape[0] <> row_count:
        raise ValueError('Row count of dataframe changed during merge')
        
#    # Calculate the value of ITC (accounting for reduced costs from state/local incentives)
    df['applicable_ic'] = (df['installed_costs_dollars_per_kw'] * df['system_size_kw']) - (df['value_of_tax_credit_or_deduction'] + df['value_of_rebate'] + df['value_of_increment'])
    df['value_of_itc'] =  (
                            df['applicable_ic'] *
                            df['itc_fraction'] *
                            (df['system_size_kw'] > df['min_size_kw']) * # filter for system sizes (only applies to wind) [ this is redundant with the filter above ]
                            (df['system_size_kw'] <= df['max_size_kw'])
                          )
                          
    df = df.drop(['applicable_ic', 'itc_fraction'], axis = 1)
    
    return df

    
