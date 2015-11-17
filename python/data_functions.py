# -*- coding: utf-8 -*-
"""
Functions for pulling data
Created on Mon Mar 24 08:59:44 2014
@author: bsigrin
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
from config import show_times
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


@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 1, prefix = '')
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
    
    sql = '''SELECT clone_schema('%(source_schema)s', '%(dest_schema)s', 'diffusion-writers', %(include_data)s);''' % inputs
    cur.execute(sql)        
    con.commit()

    logger.info('\tOutput schema is: %s' % dest_schema)
   
    return dest_schema

@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 1, prefix = '')
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
    
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 1, prefix = '')
def combine_temporal_data(cur, con, schema, techs, start_year, end_year, sector_abbrs):

    logger.info("Combining Temporal Factors")

    if 'wind' in techs:
        combine_temporal_data_wind(cur, con, schema, start_year, end_year, sector_abbrs)
    
    if 'solar' in techs:
        combine_temporal_data_solar(cur, con, schema, start_year, end_year, sector_abbrs)
    

def combine_temporal_data_solar(cur, con, schema, start_year, end_year, sector_abbrs):
    
     # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()       

    # combine all of the temporal data (this only needs to be done once for all sectors)        
    sql = """DROP TABLE IF EXISTS %(schema)s.temporal_factors_solar;
            CREATE UNLOGGED TABLE %(schema)s.temporal_factors_solar as 
            SELECT a.year, 
                	a.efficiency_improvement_factor,
                	a.density_w_per_sqft,
                  a.inverter_lifetime_yrs,
                	d.sector_abbr,
                	d.census_division_abbr,
                	d.scenario as load_growth_scenario,
                	d.load_multiplier
            FROM %(schema)s.input_solar_performance_improvements a
            LEFT JOIN diffusion_shared.aeo_load_growth_projections d
                ON a.year = d.year
            WHERE a.year BETWEEN %(start_year)s AND %(end_year)s
                AND d.sector_abbr in (%(sector_abbrs)s);""" % inputs
    cur.execute(sql)
    con.commit()
    
    # create indices for subsequent joins
    sql =  """CREATE INDEX temporal_factors_sector_abbr_btree 
              ON %(schema)s.temporal_factors_solar 
              USING BTREE(sector_abbr);
              
              CREATE INDEX temporal_factors_load_growth_scenario_btree 
              ON %(schema)s.temporal_factors_solar 
              USING BTREE(load_growth_scenario);
              
              CREATE INDEX temporal_factors_census_division_abbr_btree 
              ON %(schema)s.temporal_factors_solar 
              USING BTREE(census_division_abbr);""" % inputs
    cur.execute(sql)
    con.commit()  


def combine_temporal_data_wind(cur, con, schema, start_year, end_year, sector_abbrs):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()       
    
    # combine the temporal data (this only needs to be done once for all sectors)
    
    # combined temporal data for technology specific factors
    sql = """DROP TABLE IF EXISTS %(schema)s.temporal_factors_wind;
            CREATE UNLOGGED TABLE %(schema)s.temporal_factors_wind as
            SELECT      a.year, 
                    	a.turbine_size_kw, 
                    	a.power_curve_id,
                    	b.turbine_height_m,
                    	d.derate_factor
            FROM %(schema)s.input_wind_performance_improvements a
            LEFT JOIN diffusion_wind.allowable_turbine_sizes b
                	ON a.turbine_size_kw = b.turbine_size_kw
            LEFT JOIN %(schema)s.input_wind_performance_gen_derate_factors d
                	ON a.year = d.year
                 AND  a.turbine_size_kw = d.turbine_size_kw
            WHERE a.year BETWEEN %(start_year)s AND %(end_year)s
            
            UNION ALL
            
            SELECT GENERATE_SERIES(%(start_year)s, %(end_year)s, 2) as year,
                	0 as turbine_size_kw,
                	0 as power_curve_id,
                	0 as turbine_height_m,
                	0 as derate_factor;""" % inputs
    cur.execute(sql)
    con.commit()
    
    
    # create indices for subsequent joins
    sql =  """CREATE INDEX temporal_factors_technology_turbine_height_m_btree 
              ON %(schema)s.temporal_factors_wind
              USING BTREE(turbine_height_m);
              
              CREATE INDEX temporal_factors_technology_power_curve_id_btree 
              ON %(schema)s.temporal_factors_wind
              USING BTREE(power_curve_id);
              
              CREATE INDEX temporal_factors_technology_year_btree 
              ON %(schema)s.temporal_factors_wind
              USING BTREE(year);""" % inputs
    cur.execute(sql)
    con.commit()                


    
def clear_outputs(con, cur, schema):
    """Delete all rows from the res, com, and ind output tables"""
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()      
    
    sql = """DELETE FROM %(schema)s.outputs_res;
            DELETE FROM %(schema)s.outputs_com;
            DELETE FROM %(schema)s.outputs_ind;""" % inputs
    cur.execute(sql)
    con.commit()


def write_outputs(con, cur, outputs_df, sectors, schema):
    
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
                'carbon_price_cents_per_kwh', 
                'fixed_om_dollars_per_kw_per_yr',
                'variable_om_dollars_per_kwh', 
                'installed_costs_dollars_per_kw',    
                'inverter_cost_dollars_per_kw',
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
                'excess_generation_percent',
                'total_value_of_incentives',
                'value_of_itc',
                'tech',
                'selected_option']    

    # convert formatting of fields list
    inputs['fields_str'] = utilfunc.pylist_2_pglist(fields).replace("'","")       
    
    for sector_abbr, sector in sectors.iteritems():    
        inputs['sector_abbr'] = sector_abbr
        # open an in memory stringIO file (like an in memory csv)
        s = StringIO()
        # write the data to the stringIO
        outputs_df.loc[outputs_df['sector_abbr'] == sector_abbr, fields].to_csv(s, index = False, header = False)
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

def combine_outputs_wind(schema, sectors, cur, con):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()   

    sql = '''DROP TABLE IF EXISTS %(schema)s.outputs_all_wind CASCADE;
            CREATE UNLOGGED TABLE %(schema)s.outputs_all_wind AS  ''' % inputs  
    
    for i, sector_abbr in enumerate(sectors.keys()):
        inputs['sector'] = sectors[sector_abbr].lower()
        inputs['sector_abbr'] = sector_abbr
        if i > 0:
            inputs['union'] = 'UNION ALL '
        else:
            inputs['union'] = ''
        
        sub_sql = '''%(union)s 
                    SELECT a.tech, '%(sector)s'::text as sector, 

                    a.micro_id, a.county_id, a.bin_id, a.year, a.business_model, a.loan_term_yrs, 
                    a.loan_rate, a.down_payment, a.discount_rate, a.tax_rate, a.length_of_irr_analysis_yrs, 
                    a.market_share_last_year, a.number_of_adopters_last_year, a.installed_capacity_last_year, 
                    a.market_value_last_year, a.value_of_increment, a.value_of_pbi_fit, 
                    a.value_of_ptc, a.pbi_fit_length, a.ptc_length, a.value_of_rebate, a.value_of_tax_credit_or_deduction, 
                    a.ic, a.metric, a.metric_value, a.lcoe, a.max_market_share, 
                    a.diffusion_market_share, a.new_market_share, a.new_adopters, a.new_capacity, 
                    a.new_market_value, a.market_share, a.number_of_adopters, a.installed_capacity, 
                    a.market_value, a.first_year_bill_with_system, a.first_year_bill_without_system, 
                    a.npv4, a.excess_generation_percent, a.value_of_itc, a.total_value_of_incentives, a.selected_option,
                    a.carbon_price_cents_per_kwh, 
                    a.fixed_om_dollars_per_kw_per_yr, 
                    a.variable_om_dollars_per_kwh, a.installed_costs_dollars_per_kw, 
                    a.inverter_cost_dollars_per_kw, 

                    b.state_abbr, b.census_division_abbr, b.utility_type, b.hdf_load_index,
                    b.pca_reg, b.reeds_reg, b.incentive_array_id, b.ranked_rate_array_id,
                    b.ann_cons_kwh, 
                    b.customers_in_bin, b.initial_customers_in_bin, 
                    b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
                    b.crb_model, b.max_demand_kw, b.rate_id_alias, b.rate_source, 
                    b.ur_enable_net_metering, b.nem_system_size_limit_kw,
                    b.ur_nm_yearend_sell_rate, b.ur_flat_sell_rate,
                    b.naep/8760 as cf, b.naep, b.aep, b.system_size_kw,
                    CASE WHEN b.turbine_size_kw = 1500 AND b.nturb > 1 THEN '1500+'::TEXT 
                    ELSE b.turbine_size_kw::TEXT 
                    END as system_size_factors,
                    b.turbine_id,
                    b.i, b.j, b.cf_bin,
                    b.nturb, b.turbine_size_kw, 
                    b.turbine_height_m, b.scoe,
                    
                    a.first_year_bill_without_system/b.load_kwh_per_customer_in_bin as cost_of_elec_dols_per_kwh,
                    
                    c.initial_market_share, c.initial_number_of_adopters,
                    c.initial_capacity_kw * 1000 as initial_capacity_mw
                                        
                    
                    FROM %(schema)s.outputs_%(sector_abbr)s a
                    
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind b
                    ON a.county_id = b.county_id
                    AND a.bin_id = b.bin_id
                    and a.year = b.year
                    
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_initial_market_shares_wind c
                    ON a.county_id = c.county_id
                    AND a.bin_id = c.bin_id
                    
                    WHERE a.tech = 'wind'
                    ''' % inputs
        sql += sub_sql
    
    sql += ';'
    cur.execute(sql)
    con.commit()

    # create indices that will be needed for various aggregations in R visualization script
    sql = '''CREATE INDEX outputs_all_wind_year_btree ON %(schema)s.outputs_all_wind USING BTREE(year);
             CREATE INDEX outputs_all_wind_state_abbr_btree ON %(schema)s.outputs_all_wind USING BTREE(state_abbr);
             CREATE INDEX outputs_all_wind_sector_btree ON %(schema)s.outputs_all_wind USING BTREE(sector);
             CREATE INDEX outputs_all_wind_business_model_btree ON %(schema)s.outputs_all_wind USING BTREE(business_model);
             CREATE INDEX outputs_all_wind_system_size_factors_btree ON %(schema)s.outputs_all_wind USING BTREE(system_size_factors);                          
             CREATE INDEX outputs_all_wind_metric_btree ON %(schema)s.outputs_all_wind USING BTREE(metric);             
             CREATE INDEX outputs_all_wind_turbine_height_m_btree ON %(schema)s.outputs_all_wind USING BTREE(turbine_height_m);
             CREATE INDEX outputs_all_wind_tech_btree ON %(schema)s.outputs_all_wind USING BTREE(tech);
             ''' % inputs
    cur.execute(sql)
    con.commit()


def combine_outputs_solar(schema, sectors, cur, con):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()   

    sql = '''DROP TABLE IF EXISTS %(schema)s.outputs_all_solar CASCADE;
            CREATE UNLOGGED TABLE %(schema)s.outputs_all_solar AS  ''' % inputs  
    
    for i, sector_abbr in enumerate(sectors.keys()):
        inputs['sector'] = sectors[sector_abbr].lower()
        inputs['sector_abbr'] = sector_abbr
        if i > 0:
            inputs['union'] = 'UNION ALL '
        else:
            inputs['union'] = ''
        
        sub_sql = '''%(union)s 
                    SELECT a.tech, '%(sector)s'::text as sector, 

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
                    a.npv4, a.excess_generation_percent, a.value_of_itc, a.total_value_of_incentives, a.selected_option,
                    a.carbon_price_cents_per_kwh, 
                    a.fixed_om_dollars_per_kw_per_yr, 
                    a.variable_om_dollars_per_kwh, a.installed_costs_dollars_per_kw, 
                    a.inverter_cost_dollars_per_kw, 
                    
                    b.state_abbr, b.census_division_abbr, b.utility_type, b.hdf_load_index,
                    b.pca_reg, b.reeds_reg, b.incentive_array_id, b.ranked_rate_array_id,
                    b.ann_cons_kwh, 
                    b.customers_in_bin, b.initial_customers_in_bin, 
                    b.load_kwh_in_bin, b.initial_load_kwh_in_bin, b.load_kwh_per_customer_in_bin, 
                    b.crb_model, b.max_demand_kw, b.rate_id_alias, b.rate_source, 
                    b.ur_enable_net_metering, b.nem_system_size_limit_kw,
                    b.ur_nm_yearend_sell_rate, b.ur_flat_sell_rate,   
                    b.naep/8760 as cf, b.naep, b.aep, b.system_size_kw, 
                    r_cut(b.system_size_kw, ARRAY[0,2.5,5.0,10.0,20.0,50.0,100.0,250.0,500.0,750.0,1000.0,1500.0]) 
                        as system_size_factors,
                    b.npanels, 
                    b.tilt, b.azimuth,
                    b.pct_developable, b.solar_re_9809_gid, 
                    b.density_w_per_sqft, b.inverter_lifetime_yrs, 
                    b.available_roof_sqft, b.bldg_size_class, b.ground_cover_ratio,
                    
                    a.first_year_bill_without_system/b.load_kwh_per_customer_in_bin as cost_of_elec_dols_per_kwh,
                    
                    c.initial_market_share, c.initial_number_of_adopters,
                     c.initial_capacity_kw * 1000 as initial_capacity_mw
                    
                    FROM %(schema)s.outputs_%(sector_abbr)s a
                    
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar b
                    ON a.county_id = b.county_id
                    AND a.bin_id = b.bin_id
                    and a.year = b.year
                    
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_initial_market_shares_solar c
                    ON a.county_id = c.county_id
                    AND a.bin_id = c.bin_id
                    
                    WHERE a.tech = 'solar'
                    ''' % inputs
        sql += sub_sql
    
    sql += ';'
    cur.execute(sql)
    con.commit()

    # create indices that will be needed for various aggregations in R visualization script
    sql = '''CREATE INDEX outputs_all_solar_year_btree ON %(schema)s.outputs_all_solar USING BTREE(year);
             CREATE INDEX outputs_all_solar_state_abbr_btree ON %(schema)s.outputs_all_solar USING BTREE(state_abbr);
             CREATE INDEX outputs_all_solar_sector_btree ON %(schema)s.outputs_all_solar USING BTREE(sector);
             CREATE INDEX outputs_all_solar_business_model_btree ON %(schema)s.outputs_all_solar USING BTREE(business_model);
             CREATE INDEX outputs_all_solar_system_size_factors_btree ON %(schema)s.outputs_all_solar USING BTREE(system_size_factors);                          
             CREATE INDEX outputs_all_solar_metric_btree ON %(schema)s.outputs_all_solar USING BTREE(metric);
             CREATE INDEX outputs_all_solar_tech_btree ON %(schema)s.outputs_all_solar USING BTREE(tech);
             ''' % inputs
    cur.execute(sql)
    con.commit()


def combine_output_view(schema, cur, con, techs):
    
    inputs = locals().copy()
    
    sql_list = []
    for tech in techs:
        inputs['tech'] = tech
        sql = """SELECT tech, sector, micro_id, county_id, bin_id, year, business_model,
                        loan_term_yrs, loan_rate, down_payment, discount_rate, tax_rate,
                        length_of_irr_analysis_yrs, market_share_last_year, 
                        number_of_adopters_last_year, installed_capacity_last_year, 
                        market_value_last_year, value_of_increment, value_of_pbi_fit, 
                        value_of_ptc, pbi_fit_length, ptc_length, value_of_rebate, 
                        value_of_tax_credit_or_deduction, ic, metric, metric_value, 
                        lcoe, max_market_share, diffusion_market_share, new_market_share,
                        new_adopters, new_capacity, new_market_value, market_share, 
                        number_of_adopters, installed_capacity, market_value, 
                        first_year_bill_with_system, first_year_bill_without_system, 
                        npv4, excess_generation_percent, value_of_itc, total_value_of_incentives, 
                        state_abbr, census_division_abbr, utility_type, hdf_load_index, 
                        pca_reg, reeds_reg, incentive_array_id, ranked_rate_array_id, 
                        carbon_price_cents_per_kwh, fixed_om_dollars_per_kw_per_yr, 
                        variable_om_dollars_per_kwh, installed_costs_dollars_per_kw, 
                        customers_in_bin, initial_customers_in_bin, load_kwh_in_bin, 
                        initial_load_kwh_in_bin, load_kwh_per_customer_in_bin, crb_model, 
                        max_demand_kw, rate_id_alias, rate_source, ur_enable_net_metering, 
                        nem_system_size_limit_kw, ur_nm_yearend_sell_rate, ur_flat_sell_rate, 
                        cf, naep, aep, system_size_kw, system_size_factors, 
                        cost_of_elec_dols_per_kwh, 
                        initial_market_share, initial_number_of_adopters, 
                        initial_capacity_mw, selected_option
                 FROM %(schema)s.outputs_all_%(tech)s""" % inputs
        sql_list.append(sql)
    
    inputs['sql_combined'] = ' UNION ALL '.join(sql_list)
    sql = """DROP VIEW IF EXISTS %(schema)s.outputs_all;
              CREATE VIEW %(schema)s.outputs_all AS
              %(sql_combined)s;""" % inputs
    cur.execute(sql)
    con.commit()


def combine_outputs(techs, schema, sectors, cur, con):
        
    if 'wind' in techs:
        combine_outputs_wind(schema, sectors, cur, con)

    if 'solar' in techs:
        combine_outputs_solar(schema, sectors, cur, con)        

    combine_output_view(schema, cur, con, techs)

@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def copy_outputs_to_csv(techs, schema, out_scen_path, cur, con, file_suffix = ''):
    
    logger.info('\tExporting Results from Database')

    # copy data to csv
    for tech in techs:
        out_file = os.path.join(out_scen_path, tech, 'outputs_%s%s.csv.gz' % (tech, file_suffix))
        f = gzip.open(out_file,'w',1)
        cur.copy_expert('COPY %s.outputs_all_%s TO STDOUT WITH CSV HEADER;' % (schema, tech), f)
        f.close()
    
    # write the scenario optoins to csv as well
    f2 = open(os.path.join(out_scen_path, 'scenario_options_summary.csv'),'w')
    cur.copy_expert('COPY %s.input_main_scenario_options TO STDOUT WITH CSV HEADER;' % schema, f2)
    f2.close()
    
    
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def create_scenario_report(techs, schema, scen_name, out_scen_path, cur, con, Rscript_path, pg_params_file, file_suffix = ''):
    
    if len(techs) > 1:
        logger.info('\tCompiling Output Reports')
    else:
        logger.info('\tCompiling Output Report')
    
    # path to the plot_outputs R script        
    plot_outputs_path = '%s/r/graphics/plot_outputs.R' % os.path.dirname(os.getcwd())        
    
    
    for tech in techs:
        out_tech_path = os.path.join(out_scen_path, tech)
        #command = ("%s --vanilla ../r/graphics/plot_outputs.R %s" %(Rscript_path, runpath))
        # for linux and mac, this needs to be formatted as a list of args passed to subprocess
        command = [Rscript_path,'--vanilla', plot_outputs_path, out_tech_path, scen_name, tech, schema, pg_params_file, file_suffix]
        proc = subprocess.Popen(command,stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        messages = proc.communicate()
        if 'error' in messages[1].lower():
            logger.error(messages[1])
        if 'warning' in messages[1].lower():
            logger.warning(messages[1])

@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
def create_tech_choice_report(choose_tech, schema, scen_name, out_scen_path, cur, con, Rscript_path, pg_params_file, file_suffix = ''):
    
    if choose_tech == True:
        logger.info('\tCompiling Technology Choice Report')        
    
        # path to the plot_outputs R script        
        plot_outputs_path = '%s/r/graphics/tech_choice_report.R' % os.path.dirname(os.getcwd())        
            
        out_path = os.path.join(out_scen_path, 'tech_choice')
        #command = ("%s --vanilla ../r/graphics/plot_outputs.R %s" %(Rscript_path, runpath))
        # for linux and mac, this needs to be formatted as a list of args passed to subprocess
        command = [Rscript_path,'--vanilla', plot_outputs_path, out_path, scen_name, schema, pg_params_file, file_suffix]
        proc = subprocess.Popen(command,stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        messages = proc.communicate()
        if 'error' in messages[1].lower():
            logger.error(messages[1])
        if 'warning' in messages[1].lower():
            logger.warning(messages[1])
    else:
        logger.info("\tSkipping Creation of Technology Choice Report (Not Applicable)")

@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 1, prefix = '')
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
                    micro_id integer,
                    county_id integer,
                    bin_id integer,
                    year integer,
                    state_abbr text,
                    census_division_abbr text,
                    utility_type text,
                    pca_reg text,
                    reeds_reg integer,
                    incentive_array_id integer,
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

@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 3, prefix = '')
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


@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 3, prefix = '')
def get_economics_df(con, schema, year):

    inputs = locals().copy()
    
    logger.info("\t\tLoading economics results from database")

    sql = """SELECT * FROM 
            %(schema)s.economic_results
            WHERE year = %(year)s;""" % inputs

    df = pd.read_sql(sql, con)
    
    return df

@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 2, prefix = '')
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


@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 1, prefix = '')
def copy_deployment_summary_to_csv(schema, out_scen_path, cur, con):
    
    logger.info('Exporting Deployment Summary from Database')

    # copy data to csv
    out_file = os.path.join(out_scen_path, 'deployment_summary.csv.gz')
    f = gzip.open(out_file,'w',1)
    cur.copy_expert('COPY %s.deployment_summary TO STDOUT WITH CSV HEADER;' % schema, f)
    f.close()


def generate_customer_bins(cur, con, techs, schema, n_bins, sectors, start_year, end_year,
                           npar, pg_conn_string, scenario_opts):
                               

    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    
    # extract settings from scenario opts
    load_growth_scenario = scenario_opts['load_growth_scenario'].lower()    
    seed = scenario_opts['random_generator_seed']
    
    # get rate structure settings (this will be for all sectors)
    rate_structures = get_rate_structures(con, schema)
    
    # combine all temporally varying data
    combine_temporal_data(cur, con, schema, techs, start_year, end_year, utilfunc.pylist_2_pglist(sectors.keys()))                           
    
    # break counties into subsets for parallel processing
    county_chunks, npar = split_counties(cur, schema, npar)    
    
    for sector_abbr, sector in sectors.iteritems():
        with utilfunc.Timer() as t:
            logger.info("Creating Agents for %s Sector" % sector)
                
            #==============================================================================
            #     sample customer locations and load. and link together    
            #==============================================================================
            sample_customers_and_load(schema, sector_abbr, county_chunks, n_bins, seed, npar, pg_conn_string)
            
            #==============================================================================
            #     get rate for each agent
            #==============================================================================
            rate_structure = rate_structures[sector_abbr]
            find_rates(schema, sector_abbr, county_chunks, seed, npar, pg_conn_string, rate_structure, techs)
            
            #==============================================================================
            #     run the portions that are technology specific
            #==============================================================================
            if 'wind' in techs:
                resource_key = 'i,j,cf_bin'
                technology = 'wind'
                logger.info('\tAttributing Agents with Wind Data')
                generate_customer_bins_wind(cur, con, technology, schema, seed, n_bins, sector_abbr, sector, start_year, end_year, county_chunks,
                                            load_growth_scenario, resource_key, npar, pg_conn_string)
        
            if 'solar' in techs:
                resource_key = 'solar_re_9809_gid'
                technology = 'solar'
                logger.info('\tAttributing Agents with Solar Data')
                generate_customer_bins_solar(cur, con, technology, schema, seed, n_bins, sector_abbr, sector, start_year, end_year, county_chunks,
                                             load_growth_scenario, resource_key, npar, pg_conn_string)  
        
            
            #==============================================================================
            #   clean up intermediate tables
            #==============================================================================
            cleanup_intermediate_tables(schema, sector_abbr, county_chunks, npar, pg_conn_string, cur, con, inputs['i_place_holder'])
            
            
        logger.info('\tTotal time to create agents for %s sector: %0.1fs' % (sector.lower(), t.interval)) 



def check_rooftop_tech_potential_limits(cur, con, schema, techs, sectors, out_dir):
    
    inputs = locals().copy()    
    
    logger.info('Checking Agent Tech Potential Against State Tech Potential Limits')    
    
    for tech in techs:
        inputs['tech'] = tech
        if tech == 'wind':
            logger.warning('\tTech potential limits are not available for distributed wind. Agents cannot be checked.')
        elif tech == 'solar':
            sql_list = []
            for sector_abbr, sector in sectors.iteritems():
                inputs['sector_abbr'] = sector_abbr
                sql = """SELECT state_abbr, bldg_size_class, 
                                sum(aep * initial_customers_in_bin)/1e6 as gen_gwh,
                                sum(available_roof_sqft * initial_customers_in_bin)/10.7639 as area_m2,
                                sum(system_size_kw * initial_customers_in_bin)/1e6 as cap_gw
                       FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
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
                logger.warning('\tModel tech potential exceeds actual %(tech)s tech potential for some states. See: %(out_overage_csv)s for details.' % inputs)                
                overage.to_csv(inputs['out_overage_csv'], index = False, header = True)
            else:
                inputs['out_ratios_csv'] = os.path.join(out_dir, 'tech_potential_ratios_solar.csv')
                logger.info('\tModel tech potential is within state %(tech)s tech potential limits. See: %(out_ratios_csv)s for details.' % inputs)
                sql = """SELECT *
                     FROM %(schema)s.tech_potential_ratios_solar""" % inputs
                ratios = pd.read_sql(sql, con)
                ratios.to_csv(inputs['out_ratios_csv'], index = False, header = True)
            




def cleanup_intermediate_tables(schema, sector_abbr, county_chunks, npar, pg_conn_string, cur, con, i_place_holder):
    
    inputs = locals().copy()    
    
    #==============================================================================
    #   clean up intermediate tables
    #==============================================================================
    msg = "\tCleaning Up Intermediate Tables..."
    logger.info(msg)
    intermediate_tables = [ '%(schema)s.county_rooftop_availability_samples_%(sector_abbr)s_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_and_resource_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_%(i_place_holder)s' % inputs,
                            '%(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_applicable_rates_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_rate_allowable_turbines_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_rate_turbine_resource_%(i_place_holder)s' % inputs,
                            '%(schema)s.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s' % inputs,   
                            '%(schema)s.pt_%(sector_abbr)s_sample_load_rooftop_cities__%(i_place_holder)s' % inputs                 
                            ]
        
    sql = 'DROP TABLE IF EXISTS %s;'
    for intermediate_table in intermediate_tables:
        isql = sql % intermediate_table
        if '%(i)s' in intermediate_table:
            p_run(pg_conn_string, isql, county_chunks, npar)
        else:
            cur.execute(isql)
            con.commit()        


def calc_utility_bills(cur, con, schema, sectors, techs, npar, pg_conn_string, gross_fit_mode, local_cores):
    logger.info("---------Calculating Energy Savings---------")
                
    for tech in techs:
        # find all unique combinations of rates, load, and generation
        
            logger.info('Calculating Annual Electric Bill Savings for %s' % tech.title())
            logger.info('\tFinding Unique Combinations of Rates, Load, and Generation')
            get_unique_parameters_for_urdb3(cur, con, tech, schema, sectors)         
            # determine how many rate/load/gen combinations can be processed given the local memory resources
            row_count_limit = get_max_row_count_for_utilityrate3()            
            sam_results_list = []
            # set up chunks
            uid_lists = split_utilityrate3_inputs(row_count_limit, cur, con, schema, tech)
            nbatches = len(uid_lists)
            t0 = time.time()
            logger.info("\tSAM calculations will be run in %s batches to prevent memory overflow" % nbatches)
            for i, uids in enumerate(uid_lists): 
                logger.info("\t\tWorking on SAM Batch %s of %s" % (i+1, nbatches))
                # collect data for all unique combinations
                logger.info('\t\t\tCollecting SAM Inputs')
                t1 = time.time()
                rate_input_df = get_utilityrate3_inputs(uids, cur, con, tech, schema, npar, 
                                                        pg_conn_string, gross_fit_mode)
                excess_gen_df = rate_input_df[['uid', 'excess_generation_percent', 'net_fit_credit_dollars']]
                logger.info('\t\t\t\tCompleted in: %0.1fs' % (time.time() - t1))        
                # calculate value of energy for all unique combinations
                logger.info('\t\t\tCalculating Energy Savings Using SAM')
                # run sam calcs
                sam_results_df = pssc_mp.pssc_mp(rate_input_df,  local_cores)
                logger.info('\t\t\t\tCompleted in: %0.1fs' % (time.time() - t1),)                                        
                # append the excess_generation_percent and net_fit_credit_dollars to the sam_results_df
                sam_results_df = pd.merge(sam_results_df, excess_gen_df, on = 'uid')

                # adjust the elec_cost_with_system_year1 to account for the net_fit_credit_dollars
                sam_results_df['elec_cost_with_system_year1'] = sam_results_df['elec_cost_with_system_year1'] - sam_results_df['net_fit_credit_dollars']              
                sam_results_list.append(sam_results_df)
                # drop the rate_input_df to save on memory
                del rate_input_df, excess_gen_df
       
            # write results to postgres
            logger.info("\tWriting SAM Results to Database")
            write_utilityrate3_to_pg(cur, con, sam_results_list, schema, sectors, tech)
            logger.info('\tTotal time to calculate all electric bills: %0.1fs' % (time.time() - t0),)  


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
    
    if len(counties) > npar:
        county_chunks = map(list,np.array_split(counties, npar))
    else:
        county_chunks = [counties]
        npar = 1
    
    return county_chunks, npar

def sample_customers_and_load(schema, sector_abbr, county_chunks, n_bins, seed, npar, pg_conn_string):


    inputs_dict = locals().copy()
    
    inputs_dict['i_place_holder'] = '%(i)s'
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
    msg = '\tSampling from Point Microdata for Each County'
    logger.info(msg)
    t0 = time.time() 
    sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_%(i_place_holder)s;
             CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_%(i_place_holder)s AS
            WITH b as 
            (
                SELECT unnest(sample(array_agg(a.micro_id ORDER BY a.micro_id),%(n_bins)s,%(seed)s,True,array_agg(a.point_weight ORDER BY a.micro_id))) as micro_id
                FROM diffusion_shared.point_microdata_%(sector_abbr)s_us a
                WHERE a.county_id IN (%(chunk_place_holder)s)
                GROUP BY a.county_id
            )
                
            SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a.county_id ORDER BY a.county_id, a.micro_id) as bin_id
            FROM %(schema)s.point_microdata_%(sector_abbr)s_us_joined a
            INNER JOIN b
            ON a.micro_id = b.micro_id
            WHERE a.county_id IN (%(chunk_place_holder)s);""" % inputs_dict

    p_run(pg_conn_string, sql, county_chunks, npar)
    logger.info('\t\tCompleted in: %0.1fs' %(time.time() - t0))  

    #==============================================================================
    #    create lookup table with random values for each load bin 
    #==============================================================================
    msg = "\tSampling from Load Microdata"
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
    
    # add an index on county id and row_number
    sql = """CREATE INDEX county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s_join_fields_btree 
            ON %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s USING BTREE(county_id, bin_id);
            CREATE INDEX county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s_crb_model_btree 
            ON %(schema)s.county_load_bins_random_lookup_%(sector_abbr)s_%(i_place_holder)s USING BTREE(crb_model);""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
    logger.info('\t\tCompleted in: %0.1fs' %(time.time() - t0))  
   
    #==============================================================================
    #     link each point to a load bin
    #==============================================================================
    # use random weighted sampling on the load bins to ensure that countyies with <N points
    # have a representative sample of load bins 
    msg = '\tJoining Point and Load Samples'    
    logger.info(msg)
    t0 = time.time()
    sql =  """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s;
            CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_%(i_place_holder)s AS
            WITH binned as
            (
                SELECT a.*, b.crb_model, b.ann_cons_kwh, b.weight as eia_weight, 
                       CASE WHEN b.roof_sqft < 5000 THEN 'small'::character varying(6)
                            WHEN b.roof_sqft >= 5000 and b.roof_sqft < 25000 THEN 'medium'::character varying(6)
                            WHEN b.roof_sqft >= 25000 THEN 'large'::character varying(6)
                        END as bldg_size_class,
                        b.roof_sqft, b.roof_style, b.ownocc8,
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
    logger.info('\t\tCompleted in: %0.1fs' %(time.time() - t0))  


def find_rates(schema, sector_abbr, county_chunks, seed, npar, pg_conn_string, rate_structure, techs):

    inputs_dict = locals().copy()
    inputs_dict['i_place_holder'] = '%(i)s'
    inputs_dict['chunk_place_holder'] = '%(county_ids)s'
    
    logger.info("\tSelecting Rates for Each Agent")
    t0 = time.time()
    excluded_rates = pd.read_csv('./excluded_rates_ids.csv', header=None)
    inputs_dict['excluded_rate_ids'] = '(' + ', '.join([str(i[0]) for i in excluded_rates.values]) + ')'

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
                              b.pct_of_customers,
                    		c.rank as rate_rank
                    	FROM %(schema)s.pt_%(sector_abbr)s_sample_load_demandmax_%(i_place_holder)s a
                    	LEFT JOIN diffusion_shared.urdb_rates_by_state_%(sector_abbr)s b
                                	ON a.max_demand_kw <= b.urdb_demand_max
                                	AND a.max_demand_kw >= b.urdb_demand_min
                                	AND a.state_abbr = b.state_abbr
                    	LEFT JOIN diffusion_shared.ranked_rate_array_lkup_%(sector_abbr)s c
                    	ON a.ranked_rate_array_id = c.ranked_rate_array_id
                    	AND b.rate_id_alias = c.rate_id_alias
                        WHERE b.rate_id_alias NOT IN %(excluded_rate_ids)s
                    ),
                b as
                (
                    	SELECT *, rank() OVER (PARTITION BY county_id, bin_id ORDER BY rate_rank ASC) as rank
                    	FROM a
                )
                SELECT b.*, COALESCE(b.pct_of_customers, c.%(sector_abbr)s_weight) as rate_type_weight
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
    if 'wind' in inputs_dict['techs']:    
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
        
    if 'solar' in inputs_dict['techs']:  
        # add an index on county id and row_number
        sql = """CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_join_fields_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s USING BTREE(county_id, bin_id);
                
                CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_bldg_size_class_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s USING BTREE(bldg_size_class);          
                
                CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_ulocale_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s USING BTREE(ulocale);   
                
                CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_county_id_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s USING BTREE(county_id);   
                
                CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_bin_id_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s USING BTREE(bin_id);   
                
                CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_roof_style_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s USING BTREE(roof_style); 
                
                CREATE INDEX pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s_state_abbr_btree 
                ON %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s USING BTREE(state_abbr);""" % inputs_dict
        p_run(pg_conn_string, sql, county_chunks, npar)

    logger.info('\t\tCompleted in: %0.1fs' %(time.time() - t0))  

def assign_roof_characteristics(inputs_dict, county_chunks, npar, pg_conn_string, con):
     
   
    # get the rooftop source
    sql = """SELECT * 
             FROM %(schema)s.input_solar_rooftop_source;""" % inputs_dict
    rooftop_source_df = pd.read_sql(sql, con)
    rooftop_source = rooftop_source_df['rooftop_source'].iloc[0]
   
    if rooftop_source == 'recs_cbecs':
        #=============================================================================================================
        #     link each point to a rooftop orientation based on roof_style and prob weights in rooftop_dsolar_characteristics
        #=============================================================================================================
        sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s;
                CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s AS
                WITH all_roof_options AS
                (
                    	SELECT a.county_id, a.bin_id, 
                             b.uid as roof_char_uid, b.prob_weight
                    	FROM %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s a
                    	LEFT JOIN diffusion_solar.rooftop_solards_characteristics b
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
                    	c.tilt, c.azimuth, 1-d.pct_shaded as pct_developable,
                    	a.roof_sqft * c.rooftop_portion * c.slope_area_multiplier * c.unshaded_multiplier * c.gcr as available_roof_sqft,
                        c.gcr as ground_cover_ratio
                FROM %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s a
                LEFT JOIN selected_roof_options b
                        ON a.county_id = b.county_id 
                        AND a.bin_id = b.bin_id
                LEFT JOIN diffusion_solar.rooftop_solards_characteristics c
                        ON b.roof_char_uid = c.uid
                LEFT JOIN diffusion_solar.solar_ds_regional_shading_assumptions d
                        ON a.state_abbr = d.state_abbr;""" % inputs_dict
        p_run(pg_conn_string, sql, county_chunks, npar)
    else:
        inputs_dict['rooftop_source'] = rooftop_source
        if inputs_dict['sector_abbr'] == 'res':
            inputs_dict['zone'] = 'residential'
        else:
            inputs_dict['zone'] = 'com_ind'
        
        # find the correct city to sample from
        sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s;
                CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s AS
                with a as
                (
                    	SELECT a.*, b.city_id, b.rank as city_rank
                    	FROM %(schema)s.pt_%(sector_abbr)s_sample_load_selected_rate_%(i_place_holder)s a
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
                WHERE rank = 1;""" % inputs_dict
        p_run(pg_conn_string, sql, county_chunks, npar)
        
        # add indices on join keys
        sql =  """CREATE INDEX pt_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s_join_keys_btree 
                  ON %(schema)s.pt_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s 
                  USING BTREE(city_id, bldg_size_class, ulocale);""" % inputs_dict
        p_run(pg_conn_string, sql, county_chunks, npar)
        
        # sample from the lidar bins for that city
        sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s;
                CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s AS
                WITH b as
                (
                	SELECT a.county_id, a.bin_id,
                		unnest(sample(array_agg(b.pid ORDER BY b.pid), 1, 
                		%(seed)s * a.bin_id * a.county_id, FALSE, 
                        array_agg(b.count ORDER BY b.pid))) as pid
                	FROM %(schema)s.pt_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s a
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
                FROM %(schema)s.pt_%(sector_abbr)s_sample_load_rooftop_cities_%(i_place_holder)s a
                INNER JOIN b
                	ON a.county_id = b.county_id
                	and a.bin_id = b.bin_id
                INNER JOIN diffusion_solar.rooftop_orientation_frequencies_%(rooftop_source)s c
                	ON b.pid = c.pid
                INNER JOIN diffusion_solar.rooftop_ground_cover_ratios d
                	on c.flat_roof = d.flat_roof
                INNER JOIN diffusion_solar.rooftop_percent_developable_buildings_by_state e
                	ON a.state_abbr = e.state_abbr
                	AND a.bldg_size_class = e.size_class;""" % inputs_dict   
        p_run(pg_conn_string, sql, county_chunks, npar)
        
        
    # query for indices creation    
    sql =  """CREATE INDEX pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s_census_division_abbr_btree 
              ON %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s 
              USING BTREE(census_division_abbr);
              
              CREATE INDEX pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s_resource_key_btree 
              ON %(schema)s.pt_%(sector_abbr)s_sample_load_rooftops_%(i_place_holder)s 
              USING BTREE(solar_re_9809_gid, tilt, azimuth);""" % inputs_dict
    p_run(pg_conn_string, sql, county_chunks, npar)
    


def generate_customer_bins_solar(cur, con, technology, schema, seed, n_bins, sector_abbr, sector, start_year, end_year, county_chunks,
                                 load_growth_scenario, resource_key, npar, pg_conn_string):

    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()  
    inputs['i_place_holder'] = '%(i)s'
    inputs['seed_str'] = str(seed).replace('.','p')

    #==============================================================================
    #     Assign rooftop characterisics
    #==============================================================================  
    msg = "\t\tAssigning Rooftop Characteristics"
    logger.info(msg)
    t0 = time.time()
    assign_roof_characteristics(inputs, county_chunks, npar, pg_conn_string, con)
    logger.info('\t\t\tCompleted in: %0.1fs' %(time.time() - t0)) 

    #==============================================================================
    #     Join to Resource
    #==============================================================================
    msg = "\t\tFinding Solar Resource for Each Agent"
    logger.info(msg)
    t0 = time.time()
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
    logger.info('\t\t\tCompleted in: %0.1fs' %(time.time() - t0))  

    #==============================================================================
    #     Find All Combinations of Costs and Resource for Each Customer Bin
    #==============================================================================
    msg = "\t\tCombining Temporal Factors and Resource and Selecting System Configuration for Each Agent"
    t0 = time.time()
    logger.info(msg)
    
    sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar;
                CREATE UNLOGGED TABLE  %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
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
                  incentive_array_id integer,
                  ranked_rate_array_id integer,
                  cap_cost_multiplier numeric,
                  ann_cons_kwh numeric,
                  eia_weight numeric,
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
                  pct_developable double precision,
                  solar_re_9809_gid integer,
                  density_w_per_sqft numeric,
                  inverter_lifetime_yrs integer,
                  available_roof_sqft integer,
                  bldg_size_class character varying(6),
                  ground_cover_ratio numeric,
                  owner_occupancy_status integer
                );""" % inputs
    cur.execute(sql)
    con.commit()
    
    sql =  """INSERT INTO %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
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
                  a.incentive_array_id_solar as incentive_array_id,
                  a.ranked_rate_array_id,
                  a.cap_cost_multiplier_solar as cap_cost_multiplier,
                	a.ann_cons_kwh, 
                  a.eia_weight,
                	b.load_multiplier * a.customers_in_bin * a.pct_developable as customers_in_bin, 
                	a.customers_in_bin * a.pct_developable as initial_customers_in_bin, 
                	b.load_multiplier * a.load_kwh_in_bin * a.pct_developable AS load_kwh_in_bin,
                	a.load_kwh_in_bin * a.pct_developable AS initial_load_kwh_in_bin,
                	a.load_kwh_per_customer_in_bin,
                  a.crb_model,                  
                  a.max_demand_kw,
                  a.rate_id_alias,
                  a.rate_source,
                	a.naep * b.efficiency_improvement_factor as naep,
                  a.tilt,
                  a.azimuth,
                  a.pct_developable,
                  a.solar_re_9809_gid,
                  b.density_w_per_sqft, 
                  b.inverter_lifetime_yrs,
                  c.system_size_limit_kw as nem_system_size_limit_kw,
                  c.year_end_excess_sell_rate_dlrs_per_kwh as ur_nm_yearend_sell_rate,
                  c.hourly_excess_sell_rate_dlrs_per_kwh as ur_flat_sell_rate,
                  a.available_roof_sqft,
                  a.bldg_size_class,
                  a.ground_cover_ratio,
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
                INNER JOIN %(schema)s.temporal_factors_solar b
                    ON a.census_division_abbr = b.census_division_abbr
                    AND b.sector_abbr = '%(sector_abbr)s'
                    AND b.load_growth_scenario = '%(load_growth_scenario)s'
                LEFT JOIN %(schema)s.input_main_nem_scenario c
                    ON c.state_abbr = a.state_abbr
                    AND c.utility_type = a.utility_type
                    AND c.year = b.year
                    AND c.sector_abbr = '%(sector_abbr)s'
                LEFT JOIN %(schema)s.input_solar_performance_system_sizing_factors d
                    ON d.sector_abbr = '%(sector_abbr)s'
            )
                SELECT micro_id, county_id, bin_id, year, state_abbr, census_division_abbr, utility_type, hdf_load_index,
                   pca_reg, reeds_reg, incentive_array_id, ranked_rate_array_id,
                   
                   cap_cost_multiplier,
            
                   ann_cons_kwh, eia_weight,
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
                   pct_developable,
                   solar_re_9809_gid,
                   density_w_per_sqft,
                   inverter_lifetime_yrs,
                   available_roof_sqft,
                   bldg_size_class,
                   ground_cover_ratio,
                   ownocc8 as owner_occupancy_state
          FROM combined;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    # create indices
    sql = """CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_join_fields_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(county_id,bin_id);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_year_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(year);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_incentive_array_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(incentive_array_id);    
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_solar_re_9809_gid_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(solar_re_9809_gid);            
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_tilt_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(tilt);     

             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_azimuth_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(azimuth);        
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_system_size_kw_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(system_size_kw);  

             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_rate_id_alias_source_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(rate_id_alias, rate_source);         
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_hdf_load_index_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(hdf_load_index);  
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_crb_model_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(crb_model);  
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_bldg_size_class_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(bldg_size_class);  
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_state_abbr_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(state_abbr); 
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_load_kwh_per_customer_in_bin_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(load_kwh_per_customer_in_bin);    
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_solar_nem_fields_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_solar
             USING BTREE(ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate);
            """ % inputs
    cur.execute(sql)
    con.commit()
    logger.info('\t\t\tCompleted in: %0.1fs' %(time.time() - t0))  
    

def apply_siting_restrictions(inputs_dict, county_chunks, npar, pg_conn_string):
    
    #==============================================================================
    #     Find the allowable turbine heights and sizes (kw) for each customer bin
    #==============================================================================    
    # (note: some counties will have fewer than N points, in which case, all are returned) 
   
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


########################################################################################################################
########################################################################################################################
########################################################################################################################
def generate_customer_bins_wind(cur, con, technology, schema, seed, n_bins, sector_abbr, sector, start_year, end_year, county_chunks,
                                load_growth_scenario, resource_key, npar, pg_conn_string):

    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()
    inputs['i_place_holder'] = '%(i)s'
    inputs['seed_str'] = str(seed).replace('.','p')
        
    #==============================================================================
    #     apply turbine siting restrictions
    #==============================================================================   
    logger.info('\t\tApplying Turbine Siting Restrictions')
    t0 = time.time()
    apply_siting_restrictions(inputs, county_chunks, npar, pg_conn_string)
    logger.info('\t\t\tCompleted in: %0.1fs' %(time.time() - t0))  
    
    #==============================================================================
    #     Find All Combinations of Points and Wind Resource
    #==============================================================================  
    msg = "\t\tFinding Wind Resource for Each Agent"
    t0 = time.time()
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
    logger.info('\t\t\tCompleted in: %0.1fs' %(time.time() - t0))  


    #==============================================================================
    #     Find All Combinations of Costs and Resource for Each Customer Bin
    #==============================================================================
    msg = "\t\tCombining Temporal Factors and Resource Data"
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
                      a.incentive_array_id_wind as incentive_array_id,
                      a.ranked_rate_array_id,
                      a.ownocc8,
                  
                  a.cap_cost_multiplier_wind as cap_cost_multiplier,
                  
                	a.ann_cons_kwh, a.eia_weight,
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

                INNER JOIN %(schema)s.temporal_factors_wind e
                    ON a.turbine_height_m = e.turbine_height_m
                    AND a.turbine_size_kw = e.turbine_size_kw
                    AND a.power_curve_id = e.power_curve_id
                
                INNER JOIN diffusion_shared.aeo_load_growth_projections b
                    	ON a.census_division_abbr = b.census_division_abbr
                        AND b.sector_abbr = '%(sector_abbr)s'  
                        AND b.scenario = '%(load_growth_scenario)s'                    
                        AND b.year = e.year
                   
                LEFT JOIN %(schema)s.input_main_nem_scenario c
                    ON c.state_abbr = a.state_abbr
                    AND c.utility_type = a.utility_type
                    AND c.year = b.year
                    AND c.sector_abbr = '%(sector_abbr)s'
                
                LEFT JOIN %(schema)s.input_wind_performance_system_sizing_factors d
                    ON d.sector_abbr = '%(sector_abbr)s'

            )
                SELECT micro_id, county_id, bin_id, year, state_abbr, census_division_abbr, utility_type, hdf_load_index,
                   pca_reg, reeds_reg, incentive_array_id, ranked_rate_array_id, 
                   cap_cost_multiplier,
            
                   ann_cons_kwh, eia_weight,
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
    logger.info('\t\t\tCompleted in: %0.1fs' %(time.time() - t0))  


    #==============================================================================
    #    Find the Most Cost-Effective Wind Turbine Configuration for Each Customer Bin
    #==============================================================================
    msg = "\t\tSelecting System Configuration for Each Agent"
    t0 = time.time()
    logger.info(msg)
    # create empty table
    sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind;
            CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind AS
            SELECT *
            FROM %(schema)s.pt_%(sector_abbr)s_sample_all_combinations_0
            LIMIT 0;""" % inputs    
    cur.execute(sql)
    con.commit()
    
    sql =  """INSERT INTO %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
              SELECT distinct on (a.county_id, a.bin_id, a.year) a.*
              FROM  %(schema)s.pt_%(sector_abbr)s_sample_all_combinations_%(i_place_holder)s a
              ORDER BY a.county_id ASC, a.bin_id ASC, a.year ASC, a.scoe ASC,
                       a.system_size_kw ASC, a.turbine_height_m ASC;""" % inputs
    p_run(pg_conn_string, sql, county_chunks, npar)
    
    # create indices
    sql = """CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_join_fields_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(county_id,bin_id);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_year_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(year);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_incentive_array_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(incentive_array_id);              
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_i_j_cf_bin_height_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(i, j, cf_bin, turbine_height_m);     
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_turbine_id_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(turbine_id);   
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_system_size_kw_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(system_size_kw);
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_rate_id_alias_source_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(rate_id_alias, rate_source);             
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_hdf_load_index_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(hdf_load_index);  
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_crb_model_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(crb_model);  

             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_load_kwh_per_customer_in_bin_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(load_kwh_per_customer_in_bin);      
             
             CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_wind_nem_fields_btree 
             ON %(schema)s.pt_%(sector_abbr)s_best_option_each_year_wind
             USING BTREE(ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate);                  
             
             """ % inputs
    cur.execute(sql)
    con.commit()
    logger.info('\t\t\tCompleted in: %0.1fs' %(time.time() - t0))  





def get_unique_parameters_for_urdb3(cur, con, tech, schema, sectors):
    
    
    inputs_dict = locals().copy()     
       
    if tech == 'wind':
        inputs_dict['resource_keys'] = 'i, j, cf_bin, turbine_height_m, turbine_id'
    elif tech == 'solar':
        inputs_dict['resource_keys'] = 'solar_re_9809_gid, tilt, azimuth'


    sqls = []
    for sector_abbr, sector in sectors.iteritems():
        inputs_dict['sector'] = sector
        inputs_dict['sector_abbr'] = sector_abbr
        sql = """SELECT  rate_id_alias, rate_source,
                    	hdf_load_index, crb_model, load_kwh_per_customer_in_bin,
                        %(resource_keys)s, system_size_kw, 
                        ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate
                FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year_%(tech)s
                GROUP BY  rate_id_alias, rate_source,
                    	 hdf_load_index, crb_model, load_kwh_per_customer_in_bin,
                    	 %(resource_keys)s, system_size_kw,
                         ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate""" % inputs_dict
        sqls.append(sql)      
    
    
    inputs_dict['sql'] = ' UNION '.join(sqls)    
    sql = """DROP TABLE IF EXISTS %(schema)s.unique_rate_gen_load_combinations_%(tech)s;
             CREATE UNLOGGED TABLE %(schema)s.unique_rate_gen_load_combinations_%(tech)s AS
             %(sql)s;""" % inputs_dict
    cur.execute(sql)
    con.commit()
    
    
    # create indices on: rate_id_alias, hdf_load_index, crb_model, resource keys
    sql = """CREATE INDEX unique_rate_gen_load_combinations_%(tech)s_rate_id_alias_btree
             ON %(schema)s.unique_rate_gen_load_combinations_%(tech)s
             USING BTREE(rate_id_alias);
             
             CREATE INDEX unique_rate_gen_load_combinations_%(tech)s_rate_source_btree
             ON %(schema)s.unique_rate_gen_load_combinations_%(tech)s
             USING BTREE(rate_source);
            
             CREATE INDEX unique_rate_gen_load_combinations_%(tech)s_hdf_load_index_btree
             ON %(schema)s.unique_rate_gen_load_combinations_%(tech)s
             USING BTREE(hdf_load_index);
             
             CREATE INDEX unique_rate_gen_load_combinations_%(tech)s_crb_model_btree
             ON %(schema)s.unique_rate_gen_load_combinations_%(tech)s
             USING BTREE(crb_model);

             CREATE INDEX unique_rate_gen_load_combinations_%(tech)s_load_kwh_btree
             ON %(schema)s.unique_rate_gen_load_combinations_%(tech)s
             USING BTREE(load_kwh_per_customer_in_bin);
             
             CREATE INDEX unique_rate_gen_load_combinations_%(tech)s_system_size_kw_btree
             ON %(schema)s.unique_rate_gen_load_combinations_%(tech)s
             USING BTREE(system_size_kw);
             
             CREATE INDEX unique_rate_gen_load_combinations_%(tech)s_resource_keys_btree
             ON %(schema)s.unique_rate_gen_load_combinations_%(tech)s
             USING BTREE(%(resource_keys)s);
             
             CREATE INDEX unique_rate_gen_load_combinations_%(tech)s_nem_fields_btree
             ON %(schema)s.unique_rate_gen_load_combinations_%(tech)s
             USING BTREE(ur_enable_net_metering, ur_nm_yearend_sell_rate, ur_flat_sell_rate);
             """ % inputs_dict
             
    cur.execute(sql)
    con.commit()
    
    # add a unique id/primary key
    sql = """ALTER TABLE %(schema)s.unique_rate_gen_load_combinations_%(tech)s
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
    

def split_utilityrate3_inputs(row_count_limit, cur, con, schema, tech):

    inputs_dict = locals().copy()    
    
   # find the set of uids     
    sql =   """SELECT uid 
               FROM %(schema)s.unique_rate_gen_load_combinations_%(tech)s
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

    
def get_utilityrate3_inputs(uids, cur, con, tech, schema, npar, pg_conn_string, gross_fit_mode = False):
    
    
    inputs_dict = locals().copy()     
       
    inputs_dict['load_scale_offset'] = 1e8
    if tech == 'wind':
        inputs_dict['gen_join_clause'] = """a.i = d.i
                                            AND a.j = d.j
                                            AND a.cf_bin = d.cf_bin
                                            AND a.turbine_height_m = d.height
                                            AND a.turbine_id = d.turbine_id"""
        inputs_dict['gen_scale_offset'] = 1e3
    elif tech == 'solar':
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
                        a.ur_enable_net_metering as apply_net_metering, a.ur_nm_yearend_sell_rate, a.ur_flat_sell_rate
            	
            FROM %(schema)s.unique_rate_gen_load_combinations_%(tech)s a
            
            -- JOIN THE RATE DATA
            LEFT JOIN %(schema)s.all_rate_jsons b 
                    ON a.rate_id_alias = b.rate_id_alias
                    AND a.rate_source = b.rate_source
            
            -- JOIN THE LOAD DATA
            LEFT JOIN eplus c
                    ON a.crb_model = c.crb_model
                    AND a.hdf_load_index = c.hdf_index
            
            -- JOIN THE RESOURCE DATA
            LEFT JOIN diffusion_%(tech)s.%(tech)s_resource_hourly d
                    ON %(gen_join_clause)s
            
            WHERE a.uid IN (%(chunk_place_holder)s);""" % inputs_dict

    results = JoinableQueue()    
    jobs = []
 
    for i in range(npar):
        place_holders = {'uids': utilfunc.pylist_2_pglist(uid_chunks[i])}
        isql = sql % place_holders
        proc = Process(target = p_get_utilityrate3_inputs, args = (inputs_dict, pg_conn_string, isql, results, gross_fit_mode))
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

def p_get_utilityrate3_inputs(inputs_dict, pg_conn_string, sql, queue, gross_fit_mode = False):
    try:
        # create cursor and connection
        con, cur = utilfunc.make_con(pg_conn_string)  
        # get the data from postgres
        df = pd.read_sql(sql, con, coerce_float = False)
        # close cursor and connection
        con.close()
        cur.close()
        
        # scale the normalized hourly load based on the annual load and scale offset factor
        df = df.apply(scale_array, axis = 1, args = ('consumption_hourly','load_kwh_per_customer_in_bin', inputs_dict['load_scale_offset']))
        
        # scale the hourly cfs into hourly kw using the system size
        df = df.apply(scale_array, axis = 1, args = ('generation_hourly','system_size_kw', inputs_dict['gen_scale_offset']))

        # calculate the excess generation and make necessary NEM modifications
        df = excess_generation_vectorized(df, gross_fit_mode)
        #df = df.apply(excess_generation_calcs, axis = 1, args = (gross_fit_mode,))
        
        # update the net metering fields in the rate_json
        df = df.apply(update_rate_json_w_nem_fields, axis = 1)

        # add the results to the queue
        queue.put(df[['uid','rate_json','consumption_hourly','generation_hourly', 'excess_generation_percent', 'net_fit_credit_dollars']])
        
    except Exception, e:
        print 'Error: %s' % e
        print sql
    

def run_utilityrate3(df):
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
    

def write_utilityrate3_to_pg(cur, con, sam_results_list, schema, sectors, tech):
    
    inputs_dict = locals().copy()  

    # concatenate all of the dataframes into a single data frame
    sam_results_df = pd.concat(sam_results_list)
    # reindex the dataframe
    sam_results_df.reset_index(drop = True, inplace = True)     
    
    # set the join clauses depending on the technology
    if tech == 'wind':
        inputs_dict['resource_join_clause'] = """a.i = b.i
                                            AND a.j = b.j
                                            AND a.cf_bin = b.cf_bin
                                            AND a.turbine_height_m = b.turbine_height_m
                                            AND a.turbine_id = b.turbine_id """
    elif tech == 'solar':
        inputs_dict['resource_join_clause'] = """a.solar_re_9809_gid = b.solar_re_9809_gid
                                            AND a.tilt = b.tilt
                                            AND a.azimuth = b.azimuth """
    
      
    #==============================================================================
    #     CREATE TABLE TO HOLD RESULTS
    #==============================================================================
    sql = """DROP TABLE IF EXISTS %(schema)s.utilityrate3_results_%(tech)s;
             CREATE UNLOGGED TABLE %(schema)s.utilityrate3_results_%(tech)s
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
    cur.copy_expert('COPY %(schema)s.utilityrate3_results_%(tech)s FROM STDOUT WITH CSV' % inputs_dict, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()    
    s.close()
    
    # add primary key constraint to uid field
    sql = """ALTER TABLE %(schema)s.utilityrate3_results_%(tech)s ADD PRIMARY KEY (uid);""" % inputs_dict
    cur.execute(sql)
    con.commit()
    
    
    #==============================================================================
    #     APPEND THE RESULTS TO CUSTOMER BINS
    #==============================================================================
    for sector_abbr, sector in sectors.iteritems():
        inputs_dict['sector_abbr'] = sector_abbr
        inputs_dict['sector'] = sector
        sql = """   DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_elec_costs_%(tech)s;
                    CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_elec_costs_%(tech)s AS
                    
                    SELECT a.county_id, a.bin_id, a.year, 
                        c.elec_cost_with_system_year1 as first_year_bill_with_system, 
                        c.elec_cost_without_system_year1 as first_year_bill_without_system,
                        c.excess_generation_percent as excess_generation_percent
                    FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year_%(tech)s a
                
                    LEFT JOIN %(schema)s.unique_rate_gen_load_combinations_%(tech)s b
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
                        
                    LEFT JOIN %(schema)s.utilityrate3_results_%(tech)s c
                        ON b.uid = c.uid
            
        """ % inputs_dict
        
        cur.execute(sql)
        con.commit()
    
        # add indices on: county_id, bin_id, year
        sql = """CREATE INDEX pt_%(sector_abbr)s_elec_costs_%(tech)s_join_fields_btree 
                 ON %(schema)s.pt_%(sector_abbr)s_elec_costs_%(tech)s
                 USING BTREE(county_id,bin_id);
             
                 CREATE INDEX pt_%(sector_abbr)s_elec_costs_%(tech)s_year_btree 
                 ON %(schema)s.pt_%(sector_abbr)s_elec_costs_%(tech)s
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


def get_technologies(con, schema):
    
    
    sql = '''with a as
            (
                	select unnest(array['wind', 'solar']) as tech, 
                         unnest(array[run_wind, run_solar]) as enabled
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
    

def get_system_degradation(con, schema):
    '''Return the annual system degradation rate as float.
        '''    
        

    sql = '''SELECT ann_system_degradation, tech
         FROM %s.input_performance_annual_system_degradation;''' % schema
    ann_system_degradation = pd.read_sql(sql, con)

    return ann_system_degradation    
    
        
def get_depreciation_schedule(con, schema, macrs = True):
    ''' Pull depreciation schedule from dB
    
        IN: type - string - [all, macrs, standard] 
        OUT: df  - pd dataframe - year, depreciation schedule:

    '''
    inputs = locals().copy()    
    
    if macrs == True:
        inputs['field'] = 'macrs'
    else:
        inputs['field'] = 'standard'
        
    sql = '''SELECT tech, array_agg(%(field)s ORDER BY year ASC)::DOUBLE PRECISION[] as deprec
            FROM %(schema)s.input_finances_depreciation_schedule
            GROUP BY tech
            ORDER BY tech;''' % inputs
    df = pd.read_sql(sql, con)
    
    return df
    
def get_scenario_options(cur, schema):
    ''' Pull scenario options from dB
    
    '''
    sql = '''SELECT * 
             FROM %s.input_main_scenario_options;''' % schema
    cur.execute(sql)
    results = cur.fetchall()[0]
    return results


def cleanup_incentives(df, default_exp_yr):
    
    # add in columns that may be missing
    for col in ['increment_4_capacity_kw','increment_4_rebate_dlrs_kw',
                'pbi_fit_max_size_for_dlrs_calc_kw','tax_credit_dlrs_kw',
                'pbi_fit_min_output_kwh_yr','increment_3_rebate_dlrs_kw',
                'increment_4_rebate_dlrs_kw']:
        if col not in df.columns:
            df[col] = np.nan
    
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
    exp_date = datetime.date(default_exp_yr, 12, 31) # note: this was formerly set to 1/1/16 for ITC, and 12/31/16 for all other incentives
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
    df.loc[:, 'ptc_end_date'] = df.ptc_end_date.astype('O').fillna(exp_date)
    df.loc[:, 'pbi_fit_end_date'] = df.pbi_fit_end_date.astype('O').fillna(exp_date) # Assign expiry if no date    
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
    
    return df


def get_bass_params(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT *
             FROM %(schema)s.input_solar_bass_params
             UNION ALL
             SELECT *
             FROM %(schema)s.input_wind_bass_params;""" % inputs
    
    bass_df = pd.read_sql(sql, con, coerce_float = True)
    
    return bass_df

def get_itc_incentives(con, schema):
    
    inputs = locals().copy()
    
    sql = """SELECT year, lower(sector) as sector, itc_fraction
             FROM %(schema)s.input_main_itc_options;""" % inputs
    itc_options = pd.read_sql(sql, con) 
    
    return itc_options

def get_dsire_incentives(cur, con, schema, techs, sectors, pg_conn_string, default_exp_yr = 2016):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()
    
    sql_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        for tech in techs:
            inputs['tech'] = tech
            sql =   """SELECT a.incentive_array_id, c.*, '%(tech)s'::TEXT as tech
                        FROM 
                        (SELECT DISTINCT incentive_array_id as incentive_array_id
                        	FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year_%(tech)s
                        	WHERE year = 2014) a
                        LEFT JOIN diffusion_%(tech)s.dsire_incentives_simplified_lkup_%(sector_abbr)s b
                            ON a.incentive_array_id = b.incentive_array_id
                        LEFT JOIN diffusion_%(tech)s.incentives c
                            ON b.incentives_uid = c.uid
                        WHERE c.sector_abbr = '%(sector_abbr)s' AND c.incentive_id <> 124 
                    """ % inputs
            sql_list.append(sql)
    
    sql = ' UNION ALL '.join(sql_list)
    # get the data
    df = pd.read_sql(sql, con, coerce_float = True)
    # clean it up
    df = cleanup_incentives(df, default_exp_yr)

    return df


def get_srecs(cur, con, schema, techs, pg_conn_string, default_exp_yr):
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
    df = cleanup_incentives(df, default_exp_yr)
    
    return df


def get_initial_market_shares(cur, con, techs, sectors, schema, initial_market_calibrate_mode, econ_df):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()     
    # add the starting capacity table to the inputs dict
    inputs['cap_table'] = 'starting_capacities_mw_2012_q4_us'
    
    sql_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        inputs['sector'] = sector
        for tech in techs:
            inputs['tech'] = tech
            if tech == 'wind':
                inputs['cost_table_join'] = """LEFT JOIN %(schema)s.turbine_costs_per_size_and_year b
                                             ON a.turbine_size_kw = b.turbine_size_kw
                                             AND a.year = b.year
                                             AND a.turbine_height_m = b.turbine_height_m""" % inputs
            elif tech == 'solar':
                inputs['cost_table_join'] = """LEFT JOIN %(schema)s.input_solar_cost_projections_to_model b
                                             ON a.year = b.year
                                             AND b.sector = '%(sector_abbr)s'""" % inputs
            
            
            if initial_market_calibrate_mode == False:
                sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_initial_market_shares_%(tech)s;
                         CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_initial_market_shares_%(tech)s AS
                         WITH a as
                         (
            			SELECT a.county_id, a.bin_id, a.state_abbr,
            				CASE  WHEN a.system_size_kw = 0 then 0
            					ELSE a.customers_in_bin
            				END AS customers_in_bin, 
                              COALESCE(b.installed_costs_dollars_per_kw * a.cap_cost_multiplier, 0) as installed_costs_dollars_per_kw
            			FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year_%(tech)s a
                        %(cost_table_join)s
            			WHERE a.year = 2014			
                         ),
                         b as
                         (
                            	SELECT a.county_id, a.bin_id,
                            		(a.customers_in_bin/sum(a.customers_in_bin) OVER (PARTITION BY a.state_abbr)) * b.systems_count_%(sector)s AS initial_number_of_adopters,
                            		(a.customers_in_bin/sum(a.customers_in_bin) OVER (PARTITION BY a.state_abbr)) * b.capacity_mw_%(sector)s AS initial_capacity_mw,
                            		a.customers_in_bin,
                                     a.installed_costs_dollars_per_kw
                            	FROM a
                            	LEFT JOIN diffusion_%(tech)s.starting_capacities_mw_2012_q4_us b
                            		ON a.state_abbr = b.state_abbr
                        ) 
                        SELECT b.county_id, b.bin_id,
                             ROUND(COALESCE(b.initial_number_of_adopters, 0)::NUMERIC, 6) as initial_number_of_adopters,
                             1000 * ROUND(COALESCE(b.initial_capacity_mw, 0)::NUMERIC, 6) as initial_capacity_kw,
                    	     CASE  WHEN customers_in_bin = 0 then 0
                                   ELSE ROUND(COALESCE(b.initial_number_of_adopters/b.customers_in_bin, 0)::NUMERIC, 6) 
                             END AS initial_market_share,
                             b.installed_costs_dollars_per_kw
                        FROM b;""" % inputs
                cur.execute(sql)
                con.commit()    
            else:
                # write the econ df to postgres
                sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_first_year_economics_%(tech)s;
                         CREATE TABLE %(schema)s.pt_%(sector_abbr)s_first_year_economics_%(tech)s
                         (
                            county_id integer,
                            bin_id    integer,
                            state_abbr varchar(2),
                            max_market_share numeric,
                            npv4 numeric,
                            customers_in_bin numeric,
                            system_size_kw numeric,
                            installed_costs_dollars_per_kw numeric
                         );""" % inputs
                cur.execute(sql)
                con.commit()
                
                s = StringIO()
                fields = ['county_id', 'bin_id', 'state_abbr', 'max_market_share', 'npv4', 'customers_in_bin', 'system_size_kw', 'installed_costs_dollars_per_kw']
                out_df = econ_df.loc[(econ_df['tech'] == tech) & (econ_df['sector_abbr'] == sector_abbr), fields]                
                out_df.to_csv(s, index = False, header = False)
                # seek back to the beginning of the stringIO file
                s.seek(0)
                # copy the data from the stringio file to the postgres table
                cur.copy_expert('COPY %(schema)s.pt_%(sector_abbr)s_first_year_economics_%(tech)s FROM STDOUT WITH CSV' % inputs, s)
                # commit the additions and close the stringio file (clears memory)
                con.commit()    
                s.close()   
                
                # add index on state_abbr
                sql = """CREATE INDEX pt_%(sector_abbr)s_first_year_economics_%(tech)s_state_abbr_btree
                         ON %(schema)s.pt_%(sector_abbr)s_first_year_economics_%(tech)s
                         USING BTREE(state_abbr);
                      """ % inputs
                cur.execute(sql)
                con.commit()
                
                # add primary key
                sql = """ALTER TABLE %(schema)s.pt_%(sector_abbr)s_first_year_economics_%(tech)s
                         ADD PRIMARY KEY(county_id, bin_id);""" % inputs
                cur.execute(sql)
                con.commit()                         
                
                # now disaggregate the data to bins relative to one of the 2014 economic factors
                weight_factor = 'max_market_share'
                # or
                # weight_factor = 'npv4'
                inputs['weight_factor'] = weight_factor
                
                sql = """DROP TABLE IF EXISTS %(schema)s.pt_%(sector_abbr)s_initial_market_shares_%(tech)s;
                         CREATE UNLOGGED TABLE %(schema)s.pt_%(sector_abbr)s_initial_market_shares_%(tech)s AS
                         WITH b as
                         (
                            	SELECT a.county_id, a.bin_id,
                            		((a.customers_in_bin * a.%(weight_factor)s)/sum(a.customers_in_bin * a.%(weight_factor)s) OVER (PARTITION BY a.state_abbr)) * b.systems_count_%(sector)s AS initial_number_of_adopters,
                            		((a.customers_in_bin * a.%(weight_factor)s * a.system_size_kw)/sum(a.customers_in_bin * a.%(weight_factor)s * a.system_size_kw) OVER (PARTITION BY a.state_abbr)) * b.capacity_mw_%(sector)s AS initial_capacity_mw,
                            		a.customers_in_bin,
                                     a.installed_costs_dollars_per_kw
                            	FROM %(schema)s.pt_%(sector_abbr)s_first_year_economics_%(tech)s a
                            	LEFT JOIN diffusion_%(tech)s.starting_capacities_mw_2012_q4_us b
                            		ON a.state_abbr = b.state_abbr
                              WHERE a.customers_in_bin > 0
                                AND a.system_size_kw > 0
                                AND a.%(weight_factor)s > 0
                        ) 
                        SELECT a.county_id, a.bin_id,
                             ROUND(COALESCE(b.initial_number_of_adopters, 0)::NUMERIC, 6) AS initial_number_of_adopters,
                             1000 * ROUND(COALESCE(b.initial_capacity_mw, 0)::NUMERIC, 6) AS initial_capacity_kw,
                             ROUND(COALESCE(b.initial_number_of_adopters/a.customers_in_bin, 0)::NUMERIC, 6) AS initial_market_share,
                             a.installed_costs_dollars_per_kw
                        FROM %(schema)s.pt_%(sector_abbr)s_first_year_economics_%(tech)s a
                        LEFT JOIN b
                        ON a.county_id = b.county_id
                            AND a.bin_id = b.bin_id;""" % inputs               
                cur.execute(sql)
                con.commit()    
        
        
            # regardless of creation method, add indices
            sql = """CREATE INDEX pt_%(sector_abbr)s_initial_market_shares_%(tech)s_join_fields_btree 
                     ON %(schema)s.pt_%(sector_abbr)s_initial_market_shares_%(tech)s 
                     USING BTREE(county_id,bin_id);""" % inputs
            cur.execute(sql)
            con.commit()

            # write sql to pull the data
            sql = """SELECT county_id, bin_id, 
                            initial_market_share AS market_share_last_year,
                            initial_number_of_adopters AS number_of_adopters_last_year,
                            initial_capacity_kw AS installed_capacity_last_year,
                            installed_costs_dollars_per_kw * initial_capacity_kw as market_value_last_year,
                            '%(tech)s'::TEXT as tech, '%(sector_abbr)s'::Character Varying(3) as sector_abbr
                    FROM %(schema)s.pt_%(sector_abbr)s_initial_market_shares_%(tech)s""" % inputs
            sql_list.append(sql)
        
    sql = ' UNION ALL '.join(sql_list)
    df = pd.read_sql(sql, con)
    
    
    return df  


def get_market_last_year(cur, con, is_first_year, techs, sectors, schema, initial_market_calibrate_mode, econ_df):

    inputs = locals().copy()
    
    
    if is_first_year == True:
        last_year_df = get_initial_market_shares(cur, con, techs, sectors, schema, initial_market_calibrate_mode, econ_df)
    else:
        sql = """SELECT *
                FROM %(schema)s.output_market_last_year;""" % inputs
        last_year_df = pd.read_sql(sql, con, coerce_float = False)
    
    return last_year_df
    

def write_last_year(con, cur, market_last_year, schema):
    
    inputs = locals().copy()    
    
    inputs['out_table'] = '%(schema)s.output_market_last_year'  % inputs
    
    sql = """DELETE FROM %(out_table)s;"""  % inputs
    cur.execute(sql)
    con.commit()

    # open an in memory stringIO file (like an in memory csv)
    s = StringIO()
    # write the data to the stringIO
    out_cols = ['county_id', 'bin_id', 'market_share_last_year', 'max_market_share_last_year', 'number_of_adopters_last_year', 'installed_capacity_last_year', 'market_value_last_year', 'tech', 'sector_abbr']
    market_last_year[out_cols].to_csv(s, index = False, header = False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    cur.copy_expert('COPY %(out_table)s FROM STDOUT WITH CSV' % inputs, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()    
    s.close()


def get_main_dataframe(con, sectors, schema, year, techs):
    ''' Pull main pre-processed dataframe from dB
    
        IN: con - pg con object - connection object
        OUT: df  - pd dataframe - pre-processed resource,bins, rates, etc. for all years:

    '''
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()     
    

    
    sql_list = []
    for tech in techs:
        inputs['tech'] = tech
        
        if tech == 'wind':
            inputs['add_cols'] = """NULL::INTEGER as tilt, NULL::TEXT as azimuth, NULL::INTEGER as available_roof_sqft, 
                                    0::NUMERIC as inverter_cost_dollars_per_kw, 0::INTEGER as inverter_lifetime_yrs, 
                                    'wind'::text as tech"""
            cost_table_join = """LEFT JOIN %(schema)s.turbine_costs_per_size_and_year d
                                             ON a.turbine_size_kw = d.turbine_size_kw
                                             AND a.year = d.year
                                             AND a.turbine_height_m = d.turbine_height_m"""
        elif tech == 'solar':
            inputs['add_cols'] = """a.tilt, a.azimuth, a.available_roof_sqft, 
                                    d.inverter_cost_dollars_per_kw * a.cap_cost_multiplier as inverter_cost_dollars_per_kw, a.inverter_lifetime_yrs, 
                                    'solar'::TEXT as tech"""       
            cost_table_join = """LEFT JOIN %(schema)s.input_solar_cost_projections_to_model d
                                             ON a.year = d.year
                                             AND d.sector = '%(sector_abbr)s'"""
        for sector_abbr, sector in sectors.iteritems():
            inputs['sector_abbr'] = sector_abbr
            inputs['cost_table_join'] = cost_table_join % inputs
            sql = """SELECT a.micro_id, 
                            a.county_id, 
                            a.bin_id, 
                            a.year, 
                            a.state_abbr, 
                            a.census_division_abbr, 
                            a.utility_type, 
                            a.pca_reg, 
                            a.reeds_reg, 
                            a.incentive_array_id, 
                            f.carbon_intensity_t_per_kwh * 100 * e.carbon_dollars_per_ton as carbon_price_cents_per_kwh, 
                            -- use coalesce for wind agents with no allowable system
                            COALESCE(d.fixed_om_dollars_per_kw_per_yr, 0) as fixed_om_dollars_per_kw_per_yr, 
                            COALESCE(d.variable_om_dollars_per_kwh, 0) as variable_om_dollars_per_kwh, 
                            COALESCE(d.installed_costs_dollars_per_kw * a.cap_cost_multiplier, 0) as installed_costs_dollars_per_kw, 
                            a.customers_in_bin, 
                            a.load_kwh_per_customer_in_bin, 
                            a.system_size_kw, 
                            a.aep,  
                            a.owner_occupancy_status,
                            %(add_cols)s,
                            b.first_year_bill_with_system, 
                            b.first_year_bill_without_system, 
                            b.excess_generation_percent, 
                            c.leasing_allowed,
                            '%(sector_abbr)s'::CHARACTER VARYING(3) as sector_abbr
                    FROM %(schema)s.pt_%(sector_abbr)s_best_option_each_year_%(tech)s a
                    LEFT JOIN %(schema)s.pt_%(sector_abbr)s_elec_costs_%(tech)s b
                            ON a.county_id = b.county_id
                            AND a.bin_id = b.bin_id
                            AND a.year = b.year
                    -- LEASING AVAILABILITY
                    LEFT JOIN %(schema)s.input_%(tech)s_leasing_availability c
                        ON a.state_abbr = c.state_abbr
                        AND a.year = c.year               
                    -- COSTS
                    %(cost_table_join)s
                    -- CARBON COSTS
                    LEFT JOIN %(schema)s.input_main_market_projections e
                        ON a.year = e.year
                    LEFT JOIN %(schema)s.carbon_intensities_to_model f
                        ON a.state_abbr = f.state_abbr
                    WHERE a.year = %(year)s""" % inputs
            sql_list.append(sql)
            
    sql = ' UNION ALL '.join(sql_list)
    
    df = pd.read_sql(sql, con, coerce_float = False)

    return df
    

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


def get_manual_incentive_options(con, schema):
    
    inputs = locals().copy()
    
    sql = '''SELECT *
             FROM %(schema)s.input_incentive_options;''' % inputs
    df = pd.read_sql(sql, con)
    
    return df            

    
def get_manual_incentives(con, schema):
    ''' Pull manual incentives from input sheet
    
        IN: con - pg con object - connection object
        OUT: inc - pd dataframe - dataframe of manual incentives
    '''
    inputs = locals().copy()    
    
    sql = '''SELECT region as state_abbr, 
                    CASE WHEN lower(sector) = 'residential' THEN 'res'::VARCHAR(3)
                         WHEN lower(sector) = 'commercial' THEN 'com'::VARCHAR(3)
                         WHEN lower(sector) = 'industrial' THEN 'ind'::VARCHAR(3)
                    END as sector_abbr, 
                    type, incentive, cap, expire, incentives_c_per_kwh, 
                    no_years, dol_per_kw, utility_type, 
                    'solar'::TEXT as tech
             FROM %(schema)s.input_solar_incentives
            UNION ALL
            SELECT region as state_abbr, 
                    CASE WHEN lower(sector) = 'residential' THEN 'res'::VARCHAR(3)
                         WHEN lower(sector) = 'commercial' THEN 'com'::VARCHAR(3)
                         WHEN lower(sector) = 'industrial' THEN 'ind'::VARCHAR(3)
                    END as sector_abbr, 
                    type, incentive, cap, expire, incentives_c_per_kwh, 
                    no_years, dol_per_kw, utility_type,
                    'wind'::TEXT as tech
            FROM %(schema)s.input_wind_incentives;''' % inputs
    df = pd.read_sql(sql, con)

    return df
 
def calc_manual_incentives(df, cur_year, inc):
    ''' Calculate the value in first year and length for incentives manually 
    entered in input sheet. 

        IN: df - pandas DataFrame - main dataframe
            cur - SQL cursor 
                        
        OUT: manual_incentives_value - pandas DataFrame - value of rebate, tax incentives, and PBI
    '''
    # Join manual incentives with main df   
    df = pd.merge(df, inc, how = 'left', on = ['state_abbr','sector_abbr','utility_type', 'tech'])
        
    # Calculate value of incentive and rebate, and value and length of PBI
    df['value_of_tax_credit_or_deduction'] = df['incentive'] * df['installed_costs_dollars_per_kw'] * df['system_size_kw'] * (cur_year <= df['expire'])
    df['value_of_tax_credit_or_deduction'] = df['value_of_tax_credit_or_deduction'].astype(float)
    # set to zero if cur_year < incentive_start_year    
    df['value_of_tax_credit_or_deduction'] = np.where(cur_year >= df['incentive_start_year'], df['value_of_tax_credit_or_deduction'], 0)
    
    df['value_of_pbi_fit'] = 0.01 * df['incentives_c_per_kwh'] * df['aep'] * (cur_year <= df['expire']) # First year value  
    # set to zero if cur_year < incentive_start_year    
    df['value_of_pbi_fit'] = np.where(cur_year >= df['incentive_start_year'], df['value_of_pbi_fit'], 0)

    df['value_of_rebate'] = np.minimum(1000 * df['dol_per_kw'] * df['system_size_kw'] * (cur_year <= df['expire']), df['system_size_kw'])
    # set to zero if cur_year < incentive_start_year        
    df['value_of_rebate'] = np.where(cur_year >= df['incentive_start_year'], df['value_of_rebate'], 0)

    df['pbi_fit_length'] = df['no_years']
    # set to zero if cur_year < incentive_start_year        
    df['pbi_fit_length'] = np.where(cur_year >= df['incentive_start_year'], df['pbi_fit_length'], 0)

    # These values are not used, but necessary for cashflow calculations later
    # Convert dtype to float s.t. columns are included in groupby calculation.
    df['value_of_increment'] = 0
    df['value_of_ptc'] = 0
    df['ptc_length'] = 0
    
    df['value_of_tax_credit_or_deduction'] = df['value_of_tax_credit_or_deduction'].astype(float)
    df['value_of_pbi_fit'] = df['value_of_pbi_fit'].astype(float)
    df['value_of_rebate'] = df['value_of_rebate'].astype(float)
    df['pbi_fit_length'] = df['pbi_fit_length'].astype(float)
    '''
    Because a system could potentially qualify for several incentives, the left 
    join above could join on multiple rows. Thus, groupby by county_id & bin_id 
    to sum over incentives and condense back to unique county_id/bin_id/business_model combinations
    '''
    
    if df.shape[0] > 0:
        value_of_incentives = df[['tech', 'sector_abbr', 'county_id', 'bin_id', 'business_model','value_of_increment', 'value_of_pbi_fit', 'value_of_ptc', 'pbi_fit_length', 'ptc_length', 'value_of_rebate', 'value_of_tax_credit_or_deduction']].groupby(['tech', 'sector_abbr', 'county_id','bin_id','business_model']).sum().reset_index() 
    else:
        value_of_incentives = df[['tech', 'sector_abbr', 'county_id', 'bin_id', 'business_model','value_of_increment', 'value_of_pbi_fit', 'value_of_ptc', 'pbi_fit_length', 'ptc_length', 'value_of_rebate', 'value_of_tax_credit_or_deduction']]
    
    return value_of_incentives


def calc_dsire_incentives(df, dsire_incentives, srecs, cur_year, default_exp_yr = 2016, assumed_duration = 10):
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
    
    dsire_df = pd.merge(df, dsire_incentives, how = 'left', on = ['incentive_array_id', 'sector_abbr', 'tech'])    
    srecs_df = pd.merge(df, srecs, how = 'left', on = ['state_abbr', 'sector_abbr', 'tech'])

    # combine sr and inc
    inc = pd.concat([dsire_df, srecs_df], axis = 0, ignore_index = True) 

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
    inc['pbi_fit_cap'] = np.where(inc['system_size_kw'] < inc.pbi_fit_min_size_kw, 0, inc['system_size_kw'])
    inc.loc[:, 'pbi_fit_cap'] = np.where(inc['pbi_fit_cap'] > inc.pbi_fit_max_size_kw, inc.pbi_fit_max_size_kw, inc['pbi_fit_cap'])
    inc['pbi_fit_aep'] = np.where(inc['aep'] < inc.pbi_fit_min_output_kwh_yr, 0, inc['aep'])
    
    # If exists pbi_fit_kwh > 0 but no duration, assume duration
    inc.loc[(inc.pbi_fit_dlrs_kwh > 0) & inc.pbi_fit_duration_years.isnull(), 'pbi_fit_duration_years']  = assumed_duration
    inc['value_of_pbi_fit'] = inc['pbi_fit_still_exists'] * np.minimum(inc.pbi_fit_dlrs_kwh, inc.max_dlrs_yr) * inc['pbi_fit_aep']
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

    # check whether the credits are still active (this can be applied universally because DSIRE does not provide specific info 
    # about expirations for each tax credit or deduction). 
    #Assume that expiration date is inclusive e.g. consumer receives incentive in 2016 if expiration date of 2016 (or greater)
    if datetime.date(cur_year, 1, 1) >= datetime.date(default_exp_yr, 12, 31):
        inc['value_of_rebate'] = 0.0
    else:
        inc['rebate_cap'] = np.where(inc['system_size_kw'] < inc.rebate_min_size_kw, 0, inc['system_size_kw'])
        inc.loc[:, 'rebate_cap'] = np.where(inc['rebate_cap'] > inc.rebate_max_size_kw, inc.rebate_max_size_kw, inc['rebate_cap'])
        inc['value_of_rebate'] = inc.rebate_dlrs_kw * inc['rebate_cap']
        inc.loc[:, 'value_of_rebate'] = np.minimum(inc.rebate_max_dlrs, inc['value_of_rebate'])
        inc.loc[:, 'value_of_rebate'] = np.minimum(inc.rebate_pcnt_cost_max * inc['ic'], inc['value_of_rebate'])
        inc.loc[:, 'value_of_rebate'] = inc.value_of_rebate.fillna(0)    

    # 5. # Calculate Value of Tax Credit
    # Assume able to fully monetize tax credits
    
    # check whether the credits are still active (this can be applied universally because DSIRE does not provide specific info 
    # about expirations for each tax credit or deduction). 
    #Assume that expiration date is inclusive e.g. consumer receives incentive in 2016 if expiration date of 2016 (or greater)
    if datetime.date(cur_year, 1, 1) >= datetime.date(default_exp_yr, 12, 31):
        inc['value_of_tax_credit_or_deduction'] = 0.0
    else:
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


def excess_generation_calcs(row, gross_fit_mode = False):
    ''' Function to calculate percent of excess generation given 8760-lists of 
    consumption and generation. Currently function is configured to work only with
    the rate_input_df to avoid pulling generation and consumption profiles
    '''

    con = 'consumption_hourly'
    gen = 'generation_hourly'

    annual_generation = sum(row[gen])
    excess_gen_hourly = np.maximum(row[gen] - row[con],0)
    excess_gen_annual = np.sum(excess_gen_hourly)
    
    # calculate the excess generation percent
    if annual_generation == 0:
        row['excess_generation_percent'] = 0
    else:
        # Determine the annual amount of generation (kWh) that exceeds consumption,
        # and must be sold to the grid to receive value
        row['excess_generation_percent'] = excess_gen_annual / annual_generation # unitless (kWh/kWh)

    if gross_fit_mode == True:
        # under gross fit, we will simply feed all inputs into SAM as-is and let the utilityrate3 module
        # handle all calculations with no modifications
    
        # no excess generation will be credited at the flat sell rate (outside of SAM)
        row['flat_rate_excess_gen_kwh'] = 0
        
        # set ur_enable_net_metering equal to apply_net_metering
        row['ur_enable_net_metering'] = row['apply_net_metering']
        
    else: # otherwise, we will make some modifications so that we can apply net fit for non-nem cases
        
        if row['apply_net_metering'] == True: 
            
            # there will be zero excess generation credited at the flat sell rate (it will all be net metered)
            row['flat_rate_excess_gen_kwh'] = 0

            # set to run net metering in SAM
            row['ur_enable_net_metering'] = True
            
        else: 
            # set the data up to be able to run a net fit mode
        
            # all excess generation will be credited at the flat sell rate
            row['flat_rate_excess_gen_kwh'] = excess_gen_annual
            
            # calculate the non-excess portion of hourly generation and re-assign it to the gen column
            offset_generation = row[gen] - excess_gen_hourly
            row[gen] = offset_generation
            
            # set to run net metering in SAM 
            # (note: since we've modified gen to drop any excess generation, this will simply account for offset consumption)
            row['ur_enable_net_metering'] = True

    row['net_fit_credit_dollars'] = row['flat_rate_excess_gen_kwh'] * row['ur_flat_sell_rate']

    return row



def excess_generation_vectorized(df, gross_fit_mode = False):
    ''' Function to calculate percent of excess generation given 8760-lists of 
    consumption and generation. Currently function is configured to work only with
    the rate_input_df to avoid pulling generation and consumption profiles
    '''

    con = 'consumption_hourly'
    gen = 'generation_hourly'

    gen_array = np.array(list(df[gen]))
    excess_gen_hourly = np.maximum(gen_array - np.array(list(df[con])), 0)
    annual_generation = np.sum(gen_array, 1)
    excess_gen_annual = np.sum(excess_gen_hourly, 1)
    offset_generation = (gen_array - excess_gen_hourly).tolist()

    with np.errstate(invalid = 'ignore'):
        df['excess_generation_percent'] = np.where(annual_generation == 0, 0, excess_gen_annual/annual_generation)
    
        
    if gross_fit_mode == True:
        # under gross fit, we will simply feed all inputs into SAM as-is and let the utilityrate3 module
        # handle all calculations with no modifications
    
        # no excess generation will be credited at the flat sell rate (outside of SAM)
        df['flat_rate_excess_gen_kwh'] = 0
        
        # set ur_enable_net_metering equal to apply_net_metering
        df['ur_enable_net_metering'] = df['apply_net_metering']
        
    else: # otherwise, we will make some modifications so that we can apply net fit for non-nem cases
        
        
        df['flat_rate_excess_gen_kwh'] = np.where(df['apply_net_metering'] == True, 0, excess_gen_annual)
        df[gen] = np.where(df['apply_net_metering'] == True, df[gen], pd.Series(offset_generation)) 
        df['ur_enable_net_metering'] = True

    df['net_fit_credit_dollars'] = df['flat_rate_excess_gen_kwh'] * df['ur_flat_sell_rate']

    return df

def calc_value_of_itc(df, itc_options, year):
    
    # add the sector_abbr column
    itc_options['sector_abbr'] = itc_options['sector'].str.lower()
    itc_options['sector_abbr'] = itc_options['sector_abbr'].str[:3] 
    
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
    
    # merge to df
    df = pd.merge(df, itc_all, how = 'left', on = ['sector_abbr', 'year', 'business_model'])
        
    # Calculate the value of ITC
    df['value_of_itc'] = df['installed_costs_dollars_per_kw'] * df['system_size_kw'] * df['itc_fraction'] #'ic' not in the df at this point
    df = df.drop(['sector', 'itc_fraction'], axis = 1)
    
    return df

    
