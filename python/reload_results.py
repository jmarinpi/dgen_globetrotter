# -*- coding: utf-8 -*-
"""
Created on Wed Jun 11 08:59:39 2014

@author: mgleason
"""

import psycopg2 as pg
import data_functions as datfunc
import config as cfg
import gzip
import os

####################################################################################################
# input arguments
#source_csv = '/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/runs/results_20140610_175211/co_res_demo/outputs.csv.gz'
source_csv = '/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/runs/results_20140611_152914/co_res_demo/outputs.csv.gz'
scenario_options_csv = '/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/runs/results_20140611_152914/co_res_demo/scenario_options_summary.csv'
out_path = '/Volumes/Staff/mgleason/DG_Wind/diffusion_repo/runs/test2'
####################################################################################################

if not os.path.exists(source_csv):
    raise Exception('Source csv does not exist: %s' % source_csv)

# connect to postgres
con, cur = datfunc.make_con(cfg.pg_conn_string)

# drop and recreate table
print "Recreating empty wind_ds.outputs_all table"
sql = '''DROP TABLE wind_ds.outputs_all;
        CREATE TABLE wind_ds.outputs_all
        (
          sector text,
          gid integer,
          year integer,
          customer_expec_elec_rates numeric,
          ownership_model text,
          loan_term_yrs integer,
          loan_rate numeric,
          down_payment numeric,
          discount_rate numeric,
          tax_rate numeric,
          length_of_irr_analysis_yrs integer,
          market_share_last_year numeric,
          number_of_adopters_last_year numeric,
          installed_capacity_last_year numeric,
          market_value_last_year numeric,
          value_of_increment numeric,
          value_of_pbi_fit numeric,
          value_of_ptc numeric,
          pbi_fit_length numeric,
          ptc_length integer,
          value_of_rebate numeric,
          value_of_tax_credit_or_deduction numeric,
          cap numeric,
          ic numeric,
          aep numeric,
          payback_period numeric,
          lcoe numeric,
          payback_key integer,
          max_market_share numeric,
          diffusion_market_share numeric,
          new_market_share numeric,
          new_adopters numeric,
          new_capacity numeric,
          new_market_value numeric,
          market_share numeric,
          number_of_adopters numeric,
          installed_capacity numeric,
          market_value numeric,
          county_id integer,
          state_abbr character varying(2),
          census_division_abbr text,
          utility_type character varying(9),
          census_region text,
          row_number bigint,
          max_height integer,
          elec_rate_cents_per_kwh numeric,
          carbon_price_cents_per_kwh numeric,
          cap_cost_multiplier numeric,
          fixed_om_dollars_per_kw_per_yr numeric,
          variable_om_dollars_per_kwh numeric,
          installed_costs_dollars_per_kw numeric,
          ann_cons_kwh numeric,
          prob numeric,
          weight numeric,
          customers_in_bin numeric,
          initial_customers_in_bin numeric,
          load_kwh_in_bin numeric,
          initial_load_kwh_in_bin numeric,
          load_kwh_per_customer_in_bin numeric,
          nem_system_limit_kw double precision,
          excess_generation_factor numeric,
          i integer,
          j integer,
          cf_bin integer,
          aep_scale_factor numeric,
          derate_factor numeric,
          naep numeric,
          nameplate_capacity_kw numeric,
          power_curve_id integer,
          turbine_height_m integer,
          scoe double precision,
          initial_market_share numeric,
          initial_number_of_adopters numeric,
          initial_capacity_mw numeric
        );'''
cur.execute(sql)
con.commit()

print "Copying csv data to postgres"
# open the source csv.gz file
f = gzip.open(source_csv, 'r')
# copy the data to the table
cur.copy_expert('COPY wind_ds.outputs_all FROM STDIN WITH CSV HEADER;',f)
# commit changes
con.commit()
# close the source csv.gz
f.close()

# clear existing scenario options table in postgres
sql = 'DELETE FROM wind_ds.scenario_options;'
cur.execute(sql)
con.commit()

# load the scenario options from the csv file
# open the csv
f2 = open(scenario_options_csv, 'r')
# copy the data to the table
cur.copy_expert('COPY wind_ds.scenario_options FROM STDIN WITH CSV HEADER;',f2)
# commit changes
con.commit()
# close the source csv.gz
f2.close()

# get the scenario name
sql = 'SELECT scenario_name FROM wind_ds.scenario_options;'
cur.execute(sql)
scenario_name = cur.fetchone()['scenario_name']

# create output html report
datfunc.create_scenario_report(scenario_name, out_path, cur, con, cfg.Rscript_path)

print "Process completed successfully"
