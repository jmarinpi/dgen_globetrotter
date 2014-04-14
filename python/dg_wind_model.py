"""
Distributed Wind Diffusion Model
National Renewable Energy Lab

@author: bsigrin
"""

# 1. # Initialize Model
import time
import os

t0 = time.time()
print 'Initiating model at %s' %time.ctime()

# 2. # Import modules and global vars
print 'Importing Modules'
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg
import psycopg2.extras as pgx
import numpy as np
import scipy as sp
import matplotlib as mpl
import collections
import diffusion_functions as diffunc
import financial_functions as finfunc
import data_functions as datfunc
import DG_Wind_NamedRange_xl2pg as loadXL
import subprocess
import datetime
# load in a bunch of the configuration variables as global vars
from config import *

# 3. Connect to Postgres and configure connection
# create connection to Postgres Database
# (to edit login information, edit config.py)
con, cur = datfunc.make_con(pg_conn_string)
# register access to hstore in postgres
pgx.register_hstore(con)
# configure pandas display options
pd.set_option('max_columns', 9999)
pd.set_option('max_rows',10)



# 4. Load Input excel spreadsheet to Postgres
print 'Loading input data from Input Scenario Worksheet'
try:
    loadXL.main(input_xls, con, verbose = False)
except loadXL.ExcelError, e:
    print 'Loading failed with the following error: %s' % e
    print 'Model aborted'
    sys.exit(-1)


# 5. Read in scenario option variables 

scenario_opts = datfunc.get_scenario_options(cur) 
exclusions = datfunc.get_exclusions(cur) # get exclusions
load_growth_scenario = scenario_opts['load_growth_scenario'] # get financial variables
net_metering = scenario_opts['net_metering_availability']
inflation = scenario_opts['ann_inflation']

# start year comes from config
end_year = scenario_opts['end_year']
model_years = range(start_year,end_year+1,2)

# get the sectors to model
sectors = datfunc.get_sectors(cur)

deprec_schedule = datfunc.get_depreciation_schedule(con, type = 'standard').values
financial_parameters = datfunc.get_financial_parameters(con, res_model = 'Existing Home', com_model = 'Host Owned', ind_model = 'Host Owned')
max_market_share = datfunc.get_max_market_share(con, sectors.values(), residential_type = 'retrofit', commercial_type = 'retrofit', industrial_type = 'retrofit')
market_projections = datfunc.get_market_projections(con)


# 6. Combine All of the Temporally Varying Data in a new Table in Postgres
datfunc.combine_temporal_data(cur, con, start_year, end_year, datfunc.pylist_2_pglist(sectors.values()))

# 7. Set up the Main Data Frame for each sector
outputs = pd.DataFrame()
for sector_abbr, sector in sectors.iteritems():
    # define the rate escalation source and max market curve for the current sector
    rate_escalation_source = scenario_opts['%s_rate_escalation' % sector_abbr]
    max_market_curve = scenario_opts['%s_max_market_curve' % sector_abbr]
    # create the Main Table in Postgres (optimal turbine size and height for each year and customer bin)
    main_table = datfunc.generate_customer_bins(cur, con, random_generator_seed, customer_bins, sector_abbr, sector, 
                                   start_year, end_year, rate_escalation_source, load_growth_scenario, exclusions,
                                   oversize_turbine_factor, undersize_turbine_factor, preprocess)
    # get dsire incentives for the generated customer bins
    dsire_incentives = datfunc.get_dsire_incentives(cur, con, sector_abbr)
    # Pull data from the Main Table to a Data Frame for each year
    
    for year in model_years:
        print 'Working on %s for %s sector' %(year, sector_abbr) 
        df = datfunc.get_main_dataframe(con, main_table, year)
        df['sector'] = sector.lower()
        df = pd.merge(df,market_projections[['year', 'customer_expec_elec_rates']], how = 'left', on = 'year')
        df = pd.merge(df,financial_parameters, how = 'left', on = 'sector')
        
        ## Diffusion from previous year ## 
        if year == start_year: 
            # get the initial market share per bin by county
            initial_market_shares = datfunc.get_initial_wind_capacities(cur, con, customer_bins, sector_abbr, sector)
            # join this to the df to on county_id
            df = pd.merge(df, initial_market_shares, how = 'left', on = 'county_id')
            df['number_of_adopters_last_year'] = df['market_share_last_year'] * df['customers_in_bin']
            df['installed_capacity_last_year'] = df['number_of_adopters_last_year'] * df['nameplate_capacity_kw']
            df['market_value_last_year'] = df['number_of_adopters_last_year'] * df['nameplate_capacity_kw'] * df['installed_costs_dollars_per_kw']
            
        else:
            df = pd.merge(df,market_last_year, how = 'left', on = 'gid')
       
        # 8. Calculate economics including incentives
        # Calculate value of incentives. Manual and DSIRE incentives can't stack. DSIRE ptc/pbi/fit are assumed to disburse over 10 years. 
        if scenario_opts['overwrite_exist_inc']:
            value_of_incentives = datfunc.calc_manual_incentives(df,con, year)
        else:
            inc = pd.merge(df,dsire_incentives,how = 'left', on = 'gid')
            value_of_incentives = datfunc.calc_dsire_incentives(inc, year, default_exp_yr = 2016, assumed_duration = 10)
        df = pd.merge(df, value_of_incentives, how = 'left', on = 'gid')
        
        revenue, costs, cfs = finfunc.calc_cashflows(df,deprec_schedule,  yrs = 30)      
        
        #Disabled at moment because of computation time
        #df['irr'] = finfunc.calc_irr(cfs)
        #df['mirr'] = finfunc.calc_mirr(cfs, finance_rate = df.discount_rate, reinvest_rate = df.discount_rate + 0.02)
        #df['npv'] = finfunc.calc_npv(cfs,df.discount_rate)
        
        payback = finfunc.calc_payback(cfs)
        ttd = finfunc.calc_ttd(cfs)  

        df['payback_period'] = np.where(df['sector'] == 'residential',payback, ttd)
        df['lcoe'] = finfunc.calc_lcoe(costs,df.aep.values, df.discount_rate)
        df['payback_key'] = (df['payback_period']*10).astype(int)
        df = pd.merge(df,max_market_share, how = 'left', on = ['sector', 'payback_key'])
        
        # 9. Calulate diffusion
        df['market_share'] = diffunc.calc_diffusion(df.payback_period.values,df.max_market_share.values, df.market_share_last_year.values)
        df['number_of_adopters'] = np.maximum(df['market_share'] * df['customers_in_bin'], df['number_of_adopters_last_year'])
        df['installed_capacity'] = np.maximum(df['number_of_adopters'] * df['cap'], df['installed_capacity_last_year'])
        df['market_value'] = np.maximum(df['number_of_adopters'] *df['nameplate_capacity_kw'] * df['installed_costs_dollars_per_kw'], df['market_value_last_year'])
        
        #10. Update parameters for next solve
        outputs = outputs.append(df, ignore_index = 'True')
        market_last_year = df[['gid','market_share', 'number_of_adopters', 'installed_capacity', 'market_value']] # Update dataframe for next solve year
        market_last_year.columns = ['gid', 'market_share_last_year', 'number_of_adopters_last_year', 'installed_capacity_last_year', 'market_value_last_year' ]
        
## 11. Outputs & Visualization
# set output folder
cdate = time.strftime('%Y%m%d_%H%M%S')
scen_name = '%s_%s' % (scenario_opts['scenario_name'],cdate)
runpath = '../runs/' + scen_name
while os.path.exists(runpath): 
    print 'Warning: A scenario folder with that name exists. It will be overwritten.'
    os.remove(runpath)
os.makedirs(runpath)
        
        
        
print 'Writing outputs'
outputs = outputs.fillna(0)
outputs.to_csv(runpath + '/outputs.csv')

command = ("%s --vanilla ../r/graphics/plot_outputs.R %s" %(Rscript_path, runpath))
print 'Creating outputs report'            
proc = subprocess.Popen(command,stdout=subprocess.PIPE)
messages = proc.communicate()
returncode = proc.returncode
print 'Model completed at %s run took %.1f seconds' %(time.ctime(), time.time() - t0)