"""
Distributed Wind Diffusion Model
National Renewable Energy Lab

@author: bsigrin
"""
### NOTE: THESE SHOULD LATER BE BROKEN INTO SEPARATE SCRIPTS

# 1. # Initialize Model
import time
t0 = time.clock()
print 'Initiating model at %s' %time.ctime()

scen_name = 'all_outputs'
runpath = '../runs/' + scen_name
while os.path.exists(runpath): 
    print 'A scenario folder with that name exists, renaming'
    runpath = runpath + '1'
os.makedirs(runpath)


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
# load in a bunch of the configuration variables as global vars
from config import *

# 3. Connect to Postgres and configure connection
# create connection to Postgres Database
# (to edit login information, edit config.py)
con, cur = datfunc.make_con(pg_conn_string)
# register access to hstore in postgres
pgx.register_hstore(con)


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

deprec_schedule = datfunc.get_depreciation_schedule(con, type = 'standard').values
financial_parameters = datfunc.get_financial_parameters(con, res_model = 'Existing Home', com_model = 'Host Owned', ind_model = 'Host Owned')
max_market_share = datfunc.get_max_market_share(con, scenario_opts, res_type = 'retrofit', com_type = 'retrofit', ind_type = 'retrofit')
market_projections = datfunc.get_market_projections(con)
# get the sectors to model
sectors = datfunc.get_sectors(cur)

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
                                   oversize_turbine_factor, undersize_turbine_factor, process_inputs)
    # Pull data from the Main Table to a Data Frame for each year
    
    for year in model_years:
        print 'Working on %s for %s sector' %(year, sector_abbr) 
        df = datfunc.get_main_dataframe(con, main_table, year)
        df['sector'] = sector.lower()
        df = pd.merge(df,market_projections[['year', 'customer_expec_elec_rates']],how = 'left', on = 'year')
        df = pd.merge(df,financial_parameters, how = 'left', on = 'sector')
        
        ## Diffusion from previous year ## 
        if year == start_year: 
            market_share_last_year = df[['gid']].copy()
            df['market_share_last_year'] = 0.002
        else:
            df = pd.merge(df,market_share_last_year, how = 'left', on = 'gid')
        
        # 8. Calculate economics        
        revenue, costs, cfs = finfunc.calc_cashflows(df,deprec_schedule, value_of_incentive = 0, value_of_rebate = 0,  yrs = 30)      
        
        #Disabled at moment because of computation time
        #df['irr'] = finfunc.calc_irr(cfs)
        #df['mirr'] = finfunc.calc_mirr(cfs, finance_rate = df.discount_rate, reinvest_rate = df.discount_rate + 0.02)
        #df['npv'] = finfunc.calc_npv(cfs,df.discount_rate)
        
        payback = finfunc.calc_payback(cfs)
        ttd = finfunc.calc_ttd(cfs)  

        df['payback_period'] = np.where(df['sector'] == 'residential',payback, ttd)
        df['payback_key'] = (df['payback_period']*10).astype(int)
        df = pd.merge(df,max_market_share, how = 'left', on = ['sector', 'payback_key'])
        
        # 9. Calulate diffusion
        df['market_share'] = diffunc.calc_diffusion(df.payback_period.values,df.max_market_share.values, df.market_share_last_year.values)
        df['number_of_adopters'] = df['market_share'] * df['customers_in_bin']
        df['installed_capacity'] = df['number_of_adopters'] * df['cap']
        #outputs_this_year = df[['gid', 'year', 'county_id', 'sector', 'state_abbr', 'number_of_adopters', 'installed_capacity', 'payback_period']]
        
        #10. Update parameters for next solve
        # BOS - would like to save entire main dataframe, at lease for diagnostics
        outputs = outputs.append(df, ignore_index = 'True')
        market_share_last_year = df[['gid','market_share']] # Update dataframe for next solve year
        market_share_last_year.columns = ['gid', 'market_share_last_year']
        
## 11. Outputs & Visualization
print 'Starting visualization'
outputs.to_csv(runpath + '/outputs.csv')
print 'Model completed at %s run took %.1f seconds' %(time.ctime(), time.time() - t0)
national_installed_capacity = outputs.groupby(['year'])

outputs['installed_capacity_gw'] = outputs['installed_capacity'] / 1e6


fig = plt.figure()
ax1 = fig.add_subplot(1,1,1)
ax1.set_title('National Installed Capacity (GW)')
ax1.set_ylabel('Installled Capacity (GW)')
outputs.groupby(['year']).sum()['installed_capacity_gw'].plot(ax = ax1)
savefig(runpath + '/National Installed Capacity.png')

#.sum('installed_capacity_gw')#['installed_capacity_gw'].plot(ax = ax1)


# Make scatter plot of adoption over time