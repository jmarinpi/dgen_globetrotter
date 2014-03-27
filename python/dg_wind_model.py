"""
Distributed Wind Diffusion Model
National Renewable Energy Lab

@author: bsigrin
"""
### NOTE: THESE SHOULD LATER BE BROKEN INTO SEPARATE SCRIPTS

# 1. # Set model paths
# NOTE TO BEN: you don't need to do this as long as all of the other modules are stored in the same locatio
            # as the main module (dg_wind_model.py)
#os.chdir('/Users/bsigrin/Desktop/diffusion/python')
#model_path = os.getcwd()

# 2. # Import modules and global vars
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg
import psycopg2.extras as pgx
import numpy as np
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
# get scenario options as a dictionary
scenario_opts = datfunc.get_scenario_options(cur)
# get exclusions
exclusions = datfunc.get_exclusions(cur)
# get financial variables
load_growth_scenario = scenario_opts['load_growth_scenario']
net_metering = scenario_opts['net_metering_availability']
inflation = scenario_opts['ann_inflation']
# set the range of years to model
start_year = scenario_opts['starting_year']
end_year = scenario_opts['end_year']
model_years = range(start_year,end_year+1,2)
# get the sectors to model
sectors = datfunc.get_sectors(cur)

# 6. Combine All of the Temporally Varying Data in a new Table in Postgres
datfunc.combine_temporal_data(cur, con, start_year, end_year, datfunc.pylist_2_pglist(sectors.values()))

# 7. Set up the Main Data Frame for each sector
for sector_abbr, sector in sectors.iteritems():
    # define the rate escalatin source and max market curve for the current sector
    rate_escalation_source = scenario_opts['%s_rate_escalation' % sector_abbr]
    max_market_curve = scenario_opts['%s_max_market_curve' % sector_abbr]
    # create the Main Table in Postgres (optimal turbine size and height for each year and customer bin)
    main_table = datfunc.generate_customer_bins(cur, con, random_generator_seed, customer_bins, sector_abbr, sector, 
                                   start_year, end_year, rate_escalation_source, load_growth_scenario, exclusions,
                                   oversize_turbine_factor, undersize_turbine_factor)
    # Pull data from the Main Table to a Data Frame for each year
    for year in model_years:
        df = datfunc.get_main_dataframe(con, main_table, year)
        crash
        # Ben -- Do the rest of your calcs here. 


### REPLACE ##
#df['sector'] = 'Residential'
#df['cust_expected_rate_growth'] = 1 + 0.025
### REPLACE ##
#
#
#deprec_schedule = datfunc.get_depreciation_schedule(con, type = 'standard').values
#financial_parameters = datfunc.get_financial_parameters(con, res_model = 'Existing Home', com_model = 'Host Owned', ind_model = 'Host Owned')
##max_market = make_max_market_table(source = 'NAV_NEW')
#
## The people demand JOIN      
#df = pd.merge(df,financial_parameters, how = 'left', on = 'sector') 
#     
#con.close()
#
#
##Initialize mkt dataframe, which shows all counties, pop bins, and segment indices
##ASSUME: all indices are constant
#
##mkt = cf_data[:]
##
##mkt['diffusion'] = 0.00
##mkt['pp'] = 0.00
##mkt['Population'] = 100
##mkt.diffusion[(mkt.Year == 2014)] = 0.01
#
#    
## 8. # Calculate economics
#revenue, costs, cfs = finfunc.calc_cashflows(df,deprec_schedule, value_of_incentive = 0, value_of_rebate = 0,  yrs = 30) 
#
#irr = finfunc.calc_irr(cfs)
#mirr = finfunc.calc_mirr(cfs, finance_rate = df.discount_rate, reinvest_rate = df.discount_rate + 0.02)
#npv = finfunc.calc_npv(cfs,df.discount_rate)
#payback = finfunc.calc_payback(cfs)
#ttd = finfunc.calc_ttd(cfs)  
#
### BOS - 03/26/14 - EVERTHING BELOW HERE IS PROBABLY JUNK   
#
#
#
# 
## 9. Calulate diffusion
#list_of_counties = list(cf_data['County'].unique())
#
#for t in model_years:
#
#    print 'Working on year %1i' %t    
#    mkt_this_year = mkt[(mkt.Year == t)]
#    ms =[]
#    pp = []
#    cap_cost, fom_cost, vom_cost, default_tower_height, cost_for_higher_tower = Wind_Cost_Projections[str(t)]   
#    
#    for c in list_of_counties:
#        
#    # Vectorize this capability
#    for i, row in mkt_this_year.iterrows():
#    
#        capacity_factor = row['CF']
#        current_market_share = row['diffusion']
#        segment = row['Customer_Segment']
#        
#        costs, principle = calc_cost_cashflows(cap_cost, incent_frac, incent_rebate, tax_rt, downpay_frac, loan_term, loan_rate, 
#                      disc_rate, MACRS, fom_cost, vom_cost, capacity_factor, yrs = 30)
#                      
#        revenue = calc_revenue_cashflows(capacity_factor,avg_rate,rate_growth,yrs = 30)
#    
#        payback = calc_payback(costs, revenue)
#
#        new_market_share = calc_diffusion(payback,current_market_share,segment,max_market)
#        ms.append(new_market_share)
#        pp.append(payback)
#    if t < end_year:
#        mkt.diffusion[(mkt.Year == t+2)] = ms
#        mkt.pp[(mkt.Year == t+2)] = pp    
#
## 10. Outputs & Visualization
#
#mkt['diff_population'] = (mkt.diffusion * mkt.Population)
#total_mkt = mkt.groupby(['County','Year']).sum()
#total_mkt['diff_percent'] = total_mkt.diff_population / total_mkt.Population
#
## Make scatter plot of adoption over time
#
#fig = plt.figure()
#total_mkt.ix['Year'].plot()
#total_mkt['diff_percent'].plot()
#ax1 = fig.add_subplot(1,2,1)
#ax1.set_xticks(range(len(model_years)))
#ax1.set_xticklabels(model_years)
#plt.plot(total_mkt['diff_percent'], 'k--')
#
#plt.boxplot(mkt['pp'])