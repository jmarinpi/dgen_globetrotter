"""
Distributed Wind Diffusion Model
National Renewable Energy Lab

@author: bsigrin
"""
### NOTE: THESE SHOULD LATER BE BROKEN INTO SEPARATE SCRIPTS

# 1. # Set model paths
os.chdir('/Users/bsigrin/Desktop/diffusion/python')
model_path = os.getcwd()

# 2. # Import modules
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg
import numpy as np
import scipy as sp
import matplotlib as mpl
import collections
import diffusion_functions as diffunc
import financial_functions as finfunc
import data_functions as datfunc

# 3.Read data - Most of this should be run in a loading script ================

con = datfunc.make_con()

df = datfunc.get_main_dataframe(con)
## REPLACE ##
df['sector'] = 'residential'
df['cust_expected_rate_growth'] = 1 + 0.025
df['current_market_share'] = 0.01
## REPLACE ##

scenario_options = datfunc.get_scenario_options(con)
deprec_schedule = datfunc.get_depreciation_schedule(con, type = 'standard').values
financial_parameters = datfunc.get_financial_parameters(con, res_model = 'Existing Home', com_model = 'Host Owned', ind_model = 'Host Owned')
max_market_share = datfunc.get_max_market_share(con, scenario_options, res_type = 'retrofit', com_type = 'retrofit', ind_type = 'retrofit')

# The people demand JOIN      
df = pd.merge(df,financial_parameters, how = 'left', on = 'sector') 
     
con.close()


start_year = 2014
end_year = scenario_options.end_year[0]
model_years  = range(start_year,end_year+2,2)


#Initialize mkt dataframe, which shows all counties, pop bins, and segment indices
#ASSUME: all indices are constant

#mkt = cf_data[:]
#
#mkt['diffusion'] = 0.00
#mkt['pp'] = 0.00
#mkt['Population'] = 100
#mkt.diffusion[(mkt.Year == 2014)] = 0.01

    
# 4. # Calculate economics
revenue, costs, cfs = finfunc.calc_cashflows(df,deprec_schedule, value_of_incentive = 0, value_of_rebate = 0,  yrs = 30) 

df['irr'] = finfunc.calc_irr(cfs)
df['mirr'] = finfunc.calc_mirr(cfs, finance_rate = df.discount_rate, reinvest_rate = df.discount_rate + 0.02)
df['npv'] = finfunc.calc_npv(cfs,df.discount_rate)

payback = finfunc.calc_payback(cfs)
ttd = finfunc.calc_ttd(cfs)  

df['payback_period'] = np.where(df['sector'] == 'residential',payback, ttd)
df['payback_key'] = (df['payback_period']*10).astype(int)
df = pd.merge(df,max_market_share, how = 'left', on = ['sector', 'payback_key']) 
# JOIN mm share here
  
# 5. Calulate diffusion

for t in model_years:

    print 'Working on year %1i' %t    
    mkt_this_year = mkt[(mkt.Year == t)]
    ms =[]
    pp = []
new_market_share = diffunc.calc_diffusion(df.payback_period.values,df.max_market_share.values, df.current_market_share.values)        
    ms.append(new_market_share)
    pp.append(payback)
    
    if t < end_year:
        mkt.diffusion[(mkt.Year == t+2)] = ms
        mkt.pp[(mkt.Year == t+2)] = pp    

# 6. Outputs & Visualization

mkt['diff_population'] = (mkt.diffusion * mkt.Population)
total_mkt = mkt.groupby(['County','Year']).sum()
total_mkt['diff_percent'] = total_mkt.diff_population / total_mkt.Population

# Make scatter plot of adoption over time

fig = plt.figure()
total_mkt.ix['Year'].plot()
total_mkt['diff_percent'].plot()
ax1 = fig.add_subplot(1,2,1)
ax1.set_xticks(range(len(model_years)))
ax1.set_xticklabels(model_years)
plt.plot(total_mkt['diff_percent'], 'k--')

plt.boxplot(mkt['pp'])