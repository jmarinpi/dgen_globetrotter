# -*- coding: utf-8 -*-
"""
3_Read_Data

@author: bsigrin
"""
import data_functions as datfunc
import psycopg2 as pg

con = datfunc.make_con()

df = datfunc.get_main_dataframe(con)
## REPLACE ##
df['sector'] = 'Residential'
df['cust_expected_rate_growth'] = 1 + 0.025
## REPLACE ##

scenario_options = datfunc.get_scenario_options(con)
deprec_schedule = datfunc.get_depreciation_schedule(con, type = 'standard').values
financial_parameters = datfunc.get_financial_parameters(con, res_model = 'Existing Home', com_model = 'Host Owned', ind_model = 'Host Owned')


## The people demand JOIN      
df = pd.merge(df,financial_parameters, how = 'left', on = 'sector') 
     
con.close()