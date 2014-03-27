# -*- coding: utf-8 -*-
"""
Functions for pulling data
Created on Mon Mar 24 08:59:44 2014
@author: bsigrin
"""
import psycopg2 as pg
import pandas.io.sql as sqlio
import numpy as np
from scipy.interpolate import interp1d as interp1d
import pandas as pd

def make_con(host='gispgdb',dbname='dav-gis', user='bsigrin', password='bsigrin'):
    con = pg.connect('host=%s dbname=%s user=%s password=%s' % (host, dbname, user, password))
    return con

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
        sql = 'SELECT macrs FROM wind_ds.depreciation_schedule'
    elif type.lower() == 'standard':
        sql = 'SELECT standard FROM wind_ds.depreciation_schedule'
    else:
        sql = 'SELECT * FROM wind_ds.depreciation_schedule'
    df = sqlio.read_frame(sql, con)
    return df
    
def get_scenario_options(con):
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
    df = sqlio.read_frame("SELECT * FROM wind_ds.scenario_options", con)
    return df
    
def get_main_dataframe(con):
    ''' Pull main pre-processed dataframe from dB
    
        IN: con - pg con object - connection object
        OUT: df  - pd dataframe - pre-processed resource,bins, rates, etc. for all years:

    '''
    if not con:
        close_con = True
        con = make_con()
    else:
        close_con = False
    sql = 'SELECT * FROM wind_ds.sample_10'
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
    
    sql = 'SELECT * FROM wind_ds.financial_parameters'
    df = sqlio.read_frame(sql, con)
    
    # Filter based on ownership models selected
    df['sector'] = ['residential', 'residential', 'residential', 'commercial', 'commercial','industrial','industrial']
        
    
    df = df[((df['cust_disposition'] == res_model) & (df['cust_id'] == 1)) | 
      ((df['cust_disposition'] == com_model) & (df['cust_id'] == 2)) |
      ((df['cust_disposition'] == ind_model) & (df['cust_id'] == 3))]
      
    df = df.drop('cust_id',1)
    return df
 
#==============================================================================
   
def get_max_market_share(con, scenario_options, res_type = 'retrofit', com_type = 'retrofit', ind_type = 'retrofit'):
    ''' Pull max market share from dB, select curve based on scenario_options, and interpolate to tenth of a year. 
        Use passed parameters to determine ownership type
    
        IN: con - pg con object - connection object
            res_type - string - which residential ownership structure to use (new or retrofit)
            com_type - string - which commercial ownership structure to use (new or retrofit)
            ind_type - string - which industrial ownership structure to use (new or retrofit)
            
        OUT: max_market_share  - pd dataframe - dataframe to join on main df to determine max share 
                                                keys are sector & payback period 
    '''
    # Query data    
    max_market_share = sqlio.read_frame('SELECT * FROM wind_ds.max_market_share', con)
    user_defined_max_market_share = sqlio.read_frame('SELECT * FROM wind_ds.user_defined_max_market_share', con)
    
    # Rename so the user-defined can be concatenated
    max_market_share.columns = ['year','new','retrofit','sector','source']
    max_market_share = max_market_share[['year','sector','new','retrofit','source']]
    max_market_share['sector']  = [i.lower() for i in max_market_share['sector'].values]
    
    user_defined_max_market_share['source'] = 'User Defined'
    user_defined_max_market_share['sector']  = [i.lower() for i in user_defined_max_market_share['sector'].values]
    
    mm = max_market_share.append(user_defined_max_market_share, ignore_index = 'TRUE')
    
    # Select the max market share curve for each sector
    max_market_res = mm[(mm['sector'] == 'residential') & (mm['source'] == scenario_options.res_max_market_curve[0])]
    max_market_com = mm[(mm['sector'] == 'commercial') & (mm['source'] == scenario_options.com_max_market_curve[0])]
    max_market_ind = mm[(mm['sector'] == 'industrial') & (mm['source'] == scenario_options.ind_max_market_curve[0])]     
    
    # Now interpolate each curve
    yrs=np.linspace(0,30,31);
    yrs2=np.linspace(0,30,301);
    
    f1 = interp1d(max_market_res['year'], max_market_res[res_type]);
    f2 = interp1d(max_market_com['year'], max_market_com[com_type]);
    f3 = interp1d(max_market_ind['year'], max_market_ind[ind_type]);
    
#    res_max_market = pd.DataFrame({'payback_period': yrs2, 'max_market_share': f1(yrs2),'sector': 'residential','payback_key': np.arange(301)})
#    com_max_market = pd.DataFrame({'payback_period': yrs2, 'max_market_share': f2(yrs2),'sector': 'commercial','payback_key': np.arange(301)})   
#    ind_max_market = pd.DataFrame({'payback_period': yrs2, 'max_market_share': f3(yrs2),'sector': 'industrial','payback_key': np.arange(301)})
    
    res_max_market = pd.DataFrame({'max_market_share': f1(yrs2),'sector': 'residential','payback_key': np.arange(301)})
    com_max_market = pd.DataFrame({'max_market_share': f2(yrs2),'sector': 'commercial','payback_key': np.arange(301)})   
    ind_max_market = pd.DataFrame({'max_market_share': f3(yrs2),'sector': 'industrial','payback_key': np.arange(301)}) 
    return res_max_market.append(com_max_market, ignore_index = 'TRUE').append(ind_max_market, ignore_index = 'TRUE')