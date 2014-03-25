# -*- coding: utf-8 -*-
"""
Functions for pulling data
Created on Mon Mar 24 08:59:44 2014
@author: bsigrin
"""

def get_depreciation_schedule(type = 'all'):
    ''' Pull depreciation schedule from dB
    
        IN: type - string - [all, macrs, standard] 
        OUT: df  - pd dataframe - year, depreciation schedule:

    '''
    
    pg_params = "dbname=dav-gis user=bsigrin password=bsigrin host=gispgdb.nrel.gov" 
    con = pg.connect(pg_params)
    if type.lower() == 'macrs':
        sql = 'SELECT macrs FROM wind_ds.depreciation_schedule'
    elif type.lower() == 'standard':
        sql = 'SELECT standard FROM wind_ds.depreciation_schedule'
    else:
        sql = 'SELECT * FROM wind_ds.depreciation_schedule'
    df = sqlio.read_frame(sql, con)
    con.close()
    return df
    
def get_scenario_options():
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
    pg_params = "dbname=dav-gis user=bsigrin password=bsigrin host=gispgdb.nrel.gov" 
    con = pg.connect(pg_params)
    df = sqlio.read_frame("SELECT * FROM wind_ds.scenario_options", con)
    con.close()
    return df