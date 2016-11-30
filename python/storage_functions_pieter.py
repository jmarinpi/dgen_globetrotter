# -*- coding: utf-8 -*-
"""
Created on Thu Jun  9 11:23:55 2016

@author: mgleason
"""

import psycopg2 as pg
import numpy as np
import pandas as pd
import decorators
import utility_functions as utilfunc
import multiprocessing
import traceback
from agent import Agent, Agents, AgentsAlgorithm
from cStringIO import StringIO

# Import from support function repo
#import sys
#sys.path.append('C:/users/pgagnon/desktop/support_functions/python')
import dispatch_functions as dFuncs
import tariff_functions as tFuncs
import financial_functions as fFuncs
import general_functions as gFuncs

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================

#%%

def system_size_and_bill_calc(agent, e_escalation_sch, deprec_sch_df, pv_cf_profile_df, rates_rank_df, rates_json_df):
    
    print "Agent ID:", agent['agent_id']
    if agent['agent_id']==945:
        print "full stop"
        
    # Extract load profile
    load_profile = agent['consumption_hourly']

    # Create export tariff object
    export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)

    # Filter for list of tariffs available to this agent
    agent_rate_list = rates_rank_df[rates_rank_df['agent_id']==agent['agent_id']]

    # drop duplicate tariffs - temporary fix uptil Meghan's update comes through
    agent_rate_list = agent_rate_list.drop_duplicates()  
    
    if len(agent_rate_list > 1):
        # determine which of the tariffs has the cheapest cost of electricity without a system
        agent_rate_list['bills'] = None
        for index in agent_rate_list.index:
            rate_id = agent_rate_list.loc[index, 'rate_id_alias']   
            if np.any(rate_id==np.array([4097, 4531, 5274])):
                agent_rate_list.loc[index, 'bills'] = 9999999999                

            else:
                tariff_dict = rates_json_df.loc[rate_id, 'rate_json']
                tariff = tFuncs.Tariff(dict_obj=tariff_dict)
                # TODO: remove this once tariffs are reloaded
                # temp fix because rate jsons were built incorrectly, and skipping 
                # a set of tariffs that will eventually be removed
                tariff.d_flat_levels = np.zeros([1, 12]) + tariff.d_flat_levels[0,0]
                tariff.d_flat_prices = np.zeros([1, 12]) + tariff.d_flat_prices[0,0]
                tariff.coincident_peak_exists = False
                bill, _ = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
                agent_rate_list.loc[index, 'bills'] = bill
                
    # Select the tariff that had the cheapest electricity. Note that there is
    # currently no rate switching, if it would be cheaper once a system is 
    # installed. This is currently for computational reasons.
    rate_id = agent_rate_list.loc[agent_rate_list['bills'].idxmin(), 'rate_id_alias']
    tariff_dict = rates_json_df.loc[rate_id, 'rate_json']
    tariff = tFuncs.Tariff(dict_obj=tariff_dict)
    tariff.d_flat_levels = np.zeros([1, 12]) + tariff.d_flat_levels[0,0]
    tariff.d_flat_prices = np.zeros([1, 12]) + tariff.d_flat_prices[0,0]
    tariff.coincident_peak_exists = False

    deprec_sch = np.array(deprec_sch_df.loc[agent['depreciation_sch_index'], 'deprec'])
    pv_cf_profile = np.array(pv_cf_profile_df.loc[agent['resource_index_solar'], 'generation_hourly'])/1e6 # Is this correct? The 1e6?
    agent['naep'] = float(np.sum(pv_cf_profile))    
    agent['max_pv_size'] = np.min([agent['load_kwh_per_customer_in_bin']/agent['naep'], agent['developable_roof_sqft']*agent['pv_density_w_per_sqft']*1000.0*agent['gcr']])

    d_inc_n = 20    
    DP_inc = 12
    pv_inc = 3
    batt_inc = 3
    pv_sizes = np.linspace(0, agent['max_pv_size'], pv_inc)
    batt_powers = np.linspace(0, np.array(agent['max_demand_kw']) * 0.2, batt_inc)
    original_bill, original_results = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
    batt = dFuncs.Battery()
    
#    params_df = pd.DataFrame(index=pv_sizes, columns = ['params'], dtype=object)
#
#    for p, pv_size in enumerate(pv_sizes):
#        load_and_pv_profile = load_profile - pv_size*pv_cf_profile
#        estimator_params = dFuncs.calc_estimator_params(load_and_pv_profile, tariff, export_tariff, batt.eta_charge, batt.eta_discharge)
#        params_df['params'][pv_size] = estimator_params
    
    system_sizes = gFuncs.cartesian([pv_sizes, batt_powers])
    
    system_df = pd.DataFrame(system_sizes, columns=['pv', 'batt'])
    system_df['est_bills'] = None
    n_sys = len(system_df)

    for i in system_df.index:    
        pv_size = system_df['pv'][i].copy()
        batt_power = system_df['batt'][i].copy()
        load_and_pv_profile = load_profile - pv_size*pv_cf_profile
        estimator_params = dFuncs.calc_estimator_params(load_and_pv_profile, tariff, export_tariff, batt.eta_charge, batt.eta_discharge)
                
        batt.set_cap_and_power(batt_power*3.0, batt_power)    

        estimated_results = dFuncs.determine_optimal_dispatch(load_profile, pv_size*pv_cf_profile, batt, tariff, export_tariff, estimator_params=estimator_params, estimated=True, d_inc_n=d_inc_n)
        system_df.loc[i, 'est_bills'] = estimated_results['bill_under_dispatch']   
        
    est_bill_savings = np.zeros([n_sys, agent['analysis_years']+1])
    est_bill_savings[:,1:] = (original_bill - np.array(system_df['est_bills'])).reshape([n_sys, 1])
    system_df['est_bill_savings'] = (original_bill - np.array(system_df['est_bills'])).reshape([n_sys, 1]) 
    
    batt_chg_frac = 1.0 # just a placeholder...
    
    cf_results_est = fFuncs.cashflow_constructor(est_bill_savings, 
                         system_sizes[:,0], agent['pv_cost_per_kw'], 0, agent['fixed_om_dollars_per_kw_per_yr'],
                         system_sizes[:,1]*3, system_sizes[:,1], 
                         agent['batt_cost_per_kw'], agent['batt_cost_per_kwh'], 
                         agent['batt_replace_cost_per_kw'], agent['batt_replace_cost_per_kwh'],
                         batt_chg_frac,
                         agent['batt_replace_yr'], agent['batt_om'],
                         agent['sector'], agent['itc_fraction'], deprec_sch, 
                         agent['tax_rate'], 0, agent['discount_rate'],  
                         agent['analysis_years'], agent['inflation'], 
                         agent['down_payment'], agent['loan_rate'], agent['loan_term_yrs'])
                
                                                      
    system_df['npv'] = cf_results_est['npv']
    
    index_of_max_npv = system_df['npv'].idxmax()
    
    opt_pv_size = system_df['pv'][index_of_max_npv].copy()
    opt_batt_power = system_df['batt'][index_of_max_npv].copy()
    opt_batt_cap = opt_batt_power*3.0
    batt.set_cap_and_power(opt_batt_cap, opt_batt_power)    
    load_and_pv_profile = load_profile - opt_pv_size*pv_cf_profile
    accurate_results = dFuncs.determine_optimal_dispatch(load_profile, opt_pv_size*pv_cf_profile, batt, tariff, export_tariff, estimated=False, d_inc_n=d_inc_n, DP_inc=DP_inc)
    opt_bill = accurate_results['bill_under_dispatch']   
    opt_bill_savings = np.zeros([1, agent['analysis_years']+1])
    opt_bill_savings[:, 1:] = (original_bill - opt_bill)
    
    cf_results_opt = fFuncs.cashflow_constructor(opt_bill_savings, 
                     opt_pv_size, agent['pv_cost_per_kw'], 0, agent['fixed_om_dollars_per_kw_per_yr'],
                     opt_batt_power*3, opt_batt_power, 
                     agent['batt_cost_per_kw'], agent['batt_cost_per_kwh'], 
                     agent['batt_replace_cost_per_kw'], agent['batt_replace_cost_per_kwh'],
                     batt_chg_frac,
                     agent['batt_replace_yr'], agent['batt_om'],
                     agent['sector'], agent['itc_fraction'], deprec_sch, 
                     agent['tax_rate'], 0, agent['discount_rate'],  
                     agent['analysis_years'], agent['inflation'], 
                     agent['down_payment'], agent['loan_rate'], agent['loan_term_yrs']) 
               
    agent['pv_kw'] = opt_pv_size
    agent['batt_kw'] = opt_batt_power
    agent['batt_kwh'] = opt_batt_cap
    agent['npv'] = cf_results_opt['npv'][0]
    agent['cash_flow'] = cf_results_opt['cf']
    
    if opt_pv_size != 0 or opt_batt_cap != 0: agent['system_built'] = True
    else: agent['system_built'] = False
                 
    return agent
    
    
#%%
def system_size_driver(agent_df, rate_growth_df, deprec_sch_df, pv_cf_profile_df, rates_rank_df, rates_json_df):  
    
    agent_df = agent_df.apply(system_size_and_bill_calc, axis=1, args=(rate_growth_df, deprec_sch_df, pv_cf_profile_df, rates_rank_df, rates_json_df))

    return agent_df
    
    
    

    