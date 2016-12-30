# -*- coding: utf-8 -*-
"""
Created on Thu Jun  9 11:23:55 2016

@author: mgleason
"""

import numpy as np
import pandas as pd
import utility_functions as utilfunc
import multiprocessing as mp
from concurrent import futures
import os
import time

# Import from support function repo
import dispatch_functions as dFuncs
import tariff_functions as tFuncs
import financial_functions as fFuncs
import general_functions as gFuncs

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================

#%%
#def system_size_and_bill_calc_simple(agent, deprec_sch_df, pv_cf_profile_df, rates_rank_df, rates_json_df):
#    
#    # Temporary list of rates to ignore
#    # TODO remove this when tariffs are updated
#    tariffs_to_ignore = np.array([3956, 3962, 3963, 3964])  
#    
#    # Extract load profile
#    load_profile = agent['consumption_hourly']
#
#    # Create export tariff object
#    export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
#
#    # Filter for list of tariffs available to this agent
#    agent_rate_list = rates_rank_df[rates_rank_df['agent_id']==agent['agent_id']]
#
#    # drop duplicate tariffs - temporary fix uptil Meghan's update comes through
#    agent_rate_list = agent_rate_list.drop_duplicates()  
#    
#    if len(agent_rate_list > 1):
#        # determine which of the tariffs has the cheapest cost of electricity without a system
#        agent_rate_list['bills'] = None
#        for index in agent_rate_list.index:
#            rate_id = agent_rate_list.loc[index, 'rate_id_alias'] 
#            tariff_dict = rates_json_df.loc[rate_id, 'rate_json']
#            tariff = tFuncs.Tariff(dict_obj=tariff_dict)
#            
#            #if np.any(rate_id==np.array([4097, 4531, 5274])):
#            if tariff.e_n == 0 or np.any(rate_id==tariffs_to_ignore):       
#                print "tariff without e_n:", rate_id
#                agent_rate_list.loc[index, 'bills'] = 9999999999                
#            else:
#                # TODO: remove this once tariffs are reloaded
#                # temp fix because rate jsons were built incorrectly, and skipping 
#                # a set of tariffs that will eventually be removed
#                tariff.d_flat_levels = np.zeros([1, 12]) + tariff.d_flat_levels[0,0]
#                tariff.d_flat_prices = np.zeros([1, 12]) + tariff.d_flat_prices[0,0]
#                tariff.coincident_peak_exists = False
#                bill, _ = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
#                agent_rate_list.loc[index, 'bills'] = bill
#                
#    # Select the tariff that had the cheapest electricity. Note that there is
#    # currently no rate switching, if it would be cheaper once a system is 
#    # installed. This is currently for computational reasons.
#    rate_id = agent_rate_list.loc[agent_rate_list['bills'].idxmin(), 'rate_id_alias']
#    tariff_dict = rates_json_df.loc[rate_id, 'rate_json']
#    tariff = tFuncs.Tariff(dict_obj=tariff_dict)
#    tariff.d_flat_levels = np.zeros([1, 12]) + tariff.d_flat_levels[0,0]
#    tariff.d_flat_prices = np.zeros([1, 12]) + tariff.d_flat_prices[0,0]
#    tariff.coincident_peak_exists = False
#
#    deprec_sch = np.array(deprec_sch_df.loc[agent['depreciation_sch_index'], 'deprec'])
#    pv_cf_profile = np.array(pv_cf_profile_df.loc[agent['resource_index_solar'], 'generation_hourly'])/1e6 # Is this correct? The 1e6?
#    agent['naep'] = float(np.sum(pv_cf_profile))    
#    agent['max_pv_size'] = np.min([agent['load_kwh_per_customer_in_bin']/agent['naep'], agent['developable_roof_sqft']*agent['pv_density_w_per_sqft']/1000.0])
#
#    original_bill, original_results = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
#    batt = dFuncs.Battery()
#
#    d_inc_n = 20    
#    DP_inc = 12
#    
#    # If full retail nem is present, size system to 95% of max. If not, size to
#    # 50% of max. 
#    if agent['nem_system_size_limit_kw']!=0.0:
#        pv_sizes = np.array([agent['max_pv_size']]) * [0.5, 0.95]
#    else: 
#        pv_sizes = np.array([agent['max_pv_size']]) * 0.50
#    
#    # Only evaluate a battery if there are demand charges or TOU energy charges
#    # TODO: remove once tariffs are reloaded    
#    e_12by24_max_prices_wkday = tariff.e_prices_no_tier[tariff.e_wkday_12by24]
#    e_12by24_max_prices_wkend = tariff.e_prices_no_tier[tariff.e_wkend_12by24]
#    e_max_price_differential_wkday = np.max(e_12by24_max_prices_wkday, 1) - np.min(e_12by24_max_prices_wkday, 1)
#    e_max_price_differential_wkend = np.max(e_12by24_max_prices_wkend, 1) - np.min(e_12by24_max_prices_wkend, 1)
#    tariff.e_max_difference = np.max([e_max_price_differential_wkday, e_max_price_differential_wkend])
#    if tariff.d_flat_exists or tariff.d_tou_exists or tariff.e_max_difference>0.02 or export_tariff.full_retail_nem==False:
#        batt_powers = np.array([0, np.array(agent['max_pv_size'])*0.1])
#        batt_eval = True
#    else:
#        batt_powers = np.zeros(1)
#        batt_eval = False
#        
#    system_sizes = gFuncs.cartesian([pv_sizes, batt_powers])
#        
#    system_df = pd.DataFrame(system_sizes, columns=['pv', 'batt'])
#    system_df['bills'] = None
#    n_sys = len(system_df)
#
#    for i in system_df.index:    
#        pv_size = system_df['pv'][i].copy()
#        if pv_size<=agent['nem_system_size_limit_kw']:
#            export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
#        else:
#            export_tariff.set_constant_sell_price(0.03)
#
#        batt_power = system_df['batt'][i].copy()
#        batt.set_cap_and_power(batt_power*3.0, batt_power)    
#
#        results = dFuncs.determine_optimal_dispatch(load_profile, pv_size*pv_cf_profile, batt, tariff, export_tariff, DP_inc=DP_inc, d_inc_n=d_inc_n, restrict_charge_to_pv_gen=True)
#        system_df.loc[i, 'bills'] = results['bill_under_dispatch']   
#        
#    bill_savings = np.zeros([n_sys, agent['analysis_years']+1])
#    bill_savings[:,1:] = (original_bill - np.array(system_df['bills'])).reshape([n_sys, 1])
#    
#    # Escalate bill savings for price increase relative to 2016 (when tariffs were curated)
#    escalator = (np.zeros(agent['analysis_years']+1) + agent['elec_price_escalator'] + 1)**range(agent['analysis_years']+1)
#    bill_savings = bill_savings * agent['elec_price_multiplier'] * escalator
#    system_df['bill_savings'] = bill_savings[:, 1]
#    
#    batt_chg_frac = 1.0 # just a placeholder...
#    
#    cf_results = fFuncs.cashflow_constructor(bill_savings, 
#                         system_sizes[:,0], agent['pv_cost_per_kw'], 0, agent['fixed_om_dollars_per_kw_per_yr'],
#                         system_sizes[:,1]*3, system_sizes[:,1], 
#                         agent['batt_cost_per_kw'], agent['batt_cost_per_kwh'], 
#                         agent['batt_replace_cost_per_kw'], agent['batt_replace_cost_per_kwh'],
#                         batt_chg_frac,
#                         agent['batt_replace_yr'], agent['batt_om'],
#                         agent['sector'], agent['itc_fraction'], deprec_sch, 
#                         agent['tax_rate'], 0, agent['discount_rate'],  
#                         agent['analysis_years'], agent['inflation'], 
#                         agent['down_payment'], agent['loan_rate'], agent['loan_term_yrs'])
#                
#                                                      
#    system_df['npv'] = cf_results['npv']
#    
#    index_of_max_npv = system_df['npv'].idxmax()
#    
#    opt_pv_size = system_df['pv'][index_of_max_npv].copy()
#    opt_batt_power = system_df['batt'][index_of_max_npv].copy()
#    opt_batt_cap = opt_batt_power*3.0
#               
#    agent['pv_kw'] = opt_pv_size
#    agent['batt_kw'] = opt_batt_power
#    agent['batt_kwh'] = opt_batt_cap
#    agent['npv'] = cf_results['npv'][index_of_max_npv]
#    agent['cash_flow'] = cf_results['cf'][index_of_max_npv]
#    
#    if opt_pv_size != 0 or opt_batt_cap != 0: agent['system_built'] = True
#    else: agent['system_built'] = False
#    
#    print "Agent ID:", agent['agent_id'], ". System built:", agent['system_built'], ". Batt:", batt_eval, "d_flat:", tariff.d_flat_exists, "d_tou:", tariff.d_tou_exists, "e_diff:", tariff.e_max_difference 
#    
#    return agent

#%%
def system_size_and_bill_calc_optimal(agent, deprec_sch_df, pv_cf_profile_df, rates_rank_df, rates_json_df):
        
    # Temporary list of rates to ignore
    # TODO remove this when tariffs are updated
    tariffs_to_ignore = np.array([3956, 3962, 3963, 3964])  

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
            tariff_dict = rates_json_df.loc[rate_id, 'rate_json']
            tariff = tFuncs.Tariff(dict_obj=tariff_dict)
            
            #if np.any(rate_id==np.array([4097, 4531, 5274])):
            if tariff.e_n == 0 or np.any(rate_id==tariffs_to_ignore):       
                print "ignored tariff:", rate_id
                agent_rate_list.loc[index, 'bills'] = 9999999999                
            else:
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
    agent['max_pv_size'] = np.min([agent['load_kwh_per_customer_in_bin']/agent['naep'], agent['developable_roof_sqft']*agent['pv_density_w_per_sqft']/1000.0])

    original_bill, original_results = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
    batt = dFuncs.Battery()
    
    # Recalculate the e_max_difference
    # TODO: remove once tariffs are reloaded    
    e_12by24_max_prices_wkday = tariff.e_prices_no_tier[tariff.e_wkday_12by24]
    e_12by24_max_prices_wkend = tariff.e_prices_no_tier[tariff.e_wkend_12by24]
    e_max_price_differential_wkday = np.max(e_12by24_max_prices_wkday, 1) - np.min(e_12by24_max_prices_wkday, 1)
    e_max_price_differential_wkend = np.max(e_12by24_max_prices_wkend, 1) - np.min(e_12by24_max_prices_wkend, 1)
    tariff.e_max_difference = np.max([e_max_price_differential_wkday, e_max_price_differential_wkend])
    
    # Set PV sizes to evaluate
    pv_inc = 3
    pv_sizes = np.linspace(0, agent['max_pv_size']*0.95, pv_inc)
    
    # Set battery sizes to evaluate
    # Only evaluate a battery if there are demand charges or TOU energy charges
    batt_inc = 3
    if tariff.d_flat_exists or tariff.d_tou_exists or tariff.e_max_difference>0.02 or export_tariff.full_retail_nem==False:
        batt_powers = np.linspace(0, np.array(agent['max_demand_kw']) * 0.2, batt_inc)
    else:
        batt_powers = np.zeros(1)
    
    system_sizes = gFuncs.cartesian([pv_sizes, batt_powers])
    
    system_df = pd.DataFrame(system_sizes, columns=['pv', 'batt'])
    system_df['est_bills'] = None
    n_sys = len(system_df)

    d_inc_n = 10    
    DP_inc = 12
    
    for i in system_df.index:    
        pv_size = system_df['pv'][i].copy()
        load_and_pv_profile = load_profile - pv_size*pv_cf_profile
        
        if pv_size<=agent['nem_system_size_limit_kw']:
            export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
        else:
            export_tariff.set_constant_sell_price(0.03)

        batt_power = system_df['batt'][i].copy()
        batt.set_cap_and_power(batt_power*3.0, batt_power)  

        estimator_params = dFuncs.calc_estimator_params(load_and_pv_profile, tariff, export_tariff, batt.eta_charge, batt.eta_discharge)

        estimated_results = dFuncs.determine_optimal_dispatch(load_profile, pv_size*pv_cf_profile, batt, tariff, export_tariff, estimator_params=estimator_params, estimated=True, DP_inc=DP_inc, d_inc_n=d_inc_n, restrict_charge_to_pv_gen=True, estimate_demand_levels=True)
        system_df.loc[i, 'est_bills'] = estimated_results['bill_under_dispatch']   
        
    est_bill_savings = np.zeros([n_sys, agent['analysis_years']+1])
    est_bill_savings[:,1:] = (original_bill - np.array(system_df['est_bills'])).reshape([n_sys, 1])

    # Escalate bill savings for price increase relative to 2016 (when tariffs were curated)
    escalator = (np.zeros(agent['analysis_years']+1) + agent['elec_price_escalator'] + 1)**range(agent['analysis_years']+1)
    est_bill_savings = est_bill_savings * agent['elec_price_multiplier'] * escalator
    system_df['est_bill_savings'] = est_bill_savings[:, 1]
        
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
    if opt_pv_size<=agent['nem_system_size_limit_kw']:
        export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
    else:
        export_tariff.set_constant_sell_price(0.03)
    
    d_inc_n = 20     
    accurate_results = dFuncs.determine_optimal_dispatch(load_profile, opt_pv_size*pv_cf_profile, batt, tariff, export_tariff, estimated=False, d_inc_n=d_inc_n, DP_inc=DP_inc)
    opt_bill = accurate_results['bill_under_dispatch']   
    opt_bill_savings = np.zeros([1, agent['analysis_years']+1])
    opt_bill_savings[:, 1:] = (original_bill - opt_bill)
    opt_bill_savings = opt_bill_savings * agent['elec_price_multiplier'] * escalator

    
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
    
    print "ID:", agent['agent_id'], ", opt PV:", opt_pv_size, np.round(opt_pv_size/agent['max_pv_size'],2), ", opt batt kW:", opt_batt_power, np.round(opt_batt_power/opt_pv_size,2) 
                 
    return agent
    
#%%
def system_size_and_bill_calc_optimal_dict(agent_dict, deprec_sch, pv_cf_profile, agent_rate_list, rates_json_df):
        
    # Temporary list of rates to ignore
    # TODO remove this when tariffs are updated
    tariffs_to_ignore = np.array([3956, 3962, 3963, 3964])  

    # Extract load profile
    load_profile = agent_dict['consumption_hourly']

    # Create export tariff object
    export_tariff = tFuncs.Export_Tariff(full_retail_nem=True) 
    
#    # Filter for list of tariffs available to this agent
#    agent_rate_list = rates_rank_df[rates_rank_df['agent_id']==agent_dict['agent_id']]
#
#    # drop duplicate tariffs - temporary fix uptil Meghan's update comes through
#    agent_rate_list = agent_rate_list.drop_duplicates()  
    
    if len(agent_rate_list > 1):
        # determine which of the tariffs has the cheapest cost of electricity without a system
        agent_rate_list['bills'] = None
        for index in agent_rate_list.index:
            rate_id = agent_rate_list.loc[index, 'rate_id_alias'] 
            tariff_dict = rates_json_df.loc[rate_id, 'rate_json']
            tariff = tFuncs.Tariff(dict_obj=tariff_dict)
            
            #if np.any(rate_id==np.array([4097, 4531, 5274])):
            if tariff.e_n == 0 or np.any(rate_id==tariffs_to_ignore):       
                print "ignored tariff:", rate_id
                agent_rate_list.loc[index, 'bills'] = 9999999999                
            else:
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

    agent_dict['naep'] = float(np.sum(pv_cf_profile))    
    agent_dict['max_pv_size'] = np.min([agent_dict['load_kwh_per_customer_in_bin']/agent_dict['naep'], agent_dict['developable_roof_sqft']*agent_dict['pv_density_w_per_sqft']/1000.0])

    original_bill, original_results = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
    batt = dFuncs.Battery()
    
    # Recalculate the e_max_difference
    # TODO: remove once tariffs are reloaded    
    e_12by24_max_prices_wkday = tariff.e_prices_no_tier[tariff.e_wkday_12by24]
    e_12by24_max_prices_wkend = tariff.e_prices_no_tier[tariff.e_wkend_12by24]
    e_max_price_differential_wkday = np.max(e_12by24_max_prices_wkday, 1) - np.min(e_12by24_max_prices_wkday, 1)
    e_max_price_differential_wkend = np.max(e_12by24_max_prices_wkend, 1) - np.min(e_12by24_max_prices_wkend, 1)
    tariff.e_max_difference = np.max([e_max_price_differential_wkday, e_max_price_differential_wkend])
    
    # Set PV sizes to evaluate
    pv_inc = 3
    pv_sizes = np.linspace(0, agent_dict['max_pv_size']*0.95, pv_inc)
    
    # Set battery sizes to evaluate
    # Only evaluate a battery if there are demand charges or TOU energy charges
    batt_inc = 3
    if tariff.d_flat_exists or tariff.d_tou_exists or tariff.e_max_difference>0.02 or export_tariff.full_retail_nem==False:
        batt_powers = np.linspace(0, np.array(agent_dict['max_demand_kw']) * 0.2, batt_inc)
    else:
        batt_powers = np.zeros(1)
    
    system_sizes = gFuncs.cartesian([pv_sizes, batt_powers])
    
    system_df = pd.DataFrame(system_sizes, columns=['pv', 'batt'])
    system_df['est_bills'] = None
    n_sys = len(system_df)

    d_inc_n = 10    
    DP_inc = 12
    
    for i in system_df.index:    
        pv_size = system_df['pv'][i].copy()
        load_and_pv_profile = load_profile - pv_size*pv_cf_profile
        
        if pv_size<=agent_dict['nem_system_size_limit_kw']:
            export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
        else:
            export_tariff.set_constant_sell_price(0.03)

        batt_power = system_df['batt'][i].copy()
        batt.set_cap_and_power(batt_power*3.0, batt_power)  

        estimator_params = dFuncs.calc_estimator_params(load_and_pv_profile, tariff, export_tariff, batt.eta_charge, batt.eta_discharge)

        estimated_results = dFuncs.determine_optimal_dispatch(load_profile, pv_size*pv_cf_profile, batt, tariff, export_tariff, estimator_params=estimator_params, estimated=True, DP_inc=DP_inc, d_inc_n=d_inc_n, restrict_charge_to_pv_gen=True, estimate_demand_levels=True)
        system_df.loc[i, 'est_bills'] = estimated_results['bill_under_dispatch']   
        
    est_bill_savings = np.zeros([n_sys, agent_dict['analysis_years']+1])
    est_bill_savings[:,1:] = (original_bill - np.array(system_df['est_bills'])).reshape([n_sys, 1])

    # Escalate bill savings for price increase relative to 2016 (when tariffs were curated)
    escalator = (np.zeros(agent_dict['analysis_years']+1) + agent_dict['elec_price_escalator'] + 1)**range(agent_dict['analysis_years']+1)
    est_bill_savings = est_bill_savings * agent_dict['elec_price_multiplier'] * escalator
    system_df['est_bill_savings'] = est_bill_savings[:, 1]
        
    batt_chg_frac = 1.0 # just a placeholder...
    
    cf_results_est = fFuncs.cashflow_constructor(est_bill_savings, 
                         system_sizes[:,0], agent_dict['pv_cost_per_kw'], 0, agent_dict['fixed_om_dollars_per_kw_per_yr'],
                         system_sizes[:,1]*3, system_sizes[:,1], 
                         agent_dict['batt_cost_per_kw'], agent_dict['batt_cost_per_kwh'], 
                         agent_dict['batt_replace_cost_per_kw'], agent_dict['batt_replace_cost_per_kwh'],
                         batt_chg_frac,
                         agent_dict['batt_replace_yr'], agent_dict['batt_om'],
                         agent_dict['sector'], agent_dict['itc_fraction'], deprec_sch, 
                         agent_dict['tax_rate'], 0, agent_dict['discount_rate'],  
                         agent_dict['analysis_years'], agent_dict['inflation'], 
                         agent_dict['down_payment'], agent_dict['loan_rate'], agent_dict['loan_term_yrs'])
                
                                                      
    system_df['npv'] = cf_results_est['npv']
    
    index_of_max_npv = system_df['npv'].idxmax()
    
    opt_pv_size = system_df['pv'][index_of_max_npv].copy()
    opt_batt_power = system_df['batt'][index_of_max_npv].copy()
    opt_batt_cap = opt_batt_power*3.0
    batt.set_cap_and_power(opt_batt_cap, opt_batt_power)    
    load_and_pv_profile = load_profile - opt_pv_size*pv_cf_profile
    if opt_pv_size<=agent_dict['nem_system_size_limit_kw']:
        export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
    else:
        export_tariff.set_constant_sell_price(0.03)
    
    d_inc_n = 20     
    accurate_results = dFuncs.determine_optimal_dispatch(load_profile, opt_pv_size*pv_cf_profile, batt, tariff, export_tariff, estimated=False, d_inc_n=d_inc_n, DP_inc=DP_inc)
    opt_bill = accurate_results['bill_under_dispatch']   
    opt_bill_savings = np.zeros([1, agent_dict['analysis_years']+1])
    opt_bill_savings[:, 1:] = (original_bill - opt_bill)
    opt_bill_savings = opt_bill_savings * agent_dict['elec_price_multiplier'] * escalator

    
    cf_results_opt = fFuncs.cashflow_constructor(opt_bill_savings, 
                     opt_pv_size, agent_dict['pv_cost_per_kw'], 0, agent_dict['fixed_om_dollars_per_kw_per_yr'],
                     opt_batt_power*3, opt_batt_power, 
                     agent_dict['batt_cost_per_kw'], agent_dict['batt_cost_per_kwh'], 
                     agent_dict['batt_replace_cost_per_kw'], agent_dict['batt_replace_cost_per_kwh'],
                     batt_chg_frac,
                     agent_dict['batt_replace_yr'], agent_dict['batt_om'],
                     agent_dict['sector'], agent_dict['itc_fraction'], deprec_sch, 
                     agent_dict['tax_rate'], 0, agent_dict['discount_rate'],  
                     agent_dict['analysis_years'], agent_dict['inflation'], 
                     agent_dict['down_payment'], agent_dict['loan_rate'], agent_dict['loan_term_yrs']) 
               
    agent_dict['pv_kw'] = opt_pv_size
    agent_dict['batt_kw'] = opt_batt_power
    agent_dict['batt_kwh'] = opt_batt_cap
    agent_dict['npv'] = cf_results_opt['npv'][0]
    agent_dict['cash_flow'] = cf_results_opt['cf']
    
    if opt_pv_size != 0 or opt_batt_cap != 0: system_built_bool = True
    else: system_built_bool = False
    
    print "ID:", agent_dict['agent_id'], ", opt PV:", opt_pv_size, np.round(opt_pv_size/agent_dict['max_pv_size'],2), ", opt batt kW:", opt_batt_power, np.round(opt_batt_power/opt_pv_size,2) 
    
    results_dict = {'agent_id':agent_dict['agent_id'],
                    'pv_kw':opt_pv_size,
                    'batt_kw':opt_batt_power,
                    'batt_kwh':opt_batt_cap,
                    'npv':cf_results_opt['npv'][0],
                    'cash_flow':cf_results_opt['cf'][0,:],
                    'system_built':system_built_bool}
             
    return results_dict
    
    
#%%
def system_size_driver(agent_df, deprec_sch_df, pv_cf_profile_df, rates_rank_df, rates_json_df, n_workers=mp.cpu_count()-1):  
    
#    t_apply_start = time.time()    
#    agent_df_old = agent_df.apply(system_size_and_bill_calc_optimal, axis=1, args=(deprec_sch_df, pv_cf_profile_df, rates_rank_df, rates_json_df))
#    t_apply_end = time.time()
#    t_apply = t_apply_end - t_apply_start
    
#    n_workers = 4
    
    agent_dict = agent_df.T.to_dict()
    deprec_sch_dict = deprec_sch_df.T.to_dict()
    pv_cf_profile_dict = pv_cf_profile_df.T.to_dict()
#    rates_rank_dict = rates_rank_df.T.to_dict()
    
    if 'ix' not in os.name:
        EXECUTOR = futures.ThreadPoolExecutor
    else:
        EXECUTOR = futures.ProcessPoolExecutor
        
    future_list = list()
    
    with EXECUTOR(max_workers=n_workers) as executor:
        for key in agent_dict:    
        
            # Filter for list of tariffs available to this agent
            agent_rate_list = rates_rank_df[rates_rank_df['agent_id']==agent_dict[key]['agent_id']].drop_duplicates()
#            agent_rate_list = np.array(agent_rate_list['rate_id_alias'])
            agent_rate_jsons = rates_json_df[rates_json_df.index.isin(np.array(agent_rate_list['rate_id_alias']))]
            
            future_list.append(executor.submit(system_size_and_bill_calc_optimal_dict, agent_dict[key],
                                               np.array(deprec_sch_dict[agent_dict[key]['depreciation_sch_index']]['deprec']),
                                               np.array(pv_cf_profile_dict[agent_dict[key]['resource_index_solar']]['generation_hourly'])/1e6, 
                                               agent_rate_list,
                                               agent_rate_jsons))
    
    results_df = pd.DataFrame([f.result() for f in future_list])

    agent_df = pd.merge(agent_df, results_df, how='left', on=['agent_id'])

    return agent_df
    
        