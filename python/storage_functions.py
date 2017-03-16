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
import financial_functions_elec as fFuncs_dGen
import decorators


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
def calc_system_size_and_financial_performance(agent):
    '''
    Purpose: This function accepts the characteristics of a single agent and
            evaluates the financial performance of a set of solar+storage
            system sizes. The system size with the highest NPV is selected.
            
    Returns: Selected system size and business model and corresponding
            financial performance.
    '''
    #=========================================================================#
    # Setup
    #=========================================================================#
    # Set resolution of dispatcher    
    d_inc_n_est = 10    
    DP_inc_est = 12
    d_inc_n_acc = 20     
    DP_inc_acc = 12

    # Extract load profile
    load_profile = np.array(agent['consumption_hourly'])    

    # Create export tariff object
    if agent['nem_system_size_limit_kw'] != 0: 
        export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
    else:
        export_tariff = tFuncs.Export_Tariff(full_retail_nem=False)

    # Misc. calculations
    pv_cf_profile = np.array(agent['solar_cf_profile']) / 1e6
    agent['naep'] = float(np.sum(pv_cf_profile))    
    agent['max_pv_size'] = np.min([agent['load_kwh_per_customer_in_bin']/agent['naep'], agent['developable_roof_sqft']*agent['pv_power_density_w_per_sqft']/1000.0])

    # Create battery object
    batt = dFuncs.Battery()

    tariff_dict = agent['tariff_dict']
    tariff = tFuncs.Tariff(dict_obj=tariff_dict)

    original_bill, original_results = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
    
    #=========================================================================#
    # Estimate bill savings revenue from a set of solar+storage system sizes
    #=========================================================================#    
    # Set PV sizes to evaluate
#    pv_inc = 1
#    pv_sizes = np.linspace(0, agent['max_pv_size']*0.95, pv_inc)
    if export_tariff.full_retail_nem==True:
        pv_sizes = np.array([agent['max_pv_size']*0.95])
    else:
        pv_sizes = np.array([agent['max_pv_size']*0.5])

    # Set battery sizes to evaluate
    # Only evaluate a battery if there are demand charges, TOU energy charges, or no NEM
    batt_inc = 3
    if tariff.d_flat_exists or tariff.d_tou_exists or tariff.e_max_difference>0.02 or export_tariff.full_retail_nem==False:
        batt_powers = np.linspace(0, np.array(agent['max_demand_kw']) * 0.2, batt_inc)
    else:
        batt_powers = np.zeros(1)
        
    # Calculate the estimation parameters for each PV size
    est_params_df = pd.DataFrame(index=pv_sizes)
    est_params_df['estimator_params'] = 'temp'
    for pv_size in pv_sizes:
        load_and_pv_profile = load_profile - pv_size*pv_cf_profile
        est_params_df.set_value(pv_size, 'estimator_params', dFuncs.calc_estimator_params(load_and_pv_profile, tariff, export_tariff, batt.eta_charge, batt.eta_discharge))
    
    # Create df with all combinations of solar+storage sizes
    system_df = pd.DataFrame(gFuncs.cartesian([pv_sizes, batt_powers]), columns=['pv', 'batt'])
    system_df['est_bills'] = None
    n_sys = len(system_df)
    deg_ten_years = (1 - agent['pv_deg'])**10.0
    
    for i in system_df.index:    
        pv_size = system_df['pv'][i].copy() * deg_ten_years
        load_and_pv_profile = load_profile - pv_size*pv_cf_profile
        
        if pv_size<=agent['nem_system_size_limit_kw']:
            export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
        else:
            export_tariff.set_constant_sell_price(agent['wholesale_elec_price'])

        batt_power = system_df['batt'][i].copy()
        batt.set_cap_and_power(batt_power*3.0, batt_power)  
        batt.set_cycle_deg(1500) #Setting degradation to 86%, which is the weighted "average" capacity over a 10 year period, where weight of degradation is discounted at 8% annually

        if batt_power > 0:
            estimator_params = est_params_df.loc[system_df['pv'][i].copy(), 'estimator_params']
            estimated_results = dFuncs.determine_optimal_dispatch(load_profile, pv_size*pv_cf_profile, batt, tariff, export_tariff, estimator_params=estimator_params, estimated=True, DP_inc=DP_inc_est, d_inc_n=d_inc_n_est, estimate_demand_levels=True)
            system_df.loc[i, 'est_bills'] = estimated_results['bill_under_dispatch']  
        else:
            bill_with_PV, _ = tFuncs.bill_calculator(load_and_pv_profile, tariff, export_tariff)
            system_df.loc[i, 'est_bills'] = bill_with_PV
    
    # Calculate bill savings cash flow
    # elec_price_multiplier is the scalar increase in the cost of electricity since 2016, when the tariffs were curated
    # elec_price_escalator is this agent's assumption about how the price of electricity will change in the future.
    avg_est_bill_savings = (original_bill - np.array(system_df['est_bills'])).reshape([n_sys, 1]) * agent['elec_price_multiplier']
    est_bill_savings = np.zeros([n_sys, agent['economic_lifetime']+1])
    est_bill_savings[:,1:] = avg_est_bill_savings
    escalator = (np.zeros(agent['economic_lifetime']+1) + agent['elec_price_escalator'] + 1)**range(agent['economic_lifetime']+1)
    est_bill_savings = est_bill_savings * escalator
    system_df['est_bill_savings'] = est_bill_savings[:, 1]
        
    # simple representation of 70% minimum of batt charging from PV in order to
    # qualify for the ITC. Here, if batt kW is greater than 25% of PV kW, no ITC.
    batt_chg_frac = np.where(system_df['pv'] >= system_df['batt']*4.0, 1.0, 0)
                
    #=========================================================================#
    # Determine financial performance of each system size
    #=========================================================================#  
    cf_results_est = fFuncs.cashflow_constructor(est_bill_savings, 
                         np.array(system_df['pv']), agent['pv_price_per_kw'], agent['pv_om_per_kw'],
                         np.array(system_df['batt'])*3, np.array(system_df['batt']), 
                         agent['batt_price_per_kw'], agent['batt_price_per_kwh'], 
                         agent['batt_om_per_kw'], agent['batt_om_per_kwh'],
                         batt_chg_frac,
                         agent['sector_abbr'], agent['itc_fraction'], agent['deprec_sch'], 
                         agent['tax_rate'], 0, agent['real_discount'],  
                         agent['economic_lifetime'], agent['inflation'], 
                         agent['down_payment'], agent['loan_rate'], agent['loan_term'])
                    
    system_df['npv'] = cf_results_est['npv']
   
    #=========================================================================#
    # Select system size and business model for this agent
    #=========================================================================# 
    index_of_best_fin_perform_ho = system_df['npv'].idxmax()

    opt_pv_size = system_df['pv'][index_of_best_fin_perform_ho].copy()
    opt_batt_power = system_df['batt'][index_of_best_fin_perform_ho].copy()
    opt_batt_cap = opt_batt_power*3.0
    batt.set_cap_and_power(opt_batt_cap, opt_batt_power) 
    batt.set_cycle_deg(1500)
    load_and_pv_profile = load_profile - opt_pv_size*pv_cf_profile
    if opt_pv_size<=agent['nem_system_size_limit_kw']:
        export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
    else:
        export_tariff.set_constant_sell_price(agent['wholesale_elec_price'])

    #=========================================================================#
    # Determine dispatch trajectory for chosen system size
    #=========================================================================#     
    
    accurate_results = dFuncs.determine_optimal_dispatch(load_profile, opt_pv_size*pv_cf_profile*deg_ten_years, batt, tariff, export_tariff, estimated=False, d_inc_n=d_inc_n_acc, DP_inc=DP_inc_acc)
    opt_bill = accurate_results['bill_under_dispatch']   
    opt_bill_savings = np.zeros([1, agent['economic_lifetime']+1])
    opt_bill_savings[:, 1:] = (original_bill - opt_bill)
    opt_bill_savings = opt_bill_savings * agent['elec_price_multiplier'] * escalator
    if opt_pv_size >= opt_batt_power*4:
        batt_chg_frac = 1.0
    else:
        batt_chg_frac = 0.0

    cf_results_opt = fFuncs.cashflow_constructor(opt_bill_savings, 
                     opt_pv_size, agent['pv_price_per_kw'], agent['pv_om_per_kw'],
                     opt_batt_power*3, opt_batt_power, 
                     agent['batt_price_per_kw'], agent['batt_price_per_kwh'], 
                     agent['batt_om_per_kw'], agent['batt_om_per_kwh'],
                     batt_chg_frac,
                     agent['sector_abbr'], agent['itc_fraction'], agent['deprec_sch'], 
                     agent['tax_rate'], 0, agent['real_discount'],  
                     agent['economic_lifetime'], agent['inflation'], 
                     agent['down_payment'], agent['loan_rate'], agent['loan_term']) 
                     
    #=========================================================================#
    # Package results
    #=========================================================================# 
               
    agent['pv_kw'] = opt_pv_size
    agent['batt_kw'] = opt_batt_power
    agent['batt_kwh'] = opt_batt_cap
    agent['npv'] = cf_results_opt['npv'][0]
    agent['cash_flow'] = cf_results_opt['cf'][0]
    agent['batt_dispatch_profile'] = accurate_results['batt_dispatch_profile']
    
#    print "Opt PV:", opt_pv_size, np.round(opt_pv_size/agent['max_pv_size'],2), ", opt batt kW:", opt_batt_power, np.round(opt_batt_power/opt_pv_size,2) 
    return agent

    
#%%
def system_size_driver(agent_df, deprec_sch_df, rates_rank_df, rates_json_df, n_workers=mp.cpu_count()-1):  
    
    agent_dict = agent_df.T.to_dict()
    deprec_sch_dict = deprec_sch_df.T.to_dict()
    
    if 'ix' not in os.name:
        EXECUTOR = futures.ThreadPoolExecutor
    else:
        EXECUTOR = futures.ProcessPoolExecutor
        
    future_list = list()
    
    with EXECUTOR(max_workers=n_workers) as executor:
        for key in agent_dict:    
        
            
            future_list.append(executor.submit(calc_system_size_and_financial_performance, 
                                               agent_dict[key],
                                               np.array(deprec_sch_dict[agent_dict[key]['depreciation_sch_index']]['deprec'])))
    
    results_df = pd.DataFrame([f.result() for f in future_list])

    agent_df = pd.merge(agent_df, results_df, how='left', on=['agent_id'])

    return agent_df
    
        