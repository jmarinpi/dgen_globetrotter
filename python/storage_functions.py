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
    print agent['county_id']
    # Set resolution of dispatcher    
    d_inc_n_est = 10    
    DP_inc_est = 12
    d_inc_n_acc = 20     
    DP_inc_acc = 12

    # Extract load profile
    load_profile = np.array(agent['consumption_hourly'])    

    # Misc. calculations
    pv_cf_profile = np.array(agent['solar_cf_profile']) / 1e6
    agent['naep'] = float(np.sum(pv_cf_profile))    
    agent['max_pv_size'] = np.min([agent['load_kwh_per_customer_in_bin']/agent['naep'], agent['developable_roof_sqft']*agent['pv_power_density_w_per_sqft']/1000.0])

    # Create battery object
    batt = dFuncs.Battery()
    batt_ratio = 3.0

    tariff = tFuncs.Tariff(dict_obj=agent['tariff_dict'])

    # Create export tariff object
    if agent['nem_system_size_limit_kw'] != 0: 
        export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
        export_tariff.periods_8760 = tariff.e_tou_8760
        export_tariff.prices = tariff.e_prices_no_tier
    else:
        export_tariff = tFuncs.Export_Tariff(full_retail_nem=False)

    original_bill, original_results = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
    agent['fy_bill_without_sys'] = original_bill * agent['elec_price_multiplier']
    if agent['fy_bill_without_sys'] == 0: agent['fy_bill_without_sys']=1.0
    agent['fy_elec_cents_per_kwh_without_sys'] = agent['fy_bill_without_sys'] / agent['load_kwh_per_customer_in_bin']

    #=========================================================================#
    # Estimate bill savings revenue from a set of solar+storage system sizes
    #=========================================================================#    
    # Set PV sizes to evaluate
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
    
    for i in system_df.index:    
        pv_size = system_df['pv'][i].copy()
        load_and_pv_profile = load_profile - pv_size*pv_cf_profile
        
        if pv_size<=agent['nem_system_size_limit_kw']:
            export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
            export_tariff.periods_8760 = tariff.e_tou_8760
            export_tariff.prices = tariff.e_prices_no_tier
        else:
            export_tariff.set_constant_sell_price(agent['wholesale_elec_price'])

        batt_power = system_df['batt'][i].copy()
        batt.set_cap_and_power(batt_power*batt_ratio, batt_power)  

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
    degradation = (np.zeros(agent['economic_lifetime']+1) + 1 - agent['pv_deg'])**range(agent['economic_lifetime']+1)
    est_bill_savings = est_bill_savings * escalator * degradation
    system_df['est_bill_savings'] = est_bill_savings[:, 1]
        
    # simple representation of 70% minimum of batt charging from PV in order to
    # qualify for the ITC. Here, if batt kW is greater than 25% of PV kW, no ITC.
    batt_chg_frac = np.where(system_df['pv'] >= system_df['batt']*4.0, 1.0, 0)
    
    if agent['year'] <= 2016: cash_incentives = np.array(system_df['pv']) * agent['pv_price_per_kw'] * 0.3
    else: cash_incentives = np.array([0])
        
    #=========================================================================#
    # Determine financial performance of each system size
    #=========================================================================#  
    cf_results_est = fFuncs.cashflow_constructor(est_bill_savings, 
                         np.array(system_df['pv']), agent['pv_price_per_kw'], agent['pv_om_per_kw'],
                         np.array(system_df['batt'])*batt_ratio, np.array(system_df['batt']), 
                         agent['batt_price_per_kw'], agent['batt_price_per_kwh'], 
                         agent['batt_om_per_kw'], agent['batt_om_per_kwh'],
                         batt_chg_frac,
                         agent['sector_abbr'], agent['itc_fraction'], agent['deprec_sch'], 
                         agent['tax_rate'], 0, agent['real_discount'],  
                         agent['economic_lifetime'], agent['inflation'], 
                         agent['down_payment'], agent['loan_rate'], agent['loan_term'],
                         cash_incentives=cash_incentives)
                    
    system_df['npv'] = cf_results_est['npv']
   
    #=========================================================================#
    # Select system size and business model for this agent
    #=========================================================================# 
    index_of_best_fin_perform_ho = system_df['npv'].idxmax()

    opt_pv_size = system_df['pv'][index_of_best_fin_perform_ho].copy()
    opt_batt_power = system_df['batt'][index_of_best_fin_perform_ho].copy()
    opt_batt_cap = opt_batt_power*batt_ratio
    batt.set_cap_and_power(opt_batt_cap, opt_batt_power) 
    load_and_pv_profile = load_profile - opt_pv_size*pv_cf_profile
    if opt_pv_size<=agent['nem_system_size_limit_kw']:
        export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
        export_tariff.periods_8760 = tariff.e_tou_8760
        export_tariff.prices = tariff.e_prices_no_tier
    else:
        export_tariff.set_constant_sell_price(agent['wholesale_elec_price'])

    # add system size class
    system_size_breaks = [0.0, 2.5, 5.0, 10.0, 20.0, 50.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 1500.0, 3000.0]
    
    #=========================================================================#
    # Determine dispatch trajectory for chosen system size
    #=========================================================================#     
    
    accurate_results = dFuncs.determine_optimal_dispatch(load_profile, opt_pv_size*pv_cf_profile, batt, tariff, export_tariff, estimated=False, d_inc_n=d_inc_n_acc, DP_inc=DP_inc_acc)
    opt_bill = accurate_results['bill_under_dispatch']   
    agent['fy_bill_with_sys'] = opt_bill * agent['elec_price_multiplier']
    agent['fy_bill_savings'] = agent['fy_bill_without_sys'] - agent['fy_bill_with_sys']
    agent['fy_bill_savings_frac'] = agent['fy_bill_savings'] / agent['fy_bill_without_sys']
    opt_bill_savings = np.zeros([1, agent['economic_lifetime']+1])
    opt_bill_savings[:, 1:] = (original_bill - opt_bill)
    opt_bill_savings = opt_bill_savings * agent['elec_price_multiplier'] * escalator * degradation
    
    # If the batt kW is less than 25% of the PV kW, apply the ITC
    if opt_pv_size >= opt_batt_power*4:
        batt_chg_frac = 1.0
    else:
        batt_chg_frac = 0.0


    if agent['year'] <= 2016: cash_incentives = np.array([opt_pv_size * agent['pv_price_per_kw'] * 0.3])
    else: cash_incentives = np.array([0])
    
    cf_results_opt = fFuncs.cashflow_constructor(opt_bill_savings, 
                     opt_pv_size, agent['pv_price_per_kw'], agent['pv_om_per_kw'],
                     opt_batt_cap, opt_batt_power, 
                     agent['batt_price_per_kw'], agent['batt_price_per_kwh'], 
                     agent['batt_om_per_kw'], agent['batt_om_per_kwh'],
                     batt_chg_frac,
                     agent['sector_abbr'], agent['itc_fraction'], agent['deprec_sch'], 
                     agent['tax_rate'], 0, agent['real_discount'],  
                     agent['economic_lifetime'], agent['inflation'], 
                     agent['down_payment'], agent['loan_rate'], agent['loan_term'],
                     cash_incentives=cash_incentives) 
                     
    #=========================================================================#
    # Package results
    #=========================================================================# 
               
    agent['pv_kw'] = opt_pv_size
    agent['batt_kw'] = opt_batt_power
    agent['batt_kwh'] = opt_batt_cap
    agent['npv'] = cf_results_opt['npv'][0]
    agent['cash_flow'] = cf_results_opt['cf'][0]
    agent['batt_dispatch_profile'] = accurate_results['batt_dispatch_profile']
    
    agent['bill_savings'] = opt_bill_savings
    agent['aep'] = agent['pv_kw'] * agent['naep']
    agent['cf'] = agent['naep']/8760
    agent['system_size_factors'] = np.where(agent['pv_kw'] == 0, 0, pd.cut([agent['pv_kw']], system_size_breaks))[0]
        
    out_cols = ['agent_id',
                'pv_kw',
                'batt_kw',
                'batt_kwh',
                'npv',
                'cash_flow',
                'batt_dispatch_profile',
                'aep',
                'naep',
                'cf',
                'system_size_factors',
                'fy_bill_with_sys',
                'fy_bill_savings',
                'fy_bill_savings_frac',
                'max_pv_size',
                'fy_bill_without_sys',
                'fy_elec_cents_per_kwh_without_sys']
            
#    print "Opt PV:", opt_pv_size, np.round(opt_pv_size/agent['max_pv_size'],2), ", opt batt kW:", opt_batt_power, np.round(opt_batt_power/opt_pv_size,2) 
    return agent[out_cols]
