# -*- coding: utf-8 -*-
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
import datetime

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
def check_incentive_constraints(incentive_data, temp, system_costs):
    # Reduce the incentive if is is more than the max allowable payment (by percent total costs)
    if not pd.isnull(incentive_data['max_incentive_usd']):
        temp = temp.apply(lambda x: min(x, incentive_data['max_incentive_usd']))

    # Reduce the incentive if is is more than the max allowable payment (by percent of total installed costs)
    if not pd.isnull(incentive_data['max_incentive_pct']):
        temp = temp.combine(system_costs * incentive_data['max_incentive_pct'], min)

    # Set the incentive to zero if it is less than the minimum incentive
    if not pd.isnull(incentive_data['min_incentive_usd']):
        temp = temp * temp.apply(lambda x: int(x > incentive_data['min_incentive_usd']))

    return temp

# %%
def calculate_investment_based_incentives(system_df, agent):
    # Get State Incentives that have a valid Investment Based Incentive value (based on percent of total installed costs)
    cbi_list = agent.loc['state_incentives'].loc[pd.notnull(agent.loc['state_incentives']['ibi_pct'])]

    # Create a empty dataframe to store cumulative ibi's for each system configuration
    result = np.zeros(system_df.shape[0])

    # Loop through each incenctive and add it to the result df
    for row in cbi_list.to_dict('records'):
        if row['tech'] == 'solar':
            # Size filer calls a function to check for valid system size limitations - a boolean so if the size in invalid it will add zero's to the results df
            size_filter = check_minmax(system_df['pv'], row['min_kw'], row['max_kw'])

            # Scale costs based on system size
            system_costs = (system_df['pv'] * agent.loc['pv_price_per_kw'])

        if row['tech'] == 'storage':
            # Size filer calls a function to check for valid system size limitations - a boolean so if the size in invalid it will add zero's to the results df
            size_filter = check_minmax(system_df['batt_kwh'], row['min_kwh'], row['max_kwh'])
            size_filter = size_filter * check_minmax(system_df['batt_kw'], row['min_kw'], row['max_kw'])

            # Calculate system costs
            system_costs = (system_df['batt_kw'] * agent.loc['batt_price_per_kw']) + (system_df['batt_kwh'] * agent.loc['batt_price_per_kwh'])

        # Total incentive
        temp = (system_costs * row['ibi_pct']) * size_filter

        # Add the result to the cumulative total
        result += check_incentive_constraints(row, temp,system_costs)

    return result


#%%
def calculate_capacity_based_incentives(system_df, agent):

    # Get State Incentives that have a valid Capacity Based Incentive value (based on $ per watt)
    cbi_list = agent.loc['state_incentives'].loc[pd.notnull(agent.loc['state_incentives']['cbi_usd_p_w']) | pd.notnull(agent.loc['state_incentives']['cbi_usd_p_wh'])]

    # Create a empty dataframe to store cumulative cbi's for each system configuration
    result = np.zeros(system_df.shape[0])

    # Loop through each incenctive and add it to the result df
    for row in cbi_list.to_dict('records'):

        if row['tech'] == 'solar':
            # Size filer calls a function to check for valid system size limitations - a boolean so if the size in invalid it will add zero's to the results df
            size_filter = check_minmax(system_df['pv'], row['min_kw'], row['max_kw'])

            # Calculate incentives
            temp = (system_df['pv'] * (row['cbi_usd_p_w']*1000)) * size_filter

            # Calculate system costs
            system_costs = system_df['pv'] * agent.loc['pv_price_per_kw']


        if row['tech'] == 'storage' and not np.isnan(row['cbi_usd_p_wh']):
            # Size filer calls a function to check for valid system size limitations - a boolean so if the size in invalid it will add zero's to the results df
            size_filter = check_minmax(system_df['batt_kwh'], row['min_kwh'], row['max_kwh'])
            size_filter = size_filter * check_minmax(system_df['batt_kw'], row['min_kw'], row['max_kw'])

            # Calculate incentives
            temp = row['cbi_usd_p_wh']* system_df['batt_kw'] * 1000  * size_filter

            # Calculate system costs
            system_costs = (system_df['batt_kw'] * agent.loc['batt_price_per_kw']) + (system_df['batt_kwh'] * agent.loc['batt_price_per_kwh'])

        result += check_incentive_constraints(row, temp, system_costs)

    return result

#%%
def check_minmax(value, min_, max_):
    #Returns 1 if the value is within a valid system size limitation - works for single numbers and arrays (assumes valid is system size limitation are not known)

    output = value.apply(lambda x: True)

    if isinstance(min_,float):
        if not np.isnan(min_):
            output = output * value.apply(lambda x: x >= min_)

    if isinstance(max_, float):
        if not np.isnan(max_):
            output = output * value.apply(lambda x: x <= max_)

    return output

#%%
def get_expiration(end_date, current_year, timesteps_per_year):
    #Calculates the timestep at which the end date occurs based on pytoh datetime.date objects and a number of timesteps per year
    return  float(((end_date - datetime.date(current_year, 1, 1)).days / 365.0) * timesteps_per_year)

#%%
def eqn_builder(method,incentive_info, info_params, default_params,additional_data):
    #Builds an equation to scale a series of timestep values
        #method:            'linear_decay' linearly drop from the full price to zero at a given timestep (used for SREC's currently)
        #                   'flat_rate' used as a defualt to keep the consistent value until an endpoint at which point the value is always zero
        #incentive_info:    a row from the agent['state_incentives'] dataframe from which to draw info to customize and equation
        #incentive params:  an array containing the names of the params in agent['state_incentives'] to use in the equation
        #default params:    an array of default values for each incentive param. Entries must match the order of the incentive params.
        #additional_data:    Addtional data can be used to customize the equation

    #Loop through params and grab the default value is the agent['state_incentives'] entry does not have a valid value for it
    for i, r in enumerate(info_params):
        try:
            if np.isnan(incentive_info[r]):
                incentive_info[r] = default_params[i]
        except:
            if incentive_info[r] is None:
                incentive_info[r] = default_params[i]

    pbi_usd_p_kwh = float(incentive_info[info_params[0]])
    years = float(incentive_info[info_params[1]])
    end_date = incentive_info[info_params[2]]

    current_year = int(additional_data[0])
    timesteps_per_year = float(additional_data[1])

    #Get the timestep at which the incentive expires
    try:
        #Find expiration timestep by explict program end date
        expiration = get_expiration(end_date, current_year, timesteps_per_year)
    except:
        #Assume the incetive applies for all years if there is an error in the previous step
        expiration = years * timesteps_per_year

    #Reduce the expiration if there is a cap on the number of years the incentive can be applied
    expiration = min(years * timesteps_per_year, expiration)

    if method =='linear_decay':
        #Linear decline to zero at expiration
        def function(ts):
            if ts > expiration:
                return  0.0
            else:
                if expiration - ts < 1:
                    fraction = expiration - ts
                else:
                    fraction = 1
                return fraction * (pbi_usd_p_kwh + ((-1 * (pbi_usd_p_kwh / expiration) * ts)))

        return function


    if method == 'flat_rate':
        # Flat rate until expiration, and then zero
        def function(ts):
            if ts > expiration:
                return 0.0
            else:
                if expiration - ts < 1:
                    fraction = expiration - ts
                else:
                    fraction = 1

                return fraction * pbi_usd_p_kwh

        return function

#%%
def eqn_linear_decay_to_zero(incentive_info, info_params, default_params,additional_params):
    return eqn_builder('linear_decay',incentive_info, info_params, default_params,additional_params)

#%%
def eqn_flat_rate(incentive_info, info_params, default_params,additional_params):
    return eqn_builder('flat_rate', incentive_info, info_params, default_params,additional_params)

#%%
def calculate_production_based_incentives(system_df, agent, function_templates={}):

    # Get State Incentives that have a valid Production Based Incentive value
    pbi_list = agent.loc['state_incentives'].loc[pd.notnull(agent.loc['state_incentives']['pbi_usd_p_kwh'])]

    # Create a empty dataframe to store cumulative pbi's for each system configuration (each system should have an array as long as the number of years times the number of timesteps per year)
    result = np.tile( np.array([0]*agent.loc['economic_lifetime']*agent.loc['timesteps_per_year']), (system_df.shape[0],1))

    #Loop through incentives
    for row in pbi_list.to_dict('records'):
        #Build boolean array to express if system sizes are valid
        size_filter = check_minmax(system_df['pv'], row['min_kw'], row['max_kw'])

        if row['tech'] == 'solar':
            # Get the incentive type - this should match a key in the function dictionary
            if row['incentive_type'] in function_templates.keys():
                f_name = row['incentive_type']
            else:
                f_name = 'default'

            # Grab infomation about the incentive from the function template
            fn = function_templates[f_name]

            # Vectorize the function
            f =  np.vectorize(fn['function'](row,fn['row_params'],fn['default_params'],fn['additional_params']))

            # Apply the function to each row (containing an array of timestep values)
            temp = system_df['kwh_by_timestep'].apply(lambda x: x * f(range(0,len(x))))

            #Add the pbi the cumulative total
            result = result + list(temp * size_filter)

    #Sum the incentive at each timestep by year for each system size
    result =  [np.array(map(lambda x: sum(x), np.split(x,agent.loc['economic_lifetime'] ))) for x in result]

    return result

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
    print agent.loc['county_id']
    # Set resolution of dispatcher    
    d_inc_n_est = 10    
    DP_inc_est = 12
    d_inc_n_acc = 20     
    DP_inc_acc = 12

    # Extract load profile
    load_profile = np.array(agent.loc['consumption_hourly'])
    agent.loc['timesteps_per_year'] = 1

    # Misc. calculations
    pv_cf_profile = np.array(agent['solar_cf_profile']) / 1e6
    agent.loc['naep'] = float(np.sum(pv_cf_profile))
    agent.loc['max_pv_size'] = np.min([agent.loc['load_kwh_per_customer_in_bin']/agent.loc['naep'], agent.loc['developable_roof_sqft']*agent.loc['pv_power_density_w_per_sqft']/1000.0])

    # Create battery object
    batt = dFuncs.Battery()
    batt_ratio = 3.0

    tariff = tFuncs.Tariff(dict_obj=agent.loc['tariff_dict'])

    # Create export tariff object
    if agent.loc['pv_kw_limit'] != 0:
        export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
        export_tariff.periods_8760 = tariff.e_tou_8760
        export_tariff.prices = tariff.e_prices_no_tier
    else:
        export_tariff = tFuncs.Export_Tariff(full_retail_nem=False)

    original_bill, original_results = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
    agent.loc['fy_bill_without_sys'] = original_bill * agent.loc['elec_price_multiplier']
    if agent.loc['fy_bill_without_sys'] == 0: agent.loc['fy_bill_without_sys']=1.0
    agent.loc['fy_elec_cents_per_kwh_without_sys'] = agent.loc['fy_bill_without_sys'] / agent.loc['load_kwh_per_customer_in_bin']

    #=========================================================================#
    # Estimate bill savings revenue from a set of solar+storage system sizes
    #=========================================================================#    
    # Set PV sizes to evaluate
    if export_tariff.full_retail_nem==True:
        pv_sizes = np.array([agent.loc['max_pv_size']*0.95])
    else:
        pv_sizes = np.array([agent.loc['max_pv_size']*0.5])

    # Set battery sizes to evaluate
    # Only evaluate a battery if there are demand charges, TOU energy charges, or no NEM
    batt_inc = 3
    if tariff.d_flat_exists or tariff.d_tou_exists or tariff.e_max_difference>0.02 or export_tariff.full_retail_nem==False:
        batt_powers = np.linspace(0, np.array(agent.loc['max_demand_kw']) * 0.2, batt_inc)
    else:
        batt_powers = np.zeros(1)
        
    # Calculate the estimation parameters for each PV size
    est_params_df = pd.DataFrame(index=pv_sizes)
    est_params_df['estimator_params'] = 'temp'
    for pv_size in pv_sizes:
        load_and_pv_profile = load_profile - pv_size*pv_cf_profile
        est_params_df.set_value(pv_size, 'estimator_params', dFuncs.calc_estimator_params(load_and_pv_profile, tariff, export_tariff, batt.eta_charge, batt.eta_discharge))
    
    # Create df with all combinations of solar+storage sizes
    system_df = pd.DataFrame(gFuncs.cartesian([pv_sizes, batt_powers]), columns=['pv', 'batt_kw'])
    system_df['est_bills'] = None

    pv_kwh_by_year = np.array(map(lambda x: sum(x), np.split(np.array(pv_cf_profile), agent.loc['timesteps_per_year'])))
    pv_kwh_by_year = np.concatenate([(pv_kwh_by_year - ( pv_kwh_by_year * agent.loc['pv_deg'] * i)) for i in range(1, agent.loc['economic_lifetime']+1)])
    system_df['kwh_by_timestep'] = system_df['pv'].apply(lambda x: x * pv_kwh_by_year)

    n_sys = len(system_df)
    
    for i in system_df.index:    
        pv_size = system_df['pv'][i].copy()
        load_and_pv_profile = load_profile - pv_size*pv_cf_profile
        
        if pv_size<=agent.loc['pv_kw_limit']:
            export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
            export_tariff.periods_8760 = tariff.e_tou_8760
            export_tariff.prices = tariff.e_prices_no_tier
        else:
            export_tariff.set_constant_sell_price(agent.loc['wholesale_elec_price'])

        batt_power = system_df['batt_kw'][i].copy()
        batt.set_cap_and_power(batt_power*batt_ratio, batt_power)  

        if batt_power > 0:
            estimator_params = est_params_df.loc[system_df['pv'][i].copy(), 'estimator_params']
            estimated_results = dFuncs.determine_optimal_dispatch(load_profile, pv_size*pv_cf_profile, batt, tariff, export_tariff, estimator_params=estimator_params, estimated=True, DP_inc=DP_inc_est, d_inc_n=d_inc_n_est, estimate_demand_levels=True)
            system_df.loc[i, 'est_bills'] = estimated_results['bill_under_dispatch']  
        else:
            bill_with_PV, _ = tFuncs.bill_calculator(load_and_pv_profile, tariff, export_tariff)
            system_df.loc[i, 'est_bills'] = bill_with_PV

    system_df['batt_kwh'] = system_df['batt_kw'] * batt_ratio
    
    # Calculate bill savings cash flow
    # elec_price_multiplier is the scalar increase in the cost of electricity since 2016, when the tariffs were curated
    # elec_price_escalator is this agent's assumption about how the price of electricity will change in the future.
    avg_est_bill_savings = (original_bill - np.array(system_df['est_bills'])).reshape([n_sys, 1]) * agent.loc['elec_price_multiplier']
    est_bill_savings = np.zeros([n_sys, agent.loc['economic_lifetime']+1])
    est_bill_savings[:,1:] = avg_est_bill_savings
    escalator = (np.zeros(agent.loc['economic_lifetime']+1) + agent.loc['elec_price_escalator'] + 1)**range(agent.loc['economic_lifetime']+1)
    degradation = (np.zeros(agent.loc['economic_lifetime']+1) + 1 - agent.loc['pv_deg'])**range(agent.loc['economic_lifetime']+1)
    est_bill_savings = est_bill_savings * escalator * degradation
    system_df['est_bill_savings'] = est_bill_savings[:, 1]
        
    # simple representation of 70% minimum of batt charging from PV in order to
    # qualify for the ITC. Here, if batt kW is greater than 25% of PV kW, no ITC.
    batt_chg_frac = np.where(system_df['pv'] >= system_df['batt_kw']*4.0, 1.0, 0)
    
    if agent.loc['year'] <= 2016: cash_incentives = np.array(system_df['pv']) * agent.loc['pv_price_per_kw'] * 0.3
    else: cash_incentives = np.array([0]*system_df.shape[0])
        
    #=========================================================================#
    # Determine financial performance of each system size
    #=========================================================================#

    if not isinstance(agent.loc['state_incentives'],float):
        investment_incentives = calculate_investment_based_incentives(system_df, agent)
        capacity_based_incentives = calculate_capacity_based_incentives(system_df, agent)

        default_expiration = datetime.date(agent.loc['year'] + agent.loc['economic_lifetime'],1,1)
        pbi_by_timestep_functions = {
                                    "default":
                                            {   'function':eqn_flat_rate,
                                                'row_params':['pbi_usd_p_kwh','incentive_duration_yrs','end_date'],
                                                'default_params':[0, agent.loc['economic_lifetime'], default_expiration],
                                                'additional_params':[agent.loc['year'], agent.loc['timesteps_per_year']]},
                                    "SREC":
                                            {   'function':eqn_linear_decay_to_zero,
                                                'row_params':['pbi_usd_p_kwh','incentive_duration_yrs','end_date'],
                                                'default_params':[0, 10, default_expiration],
                                                'additional_params':[agent.loc['year'], agent.loc['timesteps_per_year']]}
                                      }
        production_based_incentives =  calculate_production_based_incentives(system_df, agent, function_templates=pbi_by_timestep_functions)
    else:
        investment_incentives = np.zeros(system_df.shape[0])
        capacity_based_incentives = np.zeros(system_df.shape[0])
        production_based_incentives = np.tile( np.array([0]*agent.loc['economic_lifetime']), (system_df.shape[0],1))

    cf_results_est = fFuncs.cashflow_constructor(est_bill_savings, 
                         np.array(system_df['pv']), agent.loc['pv_price_per_kw'], agent.loc['pv_om_per_kw'],
                         np.array(system_df['batt_kw'])*batt_ratio, np.array(system_df['batt_kw']),
                         agent.loc['batt_price_per_kw'], agent.loc['batt_price_per_kwh'],
                         agent.loc['batt_om_per_kw'], agent.loc['batt_om_per_kwh'],
                         batt_chg_frac,
                         agent.loc['sector_abbr'], agent.loc['itc_fraction'], agent.loc['deprec_sch'],
                         agent['tax_rate'], 0, agent['real_discount'],
                         agent.loc['economic_lifetime'], agent.loc['inflation'],
                         agent.loc['down_payment'], agent.loc['loan_rate'], agent.loc['loan_term'],
                         cash_incentives,investment_incentives, capacity_based_incentives, production_based_incentives)
                    
    system_df['npv'] = cf_results_est['npv']
   
    #=========================================================================#
    # Select system size and business model for this agent
    #=========================================================================# 
    index_of_best_fin_perform_ho = system_df['npv'].idxmax()

    opt_pv_size = system_df['pv'][index_of_best_fin_perform_ho].copy()
    opt_batt_power = system_df['batt_kw'][index_of_best_fin_perform_ho].copy()

    opt_batt_cap = opt_batt_power*batt_ratio
    batt.set_cap_and_power(opt_batt_cap, opt_batt_power) 
    #load_and_pv_profile = load_profile - opt_pv_size*pv_cf_profile  not used

    if opt_pv_size<=agent.loc['pv_kw_limit']:
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
    agent.loc['fy_bill_with_sys'] = opt_bill * agent.loc['elec_price_multiplier']
    agent.loc['fy_bill_savings'] = agent.loc['fy_bill_without_sys'] - agent.loc['fy_bill_with_sys']
    agent.loc['fy_bill_savings_frac'] = agent.loc['fy_bill_savings'] / agent.loc['fy_bill_without_sys']
    opt_bill_savings = np.zeros([1, agent.loc['economic_lifetime']+1])
    opt_bill_savings[:, 1:] = (original_bill - opt_bill)
    opt_bill_savings = opt_bill_savings * agent.loc['elec_price_multiplier'] * escalator * degradation
    
    # If the batt kW is less than 25% of the PV kW, apply the ITC
    batt_chg_frac = int( opt_batt_power/opt_pv_size < 0.25)

    cash_incentives = np.array([cash_incentives[index_of_best_fin_perform_ho]])
    investment_incentives = np.array(investment_incentives[index_of_best_fin_perform_ho])
    capacity_based_incentives = np.array(capacity_based_incentives[index_of_best_fin_perform_ho])
    production_based_incentives = np.array(production_based_incentives[index_of_best_fin_perform_ho])
    
    cf_results_opt = fFuncs.cashflow_constructor(opt_bill_savings, 
                     opt_pv_size, agent.loc['pv_price_per_kw'], agent.loc['pv_om_per_kw'],
                     opt_batt_cap, opt_batt_power,
                     agent.loc['batt_price_per_kw'], agent.loc['batt_price_per_kwh'],
                     agent['batt_om_per_kw'], agent['batt_om_per_kwh'],
                     batt_chg_frac,
                     agent.loc['sector_abbr'], agent.loc['itc_fraction'], agent.loc['deprec_sch'],
                     agent.loc['tax_rate'], 0, agent.loc['real_discount'],
                     agent.loc['economic_lifetime'], agent.loc['inflation'],
                     agent.loc['down_payment'], agent.loc['loan_rate'], agent.loc['loan_term'],
                     cash_incentives, investment_incentives, capacity_based_incentives, production_based_incentives)
                     
    #=========================================================================#
    # Package results
    #=========================================================================# 

    agent.loc['pv_kw'] = opt_pv_size
    agent.loc['batt_kw'] = opt_batt_power
    agent.loc['batt_kwh'] = opt_batt_cap
    agent.loc['npv'] = cf_results_opt['npv'][0]
    agent.loc['cash_flow'] = cf_results_opt['cf'][0]
    agent.loc['batt_dispatch_profile'] = accurate_results['batt_dispatch_profile']

    agent.loc['bill_savings'] = opt_bill_savings
    agent.loc['aep'] = agent.loc['pv_kw'] * agent.loc['naep']
    agent.loc['cf'] = agent.loc['naep']/8760
    agent.loc['system_size_factors'] = np.where(agent.loc['pv_kw'] == 0, 0, pd.cut([agent.loc['pv_kw']], system_size_breaks))[0]
        
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
