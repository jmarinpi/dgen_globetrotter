# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import utility_functions as utilfunc
import sys
import config

# Import from support function repo
import dispatch_functions as dFuncs
import tariff_functions as tFuncs

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================
#%%
def calc_system_size_and_financial_performance(agent):
    """
    This function accepts the characteristics of a single agent and
    evaluates the financial performance of a set of solar+storage
    system sizes. The system size with the highest NPV is selected.
            
    Parameters
    ----------
    agent : pandas.Series
        Single agent (row) from an agent dataframe.

    Returns
    -------
    pandas.Series
        Agent with system size, business model and corresponding financial performance.
    """
    #=========================================================================#
    # Setup
    #=========================================================================#

    try:
        in_cols = list(agent.index)

        if config.VERBOSE:
            logger.info(' ')
            logger.info("\tRunning system size calculations for: {}, {}, {}".format(agent['state'], agent['tariff_class'], agent['sector_abbr']))
            logger.info('real_discount: {}'.format(agent['discount_rate']))
            logger.info('loan_rate: {}'.format(agent['loan_rate']))
            logger.info('down_payment: {}'.format(agent['down_payment']))

        # Set resolution of dispatcher    
        d_inc_n_est = 10    
        DP_inc_est = 12
        d_inc_n_acc = 20     
        DP_inc_acc = 12

        # Extract load profile
        load_profile = np.array(agent['consumption_hourly'])    
        agent.loc['timesteps_per_year'] = 1

        # Extract load profile TODO: See Paritosh's SS19 Implementation for better memory usage
        pv_cf_profile = np.array(agent['solar_cf_profile']) / 1e3
        agent['naep'] = float(np.sum(pv_cf_profile))   

        # Create battery object
        batt = dFuncs.Battery()
        batt_ratio = 3.0 
        
        tariff = tFuncs.Tariff(dict_obj=agent.loc['tariff_dict']) #TODO: map this from tariff_dict object for better memory usage

        # Create export tariff object
        if agent['nem_system_size_limit_kw'] != 0:
            export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
            export_tariff.periods_8760 = tariff.e_tou_8760
            export_tariff.prices = tariff.e_prices_no_tier
        else:
            export_tariff = tFuncs.Export_Tariff(full_retail_nem=False)


        original_bill, original_results = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
        if config.VERBOSE:
            logger.info('original_bill: {}'.format(original_bill))

        agent['first_year_elec_bill_without_system'] = original_bill * agent['elec_price_multiplier']
        
        if config.VERBOSE:
            logger.info('multiplied original bill: {}'.format(agent['first_year_elec_bill_without_system']))

        if agent['first_year_elec_bill_without_system'] == 0: 
            agent['first_year_elec_bill_without_system']=1.0
        agent['first_year_elec_cents_per_kwh_without_system'] = agent['first_year_elec_bill_without_system'] / agent['load_per_customer_in_bin_kwh']


        #=========================================================================#
        # Estimate bill savings revenue from a set of solar+storage system sizes
        #=========================================================================#    

        max_size_load = agent.loc['load_per_customer_in_bin_kwh']/agent.loc['naep']
        max_size_roof = agent.loc['developable_roof_sqft'] * agent.loc['developable_buildings_pct'] * agent.loc['pv_power_density_w_per_sqft']/1000.0
        agent.loc['max_pv_size'] = min([max_size_load, max_size_roof, agent.loc['nem_system_size_limit_kw']])
        if config.VERBOSE:
            logger.info('max_size_load: {}'.format(max_size_load))
            logger.info('max_size_roof: {}'.format(max_size_roof))
        dynamic_sizing = True #False

        if dynamic_sizing:
            pv_sizes = np.arange(0, 1.1, 0.1) * agent.loc['max_pv_size']
        else:
            # Size the PV system depending on NEM availability, either to 95% of load w/NEM, or 50% w/o NEM. In both cases, roof size is a constraint.
            if export_tariff.full_retail_nem==True:
                pv_sizes = np.array([min(max_size_load * 0.95, max_size_roof)])
            else:
                pv_sizes = np.array([min(max_size_load * 0.5, max_size_roof)])
                

        batt_powers = np.zeros(1)

        # Calculate the estimation parameters for each PV size
        est_params_df = pd.DataFrame(index=pv_sizes)
        est_params_df['estimator_params'] = 'temp'

        for pv_size in pv_sizes:
            load_and_pv_profile = load_profile - pv_size*pv_cf_profile
            est_params_df.at[pv_size, 'estimator_params'] = dFuncs.calc_estimator_params(load_and_pv_profile, tariff, export_tariff, batt.eta_charge, batt.eta_discharge)
            
        # Create df with all combinations of solar+storage sizes
        system_df = pd.DataFrame(dFuncs.cartesian([pv_sizes, batt_powers]), columns=['pv', 'batt_kw'])
        system_df['est_bills'] = None

        pv_kwh_by_year = np.array([sum(x) for x in np.split(np.array(pv_cf_profile), agent.loc['timesteps_per_year'])])
        pv_kwh_by_year = np.concatenate([(pv_kwh_by_year - ( pv_kwh_by_year * agent.loc['pv_deg'] * i)) for i in range(1, agent.loc['economic_lifetime']+1)])
        system_df['kwh_by_timestep'] = system_df['pv'].apply(lambda x: x * pv_kwh_by_year)

        n_sys = len(system_df)

        for i in system_df.index:    
            pv_size = system_df['pv'][i].copy()
            load_and_pv_profile = load_profile - pv_size*pv_cf_profile

            # for buy all sell all agents: calculate value of generation based on wholesale prices and subtract from original bill
            if agent.loc['compensation_style'] == 'Buy All Sell All':
                sell_all = np.sum(pv_size * pv_cf_profile * agent.loc['wholesale_elec_use_per_kwh'])
                system_df.loc[i, 'est_bills'] = original_bill - sell_all
            
            # for net billing agents: if system size within policy limits, set sell rate to wholesale price -- otherwise, set sell rate to 0
            elif (agent.loc['compensation_style'] == 'Net Billing (Wholesale)') or (agent.loc['compensation_style'] == 'Net Billing (Avoided Cost)'):
                export_tariff = tFuncs.Export_Tariff(full_retail_nem=False)
                if pv_size<=agent.loc['nem_system_size_limit_kw']:
                    if agent.loc['compensation_style'] == 'Net Billing (Wholesale)':
                        export_tariff.set_constant_sell_price(agent.loc['wholesale_elec_usd_per_kwh'])
                    elif agent.loc['compensation_style'] == 'Net Billing (Avoided Cost)':
                        export_tariff.set_constant_sell_price(agent.loc['hourly_excess_sell_rate_usd_per_kwh'])
                else:
                    export_tariff.set_constant_sell_price(0.)
        
                batt_power = system_df['batt_kw'][i].copy()
                batt.set_cap_and_power(batt_power*batt_ratio, batt_power)

                if batt_power > 0:
                    estimator_params = est_params_df.loc[system_df['pv'][i].copy(), 'estimator_params']
                    estimated_results = dFuncs.determine_optimal_dispatch(load_profile, pv_size*pv_cf_profile, batt, tariff, export_tariff, estimator_params=estimator_params, estimated=True, DP_inc=DP_inc_est, d_inc_n=d_inc_n_est, estimate_demand_levels=True)
                    system_df.loc[i, 'est_bills'] = estimated_results['bill_under_dispatch']  
                else:
                    bill_with_PV, _ = tFuncs.bill_calculator(load_and_pv_profile, tariff, export_tariff)
                    system_df.loc[i, 'est_bills'] = bill_with_PV  #+ one_time_charge
        
            # for net metering agents: if system size within policy limits, set full_retail_nem=True -- otherwise set export value to wholesale price
            elif agent.loc['compensation_style'] == 'Net Metering':
                
                if pv_size<=agent.loc['nem_system_size_limit_kw']:
                    export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
                    export_tariff.periods_8760 = tariff.e_tou_8760
                    export_tariff.prices = tariff.e_prices_no_tier
                else:
                    export_tariff = tFuncs.Export_Tariff(full_retail_nem=False)
                    export_tariff.set_constant_sell_price(agent.loc['wholesale_elec_usd_per_kwh'])
        
                batt_power = system_df['batt_kw'][i].copy()
                batt.set_cap_and_power(batt_power*batt_ratio, batt_power)  
        
                if batt_power > 0:
                    estimator_params = est_params_df.loc[system_df['pv'][i].copy(), 'estimator_params']
                    estimated_results = dFuncs.determine_optimal_dispatch(load_profile, pv_size*pv_cf_profile, batt, tariff, export_tariff, estimator_params=estimator_params, estimated=True, DP_inc=DP_inc_est, d_inc_n=d_inc_n_est, estimate_demand_levels=True)
                    system_df.loc[i, 'est_bills'] = estimated_results['bill_under_dispatch']  
                else:
                    bill_with_PV, _ = tFuncs.bill_calculator(load_and_pv_profile, tariff, export_tariff)
                    system_df.loc[i, 'est_bills'] = bill_with_PV  #+ one_time_charge
                
            # for agents with no compensation mechanism: set sell rate to 0 and calculate bill with net load profile
            else:
                
                export_tariff = tFuncs.Export_Tariff(full_retail_nem=False)
                export_tariff.set_constant_sell_price(0.)
                
                batt_power = system_df['batt_kw'][i].copy()
                batt.set_cap_and_power(batt_power*batt_ratio, batt_power)  
        
                if batt_power > 0:
                    estimator_params = est_params_df.loc[system_df['pv'][i].copy(), 'estimator_params']
                    estimated_results = dFuncs.determine_optimal_dispatch(load_profile, pv_size*pv_cf_profile, batt, tariff, export_tariff, estimator_params=estimator_params, estimated=True, DP_inc=DP_inc_est, d_inc_n=d_inc_n_est, estimate_demand_levels=True)
                    system_df.loc[i, 'est_bills'] = estimated_results['bill_under_dispatch']  
                else:
                    bill_with_PV, _ = tFuncs.bill_calculator(load_and_pv_profile, tariff, export_tariff)
                    system_df.loc[i, 'est_bills'] = bill_with_PV #+ one_time_charge
        
        # Calculate bill savings cash flow
        # elec_price_multiplier is the scalar increase in the cost of electricity since 2016, when the tariffs were curated
        # elec_price_escalator is this agent's assumption about how the price of electricity will change in the future.
        avg_est_bill_savings = (original_bill - np.array(system_df['est_bills'])).reshape([n_sys, 1]) * agent['elec_price_multiplier']
        est_bill_savings = np.zeros([n_sys, agent['economic_lifetime']+1])
        est_bill_savings[:,1:] = avg_est_bill_savings
        escalator = (np.zeros(agent['economic_lifetime']+1) + agent['elec_price_escalator'] + 1)**list(range(agent['economic_lifetime']+1))
        degradation = (np.zeros(agent['economic_lifetime']+1) + 1 - agent['pv_deg'])**list(range(agent['economic_lifetime']+1))
        est_bill_savings = est_bill_savings * escalator * degradation
        system_df['est_bill_savings'] = est_bill_savings[:, 1]
        
        # simple representation of 70% minimum of batt charging from PV in order to
        # qualify for the ITC. Here, if batt kW is greater than 25% of PV kW, no ITC.
        batt_chg_frac = np.where(system_df['pv'] >= system_df['batt_kw']*4.0, 1.0, 0)

        #=========================================================================#
        # Determine financial performance of each system size
        #=========================================================================#  

        cash_incentives = np.array([0]*system_df.shape[0])

        if 'state_incentives' in agent.index:
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
            production_based_incentives = np.tile(np.array([0]*agent.loc['economic_lifetime']), (system_df.shape[0],1))

        cf_results_est = cashflow_constructor(bill_savings=est_bill_savings, 
                            pv_size=np.array(system_df['pv']), pv_price=agent.loc['pv_price_per_kw'], pv_om=agent.loc['pv_om_per_kw'],
                            batt_cap=np.array(system_df['batt_kw'])*batt_ratio, batt_power=np.array(system_df['batt_kw']),
                            batt_cost_per_kw=agent.loc['batt_price_per_kw'], batt_cost_per_kwh=agent.loc['batt_price_per_kwh'],
                            batt_om_per_kw=agent.loc['batt_om_per_kw'], batt_om_per_kwh=agent.loc['batt_om_per_kwh'],
                            batt_chg_frac=batt_chg_frac,
                            sector=agent.loc['sector_abbr'], itc=agent.loc['itc_fraction'], deprec_sched=agent.loc['deprec_sch'],
                            fed_tax_rate=agent['tax_rate'], state_tax_rate=0, real_d=agent['discount_rate'],
                            analysis_years=agent.loc['economic_lifetime'], inflation=agent.loc['inflation'],
                            down_payment_fraction=agent.loc['down_payment'], loan_rate=agent.loc['loan_rate'], loan_term=agent.loc['loan_term'],
                            cash_incentives=cash_incentives,ibi=investment_incentives, cbi=capacity_based_incentives, pbi=production_based_incentives)
                        
        system_df['npv'] = cf_results_est['npv']

        #=========================================================================#
        # Select system size and business model for this agent
        #=========================================================================# 
        index_of_best_fin_perform_ho = system_df['npv'].idxmax()
       
        opt_pv_size = system_df['pv'][index_of_best_fin_perform_ho].copy()
        opt_batt_power = system_df['batt_kw'][index_of_best_fin_perform_ho].copy()
        
        opt_batt_cap = opt_batt_power*batt_ratio
        batt.set_cap_and_power(opt_batt_cap, opt_batt_power) 

        tariff = tFuncs.Tariff(dict_obj=agent.loc['tariff_dict'])
    
        # for buy all sell all agents: calculate value of generation based on wholesale prices and subtract from original bill
        if agent.loc['compensation_style'] == 'Buy All Sell All':
            sell_all = np.sum(opt_pv_size * pv_cf_profile * agent.loc['wholesale_elec_usd_per_kwh'])
            opt_bill = original_bill - sell_all
            # package into "dummy" dispatch results dictionary
            accurate_results = {'bill_under_dispatch' : opt_bill, 'batt_dispatch_profile' : np.zeros(len(load_profile))}

        # for net billing agents: if system size within policy limits, set sell rate to wholesale price -- otherwise, set sell rate to 0
        elif (agent.loc['compensation_style'] == 'Net Billing (Wholesale)') or (agent.loc['compensation_style'] == 'Net Billing (Avoided Cost)'):
            export_tariff = tFuncs.Export_Tariff(full_retail_nem=False)
            if opt_pv_size<=agent.loc['nem_system_size_limit_kw']:
                if agent.loc['compensation_style'] == 'Net Billing (Wholesale)':
                        export_tariff.set_constant_sell_price(agent.loc['wholesale_elec_usd_per_kwh'])
                elif agent.loc['compensation_style'] == 'Net Billing (Avoided Cost)':
                    export_tariff.set_constant_sell_price(agent.loc['hourly_excess_sell_rate_usd_per_kwh'])
            else:
                export_tariff.set_constant_sell_price(0.)
            accurate_results = dFuncs.determine_optimal_dispatch(load_profile, opt_pv_size*pv_cf_profile, batt, tariff, export_tariff, estimated=False, d_inc_n=d_inc_n_acc, DP_inc=DP_inc_acc)
    
        # for net metering agents: if system size within policy limits, set full_retail_nem=True -- otherwise set export value to wholesale price
        elif agent.loc['compensation_style'] == 'Net Metering':  
            export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
            if opt_pv_size<=agent.loc['nem_system_size_limit_kw']:
                export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
                export_tariff.periods_8760 = tariff.e_tou_8760
                export_tariff.prices = tariff.e_prices_no_tier
            else:
                export_tariff = tFuncs.Export_Tariff(full_retail_nem=False)
                export_tariff.set_constant_sell_price(agent.loc['wholesale_elec_usd_per_kwh'])
            accurate_results = dFuncs.determine_optimal_dispatch(load_profile, opt_pv_size*pv_cf_profile, batt, tariff, export_tariff, estimated=False, d_inc_n=d_inc_n_acc, DP_inc=DP_inc_acc)

        else:
            export_tariff = tFuncs.Export_Tariff(full_retail_nem=False)
            export_tariff.set_constant_sell_price(0.)
            accurate_results = dFuncs.determine_optimal_dispatch(load_profile, opt_pv_size*pv_cf_profile, batt, tariff, export_tariff, estimated=False, d_inc_n=d_inc_n_acc, DP_inc=DP_inc_acc)

        # add system size class
        system_size_breaks = [0.0, 2.5, 5.0, 10.0, 20.0, 50.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 1500.0, 3000.0]

        #=========================================================================#
        # Determine dispatch trajectory for chosen system size
        #=========================================================================#     
        opt_bill = accurate_results['bill_under_dispatch'] #+ one_time_charge
        agent.loc['first_year_elec_bill_with_system'] = opt_bill * agent.loc['elec_price_multiplier']
        agent.loc['first_year_elec_bill_savings'] = agent.loc['first_year_elec_bill_without_system'] - agent.loc['first_year_elec_bill_with_system']
        agent.loc['first_year_elec_bill_savings_frac'] = agent.loc['first_year_elec_bill_savings'] / agent.loc['first_year_elec_bill_without_system']
        opt_bill_savings = np.zeros([1, agent.loc['economic_lifetime'] + 1])
        opt_bill_savings[:, 1:] = (original_bill - opt_bill)
        opt_bill_savings = opt_bill_savings * agent.loc['elec_price_multiplier'] * escalator * degradation

        # If the batt kW is less than 25% of the PV kW, apply the ITC
        if opt_pv_size >= opt_batt_power*4:
            batt_chg_frac = 1.0
        else:
            batt_chg_frac = 0.0

        cash_incentives = np.array([cash_incentives[index_of_best_fin_perform_ho]])
        investment_incentives = np.array([investment_incentives[index_of_best_fin_perform_ho]])
        capacity_based_incentives = np.array([capacity_based_incentives[index_of_best_fin_perform_ho]])
        production_based_incentives = np.array(production_based_incentives[index_of_best_fin_perform_ho])

        cf_results_opt = cashflow_constructor(bill_savings=opt_bill_savings, 
                     pv_size=opt_pv_size, pv_price=agent.loc['pv_price_per_kw'], pv_om=agent.loc['pv_om_per_kw'],
                     batt_cap=opt_batt_cap, batt_power=opt_batt_power,
                     batt_cost_per_kw=agent.loc['batt_price_per_kw'], batt_cost_per_kwh=agent.loc['batt_price_per_kwh'],
                     batt_om_per_kw=agent['batt_om_per_kw'], batt_om_per_kwh=agent['batt_om_per_kwh'],
                     batt_chg_frac=batt_chg_frac,
                     sector=agent.loc['sector_abbr'], itc=agent.loc['itc_fraction'], deprec_sched=agent.loc['deprec_sch'],
                     fed_tax_rate=agent.loc['tax_rate'], state_tax_rate=0, real_d=agent.loc['discount_rate'],
                     analysis_years=agent.loc['economic_lifetime'], inflation=agent.loc['inflation'],
                     down_payment_fraction=agent.loc['down_payment'], loan_rate=agent.loc['loan_rate'], loan_term=agent.loc['loan_term'],
                     cash_incentives=cash_incentives, ibi=investment_incentives, cbi=capacity_based_incentives, pbi=production_based_incentives)
                     
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
        agent['export_tariff_results'] = original_results

        out_cols = list(agent.index)
        new_cols = [i for i in out_cols if i not in in_cols] + ['agent_id']
        agent = agent.loc[agent.index.isin(new_cols)]

        
    except Exception as e:
        logger.info(' ')
        logger.info('--------------------------------------------')
        logger.info("failed in calc_system_size_and_financial_performance")
        logger.info(('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e), e))
        logger.info('agent that failed')
        logger.info(agent)
        logger.info('--------------------------------------------')
        agent.to_pickle('agent_that_failed.pkl')

    return agent

#%%
def check_incentive_constraints(incentive_data, temp, system_costs):
    raise NotImplementedError("Not implemented yet, would require a dict on the agent called 'state_incentives', see SS19 branch.")

# %%
def calculate_investment_based_incentives(system_df, agent):
    raise NotImplementedError("Not implemented yet, would require a dict on the agent called 'state_incentives', see SS19 branch.")
#%%
def calculate_capacity_based_incentives(system_df, agent):
    raise NotImplementedError("Not implemented yet, would require a dict on the agent called 'state_incentives', see SS19 branch.")
#%%
def calculate_production_based_incentives(system_df, agent, function_templates={}):
    raise NotImplementedError("Not implemented yet, would require a dict on the agent called 'state_incentives', see SS19 branch.")


import numpy as np
np.seterr(divide='ignore', invalid='ignore')
import pandas as pd
import utility_functions as utilfunc
import decorators

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_financial_performance(dataframe):
    """
    Function to calculate the payback period and join it on the agent dataframe.
    
    Parameters
    ----------
    dataframe : pandas.DataFrame
        Agent dataframe
    
    Returns
    -------
    pandas.DataFrame
        Agent dataframe with `payback_period` joined on dataframe
    """
#    dataframe = dataframe.reset_index()

    cfs = np.vstack(dataframe['cash_flow']).astype(np.float)    

    logger.info('** in calc_financial_performance **')
    
    # calculate payback period
    tech_lifetime = np.shape(cfs)[1] - 1

    payback = calc_payback_vectorized(cfs, tech_lifetime)
    # calculate time to double
    ttd = calc_ttd(cfs)

    metric_value = np.where(dataframe['sector_abbr']=='res', payback, ttd)

    dataframe['metric_value'] = metric_value
    
    dataframe = dataframe.set_index('agent_id')
    return dataframe
    
#%%


@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_max_market_share(dataframe, max_market_share_df):
    """
    Calculates the maximum marketshare available for each agent. 
    Parameters
    ----------
    dataframe : pandas.DataFrame
        Attributes
        ----------
        metric_value : float
            
    max_market_share_df : pandas.DataFrame
        Set by :meth:`settings.ScenarioSettings.get_max_marketshare`.
    Returns
    -------
    pandas.DataFrame
        Input DataFrame with `max_market_share` and `metric` columns joined on.
    """

    in_cols = list(dataframe.columns)
    dataframe = dataframe.reset_index()
    
    dataframe['business_model'] = 'host_owned'
    dataframe['metric'] = 'payback_period'
    
    # Convert metric value to integer as a primary key, then bound within max market share ranges
    max_payback = max_market_share_df[max_market_share_df.metric == 'payback_period'].metric_value.max()
    min_payback = max_market_share_df[max_market_share_df.metric == 'payback_period'].metric_value.min()
    max_mbs = max_market_share_df[max_market_share_df.metric == 'percent_monthly_bill_savings'].metric_value.max()
    min_mbs = max_market_share_df[max_market_share_df.metric == 'percent_monthly_bill_savings'].metric_value.min()
    
    # copy the metric valeus to a new column to store an edited version
    metric_value_bounded = dataframe['metric_value'].values.copy()
    
    # where the metric value exceeds the corresponding max market curve bounds, set the value to the corresponding bound
    metric_value_bounded[np.where((dataframe.metric == 'payback_period') & (dataframe['metric_value'] < min_payback))] = min_payback
    metric_value_bounded[np.where((dataframe.metric == 'payback_period') & (dataframe['metric_value'] > max_payback))] = max_payback    
    metric_value_bounded[np.where((dataframe.metric == 'percent_monthly_bill_savings') & (dataframe['metric_value'] < min_mbs))] = min_mbs
    metric_value_bounded[np.where((dataframe.metric == 'percent_monthly_bill_savings') & (dataframe['metric_value'] > max_mbs))] = max_mbs
    dataframe['metric_value_bounded'] = metric_value_bounded

    # scale and round to nearest int    
    dataframe['metric_value_as_factor'] = [int(round(i,1) * 100) for i in dataframe['metric_value_bounded']]
    # add a scaled key to the max_market_share dataframe too
    max_market_share_df['metric_value_as_factor'] = [int(round(float(i), 1) * 100) for i in max_market_share_df['metric_value']]

    # Join the max_market_share table and dataframe in order to select the ultimate mms based on the metric value. 
    dataframe = pd.merge(dataframe, max_market_share_df[['sector_abbr', 'max_market_share','metric_value_as_factor', 'metric', 'business_model']], how = 'left', on = ['sector_abbr','metric_value_as_factor','metric', 'business_model'])

    # Derate the maximum market share for commercial and industrial customers in leased buildings by (2/3)
    # based on the owner occupancy status (1 = owner-occupied, 2 = leased)
    dataframe['max_market_share'] = np.where(dataframe.owner_occupancy_status == 2, dataframe['max_market_share']/3,dataframe['max_market_share'])
    
    # out_cols = in_cols + ['max_market_share', 'metric']    
    out_cols = in_cols + ['max_market_share', 'metric_value_as_factor', 'metric', 'metric_value_bounded']
    return dataframe[out_cols]

def calc_ttd(cfs):
    """
    Calculate time to double investment based on the MIRR. 
    
    This is used for the commercial and industrial sectors.
    
    Parameters
    ----------
    cfs : numpy.ndarray
        Project cash flows ($/yr).
    Returns
    -------
    ttd : numpy.ndarray
        Time to double investment (years).
    """
    irrs = virr(cfs, precision = 0.005, rmin = 0, rmax1 = 0.3, rmax2 = 0.5)
    # suppress errors due to irrs of nan
    with np.errstate(invalid = 'ignore'):
        irrs = np.where(irrs<=0,1e-6,irrs)
    ttd = np.log(2) / np.log(1 + irrs)
    ttd[ttd <= 0] = 0
    ttd[ttd > 30] = 30.1
    # also deal with ttd of nan by setting to max payback period (this should only occur when cashflows = 0)
    if not np.all(np.isnan(ttd) == np.all(cfs == 0, axis = 1)):
        raise Exception("np.nan found in ttd for non-zero cashflows")
    ttd[np.isnan(ttd)] = 30.1
    
    return ttd.round(decimals = 1) # must be rounded to nearest 0.1 to join with max_market_share

#%%
def cashflow_constructor(bill_savings, 
                         pv_size, pv_price, pv_om,
                         batt_cap, batt_power, 
                         batt_cost_per_kw, batt_cost_per_kwh, 
                         batt_om_per_kw, batt_om_per_kwh,
                         batt_chg_frac,
                         sector, itc, deprec_sched, 
                         fed_tax_rate, state_tax_rate, real_d,  
                         analysis_years, inflation, 
                         down_payment_fraction, loan_rate, loan_term, 
                         cash_incentives=np.array([0]), ibi=np.array([0]), cbi=np.array([0]), pbi=np.array([[0]]), print_statements=False):
    """
    Calculate the system cash flows based on the capex, opex, bill savings, incentives, tax implications, and other factors
    
    Parameters
    ----------
    bill_savings : "numpy.ndarray"
        Annual bill savings ($/yr) from system adoption from 1st year through system lifetime
    pv_size : "numpy.float64"
        system capacity selected by agent (kW)
    pv_price : "float"
        system capex ($/kW)
    pv_om : "float"
        system operation and maintanence cost ($/kW)
    batt_cap : "numpy.float64"
        energy capacity of battery selected (kWh)
    batt_power : "numpy.float64"
        demand capacity of battery selected (kW)
    batt_cost_per_kw : "float"
        capex of battery per kW installed ($/kW)
    batt_cost_per_kwh : "float"
        capex of battery per kWh installed ($/kWh)
    batt_om_per_kw : "float"
        opex of battery per kW installed ($/kW-yr)
    batt_om_per_kwh : "float"
        opex of battery per kW installed ($/kWh-yr)
    batt_chg_frac : "int"
        fraction of the battery's energy that it gets from a co-hosted PV system. Used for ITC calculation.
    sector : "str"
        agent sector
    itc : "float"
        fraction of capex offset by federal investment tax credit
    deprec_sched : "list"
        fraction of capex eligible for tax-based depreciation
    fed_tax_rate : "float"
        average tax rate as fraction from federal taxes
    state_tax_rate : "int"
        average tax rate as fraction from state taxes
    real_d : "float"
        annua discount rate in real terms
    analysis_years : "int"
        number of years to use in economic analysis
    inflation : "float"
        annual average inflation rate as fraction e.g. 0.025
    down_payment_fraction : "int"
        fraction of capex used as system down payment
    loan_rate_real : "float"
        real interest rate for debt payments
    loan_term : "int"
        number of years for loan term
    cash_incentives : "numpy.ndarray"
        array describing eligible cash-based incentives e.g. $
    ibi : "numpy.ndarray"
        array describing eligible investment-based incentives e.g. 0.2
    cbi : "numpy.ndarray"
        array describing eligible one-time capacity-based incentives e.g. $/kW
    pbi : "numpy.ndarray"
        array describing eligible ongoing performance-based incentives e.g $/kWh-yr
    
    Returns
    -------
    cf : 'dtype
        Annual cash flows of project investment ($/yr)
    cf_discounted : 'dtype'
        Annual discounted cash flows of project investment ($/yr)
    npv : 'dtype'
        Net present value ($) of project investment using WACC
    bill_savings : 'dtype'
        Nominal cash flow of the annual bill savings over the lifetime of the system
    after_tax_bill_savings : 'dtype'
        Effective after-tax bill savings (electricity costs are tax-deductible for commercial entities)
    pv_cost : 'dtype'
        Capex of system in ($)
    batt_cost : 'dtype'
        Capex of battery in ($)
    installed_cost : 'dtype'
        Combined capex of system + battery
    up_front_cost : 'dtype
        Capex in 0th year as down payment
    batt_om_cf : 'dtype'
        Annual cashflows of battery opex
    operating_expenses : 'dtype'
        Combined annual opex of system + battery ($/yr) 
    pv_itc_value : 'dtype'
        Absolute value of investment tax credit for system ($)
    batt_itc_value : 'dtype'
        Absolute value of investment tax credit for battery ($)
    itc_value : 'dtype'
        Absolute value of investment tax credit for combined system + battery ($)
    deprec_basis : 'dtype'
        Absolute value of depreciable basis of system ($)
    deprec_deductions : 'dtype'
        Annual amount of depreciable capital in given year ($) 
    initial_debt : 'dtype'
        Amount of debt for loan ($)
    annual_principal_and_interest_payment : 'dtype'
        Annual amount of debt service payment, principal + interest ($)
    debt_balance : 'dtype'
        Annual amount of debt remaining in given year ($)
    interest_payments : 'dtype'
        Annual amount of interest payment in given year ($)
    principal_and_interest_payments : 'dtype'
        Array of annual principal and interest payments ($)
    total_taxable_income : 'dtype'
        Amount of stateincome from incentives eligible for taxes
    state_deductions : 'dtype'
        Reduction to state taxable income from interest, operating expenses, or bill savings depending on sector
    total_taxable_state_income_less_deductions : 'dtype'
        Total taxable state income less any applicable deductions
    state_income_taxes : 'dtype'
        Amount of state income tax i.e. net taxable income by tax rate
    fed_deductions : 'dtype'
        Reduction to federal taxable income from interest, operating expenses, or bill savings depending on sector
    total_taxable_fed_income_less_deductions : 'dtype'
        Total taxable federal income less any applicable deductions
    fed_income_taxes : 'dtype'
        Amount of federal income tax i.e. net taxable income by tax rate
    interest_payments_tax_savings : 'dtype'
        Amount of tax savings from deductions of interest payments
    operating_expenses_tax_savings : 'dtype'
        Amount of tax savings from deductions of operating expenses
    deprec_deductions_tax_savings : 'dtype'
        Amount of tax savings from deductions of capital depreciation
    elec_OM_deduction_decrease_tax_liability : 'dtype'
        Amount of tax savings from deductions of electricity costs as deductible business expense
    
    Todo
    ----
    1)  Sales tax basis and rate
    2)  note that sales tax goes into depreciable basis
    3)  Propery taxes (res can deduct from income taxes, I think)
    4)  insurance
    5)  add pre-tax cash flow
    6)  add residential mortgage option
    7)  add carbon tax revenue
    8)  More exhaustive checking. I have confirmed basic formulations against SAM, but there are many permutations that haven't been checked.
    9)  make incentives reduce depreciable basis
    10) add a flag for high incentive levels
    11) battery price schedule, for replacements
    12) improve inverter replacement
    13) improve battery replacement
    14) add inflation adjustment for replacement prices
    15) improve deprec schedule handling
    16) Make financing unique to each agent
    17) Make battery replacements depreciation an input, with default of 7 year MACRS
    18) Have a better way to deal with capacity vs effective capacity and battery costs
    19) Make it so it can accept different loan terms
    """

        #################### Massage inputs ########################################
    # If given just a single value for an agent-specific variable, repeat that
    # variable for each agent. This assumes that the variable is intended to be
    # applied to each agent.

    if np.size(np.shape(bill_savings)) == 1:
        shape = (1, analysis_years + 1)
    else:
        shape = (np.shape(bill_savings)[0], analysis_years + 1)

    n_agents = shape[0]

    if np.size(sector) != n_agents or n_agents == 1: 
        sector = np.repeat(sector, n_agents)
    if np.size(fed_tax_rate) != n_agents or n_agents == 1: 
        fed_tax_rate = np.repeat(fed_tax_rate, n_agents)
    if np.size(state_tax_rate) != n_agents or n_agents == 1: 
        state_tax_rate = np.repeat(state_tax_rate, n_agents)
    if np.size(itc) != n_agents or n_agents == 1: 
        itc = np.repeat(itc, n_agents)
    if np.size(pv_size) != n_agents or n_agents == 1: 
        pv_size = np.repeat(pv_size, n_agents)
    if np.size(pv_price) != n_agents or n_agents == 1: 
        pv_price = np.repeat(pv_price, n_agents)
    if np.size(pv_om) != n_agents or n_agents == 1: 
        pv_om = np.repeat(pv_om, n_agents)
    if np.size(batt_cap) != n_agents or n_agents == 1: 
        batt_cap = np.repeat(batt_cap, n_agents)
    if np.size(batt_power) != n_agents or n_agents == 1: 
        batt_power = np.repeat(batt_power, n_agents)
    if np.size(batt_cost_per_kw) != n_agents or n_agents == 1: 
        batt_cost_per_kw = np.repeat(batt_cost_per_kw, n_agents)
    if np.size(batt_cost_per_kwh) != n_agents or n_agents == 1: 
        batt_cost_per_kwh = np.repeat(batt_cost_per_kwh,n_agents)
    if np.size(batt_chg_frac) != n_agents or n_agents == 1: 
        batt_chg_frac = np.repeat(batt_chg_frac, n_agents)
    if np.size(batt_om_per_kw) != n_agents or n_agents == 1: 
        batt_om_per_kw = np.repeat(batt_om_per_kw, n_agents)
    if np.size(batt_om_per_kwh) != n_agents or n_agents == 1: 
        batt_om_per_kwh = np.repeat(batt_om_per_kwh, n_agents)
    if np.size(real_d) != n_agents or n_agents == 1: 
        real_d = np.repeat(real_d, n_agents)
    if np.size(down_payment_fraction) != n_agents or n_agents == 1: 
        down_payment_fraction = np.repeat(down_payment_fraction, n_agents)
    if np.size(loan_rate) != n_agents or n_agents == 1:
        loan_rate = np.repeat(loan_rate, n_agents)
    if np.size(ibi) != n_agents or n_agents == 1:
        ibi = np.repeat(ibi, n_agents)
    if np.size(cbi) != n_agents or n_agents == 1:
        cbi = np.repeat(cbi, n_agents)
    if len(pbi) != n_agents:
        if len(pbi) > 0:
            pbi = np.tile(pbi[0], (n_agents, 1))
        else:
            pbi = np.tile(np.array([0] * analysis_years), (n_agents, 1))

    if np.array(deprec_sched).ndim == 1 or n_agents == 1:
        deprec_sched = np.array(deprec_sched)

    #################### Setup #########################################
    effective_tax_rate = fed_tax_rate * (1 - state_tax_rate) + state_tax_rate
    if print_statements:
        logger.info('effective_tax_rate')
        logger.info(effective_tax_rate)
        logger.info(' ')
    cf = np.zeros(shape) 
    inflation_adjustment = (1+inflation)**np.arange(analysis_years+1)
    
    #################### Bill Savings #########################################
    # For C&I customers, bill savings are reduced by the effective tax rate,
    # assuming the cost of electricity could have otherwise been counted as an
    # O&M expense to reduce federal and state taxable income.
    bill_savings = bill_savings*inflation_adjustment # Adjust for inflation

    after_tax_bill_savings = np.zeros(shape)
    after_tax_bill_savings = (bill_savings.T * (1 - (sector!='res')*effective_tax_rate)).T # reduce value of savings because they could have otherwise be written off as operating expenses

    cf += bill_savings
    if print_statements:
        logger.info('bill savings cf')
        logger.info(np.sum(cf,1))
        logger.info(' ')
    
    #################### Installed Costs ######################################
    # Assumes that cash incentives, IBIs, and CBIs will be monetized in year 0,
    # reducing the up front installed cost that determines debt levels. 

    pv_cost = pv_size*pv_price     # assume pv_price includes initial inverter purchase
    batt_cost = batt_power*batt_cost_per_kw + batt_cap*batt_cost_per_kwh
    installed_cost = pv_cost + batt_cost
    if print_statements:
        logger.info('installed_cost')
        logger.info(pv_cost)
        logger.info(' ')

    net_installed_cost = installed_cost - cash_incentives - ibi - cbi

    wacc = (((down_payment_fraction*net_installed_cost)/net_installed_cost) * real_d) + ((((1-down_payment_fraction)*net_installed_cost)/net_installed_cost) * loan_rate)

    up_front_cost = net_installed_cost * down_payment_fraction
    if print_statements:
        logger.info('wacc')
        logger.info(wacc)
        logger.info(' ')

    cf[:,0] -= net_installed_cost #all installation costs upfront for WACC
    if print_statements:
        logger.info('bill savings minus up front cost')
        logger.info(np.sum(cf,1))
        logger.info(' ')
    
    #################### Operating Expenses ###################################
    # Nominally includes O&M, replacement costs, fuel, insurance, and property 
    # tax - although currently only includes O&M and replacements.
    # All operating expenses increase with inflation
    operating_expenses_cf = np.zeros(shape)
    batt_om_cf = np.zeros(shape)

    # Battery O&M (replacement costs added to base O&M when costs were ingested)
    batt_om_cf[:,1:] = (batt_power*batt_om_per_kw + batt_cap*batt_om_per_kwh).reshape(n_agents, 1)
    
    # PV O&M
    operating_expenses_cf[:,1:] = (pv_om * pv_size).reshape(n_agents, 1)
    
    operating_expenses_cf += batt_om_cf
    operating_expenses_cf = operating_expenses_cf*inflation_adjustment
    cf -= operating_expenses_cf
    if print_statements:
        logger.info('minus operating expenses')
        logger.info(cf)
        logger.info(' ')
    
    #################### Federal ITC #########################################
    pv_itc_value = pv_cost * itc
    batt_itc_value = batt_cost * itc * batt_chg_frac * (batt_chg_frac>=0.75)
    itc_value = pv_itc_value + batt_itc_value
    # itc value added in fed_tax_savings_or_liability
    if print_statements:
        logger.info('itc value')
        logger.info(itc_value)
        logger.info(' ')

    #################### Depreciation #########################################
    # Per SAM, depreciable basis is sum of total installed cost and total 
    # construction financing costs, less 50% of ITC and any incentives that
    # reduce the depreciable basis.
    deprec_deductions = np.zeros(shape)
    deprec_basis = installed_cost - itc_value * 0.5
    deprec_deductions[:, 1: np.size(deprec_sched) + 1] = np.array([x * deprec_sched.T for x in deprec_basis])
    # to be used later in fed tax calcs
    if print_statements:
        logger.info('deprec_deductions')
        logger.info(deprec_deductions)
        logger.info(' ')
    
    #################### Debt cash flow #######################################
    # Deduct loan interest payments from state & federal income taxes for res 
    # mortgage and C&I. No deduction for res loan.
    # note that the debt balance in year0 is different from principal if there 
    # are any ibi or cbi. Not included here yet.
    # debt balance, interest payment, principal payment, total payment
    
    initial_debt = net_installed_cost - up_front_cost
    if print_statements:
        logger.info('initial_debt')
        logger.info(initial_debt)
        logger.info(' ')

    annual_principal_and_interest_payment = initial_debt * (loan_rate*(1+loan_rate)**loan_term) / ((1+loan_rate)**loan_term - 1)
    if print_statements:
        logger.info('annual_principal_and_interest_payment')
        logger.info(annual_principal_and_interest_payment)
        logger.info(' ')

    debt_balance = np.zeros(shape)
    interest_payments = np.zeros(shape)
    principal_and_interest_payments = np.zeros(shape)
    
    debt_balance[:,:loan_term] = (initial_debt*((1+loan_rate.reshape(n_agents,1))**np.arange(loan_term)).T).T - (annual_principal_and_interest_payment*(((1+loan_rate).reshape(n_agents,1)**np.arange(loan_term) - 1.0)/loan_rate.reshape(n_agents,1)).T).T  
    interest_payments[:,1:] = (debt_balance[:,:-1].T * loan_rate).T
    if print_statements:
        logger.info('interest_payments')
        logger.info(interest_payments)
        logger.info(' ')
        logger.info('sum of interst_payments')
        logger.info(np.sum(interest_payments))
        logger.info(' ')
        logger.info('net_installed_cost')
        logger.info(net_installed_cost)
        logger.info(' ')
        logger.info('sum of net_installed_cost and interest payments')
        logger.info(net_installed_cost + np.sum(interest_payments))
        logger.info(' ')

    principal_and_interest_payments[:,1:loan_term+1] = annual_principal_and_interest_payment.reshape(n_agents, 1)
    if print_statements:
        logger.info('principal_and_interest_payments')
        logger.info(principal_and_interest_payments)
        logger.info(' ')
        logger.info('sum of principal and interest payments, and upfront cost')
        logger.info(np.sum(principal_and_interest_payments) + up_front_cost)
        logger.info(' ')
        logger.info('cf minus intrest payments')
        logger.info(np.sum(cf,1))
        logger.info(' ')
    
    #################### State Income Tax #########################################
    # Per SAM, taxable income is CBIs and PBIs (but not IBIs)
    # Assumes no state depreciation
    # Assumes that revenue from DG is not taxable income
    # total_taxable_income = np.zeros(shape)
    # total_taxable_income[:,1] = cbi
    # total_taxable_income[:,:np.shape(pbi)[1]] += pbi
    total_taxable_income = np.zeros(shape)
    total_taxable_income[:, 1] = cbi
    total_taxable_income[:, 1:] += pbi
    
    state_deductions = np.zeros(shape)
    state_deductions += (interest_payments.T * (sector!='res')).T
    state_deductions += (operating_expenses_cf.T * (sector!='res')).T
    state_deductions -= (bill_savings.T * (sector!='res')).T
    
    total_taxable_state_income_less_deductions = total_taxable_income - state_deductions
    state_income_taxes = (total_taxable_state_income_less_deductions.T * state_tax_rate).T
    
    state_tax_savings_or_liability = -state_income_taxes
    if print_statements:
        logger.info('state_tax_savings')
        logger.info(state_tax_savings_or_liability)
    
    cf += state_tax_savings_or_liability
        
    ################## Federal Income Tax #########################################
    # Assumes all deductions are federal
    fed_deductions = np.zeros(shape)
    fed_deductions += (interest_payments.T * (sector!='res')).T
    fed_deductions += (deprec_deductions.T * (sector!='res')).T
    fed_deductions += state_income_taxes
    fed_deductions += (operating_expenses_cf.T * (sector!='res')).T
    fed_deductions -= (bill_savings.T * (sector!='res')).T
    
    total_taxable_fed_income_less_deductions = total_taxable_income - fed_deductions
    fed_income_taxes = (total_taxable_fed_income_less_deductions.T * fed_tax_rate).T
    
    fed_tax_savings_or_liability_less_itc = -fed_income_taxes
    if print_statements:
        logger.info('federal_tax_savings')
        logger.info(fed_tax_savings_or_liability_less_itc)
    
    cf += fed_tax_savings_or_liability_less_itc
    cf[:,1] += itc_value
    
    
    ######################## Packaging tax outputs ############################
    # interest_payments_tax_savings = (interest_payments.T * effective_tax_rate).T
    operating_expenses_tax_savings = (operating_expenses_cf.T * effective_tax_rate).T
    deprec_deductions_tax_savings = (deprec_deductions.T * fed_tax_rate).T    
    elec_OM_deduction_decrease_tax_liability = (bill_savings.T * effective_tax_rate).T
    
    ########################### Post Processing ###############################
      
    powers = np.zeros(shape, int)
    powers[:,:] = np.array(list(range(analysis_years+1)))

    discounts = np.zeros(shape, float)
    discounts[:,:] = (1/(1+wacc)).reshape(n_agents, 1)
    if print_statements:
        logger.info('discounts')
        logger.info(np.mean(discounts,1))
        logger.info(' ')

    cf_discounted = cf * np.power(discounts, powers)
    cf_discounted = np.nan_to_num(cf_discounted)
    if print_statements:
        logger.info('cf not discounted')
        logger.info(cf)
        logger.info(' ') 

    if print_statements:
        logger.info('cf_discounted')
        logger.info(cf_discounted)
        logger.info(' ')

    npv = np.sum(cf_discounted, 1)
    if print_statements:
        logger.info('npv')
        logger.info(npv)
        logger.info(' ')

    
    ########################### Package Results ###############################
    
    results = {'cf':cf,
               'cf_discounted':cf_discounted,
               'npv':npv,
               'bill_savings':bill_savings,
               'after_tax_bill_savings':after_tax_bill_savings,
               'pv_cost':pv_cost,
               'batt_cost':batt_cost,
               'installed_cost':installed_cost,
               'up_front_cost':up_front_cost,
               'batt_om_cf':batt_om_cf,              
               'operating_expenses':operating_expenses_cf,
               'pv_itc_value':pv_itc_value,
               'batt_itc_value':batt_itc_value,
               'itc_value':itc_value,
               'deprec_basis':deprec_basis,
               'deprec_deductions':deprec_deductions,
               'initial_debt':initial_debt,
               'annual_principal_and_interest_payment':annual_principal_and_interest_payment,
               'debt_balance':debt_balance,
               'interest_payments':interest_payments,
               'principal_and_interest_payments':principal_and_interest_payments,
               'total_taxable_income':total_taxable_income,
               'state_deductions':state_deductions,
               'total_taxable_state_income_less_deductions':total_taxable_state_income_less_deductions,
               'state_income_taxes':state_income_taxes,
               'fed_deductions':fed_deductions,
               'total_taxable_fed_income_less_deductions':total_taxable_fed_income_less_deductions,
               'fed_income_taxes':fed_income_taxes,
            #    'interest_payments_tax_savings':interest_payments_tax_savings,
               'operating_expenses_tax_savings':operating_expenses_tax_savings,
               'deprec_deductions_tax_savings':deprec_deductions_tax_savings,
               'elec_OM_deduction_decrease_tax_liability':elec_OM_deduction_decrease_tax_liability}

    return results

    
#==============================================================================
     
def calc_payback_vectorized(cfs, tech_lifetime):
    """
    Payback calculator.
    Can be either simple payback or discounted payback, depending on whether
    the input cash flow is discounted.    
    
    Parameters
    ----------
    cfs : numpy.ndarray
        Project cash flows ($/yr).
    tech_lifetime : int
        Lifetime of technology used for project.
    
    Returns
    -------
    pp : numpy.ndarray
        Interpolated payback period (years)
    """
    
    years = np.array([np.arange(0, tech_lifetime)] * cfs.shape[0])
    
    cum_cfs = cfs.cumsum(axis = 1)   
    no_payback = np.logical_or(cum_cfs[:, -1] <= 0, np.all(cum_cfs <= 0, axis = 1))
    instant_payback = np.all(cum_cfs > 0, axis = 1)
    neg_to_pos_years = np.diff(np.sign(cum_cfs)) > 0
    base_years = np.amax(np.where(neg_to_pos_years, years, -1), axis = 1)

    # replace values of -1 with 30
    base_years_fix = np.where(base_years == -1, tech_lifetime - 1, base_years)
    base_year_mask = years == base_years_fix[:, np.newaxis]
    
    # base year values
    base_year_values = cum_cfs[:, :-1][base_year_mask]
    next_year_values = cum_cfs[:, 1:][base_year_mask]
    frac_years = base_year_values/(base_year_values - next_year_values)

    pp_year = base_years_fix + frac_years
    pp_precise = np.where(no_payback, tech_lifetime, np.where(instant_payback, 0, pp_year))
    
    pp_final = np.array(pp_precise).round(decimals = 3)
    
    return pp_final
    
#%%
def virr(cfs, precision = 0.005, rmin = 0, rmax1 = 0.3, rmax2 = 0.5):
    """
    Vectorized IRR calculator. 
    
    First calculate a 3D array of the discounted cash flows along cash flow series, time period, and discount rate. Sum over time to 
    collapse to a 2D array which gives the NPV along a range of discount rates 
    for each cash flow series. Next, find crossover where NPV is zero--corresponds
    to the lowest real IRR value. 
    
    Parameters
    ----------
    cfs : numpy.ndarray
        Rows are cash flow series, cols are time periods
    precision : float
        Level of accuracy for the inner IRR band, default value 0.005%
    rmin : float
        Lower bound of the inner IRR band default value 0%
    rmax1 : float
        Upper bound of the inner IRR band default value 30%
    rmax2 : float
        upper bound of the outer IRR band. e.g. 50% Values in the outer 
        band are calculated to 1% precision, IRRs outside the upper band 
        return the rmax2 value.
    
    Returns
    -------
    numpy.ndarray
        IRRs for cash flow series
    Notes
    -----
    For performance, negative IRRs are not calculated, returns "-1" and values are only calculated to an acceptable precision.
    """
    
    if cfs.ndim == 1: 
        cfs = cfs.reshape(1,len(cfs))

    # Range of time periods
    years = np.arange(0,cfs.shape[1])
    
    # Range of the discount rates
    rates_length1 = int((rmax1 - rmin)/precision) + 1
    rates_length2 = int((rmax2 - rmax1)/0.01)
    rates = np.zeros((rates_length1 + rates_length2,))
    rates[:rates_length1] = np.linspace(0,0.3,rates_length1)
    rates[rates_length1:] = np.linspace(0.31,0.5,rates_length2)

    # Discount rate multiplier rows are years, cols are rates
    drm = (1+rates)**-years[:,np.newaxis]

    # Calculate discounted cfs   
    discounted_cfs = cfs[:,:,np.newaxis] * drm
    
    # Calculate NPV array by summing over discounted cashflows
    npv = discounted_cfs.sum(axis = 1)
    
    # Convert npv into boolean for positives (0) and negatives (1)
    signs = npv < 0
    
    # Find the pairwise differences in boolean values
    # sign crosses over, the pairwise diff will be True
    crossovers = np.diff(signs,1,1)
    
    # Extract the irr from the first crossover for each row
    irr = np.min(np.ma.masked_equal(rates[1:]* crossovers,0),1)
    
    # deal with negative irrs
    negative_irrs = cfs.sum(1) < 0
    r = np.where(negative_irrs,-1,irr)
    
    # where the implied irr exceeds 0.5, simply cap it at 0.5
    r = np.where(irr.mask * (negative_irrs == False), 0.5, r)

    # where cashflows are all zero, set irr to nan
    r = np.where(np.all(cfs == 0, axis = 1), np.nan, r)
        
    return r