# -*- coding: utf-8 -*-
"""
Edited Monday Nov 5, 218
@author: tkwasnik
"""

import pandas as pd
import numpy as np
import decorators
import utility_functions as utilfunc


# GLOBAL SETTINGS
# load logger
logger = utilfunc.get_logger()

@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def aggregate_outputs_solar(agent_df, year, is_first_year,
                            scenario_settings,
                            interyear_results_aggregations=None):
                                
    ''' 
    Aggregate agent-level results into ba-level results for the given year. 
    
    
    '''

    # Unpack results dict from previous years
    if interyear_results_aggregations != None:
        ba_cum_pv_mw = interyear_results_aggregations['ba_cum_pv_mw']                                  #==========================================================================================================
    # Set up objects
    #==========================================================================================================   
    ba_list = np.unique(np.array(agent_df['control_reg']))
        
    col_list_8760 = list(['control_reg', 'year'])
    hour_list = list(np.arange(1,8761))
    col_list_8760 = col_list_8760 + hour_list
    
    if is_first_year == True:  
        # PV and batt capacities
        ba_cum_pv_mw = pd.DataFrame(index=ba_list)
    
    # Set up for groupby
    agent_df['index'] = range(len(agent_df))
    agent_df_to_group = agent_df[['control_reg', 'index']]
    agents_grouped = agent_df_to_group.groupby(['control_reg']).aggregate(lambda x: tuple(x))
    
    #==========================================================================================================
    # Aggregate PV and Batt capacity by reeds region
    #========================================================================================================== 
    agent_cum_capacities = agent_df[[ 'control_reg', 'pv_kw_cum']]
    ba_cum_pv_kw_year = agent_cum_capacities.groupby(by='control_reg').sum()
    ba_cum_pv_kw_year['control_reg'] = ba_cum_pv_kw_year.index
    ba_cum_pv_mw[year] = ba_cum_pv_kw_year['pv_kw_cum'] / 1000.0
    ba_cum_pv_mw.round(3).to_csv(scenario_settings.out_scen_path + '/dpv_MW_by_ba_and_year.csv', index_label='control_reg')                     
    
    #==========================================================================================================
    # Aggregate PV generation profiles and calculate capacity factor profiles
    #==========================================================================================================   
    # DPV CF profiles are only calculated for the last year, since they change
    # negligibly from year-to-year. A ten-year degradation is applied, to 
    # approximate the age of a mature fleet.
    if year==scenario_settings.model_years[-1]:
        pv_gen_by_agent = np.vstack(agent_df['solar_cf_profile']).astype(np.float) / 1e3 * np.array(agent_df['pv_kw_cum']).reshape(len(agent_df), 1)
        
        # Sum each agent's profile into a total dispatch in each BA
        pv_gen_by_ba = np.zeros([len(ba_list), 8760])
        for ba_n, ba in enumerate(ba_list):
            list_of_agent_indicies = np.array(agents_grouped.loc[ba, 'index'])
            pv_gen_by_ba[ba_n, :] = np.sum(pv_gen_by_agent[list_of_agent_indicies, :], axis=0)
       
        # Apply ten-year degradation
        pv_deg_rate = agent_df.loc[agent_df.index[0], 'pv_deg'] 
        pv_gen_by_ba = pv_gen_by_ba * (1-pv_deg_rate)**10   
        
        # Change the numpy array into pandas dataframe
        pv_gen_by_ba_df = pd.DataFrame(pv_gen_by_ba, columns=hour_list)
        pv_gen_by_ba_df.index = ba_list

        # Convert generation into capacity factor by diving by total capacity
        pv_cf_by_ba = pv_gen_by_ba_df[hour_list].divide(ba_cum_pv_mw[year]*1000.0, 'index')
        pv_cf_by_ba['control_reg'] = ba_list
    
        # write output
        pv_cf_by_ba = pv_cf_by_ba[['control_reg'] + hour_list]
        pv_cf_by_ba.round(3).to_csv(scenario_settings.out_scen_path + '/dpv_cf_by_ba.csv', index=False)                                         
                            
    interyear_results_aggregations = {'ba_cum_pv_mw':ba_cum_pv_mw}

    #==========================================================================================================
    # Package interyear results
    #========================================================================================================== 
 
    return interyear_results_aggregations

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_elec_price_multiplier_and_escalator(dataframe, year, elec_price_change_traj):
    '''
    Obtain a single scalar multiplier for each agent, that is the cost of
    electricity relative to 2016 (when the tariffs were curated).
    Also calculate the average increase in the price of electricity over the
    past ten years, which will be the escalator that they use to project
    electricity changes in their bill calculations.

    Note that many customers will not differentiate between real and nomianl,
    and therefore many would overestimate the real escalation of electriicty
    prices.
    '''
    dataframe = dataframe.reset_index()

    elec_price_multiplier = elec_price_change_traj[elec_price_change_traj['year']==year].reset_index()

    horizon_year = year-10

    elec_price_escalator_df = elec_price_multiplier.copy()
    if horizon_year in elec_price_change_traj.year.values:
        elec_price_escalator_df['historical'] = elec_price_change_traj[elec_price_change_traj['year']==horizon_year]['elec_price_multiplier'].values
    else:
        first_year = np.min(elec_price_change_traj['year'])
        first_year_df = elec_price_change_traj[elec_price_change_traj['year']==first_year].reset_index()
        missing_years = first_year - horizon_year
        elec_price_escalator_df['historical'] = first_year_df['elec_price_multiplier']*0.99**missing_years
    
    elec_price_escalator_df['elec_price_escalator'] = (elec_price_escalator_df['elec_price_multiplier'] / elec_price_escalator_df['historical'])**(1.0/10) - 1.0

    # Set lower bound of escalator at 0, assuming that potential customers would not evaluate declining electricity costs
    elec_price_escalator_df['elec_price_escalator'] = np.maximum(elec_price_escalator_df['elec_price_escalator'], 0)

    dataframe = pd.merge(dataframe, elec_price_multiplier[['elec_price_multiplier', 'control_reg_id', 'country_abbr', 'sector_abbr']], how='left', on=['control_reg_id', 'country_abbr', 'sector_abbr'])
    dataframe = pd.merge(dataframe, elec_price_escalator_df[['control_reg_id', 'country_abbr', 'sector_abbr', 'elec_price_escalator']],
                         how='left', on=['control_reg_id', 'country_abbr', 'sector_abbr'])

    dataframe = dataframe.set_index('agent_id')

    return dataframe
    
#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_export_tariff_params(dataframe, net_metering_df):

    dataframe = dataframe.reset_index()
    dataframe = pd.merge(dataframe, net_metering_df[
                         ['control_reg_id', 'sector_abbr', 'nem_system_size_limit_kw']], how='left', on=['control_reg_id', 'sector_abbr'])
    dataframe = dataframe.set_index('agent_id')

    return dataframe

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_wholesale_elec_prices(dataframe, df):

    dataframe = dataframe.reset_index()
    dataframe = pd.merge(dataframe, df[['control_reg_id', 'country_abbr','sector_abbr','wholesale_elec_price','year']], 
        how='left', on=['year','control_reg_id', 'country_abbr','sector_abbr'])
    dataframe = dataframe.set_index('agent_id')

    return dataframe

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_depreciation_schedule(dataframe, deprec_sch):

    dataframe = dataframe.reset_index()

    dataframe = pd.merge(dataframe, deprec_sch[['sector_abbr', 'deprec_sch', 'year']],
                         how='left', on=['sector_abbr', 'year'])
                         
    dataframe = dataframe.set_index('agent_id')


    return dataframe
    
#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_pv_specs(dataframe, pv_specs):

    dataframe = dataframe.reset_index()

    # join the data
    dataframe = pd.merge(dataframe, pv_specs, how='left', on=['sector_abbr', 'year'])

    # apply the capital cost multipliers
    dataframe['pv_price_per_kw'] = (dataframe['pv_price_per_kw'] * dataframe['cap_cost_multiplier'])

    dataframe = dataframe.set_index('agent_id')

    return dataframe

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_storage_specs(dataframe, batt_price_traj, year, scenario_settings):

    dataframe = dataframe.reset_index()

    # Merge on prices
    dataframe = pd.merge(dataframe, batt_price_traj, how = 'left', on = ['sector_abbr', 'year'])
    del dataframe['batt_om_per_kwh']
    del dataframe['batt_om_per_kw']
                         
    # Add replacement cost payments to base O&M
    storage_replace_values = batt_price_traj[batt_price_traj['year']==year+scenario_settings.storage_options['batt_replacement_yr']]
    storage_replace_values['kw_replace_price'] = storage_replace_values['batt_price_per_kw'] * scenario_settings.storage_options['batt_replacement_frac_kw']
    storage_replace_values['kwh_replace_price'] = storage_replace_values['batt_price_per_kwh'] * scenario_settings.storage_options['batt_replacement_frac_kwh']
    
    # Calculate the present value of the replacements
    replace_discount = 0.08 # Use a different discount rate to represent the discounting of the third party doing the replacing
    replace_fraction = 1 / (1.0+replace_discount)**scenario_settings.storage_options['batt_replacement_yr']
    storage_replace_values['kw_replace_present'] = storage_replace_values['kw_replace_price'] * replace_fraction
    storage_replace_values['kwh_replace_present'] = storage_replace_values['kwh_replace_price'] * replace_fraction

    # Calculate the level of annual payments whose present value equals the present value of a replacement
    storage_replace_values['batt_om_per_kw'] += storage_replace_values['kw_replace_present'] * (replace_discount*(1+replace_discount)**20) / ((1+replace_discount)**20 - 1)
    storage_replace_values['batt_om_per_kwh'] += storage_replace_values['kwh_replace_present'] * (replace_discount*(1+replace_discount)**20) / ((1+replace_discount)**20 - 1)

    dataframe = pd.merge(dataframe, storage_replace_values[['sector_abbr', 'batt_om_per_kwh', 'batt_om_per_kw']], how='left', on=['sector_abbr'])

    dataframe['batt_replace_yr'] = scenario_settings.storage_options['batt_replacement_yr']

    dataframe = dataframe.set_index('agent_id')

    return dataframe

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_financial_params(dataframe, financing_terms, itc_options, inflation_rate):

    dataframe = dataframe.reset_index()

    dataframe = dataframe.merge(financing_terms, how='left', on=['year', 'sector_abbr'])

    dataframe = dataframe.merge(itc_options[['itc_fraction', 'year', 'tech', 'sector_abbr']], 
                                how='left', on=['year', 'tech', 'sector_abbr'])

    dataframe['inflation'] = inflation_rate
    
    dataframe = dataframe.set_index('agent_id')
    
    return dataframe

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_load_growth(dataframe, load_growth_df):

    dataframe = dataframe.reset_index()

    dataframe = pd.merge(dataframe, load_growth_df, how='left', on=['control_reg_id', 'country_abbr', 'sector_abbr'])
    
    # for res, load growth translates to kwh_per_customer change
    dataframe['load_kwh_per_customer_in_bin'] = np.where(dataframe['sector_abbr']=='res',
                                                dataframe['load_kwh_per_customer_in_bin_initial'] * dataframe['load_multiplier'],
                                                dataframe['load_kwh_per_customer_in_bin_initial'])
                                                
    # for C&I, load growth translates to customer count change
    dataframe['customers_in_bin'] = np.where(dataframe['sector_abbr']!='res',
                                                dataframe['customers_in_bin_initial'] * dataframe['load_multiplier'],
                                                dataframe['customers_in_bin_initial'])
                                                
    # for all sectors, total kwh_in_bin changes
    dataframe['load_kwh_in_bin'] = dataframe['load_kwh_in_bin_initial'] * dataframe['load_multiplier']
    
    dataframe = dataframe.set_index('agent_id')

    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def calculate_developable_customers_and_load(dataframe):

    dataframe = dataframe.reset_index()

    dataframe['developable_customers_in_bin'] = dataframe['pct_of_bldgs_developable'] * dataframe['customers_in_bin']

    dataframe['developable_load_kwh_in_bin'] = dataframe['pct_of_bldgs_developable'] * dataframe['load_kwh_in_bin']

    dataframe = dataframe.set_index('agent_id')

    return dataframe

def scale_array_sum(row, array_col, scale_col):
    hourly_array = np.array(row[array_col], dtype='float64')
    row[array_col] = hourly_array / \
    hourly_array.sum() * np.float64(row[scale_col])
    return row

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_scale_normalized_load_profiles(dataframe):

    dataframe = dataframe.reset_index()

    # scale the normalized profile to sum to the total load
    dataframe = dataframe.apply(scale_array_sum, axis=1, args=(
        'consumption_hourly_initial', 'load_kwh_per_customer_in_bin'))

    dataframe['consumption_hourly'] = dataframe['consumption_hourly_initial']
    #dataframe.rename(columns={'consumption_hourly_initial':'consumption_hourly'}, inplace=True)
    
    dataframe = dataframe.set_index('agent_id')

    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_carbon_intensities(dataframe, carbon_intensities):

    dataframe = dataframe.reset_index()

    dataframe = pd.merge(dataframe, carbon_intensities, how='left', on=['country_abbr','sector_abbr', 'control_reg_id'])

    dataframe = dataframe.set_index('agent_id')

    return dataframe
    


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def estimate_initial_market_shares(dataframe):

    # record input columns
    in_cols = list(dataframe.columns)
    dataframe = dataframe.reset_index()

    # find the total number of customers in each state (by technology and
    # sector)
    state_total_developable_customers = dataframe[['control_reg_id', 'tariff_class', 'state_id', 'country_abbr', 'sector_abbr', 'tech', 'developable_customers_in_bin']].groupby(
        ['control_reg_id', 'tariff_class', 'state_id', 'country_abbr', 'sector_abbr', 'tech']).sum().reset_index()
    state_total_agents = dataframe[['control_reg_id', 'tariff_class', 'state_id', 'country_abbr', 'sector_abbr', 'tech', 'developable_customers_in_bin']].groupby(
        ['control_reg_id', 'tariff_class', 'state_id', 'country_abbr', 'sector_abbr', 'tech']).count().reset_index()
    # rename the final columns
    state_total_developable_customers.columns = state_total_developable_customers.columns.str.replace(
        'developable_customers_in_bin', 'developable_customers_in_state')
    state_total_agents.columns = state_total_agents.columns.str.replace(
        'developable_customers_in_bin', 'agent_count')
    # merge together
    state_denominators = pd.merge(state_total_developable_customers, state_total_agents, how='left', on=[
                                  'control_reg_id', 'state_id', 'tariff_class', 'country_abbr', 'sector_abbr', 'tech'])
    
    state_denominators.control_reg_id  = state_denominators.control_reg_id.astype(str)

    # merge back to the main dataframe
    dataframe = pd.merge(dataframe, state_denominators, how='left', 
        on=['control_reg_id', 'tariff_class', 'state_id', 'country_abbr', 'sector_abbr', 'tech'])

    # determine the portion of initial load and systems that should be allocated to each agent
    # (when there are no developable agnets in the state, simply apportion evenly to all agents)
    dataframe['portion_of_state'] = np.where(dataframe['developable_customers_in_state'] > 0,
                                             dataframe[
                                                 'developable_customers_in_bin'] / dataframe['developable_customers_in_state'],
                                             1. / dataframe['agent_count'])
    # apply the agent's portion to the total to calculate starting capacity
    # and systems
    dataframe['number_of_adopters_last_year'] = dataframe['portion_of_state'] * dataframe['pv_systems_count']
    dataframe['pv_kw_cum_last_year'] = dataframe['portion_of_state'] * dataframe['pv_capacity_mw'] * 1000.0
    dataframe['batt_kw_cum_last_year'] = 0.0
    dataframe['batt_kwh_cum_last_year'] = 0.0

    dataframe['market_share_last_year'] = np.where(dataframe['developable_customers_in_bin'] == 0, 0,
                                                   dataframe['number_of_adopters_last_year'] / dataframe['developable_customers_in_bin'])

    dataframe['market_value_last_year'] = dataframe['pv_price_per_kw'] * dataframe['pv_kw_cum_last_year']

    # reproduce these columns as "initial" columns too
    dataframe['initial_number_of_adopters'] = dataframe['number_of_adopters_last_year']
    dataframe['initial_pv_kw'] = dataframe['pv_kw_cum_last_year']
    dataframe['initial_market_share'] = dataframe['market_share_last_year']
    dataframe['initial_market_value'] = 0

    # isolate the return columns
    return_cols = ['initial_number_of_adopters', 'initial_pv_kw', 'initial_market_share', 'initial_market_value',
                   'number_of_adopters_last_year', 'pv_kw_cum_last_year', 'batt_kw_cum_last_year', 'batt_kwh_cum_last_year', 'market_share_last_year', 'market_value_last_year']

    dataframe[return_cols] = dataframe[return_cols].fillna(0)

    out_cols = in_cols + return_cols
    dataframe = dataframe.set_index('agent_id')

    return dataframe[out_cols]


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_market_last_year(dataframe, market_last_year_df):

    dataframe = dataframe.reset_index()

#    dataframe = dataframe.join(market_last_year_df)
    dataframe = pd.merge(dataframe, market_last_year_df, how = 'left', on = ['tech', 'control_reg_id', 'state_id', 'tariff_class', 'state_id', 'country_abbr', 'sector_abbr'])

    dataframe = dataframe.set_index('agent_id')

    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def estimate_total_generation(dataframe):
    dataframe['total_gen_twh'] = ((dataframe['number_of_adopters'] - dataframe['initial_number_of_adopters']) * dataframe['aep'] * 1e-9) + (0.23 * 8760 * dataframe['initial_pv_kw'] * 1e-6)
    return dataframe
