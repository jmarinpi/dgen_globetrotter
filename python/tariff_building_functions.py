# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 17:22:38 2017

@author: pgagnon
"""


import sys
sys.path.append('C:/users/pgagnon/desktop/support_functions/python')

import numpy as np
import pandas as pd
import tariff_functions as tFuncs
import financial_functions as fFuncs
import matplotlib.pyplot as plt
from bokeh.io import output_file, show
from bokeh.plotting import figure
from bokeh.layouts import gridplot
from bokeh.models import Range1d
import scipy.special



def design_tariffs(agent_df, rto_df, ts_df, ts_map):

    ###################### General Setup #################################
    tariff_component_df = pd.DataFrame(index=rto_df.index)
    rto_list = np.unique(rto_df.index)
    ts_list = ['H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16']


    winter_month_indicies = [10,11,0,1]
    spring_month_indicies = [2,3,4]
    summer_month_indicies = [5,6,7]
    fall_month_indicies = [8,9]
    
    e_wkend_12by24 = np.zeros([12,24], int)
    e_wkday_12by24 = np.zeros([12,24], int)
    e_wkend_12by24[summer_month_indicies, :] = np.array([0,0,0,0,0,0,1,1,1,1,1,1,1,2,2,2,2,3,3,3,3,3,0,0])
    e_wkday_12by24[summer_month_indicies, :] = np.array([0,0,0,0,0,0,1,1,1,1,1,1,1,2,2,2,2,3,3,3,3,3,0,0])
    e_wkend_12by24[fall_month_indicies, :] = 4 + np.array([0,0,0,0,0,0,1,1,1,1,1,1,1,2,2,2,2,3,3,3,3,3,0,0])
    e_wkday_12by24[fall_month_indicies, :] = 4 + np.array([0,0,0,0,0,0,1,1,1,1,1,1,1,2,2,2,2,3,3,3,3,3,0,0])
    e_wkend_12by24[winter_month_indicies, :] = 8 + np.array([0,0,0,0,0,0,1,1,1,1,1,1,1,2,2,2,2,3,3,3,3,3,0,0])
    e_wkday_12by24[winter_month_indicies, :] = 8 + np.array([0,0,0,0,0,0,1,1,1,1,1,1,1,2,2,2,2,3,3,3,3,3,0,0])
    e_wkend_12by24[spring_month_indicies, :] = 12 + np.array([0,0,0,0,0,0,1,1,1,1,1,1,1,2,2,2,2,3,3,3,3,3,0,0])
    e_wkday_12by24[spring_month_indicies, :] = 12 + np.array([0,0,0,0,0,0,1,1,1,1,1,1,1,2,2,2,2,3,3,3,3,3,0,0])
    
    d_wkend_12by24 = np.zeros([12,24], int)
    d_wkday_12by24_base = np.zeros([12,24], int)
    
    hour_map = {'H1':[0,1,2,3,4,5,22,23],
                'H2':[6,7,8,9,10,11,12],
                'H3':[13,14,15,16],
                'H4':[17,18,19,20,21],
                'H5':[0,1,2,3,4,5,22,23],
                'H6':[6,7,8,9,10,11,12],
                'H7':[13,14,15,16],
                'H8':[17,18,19,20,21],
                'H9':[0,1,2,3,4,5,22,23],
                'H10':[6,7,8,9,10,11,12],
                'H11':[13,14,15,16],
                'H12':[17,18,19,20,21],
                'H13':[0,1,2,3,4,5,22,23],
                'H14':[6,7,8,9,10,11,12],
                'H15':[13,14,15,16],
                'H16':[17,18,19,20,21]}
    
    # 8760 vector of month numbers
    month_hours = np.array([0, 744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760], int)
    month_index = np.zeros(8760, int)
    for month, hours in enumerate(month_hours):
        month_index[month_hours[month-1]:hours] = month-1
        
    tariff_component_df['e_prices_res'] = 'temp'
    tariff_component_df['e_prices_com'] = 'temp'
    tariff_component_df['e_prices_ind'] = 'temp'
    tariff_component_df['d_wkday_12by24'] = 'temp'
    tariff_component_df['d_wkend_12by24'] = 'temp'
    tariff_component_df['e_wkday_12by24'] = 'temp'
    tariff_component_df['e_wkend_12by24'] = 'temp'   


    tariff_dict_df = pd.DataFrame(index=tariff_component_df.index)
    tariff_dict_df['tariff_components_dict_res'] = 'temp'
    tariff_dict_df['tariff_components_dict_com'] = 'temp'
    tariff_dict_df['tariff_components_dict_ind'] = 'temp'     
    
    ###################### Commercial Tariffs #################################
    # Calculate Tariff components
   
    for rto in rto_list:
        # Determine which timeslices had the greatest energy prices
        
        rto_ts_values = ts_df[ts_df['rto']==rto].copy()
        rto_ts_values.set_index('ts', inplace=True)
        
        sum_peak_ts = rto_ts_values.loc[['H1', 'H2', 'H3', 'H4']]['EnergyValue'].idxmax()
        fal_peak_ts = rto_ts_values.loc[['H5', 'H6', 'H7', 'H8']]['EnergyValue'].idxmax()
        win_peak_ts = rto_ts_values.loc[['H9', 'H10', 'H11', 'H12']]['EnergyValue'].idxmax()
        spr_peak_ts = rto_ts_values.loc[['H13', 'H14', 'H15', 'H16']]['EnergyValue'].idxmax()    
        
        sum_peak_hours = hour_map[sum_peak_ts]
        fal_peak_hours = hour_map[fal_peak_ts]
        win_peak_hours = hour_map[win_peak_ts]
        spr_peak_hours = hour_map[spr_peak_ts]        
        
        tariff_component_df.loc[rto, 'sum_peak_ts'] = sum_peak_ts
        tariff_component_df.loc[rto, 'fal_peak_ts'] = fal_peak_ts
        tariff_component_df.loc[rto, 'win_peak_ts'] = win_peak_ts
        tariff_component_df.loc[rto, 'spr_peak_ts'] = spr_peak_ts
    
        agent_df_rto = agent_df[agent_df['rto'] == rto].copy()
        agent_df_rto_res = agent_df_rto[agent_df_rto['sector_abbr'] == 'res'].copy()
        agent_df_rto_com = agent_df_rto[agent_df_rto['sector_abbr'] == 'com'].copy()
        agent_df_rto_ind = agent_df_rto[agent_df_rto['sector_abbr'] == 'ind'].copy()

        agent_n_res = len(agent_df_rto_res)
        agent_n_com = len(agent_df_rto_com)
        agent_n_ind = len(agent_df_rto_ind)
    
        ####################### Calculate energy prices ###############################
        load_by_agent_res = np.vstack(agent_df_rto_res['consumption_hourly']).astype(np.float) * np.array(agent_df_rto_res['customers_in_bin_initial']).reshape(agent_n_res, 1).astype(np.float)
        load_by_agent_com = np.vstack(agent_df_rto_com['consumption_hourly']).astype(np.float) * np.array(agent_df_rto_com['customers_in_bin_initial']).reshape(agent_n_com, 1).astype(np.float)
        load_by_agent_ind = np.vstack(agent_df_rto_ind['consumption_hourly']).astype(np.float) * np.array(agent_df_rto_ind['customers_in_bin_initial']).reshape(agent_n_ind, 1).astype(np.float)

        net_load_by_agent_res = load_by_agent_res - (np.vstack(agent_df_rto_res['solar_cf_profile']).astype(np.float) * 1e-6 * np.array(agent_df_rto_res['pv_kw_cum']).reshape(agent_n_res, 1).astype(np.float))
        net_load_by_agent_com = load_by_agent_com - (np.vstack(agent_df_rto_com['solar_cf_profile']).astype(np.float) * 1e-6 * np.array(agent_df_rto_com['pv_kw_cum']).reshape(agent_n_com, 1).astype(np.float))
        net_load_by_agent_ind = load_by_agent_ind - (np.vstack(agent_df_rto_ind['solar_cf_profile']).astype(np.float) * 1e-6 * np.array(agent_df_rto_ind['pv_kw_cum']).reshape(agent_n_ind, 1).astype(np.float))

        total_loads_res = np.sum(net_load_by_agent_res, axis=0)
        total_loads_com = np.sum(net_load_by_agent_com, axis=0)
        total_loads_ind = np.sum(net_load_by_agent_ind, axis=0)
            
        for ts in ts_list:
            ts_array = np.array(ts_map['ts'])==ts
            rto_ts_values.loc[ts, 'total_kwh_res'] = np.sum(total_loads_res[ts_array])
            rto_ts_values.loc[ts, 'total_kwh_com'] = np.sum(total_loads_com[ts_array])
            rto_ts_values.loc[ts, 'total_kwh_ind'] = np.sum(total_loads_ind[ts_array])
        
        rto_ts_values['rev_marginals_res'] = rto_ts_values['total_kwh_res'] * rto_ts_values['EnergyValue']
        rto_ts_values['rev_marginals_com'] = rto_ts_values['total_kwh_com'] * rto_ts_values['EnergyValue']
        rto_ts_values['rev_marginals_ind'] = rto_ts_values['total_kwh_ind'] * rto_ts_values['EnergyValue']

        rto_ts_values['e_prices_res'] = rto_ts_values['EnergyValue'] * rto_df.loc[rto, 'e_rev_req_res'] / np.sum(rto_ts_values['rev_marginals_res'])
        rto_ts_values['e_prices_com'] = rto_ts_values['EnergyValue'] * rto_df.loc[rto, 'e_rev_req_com'] / np.sum(rto_ts_values['rev_marginals_com'])
        rto_ts_values['e_prices_ind'] = rto_ts_values['EnergyValue'] * rto_df.loc[rto, 'e_rev_req_ind'] / np.sum(rto_ts_values['rev_marginals_ind'])
    
    
        ####################### Calculate demand prices ###############################    
        d_wkday_12by24 = d_wkday_12by24_base.copy()
        d_wkday_12by24[np.ix_(summer_month_indicies, sum_peak_hours)] = 1
        d_wkday_12by24[np.ix_(fall_month_indicies, fal_peak_hours)] = 1
        d_wkday_12by24[np.ix_(winter_month_indicies, win_peak_hours)] = 1
        d_wkday_12by24[np.ix_(spring_month_indicies, spr_peak_hours)] = 1
    
        # Build an 8760 of peak hours   
        d_tou_8760 = tFuncs.build_8760_from_12by24s(d_wkday_12by24, d_wkend_12by24, start_day=6)
        d_tou_n = 2
        
        period_matrix = np.zeros([8760, d_tou_n*12], bool)
        period_matrix[range(8760),d_tou_8760+month_index*d_tou_n] = True
    
        # Calculate demand totals for Com
        total_flat_demand_com = 0
        total_tou_demand_com = 0
        for n in range(agent_n_com):       
            load_distributed = net_load_by_agent_com[n,:][np.newaxis, :].T*period_matrix
            period_maxs = np.max(load_distributed, axis=0).reshape([d_tou_n,12], order='F')
            
            flat_demands = np.max(period_maxs, axis=0)
            tou_demands = period_maxs[1, :]

            total_flat_demand_com += np.sum(flat_demands)            
            total_tou_demand_com += np.sum(tou_demands)

        # Calculate demand totals for Ind
        total_flat_demand_ind = 0
        total_tou_demand_ind = 0
        for n in range(agent_n_ind):       
            load_distributed = net_load_by_agent_ind[n,:][np.newaxis, :].T*period_matrix
            period_maxs = np.max(load_distributed, axis=0).reshape([d_tou_n,12], order='F')
            
            flat_demands = np.max(period_maxs, axis=0)
            tou_demands = period_maxs[1, :]

            total_flat_demand_ind += np.sum(flat_demands)            
            total_tou_demand_ind += np.sum(tou_demands)
                
        tariff_component_df.loc[rto, 'd_flat_price_com'] = rto_df.loc[rto, 'd_flat_rev_req_com'] / total_flat_demand_com
        tariff_component_df.loc[rto, 'd_tou_price_com'] = rto_df.loc[rto, 'd_tou_rev_req_com'] / total_tou_demand_com

        tariff_component_df.loc[rto, 'd_flat_price_ind'] = rto_df.loc[rto, 'd_flat_rev_req_ind'] / total_flat_demand_ind
        tariff_component_df.loc[rto, 'd_tou_price_ind'] = rto_df.loc[rto, 'd_tou_rev_req_ind'] / total_tou_demand_ind

    
        ########################### Fixed charges #####################################
        total_res_cust = np.sum(agent_df_rto_res['customers_in_bin_initial'])
        total_com_cust = np.sum(agent_df_rto_com['customers_in_bin_initial'])
        total_ind_cust = np.sum(agent_df_rto_ind['customers_in_bin_initial'])

        tariff_component_df.loc[rto, 'fixed_monthly_charge_res'] = rto_df.loc[rto, 'f_rev_req_res'] / total_res_cust / 12.0
        tariff_component_df.loc[rto, 'fixed_monthly_charge_com'] = rto_df.loc[rto, 'f_rev_req_com'] / total_com_cust / 12.0
        tariff_component_df.loc[rto, 'fixed_monthly_charge_ind'] = rto_df.loc[rto, 'f_rev_req_ind'] / total_ind_cust / 12.0
    
        ############### Store variables for tariff design ##########################
        tariff_component_df.set_value(rto, 'e_prices_res', np.array(rto_ts_values.loc[ts_list,'e_prices_res']))
        tariff_component_df.set_value(rto, 'e_prices_com', np.array(rto_ts_values.loc[ts_list,'e_prices_com']))
        tariff_component_df.set_value(rto, 'e_prices_ind', np.array(rto_ts_values.loc[ts_list,'e_prices_ind']))
        tariff_component_df.set_value(rto, 'd_wkday_12by24', d_wkday_12by24)
        tariff_component_df.set_value(rto, 'd_wkend_12by24', d_wkend_12by24)
        tariff_component_df.set_value(rto, 'e_wkday_12by24', e_wkday_12by24)
        tariff_component_df.set_value(rto, 'e_wkend_12by24', e_wkend_12by24)
        
        ############### Store tariffs as dicts ##########################
        tariff_component_dict_res = {'e_prices':tariff_component_df.loc[rto, 'e_prices_res'],
                                     'e_wkday_12by24':tariff_component_df.loc[rto, 'e_wkday_12by24'],
                                     'e_wkend_12by24':tariff_component_df.loc[rto, 'e_wkend_12by24'],
                                     'fixed_monthly_charge':tariff_component_df.loc[rto, 'fixed_monthly_charge_res']}
                                     
        tariff_component_dict_com = {'e_prices':tariff_component_df.loc[rto, 'e_prices_com'],
                                     'e_wkday_12by24':tariff_component_df.loc[rto, 'e_wkday_12by24'],
                                     'e_wkend_12by24':tariff_component_df.loc[rto, 'e_wkend_12by24'],
                                     'd_wkday_12by24':tariff_component_df.loc[rto, 'd_wkday_12by24'],
                                     'd_wkend_12by24':tariff_component_df.loc[rto, 'd_wkend_12by24'],
                                     'fixed_monthly_charge':tariff_component_df.loc[rto, 'fixed_monthly_charge_com'],
                                     'd_flat_price':tariff_component_df.loc[rto, 'd_flat_price_com'],
                                     'd_tou_price':tariff_component_df.loc[rto, 'd_tou_price_com']}
                                     
        tariff_component_dict_ind = {'e_prices':tariff_component_df.loc[rto, 'e_prices_ind'],
                                     'e_wkday_12by24':tariff_component_df.loc[rto, 'e_wkday_12by24'],
                                     'e_wkend_12by24':tariff_component_df.loc[rto, 'e_wkend_12by24'],
                                     'd_wkday_12by24':tariff_component_df.loc[rto, 'd_wkday_12by24'],
                                     'd_wkend_12by24':tariff_component_df.loc[rto, 'd_wkend_12by24'],
                                     'fixed_monthly_charge':tariff_component_df.loc[rto, 'fixed_monthly_charge_ind'],
                                     'd_flat_price':tariff_component_df.loc[rto, 'd_flat_price_ind'],
                                     'd_tou_price':tariff_component_df.loc[rto, 'd_tou_price_ind']}
        
        tariff_dict_df.set_value(rto, 'tariff_components_dict_res', tariff_component_dict_res)
        tariff_dict_df.set_value(rto, 'tariff_components_dict_com', tariff_component_dict_com)
        tariff_dict_df.set_value(rto, 'tariff_components_dict_ind', tariff_component_dict_ind)
        
    return tariff_component_df, tariff_dict_df



#%%
def calc_revenue_fracs_from_reeds_data(agent_df, input_dir, scenario, start_year, end_year, base_year):
    # if the agent_df doesn't have ref bills, calculate them and resave the file
    if 'rev_base_year' not in list(agent_df.columns):
        agent_df = evaluate_agent_ref_bills(agent_df)
    
    ############# Import maps and historical capacity costs ###################
    ts_map = pd.read_csv('%s/timeslice_8760_noH17.csv' % input_dir)
    rto_map = pd.read_csv('%s/BA_to_RTO_mapping.csv' % input_dir)
    cap_cost_all_years_df = pd.read_pickle('%s/historical_cap_cost_df.pkl' % input_dir) # Import historical capacity costs
    
    
    ###################### Import ReEDS values ################################
    # Import reeds timeslice data
    ts_df = pd.read_csv('%s/%s_TimesliceData.csv' % (input_dir, scenario))
    ts_df.rename(columns={'n':'ba', 
                          'm':'ts',
                          'yr':'year'}, inplace=True)
    
    # If the ReEDS timeslice file contains H17, merge those values into H3    
    if 'H17' in ts_df['ts'].as_matrix():                 
        H17_df = ts_df[ts_df['ts']=='H17']
        for idx in ts_df.index:
            if ts_df.loc[idx, 'ts'] == 'H3':
                year = ts_df.loc[idx, 'year']
                ts_df_year = ts_df[ts_df['year'] == year]
                H17_df_year = H17_df[H17_df['year'] == year]
                H17_ba_EnergyValue = H17_df_year[H17_df_year['ba']==ts_df_year.loc[idx, 'ba']]['EnergyValue']
                H17_ba_Consumption = H17_df_year[H17_df_year['ba']==ts_df_year.loc[idx, 'ba']]['Consumption']
                H3_EnergyValue = ts_df.loc[idx, 'EnergyValue']
                H3_Consumption = ts_df.loc[idx, 'Consumption']
                ts_df.loc[idx, 'EnergyValue'] = float((H17_ba_EnergyValue*H17_ba_Consumption + H3_EnergyValue*H3_Consumption) / (H3_Consumption+H17_ba_Consumption))
                ts_df.loc[idx, 'Consumption'] = float(H3_Consumption+H17_ba_Consumption)
            
        # Drop H17
        ts_df = ts_df[ts_df['ts'] != 'H17']
    
    # Aggregate timeslice data by rto
    ts_df = pd.merge(ts_df, rto_map, on='ba')
    ts_df_gp = pd.groupby(ts_df[['Consumption', 'ts', 'year', 'rto']].copy(), by=['ts', 'year', 'rto']).sum().reset_index()
    ts_df_gp.rename(columns={'Consumption':'Consumption_in_gp'}, inplace=True)
    ts_df = pd.merge(ts_df, ts_df_gp, on=['ts', 'year', 'rto'])
    ts_df['EnergyValue_weighted'] = ts_df['EnergyValue'] * ts_df['Consumption'] / ts_df['Consumption_in_gp']
    ts_df_rto = pd.groupby(ts_df[['rto', 'year', 'ts', 'Consumption', 'EnergyValue_weighted']], by=['rto', 'year', 'ts']).sum().reset_index()
    ts_df_rto.rename(columns={'EnergyValue_weighted':'EnergyValue'}, inplace=True)
           
    # Import reeds annual data        
    annual_df = pd.read_csv('%s/%s_AnnualData.csv' % (input_dir, scenario))
    annual_df.rename(columns={'n':'ba', 
                              'm':'ts',
                              'yr':'year'}, inplace=True)
    
    # Misc setup
    years_sy = np.unique(annual_df['year']) # Just the reeds solve years
    years_all = np.arange(2010, 2051, 1)
    agent_df = pd.merge(agent_df, rto_map, on='ba')
    rto_list = list(agent_df['rto'].unique())
    
    #### Calculate reeds-projected energy and capacity costs, group by RTO ####
    annual_df['cap_cost'] = annual_df['CapacityValue'] * annual_df['Capacity']
    annual_df['energy_cost'] = annual_df['EnergyValue'] * annual_df['Consumption']
    annual_df['total_cost'] = annual_df['cap_cost'] + annual_df['energy_cost']
    
    annual_df = pd.merge(annual_df, rto_map, on='ba')
    
    costs_by_rto = annual_df[['rto', 'year', 'cap_cost', 'energy_cost', 'total_cost', 'Consumption']].groupby(by=['rto', 'year']).sum()
    costs_by_rto.reset_index(inplace=True)
    
    ########################## Reformat Costs #################################
    # Reformat costs (which were tidy) as unique dfs for each cost, for ease of
    # later calculations
    total_cost_df_sy = pd.DataFrame(index=rto_list, columns=years_sy)
    energy_cost_df_sy = pd.DataFrame(index=rto_list, columns=years_sy)
    cap_cost_df_sy = pd.DataFrame(index=rto_list, columns=years_sy)
    mwh_df_sy = pd.DataFrame(index=rto_list, columns=years_sy)
    
    for rto in rto_list:
        costs_by_rto_single = costs_by_rto[costs_by_rto['rto']==rto].copy()
        costs_by_rto_single.set_index('year', inplace=True)
        
        total_cost_df_sy.loc[rto, :] = costs_by_rto_single['total_cost'].transpose()
        energy_cost_df_sy.loc[rto, :] = costs_by_rto_single['energy_cost'].transpose()
        cap_cost_df_sy.loc[rto, :] = costs_by_rto_single['cap_cost'].transpose()
        mwh_df_sy.loc[rto, :] = costs_by_rto_single['Consumption'].transpose()
    
    ###########################################################################
    # Interpolate reeds-projected energy and cost values, from solve years to all years
    total_cost_df = pd.DataFrame(index=rto_list, columns=years_all)
    energy_cost_df = pd.DataFrame(index=rto_list, columns=years_all)
    cap_cost_df = pd.DataFrame(index=rto_list, columns=years_all)
    mwh_df = pd.DataFrame(index=rto_list, columns=years_all)
    
    for year in years_all:
        if year in years_sy:
            total_cost_df.loc[:, year] = total_cost_df_sy.loc[:, year]
            energy_cost_df.loc[:, year] = energy_cost_df_sy.loc[:, year]
            cap_cost_df.loc[:, year] = cap_cost_df_sy.loc[:, year]
            mwh_df.loc[:, year] = mwh_df_sy.loc[:, year]
    
        else: 
            total_cost_df.loc[:, year] = (total_cost_df_sy.loc[:, year-1] + total_cost_df_sy.loc[:, year+1]) / 2.0
            energy_cost_df.loc[:, year] = (energy_cost_df_sy.loc[:, year-1] + energy_cost_df_sy.loc[:, year+1]) / 2.0
            cap_cost_df.loc[:, year] = (cap_cost_df_sy.loc[:, year-1] + cap_cost_df_sy.loc[:, year+1]) / 2.0
            mwh_df.loc[:, year] = (mwh_df_sy.loc[:, year-1] + mwh_df_sy.loc[:, year+1]) / 2.0
    
    energy_frac_df = energy_cost_df / total_cost_df
    cap_frac_df = cap_cost_df / total_cost_df
    
    # Combine historical capacity costs with costs from ReEDS
    for year in np.arange(2010, 2051, 1):
        cap_cost_all_years_df[year] = cap_cost_df.loc[:, year]
    
    ###########################################################################
    # Calculate the capacity payments, as if each year's capacity cost was spread out over 30 years
    # This method assumes that each year's capacity payment is paid 100% with a 30-year loan
    # Interest payments are paid each year (not added to total debt)
    # Consider: Should debt fraction be less than 100%? What interest rate is appropriate?
    cap_cost_principal_payments_df = pd.DataFrame(index=rto_list, columns=list(cap_cost_all_years_df.columns))
    cap_cost_interest_payments_df = pd.DataFrame(index=rto_list, columns=list(cap_cost_all_years_df.columns))
    cap_cost_start_year = np.min(list(cap_cost_all_years_df.columns))
    
    interest_real = 0.054 # Assuming 8% interest, 2.5% inflation
    tax_rate = 0.4
    
    for year in list(cap_cost_principal_payments_df.columns):
        calc_start_year = np.max([cap_cost_start_year, year-30]) + 1
        cap_cost_principal_payments_df.loc[:, year] = cap_cost_all_years_df.loc[:, calc_start_year:year].sum(1) / 30.0
        cap_cost_interest_payments_df.loc[:, year] = (cap_cost_all_years_df.loc[:, calc_start_year:year]*np.arange(29-year+calc_start_year,30)/30).sum(1) * interest_real * (1-tax_rate) #0.08 placeholder interest rate TODO
    
    cap_cost_smoothed_df = cap_cost_principal_payments_df + cap_cost_interest_payments_df
    total_cost_smoothed_df = energy_cost_df + cap_cost_smoothed_df.loc[:, years_all]
    cap_frac_smoothed_df = cap_cost_smoothed_df.loc[:, years_all] /  total_cost_smoothed_df
    
    ###########################################################################
    # Summarize the observed revenue collected from agent's base tariffs by sector, RTO
    # prep for revenue fraction summaries
    agent_df['d_charges_in_bin'] = agent_df['d_charges_f_ref'] * agent_df['ref_bill'] * agent_df['customers_in_bin_initial']
    agent_df['f_charges_in_bin'] = agent_df['f_charges_f_ref'] * agent_df['ref_bill'] * agent_df['customers_in_bin_initial']
    
    rev_df = pd.groupby(agent_df[['rto', 'sector_abbr', 'rev_base_year', 'load_kwh_in_bin_initial', 'd_charges_in_bin', 'f_charges_in_bin']],
                        by=['rto', 'sector_abbr']).sum()
    rev_df.reset_index(inplace=True)
    
    rev_df_res = rev_df[rev_df['sector_abbr']=='res'].copy()
    rev_df_com = rev_df[rev_df['sector_abbr']=='com'].copy()
    rev_df_ind = rev_df[rev_df['sector_abbr']=='ind'].copy()
    
    rev_df_res.set_index('rto', inplace=True)
    rev_df_com.set_index('rto', inplace=True)
    rev_df_ind.set_index('rto', inplace=True)
    
    rev_df_res.rename(columns={'rev_base_year':'rev_base_year_res',
                               'load_kwh_in_bin_initial':'load_kwh_base_year_res', 
                               'd_charges_in_bin':'d_charges_res',
                               'f_charges_in_bin':'f_charges_res'}, inplace=True)
    rev_df_com.rename(columns={'rev_base_year':'rev_base_year_com', 
                               'load_kwh_in_bin_initial':'load_kwh_base_year_com', 
                               'd_charges_in_bin':'d_charges_com',
                               'f_charges_in_bin':'f_charges_com'}, inplace=True)
    rev_df_ind.rename(columns={'rev_base_year':'rev_base_year_ind', 
                               'load_kwh_in_bin_initial':'load_kwh_base_year_ind', 
                               'd_charges_in_bin':'d_charges_ind',
                               'f_charges_in_bin':'f_charges_ind'}, inplace=True)
    
    rto_df = pd.DataFrame(index=rto_list)
    
    rto_df = rto_df.join(rev_df_res[['rev_base_year_res', 'load_kwh_base_year_res', 'd_charges_res', 'f_charges_res']])
    rto_df = rto_df.join(rev_df_com[['rev_base_year_com', 'load_kwh_base_year_com', 'd_charges_com', 'f_charges_com']])
    rto_df = rto_df.join(rev_df_ind[['rev_base_year_ind', 'load_kwh_base_year_ind', 'd_charges_ind', 'f_charges_ind']])
    
    # Calculate observed avg elec cost 
    rto_df['rev_base_year'] = rto_df['rev_base_year_res'] + rto_df['rev_base_year_com'] + rto_df['rev_base_year_ind']
    rto_df['load_kwh_base_year'] = rto_df['load_kwh_base_year_res'] + rto_df['load_kwh_base_year_com'] + rto_df['load_kwh_base_year_ind']
    rto_df['elec_cost_base_year_obs'] = rto_df['rev_base_year'] / rto_df['load_kwh_base_year']
    
    # Calculate observed d_fractions
    rto_df['d_f_res_ref'] = rto_df['d_charges_res'] / rto_df['rev_base_year_res']
    rto_df['d_f_com_ref'] = rto_df['d_charges_com'] / rto_df['rev_base_year_com']
    rto_df['d_f_ind_ref'] = rto_df['d_charges_ind'] / rto_df['rev_base_year_ind']
    rto_df['d_charges'] = rto_df['d_charges_res']+rto_df['d_charges_com']+rto_df['d_charges_ind']
    rto_df['d_f_ref'] = (rto_df['d_charges']) / rto_df['rev_base_year']
    
    # Calculate observed fixed charge fractions
    rto_df['f_f_res_ref'] = rto_df['f_charges_res'] / rto_df['rev_base_year_res']
    rto_df['f_f_com_ref'] = rto_df['f_charges_com'] / rto_df['rev_base_year_com']
    rto_df['f_f_ind_ref'] = rto_df['f_charges_ind'] / rto_df['rev_base_year_ind']
    rto_df['f_charges'] = rto_df['f_charges_res']+rto_df['f_charges_com']+rto_df['f_charges_ind']
    rto_df['f_f_ref'] = (rto_df['f_charges']) / rto_df['rev_base_year']
    
    print "observed nationwide demand charge revenue from res:", np.sum(rto_df['d_charges_res']) / np.sum(rto_df['rev_base_year_res'])
    print "observed nationwide demand charge revenue from com:", np.sum(rto_df['d_charges_com']) / np.sum(rto_df['rev_base_year_com'])
    print "observed nationwide demand charge revenue from ind:", np.sum(rto_df['d_charges_ind']) / np.sum(rto_df['rev_base_year_ind'])
    print "observed nationwide demand charge revenue all sectors:", np.sum(rto_df['d_charges']) / np.sum(rto_df['rev_base_year'])
    
    print "observed nationwide fixed charge revenue from res:", np.sum(rto_df['f_charges_res']) / np.sum(rto_df['rev_base_year_res'])
    print "observed nationwide fixed charge revenue from com:", np.sum(rto_df['f_charges_com']) / np.sum(rto_df['rev_base_year_com'])
    print "observed nationwide fixed charge revenue from ind:", np.sum(rto_df['f_charges_ind']) / np.sum(rto_df['rev_base_year_ind'])
    print "observed nationwide fixed charge revenue all sectors:", np.sum(rto_df['f_charges']) / np.sum(rto_df['rev_base_year'])
    
    ####################### Calc Adder ########################################
    # Compare the observed c/kWh revenue collected in base_year from actual tariffs to 
    # the amount calculated from reeds values. This becomes a c/kWh adder, that we 
    # assume represents costs not captured by reeds (administrative, distribution,
    # etc)
    elec_cost_df = total_cost_smoothed_df / (mwh_df*1000.0)
    rto_df['adder'] = rto_df['elec_cost_base_year_obs'] - elec_cost_df[base_year]
    rto_df['total_cost_base_year'] = total_cost_smoothed_df[base_year]
    
    
    ###################### Return results #####################################
    return rto_df, total_cost_smoothed_df, cap_frac_smoothed_df, ts_df_rto, ts_map
    
    
#%%
def design_tariff_components(agent_df, year, rto_df, total_cost_smoothed_df, cap_frac_smoothed_df, ts_df_rto, base_year, ts_map):
    # Calculate revenue requirements by sector and tariff component
    # f = fixed charges
    # e = energy charges
    # d = demand charges

    rto_df_year = rto_df.copy()
    
    # Revenue ratio is (reeds_captured_year+adder / reeds_captured_base+adder) 
    rto_df_year['rev_ratio'] = (total_cost_smoothed_df[year]+rto_df_year['adder']) / (total_cost_smoothed_df[base_year]+rto_df_year['adder'])
    
    rto_df_year['rev_req_res'] = rto_df_year['rev_base_year_res'] * rto_df_year['rev_ratio']
    rto_df_year['rev_req_com'] = rto_df_year['rev_base_year_com'] * rto_df_year['rev_ratio']
    rto_df_year['rev_req_ind'] = rto_df_year['rev_base_year_ind'] * rto_df_year['rev_ratio']
        
    rto_df_year['f_rev_req_res'] = rto_df_year['rev_req_res'] * rto_df_year['f_f_res_ref']
    rto_df_year['f_rev_req_com'] = rto_df_year['rev_req_com'] * rto_df_year['f_f_com_ref']
    rto_df_year['f_rev_req_ind'] = rto_df_year['rev_req_ind'] * rto_df_year['f_f_ind_ref']   
        
    rto_df_year['e_rev_req_res'] = rto_df_year['rev_req_res'] * (1-rto_df_year['f_f_res_ref'])
    rto_df_year['e_rev_req_com'] = rto_df_year['rev_req_com'] * (1-rto_df_year['f_f_com_ref']) * (1-cap_frac_smoothed_df[year])
    rto_df_year['e_rev_req_ind'] = rto_df_year['rev_req_ind'] * (1-rto_df_year['f_f_ind_ref']) * (1-cap_frac_smoothed_df[year])
    
    rto_df_year['d_rev_req_res'] = rto_df_year['rev_req_res'] * 0.0
    rto_df_year['d_rev_req_com'] = rto_df_year['rev_req_com'] * (1-rto_df_year['f_f_com_ref']) * (cap_frac_smoothed_df[year])
    rto_df_year['d_rev_req_ind'] = rto_df_year['rev_req_ind'] * (1-rto_df_year['f_f_ind_ref']) * (cap_frac_smoothed_df[year])
    
    # hard-code 50/50 TOU and flat energy split
    rto_df_year['d_flat_rev_req_res'] = rto_df_year['d_rev_req_res'] * 0.5
    rto_df_year['d_flat_rev_req_com'] = rto_df_year['d_rev_req_com'] * 0.5
    rto_df_year['d_flat_rev_req_ind'] = rto_df_year['d_rev_req_ind'] * 0.5
    rto_df_year['d_tou_rev_req_res'] = rto_df_year['d_rev_req_res'] * 0.5
    rto_df_year['d_tou_rev_req_com'] = rto_df_year['d_rev_req_com'] * 0.5
    rto_df_year['d_tou_rev_req_ind'] = rto_df_year['d_rev_req_ind'] * 0.5
    
             
    ########################## Design tariffs #################################            
    ts_df_rto_year = ts_df_rto[ts_df_rto['year']==year]
    tariff_component_df, tariff_dict_df = design_tariffs(agent_df, rto_df_year, ts_df_rto_year, ts_map)
    rto_df_year = rto_df_year.join(tariff_component_df)
    
    rto_df_year.reset_index(inplace=True)
    rto_df_year['year'] = year        
    rto_df_year.to_pickle('rev_and_tariff_components_by_rto_%s.pkl' % year)
    tariff_dict_df.to_pickle('tariff_dicts_by_rto_%s.pkl' % year)
    
    ###################### Assign tariffs to agents ###########################            
    for n, idx in enumerate(agent_df.index):
        print n+1, "of", len(agent_df)

        # Extract tariff components dict for this agent's sector and RTO
        rto = agent_df.loc[idx, 'rto']
        sector = agent_df.loc[idx, 'sector_abbr']        
        tariff_dict = tariff_dict_df.loc[rto, 'tariff_components_dict_%s' % sector]                 
        agent_df.set_value(idx, 'tariff_dict', tariff_dict)
        
    return agent_df    
    
        
    
#%%
def evaluate_agent_ref_bills(agent_df):
    export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)
    print "Base year agent bills were not yet evaluated\nCalculating now..."
    for n, idx in enumerate(agent_df.index):
        print n+1, "of", len(agent_df)
        load_profile = agent_df.loc[idx, 'consumption_hourly']
        tariff_ref = tFuncs.Tariff(dict_obj = agent_df.loc[idx, 'tariff_dict'])
        
        ref_bill, ref_bill_results = tFuncs.bill_calculator(load_profile, tariff_ref, export_tariff)
        agent_df.loc[idx, 'ref_bill'] = ref_bill
        agent_df.loc[idx, 'd_charges_f_ref'] = ref_bill_results['d_charges'] / ref_bill
        agent_df.loc[idx, 'e_charges_f_ref'] = ref_bill_results['e_charges'] / ref_bill
        agent_df.loc[idx, 'f_charges_f_ref'] = ref_bill_results['fixed_charges'] / ref_bill
        
        agent_df.loc[idx, 'd_flat_charges_f_ref'] = np.sum(ref_bill_results['monthly_d_flat_charges']) / ref_bill
        agent_df.loc[idx, 'd_tou_charges_f_ref'] = np.sum(ref_bill_results['monthly_d_tou_charges']) / ref_bill
        
    agent_df['rev_base_year']= agent_df['ref_bill'] * agent_df['customers_in_bin_initial']
    
    return agent_df    
    