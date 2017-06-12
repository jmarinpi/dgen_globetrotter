# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 14:33:49 2017

@author: pgagnon
"""

import pandas as pd
import numpy as np
import os

#%%
def check_table_exists(schema, table, con):

    sql = '''SELECT EXISTS (
               SELECT 1
               FROM   information_schema.tables
               WHERE  table_schema = '%s'
               AND    table_name = '%s'
               );''' % (schema, table)

    return pd.read_sql(sql, con).values[0][0]

#%%
def get_scenario_settings(schema, con):

    sql = '''SELECT * FROM %s.input_main_scenario_options'''%(schema)
    df = pd.read_sql(sql, con)

    return df


def get_userdefined_scenario_settings(schema, table_name, con):

    sql = '''SELECT * FROM %s.%s'''%(schema, table_name)
    df = pd.read_sql(sql, con)

    return df


#%%
def import_table(scenario_settings, con, input_name, csv_import_function):

    schema = scenario_settings.schema
    input_data_dir = scenario_settings.input_data_dir
    user_scenario_settings = get_scenario_settings(schema, con)
    scenario_name = user_scenario_settings[input_name].values[0]

    if scenario_name == 'User Defined':

        userdefined_table_name = "input_" + input_name + "_user_defined"
        scenario_userdefined_name = get_userdefined_scenario_settings(schema, userdefined_table_name, con)
        scenario_userdefined_value = scenario_userdefined_name['val'].values[0]


        if check_table_exists(schema, input_name, con):
            sql = '''SELECT * FROM %s.%s;''' % (schema, input_name)
            df = pd.read_sql(sql, con)

        else:
            
            df = pd.read_csv(os.path.join(input_data_dir, input_name, scenario_userdefined_value), index_col=None)
            df = csv_import_function(df, scenario_settings)

            # To Do: Ted working on fix to import dataframe into postgres database table
            #df.to_sql(scenario_userdefined_value, con, schema=schema)

    else:
        sql = '''SELECT * FROM %s.%s WHERE source = %s;''' % (schema, input_name, scenario_name)
        df = pd.read_sql(sql, con)

    return df

#%%
def stacked_sectors(df, scenario_settings):
    sectors = ['res', 'ind','com','nonres']
    output = pd.DataFrame()
    core_columns = [x for x in df.columns if x.split("_")[-1] not in sectors]


    for sector in sectors:
        if sector in set([i.split("_")[-1] for i in df.columns]):
            sector_columns = [x for x in df.columns if x.split("_")[-1] == sector]
            rename_fields = {k:"_".join(k.split("_")[0:-1]) for k in sector_columns}

            temp =  df.ix[:,core_columns + sector_columns]
            temp = temp.rename(columns=rename_fields)
            temp['sector_abbr'] = sector

            output = pd.concat([output, temp], ignore_index=True)

    return output

#%%
def deprec_schedule(df, scenario_settings):

    columns = ['1', '2', '3', '4', '5', '6']
    df['deprec_sch']=df.apply(lambda x: [x.to_dict()[y] for y in columns], axis=1)

    max_required_year = 2050
    max_input_year = np.max(df['year'])
    missing_years = np.arange(max_input_year + 1, max_required_year + 1, 1)
    last_entry = df[df['year'] == max_input_year]

    for year in missing_years:
        last_entry['year'] = year
        df = df.append(last_entry)

    return df.ix[:,['year','deprec_sch']]

#%%
def melt_year(paramater_name):

    def function(df,scenario_settings):

        years = np.arange(2014, 2051, 2)
        years = [str(year) for year in years]

        df_tify = pd.melt(df, id_vars='state_abbr', value_vars=years, var_name='year', value_name=paramater_name)

        df_tify['year'] = df_tify['year'].astype(int)

        return df_tify

    return function


#%%
def process_pv_price_trajectories(pv_price_traj, scenario_settings):

    pv_price_traj.to_csv(scenario_settings.dir_to_write_input_data + '/pv_prices.csv', index=False)
    
    res_df = pd.DataFrame(pv_price_traj['year'])
    res_df = pv_price_traj[['year', 'pv_price_res', 'pv_om_res', 'pv_variable_om_res']]
    res_df.rename(columns={'pv_price_res':'pv_price_per_kw', 
                           'pv_om_res':'pv_om_per_kw',
                           'pv_variable_om_res':'pv_variable_om_per_kw'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(pv_price_traj['year'])
    com_df = pv_price_traj[['year', 'pv_price_com', 'pv_om_com', 'pv_variable_om_com']]
    com_df.rename(columns={'pv_price_com':'pv_price_per_kw', 
                           'pv_om_com':'pv_om_per_kw',
                           'pv_variable_om_com':'pv_variable_om_per_kw'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(pv_price_traj['year'])
    ind_df = pv_price_traj[['year', 'pv_price_ind', 'pv_om_ind', 'pv_variable_om_ind']]
    ind_df.rename(columns={'pv_price_ind':'pv_price_per_kw', 
                           'pv_om_ind':'pv_om_per_kw',
                           'pv_variable_om_ind':'pv_variable_om_per_kw'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    pv_price_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)
    
    return pv_price_traj


#%%
def process_pv_tech_performance(pv_tech_traj_df, scenario_settings):
    
    pv_tech_traj_df.to_csv(scenario_settings.dir_to_write_input_data + '/pv_tech_performance.csv', index=False)
    
    res_df = pd.DataFrame(pv_tech_traj_df['year'])
    res_df = pv_tech_traj_df[['year', 'pv_deg_res', 'pv_power_density_w_per_sqft_res']]
    res_df.rename(columns={'pv_deg_res':'pv_deg',
                           'pv_power_density_w_per_sqft_res':'pv_power_density_w_per_sqft'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(pv_tech_traj_df['year'])
    com_df = pv_tech_traj_df[['year', 'pv_deg_com', 'pv_power_density_w_per_sqft_com']]
    com_df.rename(columns={'pv_deg_com':'pv_deg',
                           'pv_power_density_w_per_sqft_com':'pv_power_density_w_per_sqft'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(pv_tech_traj_df['year'])
    ind_df = pv_tech_traj_df[['year', 'pv_deg_ind', 'pv_power_density_w_per_sqft_ind']]
    ind_df.rename(columns={'pv_deg_ind':'pv_deg',
                           'pv_power_density_w_per_sqft_ind':'pv_power_density_w_per_sqft'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    pv_tech_traj_df = pd.concat([res_df, com_df, ind_df], ignore_index=True)

    return pv_tech_traj_df


    #%%
def process_batt_price_trajectories(batt_price_traj, scenario_settings):
    
    batt_price_traj.to_csv(scenario_settings.dir_to_write_input_data + '/batt_prices.csv', index=False)

    res_df = pd.DataFrame(batt_price_traj['year'])
    res_df = batt_price_traj[['year', 'batt_price_per_kwh_res', 'batt_price_per_kw_res',
                              'batt_om_per_kw_res', 'batt_om_per_kwh_res', 'batt_replace_frac_kw', 'batt_replace_frac_kwh']]
    res_df.rename(columns={'batt_price_per_kwh_res':'batt_price_per_kwh', 
                           'batt_price_per_kw_res':'batt_price_per_kw',
                           'batt_om_per_kw_res':'batt_om_per_kw',
                           'batt_om_per_kwh_res':'batt_om_per_kwh'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(batt_price_traj['year'])
    com_df = batt_price_traj[['year', 'batt_price_per_kwh_nonres', 'batt_price_per_kw_nonres',
                              'batt_om_per_kw_nonres', 'batt_om_per_kwh_nonres', 'batt_replace_frac_kw', 'batt_replace_frac_kwh']]
    com_df.rename(columns={'batt_price_per_kwh_nonres':'batt_price_per_kwh', 
                           'batt_price_per_kw_nonres':'batt_price_per_kw',
                           'batt_om_per_kw_nonres':'batt_om_per_kw',
                           'batt_om_per_kwh_nonres':'batt_om_per_kwh'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(batt_price_traj['year'])
    ind_df = batt_price_traj[['year', 'batt_price_per_kwh_nonres', 'batt_price_per_kw_nonres',
                              'batt_om_per_kw_nonres', 'batt_om_per_kwh_nonres', 'batt_replace_frac_kw', 'batt_replace_frac_kwh']]
    ind_df.rename(columns={'batt_price_per_kwh_nonres':'batt_price_per_kwh', 
                           'batt_price_per_kw_nonres':'batt_price_per_kw',
                           'batt_om_per_kw_nonres':'batt_om_per_kw',
                           'batt_om_per_kwh_nonres':'batt_om_per_kwh'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    batt_price_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)
    
    return batt_price_traj


    #%%
def process_financing_terms(financing_terms, scenario_settings):
    
    financing_terms.to_csv(scenario_settings.dir_to_write_input_data + '/financing_terms.csv', index=False)
    
    res_df = pd.DataFrame(financing_terms['year'])
    res_df = financing_terms[['year', 'economic_lifetime', 'loan_term_res', 'loan_rate_res', 'down_payment_res', 'real_discount_res', 'tax_rate_res']]
    res_df.rename(columns={'loan_term_res':'loan_term', 
                           'loan_rate_res':'loan_rate', 
                           'down_payment_res':'down_payment', 
                           'real_discount_res':'real_discount', 
                           'tax_rate_res':'tax_rate'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(financing_terms['year'])
    com_df = financing_terms[['year', 'economic_lifetime', 'loan_term_nonres', 'loan_rate_nonres', 'down_payment_nonres', 'real_discount_nonres', 'tax_rate_nonres']]
    com_df.rename(columns={'loan_term_nonres':'loan_term', 
                           'loan_rate_nonres':'loan_rate', 
                           'down_payment_nonres':'down_payment', 
                           'real_discount_nonres':'real_discount', 
                           'tax_rate_nonres':'tax_rate'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(financing_terms['year'])
    ind_df = financing_terms[['year', 'economic_lifetime', 'loan_term_nonres', 'loan_rate_nonres', 'down_payment_nonres', 'real_discount_nonres', 'tax_rate_nonres']]
    ind_df.rename(columns={'loan_term_nonres':'loan_term', 
                           'loan_rate_nonres':'loan_rate', 
                           'down_payment_nonres':'down_payment', 
                           'real_discount_nonres':'real_discount', 
                           'tax_rate_nonres':'tax_rate'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    financing_terms = pd.concat([res_df, com_df, ind_df], ignore_index=True)

    return financing_terms


#%%
def process_batt_tech_performance(batt_tech_traj, scenario_settings):
    
    batt_tech_traj.to_csv(scenario_settings.dir_to_write_input_data + '/batt_tech_performance.csv', index=False)
    
    res_df = pd.DataFrame(batt_tech_traj['year'])
    res_df = batt_tech_traj[['year', 'batt_eff_res', 'batt_lifetime_res']]
    res_df.rename(columns={'batt_eff_res':'batt_eff', 
                           'batt_lifetime_res':'batt_lifetime'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(batt_tech_traj['year'])
    com_df = batt_tech_traj[['year', 'batt_eff_com', 'batt_lifetime_com']]
    com_df.rename(columns={'batt_eff_com':'batt_eff', 
                           'batt_lifetime_com':'batt_lifetime'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(batt_tech_traj['year'])
    ind_df = batt_tech_traj[['year', 'batt_eff_ind', 'batt_lifetime_ind']]
    ind_df.rename(columns={'batt_eff_ind':'batt_eff', 
                           'batt_lifetime_ind':'batt_lifetime'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    batt_tech_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)
    
    return batt_tech_traj


#%%
def process_depreciation_schedules(deprec_schedules, scenario_settings):
    
    deprec_schedules.to_csv(scenario_settings.dir_to_write_input_data + '/depreciation_schedules.csv', index=False)
    
    deprec_schedules['deprec_sch'] = 'temp'
    
    for index in deprec_schedules.index:
        deprec_schedules.set_value(index, 'deprec_sch', np.array(deprec_schedules.loc[index, ['1','2','3','4','5','6']]))

    max_required_year = 2050
    max_input_year = np.max(deprec_schedules['year'])
    missing_years = np.arange(max_input_year+1, max_required_year+1, 1)
    last_entry = deprec_schedules[deprec_schedules['year']==max_input_year]
    
    for year in missing_years:
        last_entry['year'] = year
        deprec_schedules = deprec_schedules.append(last_entry)
        
    return deprec_schedules[['year', 'sector_abbr', 'deprec_sch']]