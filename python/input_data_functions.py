# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 14:33:49 2017

@author: pgagnon
"""

import pandas as pd
import numpy as np
import os
import sqlalchemy
import data_functions as datfunc
import agent_mutation
from agents import Agents, Solar_Agents
from pandas import DataFrame
import json


#%%
def check_table_exists(schema, table, con):

    sql = '''SELECT EXISTS (SELECT 1 FROM   information_schema.tables WHERE  table_schema = '%s' AND table_name = '%s');''' % (schema, table)

    return pd.read_sql(sql, con).values[0][0]

def get_psql_table_fields(engine, schema, name):
    sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name   = '%s'" % (schema, name)
    return np.concatenate(pd.read_sql_query(sql, engine).values)

def df_to_psql(df, engine, schema, owner, name, if_exists='replace', append_transformations=False):
    d_types = {}
    transform = {}
    f_d_type = {}
    sql_type = {}

    delete_list = []
    orig_fields = df.columns.values
    df.columns = [i.lower() for i in orig_fields]
    for f in df.columns:
        df_filter = pd.notnull(df[f]).values
        if sum(df_filter) > 0:
            f_d_type[f] = type(df[f][df_filter].values[0]).__name__.lower()

            if f_d_type[f][0:3].lower() == 'int':
                sql_type[f] = 'INTEGER'

            if f_d_type[f][0:5].lower() == 'float':
                d_types[f] = sqlalchemy.types.NUMERIC
                sql_type[f] = 'NUMERIC'

            if f_d_type[f][0:3].lower() == 'str':
                sql_type[f] = 'VARCHAR'

            if f_d_type[f] == 'list':
                d_types[f] = sqlalchemy.types.ARRAY(sqlalchemy.types.STRINGTYPE)
                transform[f] = lambda x: json.dumps(x)
                sql_type[f] = 'VARCHAR'

            if f_d_type[f] == 'ndarray':
                d_types[f] = sqlalchemy.types.ARRAY(sqlalchemy.types.STRINGTYPE)
                transform[f] = lambda x: json.dumps(list(x))
                sql_type[f] = 'VARCHAR'

            if f_d_type[f] == 'dict':
                d_types[f] = sqlalchemy.types.STRINGTYPE
                transform[f] = lambda x: json.dumps(
                    dict(map(lambda (k, v): (k, list(v)) if (type(v).__name__ == 'ndarray') else (k, v), x.items())))
                sql_type[f] = 'VARCHAR'

            if f_d_type[f] == 'interval':
                d_types[f] = sqlalchemy.types.STRINGTYPE
                transform[f] = lambda x: str(x)
                sql_type[f] = 'VARCHAR'

            if f_d_type[f] == 'dataframe':
                d_types[f] = sqlalchemy.types.STRINGTYPE
                transform[f] = lambda x: x.to_json() if isinstance(x,DataFrame) else str(x)
                sql_type[f] = 'VARCHAR'
        else:
            orig_fields = [i for i in orig_fields if i.lower()!=f]
            delete_list.append(f)

    df = df.drop(delete_list, axis=1)

    for k, v in transform.items():
        if append_transformations:
            df[k + "_" + f_d_type[k]] = df[k].apply(v)
            sql_type[k + "_" + f_d_type[k]] = sql_type[k]
            del df[k]
            del sql_type[k]
        else:
            df[k] = df[k].apply(v)

    conn = engine.connect()
    if if_exists == 'append':
        fields = [i.lower() for i in get_psql_table_fields(engine, schema, name)]
        for f in list(set(df.columns.values) - set(fields)):
            sql = "ALTER TABLE %s.%s ADD COLUMN %s %s" % (schema, name, f, sql_type[f])
            conn.execute(sql)
            
    df.to_sql(name, engine, schema=schema, index=False, dtype=d_types, if_exists=if_exists)
    sql = 'ALTER TABLE %s."%s" OWNER to "%s";' % (schema, name, owner)
    conn.execute(sql)
    
    conn.close()
    engine.dispose() 

    df.columns = orig_fields
    return df
    

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
def import_table(scenario_settings, con, engine, role, input_name, csv_import_function):

    schema = scenario_settings.schema
    shared_schema = 'diffusion_shared'
    input_data_dir = scenario_settings.input_data_dir
    user_scenario_settings = get_scenario_settings(schema, con)
    scenario_name = user_scenario_settings[input_name].values[0]

    if scenario_name == 'User Defined':

        userdefined_table_name = "input_" + input_name + "_user_defined"
        scenario_userdefined_name = get_userdefined_scenario_settings(schema, userdefined_table_name, con)
        scenario_userdefined_value = scenario_userdefined_name['val'].values[0]
        scenario_userdefined_table_name = "user_defined_" + scenario_userdefined_value

        if check_table_exists(shared_schema, scenario_userdefined_value, con):
            sql = '''SELECT * FROM %s."%s";''' % (shared_schema, scenario_userdefined_value)
            df = pd.read_sql(sql, con)

        else:
            df = pd.read_csv(os.path.join(input_data_dir, input_name, scenario_userdefined_value+".csv"), index_col=False)
            df = csv_import_function(df)

            df_to_psql(df, engine, shared_schema, role, scenario_userdefined_value)

    else:
        # To do: Convert all specific functions below into a single generalized function
        #attribute_table_name = "input_" + input_name + "_to_model"
        if input_name == 'elec_prices':
            df = datfunc.get_rate_escalations(con, scenario_settings.schema)
        elif input_name == 'load_growth':
            df = datfunc.get_load_growth(con, scenario_settings.schema)
        elif input_name == 'pv_prices':
            df = datfunc.get_technology_costs_solar(con, scenario_settings.schema)
        elif input_name == 'batt_prices':
             df = datfunc.get_storage_costs(con, scenario_settings.schema)
        elif input_name == 'wholesale_electricity_prices':
             df = datfunc.get_wholesale_electricity_prices(con, scenario_settings.schema)

    return df


#%%
def stacked_sectors(df):

    sectors = ['res', 'ind','com','nonres','all']
    output = pd.DataFrame()
    core_columns = [x for x in df.columns if x.split("_")[-1] not in sectors]


    for sector in sectors:
        if sector in set([i.split("_")[-1] for i in df.columns]):
            sector_columns = [x for x in df.columns if x.split("_")[-1] == sector]
            rename_fields = {k:"_".join(k.split("_")[0:-1]) for k in sector_columns}

            temp =  df.ix[:,core_columns + sector_columns]
            temp = temp.rename(columns=rename_fields)
            if sector =='nonres':
                sector_list = ['com', 'ind']
            elif sector=='all':
                sector_list = ['com', 'ind','res']
            else:
                sector_list = [sector]
            for s in sector_list:
                temp['sector_abbr'] = s
                output = pd.concat([output, temp], ignore_index=True)

    return output

#%%
def deprec_schedule(df):

    columns = ['1', '2', '3', '4', '5', '6']
    df['deprec_sch']=df.apply(lambda x: [x.to_dict()[y] for y in columns], axis=1)

    max_required_year = 2050
    max_input_year = np.max(df['year'])
    missing_years = np.arange(max_input_year + 1, max_required_year + 1, 1)
    last_entry = df[df['year'] == max_input_year]

    for year in missing_years:
        last_entry['year'] = year
        df = df.append(last_entry)


    return df.ix[:,['year','sector_abbr','deprec_sch']]

#%%
def melt_year(parameter_name):

    def function(df):
        years = np.arange(2014, 2051, 2)
        years = [str(year) for year in years]

        df_tidy = pd.melt(df, id_vars='state_abbr', value_vars=years, var_name='year', value_name=parameter_name)

        df_tidy['year'] = df_tidy['year'].astype(int)

        return df_tidy

    return function


#%%
def import_agent_file(scenario_settings, prng, con, cur, engine, model_settings, agent_file_status, input_name):

    schema = scenario_settings.schema
    shared_schema = 'diffusion_shared'
    role = model_settings.role
    input_agent_dir = model_settings.input_agent_dir

    if agent_file_status == ['Use pre-generated Agents']:

        userdefined_table_name = "input_" + input_name + "_user_defined"
        scenario_userdefined_name = get_userdefined_scenario_settings(schema, userdefined_table_name, con)
        scenario_userdefined_value = scenario_userdefined_name['val'].values[0]
        solar_agents = Agents(pd.read_pickle(os.path.join(input_agent_dir, scenario_userdefined_value+".pkl")))

    else:

        solar_agents = Agents(agent_mutation.init_solar_agents(model_settings, scenario_settings, prng, cur, con))

    return solar_agents


#%%
def process_elec_price_trajectories(elec_price_traj):

    base_year_prices = elec_price_traj[elec_price_traj['year']==2016]
    
    base_year_prices.rename(columns={'elec_price_res':'res_base',
                                     'elec_price_com':'com_base',
                                     'elec_price_ind':'ind_base'}, inplace=True)
    
    elec_price_change_traj = pd.merge(elec_price_traj, base_year_prices[['res_base', 'com_base', 'ind_base', 'census_division_abbr']], on='census_division_abbr')

    elec_price_change_traj['elec_price_change_res'] = elec_price_change_traj['elec_price_res'] / elec_price_change_traj['res_base']
    elec_price_change_traj['elec_price_change_com'] = elec_price_change_traj['elec_price_com'] / elec_price_change_traj['com_base']
    elec_price_change_traj['elec_price_change_ind'] = elec_price_change_traj['elec_price_ind'] / elec_price_change_traj['ind_base']

    # Melt by sector
    res_df = pd.DataFrame(elec_price_change_traj['year'])
    res_df = elec_price_change_traj[['year', 'elec_price_change_res', 'census_division_abbr']]
    res_df.rename(columns={'elec_price_change_res':'elec_price_multiplier'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(elec_price_change_traj['year'])
    com_df = elec_price_change_traj[['year', 'elec_price_change_com', 'census_division_abbr']]
    com_df.rename(columns={'elec_price_change_com':'elec_price_multiplier'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(elec_price_change_traj['year'])
    ind_df = elec_price_change_traj[['year', 'elec_price_change_ind', 'census_division_abbr']]
    ind_df.rename(columns={'elec_price_change_ind':'elec_price_multiplier'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    elec_price_change_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)

    return elec_price_change_traj


#%%
def process_load_growth(load_growth):

    base_year_load_growth = load_growth[load_growth['year']==2014]
    
    base_year_load_growth.rename(columns={'load_growth_res':'res_base',
                                     'load_growth_com':'com_base',
                                     'load_growth_ind':'ind_base'}, inplace=True)
    
    load_growth_change_traj = pd.merge(load_growth, base_year_load_growth[['res_base', 'com_base', 'ind_base', 'census_division_abbr']], on='census_division_abbr')

    load_growth_change_traj['load_growth_change_res'] = load_growth_change_traj['load_growth_res'] / load_growth_change_traj['res_base']
    load_growth_change_traj['load_growth_change_com'] = load_growth_change_traj['load_growth_com'] / load_growth_change_traj['com_base']
    load_growth_change_traj['load_growth_change_ind'] = load_growth_change_traj['load_growth_ind'] / load_growth_change_traj['ind_base']

    # Melt by sector
    res_df = pd.DataFrame(load_growth_change_traj['year'])
    res_df = load_growth_change_traj[['year', 'load_growth_change_res', 'census_division_abbr']]
    res_df.rename(columns={'load_growth_change_res':'load_multiplier'}, inplace=True)
    res_df['sector_abbr'] = 'res'
    
    com_df = pd.DataFrame(load_growth_change_traj['year'])
    com_df = load_growth_change_traj[['year', 'load_growth_change_com', 'census_division_abbr']]
    com_df.rename(columns={'load_growth_change_com':'load_multiplier'}, inplace=True)
    com_df['sector_abbr'] = 'com'
    
    ind_df = pd.DataFrame(load_growth_change_traj['year'])
    ind_df = load_growth_change_traj[['year', 'load_growth_change_ind', 'census_division_abbr']]
    ind_df.rename(columns={'load_growth_change_ind':'load_multiplier'}, inplace=True)
    ind_df['sector_abbr'] = 'ind'
    
    load_growth_change_traj = pd.concat([res_df, com_df, ind_df], ignore_index=True)

    return load_growth_change_traj
