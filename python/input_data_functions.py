# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 14:33:49 2017

@author: pgagnon
"""

import pandas as pd
import numpy as np
import os
import sqlalchemy



#%%
def check_table_exists(schema, table, con):

    sql = '''SELECT EXISTS (SELECT 1 FROM   information_schema.tables WHERE  table_schema = '%s' AND table_name = '%s');''' % (schema, table)

    return pd.read_sql(sql, con).values[0][0]

def df_to_psql(df, name, con, engine, schema, owner,data_types={}):

    df.to_sql(name, engine, schema=schema, dtype=data_types)

    sql = 'ALTER TABLE %s."%s" OWNER to "%s";' % (schema, name, owner)
    with engine.begin() as conn:
        conn.execute(sql)

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


        if check_table_exists(shared_schema, scenario_userdefined_value, con):
            sql = '''SELECT * FROM %s."%s";''' % (shared_schema, scenario_userdefined_value)
            df = pd.read_sql(sql, con)

        else:
            df = pd.read_csv(os.path.join(input_data_dir, input_name, scenario_userdefined_value+".csv"), index_col=None)
            df,d_types = csv_import_function(df)

            df_to_psql(df, scenario_userdefined_value, con, engine,shared_schema, role,data_types=d_types)

    else:
        sql = '''SELECT * FROM %s.%s WHERE source = %s;''' % (schema, input_name, scenario_name)
        df = pd.read_sql(sql, con)

    return df

#%%
def stacked_sectors(df):
    d_types={}
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

    return output, d_types

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

    d_types = {'deprec_sch': sqlalchemy.types.ARRAY(sqlalchemy.types.Float())}

    return df.ix[:,['year','sector_abbr','deprec_sch']], d_types

#%%
def melt_year(paramater_name):

    def function(df):
        d_types = {}
        years = np.arange(2014, 2051, 2)
        years = [str(year) for year in years]

        df_tify = pd.melt(df, id_vars='state_abbr', value_vars=years, var_name='year', value_name=paramater_name)

        df_tify['year'] = df_tify['year'].astype(int)

        return df_tify, d_types

    return function