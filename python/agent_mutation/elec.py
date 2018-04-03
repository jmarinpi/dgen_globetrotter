# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 11:35:14 2016

@author: mgleason
"""

import psycopg2 as pg
import numpy as np
import pandas as pd
import random
import decorators
import utility_functions as utilfunc
import traceback
import data_functions as datfunc
from cStringIO import StringIO
import pssc_mp
import os
import pickle
import multiprocessing as mp
import concurrent.futures as concur_f

# Import from support function repo
import tariff_functions as tFuncs
from agent_mutation import (get_depreciation_schedule,
                            get_leasing_availability,
                            apply_leasing_availability)

# GLOBAL SETTINGS

# load logger
logger = utilfunc.get_logger()

# configure psycopg2 to treat numeric values as floats (improves
# performance of pulling data from the database)
DEC2FLOAT = pg.extensions.new_type(
    pg.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
pg.extensions.register_type(DEC2FLOAT)



#%%
def adjust_roof_area(agent_df):
    '''
    Temporary function to make the roof areas of the agent_df equal the roof
    area from lidar data. 

    Note that this assumes small buildings == res and all others equal nonres,
    which we know is not true. This approach leads to some definitely 
    incorrect results - e.g., C&I buildings in ID, WY, MT and others are
    reduced to single-digit percentages of initial roof areas. I am proceeding,
    because total roof area is more critical at the moment.
    
    This should be handled separately in agent generation, eventually.
    '''
    
    roof_areas = pd.read_csv('developable_roof_areas.csv')
    roof_areas['actual_developable_roof_sqft'] = roof_areas['developable_roof_sqft']
    res_actual_areas = roof_areas[['actual_developable_roof_sqft', 'state_abbr']][roof_areas['sector_abbr']=='res']
    nonres_actual_areas = roof_areas[['actual_developable_roof_sqft', 'state_abbr']][roof_areas['sector_abbr']!='res']
    
    agent_df = agent_df.reset_index()
    agent_df_thin = agent_df[['developable_roof_sqft', 'customers_in_bin', 'state_abbr', 'sector_abbr']]
    
    res_df = agent_df_thin[agent_df_thin['sector_abbr']=='res']
    nonres_df = agent_df_thin[agent_df_thin['sector_abbr']!='res']
    
    res_df['total_developable_roof_sqft'] = res_df['developable_roof_sqft'] * res_df['customers_in_bin']
    nonres_df['total_developable_roof_sqft'] = nonres_df['developable_roof_sqft'] * nonres_df['customers_in_bin']
    
    
    res_areas = res_df[['state_abbr', 'total_developable_roof_sqft']].groupby(by='state_abbr').sum()
    nonres_areas = nonres_df[['state_abbr', 'total_developable_roof_sqft']].groupby(by='state_abbr').sum()
    res_areas = res_areas.reset_index()
    nonres_areas = nonres_areas.reset_index()
    
    res_areas = pd.merge(res_areas, res_actual_areas, on='state_abbr')
    nonres_areas = pd.merge(nonres_areas, nonres_actual_areas, on='state_abbr')
    
    res_areas['roof_adjustment'] = res_areas['actual_developable_roof_sqft'] / res_areas['total_developable_roof_sqft'] 
    nonres_areas['roof_adjustment'] = nonres_areas['actual_developable_roof_sqft'] / nonres_areas['total_developable_roof_sqft'] 
    com_areas = nonres_areas.copy()
    ind_areas = nonres_areas.copy()
    
    res_areas['sector_abbr'] = 'res'
    com_areas['sector_abbr'] = 'com'
    ind_areas['sector_abbr'] = 'ind'
    
    all_areas = pd.concat([res_areas, com_areas, ind_areas])
    
    agent_df = pd.merge(agent_df, all_areas[['roof_adjustment', 'state_abbr', 'sector_abbr']], on=['sector_abbr', 'state_abbr'])
    
    agent_df['developable_roof_sqft'] = agent_df['developable_roof_sqft'] * agent_df['roof_adjustment']
    
    agent_df = agent_df.set_index('agent_id')
    
    return agent_df

#%%
def select_tariff_driver(agent_df, prng, rates_rank_df, rates_json_df, n_workers=mp.cpu_count()/2):

    if 'ix' not in os.name:
        EXECUTOR = concur_f.ThreadPoolExecutor
    else:
        EXECUTOR = concur_f.ProcessPoolExecutor

    seed = prng.get_state()[1][0]

    futures = []
    with EXECUTOR(max_workers=n_workers) as executor:
        for agent_id, agent in agent_df.iterrows():
            
            prng.seed(seed)
            # Filter for list of tariffs available to this agent
            agent_rate_list = rates_rank_df.loc[agent_id].drop_duplicates()
            if np.isscalar(agent_rate_list['rate_id_alias']):
                rate_list = [agent_rate_list['rate_id_alias']]
            else:
                rate_list = agent_rate_list['rate_id_alias']
            agent_rate_jsons = rates_json_df[rates_json_df.index.isin(rate_list)]
            
            # There can be more than one utility that is potentially applicable
            # to each agent (e.g., if the agent is in a county where more than 
            # one utility has service). Select which one by random.
            utility_list = np.unique(agent_rate_jsons['eia_id'])

            # Do a random draw from the utility_list using the same seed as generated in dgen_model.py and return the utility_id that was selected
            utility_id = prng.choice(utility_list)

            agent_rate_jsons = agent_rate_jsons[agent_rate_jsons['eia_id']==utility_id]
            
            futures.append(executor.submit(select_tariff, agent, agent_rate_jsons))

        results = [future.result() for future in futures]

    agent_df = pd.concat(results, axis=1).T
    agent_df.index.name = 'agent_id'

    return agent_df

#%%
def select_tariff(agent, rates_json_df):

    # Extract load profile
    load_profile = np.array(agent['consumption_hourly'])

    # Create export tariff object
    export_tariff = tFuncs.Export_Tariff(full_retail_nem=True)

    #=========================================================================#
    # Tariff selection
    #=========================================================================#
    rates_json_df['bills'] = 0.0
    if len(rates_json_df > 1):
        # determine which of the tariffs has the cheapest cost of electricity without a system
        for index in rates_json_df.index:
            tariff_dict = rates_json_df.loc[index, 'rate_json']
            tariff = tFuncs.Tariff(dict_obj=tariff_dict)
            bill, _ = tFuncs.bill_calculator(load_profile, tariff, export_tariff)
            rates_json_df.loc[index, 'bills'] = bill

    # Select the tariff that had the cheapest electricity. Note that there is
    # currently no rate switching, if it would be cheaper once a system is
    # installed. This is currently for computational reasons.
    rates_json_df['tariff_ids'] = rates_json_df.index
    tariff_id = rates_json_df.loc[rates_json_df['bills'].idxmin(), 'tariff_ids']
    tariff_dict = rates_json_df.loc[tariff_id, 'rate_json']
    # TODO: Patch for daily energy tiers. Remove once bill calculator is improved.
    if 'energy_rate_unit' in tariff_dict:
        if tariff_dict['energy_rate_unit'] == 'kWh daily': tariff_dict['e_levels'] = np.array(tariff_dict['e_levels']) * 30.0
    tariff = tFuncs.Tariff(dict_obj=tariff_dict)

    # Removes the two 8760's from the dictionary, since these will be built from
    # 12x24's now
    if 'd_tou_8760' in tariff_dict.keys(): del tariff_dict['d_tou_8760']
    if 'e_tou_8760' in tariff_dict.keys(): del tariff_dict['e_tou_8760']

    agent['tariff_dict'] = tariff_dict
    agent['tariff_id'] = tariff_id

    return agent


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_normalized_hourly_resource_index_solar(dataframe, hourly_resource_df, techs):

    if 'solar' in techs:
        # record the columns in the input dataframe
        in_cols = list(dataframe.columns)

        # create a column that has the index value for each solar resource
        hourly_resource_i_df = hourly_resource_df[
            ['sector_abbr', 'tech', 'county_id', 'bin_id']]
        hourly_resource_i_df[
            'resource_index_solar'] = hourly_resource_i_df.index

        # join the index that corresponds to the agent's solar resource to the
        # agent dataframe
        dataframe = pd.merge(dataframe, hourly_resource_i_df, how='left', on=[
                             'sector_abbr', 'tech', 'county_id', 'bin_id'])

        # subset to only the desired output columns
        out_cols = in_cols + ['resource_index_solar']
        dataframe = dataframe[out_cols]
#    else:
#        out_cols = {'resource_index_solar' : 'object'}
#        for col, dtype in out_cols.iteritems():
#            dataframe[col] = pd.Series([], dtype = dtype)

    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_solar_capacity_factor_profile(dataframe, hourly_resource_df):

    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)

    # create a column that has the index value for each solar resource
#    hourly_resource_i_df = hourly_resource_df[['sector_abbr', 'tech', 'county_id', 'bin_id']]
#    hourly_resource_i_df['resource_index_solar'] = hourly_resource_i_df.index

    # join the index that corresponds to the agent's solar resource to the
    # agent dataframe
    dataframe = dataframe.reset_index()
    dataframe = pd.merge(dataframe, hourly_resource_df, how='left', on=[
                         'sector_abbr', 'tech', 'county_id', 'bin_id'])
    dataframe['solar_cf_profile'] = dataframe['generation_hourly']

    # subset to only the desired output columns
    out_cols = in_cols + ['solar_cf_profile']
    dataframe = dataframe[out_cols]

    return dataframe

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_elec_price_multiplier_and_escalator(dataframe, year, elec_price_change_traj):
    '''
    Obtain a single scalar multiplier for each agent, that is the cost of
    electricity relative to 2016 (when the tariffs were curated).
    Also calculate the average increase in the price of electricity over the
    past ten years, which will be the escalator that they use to project
    electricity changes in their bill calculations.
    
    elec_price_multiplier = change in present-year elec cost to 2016
    elec_price_escalator = agent's assumption about future price changes

    Note that many customers will not differentiate between real and nomianl,
    and therefore many would overestimate the real escalation of electriicty
    prices.
    
    TODO: Add in actual historical electricity prices. Right now, a simple 
        placeholder of 1% annual growth is assumed.
    '''
    dataframe = dataframe.reset_index()

    elec_price_multiplier = elec_price_change_traj[elec_price_change_traj['year']==year].reset_index()

    horizon_year = year-10

    elec_price_escalator_df = elec_price_multiplier.copy()
    if horizon_year in elec_price_change_traj['year']:
        elec_price_escalator_df['historical'] = elec_price_change_traj[elec_price_change_traj['year']==horizon_year]
    else:
        first_year = np.min(elec_price_change_traj['year'])
        first_year_df = elec_price_change_traj[elec_price_change_traj['year']==first_year].reset_index()
        missing_years = first_year - horizon_year
        elec_price_escalator_df['historical'] = first_year_df['elec_price_multiplier']*0.99**missing_years
    
    elec_price_escalator_df['elec_price_escalator'] = (elec_price_escalator_df['elec_price_multiplier'] / elec_price_escalator_df['historical'])**(1.0/10) - 1.0

    # Set lower bound of escalator at 0, assuming that potential customers would not assume declining electricity costs
    elec_price_escalator_df['elec_price_escalator'] = np.maximum(elec_price_escalator_df['elec_price_escalator'], 0)

    dataframe = pd.merge(dataframe, elec_price_multiplier[['elec_price_multiplier', 'sector_abbr', 'census_division_abbr']], how='left', on=['sector_abbr', 'census_division_abbr'])
    dataframe = pd.merge(dataframe, elec_price_escalator_df[['sector_abbr', 'census_division_abbr', 'elec_price_escalator']],
                         how='left', on=['sector_abbr', 'census_division_abbr'])

    dataframe = dataframe.set_index('agent_id')

    return dataframe
    

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_export_tariff_params(dataframe, net_metering_df):

    dataframe = dataframe.reset_index()
    dataframe = pd.merge(dataframe, net_metering_df[
                         ['state_abbr', 'sector_abbr', 'pv_kw_limit']], how='left', on=['state_abbr', 'sector_abbr'])
    dataframe = dataframe.set_index('agent_id')

    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_pv_tech_performance(dataframe, tech_traj):

    dataframe = dataframe.reset_index()

    dataframe = pd.merge(dataframe, tech_traj, how='left', on=['sector_abbr', 'year'])
                         
    dataframe = dataframe.set_index('agent_id')


    return dataframe
    

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_depreciation_schedule(dataframe, deprec_sch):

    dataframe = dataframe.reset_index()

    dataframe = pd.merge(dataframe, deprec_sch[['tech', 'sector_abbr', 'deprec_sch', 'year']],
                         how='left', on=['tech', 'sector_abbr', 'year'])
                         
    dataframe = dataframe.set_index('agent_id')


    return dataframe
    
#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_pv_prices(dataframe, pv_price_traj):

    dataframe = dataframe.reset_index()

    # join the data
    dataframe = pd.merge(dataframe, pv_price_traj, how='left', on=['sector_abbr', 'year'])

    # apply the capital cost multipliers
    dataframe['pv_price_per_kw'] = (dataframe['pv_price_per_kw'] * dataframe['cap_cost_multiplier'])

    dataframe = dataframe.set_index('agent_id')

    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def apply_batt_prices(dataframe, batt_price_traj, batt_tech_traj, year):

    dataframe = dataframe.reset_index()

    # Merge on prices
    dataframe = pd.merge(dataframe, batt_price_traj[['batt_price_per_kwh', 'batt_price_per_kw', 'sector_abbr', 'year']], 
                         how = 'left', on = ['sector_abbr', 'year'])
                     
    batt_price_traj = pd.merge(batt_price_traj, batt_tech_traj, on=['year', 'sector_abbr'])
    batt_price_traj['replace_year'] = batt_price_traj['year'] - batt_price_traj['batt_lifetime']
                         
    # Add replacement cost payments to base O&M 
    storage_replace_values = batt_price_traj[batt_price_traj['replace_year']==year]
    storage_replace_values['kw_replace_price'] = storage_replace_values['batt_price_per_kw'] * storage_replace_values['batt_replace_frac_kw']
    storage_replace_values['kwh_replace_price'] = storage_replace_values['batt_price_per_kwh'] * storage_replace_values['batt_replace_frac_kwh']
    
    # Calculate the present value of the replacements
    replace_discount = 0.06 # Use a different discount rate to represent the discounting of the third party doing the replacing
    storage_replace_values['kw_replace_present'] = storage_replace_values['kw_replace_price'] * 1 / (1.0+replace_discount)**storage_replace_values['batt_lifetime']
    storage_replace_values['kwh_replace_present'] = storage_replace_values['kwh_replace_price'] * 1 / (1.0+replace_discount)**storage_replace_values['batt_lifetime']

    # Calculate the level of annual payments whose present value equals the present value of a replacement
    storage_replace_values['batt_om_per_kw'] += storage_replace_values['kw_replace_present'] * (replace_discount*(1+replace_discount)**20) / ((1+replace_discount)**20 - 1)
    storage_replace_values['batt_om_per_kwh'] += storage_replace_values['kwh_replace_present'] * (replace_discount*(1+replace_discount)**20) / ((1+replace_discount)**20 - 1)

    dataframe = pd.merge(dataframe, storage_replace_values[['sector_abbr', 'batt_om_per_kwh', 'batt_om_per_kw']], how='left', on=['sector_abbr'])
    
    dataframe = dataframe.set_index('agent_id')

    return dataframe

    
    
#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_batt_tech_performance(dataframe, batt_tech_traj):

    dataframe = dataframe.reset_index()

    dataframe = dataframe.merge(batt_tech_traj, how='left', on=['year', 'sector_abbr'])
    
    dataframe = dataframe.set_index('agent_id')
    
    return dataframe
    


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_financial_params(dataframe, financing_terms, itc_options, inflation_rate, techs):

    in_cols = list(dataframe.columns)
    dataframe = dataframe.reset_index()

    dataframe = dataframe.merge(financing_terms, how='left', on=['year', 'sector_abbr'])
    dataframe = dataframe.merge(itc_options, how='left', on=['year', 'tech', 'sector_abbr'])
    
    if 'wind' in techs:    
        dataframe = dataframe[(dataframe['system_size_kw'] > dataframe['min_size_kw']) & (dataframe['system_size_kw'] <= dataframe['max_size_kw'])]

    dataframe['inflation'] = inflation_rate
    
    return_cols = list(financing_terms.columns) + ['itc_fraction', 'inflation']
    out_cols = list(pd.unique(in_cols + return_cols))
    
    dataframe = dataframe.set_index('agent_id')
    dataframe = dataframe[out_cols]
    
    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_load_growth(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT sector_abbr, census_division_abbr, load_multiplier
            FROM %(schema)s.load_growth_to_model
            WHERE year = %(year)s;""" % inputs

    df = pd.read_sql(sql, con, coerce_float=False)

    return df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_load_growth(dataframe, load_growth_df):

    dataframe = dataframe.reset_index()

    dataframe = pd.merge(dataframe, load_growth_df, how='left', on=['year', 'sector_abbr', 'census_division_abbr'])
    
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


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_electric_rates(cur, con, schema, sectors, seed, pg_conn_string):

    # NOTE: This function creates a lookup table for the agents in each sector, providing
    #       the county_id and bin_id for each agent, along with the rate_id_alias and rate_source.
    # This information is used in "get_electric_rate_tariffs" to load in the
    # actual rate tariff for each agent.

    inputs = locals().copy()

    inputs['i_place_holder'] = '%(i)s'
    inputs['chunk_place_holder'] = '%(county_ids)s'

    msg = "\tGenerating Electric Rate Tariff Lookup Table for Agents"
    logger.info(msg)

    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr

        sql1 =  """DROP TABLE IF EXISTS %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s;
                    CREATE UNLOGGED TABLE %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s AS (
                        WITH a AS
                        (
                            -- Unnest Rates t
                                SELECT a.agent_id, a.tract_id_alias, a.county_id, a.max_demand_kw, a.avg_monthly_kwh,
                                    b.rate_id_alias as rate_id_alias,
                                    b.rate_rank as rate_rank,
                                    b.rank_utility_type,
                                    b.rate_type_tou,
                                    b.max_demand_kw as rate_max_demand_kw,
                                    b.min_demand_kw as rate_min_demand_kw,
                                    b.max_energy_kwh as rate_max_energy_kwh,
                                    b.min_energy_kwh as rate_min_energy_kwh,
                                    b.sector as rate_sector
                                FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                                LEFT JOIN diffusion_shared.cntys_ranked_rates_lkup_20170103 b  --  *******
                                        ON a.county_id = b.county_id
                                        AND a.util_type = b.rank_utility_type
                    ),""" % inputs

        # Add logic for Commercial and Industrial
        if sector_abbr != 'res':
            if sector_abbr == 'ind':
                sector_priority_1 = 'I'
                sector_priority_2 = 'C'
            elif sector_abbr == 'com':
                sector_priority_1 = 'C'
                sector_priority_2 = 'I'

            # Select Appropriate Rates and Rank the Ranked Rates based on
            # Sector
            sql2 = """b AS
                    (
                        SELECT a.*,
                            (CASE WHEN rate_sector = '%(sector_priority_1)s' THEN 1
                                WHEN rate_sector = '%(sector_priority_2)s' THEN 2 END)::int as sector_rank

                        FROM a
                        WHERE rate_sector != 'R'
                            AND ((a.max_demand_kw <= a.rate_max_demand_kw)
                                  AND (a.max_demand_kw >= a.rate_min_demand_kw))
                            AND ((a.avg_monthly_kwh <= a.rate_max_energy_kwh)
                                  AND (a.avg_monthly_kwh >= a.rate_min_energy_kwh))
                    ),
                    c as
                    (
                            SELECT *, rank() OVER (PARTITION BY agent_id ORDER BY rate_rank ASC, sector_rank
                            ASC) as rank
                            FROM b
                    )"""

        elif sector_abbr == 'res':
            sql2 = """b AS
                    (
                        SELECT a.*
                        FROM a
                        WHERE rate_sector = 'R'
                            AND ((a.max_demand_kw <= a.rate_max_demand_kw)
                                  AND (a.max_demand_kw >= a.rate_min_demand_kw))
                            AND ((a.avg_monthly_kwh <= a.rate_max_energy_kwh)
                                  AND (a.avg_monthly_kwh >= a.rate_min_energy_kwh))
                    ),
                    c as
                    (
                            SELECT *, rank() OVER (PARTITION BY agent_id ORDER BY rate_rank ASC) as rank
                            FROM b
                    )"""

        sql3 = """ SELECT agent_id, rate_id_alias, rank, rate_type_tou
                    FROM c
                    WHERE rank = 1
                    );"""

        sql = sql1 + sql2 + sql3
        cur.execute(sql)
        con.commit()

        # get the rates
        sql = """SELECT agent_id, rate_id_alias, rate_type_tou, '%(sector_abbr)s'::VARCHAR(3) as sector_abbr
               FROM  %(schema)s.agent_electric_rate_tariffs_lkup_%(sector_abbr)s a""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float=False)
        df_list.append(df_sector)

    # combine the dfs
    df = pd.concat(df_list, axis=0, ignore_index=True)
    df = df.set_index('agent_id')

    return df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def check_rate_coverage(dataframe, rates_rank_df): #rates_json_df

    # assign a tariff to agents that are missing one
    # TODO: remove this once tariff selection process is fail-proof
    agent_ids = set(dataframe.index)
    rate_agent_ids = set(rates_rank_df.index)
    missing_agents = list(agent_ids.difference(rate_agent_ids))

    if len(missing_agents) > 0:
        print "agents who are missing tariffs:", (missing_agents)
        for missing_agent_id in missing_agents:
            agent_row = dataframe.loc[missing_agent_id]
            if agent_row['sector_abbr'] == 'res':
                agent_row['rate_id_alias'] = int(2778)
                agent_row['rate_type_tou'] = True
            else:
                agent_row['rate_id_alias'] = int(2779)
                agent_row['rate_type_tou'] = True
            rates_rank_df = rates_rank_df.append(agent_row[['sector_abbr', 'rate_id_alias', 'rate_type_tou']])

    missing_agents = list(set(dataframe.index).difference(set(rates_rank_df.index)))
    if len(missing_agents) > 0:
        raise ValueError('Some agents are missing electric rates, including the following agent_ids: {:}'.format(missing_agents))

#    # check that all rate_id_aliases have a nonnull rate json
#    # check for empty dictionary
#    if ({} in rates_json_df['rate_json'].tolist()):
#        raise ValueError('rates_json_df contains empty dictionary objects.')
#    # check for Nones
#    if (None in rates_json_df['rate_json'].tolist()):
#        raise ValueError('rates_json_df contains NoneType objects.')
#    # check for nans
#    if (np.nan in rates_json_df['rate_json'].tolist()):
#        raise ValueError('rates_json_df contains np.nan objects.')

    return rates_rank_df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def identify_selected_rate_ids(rates_rank_df):

    unique_rate_ids = rates_rank_df['rate_id_alias'].unique().tolist()

    return unique_rate_ids


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_electric_rates_json(con, unique_rate_ids):

    inputs = locals().copy()

    # reformat the rate list for use in postgres query
    inputs['rate_id_list'] = utilfunc.pylist_2_pglist(unique_rate_ids)
    inputs['rate_id_list'] = inputs['rate_id_list'].replace("L", "")

    # get (only the required) rate jsons from postgres
    sql = """SELECT a.rate_id_alias, a.rate_name, a.eia_id, a.json as rate_json
             FROM diffusion_shared.urdb3_rate_sam_jsons_20170103 a
             --LEFT JOIN diffusion_shared.urdb3_rate_sam_jsons_20170103 b
             --ON a.rate_id_alias = b.rate_id_alias
             WHERE a.rate_id_alias in (%(rate_id_list)s);""" % inputs
    df = pd.read_sql(sql, con, coerce_float=False)

    return df

@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def filter_nem_year(df, year):

    # Filter by Sector Specific Sunset Years
    df = df.loc[(df['first_year'] <= year) & (df['sunset_year'] >= year)]

    return df

@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_nem_settings(state_limits, state_by_sector, selected_scenario, year, state_capacity_by_year, cf_during_peak_demand):

    # Find States That Have Not Sunset
    valid_states = filter_nem_year(state_limits, year)

    # Filter States to Those That Have Not Exceeded Cumulative Capacity Constraints
    valid_states['filter_year'] = pd.to_numeric(valid_states['max_reference_year'], errors='coerce')
    valid_states['filter_year'][valid_states['max_reference_year'] == 'previous'] = year - 2
    valid_states['filter_year'][valid_states['max_reference_year'] == 'current'] = year
    valid_states['filter_year'][pd.isnull(valid_states['filter_year'])] = year

    state_df = pd.merge(state_capacity_by_year, valid_states , how='left', on=['state_abbr'])
    state_df = state_df[state_df['year'] == state_df['filter_year'] ]
    state_df = state_df.merge(cf_during_peak_demand, on = 'state_abbr')

    state_df = state_df.loc[ pd.isnull(state_df['max_cum_capacity_mw']) | ( pd.notnull( state_df['max_cum_capacity_mw']) & (state_df['cum_capacity_mw'] < state_df['max_cum_capacity_mw']))]
    # Calculate the maximum MW of solar capacity before reaching the NEM cap. MW are determine on a generation basis during the period of peak demand, as determined by ReEDS.
    # CF during peak period is based on ReEDS H17 timeslice, assuming average over south-facing 15 degree tilt systems (so this could be improved by using the actual tilts selected)
    state_df['max_mw'] = (state_df['max_pct_cum_capacity']/100) * state_df['peak_demand_mw'] / state_df['solar_cf_during_peak_demand_period']
    state_df = state_df.loc[ pd.isnull(state_df['max_pct_cum_capacity']) | ( pd.notnull( state_df['max_pct_cum_capacity']) & (state_df['max_mw'] > state_df['cum_capacity_mw']))]

    # Filter state and sector data to those that have not sunset
    selected_state_by_sector = state_by_sector.loc[state_by_sector['scenario'] == selected_scenario]
    valid_state_sector = filter_nem_year(selected_state_by_sector, year)

    # Filter state and sector data to those that match states which have not sunset/reached peak capacity
    valid_state_sector = valid_state_sector[valid_state_sector['state_abbr'].isin(state_df['state_abbr'].values)]

    # Return State/Sector data (or null) for all combinations of states and sectors
    full_list = state_by_sector.loc[ state_by_sector['scenario'] == 'BAU' ].ix[:, ['state_abbr', 'sector_abbr']]
    result = pd.merge( full_list, valid_state_sector, how='left', on=['state_abbr','sector_abbr'] )
    result['pv_kw_limit'].fillna(0, inplace=True)


    return result



#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_core_agent_attributes(con, schema, region):

    inputs = locals().copy()

    # get the agents from postgres
    sql = """SELECT *
             FROM %(schema)s.agent_core_attributes_all;""" % inputs

    df = pd.read_sql(sql, con, coerce_float=False)
    df = df.set_index('agent_id')


    return df

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_technology_performance_wind(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT 'wind'::VARCHAR(5) as tech,
                    a.turbine_size_kw,
                    a.derate_factor as wind_derate_factor
            FROM %(schema)s.input_wind_performance_gen_derate_factors a
            WHERE a.year = %(year)s

            UNION ALL

            SELECT 'wind'::VARCHAR(5) as tech,
                    0::NUMERIC as turbine_size_kw,
                    0::NUMERIC as wind_derate_factor;""" % inputs

    df = pd.read_sql(sql, con, coerce_float=False)

    return df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_annual_resource_wind(con, schema, year, sectors):

    inputs = locals().copy()

    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        a.county_id, a.bin_id,
                    	COALESCE(b.turbine_height_m, 0) as turbine_height_m,
                    	COALESCE(b.turbine_size_kw, 0) as turbine_size_kw,
                    	coalesce(c.interp_factor, 0) as power_curve_interp_factor,
                    	COALESCE(c.power_curve_1, -1) as power_curve_1,
                    	COALESCE(c.power_curve_2, -1) as power_curve_2,
                    	COALESCE(d.aep, 0) as naep_1,
                    	COALESCE(e.aep, 0) as naep_2
                FROM  %(schema)s.agent_core_attributes_%(sector_abbr)s a
                LEFT JOIN %(schema)s.agent_allowable_turbines_lkup_%(sector_abbr)s b
                    	ON a.county_id = b.county_id
                    	and a.bin_id = b.bin_id
                LEFT JOIN %(schema)s.input_wind_performance_power_curve_transitions c
                    	ON b.turbine_size_kw = c.turbine_size_kw
                         AND c.year = %(year)s
                LEFT JOIN diffusion_resource_wind.wind_resource_annual d
                    	ON a.i = d.i
                    	AND a.j = d.j
                    	AND a.cf_bin = d.cf_bin
                    	AND b.turbine_height_m = d.height
                    	AND c.power_curve_1 = d.turbine_id
                LEFT JOIN diffusion_resource_wind.wind_resource_annual e
                    	ON a.i = e.i
                    	AND a.j = e.j
                    	AND a.cf_bin = e.cf_bin
                    	AND b.turbine_height_m = e.height
                    	AND c.power_curve_2 = e.turbine_id;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float=False)
        df_list.append(df_sector)

    df = pd.concat(df_list, axis=0, ignore_index=True)

    return df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_annual_resource_solar(con, schema, sectors):

    inputs = locals().copy()

    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT 'solar'::VARCHAR(5) as tech,
                '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                a.county_id, a.bin_id,
                b.naep
                FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                LEFT JOIN diffusion_resource_solar.solar_resource_annual b
                    ON a.solar_re_9809_gid = b.solar_re_9809_gid
                    AND a.tilt = b.tilt
                    AND a.azimuth = b.azimuth;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float=False)
        df_list.append(df_sector)

    df = pd.concat(df_list, axis=0, ignore_index=True)

    return df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_technology_performance_wind(wind_resource_df, wind_derate_traj, year):
    
    in_cols = list(wind_resource_df)

    wind_resource_df = pd.merge(wind_resource_df, wind_derate_traj[wind_derate_traj['year'] == year], how='left', on=['turbine_size_kw'])
    wind_resource_df['naep'] = (wind_resource_df['power_curve_interp_factor'] * (wind_resource_df['naep_2'] -
                                                                                 wind_resource_df['naep_1']) + wind_resource_df['naep_1']) * wind_resource_df['wind_derate_factor']
    
    return_cols = ['wind_derate_factor', 'naep']
    out_cols = list(pd.unique(in_cols + return_cols))

    return wind_resource_df[out_cols]


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def size_systems_wind(dataframe, system_sizing_targets_df, resource_df, techs):

    if 'wind' in techs:
        in_cols = list(dataframe.columns)
        # join in system sizing targets df
        dataframe = pd.merge(dataframe, system_sizing_targets_df, how='left', on=[
                             'sector_abbr', 'tech'])

        # determine whether NEM is available in the state and sector
        dataframe['ur_enable_net_metering'] = dataframe[
            'pv_kw_limit'] > 0

        # set the target kwh according to NEM availability
        dataframe['target_kwh'] = np.where(dataframe['ur_enable_net_metering'] == False,
                                           dataframe[
                                               'load_kwh_per_customer_in_bin'] * dataframe['sys_size_target_no_nem'],
                                           dataframe['load_kwh_per_customer_in_bin'] * dataframe['sys_size_target_nem'])
        # also set the oversize limit according to NEM availability
        dataframe['oversize_limit_kwh'] = np.where(dataframe['ur_enable_net_metering'] == False,
                                                   dataframe[
                                                       'load_kwh_per_customer_in_bin'] * dataframe['sys_oversize_limit_no_nem'],
                                                   dataframe['load_kwh_per_customer_in_bin'] * dataframe['sys_oversize_limit_nem'])

        # join in the resource data
        dataframe = pd.merge(dataframe, resource_df, how='left', on=[
                             'tech', 'sector_abbr', 'county_id', 'bin_id'])

        # calculate the system generation from naep and turbine_size_kw
        dataframe['aep_kwh'] = dataframe['turbine_size_kw'] * dataframe['naep']

        # initialize values for score and n_units
        dataframe['score'] = np.absolute(
            dataframe['aep_kwh'] - dataframe['target_kwh'])
        dataframe['n_units'] = 1.

        # Handle Special Cases

        # Buildings requiring more electricity than can be generated by the largest turbine (1.5 MW)
        # Return very low rank score and the optimal continuous number of
        # turbines
        big_projects = (dataframe['turbine_size_kw'] == 1500) & (
            dataframe['aep_kwh'] < dataframe['target_kwh'])
        dataframe.loc[big_projects, 'score'] = 0
        # suppress warninings from dividing by zero
        # (only occurs where system size is zero, which is a different slice than big_projects)
        with np.errstate(divide='ignore'):
            dataframe.loc[big_projects, 'n_units'] = np.minimum(
                4, dataframe['target_kwh'] / dataframe['aep_kwh'])

        # identify oversized projects
        oversized_turbines = dataframe[
            'aep_kwh'] > dataframe['oversize_limit_kwh']
        # also identify zero production turbines
        no_kwh = dataframe['aep_kwh'] == 0
        # where either condition is true, set a high score and zero turbines
        dataframe.loc[oversized_turbines | no_kwh, 'score'] = np.array(
            [1e8]) + dataframe['turbine_size_kw'] * 100 + dataframe['turbine_height_m']
        dataframe.loc[oversized_turbines | no_kwh, 'n_units'] = 0.0
        # also disable net metering
        dataframe.loc[oversized_turbines | no_kwh,
                      'ur_enable_net_metering'] = False

        # check that the system is within the net metering size limit
        over_nem_limit = dataframe['turbine_size_kw'] > dataframe[
            'pv_kw_limit']
        dataframe.loc[over_nem_limit, 'score'] = dataframe['score'] * 2
        dataframe.loc[over_nem_limit, 'ur_enable_net_metering'] = False

        # for each agent, find the optimal turbine
        dataframe['rank'] = dataframe.groupby(['county_id', 'bin_id', 'sector_abbr'])[
            'score'].rank(ascending=True, method='first')
        dataframe_sized = dataframe[dataframe['rank'] == 1]
        # add in the system_size_kw field
        dataframe_sized.loc[:, 'system_size_kw'] = dataframe_sized[
            'turbine_size_kw'] * dataframe_sized['n_units']
        # recalculate the aep based on the system size (instead of plain
        # turbine size)
        dataframe_sized.loc[:, 'aep'] = dataframe_sized[
            'system_size_kw'] * dataframe_sized['naep']

        # add capacity factor
        dataframe_sized.loc[:, 'cf'] = dataframe_sized['naep'] / 8760.

        # add system size class
        dataframe_sized.loc[:, 'system_size_factors'] = np.where(dataframe_sized[
                                                                 'system_size_kw'] > 1500, '1500+', dataframe_sized['system_size_kw'].astype('str'))

        # where system size is zero, adjust other dependent columns:
        no_system = dataframe_sized['system_size_kw'] == 0
        dataframe_sized.loc[:, 'power_curve_1'] = np.where(
            no_system, -1, dataframe_sized['power_curve_1'])
        dataframe_sized.loc[:, 'power_curve_2'] = np.where(
            no_system, -1, dataframe_sized['power_curve_2'])
        dataframe_sized.loc[:, 'turbine_size_kw'] = np.where(
            no_system, 0, dataframe_sized['turbine_size_kw'])
        dataframe_sized.loc[:, 'turbine_height_m'] = np.where(
            no_system, 0, dataframe_sized['turbine_height_m'])
        dataframe_sized.loc[:, 'n_units'] = np.where(
            no_system, 0, dataframe_sized['n_units'])
        dataframe_sized.loc[:, 'naep'] = np.where(
            no_system, 0, dataframe_sized['naep'])
        dataframe_sized.loc[:, 'cf'] = np.where(
            no_system, 0, dataframe_sized['cf'])

        # add dummy column for inverter lifetime
        dataframe_sized.loc[:, 'inverter_lifetime_yrs'] = np.nan
        dataframe_sized.loc[:, 'inverter_lifetime_yrs'] = dataframe_sized[
            'inverter_lifetime_yrs'].astype(np.float64)

        return_cols = ['ur_enable_net_metering', 'aep', 'naep', 'cf', 'system_size_kw', 'system_size_factors', 'n_units', 'inverter_lifetime_yrs',
                       'turbine_height_m', 'turbine_size_kw', 'power_curve_1', 'power_curve_2', 'power_curve_interp_factor', 'wind_derate_factor']
        out_cols = list(pd.unique(in_cols + return_cols))

        dataframe_sized = dataframe_sized[out_cols]
    else:
        dataframe_sized = dataframe
        out_cols = {'ur_enable_net_metering': 'bool',
                    'aep': 'float64',
                    'naep': 'float64',
                    'cf': 'float64',
                    'system_size_kw': 'float64',
                    'system_size_factors': 'object',
                    'n_units': 'float64',
                    'inverter_lifetime_yrs': 'float64',
                    'turbine_height_m': 'int64',
                    'turbine_size_kw': 'float64',
                    'power_curve_1': 'int64',
                    'power_curve_2': 'int64',
                    'power_curve_interp_factor': 'float64',
                    'wind_derate_factor': 'float64'
                    }
        for col, dtype in out_cols.iteritems():
            dataframe_sized[col] = pd.Series([], dtype=dtype)

    return dataframe_sized

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_normalized_load_profiles(con, schema, sectors):

    inputs = locals().copy()

    df_list = []
    for sector_abbr, sector in sectors.iteritems():
        inputs['sector_abbr'] = sector_abbr
        sql = """SELECT DISTINCT a.agent_id, '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                        a.county_id, a.bin_id,
                        b.nkwh as consumption_hourly,
                        1e8 as scale_offset
                 FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                 LEFT JOIN diffusion_load_profiles.energy_plus_normalized_load_%(sector_abbr)s b
                     ON a.crb_model = b.crb_model
                     AND a.hdf_load_index = b.hdf_index;""" % inputs
        df_sector = pd.read_sql(sql, con, coerce_float=False)
        df_list.append(df_sector)

    df = pd.concat(df_list, axis=0, ignore_index=True)
    df = df.set_index('agent_id')
    df = df[['consumption_hourly', 'scale_offset']]

    return df


#%%
def scale_array_precision(row, array_col, prec_offset_col):

    row[array_col] = np.array(
        row[array_col], dtype='float64') / row[prec_offset_col]

    return row


#%%
def scale_array_sum(row, array_col, scale_col):

    hourly_array = np.array(row[array_col], dtype='float64')
    row[array_col] = hourly_array / \
        hourly_array.sum() * np.float64(row[scale_col])

    return row


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_normalized_load_profiles(dataframe, load_df):

    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)
    # join the dataframe and load_df
    dataframe = dataframe.join(load_df, how='left')
    # apply the scale offset to convert values to float with correct precision
    dataframe = dataframe.apply(scale_array_precision, axis=1, args=(
        'consumption_hourly', 'scale_offset'))
    # scale the normalized profile to sum to the total load
    dataframe = dataframe.apply(scale_array_sum, axis=1, args=(
        'consumption_hourly', 'load_kwh_per_customer_in_bin'))

    # subset to only the desired output columns
    out_cols = in_cols + ['consumption_hourly']

    dataframe = dataframe[out_cols]

    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_normalized_hourly_resource_solar(con, schema, sectors, techs):

    inputs = locals().copy()

    if 'solar' in techs:
        df_list = []
        for sector_abbr, sector in sectors.iteritems():
            inputs['sector_abbr'] = sector_abbr
            sql = """SELECT 'solar'::VARCHAR(5) as tech,
                            '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                            a.county_id, a.bin_id,
                            b.cf as generation_hourly,
                            1e6 as scale_offset
                    FROM %(schema)s.agent_core_attributes_%(sector_abbr)s a
                    LEFT JOIN diffusion_resource_solar.solar_resource_hourly b
                        ON a.solar_re_9809_gid = b.solar_re_9809_gid
                        AND a.tilt = b.tilt
                        AND a.azimuth = b.azimuth;""" % inputs
            df_sector = pd.read_sql(sql, con, coerce_float=False)
            df_list.append(df_sector)

        df = pd.concat(df_list, axis=0, ignore_index=True)
    else:
        # return empty dataframe with correct fields
        out_cols = {
            'tech': 'object',
            'sector_abbr': 'object',
                    'county_id': 'int64',
                    'bin_id': 'int64',
                    'generation_hourly': 'object',
                    'scale_offset': 'float64'
        }
        df = pd.DataFrame()
        for col, dtype in out_cols.iteritems():
            df[col] = pd.Series([], dtype=dtype)

    return df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_normalized_hourly_resource_wind(con, schema, sectors, cur, agents, techs):

    if 'wind' in techs:
        inputs = locals().copy()

        # isolate the information from agents regarding the power curves and
        # hub heights for each agent
        system_sizes_df = agents.df[agents.df['tech'] == 'wind'][
            ['sector_abbr', 'county_id', 'bin_id', 'i', 'j', 'cf_bin', 'turbine_height_m', 'power_curve_1', 'power_curve_2']]
        system_sizes_df['turbine_height_m'] = system_sizes_df[
            'turbine_height_m'].astype(np.int64)
        system_sizes_df['power_curve_1'] = system_sizes_df[
            'power_curve_1'].astype(np.int64)
        system_sizes_df['power_curve_2'] = system_sizes_df[
            'power_curve_2'].astype(np.int64)

        df_list = []
        for sector_abbr, sector in sectors.iteritems():
            inputs['sector_abbr'] = sector_abbr
            # write the power curve(s) and turbine heights for each agent to
            # postgres
            sql = """DROP TABLE IF EXISTS %(schema)s.agent_selected_turbines_%(sector_abbr)s;
                    CREATE UNLOGGED TABLE %(schema)s.agent_selected_turbines_%(sector_abbr)s
                    (
                        county_id integer,
                        bin_id integer,
                        i integer,
                        j integer,
                        cf_bin integer,
                        turbine_height_m integer,
                        power_curve_1 integer,
                        power_curve_2 integer
                    );""" % inputs
            cur.execute(sql)
            con.commit()

            system_sizes_sector_df = system_sizes_df[system_sizes_df['sector_abbr'] == sector_abbr][
                ['county_id', 'bin_id', 'i', 'j', 'cf_bin', 'turbine_height_m', 'power_curve_1', 'power_curve_2']]
            system_sizes_sector_df['turbine_height_m'] = system_sizes_sector_df[
                'turbine_height_m'].astype(np.int64)

            s = StringIO()
            # write the data to the stringIO
            system_sizes_sector_df.to_csv(s, index=False, header=False)
            # seek back to the beginning of the stringIO file
            s.seek(0)
            # copy the data from the stringio file to the postgres table
            cur.copy_expert(
                'COPY %(schema)s.agent_selected_turbines_%(sector_abbr)s FROM STDOUT WITH CSV' % inputs, s)
            # commit the additions and close the stringio file (clears memory)
            con.commit()
            s.close()

            # add primary key
            sql = """ALTER TABLE %(schema)s.agent_selected_turbines_%(sector_abbr)s
                     ADD PRIMARY KEY (county_id, bin_id);""" % inputs
            cur.execute(sql)
            con.commit()

            # add indices
            sql = """CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_i
                     ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                     USING BTREE(i);

                     CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_j
                     ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                     USING BTREE(j);

                     CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_cf_bin
                     ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                     USING BTREE(cf_bin);

                     CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_turbine_height_m
                     ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                     USING BTREE(turbine_height_m);

                     CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_power_curve_1
                     ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                     USING BTREE(power_curve_1);

                     CREATE INDEX agent_selected_turbines_%(sector_abbr)s_btree_power_curve_2
                     ON %(schema)s.agent_selected_turbines_%(sector_abbr)s
                     USING BTREE(power_curve_2);""" % inputs
            cur.execute(sql)
            con.commit()

            sql = """SELECT 'wind'::VARCHAR(5) as tech,
                            '%(sector_abbr)s'::VARCHAR(3) as sector_abbr,
                            a.county_id, a.bin_id,
                            COALESCE(b.cf, array_fill(1, array[8760])) as generation_hourly_1,
                            COALESCE(c.cf, array_fill(1, array[8760])) as generation_hourly_2,
                            1e3 as scale_offset
                    FROM %(schema)s.agent_selected_turbines_%(sector_abbr)s a
                    LEFT JOIN diffusion_resource_wind.wind_resource_hourly b
                        ON a.i = b.i
                        	AND a.j = b.j
                        	AND a.cf_bin = b.cf_bin
                        	AND a.turbine_height_m = b.height
                        	AND a.power_curve_1 = b.turbine_id
                    LEFT JOIN diffusion_resource_wind.wind_resource_hourly c
                        ON a.i = c.i
                        	AND a.j = c.j
                        	AND a.cf_bin = c.cf_bin
                        	AND a.turbine_height_m = c.height
                        	AND a.power_curve_2 = c.turbine_id;""" % inputs
            df_sector = pd.read_sql(sql, con, coerce_float=False)
            df_list.append(df_sector)

        df = pd.concat(df_list, axis=0, ignore_index=True)
    else:
        # return empty dataframe with correct fields
        out_cols = {
            'tech': 'object',
            'sector_abbr': 'object',
                    'county_id': 'int64',
                    'bin_id': 'int64',
                    'generation_hourly_1': 'object',
                    'generation_hourly_2': 'object',
                    'scale_offset': 'float64'
        }
        df = pd.DataFrame()
        for col, dtype in out_cols.iteritems():
            df[col] = pd.Series([], dtype=dtype)

    return df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_normalized_hourly_resource_solar(dataframe, hourly_resource_df, techs):

    if 'solar' in techs:
        # record the columns in the input dataframe
        in_cols = list(dataframe.columns)

        # join resource data to dataframe
        dataframe = pd.merge(dataframe, hourly_resource_df, how='left', on=[
                             'sector_abbr', 'tech', 'county_id', 'bin_id'])
        # apply the scale offset to convert values to float with correct
        # precision
        dataframe = dataframe.apply(scale_array_precision, axis=1, args=(
            'generation_hourly', 'scale_offset'))
        # scale the normalized profile by the system size
        dataframe = dataframe.apply(
            scale_array_sum, axis=1, args=('generation_hourly', 'aep'))
        # subset to only the desired output columns
        out_cols = in_cols + ['generation_hourly']
        dataframe = dataframe[out_cols]
    else:
        out_cols = {'generation_hourly': 'object'}
        for col, dtype in out_cols.iteritems():
            dataframe[col] = pd.Series([], dtype=dtype)

    return dataframe


#%%
def interpolate_array(row, array_1_col, array_2_col, interp_factor_col, out_col):

    if row[interp_factor_col] <> 0:
        interpolated = row[interp_factor_col] * \
            (row[array_2_col] - row[array_1_col]) + row[array_1_col]
    else:
        interpolated = row[array_1_col]
    row[out_col] = interpolated

    return row


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_normalized_hourly_resource_wind(dataframe, hourly_resource_df):

    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)
    dataframe = dataframe.reset_index()

    # join resource data to dataframe
    dataframe = pd.merge(dataframe, hourly_resource_df, how='left', on=[
                         'sector_abbr', 'tech', 'county_id', 'bin_id'])
    # apply the scale offset to convert values to float with correct
    # precision
    dataframe = dataframe.apply(scale_array_precision, axis=1, args=(
        'generation_hourly_1', 'scale_offset'))
    dataframe = dataframe.apply(scale_array_precision, axis=1, args=(
        'generation_hourly_2', 'scale_offset'))
    # interpolate power curves
    dataframe = dataframe.apply(interpolate_array, axis=1, args=(
        'generation_hourly_1', 'generation_hourly_2', 'power_curve_interp_factor', 'generation_hourly'))
    # scale the normalized profile by the system size
    dataframe = dataframe.apply(
        scale_array_sum, axis=1, args=('generation_hourly', 'aep'))
    # subset to only the desired output columns
    out_cols = in_cols + ['agent_id', 'generation_hourly']

    return dataframe[out_cols]


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_technology_costs_wind(con, schema, year):

    inputs = locals().copy()

    sql = """SELECT 'wind'::VARCHAR(5) as tech,
                    a.turbine_size_kw,
                    a.turbine_height_m,
                    a.installed_costs_dollars_per_kw,
                    a.fixed_om_dollars_per_kw_per_yr,
                    a.variable_om_dollars_per_kwh
                FROM %(schema)s.turbine_costs_per_size_and_year a
            WHERE a.year = %(year)s;""" % inputs
    df = pd.read_sql(sql, con, coerce_float=False)

    return df



#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_tech_costs_wind(dataframe, tech_costs_df):

    # record the columns in the input dataframe
    in_cols = list(dataframe.columns)
    # join the data
    dataframe = pd.merge(dataframe, tech_costs_df, how='left', on=[
                         'tech', 'turbine_size_kw', 'turbine_height_m'])
    # fill nas (these occur where system size is zero)
    dataframe['installed_costs_dollars_per_kw'] = dataframe[
        'installed_costs_dollars_per_kw'].fillna(0)
    dataframe['fixed_om_dollars_per_kw_per_yr'] = dataframe[
        'fixed_om_dollars_per_kw_per_yr'].fillna(0)
    dataframe['variable_om_dollars_per_kwh'] = dataframe[
        'variable_om_dollars_per_kwh'].fillna(0)
    # apply the capital cost multipliers to the installed costs
    dataframe['installed_costs_dollars_per_kw'] = dataframe[
        'installed_costs_dollars_per_kw'] * dataframe['cap_cost_multiplier']

    # add an empty column for the inteverter costs (for compatibility with
    # solar)
    dataframe['inverter_cost_dollars_per_kw'] = np.nan

    # identify the new columns to return
    return_cols = ['inverter_cost_dollars_per_kw', 'installed_costs_dollars_per_kw',
                   'fixed_om_dollars_per_kw_per_yr', 'variable_om_dollars_per_kwh']
    out_cols = in_cols + return_cols

    dataframe = dataframe[out_cols]

    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_carbon_intensities(dataframe, carbon_intensities):

    dataframe = dataframe.reset_index()

    dataframe = pd.merge(dataframe, carbon_intensities, how='left', on=['state_abbr', 'year'])

    dataframe = dataframe.set_index('agent_id')

    return dataframe
    

#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_wholesale_elec_prices(dataframe, wholesale_elec_prices):

    dataframe = dataframe.reset_index()

    dataframe = pd.merge(dataframe, wholesale_elec_prices, how='left', on=['state_abbr', 'year'])

    dataframe = dataframe.set_index('agent_id')

    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_state_starting_capacities(con, schema):

    inputs = locals().copy()

    sql = '''SELECT *
             FROM %(schema)s.state_starting_capacities_to_model;''' % inputs
    df = pd.read_sql(sql, con)

    return df



#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_state_incentives(dataframe, state_incentives, year, state_capacity_by_year):
    dataframe = dataframe.reset_index()

    # Filter Incentives by the Years in which they are valid
    state_incentives = state_incentives.loc[
        pd.isnull(state_incentives['start_date']) | (pd.to_datetime(state_incentives['start_date']).dt.year <= year)]
    state_incentives = state_incentives.loc[
        pd.isnull(state_incentives['end_date']) | (pd.to_datetime(state_incentives['end_date']).dt.year >= year)]

    # Combine valid incentives with the cumulative metrics for each state up until the current year
    state_incentives_mg = state_incentives.merge(state_capacity_by_year.loc[state_capacity_by_year['year'] == year],
                                                 how='left', on=["state_abbr"])

    # Filter where the states have not exceeded their cumulative installed capacity (by mw or pct generation) or total program budget
    #state_incentives_mg = state_incentives_mg.loc[pd.isnull(state_incentives_mg['incentive_cap_total_pct']) | (state_incentives_mg['cum_capacity_pct'] < state_incentives_mg['incentive_cap_total_pct'])]
    state_incentives_mg = state_incentives_mg.loc[pd.isnull(state_incentives_mg['incentive_cap_total_mw']) | (state_incentives_mg['cum_capacity_mw'] < state_incentives_mg['incentive_cap_total_mw'])]
    state_incentives_mg = state_incentives_mg.loc[pd.isnull(state_incentives_mg['budget_total_usd']) | (state_incentives_mg['cum_incentive_spending_usd'] < state_incentives_mg['budget_total_usd'])]

    output  =[]
    for i in state_incentives_mg.groupby(['state_abbr', 'sector_abbr', 'tech']):
        row = i[1]
        state, sector, tech = i[0]
        output.append({'state_abbr':state, 'sector_abbr':sector, 'tech':tech, 'state_incentives':row})

    state_inc_df = pd.DataFrame.from_records(output)
    dataframe = pd.merge(dataframe, state_inc_df, on=['state_abbr','sector_abbr','tech'], how='left')
    
    dataframe = dataframe.set_index('agent_id')


    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def estimate_initial_market_shares(dataframe, state_starting_capacities_df):

    # record input columns
    in_cols = list(dataframe.columns)
    dataframe = dataframe.reset_index()

    # find the total number of customers in each state (by technology and
    # sector)
    state_total_developable_customers = dataframe[['state_abbr', 'sector_abbr', 'tech', 'developable_customers_in_bin']].groupby(
        ['state_abbr', 'sector_abbr', 'tech']).sum().reset_index()
    state_total_agents = dataframe[['state_abbr', 'sector_abbr', 'tech', 'developable_customers_in_bin']].groupby(
        ['state_abbr', 'sector_abbr', 'tech']).count().reset_index()
    # rename the final columns
    state_total_developable_customers.columns = state_total_developable_customers.columns.str.replace(
        'developable_customers_in_bin', 'developable_customers_in_state')
    state_total_agents.columns = state_total_agents.columns.str.replace(
        'developable_customers_in_bin', 'agent_count')
    # merge together
    state_denominators = pd.merge(state_total_developable_customers, state_total_agents, how='left', on=[
                                  'state_abbr', 'sector_abbr', 'tech'])

    # merge back to the main dataframe
    dataframe = pd.merge(dataframe, state_denominators, how='left', on=[
                         'state_abbr', 'sector_abbr', 'tech'])

    # merge in the state starting capacities
    dataframe = pd.merge(dataframe, state_starting_capacities_df, how='left',
                         on=['tech', 'state_abbr', 'sector_abbr'])

    # determine the portion of initial load and systems that should be allocated to each agent
    # (when there are no developable agnets in the state, simply apportion evenly to all agents)
    dataframe['portion_of_state'] = np.where(dataframe['developable_customers_in_state'] > 0,
                                             dataframe[
                                                 'developable_customers_in_bin'] / dataframe['developable_customers_in_state'],
                                             1. / dataframe['agent_count'])
    # apply the agent's portion to the total to calculate starting capacity
    # and systems
    dataframe['number_of_adopters_last_year'] = dataframe['portion_of_state'] * dataframe['systems_count']
    dataframe['system_kw_cum_last_year'] = dataframe['portion_of_state'] * dataframe['capacity_mw'] * 1000.0
    dataframe['batt_kw_cum_last_year'] = 0.0
    dataframe['batt_kwh_cum_last_year'] = 0.0

    dataframe['market_share_last_year'] = np.where(dataframe['developable_customers_in_bin'] == 0, 0,
                                                   dataframe['number_of_adopters_last_year'] / dataframe['developable_customers_in_bin'])

    dataframe['market_value_last_year'] = np.where(dataframe['tech'] == 'solar', dataframe['pv_price_per_kw'] * dataframe['system_kw_cum_last_year'], dataframe['wind_price_per_kw'] * dataframe['system_kw_cum_last_year'])

    # reproduce these columns as "initial" columns too
    dataframe['initial_number_of_adopters'] = dataframe['number_of_adopters_last_year']
    dataframe['initial_system_kw'] = dataframe['system_kw_cum_last_year']
    dataframe['initial_market_share'] = dataframe['market_share_last_year']
    dataframe['initial_market_value'] = 0

    # isolate the return columns
    return_cols = ['initial_number_of_adopters', 'initial_system_kw', 'initial_market_share', 'initial_market_value',
                   'number_of_adopters_last_year', 'system_kw_cum_last_year', 'batt_kw_cum_last_year', 'batt_kwh_cum_last_year', 'market_share_last_year', 'market_value_last_year']

    dataframe[return_cols] = dataframe[return_cols].fillna(0)

    out_cols = in_cols + return_cols
    dataframe = dataframe.set_index('agent_id')

    return dataframe[out_cols]





#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_market_last_year(dataframe, market_last_year_df):

#    dataframe = dataframe.reset_index()

    dataframe = dataframe.join(market_last_year_df)

#    dataframe = dataframe.set_index('agent_id')

    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def estimate_total_generation(dataframe):

    dataframe['total_gen_twh'] = ((dataframe['number_of_adopters'] - dataframe['initial_number_of_adopters'])
                                  * dataframe['aep'] * 1e-9) + (0.23 * 8760 * dataframe['initial_system_kw'] * 1e-6)

    return dataframe


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_tech_potential_limits_wind(con):

    inputs = locals().copy()

    sql = """SELECT state_abbr,
                    cap_gw,
                    gen_gwh,
                    systems_count
            FROM diffusion_wind.tech_potential_by_state;"""
    df = pd.read_sql(sql, con, coerce_float=False)

    return df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def check_tech_potential_limits_wind(dataframe, tech_potential_limits_wind_df, out_dir, is_first_year):

    inputs = locals().copy()

    # only run if it's the first year
    if is_first_year == False:
        pass
    else:
        dataframe['gen_gwh_model'] = dataframe['aep'] * \
            dataframe['developable_customers_in_bin'] / 1e6
        dataframe['systems_count_model'] = dataframe[
            'developable_customers_in_bin']
        dataframe['cap_gw_model'] = dataframe[
            'developable_customers_in_bin'] * dataframe['system_size_kw'] / 1e6

        cols = ['state_abbr',
                'gen_gwh_model',
                'systems_count_model',
                'cap_gw_model']
        model_tech_potential = dataframe[cols].groupby(
            ['state_abbr']).sum().reset_index()

        # combine with tech potential limits and calculate ratios
        model_tech_potential = pd.merge(
            model_tech_potential, tech_potential_limits_wind_df, how='left', on='state_abbr')
        model_tech_potential['pct_of_tech_potential_capacity'] = model_tech_potential[
            'cap_gw_model'] / model_tech_potential['cap_gw']
        model_tech_potential['pct_of_tech_potential_generation'] = model_tech_potential[
            'gen_gwh_model'] / model_tech_potential['gen_gwh']
        model_tech_potential['pct_of_tech_potential_systems_count'] = model_tech_potential[
            'systems_count_model'] / model_tech_potential['systems_count']

        # find overages
        overages = model_tech_potential[(model_tech_potential['pct_of_tech_potential_capacity'] > 1) | (model_tech_potential[
            'pct_of_tech_potential_generation'] > 1) | (model_tech_potential['pct_of_tech_potential_systems_count'] > 1)]

        # report overages, if any
        if overages.shape[0] > 0:
            inputs['out_overage_csv'] = os.path.join(
                out_dir, 'tech_potential_overages_wind.csv')
            logger.warning(
                '\tModel WIND tech potential exceeds actual tech potential for some states. See: %(out_overage_csv)s for details.' % inputs)
            overages.to_csv(inputs['out_overage_csv'],
                            index=False, header=True)
        else:
            inputs['out_ratios_csv'] = os.path.join(
                out_dir, 'tech_potential_ratios_wind.csv')
            logger.info(
                '\tModel WIND tech potential is within state tech potential limits. See: %(out_ratios_csv)s for details.' % inputs)
            cols = ['state_abbr',
                    'pct_of_tech_potential_capacity',
                    'pct_of_tech_potential_generation',
                    'pct_of_tech_potential_systems_count']
            ratios = model_tech_potential[cols]
            ratios.to_csv(inputs['out_ratios_csv'], index=False, header=True)

    return


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def get_tech_potential_limits_solar(con):

    inputs = locals().copy()

    sql = """SELECT state_abbr,
                    size_class as bldg_size_class,
                    cap_gw,
                    gen_gwh,
                    area_m2
             FROM diffusion_solar.rooftop_tech_potential_limits_by_state;"""

    df = pd.read_sql(sql, con, coerce_float=False)

    return df


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def check_tech_potential_limits_solar(dataframe, tech_potential_limits_solar_df, out_dir, is_first_year):

    inputs = locals().copy()

    # only run if it's the first year
    if is_first_year == False:
        pass
    else:
        dataframe['gen_gwh_model'] = dataframe['aep'] * \
            dataframe['developable_customers_in_bin'] / 1e6
        dataframe['area_m2_model'] = dataframe[
            'developable_customers_in_bin'] * dataframe['developable_roof_sqft'] / 10.7639
        dataframe['cap_gw_model'] = dataframe[
            'developable_customers_in_bin'] * dataframe['system_size_kw'] / 1e6

        cols = ['state_abbr',
                'bldg_size_class',
                'gen_gwh_model',
                'area_m2_model',
                'cap_gw_model']
        model_tech_potential = dataframe[cols].groupby(
            ['state_abbr', 'bldg_size_class']).sum().reset_index()

        # combine with tech potential limits and calculate ratios
        model_tech_potential = pd.merge(model_tech_potential, tech_potential_limits_solar_df, how='left', on=[
                                        'state_abbr', 'bldg_size_class'])
        model_tech_potential['pct_of_tech_potential_capacity'] = model_tech_potential[
            'cap_gw_model'] / model_tech_potential['cap_gw']
        model_tech_potential['pct_of_tech_potential_generation'] = model_tech_potential[
            'gen_gwh_model'] / model_tech_potential['gen_gwh']
        model_tech_potential['pct_of_tech_potential_area'] = model_tech_potential[
            'area_m2_model'] / model_tech_potential['area_m2']

        # find overages
        overages = model_tech_potential[(model_tech_potential['pct_of_tech_potential_capacity'] > 1) | (model_tech_potential[
            'pct_of_tech_potential_generation'] > 1) | (model_tech_potential['pct_of_tech_potential_area'] > 1)]

        # report overages, if any
        if overages.shape[0] > 0:
            inputs['out_overage_csv'] = os.path.join(
                out_dir, 'tech_potential_overages_solar.csv')
            logger.warning(
                '\tModel SOLAR tech potential exceeds actual tech potential for some states. See: %(out_overage_csv)s for details.' % inputs)
            overages.to_csv(inputs['out_overage_csv'],
                            index=False, header=True)
        else:
            inputs['out_ratios_csv'] = os.path.join(
                out_dir, 'tech_potential_ratios_solar.csv')
            logger.info(
                '\tModel SOLAR tech potential is within state tech potential limits. See: %(out_ratios_csv)s for details.' % inputs)
            cols = ['state_abbr',
                    'bldg_size_class',
                    'pct_of_tech_potential_capacity',
                    'pct_of_tech_potential_generation',
                    'pct_of_tech_potential_area']
            ratios = model_tech_potential[cols]
            ratios.to_csv(inputs['out_ratios_csv'], index=False, header=True)

    return

#%%   
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def calc_state_capacity_by_year(con, schema, load_growth, peak_demand_mw, census_division_lkup, is_first_year, year,solar_agents,last_year_installed_capacity):

    if is_first_year:
        df = last_year_installed_capacity.query('tech == "solar"').groupby('state_abbr')['capacity_mw'].sum().reset_index()
        # Not all states have starting capacity, don't want to drop any states thus left join on peak_demand
        df = peak_demand_mw.merge(df,how = 'left').fillna(0)
        df['peak_demand_mw'] = df['peak_demand_mw_2014']
        df['cum_capacity_mw'] = df['capacity_mw']

    else:
        #installed_capacity_df = solar_agents.df[['state_abbr','pv_kw_cum','year']].copy()
        #last_year_installed_capacity = installed_capacity_df.loc[installed_capacities_df['year'] == year]
        #df = last_year_installed_capacity.groupby('state_abbr')['pv_kw_cum'].sum().reset_index()
        df = last_year_installed_capacity.copy()
        df['cum_capacity_mw'] = df['system_kw_cum']/1000
        # Load growth is resolved by census region, so a lookup table is needed
        df = df.merge(census_division_lkup, on = 'state_abbr')
        load_growth_this_year = load_growth.loc[(load_growth['year'] == year) & (load_growth['sector_abbr'] == 'res')]
        df = df.merge(load_growth_this_year, on = 'census_division_abbr')
        df = peak_demand_mw.merge(df,how = 'left', on = 'state_abbr').fillna(0)
        df['peak_demand_mw'] = df['peak_demand_mw_2014'] * df['load_multiplier']

    # TODO: drop cum_capacity_pct from table (misnomer)
    df["cum_capacity_pct"] = 0
    # TODO: enforce program spending cap
    df["cum_incentive_spending_usd"] = 0
    df['year'] = year
    df = df[["state_abbr","cum_capacity_mw","cum_capacity_pct","cum_incentive_spending_usd","peak_demand_mw","year"]]
    return df


#%%   
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def process_wind_prices(wind_allowable_turbine_sizes, wind_price_traj):
    
    # join the data
    turbine_prices = pd.merge(wind_allowable_turbine_sizes[wind_allowable_turbine_sizes['allowed'] == True],
                              wind_price_traj, how='left', on=['turbine_size_kw'])
    
    # calculate cost for taller towers
    turbine_prices['tower_cost_adder_dollars_per_kw'] = turbine_prices['cost_for_higher_towers_dollars_per_kw_per_m'] * (
            turbine_prices['turbine_height_m'] - turbine_prices['default_tower_height_m'])
    
    # calculated installed costs (per kW)
    turbine_prices['installed_costs_dollars_per_kw'] = (turbine_prices['capital_cost_dollars_per_kw'] + 
                  turbine_prices['cost_for_higher_towers_dollars_per_kw_per_m'] * (turbine_prices['turbine_height_m'] - turbine_prices['default_tower_height_m']))
    
    return_cols= ['turbine_size_kw', 'turbine_height_m', 'year', 'capital_cost_dollars_per_kw', 'fixed_om_dollars_per_kw_per_yr', 'variable_om_dollars_per_kwh',
                  'cost_for_higher_towers_dollars_per_kw_per_m', 'tower_cost_adder_dollars_per_kw', 'installed_costs_dollars_per_kw']    
    
    return turbine_prices[return_cols]


#%%
@decorators.fn_timer(logger=logger, tab_level=2, prefix='')
def apply_wind_prices(dataframe, turbine_prices):

    in_cols = list(dataframe.columns)    
    dataframe = dataframe.reset_index()

    # join the data
    dataframe = pd.merge(dataframe, turbine_prices, how='left', on=['turbine_size_kw', 'turbine_height_m', 'year'])

    # fill nas (these occur where system size is zero)
    dataframe['installed_costs_dollars_per_kw'] = dataframe['installed_costs_dollars_per_kw'].fillna(0)
    dataframe['fixed_om_dollars_per_kw_per_yr'] = dataframe['fixed_om_dollars_per_kw_per_yr'].fillna(0)
    dataframe['variable_om_dollars_per_kwh'] = dataframe['variable_om_dollars_per_kwh'].fillna(0)

    # apply the capital cost multipliers
    dataframe['wind_price_per_kw'] = (dataframe['installed_costs_dollars_per_kw'] * dataframe['cap_cost_multiplier'])
    
    # rename fixed O&M column for later compatibility
    dataframe.rename(columns={'fixed_om_dollars_per_kw_per_yr':'wind_om_per_kw'}, inplace=True)

    return_cols = ['wind_price_per_kw', 'wind_om_per_kw', 'variable_om_dollars_per_kwh']
    out_cols = list(pd.unique(in_cols + return_cols))
    
    dataframe = dataframe.set_index('agent_id')

    return dataframe[out_cols]