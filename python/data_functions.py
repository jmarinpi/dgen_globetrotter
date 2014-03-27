# -*- coding: utf-8 -*-
"""
Functions for pulling data
Created on Mon Mar 24 08:59:44 2014
@author: bsigrin
"""
import psycopg2 as pg
import psycopg2.extras as pgx
import pandas.io.sql as sqlio
import time   
import numpy as np
from scipy.interpolate import interp1d as interp1d
import pandas as pd


def pylist_2_pglist(l):
    return str(l)[1:-1]

def make_con(connection_string):    
    con = pg.connect(connection_string)
    # create cursor object
    cur = con.cursor(cursor_factory=pgx.RealDictCursor)
    # set role (this should avoid permissions issues)
    cur.execute('SET ROLE "wind_ds-writers";')    
    
    return con, cur


def combine_temporal_data(cur, con, start_year, end_year, sectors):
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()       
    
    t0 = time.time()    
    
    # combine all of the temporal data (this only needs to be done once for all sectors)
    print "Combining Temporal Factors"
    sql = "DROP TABLE IF EXISTS wind_ds.temporal_factors;\
            CREATE TABLE wind_ds.temporal_factors as \
            SELECT a.year, a.nameplate_capacity_kw, a.power_curve_id,\
            	b.turbine_height_m,\
            	c.fixed_om_dollars_per_kw_per_yr, \
            	c.variable_om_dollars_per_kwh,\
            	c.installed_costs_dollars_per_kw,\
            	d.census_division_abbr,\
            	d.sector,\
            	d.escalation_factor as rate_escalation_factor,\
            	d.source as rate_escalation_source,\
            	e.scenario as load_growth_scenario,\
            	e.load_multiplier	\
            FROM wind_ds.wind_performance_improvements a\
            LEFT JOIN wind_ds.allowable_turbine_sizes b\
            ON a.nameplate_capacity_kw = b.turbine_size_kw\
            LEFT JOIN wind_ds.turbine_costs_per_size_and_year c\
            ON a.nameplate_capacity_kw = c.turbine_size_kw\
            AND a.year = c.year\
            LEFT JOIN wind_ds.rate_escalations d\
            ON a.year = d.year\
            LEFT JOIN wind_ds.aeo_load_growth_projections e\
            ON d.census_division_abbr = e.census_division_abbr\
            AND a.year = e.year\
            WHERE a.year BETWEEN %(start_year)s AND %(end_year)s\
            AND d.sector in (%(sectors)s);" % inputs
    cur.execute(sql)
    con.commit()
    
    # create indices for subsequent joins
    sql =  "CREATE INDEX temporal_factors_turbine_height_m_btree on wind_ds.temporal_factors using btree(turbine_height_m);\
            CREATE INDEX temporal_factors_sector_btree ON wind_ds.temporal_factors using btree(sector);\
            CREATE INDEX temporal_factors_load_growth_scenario_btree ON wind_ds.temporal_factors using btree(load_growth_scenario);\
            CREATE INDEX temporal_factors_rate_escalation_source_btree ON wind_ds.temporal_factors USING btree(rate_escalation_source);\
            CREATE INDEX temporal_factors_census_division_abbr_btree ON wind_ds.temporal_factors USING btree(census_division_abbr);\
            CREATE INDEX temporal_factors_join_fields_btree ON wind_ds.temporal_factors USING btree(turbine_height_m, census_division_abbr, power_curve_id);"
    cur.execute(sql)
    con.commit()
    
    print time.time()-t0    
    
    return 1
    
    
    

def generate_customer_bins(cur, con, seed, n_bins, sector_abbr, sector, start_year, end_year, rate_escalation_source, load_growth_scenario, exclusion_type, oversize_turbine_factor,undersize_turbine_factor,preprocess):
    
    # create a dictionary out of the input arguments -- this is used through sql queries    
    inputs = locals().copy()       
    
    t0 = time.time()    
    if preprocess == 'TRUE':
        table_name_dict = {'res': 'wind_ds.pt_res_best_option_each_year', 'com' : 'wind_ds.pt_com_best_option_each_year', 'ind' : 'wind_ds.pt_ind_best_option_each_year'}
        return table_name_dict[sector_abbr]
    
    
    #==============================================================================
    #     randomly sample  N points from each county 
    #==============================================================================
    # (note: some counties will have fewer than N points, in which case, all are returned) 
    print 'Sampling Customer Bins from Each County'
    sql =  "DROP TABLE IF EXISTS wind_ds.pt_%(sector_abbr)s_sample;\
            SET LOCAL SEED TO %(seed)s;\
            CREATE TABLE wind_ds.pt_%(sector_abbr)s_sample AS\
            WITH a as (\
            	SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a.county_id order by random()) as row_number\
            	FROM wind_ds.pt_grid_us_%(sector_abbr)s_joined a\
            	INNER JOIN wind_ds.counties_to_model b\
            	ON a.county_id = b.county_id)\
            SELECT *\
            FROM a\
            where row_number <= %(n_bins)s;" % inputs
    cur.execute(sql)
    con.commit()
    print time.time()-t0
    
    #==============================================================================
    #     link each point to a load bin
    #==============================================================================
    # use random weighted sampling on the load bins to ensure that countyies with <N points
    # have a representative sample of load bins
    print 'Associating Customer Bins with Load and Customer Count'
    sql =  "DROP TABLE IF EXISTS wind_ds.pt_%(sector_abbr)s_sample_load;\
            SET LOCAL SEED TO %(seed)s;\
            CREATE TABLE wind_ds.pt_%(sector_abbr)s_sample_load AS\
            WITH weighted_county_sample as (\
            	SELECT a.county_id, row_number() OVER (PARTITION BY a.county_id ORDER BY random() * b.prob) as row_number, b.*\
            	FROM wind_ds.counties_to_model a\
            	LEFT JOIN wind_ds.binned_annual_load_kwh_%(n_bins)s_bins b\
            	ON a.census_region = b.census_region\
            	AND b.sector = 'residential'),\
            binned as(\
            SELECT a.*, b.ann_cons_kwh, b.prob, b.weight,\
            	a.county_total_customers_2011 * b.weight/sum(weight) OVER (PARTITION BY a.county_id) as customers_in_bin, \
            	a.county_total_load_mwh_2011 * 1000 * (b.ann_cons_kwh*b.weight)/sum(b.ann_cons_kwh*b.weight) OVER (PARTITION BY a.county_id) as load_kwh_in_bin	\
            FROM wind_ds.pt_%(sector_abbr)s_sample a\
            LEFT JOIN weighted_county_sample b\
            ON a.county_id = b.county_id\
            and a.row_number = b.row_number\
            where county_total_load_mwh_2011 > 0)\
            SELECT a.*,\
            	case when a.customers_in_bin > 0 THEN a.load_kwh_in_bin/a.customers_in_bin \
            	else 0\
            	end as load_kwh_per_customer_in_bin\
            FROM binned a;" % inputs
    cur.execute(sql)
    con.commit()
    
    # add primary key and indices to speed up subsequent joins
    sql =  "ALTER TABLE wind_ds.pt_%(sector_abbr)s_sample_load ADD PRIMARY Key (gid);\
            CREATE INDEX pt_%(sector_abbr)s_sample_load_census_division_abbr_btree ON wind_ds.pt_%(sector_abbr)s_sample_load USING BTREE(census_division_abbr);\
            CREATE INDEX pt_%(sector_abbr)s_sample_load_i_j_cf_bin ON wind_ds.pt_%(sector_abbr)s_sample_load using BTREE(i,j,cf_bin);" % inputs
    cur.execute(sql)
    con.commit()
    # add index for exclusions (if they apply)
    if exclusion_type is not None:
        sql =  "CREATE INDEX pt_%(sector_abbr)s_sample_load_%(exclusion_type)s_btree ON wind_ds.pt_%(sector_abbr)s_sample_load USING BTREE(%(exclusion_type)s)\
                WHERE %(exclusion_type)s > 0;" % inputs
        cur.execute(sql)
        con.commit()
    
    print time.time()-t0
    
    #==============================================================================
    #     Find All Combinations of Points and Wind Resource
    #==============================================================================
    print "Finding All Wind Resource Combinations for Each Customer Bin"
    sql =  "DROP TABLE IF EXISTS wind_ds.pt_%(sector_abbr)s_sample_load_and_wind;\
            CREATE TABLE wind_ds.pt_%(sector_abbr)s_sample_load_and_wind AS\
            SELECT a.*,\
            	c.aep*a.aep_scale_factor*a.derate_factor as naep,\
            	c.turbine_id as power_curve_id, \
            	c.height as turbine_height_m\
            	FROM wind_ds.pt_%(sector_abbr)s_sample_load a\
            	LEFT JOIN wind_ds.wind_resource_annual c\
            	ON a.i = c.i\
            	AND a.j = c.j\
            	AND a.cf_bin = c.cf_bin\
            	AND a.%(exclusion_type)s >= c.height\
            	WHERE a.%(exclusion_type)s > 0;" % inputs
    cur.execute(sql)
    con.commit()
    
    # create indices for subsequent joins
    sql =  "CREATE INDEX pt_%(sector_abbr)s_sample_load_and_wind_join_fields_btree ON wind_ds.pt_%(sector_abbr)s_sample_load_and_wind USING btree(turbine_height_m, census_division_abbr, power_curve_id);" % inputs
    cur.execute(sql)
    con.commit()
    print time.time() - t0
    
    #==============================================================================
    #     Find All Combinations of Costs and Resource for Each Customer Bin
    #==============================================================================
    print "Finding All Combination of Cost and Resource for Each Customer Bin and Year"
    # this combines all wind combos with all cost combos and calculates the simple cost of energy under each combination
    sql =  "DROP TABLE IF EXISTS wind_ds.pt_%(sector_abbr)s_sample_all_combinations;\
            CREATE TABLE wind_ds.pt_%(sector_abbr)s_sample_all_combinations AS\
            SELECT\
             	a.gid, b.year, a.county_id, a.state_abbr, a.census_division_abbr, a.census_region, a.row_number, \
             	a.%(exclusion_type)s as max_height, \
            	a.elec_rate_cents_per_kwh * b.rate_escalation_factor as elec_rate_cents_per_kwh, \
            	a.cap_cost_multiplier,\
            	b.fixed_om_dollars_per_kw_per_yr, \
            	b.variable_om_dollars_per_kwh,\
            	b.installed_costs_dollars_per_kw * a.cap_cost_multiplier::numeric as installed_costs_dollars_per_kw,\
            	a.ann_cons_kwh, a.prob, a.weight,\
            	b.load_multiplier * a.customers_in_bin as customers_in_bin, \
            	a.customers_in_bin as initial_customers_in_bin, \
            	b.load_multiplier * a.load_kwh_in_bin AS load_kwh_in_bin,\
            	a.load_kwh_in_bin AS initial_load_kwh_in_bin,\
            	a.load_kwh_per_customer_in_bin,\
            	a.i, a.j, a.cf_bin, a.aep_scale_factor, a.derate_factor,\
            	a.naep,\
            	b.nameplate_capacity_kw,\
            	a.power_curve_id, \
            	a.turbine_height_m,\
            	wind_ds.scoe(b.installed_costs_dollars_per_kw, b.fixed_om_dollars_per_kw_per_yr, b.variable_om_dollars_per_kwh, a.naep , b.nameplate_capacity_kw , a.load_kwh_per_customer_in_bin , %(oversize_turbine_factor)s, %(undersize_turbine_factor)s) as scoe\
            FROM wind_ds.pt_%(sector_abbr)s_sample_load_and_wind a\
            INNER JOIN wind_ds.temporal_factors b\
            ON a.turbine_height_m = b.turbine_height_m\
            AND a.power_curve_id = b.power_curve_id\
            AND a.census_division_abbr = b.census_division_abbr\
            WHERE b.sector = '%(sector)s'\
            AND b.rate_escalation_source = '%(rate_escalation_source)s'\
            AND b.load_growth_scenario = '%(load_growth_scenario)s';" % inputs
    cur.execute(sql)
    con.commit()
    
    print time.time()-t0

    #==============================================================================
    #    Find the Most Cost-Effective Wind Turbine Configuration for Each Customer Bin
    #==============================================================================
    print "Selecting the most cost-effective wind turbine configuration for each customer bin and year"
    sql =  "DROP TABLE IF EXISTS wind_ds.pt_%(sector_abbr)s_best_option_each_year;\
            CREATE TABLE wind_ds.pt_%(sector_abbr)s_best_option_each_year AS\
            SELECT distinct on (a.gid, a.year) a.*\
            FROM  wind_ds.pt_%(sector_abbr)s_sample_all_combinations a\
            ORDER BY a.gid, a.year, a.scoe ASC;" % inputs
    cur.execute(sql)
    con.commit()

    # create index on the year and county fields
    sql =  "CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_year_btree ON wind_ds.pt_%(sector_abbr)s_best_option_each_year using BTREE(year);\
            CREATE INDEX pt_%(sector_abbr)s_best_option_each_year_county_id_btree ON wind_ds.pt_%(sector_abbr)s_best_option_each_year using BTREE(county_id);" % inputs
    cur.execute(sql)
    con.commit()
    print time.time()-t0

    final_table = 'wind_ds.pt_%(sector_abbr)s_best_option_each_year' % inputs
    
    return final_table

def get_sectors(cur):
    '''Return the sectors to model from table view in postgres.
        Returned as a dictionary.
        '''    
    
    sql = 'SELECT sectors FROM wind_ds.sectors_to_model;'
    cur.execute(sql)
    sectors = cur.fetchone()['sectors']
    return sectors
    
    
def get_exclusions(cur):
    '''Return the sectors to model from table view in postgres.
        Returned as a dictionary.
        '''    
    
    sql = 'SELECT * FROM wind_ds.exclusions_to_model;'
    cur.execute(sql)
    exclusions = cur.fetchone()['exclusions']
    return exclusions
    
def get_depreciation_schedule(con, type = 'all'):
    ''' Pull depreciation schedule from dB
    
        IN: type - string - [all, macrs, standard] 
        OUT: df  - pd dataframe - year, depreciation schedule:

    '''
    if not con:
        close_con = True
        con = make_con()
    else:
        close_con = False    
    if type.lower() == 'macrs':
        sql = 'SELECT macrs FROM wind_ds.depreciation_schedule'
    elif type.lower() == 'standard':
        sql = 'SELECT standard FROM wind_ds.depreciation_schedule'
    else:
        sql = 'SELECT * FROM wind_ds.depreciation_schedule'
    df = sqlio.read_frame(sql, con)
    return df
    
def get_scenario_options(cur):
    ''' Pull scenario options from dB
    
        IN: none
        OUT: scenario_options - pandas data frame:
                    'region', 
                    'end_year', 
                    'markets', 
                    'cust_exp_elec_rates', 
                    'res_rate_structure', 
                    'res_rate_escalation', 
                    'res_max_market_curve', 
                    'com_rate_structure', 
                    'com_rate_escalation', 
                    'com_max_market_curve', 
                    'ind_rate_structure', 
                    'ind_rate_escalation', 
                    'ind_max_market_curve', 
                    'net_metering_availability', 
                    'carbon_price', 
                    'height_exclusions', 
                    'ann_inflation', 
                    'scenario_name', 
                    'overwrite_exist_inc', 
                    'starting_year', 
                    'utility_type_iou', 
                    'utility_type_muni', 
                    'utility_type_coop', 
                    'utility_type_allother'
        
    '''
    sql = "SELECT * FROM wind_ds.scenario_options"
    cur.execute(sql)
    results = cur.fetchall()[0]
    return results
    
def get_main_dataframe(con, main_table, year):
    ''' Pull main pre-processed dataframe from dB
    
        IN: con - pg con object - connection object
        OUT: df  - pd dataframe - pre-processed resource,bins, rates, etc. for all years:

    '''
    if not con:
        close_con = True
        con = make_con()
    else:
        close_con = False
    sql = 'SELECT * FROM %s WHERE year = %s' % (main_table,year)
    df = sqlio.read_frame(sql, con)
    return df
    
def get_financial_parameters(con, res_model = 'Existing Home', com_model = 'Host Owned', ind_model = 'Host Owned'):
    ''' Pull financial parameters dataframe from dB. Use passed parameters to subset for new/existing home/leasing/host-owned
    
        IN: con - pg con object - connection object
            res - string - which residential ownership structure to use (assume 100%)
            com - string - which commercial ownership structure to use (assume 100%)
            ind - string - which industrial ownership structure to use (assume 100%)
            
        OUT: fin_param  - pd dataframe - pre-processed resource,bins, rates, etc. for all years:
    '''
    
    sql = 'SELECT * FROM wind_ds.financial_parameters'
    df = sqlio.read_frame(sql, con)
    
    # Filter based on ownership models selected
    df['sector'] = ['residential', 'residential', 'residential', 'commercial', 'commercial','industrial','industrial']
        
    
    df = df[((df['cust_disposition'] == res_model) & (df['cust_id'] == 1)) | 
      ((df['cust_disposition'] == com_model) & (df['cust_id'] == 2)) |
      ((df['cust_disposition'] == ind_model) & (df['cust_id'] == 3))]
      
    df = df.drop('cust_id',1)
    return df
 
#==============================================================================
   
def get_max_market_share(con, scenario_opts, res_type = 'retrofit', com_type = 'retrofit', ind_type = 'retrofit'):
    ''' Pull max market share from dB, select curve based on scenario_options, and interpolate to tenth of a year. 
        Use passed parameters to determine ownership type
    
        IN: con - pg con object - connection object
            res_type - string - which residential ownership structure to use (new or retrofit)
            com_type - string - which commercial ownership structure to use (new or retrofit)
            ind_type - string - which industrial ownership structure to use (new or retrofit)
            
        OUT: max_market_share  - pd dataframe - dataframe to join on main df to determine max share 
                                                keys are sector & payback period 
    '''
    # Query data    
    max_market_share = sqlio.read_frame('SELECT * FROM wind_ds.max_market_share', con)
    user_defined_max_market_share = sqlio.read_frame('SELECT * FROM wind_ds.user_defined_max_market_share', con)
    
    # Rename so the user-defined can be concatenated
    max_market_share.columns = ['year','new','retrofit','sector','source']
    max_market_share = max_market_share[['year','sector','new','retrofit','source']]
    max_market_share['sector']  = [i.lower() for i in max_market_share['sector'].values]
    
    user_defined_max_market_share['source'] = 'User Defined'
    user_defined_max_market_share['sector']  = [i.lower() for i in user_defined_max_market_share['sector'].values]
    
    mm = max_market_share.append(user_defined_max_market_share, ignore_index = 'TRUE')
    
    # Select the max market share curve for each sector
    max_market_res = mm[(mm['sector'] == 'residential') & (mm['source'] == scenario_opts['res_max_market_curve'])]
    max_market_com = mm[(mm['sector'] == 'commercial') & (mm['source'] == scenario_opts['com_max_market_curve'])]
    max_market_ind = mm[(mm['sector'] == 'industrial') & (mm['source'] == scenario_opts['ind_max_market_curve'])]     
    
    # Now interpolate each curve
    yrs=np.linspace(0,30,31);
    yrs2=np.linspace(0,30,301);
    
    f1 = interp1d(max_market_res['year'], max_market_res[res_type]);
    f2 = interp1d(max_market_com['year'], max_market_com[com_type]);
    f3 = interp1d(max_market_ind['year'], max_market_ind[ind_type]);
    
#    res_max_market = pd.DataFrame({'payback_period': yrs2, 'max_market_share': f1(yrs2),'sector': 'residential','payback_key': np.arange(301)})
#    com_max_market = pd.DataFrame({'payback_period': yrs2, 'max_market_share': f2(yrs2),'sector': 'commercial','payback_key': np.arange(301)})   
#    ind_max_market = pd.DataFrame({'payback_period': yrs2, 'max_market_share': f3(yrs2),'sector': 'industrial','payback_key': np.arange(301)})
    
    res_max_market = pd.DataFrame({'max_market_share': f1(yrs2),'sector': 'residential','payback_key': np.arange(301)})
    com_max_market = pd.DataFrame({'max_market_share': f2(yrs2),'sector': 'commercial','payback_key': np.arange(301)})   
    ind_max_market = pd.DataFrame({'max_market_share': f3(yrs2),'sector': 'industrial','payback_key': np.arange(301)}) 
    return res_max_market.append(com_max_market, ignore_index = 'TRUE').append(ind_max_market, ignore_index = 'TRUE')