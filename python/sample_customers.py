# -*- coding: utf-8 -*-
"""
Created on Wed Mar 19 15:05:55 2014

@author: mgleason
"""

import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg
import time

# initialize a string that holds all of the Postgres connection parameters
pg_params = "dbname=dav-gis user=mgleason password=mgleason host=gispgdb.nrel.gov" 

# establish a connection to the database
conn = pg.connect(pg_params)

# create cursor
cur = conn.cursor()

def getNRandomRows(table, N, column_names = None, group_fields = None, weight_field = None, seed = 1, out_table = None):
    
    '''
        Function to randomly sample N rows from a table or from partitions of a table, specified by group_fields.
        Optionally can be used for weighted random sampling by applying a weight field
    
    '''
    
    if group_fields is not None:
        partition_by = 'PARTITION BY %s ' % ','.join(group_fields)
    else:
        partition_by = ''

    if weight_field is not None:
        order_by = 'ORDER BY random() * %s' % weight_field
    else:
        order_by = 'ORDER BY random()'
    

    if column_names is not None:
        columns = ','.join(column_names)
    else:
        columns = '*'
    
    # this is the main sampling code
    # depending on how the data will be returned (new table or pandas df), it will be inserted into wrapper sql
    sample_query = "WITH a as (\
                	SELECT %s, ROW_NUMBER() OVER (%s %s) as row_number\
                	FROM %s)\
                SELECT *\
                FROM a\
                where row_number <= %s" % (columns, partition_by, order_by, table, N)    
    
    if out_table is not None:
        # drop the table if it existed
        sql = 'DROP TABLE IF EXISTS %s;' % out_table
        cur.execute(sql)
        conn.commit()
        
        sql = "SET LOCAL SEED TO %s;\
                CREATE TABLE %s AS (%s);" % (seed,out_table, sample_query)
        cur.execute(sql)
        conn.commit()         

        return 'Table %s created' % out_table
        
    else:
        sql = "SET LOCAL SEED TO %s;\
              %s;" % (seed,sample_query)
        df = sqlio.read_frame(sql, conn)
  
        return df

# to do:
    # create views of the pts tables with all of the info joined in appropriately
    # each table needs the following
    # annual electric rate
    # AEP for each applicable turbine and height
    # Initial Incentives
    # Height Exclusions
    # Load (bin weight X county totals)
    # # of customers (bin weight X county total)
    
# process
# create views of 

customer_bins = 100
random_generator_seed = 1
#sectors = {'res':'residential','com':'commercial','ind':'industrial'}
sectors = {'res':'residential'}

# other vars we need:
starting_year = 2014
exclusions = ''

for sector in sectors.keys():
    if sector == 'ind' and customer_bins >= 83:
        n_bins = 83
    else:
        n_bins = customer_bins
    pts_all_table = 'wind_ds.pt_grid_us_%s_joined' % sector
    pts_sample_table = 'wind_ds.pt_grid_us_%s_sample' % sector
    result = getNRandomRows(pts_all_table, n_bins, group_fields = ['county_id'],seed = random_generator_seed, out_table = pts_sample_table)
    print result
    
    # join to customer bins
    pts_load = 'wind_ds.pt_grid_us_%s_sample_load' % sector
    sql = "SET LOCAL SEED TO %s;\
            CREATE TABLE %s AS \
            WITH weighted_county_sample as (\
                SELECT a.county_id, row_number() OVER (PARTITION BY a.county_id ORDER BY random() * b.prob) as row_number, b.*\
                FROM wind_ds.county_geom a\
            LEFT JOIN wind_ds.binned_annual_load_kwh_%s_bins b\
            ON a.census_region = b.census_region\
            AND b.sector = '%s')\
        SELECT a.*, b.ann_cons_kwh, b.prob, b.weight, \
                a.county_total_customers_2011 * b.weight/sum(weight) OVER (PARTITION BY a.county_id) as customers_in_bin, \
                a.county_total_load_mwh_2011 * (b.ann_cons_kwh*b.weight)/sum(b.ann_cons_kwh*b.weight) OVER (PARTITION BY a.county_id) as load_mwh_in_bin\
        FROM %s a\
        LEFT JOIN weighted_county_sample b\
        ON a.county_id = b.county_id\
        and a.row_number = b.row_number;" % (random_generator_seed, pts_load, customer_bins, sectors[sector], pts_sample_table)
    cur.execute(sql)
    conn.commit()
    
    # join to all combinations of resource values
    # see code in setting_up_df_for_script_3.sql
    
    # looking to return data frame with best turbine only and all related costs





