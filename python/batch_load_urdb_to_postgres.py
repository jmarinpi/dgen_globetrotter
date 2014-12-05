# -*- coding: utf-8 -*-
"""
Created on Tue Dec  2 12:25:46 2014

@author: mgleason
"""

import psycopg2 as pg
import psycopg2.extras as pgx
import urdb_to_sam
import pandas as pd
from cStringIO import StringIO
import datetime
import json

def pg_connect(pg_params):
    pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s' % pg_params
    con = pg.connect(pg_conn_string)
    cur = con.cursor(cursor_factory=pgx.RealDictCursor)
    
    if 'role' in pg_params.keys():
        sql = "SET ROLE '%(role)s';" % pg_params
        cur.execute(sql)
        con.commit()
    
    return con, cur


def get_rate_keys(cur, lookup_table):
    # get rate ids that we want to load
    sql = """SELECT DISTINCT(urdb_rate_id) as rate_key
             FROM %s;""" % lookup_table
    cur.execute(sql)
    rate_keys = [row['rate_key'] for row in cur.fetchall()]
    
    return rate_keys

def create_output_table(cur, con, sql_params):
    
    sql = """DROP TABLE IF EXISTS %(output_table)s;
         CREATE TABLE %(output_table)s
         (
            urdb_rate_id TEXT,
            ur_name TEXT,
            ur_schedule_name TEXT,
            ur_source TEXT,
            rateurl TEXT,
            jsonurl TEXT,
            ur_description TEXT,
            sam_json JSON,
            applicability JSON
         );""" % sql_params
    cur.execute(sql)
    con.commit()
    

def urdb_to_pg(rate_keys, cur, con, sql_params, log):
    
    # open an in-memory stringio file
    f = StringIO()
    
    # set a list of the output field names
    output_fields = ['urdb_rate_id', 'ur_name', 'ur_schedule_name', 'ur_source', 'rateurl', 'jsonurl', 'ur_description', 'sam_json', 'applicability']
    
    # 
    for rate_key in rate_keys:
        print rate_key
        try:
            rate_data = urdb_to_sam.urdb_rate_to_sam_structure(rate_key)
            # format sam_json as actual JSON string
            rate_data['sam_json'] =  json.dumps(rate_data['sam_json'])
            # format applicability as an actual JSON string
            rate_data['applicability'] =  json.dumps(rate_data['applicability'])
            # initialize list to hold the values that will be written to pg
            output_list = []
            for field in output_fields:
                if field in rate_data.keys():
                    output_list.append(str(rate_data[field]))
                else:
                    output_list.append('')
            # convert list to a single text string
            output_line = ','.join(['^%s^' % s for s in output_list]) + '\n'
            f.write(output_line)
        except Exception, e:
            print e
            log.write('Error on rate %s: %s\n' % (rate_key,e))
    
    f.seek(0)    
    # copy the data from the stringio file to the postgres table
    cur.copy_expert("COPY %(output_table)s FROM STDOUT WITH CSV QUOTE '^'" % sql_params, f)
    # commit the additions
    con.commit()    
    # close the stringio file (clears memory)
    f.close()

def open_log():
    
    cdate = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = 'batch_load_urdb_to_postgres_%s.log' % cdate
    log = file(log_file,'w')
    log.write('Log of errors for batch_load_urdb_to_postgres.py (%s)\n' % cdate)
    
    return(log)
    
#==============================================================================
# INPUT PARAMETERS

# postgres connection parameters
pg_params = {'host'     : 'gispgdb',
             'dbname'   : 'dav-gis',
             'user'     : 'mgleason',
             'password' : 'mgleason',
             'role'     : 'urdb_rates-writers'
             }


 
#==============================================================================

def main(rate_type):
    
    # OPEN LOG FILE
    log = open_log()
    
    # CONNECT TO POSTGRES
    con, cur = pg_connect(pg_params)
    
    # SET THE OUTPUT TABLE AND LOOKUP TABLE NAMES BASED ON THE RATE TYPE    
    lookup_table = 'urdb_rates.urdb3_%s_rates_lookup_20141202' % rate_type
    output_table = 'urdb_rates.urdb3_%s_rates_sam_data_20141202' % rate_type
    sql_params = {'output_table' : output_table}    
    
    # GET URDB IDS FOR THE RATES TO COLLECT
    rate_keys = get_rate_keys(cur, lookup_table)
    
    # CREATE (EMPTY) OUTPUT TABLE
    create_output_table(cur, con, sql_params)
    
    # RUN THE CONVERSION PROCESS
    urdb_to_pg(rate_keys, cur, con, sql_params, log)
    
    # CLOSE THE LOGGER
    log.close()

if __name__ == '__main__':
    
    rate_type = 'singular'
#    rate_type = 'verified'    

    main(rate_type)
    
