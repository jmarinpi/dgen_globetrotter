# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 09:14:22 2015

@author: bsigrin
"""

import psycopg2 as pg
import json
import psycopg2.extras as pgx
import pandas as pd     
import numpy as np
import time

def make_con(connection_string, async = False):    
    con = pg.connect(connection_string, async = async)
    if async:
        wait(con)
    # create cursor object
    cur = con.cursor(cursor_factory=pgx.RealDictCursor)
    # set role (this should avoid permissions issues)
    cur.execute('SET ROLE "diffusion-writers";')    
    if async:
        wait(con)
    else:
        con.commit()
    
    return con, cur

# Establish connection
pg_params_json = file('./pg_params.json','r')
pg_params = json.load(pg_params_json)
pg_params_json.close()
pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s port=%(port)s' % pg_params  
con, cur = make_con(pg_conn_string)

# Clear the output table
cur.execute("DELETE FROM diffusion_solar.hourly_resource_by_time_slice")
con.commit()

# List of distinct solar_re_9809_gid ids
ids = list(pd.read_sql("SELECT DISTINCT solar_re_9809_gid FROM diffusion_solar.solar_resource_hourly",con, coerce_float = False).solar_re_9809_gid)
lkup = pd.read_csv('reeds_ts_lkup.csv')
r = list(lkup.reeds_time.values)

# Fields in the SQL ouput tabl
fields = ['solar_re_9809_gid','tilt','azimuth',"H01","H02","H03","H04","H05","H06","H07","H08","H09","H10","H11","H12","H13","H14","H15","H16"]
t0 = time.time()

# Loop over each unique gid
for gid in ids:
    
    # Pull resource data corresponding to gid
    cur.execute("SELECT * FROM diffusion_solar.solar_resource_hourly WHERE solar_re_9809_gid = %i" %gid)
    chunk = cur.fetchall()

    # Loop over each row in the chunk (corresonds to multiple tilt/azimuth combinations)
    for row in chunk:
        
        l = [row['solar_re_9809_gid'], row['tilt'],row['azimuth']]
        
        # Determine the average CF for each time slice
        for t in ["H01","H02","H03","H04","H05","H06","H07","H08","H09","H10","H11","H12","H13","H14","H15","H16"]:
            
            # Pulls out the generation corresponding the timeslice, then find the mean generation in sample
            prod = [i for i,j in zip(row['cf'],r) if j == t]
            val = float(row['derate']) * sum(prod)/len(prod)/1e6
            l.append(val)
        p = dict(zip(fields,l))
        
        # Commit the output to the table
        cur.execute('''INSERT INTO diffusion_solar.hourly_resource_by_time_slice(solar_re_9809_gid,
            tilt, azimuth, H01, H02, H03, H04, H05, H06, H07, H08, H09, H10, H11, H12, H13, H14, H15, H16)
            VALUES (%(solar_re_9809_gid)s,
            %(tilt)s, %(azimuth)s, %(H01)s, %(H02)s, %(H03)s, %(H04)s, %(H05)s, %(H06)s, 
            %(H07)s, %(H08)s, %(H09)s, %(H10)s, %(H11)s, %(H12)s, 
            %(H13)s, %(H14)s, %(H15)s, %(H16)s)''', p)
        con.commit()
    if gid % 20 == 0:
        print 'Finished Chunk %s Total time %s' %(gid, time.time()-t0)
        
print 'I finished everything in %s' %(time.time() - t0)        