# -*- coding: utf-8 -*-
"""
Created on Mon Mar 03 09:53:27 2014

@author: mgleason
"""

import h5py
import psycopg2 as pg
import numpy as np
import psycopg2.extras as pgx
import sys
import glob
import pandas as pd
from cStringIO import StringIO
import os

def pg_connect(pg_params):
    pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s' % pg_params
    con = pg.connect(pg_conn_string)
    cur = con.cursor(cursor_factory=pgx.RealDictCursor)
    
    if 'role' in pg_params.keys():
        sql = "SET ROLE '%(role)s';" % pg_params
        cur.execute(sql)
        con.commit()
    
    return con, cur


pg_params = {'host'     : 'gispgdb',
             'dbname'   : 'dav-gis',
             'user'     : 'mgleason',
             'password' : 'mgleason',
             'role'     : 'diffusion-writers'
             }
hdf_path = '/home/mgleason/data/dg_solar/cf'     
#hdf_path = '/Users/mgleason/NREL_Projects/Projects/dgen_loading_hdf_data'
 
 
# CONNECT TO POSTGRES
con, cur = pg_connect(pg_params)

scale_offset = 1e6

# define a dictionary for converting azimuth to a nominal direction
orientations = {180: 'S',
                135: 'SE',
                225: 'SW',
                90: 'E',
                270: 'W'
                }

# create the output table
print 'Creating output table'
out_table = 'diffusion_solar.solar_resource_hourly'

# create the table
sql = 'DROP TABLE IF EXISTS %s;' % out_table
cur.execute(sql)
con.commit()

sql = '''CREATE TABLE %s 
        (
            solar_re_9809_gid INTEGER,
            tilt integer,
            azimuth CHARACTER VARYING(2),  
            derate NUMERIC,
            cf integer[]
        );''' % out_table
cur.execute(sql)
con.commit()

sql = """COMMENT ON COLUMN %s.cf IS 'scale_offset = %s';""" % (out_table, scale_offset)
cur.execute(sql)
con.commit()

# get the hdfs
print 'Finding hdf files'

hdfs = [os.path.join(hdf_path,f) for f in glob.glob1(hdf_path,'mg*.h5')]
for hdf in hdfs:
    print 'Loading %s' % hdf
    # open the h5 file
    hf = h5py.File(hdf, mode = 'r')
    
    # get the gids
    gids_all = np.array(hf['index'])
    # find gid for solar_re_9809_gid = 3101 (no data due to bad/missing tmy file)
    subset = np.where(gids_all <> 3101)[0]
    gids = gids_all[subset]
    del gids_all
    
    # extract the hourly cfs
    cf = np.round(np.array(hf['cf'][subset,:])*scale_offset,0).astype(int)
    cf_list = cf.tolist()
    del cf
    del subset
    
    # get the tilt (making sure it's not tilted at latitude)
    tilt = int(hf['cf'].attrs['tilt'])
    if tilt == -1:
        print 'Warning: Tilt was set to tilt at latitude'
    
    # get the azimuth
    azimuth = orientations[hf['cf'].attrs['azimuth']]
    
    # get the derate
    derate = float(hf['cf'].attrs['derate'])

    # close the hdf file
    hf.close()
    
    # combine intp pandas dataframe
    df = pd.DataFrame()
    df['solar_re_9809_gid'] = gids.reshape(gids.shape[0],)
    df['tilt'] = tilt
    df['azimuth'] = azimuth
    df['derate'] = derate
    df['cf'] = pd.Series(cf_list).apply(lambda l: '{%s}' % str(l)[1:-1])      
    del cf_list
    del gids

    # dump to a csv (can't use in memory because it is too large)
    print 'Writing to postgres'
    for i in range(0,df.shape[0]):
        sql = 'INSERT INTO %s VALUES %s;' % (out_table, tuple(df.ix[i].values))
        cur.execute(sql)
        con.commit()

#    f = file(os.path.join(hdf_path, 'temp.csv'),'w+')
#    # write the data to the stringI
#    df.to_csv(f, index = False, header = False)
#    # seek back to the beginning of the stringIO file
#    f.seek(0)
#    # copy the data from the stringio file to the postgres table
#    cur.copy_expert('COPY %s FROM STDOUT WITH CSV' % out_table, f)
#    # commit the additions and close the stringio file (clears memory)
#    con.commit()    
#    f.close()   

    

        