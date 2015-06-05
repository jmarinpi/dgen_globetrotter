# -*- coding: utf-8 -*-
"""
Created on Mon Mar 03 09:53:27 2014

@author: mgleason
"""

import h5py
import psycopg2 as pg
import psycopg2.extras as pgx
import numpy as np
# import pgdbUtil
# import hdfaccess
import glob
import os
import pandas as pd
from cStringIO import StringIO

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
hdf_path = '/home/mgleason/data/dg_wind/aws_2014_wind_generation_update/outputs'
         
 
# CONNECT TO POSTGRES
con, cur = pg_connect(pg_params)

schema = 'diffusion_wind'
hdfs = glob.glob1(hdf_path, '*.hdf5')
scale_offset = 1e3
for hdf in hdfs:
    # split the turbine name
    filename_parts = hdf.split('_')
    turbine_start_i = filename_parts.index('dwind')+1
    turbine_end_i = filename_parts.index('2014')
    turbine = '_'.join(filename_parts[turbine_start_i:turbine_end_i])
    
    # create the output table
    out_table = '%s.wind_resource_hourly_%s_turbine' % (schema, turbine)
    print out_table
    
    # create the table
    sql = 'DROP TABLE IF EXISTS %s;' % out_table
    cur.execute(sql)
    con.commit()
    
    sql = """CREATE TABLE %s (
                i integer,
                j integer,
                cf_bin integer,
                height integer,
                cf integer[]
            );""" % out_table
    cur.execute(sql)
    con.commit()
    
    sql = """COMMENT ON COLUMN %s.cf IS 'scale_offset = %s';""" % (out_table, scale_offset)
    cur.execute(sql)
    con.commit()
    
    print 'Loading %s to %s' % (hdf, out_table)

    hf = h5py.File(os.path.join(hdf_path, hdf),'r')
    
    cf_bins = [k for k in hf.keys() if 'cfbin' in k]
    
    ijs = np.array(hf['meta'])

    for cf_bin in cf_bins:
        print 'Working on cf_bin = %s' % cf_bin
        heights = hf[cf_bin].keys()
        for height in heights:
            print '\tWorking on height = %s' % height
            cf_path = '%s/%s/%s' % (cf_bin,height,'cf_hourly')
            cf = hdfaccess.getFilteredData(hf, cf_path)    
            unmasked = np.invert(cf.mask)[:,0]
            cf_list = np.trunc(cf[unmasked,:]*scale_offset).astype(int).tolist()
            ijs_data = ijs[unmasked]

            df = pd.DataFrame()
            df['i'] = ijs_data['i']
            df['j'] = ijs_data['j']
            df['cf_bin'] = int(cf_bin.split('_')[0])/10
            df['height'] = int(height)
            df['cf'] = pd.Series(cf_list).apply(lambda l: '{%s}' % str(l)[1:-1]) 
            
            # dump to a csv (can't use in memory because it is too large)
            print 'Writing to postgres'
            for i in range(0,df.shape[0]):
                sql = 'INSERT INTO %s VALUES %s;' % (out_table, tuple(df.ix[i].values))
                cur.execute(sql)
                con.commit()


    hf.close()

        