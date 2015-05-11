# -*- coding: utf-8 -*-
"""
Created on Mon Mar 03 09:53:27 2014

@author: mgleason
"""

import h5py
import psycopg2 as pg
import numpy as np
import pgdbUtil
import hdfaccess
import glob
import os

# connect to pg
conn, cur = pgdbUtil.pgdbConnect(True)
schema = 'diffusion_wind'

#in_path = '/home/mgleason/data/dg_wind/aws_2014_wind_generation_update/outputs'
in_path = '/Users/mgleason/gispgdb_home/mgleason/data/dg_wind/aws_2014_wind_generation_update/outputs'
hdfs = glob.glob1(in_path, '*.hdf5')

for hdf in hdfs:
    # split the turbine name
    filename_parts = hdf.split('_')
    turbine_start_i = filename_parts.index('dwind')+1
    turbine_end_i = filename_parts.index('2014')
    turbine = '_'.join(filename_parts[turbine_start_i:turbine_end_i])
    
    # create the output table
    out_table = '%s.wind_resource_%s_turbine' % (schema, turbine)
    print out_table
    
    # create the table
    sql = 'DROP TABLE IF EXISTS %s;' % out_table
    cur.execute(sql)
    conn.commit()
    
    sql = 'CREATE TABLE %s (\
            i integer,\
            j integer,\
            cf_bin integer,\
            height integer,\
            aep numeric,\
            cf_avg numeric\
            );' % out_table
    cur.execute(sql)
    conn.commit()
    
    print 'Loading %s to %s' % (hdf, out_table)

    hf = h5py.File(os.path.join(in_path, hdf),'r')
    
    cf_bins = [k for k in hf.keys() if 'cfbin' in k]
    
    ijs = np.array(hf['meta'])

    for cf_bin in cf_bins:
        print 'Working on cf_bin = %s' % cf_bin
        heights = hf[cf_bin].keys()
        for height in heights:
            print '\tWorking on height = %s' % height
            aep_path = '%s/%s/%s' % (cf_bin,height,'aep')
            aep = hdfaccess.getFilteredData(hf,aep_path)
            
            cf_avg_path = '%s/%s/%s' % (cf_bin,height,'cf_avg')
            cf_avg = hdfaccess.getFilteredData(hf,cf_avg_path)
    
            out_array = np.recarray((np.invert(aep.mask).sum(),), dtype = [('i', '<i4'), ('j', '<i4'), ('height','<i4'), ('cf_bin','<i4'), ('aep','f4'), ('cf_avg','f4')])
            out_array['aep'] = aep.data[np.invert(aep.mask)]   
            out_array['cf_avg'] = cf_avg.data[np.invert(cf_avg.mask)]
            ijs_data = ijs[np.invert(aep.mask)]
            out_array ['i'] = ijs_data['i']
            out_array ['j'] = ijs_data['j']
            out_array['height'] = height
            out_array['cf_bin'] = int(cf_bin.split('_')[0])/10
            
            for row in out_array:
                sql = 'INSERT INTO %s (i, j, height, cf_bin, aep, cf_avg) VALUES %s;' % (out_table, row)
                cur.execute(sql)
            conn.commit()
    hf.close()

        