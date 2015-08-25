# -*- coding: utf-8 -*-
"""
Created on Mon Mar 03 09:53:27 2014

@author: mgleason
"""

import h5py
import psycopg2 as pg
from psycopg2 import extras as pgx
import numpy as np
# import pgdbUtil
# import hdfaccess
import glob
import os

def getFilteredData(hfile,path,indices = [], mask = None):
    '''
    Return the filtered data, scale factor, fill value mask, and optionally list of indexes. If indices is not specified, data for all locations will be returned

    hfile = an HDF file object
    path = string giving the path to the dataset of interest in the hfile
    indices = optional list of indices for which to get data (indicating different cell locations)
    '''

    # find the scale factor (if it exists)
    if 'scale_factor' in hfile[path].attrs.keys():
        scale_factor =  hfile[path].attrs['scale_factor']
    else:
        scale_factor = 1 # this will have no effect when multiplied against the array

    # find the fill_value (if it exists)
    if 'fill_value' in hfile[path].attrs.keys():
        fill_value =  hfile[path].attrs['fill_value']
    elif hfile[path].fillvalue <> 0: # this is the default if a fill value isn't set
        fill_value = hfile[path].fillvalue
    else:
        fill_value = None # this will have no effect on np.ma.masked_equal()

    # get data, masking fill_value and applying scale factor
    extract = np.ma.masked_equal(hfile[path], fill_value) * scale_factor
    if indices <> []:
        # Extract the subset
        data = extract[indices]
    else:
        data = extract

    if mask is not None:
        result = np.ma.masked_array(data,mask)
    else:
        result = data

    return result

# connect to pg
# conn, cur = pgdbUtil.pgdbConnect(True)

pg_params = {'host'     : 'dnpdb001.bigde.nrel.gov',
             'dbname'   : 'diffusion_3',
             'user'     : 'jduckwor',
             'password' : 'H2v1^gFu^',
             'role'     : 'diffusion-writers',
             'port'     : 5433
             }

pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s port=%(port)s' % pg_params
conn = pg.connect(pg_conn_string)
cur = conn.cursor(cursor_factory=pgx.RealDictCursor)

if 'role' in pg_params.keys():
    sql = "SET ROLE '%(role)s';" % pg_params
    cur.execute(sql)
    conn.commit()

schema = 'diffusion_wind'
turbine_id_lookup = {'current_residential'          : 1, 
                    'current_small_commercial'      : 2, 
                    'current_mid_size'              : 3, 
                    'current_large'                 : 4, 
                    'residential_near_future'       : 5,
                    'residential_far_future'        : 6,
                    'sm_mid_lg_near_future'         : 7,
                    'sm_mid_lg_far_future'          : 8}

#in_path = '/home/mgleason/data/dg_wind/aws_2014_wind_generation_update/outputs'
# in_path = '/Users/mgleason/gispgdb_home/mgleason/data/dg_wind/aws_2014_wind_generation_update/outputs'
in_path = '/home/jduckwor/wind_curves/hdf'
hdfs = glob.glob1(in_path, '*.hdf5')

# print(hdfs)

for hdf in hdfs:
    # split the turbine name
    filename_parts = hdf.split('_')
    turbine_start_i = filename_parts.index('turbine')+2
    turbine_end_i = filename_parts.index('2015')
    turbine = '_'.join(filename_parts[turbine_start_i:turbine_end_i])
    turbine_id = turbine_id_lookup[turbine]

    # create the output table
    out_table = '%s.wind_resource_%s_turbine' % (schema, turbine)
    print out_table
    
    # NOTE: Be sure to archive the table before dropping and replacing

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
            cf_avg numeric,\
            turbine_id integer\
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
            # aep = hdfaccess.getFilteredData(hf,aep_path)
            aep = getFilteredData(hf,aep_path)
            
            cf_avg_path = '%s/%s/%s' % (cf_bin,height,'cf_avg')
            # cf_avg = hdfaccess.getFilteredData(hf,cf_avg_path)
            cf_avg = getFilteredData(hf,cf_avg_path)
            out_array = np.recarray((np.invert(aep.mask).sum(),), dtype = [('i', '<i4'), ('j', '<i4'), ('height','<i4'), ('cf_bin','<i4'), ('aep','f4'), ('cf_avg','f4'), ('turbine_id', '<i4')])
            out_array['aep'] = aep.data[np.invert(aep.mask)]
            out_array['cf_avg'] = cf_avg.data[np.invert(cf_avg.mask)]
            ijs_data = ijs[np.invert(aep.mask)]
            out_array ['i'] = ijs_data['i']
            out_array ['j'] = ijs_data['j']
            out_array['height'] = height
            out_array['cf_bin'] = int(cf_bin.split('_')[0])/10
            out_array['turbine_id'] = turbine_id
            
            for row in out_array:
                sql = 'INSERT INTO %s (i, j, height, cf_bin, aep, cf_avg, turbine_id) VALUES %s;' % (out_table, row)
                cur.execute(sql)
            conn.commit()
    hf.close()

        