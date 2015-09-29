# -*- coding: utf-8 -*-
"""
Created on Fri May 30 13:45:33 2014

@author: mgleason
"""

import h5py
import psycopg2 as pg
import numpy as np
import os
import pandas as pd
import pandas.io.sql as sqlio
import glob
import sys

def meta_to_pandas(dataset):
    
    return pd.DataFrame.from_records(np.array(dataset).reshape(dataset.shape[0],))
    

# define a dictionary for converting azimuth to a nominal direction
orientations = {180: 'S',
                135: 'SE',
                225: 'SW',
                90: 'E',
                270: 'W'
                }

# set the basepath
base_path = '/home/mgleason/data/dg_solar'
#base_path = '/Users/mgleason/gispgdb/data/dg_solar'

# connect to postgres
pg_conn_string = 'host=gispgdb dbname=dav-gis user=mgleason password=mgleason'
conn = pg.connect(pg_conn_string)
cur = conn.cursor()

# get lookup table that maps from i,j to transmission zone id
sql = '''SELECT solar_re_9809_gid, transmission_zone_id as tzone_id
        FROM diffusion_solar.solar_re_9809_tzone_lookup;'''
gid_tzone_lkup = sqlio.read_frame(sql,conn)

# open the hourly load data
load_path = os.path.join(base_path,'ventyx_hourly_load_by_tzone.h5')
load = h5py.File(load_path,'r')
# get the metadata for the load
load_meta = meta_to_pandas(load['meta'])
# get the actual load data from the hdf, accounting for scale factor
ci_norm_all = np.array(load['normalized_hourly_load']*load['normalized_hourly_load'].attrs['scale_factor'])
# close the load data
load.close()

# set path to the generation data
generation_path = os.path.join(base_path,'cf')
generation_hdfs = [os.path.join(generation_path,f) for f in glob.glob1(generation_path, '*.h5')]

# loop through output generation files, by turbine
for hdf_path in generation_hdfs:    
    
    print 'Working on: %s' % hdf_path
    # open the generation data
    generation = h5py.File(hdf_path,'r')
    
    # get the tilt and azimuth info
    tilt = int(generation['cf'].attrs['tilt'])
    if tilt == -1:
        print 'Warning: Tilt was set to tilt at latitude'
#        sys.exit(-1)
    azimuth = orientations[generation['cf'].attrs['azimuth']]

    # create output hdf to hold excess generation factor data
    out_hdf_filepath = os.path.join(generation_path,'excess_generation_factors_%sdeg_%s.h5' % (tilt,azimuth))
    out_hdf = h5py.File(out_hdf_filepath, 'w')

    # get the gids from the generation file
    gids = np.array(generation['index'])
    # add this to a pandas frame
    generation_meta = pd.DataFrame(data={'solar_re_9809_gid' : gids.reshape(gids.shape[0],)},
                   index = np.arange(0,np.shape(gids)[0]))
    
    # copy the index dataset from the generation file to the output file
    gen_index = np.array(generation['index'])
    out_hdf.create_dataset('index', shape = gen_index.shape, dtype = gen_index.dtype, data = gen_index) 

    # combine with the ij_tzone_lkup and load metadata using left joins to establish a full lookup from each 
    # row in the generation hdf to the correct row in the load hdf
    meta_combined = pd.merge(generation_meta,pd.merge(gid_tzone_lkup, load_meta, 'left', on = 'tzone_id', sort = False), 'left', on = 'solar_re_9809_gid', sort = False)
    # make sure the solar gid order is maintained
    if not np.all(meta_combined.solar_re_9809_gid == generation_meta.solar_re_9809_gid):
        print 'Warning: gid order will not be maintained in output!'
        sys.exit(-1)
    meta_combined.columns = ['solar_re_9809_gid', 'tzone_id', u'load_hdf_index', u'tzone_name']

    # mutate the ci data so that it matches the order and size of the generation data
    ci_norm = ci_norm_all[list(meta_combined['load_hdf_index'])]
        
    # get the generation data 
    # load the data, accounting for the fill value and scale factor
    gi = np.array(generation['cf'])
    # normalize the data over the row-wise sums
    gi_norm = gi/gi.sum(1).reshape(gi.shape[0],1)
    # clear gi from memory
    del gi
    # difference the data from the normalized consumption data
    diff = gi_norm-ci_norm
    # clear gi_norm from memory
    del gi_norm
    # limit to only hours where generation exceeds consumption
    excess_only = (diff > 0) * diff
    # clear diff from memory
    del diff
    # sum row wise to get the annual excess generation factor (would also need to divide by the row wise sums if they weren't already 1)
    excess_gen_factors = excess_only.sum(1)   
    # clear excess_only from memory
    del excess_only
    
    # write the data out
    # write to hdf, converting scale and setting fill            
    out_hdf.create_dataset('excess_gen_factors', dtype = 'int32', data = (excess_gen_factors*1000).astype('int32').reshape(excess_gen_factors.shape[0],1))
    # set attributes on the output dataset   
#    out_hdf['excess_gen_factors'].attrs['tilt'] = float(tilt)
#    out_hdf['excess_gen_factors'].attrs['azimuth'] = azimuth
    for k, v in generation['cf'].attrs.iteritems():
        out_hdf['excess_gen_factors'].attrs[k] = v
    out_hdf['excess_gen_factors'].attrs['scale_factor'] = 0.001


    # clear the ci_norm data from memory
    del ci_norm
    # close the output file
    out_hdf.close()
    # close the input file
    generation.close()

