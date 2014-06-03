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

def meta_to_pandas(dataset):
    
    return pd.DataFrame.from_records(np.array(dataset).reshape(dataset.shape[0],))
    

# connect to postgres
pg_conn_string = 'host=gispgdb dbname=dav-gis user=mgleason password=mgleason'
conn = pg.connect(pg_conn_string)
cur = conn.cursor()

# get lookup table that maps from i,j to transmission zone id
sql = '''SELECT i, j, transmission_zone_id as tzone_id
        FROM wind_ds.ij_tzone_lookup;'''
ij_tzone_lkup = sqlio.read_frame(sql,conn)

# open the hourly load data
#load_path = '/Volumes/Staff/mgleason/DG_Wind/Data/Analysis/hourly_load_by_transmission_zone/ventyx_hourly_load_by_tzone.hdf'
load_path ='F:/data/mgleason/DG_Wind/Data/Analysis/hourly_load_by_transmission_zone/ventyx_hourly_load_by_tzone.hdf'
load = h5py.File(load_path,'r')
# get the metadata for the load
load_meta = meta_to_pandas(load['meta'])
# get the actual load data from the hdf, accounting for scale factor
ci_norm_all = np.array(load['normalized_hourly_load']*load['normalized_hourly_load'].attrs['scale_factor'])
# close the load data
load.close()

# set path to the generation data
#generation_path = '/Volumes/Resources/GIS_Data_Catalog/NAM/Country/US/e_res/wind/awst_wind_licensed/AWS_ReEDS_wind/windpy/outputs/hdf/DG_Wind/'
generation_path = 'D:/data/GIS_Data_Catalog/NAM/Country/US/e_res/wind/awst_wind_licensed/AWS_ReEDS_wind/windpy/outputs/hdf/DG_Wind'
generation_hdfs = dict((f[13:].split('_2014')[0], os.path.join(generation_path,f)) for f in glob.glob1(generation_path, '*.hdf5'))

# define the cf_bins and heights to process
cf_bins = ['%03i0_cfbin' % cf for cf in range(3,69,3)]
heights = ['30','40','50','80']

# loop through output generation files, by turbine
for turbine_name, hdf_path in generation_hdfs.iteritems():
    print 'Working on Turbine: %s' % turbine_name
    # create output hdf to hold excess generation factor data
    out_hdf_filepath = os.path.join(generation_path,'excess_generation_factors_%s.hdf5' % turbine_name)
    out_hdf = h5py.File(out_hdf_filepath, 'w')
    
    # open the generation data
    generation = h5py.File(hdf_path,'r')

    # copy the metadata from the generation file to the output file
    gen_meta_recs = np.array(generation['meta'])
    out_hdf.create_dataset('meta', shape = gen_meta_recs.shape, dtype = gen_meta_recs.dtype, data = gen_meta_recs, compression = 'gzip') 
    
    # get metadata from generation file
    generation_meta = meta_to_pandas(generation['meta'])

    # combine with the ij_tzone_lkup and load metadata using left joins to establish a full lookup from each 
    # row in the generation hdf to the correct row in the load hdf
    meta_combined = pd.merge(pd.merge(generation_meta, ij_tzone_lkup, 'left'),load_meta, 'left')
    meta_combined.columns = ['i', u'j', u'tzone_id', u'load_hdf_index', u'tzone_name']

    # mutate the ci data so that it matches the order and size of the generation data
    ci_norm = ci_norm_all[list(meta_combined['load_hdf_index'])]

    for cf_bin in cf_bins:
        print '\t Working on CF Bin: %s' % cf_bin
        # make an output group for this cfbin in the hdf file
        out_hdf.create_group(cf_bin)
        for height in heights:
            # make an output group for this height in the hdf file
            out_hdf.create_group(os.path.join(cf_bin,height))
                
            # get the generation data for this height and cf bin
            # find the fill value
            fill_value = generation[cf_bin][height]['pwr_hourly'].attrs['fill_value']
            # find the scale factor
            scale_factor = generation[cf_bin][height]['pwr_hourly'].attrs['scale_factor']
            # load the data, accounting for the fill value and scale factor
            gi = np.ma.masked_equal(generation[cf_bin][height]['pwr_hourly'], fill_value) * scale_factor
            # normalize the data over the row-wise sums
            gi_norm = gi/gi.sum(1).reshape(gi.shape[0],1)
            # clear gi from memory
            del gi
            # difference the data from the normalized consumption data
            diff = gi_norm-ci_norm
            # clear gi_norm from memory
            del gi_norm
            # limit to only hourly where generation exceeds consumption
            excess_only = (diff > 0) * diff
            # clear diff from memory
            del diff
            # sum row wise to get the annual excess generation factor (would also need to divide by the row wise sums if they weren't already 1)
            excess_gen_factors = excess_only.sum(1)   
            # clear excess_only from memory
            del excess_only
            
            # write the data out
            # set the fill value for the numpy array
            excess_gen_factors.fill_value = -111
            # write to hdf, converting scale and setting fill            
            out_hdf.create_dataset('%s/excess_gen_factors' % os.path.join(cf_bin,height), dtype = 'int32', data = (excess_gen_factors*1000).filled().astype('int32').reshape(excess_gen_factors.shape[0],1),  compression = 'gzip')
            # set attributes on the output dataset   
            out_hdf['%s/excess_gen_factors' % os.path.join(cf_bin,height)].attrs['scale_factor'] = 0.001
            out_hdf['%s/excess_gen_factors' % os.path.join(cf_bin,height)].attrs['fill_value'] = -111


    # clear the ci_norm data from memory
    del ci_norm
    # close the output file
    out_hdf.close()
    # close the input file
    generation.close()

