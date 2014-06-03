# -*- coding: utf-8 -*-
"""
Created on Fri May 30 10:52:53 2014

@author: mgleason
"""

import h5py
import psycopg2 as pg
import numpy as np
import os
import pandas as pd
import pandas.io.sql as sqlio

# connect to postgres
pg_conn_string = 'host=gispgdb dbname=dav-gis user=mgleason password=mgleason'
conn = pg.connect(pg_conn_string)
cur = conn.cursor()

# define and open output hdf
hf_path = '/Volumes/Staff/mgleason/DG_Wind/Data/Analysis/hourly_load_by_transmission_zone/ventyx_hourly_load_by_tzone.hdf'

hf = h5py.File(hf_path,'w')


sql = '''SELECT hdf_index, zone_id as tzone_id, zone_name as tzone_name
         FROM ventyx.transmission_zones_07232013
         Order by hdf_index;'''
zones = sqlio.read_frame(sql, conn)

zone_count = zones.shape[0]

meta_shape = (zone_count,1)
meta_dtypes = [('hdf_index',int), ('tzone_id', int), ('tzone_name', 'S50')]

meta_dset = hf.create_dataset('meta', shape=meta_shape, dtype=meta_dtypes, compression = 'gzip')
# WRITE DATA
for column in zones.columns:
    meta_dset[column] = np.array(zones[column]).reshape(meta_shape)

# create dataset for hourly load data
abs_load = hf.create_dataset('hourly_load', shape = (zone_count, 8760), dtype = int, compression = 'gzip')
abs_load.attrs['unit'] = 'mw'
abs_load.attrs['description'] = 'Hourly Load by Transmission Zone (all sectors) from Ventyx'
abs_load.attrs['processing_notes'] = '''Source data were stored in local time, including DST. 
                                        These data were simplified to 8760 format. 
                                        One hour gaps were found for the first hour of 11/7/2010 in three transmission zones (615529, 1836089, 615603) due to DST.
                                        These gaps were filled by averaging the two neighboring hourly load values. '''
abs_load.attrs['source'] ='Ventyx (collected 2013.07.23)' 
abs_load.attrs['scale_factor'] = 1
abs_load.attrs['fill_value'] = -111



# create dataset for normalized hourly load data
norm_load = hf.create_dataset('normalized_hourly_load', shape = (zone_count, 8760), dtype = int, compression = 'gzip')
norm_load.attrs['unit'] = 'unitless ratios'
norm_load.attrs['description'] = 'Normalized hourly Load by Transmission Zone (all sectors) from Ventyx'
norm_load.attrs['processing_notes'] = '''These data were derived from the hourly_load dataset by normalizing the hourly load values to the total annual load for all hours of the year.
                                            Each value therefore represents the proportion of the total annual load used on that day'''
norm_load.attrs['source'] ='Ventyx (collected 2013.07.23)' 
norm_load.attrs['scale_factor'] = 1/10e7
norm_load.attrs['fill_value'] = -111


for hdf_index in range(0,zone_count):
    print 'Working on zone %s of %s' % (hdf_index+1,zone_count)
    zone_id = zones['tzone_id'][hdf_index]
    # get the data from postgres
    sql = '''SELECT load_mw
            FROM ventyx.hourly_load_8760_by_transmission_zone_20130723
            WHERE transmission_zone_id = %s
            ORDER BY hour_of_year ASC;''' % zone_id
    load_df = sqlio.read_frame(sql,conn)
    abs_load[hdf_index,:] = np.array(load_df['load_mw'])
    
    norm_load[hdf_index,:] = np.round(np.array(load_df['load_mw'], dtype = float)/np.sum(load_df['load_mw']) * 10e7,0).astype(int)    
    

hf.close()