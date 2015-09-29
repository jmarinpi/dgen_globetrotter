# -*- coding: utf-8 -*-
"""
Created on Thu Jun  5 15:23:32 2014

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
    

# set path to the generation data
generation_path = '/Volumes/Resources/GIS_Data_Catalog/NAM/Country/US/e_res/wind/awst_wind_licensed/AWS_ReEDS_wind/windpy/outputs/hdf/DG_Wind/'
#generation_path = 'D:/data/GIS_Data_Catalog/NAM/Country/US/e_res/wind/awst_wind_licensed/AWS_ReEDS_wind/windpy/outputs/hdf/DG_Wind'
generation_hdfs = dict((f[13:].split('_2014')[0], os.path.join(generation_path,f)) for f in glob.glob1(generation_path, '*.hdf5'))

turbine_name = 'DG_Wind_Current_Mid'
hdf_path = generation_hdfs[turbine_name]

cf_bin = '0240_cfbin'
height = '40'

    
# open the generation data
generation = h5py.File(hdf_path,'r')
# get the generation data for this height and cf bin
# find the fill value
fill_value = generation[cf_bin][height]['pwr_hourly'].attrs['fill_value']
# find the scale factor
scale_factor = generation[cf_bin][height]['pwr_hourly'].attrs['scale_factor']
# load the data, accounting for the fill value and scale factor
gi = np.ma.masked_equal(generation[cf_bin][height]['pwr_hourly'], fill_value) * scale_factor


out_array = gi.data[np.invert(gi.mask)[:,1] ]
generation.close()


np.savetxt('/Volumes/Staff/mgleason/DG_Wind/Data/Source_Data/Windpy/sample_of_gen_profiles/gen_hrly_profiles.csv', out_array[1:100,:], delimiter = ',')