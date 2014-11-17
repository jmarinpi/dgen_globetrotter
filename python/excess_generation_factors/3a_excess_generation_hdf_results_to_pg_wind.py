# -*- coding: utf-8 -*-
"""
Created on Mon Mar 03 09:53:27 2014

@author: mgleason
"""

import h5py
import psycopg2 as pg
import numpy as np
import hdfaccess
import glob
import os


# connect to postgres
pg_conn_string = 'host=gispgdb dbname=dav-gis user=mgleason password=mgleason'
conn = pg.connect(pg_conn_string)
cur = conn.cursor()

sql = 'SET ROLE "diffusion-writers";'
cur.execute(sql)
conn.commit()  

hf_path = '/home/mgleason/data/dg_wind/hourly_load_by_transmission_zone/excess_generation_factors'
hdf_results = dict((s.lower().split('.')[0].replace('_dwind_','_').lower(),s) for s in glob.glob1(hf_path,'*.hdf5'))

for name, fpath in hdf_results.iteritems():
  
    
    out_table = 'diffusion_wind_data.%s_turbine' % name
    print 'Loading %s to %s' % (fpath, out_table)

    # create the table
    sql = 'DROP TABLE IF EXISTS %s;' % out_table
    cur.execute(sql)
    conn.commit()
    
    
    sql = '''CREATE TABLE %s (
            i integer,
            j integer,
            cf_bin integer,
            height integer,
            excess_gen_factor numeric
            );''' % out_table
    cur.execute(sql)
    conn.commit()

    hf = h5py.File(os.path.join(hf_path,fpath),'r')
    # get ijs from meta dataset    
    ijs = np.array(hf['meta'])
    
    # define the cf_bins and heights to process
    cf_bins = ['%03i0_cfbin' % cf for cf in range(3,78,3)]
    heights = ['20','30','40','50','80']    
    
    for cf_bin in cf_bins:
        for height in heights:
            data_path = r'%s\%s/%s' % (cf_bin,height,'excess_gen_factors')
            # get the data ()   
            fill_value = hf[data_path].fillvalue
            excess_gen = np.ma.masked_equal(hf[data_path], fill_value)

            out_array = np.recarray((np.invert(excess_gen.mask).sum(),), dtype = [('i', '<i4'), ('j', '<i4'), ('height','<i4'), ('cf_bin','<i4'), ('excess_gen_factor','f4')])
            
            out_array['excess_gen_factor'] = excess_gen.data[np.invert(excess_gen.mask)]   
            ijs_data = ijs[np.invert(excess_gen.mask)]
            out_array ['i'] = ijs[np.invert(excess_gen.mask)]['i']
            out_array ['j'] = ijs[np.invert(excess_gen.mask)]['j']
            out_array['height'] = int(height)
            out_array['cf_bin'] = int(cf_bin.split('_')[0])/10
            
            for row in out_array:
                sql = 'INSERT INTO %s (i, j, height, cf_bin, excess_gen_factor) VALUES %s;' % (out_table, row)
                cur.execute(sql)
            conn.commit()

    hf.close()
#        
#        aep_data = aep.data[np.invert(aep.mask)]
#        cf_avg_data = cf_avg.data[np.invert(cf_avg.mask)]
#        
#        ijs_data = ijs[np.invert(aep.mask)]
        
        