# -*- coding: utf-8 -*-
"""
Created on Mon Mar 03 09:53:27 2014

@author: mgleason
"""

import h5py
import psycopg2 as pg
import numpy as np
import glob
import os
from cStringIO import StringIO
import pandas as pd


# define a dictionary for converting azimuth to a nominal direction
orientations = {180: 'S',
                135: 'SE',
                225: 'SW',
                90: 'E',
                270: 'W'
                }
                
# connect to postgres
pg_conn_string = 'host=gispgdb dbname=dav-gis user=mgleason password=mgleason'
conn = pg.connect(pg_conn_string)
cur = conn.cursor()

sql = 'SET ROLE "diffusion-writers";'
cur.execute(sql)
conn.commit()  

hf_path = '/home/mgleason/data/dg_solar/cf'
#hf_path = '/Users/mgleason/gispgdb/data/dg_solar/cf'
hdfs = [os.path.join(hf_path,f) for f in glob.glob1(hf_path,'excess_generation_factors_*.h5')]

out_table = 'diffusion_solar_data.excess_generation_factors'

# create the table
sql = 'DROP TABLE IF EXISTS %s;' % out_table
cur.execute(sql)
conn.commit()


sql = '''CREATE TABLE %s 
        (
            solar_re_9809_gid integer,
            tilt numeric,
            azimuth character varying(2),
            excess_gen_factor numeric
        );''' % out_table
cur.execute(sql)
conn.commit()

for hdf in hdfs:
  
    hf = h5py.File(hdf,'r')
    # get the gids
    gids = np.array(hf['index'])
    
    # get the tilt and azimuth
    tilt = hf['excess_gen_factors'].attrs['tilt']
    if tilt == -1:
        print 'Warning: Tilt was set to tilt at latitude'
#        sys.exit(-1)
    azimuth = orientations[hf['excess_gen_factors'].attrs['azimuth']]
    
    excess_gen = hf['excess_gen_factors']*hf['excess_gen_factors'].attrs['scale_factor']

    # create output dataframe
    df = pd.DataFrame(data={'solar_re_9809_gid' : gids.reshape(gids.shape[0],),
                                  'tilt' : tilt,
                                  'azimuth' : azimuth,
                                  'excess_gen_factor' : excess_gen.reshape(excess_gen.shape[0],)
                                  },
                   index = np.arange(0,np.shape(gids)[0]))
    # dump to an in memory csv   
    # open an in memory stringIO file (like an in memory csv)
    print 'Writing to postgres'
    s = StringIO()
    # write the data to the stringIO
    columns = ['solar_re_9809_gid','tilt','azimuth','excess_gen_factor']
    df[columns].to_csv(s, index = False, header = False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    cur.copy_expert('COPY %s FROM STDOUT WITH CSV' % out_table, s)
    # commit the additions and close the stringio file (clears memory)
    conn.commit()    
    s.close()    

    hf.close()
