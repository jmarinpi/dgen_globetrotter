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


# define a dictionary for converting azimuth to a nominal direction
orientations = {180: 'S',
                135: 'SE',
                225: 'SW',
                90: 'E',
                270: 'W'
                }

# connect to pg
print 'Connecting to postgres'
pgConnString = "dbname=dav-gis user=mgleason password=mgleason host=gispgdb"
con = pg.connect(pgConnString)
cur = con.cursor(cursor_factory=pgx.DictCursor)
sql = "SET ROLE 'diffusion-writers';"
cur.execute(sql)
con.commit()


# create the output table
print 'Creating output table'
out_table = 'diffusion_solar.solar_resource_annual'

# create the table
sql = 'DROP TABLE IF EXISTS %s;' % out_table
cur.execute(sql)
con.commit()

sql = '''CREATE TABLE %s 
        (
            solar_re_9809_gid INTEGER,
            tilt NUMERIC,
            azimuth CHARACTER VARYING(2),  
            derate NUMERIC,
            naep NUMERIC,
            cf_avg NUMERIC
        );''' % out_table
cur.execute(sql)
con.commit()

# get the hdfs
print 'Finding hdf files'
#hdf_path = '/Users/mgleason/gispgdb/data/dg_solar/cf'
hdf_path = '/home/mgleason/data/dg_solar/cf'

hdfs = [os.path.join(hdf_path,f) for f in glob.glob1(hdf_path,'mg*.h5')]

for hdf in hdfs:
    print 'Loading %s' % hdf
    # open the h5 file
    hf = h5py.File(hdf, mode = 'r')
    
    # get the gids
    gids = np.array(hf['index'])
    
    # calculate the total normalized aep
    naep = np.sum(np.array(hf['cf'], dtype = np.float),1)
    cf_avg = np.mean(np.array(hf['cf'], dtype = np.float),1)
    
    # get the tilt (making sure it's not tilted at latitude)
    tilt = hf['cf'].attrs['tilt']
    if tilt == -1:
        print 'Warning: Tilt was set to tilt at latitude'
#        sys.exit(-1)
    
    # get the azimuth
    azimuth = orientations[hf['cf'].attrs['azimuth']]
    
    # get the derate
    derate = float(hf['cf'].attrs['derate'])
    
    # combine intp pandas dataframe
    df = pd.DataFrame(data={'solar_re_9809_gid' : gids.reshape(gids.shape[0],),
                       'tilt' : tilt,
                       'azimuth' : azimuth,
                       'derate' : derate,
                       'naep' : naep.reshape(naep.shape[0],),
                       'cf_avg' : cf_avg.reshape(cf_avg.shape[0],)
                       },
                       index = np.arange(0,np.shape(gids)[0]))
    
    # dump to an in memory csv   
    # open an in memory stringIO file (like an in memory csv)
    print 'Writing to postgres'
    s = StringIO()
    # write the data to the stringIO
    columns = ['solar_re_9809_gid','tilt','azimuth','derate','naep','cf_avg']
    df[columns].to_csv(s, index = False, header = False)
    # seek back to the beginning of the stringIO file
    s.seek(0)
    # copy the data from the stringio file to the postgres table
    cur.copy_expert('COPY %s FROM STDOUT WITH CSV' % out_table, s)
    # commit the additions and close the stringio file (clears memory)
    con.commit()    
    s.close()    
    
    # close the hdf file
    hf.close()
        