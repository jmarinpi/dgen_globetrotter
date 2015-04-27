# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 16:59:16 2015

@author: bsigrin
"""


import psycopg2 as pg
import json
import psycopg2.extras as pgx
import pandas as pd     
import numpy as np
import time

def make_con(connection_string, async = False):    
    con = pg.connect(connection_string, async = async)
    if async:
        wait(con)
    # create cursor object
    cur = con.cursor(cursor_factory=pgx.RealDictCursor)
    # set role (this should avoid permissions issues)
    cur.execute('SET ROLE "diffusion-writers";')    
    if async:
        wait(con)
    else:
        con.commit()
    
    return con, cur

# Establish connection
pg_params_json = file('./pg_params.json','r')
pg_params = json.load(pg_params_json)
pg_params_json.close()
pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s port=%(port)s' % pg_params  
con, cur = make_con(pg_conn_string)

#df = pd.read_sql("SELECT * FROM diffusion_solar.outputs_all LIMIT 10000;",con)


def summarise_solar_resource_by_ts_and_pca_reg(df, con):
    
    # Query the solar resource by pca, tilt, azimuth, & timeslice and rename columns e.g. at this point resource has already been averaged over solar_re_9809_gid 
    resource = pd.read_sql("SELECT * FROM diffusion_solar.solar_resource_by_pca_summary;", con)
    resource.drop('npoints', axis =1, inplace = True)
    resource['pca_reg'] = 'p' + resource.pca_reg.map(str)
    resource.columns = ['pca_reg','tilt','azimuth','H1','H2','H3','H4','H5','H6','H7','H8','H9','H10','H11','H12','H13','H14','H15','H16']

    # Determine the percentage of adopters that have selected a given azimuth/tilt combination in the pca
    d = df[['number_of_adopters','pca_reg', 'azimuth', 'tilt']].groupby(['pca_reg', 'azimuth', 'tilt']).sum()    
    d = d.groupby(level=0).apply(lambda x: x/float(x.sum())).reset_index()
    
    # Join the resource to get the capacity factor by time slice, azimuth, tilt & pca
    d = pd.merge(d, resource, how = 'left', on = ['pca_reg','azimuth','tilt'])
    
    # Pivot to tall format
    d = d.set_index(['pca_reg','azimuth','tilt','number_of_adopters']).stack().reset_index()
    d.columns = ['pca_reg','azimuth','tilt','number_of_adopters','ts','cf']
    
    # Finally, calculate weighted mean CF by timeslice using number_of_adopters as the weight 
    d['cf'] = d['cf'] * d['number_of_adopters']
    d = d.groupby(['pca_reg','ts']).sum().reset_index()
    d.drop(['tilt','number_of_adopters'], axis = 1, inplace = True)
    
    return d


