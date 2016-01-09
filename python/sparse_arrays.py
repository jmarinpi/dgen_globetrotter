# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 15:21:15 2016

@author: mgleason
"""

import pandas as pd
import numpy as np
import cPickle
import config as cfg
import utility_functions as utilfunc
import psycopg2 as pg
from cStringIO import StringIO
import time
import pickle
from scipy import sparse



def list_to_sparse(row, col, new_col, fill_value = 0, dtype = 'int'):
    
    sparse = pd.SparseArray(row[col], fill_value = fill_value, dtype = dtype)
    row[new_col] = sparse
    
    return row
    
def apply_cpickle(row, col, new_col, pg_binary = True):
    
    pkl = cPickle.dumps(row[col], protocol = 1)
    if pg_binary == True:
        bts = pg.Binary(pkl)
    else:
        bts = pkl
        
    row[new_col] = bts
    
    return row


def bytea_to_list(row, col, new_col):
    
    l = cPickle.loads(str(row[col])).to_dense()
    row[new_col] = l
    
    return row



con, cur = utilfunc.make_con(cfg.pg_conn_string)
sql = "SET ROLE mgleason;"
cur.execute(sql)
con.commit()
       
sql = """SELECT cf
       FROM diffusion_wind.wind_resource_hourly
       LIMIT 100"""
df = pd.read_sql(sql, con)

df = df.apply(list_to_sparse, axis = 1, args = ('cf', 'cfs', 0))
df = df.apply(apply_cpickle, axis = 1, args = ('cfs', 'cfb'))


sql = """DROP TABLE IF EXISTS mgleason.sparse_data;
        CREATE TABLE mgleason.sparse_data
        (
        	cf bytea
        );
        
        """
cur.execute(sql)
con.commit()



for row in df.iterrows():
    sql = "INSERT INTO mgleason.sparse_data VALUES (%s)" % row[1]['cfb']
    cur.execute(sql)
    con.commit()

# doesnt work...
#s = StringIO()
## write the data to the stringIO
#df['cfb'].to_csv(s, index = False, header = False)
## seek back to the beginning of the stringIO file
#s.seek(0)
## copy the data from the stringio file to the postgres table
#cur.copy_expert('COPY mgleason.sparse_data FROM STDOUT WITH CSV', s)
## commit the additions and close the stringio file (clears memory)
#con.commit()    
#s.close()



sql = """DROP TABLE IF EXISTS mgleason.regular_data;
        CREATE TABLE mgleason.regular_data AS
        SELECT cf
        FROM diffusion_wind.wind_resource_hourly
        LIMIT 100;"""
cur.execute(sql)
con.commit()


t0 = time.time()
sql = """SELECT cf as cf_bytes
         FROM mgleason.sparse_data;"""
dfs = pd.read_sql(sql, con)
dfs = dfs.apply(bytea_to_list, axis = 1, args = ('cf_bytes', 'cf'))
print time.time()-t0

   

t0 = time.time()
sql = """SELECT cf
         FROM mgleason.regular_data;"""
dfr = pd.read_sql(sql, con)
print time.time()-t0

#np.all(dfr['cf'] == dfs['cf'])


##import bcolz
a = dfs['cf'][20].astype('int')


pref = pickle.dumps(pd.SparseArray(a, fill_value = 0), protocol = 2)

class SparseArray():
    
    def __init__(self, a, fill_value = 0):
        
        non_fill_indices = np.where(a <> fill_value)[0]
        fill_indices = np.where(a == fill_value)[0]
        non_fill_data = a[non_fill_indices]
        
        self.data = non_fill_data.tolist()
        self.fvalue = fill_value
        self.shape = a.shape
        self.dtype = a.dtype
        
        if len(fill_indices) < len(non_fill_indices):
            self.slice = fill_indices.tolist()
            self.fill = True
        else:
            self.slice = non_fill_indices.tolist()
            self.fill = False
    
    def to_dense(self):
        
        a = np.ones(self.shape, self.dtype) * self.fvalue
        if self.fill == False:
            a[self.slice] = np.array(self.data)
        else:
            mask = np.ones(a.shape, dtype = bool)
            mask[self.slice] = False
            a[mask] = np.array(self.data)
        
        return a

s = SparseArray(a)        
p = pickle.dumps(s, protocol = 2)
len(pickle.dumps(a, protocol = 2))
len(pref)
len(p)

np.all(s.to_dense() == a)


#b = bcolz.carray(a, dtype = 'int64', cparams=bcolz.cparams(clevel = 1, shuffle = True), rootdir = '/Users/mgleason/bcolztest.bcolz')
#p = cPickle.dumps(b, protocol = 1)
#bb = cPickle.loads(p)
#
#sp = sparse.csc_matrix(a)
#p = pickle.dumps(sp, protocol = 2)
#
#sd = pd.SparseArray(a, fill_value = 0)
#p2 = pickle.dumps(sd, protocol = 2)
#
#p3 = pickle.dumps(a, protocol = 2)
#
#import blz
#z = blz.barray(a, dtype = 'int', bparams = blz.bparams(clevel = 9))
#z2 = blz.barray(sd, dtype = 'int', bparams = blz.bparams(clevel = 9))
#z2 = 
#pickle.dumps(z)

