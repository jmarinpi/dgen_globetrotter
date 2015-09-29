# -*- coding: utf-8 -*-
"""
Created on Mon Dec 29 11:58:00 2014

@author: mgleason
"""

import psycopg2 as pg
import psycopg2.extras as pgx
import numpy as np
import pandas as pd
import time

pg_params = {'host': 'gispgdb',
             'dbname': 'dav-gis',
             'user': 'mgleason',
             'password': 'mgleason'}
pg_conn_string = 'host=%(host)s dbname=%(dbname)s user=%(user)s password=%(password)s' % pg_params

con = pg.connect(pg_conn_string)
cur = con.cursor(cursor_factory=pgx.RealDictCursor)

a = np.random.random_integers(0, 10000, 8760).tolist()

sql = """DROP TABLE IF EXISTS mgleason.test_byte;
         CREATE TABLE mgleason.test_byte
( 
    arr bytea
);"""
cur.execute(sql)
con.commit()

for i in range(0,10):
    sql = "INSERT INTO mgleason.test_byte VALUES (%s) " % pg.Binary(str(a).encode("zlib"))
#    sql = """INSERT INTO mgleason.test_byte VALUES ('{"data": %s}') """ % str(a)
#    sql = """INSERT INTO mgleason.test_byte VALUES ('%s') """ % str(a)
    cur.execute(sql)
    con.commit()

sql = """DROP TABLE IF EXISTS mgleason.test_arr;
         CREATE TABLE mgleason.test_arr
( 
    arr integer[]
);"""
cur.execute(sql)
con.commit()

for i in range(0,10):
    sql = "INSERT INTO mgleason.test_arr VALUES ('{%s}') " % str(a)[1:-1]
    cur.execute(sql)
    con.commit()

t0 = time.time()
sql1 = 'SELECT * FROM mgleason.test_byte;'
cur.execute(sql1)
rows_byte = cur.fetchall()
print time.time() - t0


t0 = time.time()
sql2 = 'SELECT * FROM mgleason.test_arr;'
cur.execute(sql2)
rows_arr = cur.fetchall()
print time.time() - t0

t0 = time.time()
d = pd.read_sql(sql1, con)
print time.time() - t0

t0 = time.time()
d = pd.read_sql(sql2, con)
print time.time() - t0


t0 = time.time()
cur.execute(sql1)
d2 = pd.DataFrame.from_dict(cur.fetchall())
print time.time() - t0


t0 = time.time()
cur.execute(sql2)
d2 = pd.DataFrame.from_dict(cur.fetchall())
print time.time() - t0