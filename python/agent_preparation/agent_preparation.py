# -*- coding: utf-8 -*-
"""
Created on Thu May 26 11:29:02 2016

@author: mgleason
"""
import psycopg2 as pg
import numpy as np
import pandas as pd
import decorators
import utility_functions as utilfunc
import multiprocessing
import traceback
import data_functions as datfunc
from agent import Agent, Agents, AgentsAlgorithm
from cStringIO import StringIO
import pssc_mp

# %% GLOBAL SETTINGS

# load logger
logger = utilfunc.get_logger()

# configure psycopg2 to treat numeric values as floats (improves
# performance of pulling data from the database)
DEC2FLOAT = pg.extensions.new_type(
    pg.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
pg.extensions.register_type(DEC2FLOAT)


# %%
def p_execute(pg_conn_string, sql):
    try:
        # create cursor and connection
        con, cur = utilfunc.make_con(pg_conn_string)
        # execute query
        cur.execute(sql)
        # commit changes
        con.commit()
        # close cursor and connection
        con.close()
        cur.close()

        return (0, None)

    except Exception, e:
        return (1, e.__str__())


# %%
def p_run(pg_conn_string, sql, county_chunks, pool):

    num_workers = pool._processes
    result_list = []
    for i in xrange(num_workers):

        place_holders = {'i': i,
                         'county_ids': utilfunc.pylist_2_pglist(county_chunks[i])}
        isql = sql % place_holders

        res = pool.apply_async(p_execute, args=(pg_conn_string, isql))
        result_list.append(res)

    # get results as they are returned
    result_returns = []
    for i, result in enumerate(result_list):
        result_return = result.get()
        result_returns.append(result_return)

    results_df = pd.DataFrame(result_returns, columns=['status_code', 'msg'])
    # find whether there are any errors
    errors_df = results_df[results_df['status_code'] == 1]
    if errors_df.shape[0] > 0:
        # errors = '\n\n'.join(errors_df['msg']) # if you'd rather print all
        # messages, but usually they will be redundant
        first_error = errors_df['msg'].tolist()[0]
        pool.close()
        raise Exception(
            'One or more SQL errors occurred.\n\nFirst error was:\n\n%s' % first_error)
    else:
        return
