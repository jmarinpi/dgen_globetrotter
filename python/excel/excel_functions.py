# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 10:34:55 2015

@author: mgleason
"""

import openpyxl as xl
import psycopg2 as pg
import sys
import os
from excel_objects import FancyNamedRange, ExcelError
import pandas as pd
import decorators


path = os.path.dirname(os.path.abspath(__file__))
par_path = os.path.dirname(path)
sys.path.append(par_path)
from config import pg_conn_string, show_times
import utility_functions as utilfunc

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================

@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 1, prefix = '')
def load_scenario(xls_file, schema, conn = None, test = False):
    
    logger.info('Loading Input Scenario Worksheet')    
    
    try:
        # check connection to PG
        if not conn:
            close_conn = True
            conn, cur = utilfunc.make_con(pg_conn_string)
        else:
            # make cursor from conn
            cur = conn.cursor()
            close_conn = False       
        
        if os.path.exists(xls_file) == False:
            raise ExcelError('The specified input worksheet (%s) does not exist' % xls_file)
        
        # get the table to named range lookup from csv
        mapping_file = os.path.join(path, 'table_range_lkup.csv')
        if os.path.exists(mapping_file) == False:
            raise ExcelError('The required file that maps from named ranges to postgres tables (%s) does not exist' % mapping_file)
        mappings = pd.read_csv(mapping_file)
        if test == True:
            mappings = mappings[mappings.run == True]
            
        # open the workbook                
        wb = xl.load_workbook(xls_file, data_only = True)
        
            
        for run, table, range_name, transpose, melt in mappings.itertuples(index = False):
            fnr = FancyNamedRange(wb, range_name)
            if transpose == True:
                fnr.__transpose_values__()
            elif melt == True:
                fnr.__melt__()
            fnr.to_postgres(conn, cur, schema, table)

        if close_conn:
            conn.close()          


    except ExcelError, e:
        raise ExcelError(e)            
    


if __name__ == '__main__':
    input_xls = '../../excel/scenario_inputs.xlsm'
    load_scenario(input_xls, schema = 'diffusion_results_2015_11_11_12h15m07s',  test = True)
    