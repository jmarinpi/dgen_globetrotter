# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 10:34:55 2015

@author: mgleason
"""

import openpyxl as xl, os, psycopg2 as pg
from cStringIO import StringIO
from config import pg_conn_string
import load_excel_shared_functions as lex
#from load_excel_shared_functions import ExcelError, list2line
from excel.excel_objects import FancyNamedRange, ExcelError
from excel import excel_functions as xlf
import numpy as np
import pandas as pd


        
def main(xls_file, schema, conn = None, mode = None, ReEDS_PV_CC = None, verbose = False):
    try:
        # check connection to PG
        if not conn:
            close_conn = True
            conn = lex.makeConn(pg_conn_string)
        else:
            iso_level = conn.isolation_level
            close_conn = False
        
        # make cursor from conn
        cur = conn.cursor()
        
        if os.path.exists(xls_file) == False:
            raise ExcelError('The specified input worksheet (%s) does not exist' % xls_file)
        
        # get the table to named range lookup from csv
        mapping_file = '/Users/mgleason/NREL_Projects/github/diffusion/python/excel/table_range_lkup.csv'
        if os.path.exists(mapping_file) == False:
            raise ExcelError('The required file that maps from named ranges to postgres tables (%s) does not exist' % mapping_file)
        mappings = pd.read_csv(mapping_file)
        
                


        # note: to prevent this next line from printing all sorts of junk to the screen,
        # I had to disable a line in C:\Python27\lib\site-packages\openpyxl\namedrange.py
        # line #86 in refers_to_range: print range_string, bool(NAMED_RANGE_RE.match(range_string)) 
        # hopefully this is fixed in more up-to-date version of openpyxl
        wb = xl.load_workbook(xls_file, data_only = True)
        

        # for testing only !!!!!!!!!!!
        mappings = mappings[mappings.run == True][['table', 'named_range', 'transpose', 'melt']]
        for table, range_name, transpose, melt in mappings.itertuples(index = False):
            global fnr
            fnr = FancyNamedRange(wb, range_name)
            if transpose == True:
                fnr.__transpose_values__()
            elif melt == True:
                fnr.__melt__()
            fnr.to_postgres(conn, cur, schema, table)
            del fnr


            
    except ExcelError, e:
        raise ExcelError(e)            

#        # create a list of the shared functions/tables that will be looped through


#        table = 'solar_cost_projections'
#        costProj(wb,schema,table,conn,cur,mode,ReEDS_PV_CC,verbose)
#        table = 'cost_multipliers'
#        costMultipliers(wb,schema,table,conn,cur,verbose)
#        table = 'learning_rates'
#        learningRates(wb,schema,table,conn,cur,verbose)
#        table = 'solar_performance_improvements'
#        perfImp(wb,schema,table,conn,cur,verbose)
#        table = 'manual_carbon_intensities'
#        manualCarbonIntensities(wb,schema,table,conn,cur,verbose)
#        table = 'system_sizing_factors'
#        systemSizing(wb,schema,table,conn,cur,verbose)
#
#        for func in lex.shared_table_functions:
#            func(wb,schema,conn,cur,verbose)
#        
#        # The solar program costs are static, so only need this to manually load the table once.
#        # Uncomment to make SPT table dynamic
#        #table = 'solar_program_target_cost_projections'
#        #sptCostProj(curWb,schema,table,conn,cur,verbose)
#
#
#        if close_conn:
#            conn.close()
#        else:
#            conn.set_isolation_level(iso_level)
#        
#        if verbose:
#            print "Process completed successfully"
#            
#        return 0
#    
#    except ExcelError, e:
#        raise ExcelError(e)
#    
#
#def costProj(curWb,schema,table,conn,cur,mode=None,ReEDS_PV_CC=None,verbose=False):
#    
#    f = StringIO()
#    sectors = ['res','com','ind']
#    for sector in sectors:
#        if sector == 'res':
#            multiplier = 1.5 # The ReEDS PV Capital Costs are for UPV, Residential prices are assumed to be 1.5 times greater than UPV
#        else:
#            multiplier = 1.25 # The ReEDS PV Capital Costs are for UPV, Industrial and Commercial prices are assumed to be 1.5 times greater than UPV
#        rname = 'costs_%s' % sector
#        named_range = curWb.get_named_range(rname)
#        if named_range == None:
#            raise ExcelError('%s named range does not exist.' % rname)
#        cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
#        columns = len(cells[0])
#        rows = len(cells)
#        c = 0
#        while c < columns:
#            r = 0
#            l = []
#            while r < rows:
#                if r == 1 and mode == 'ReEDS':
#                    # When in ReEDS mode overwrite the capital costs (row 2) with the ReEDS capital costs
#                    val = ReEDS_PV_CC.Capital_Cost[9+2*c]*multiplier
#                else:
#                    if cells[r][c].value == None:
#                        val = '0'
#                    else:
#                        val = cells[r][c].value
#                l += [val]
#                r += 1
#            in_l = l + [sector]
#            f.write(list2line(in_l))
#            #print l
#            c += 1
#    f.seek(0)
#    if verbose:
#        print 'Exporting solar_cost_projections'
#    # use "COPY" to dump the data to the staging table in PG
#    cur.execute('DELETE FROM %s.%s;' % (schema, table))
#    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
#    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
#    conn.commit()
#    f.close()
#
#def sptCostProj(curWb,schema,table,conn,cur,verbose=False):
#    
#    f = StringIO()
#    sectors = ['res','com','ind']
#    for sector in sectors:
#        rname = 'solar_program_costs_%s' % sector
#        named_range = curWb.get_named_range(rname)
#        if named_range == None:
#            raise ExcelError('%s named range does not exist.' % rname)
#        cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
#        columns = len(cells[0])
#        rows = len(cells)
#        c = 0
#        while c < columns:
#            r = 0
#            l = []
#            while r < rows:
#                if cells[r][c].value == None:
#                    val = '0'
#                else:
#                    val = cells[r][c].value
#                l += [val]
#                r += 1
#            in_l = l + [sector]
#            
#            f.write(list2line(in_l))
#            #print l
#            c += 1
#    f.seek(0)
#    #f.readline()
#    if verbose:
#        print 'Exporting solar_program_target_cost_projections'
#    # use "COPY" to dump the data to the staging table in PG
#    cur.execute('DELETE FROM %s.%s;' % (schema, table))
#    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
#    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
#    conn.commit()
#    f.close()
#
#
#def learningRates(curWb,schema,table,conn,cur,verbose=False):
#    
#    f = StringIO()
#    rname = 'learning_rates'
#    named_range = curWb.get_named_range(rname)
#    if named_range == None:
#        raise ExcelError('%s named range does not exist.' % rname)
#    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
#    columns = len(cells[0])
#    rows = len(cells)
#    c = 0
#    while c < columns:
#        r = 0
#        l = []
#        while r < rows:
#            if cells[r][c].value == None:
#                val = '0'
#            else:
#                val = cells[r][c].value
#            l += [val]
#            r += 1
#        in_l = l
#        
#        f.write(list2line(in_l))
#        #print l
#        c += 1
#    f.seek(0)
#    if verbose:
#        print 'Exporting learning_rates'
#    # use "COPY" to dump the data to the staging table in PG
#    cur.execute('DELETE FROM %s.%s;' % (schema, table))
#    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
#    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
#    conn.commit()
#    f.close()
#
#def costMultipliers(curWb,schema,table,conn,cur,verbose=False):
#    
#    f = StringIO()
#    rname = 'cost_multipliers'
#    named_range = curWb.get_named_range(rname)
#    if named_range == None:
#        raise ExcelError('%s named range does not exist.' % rname)
#    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
#    columns = len(cells[0])
#    rows = len(cells)
#    c = 0
#    while c < columns:
#        r = 0
#        l = []
#        while r < rows:
#            if cells[r][c].value == None:
#                val = '0'
#            else:
#                val = cells[r][c].value
#            l += [val]
#            r += 1
#        in_l = l
#        
#        f.write(list2line(in_l))
#        #print l
#        c += 1
#    f.seek(0)
#    if verbose:
#        print 'Exporting cost_multipliers'
#    # use "COPY" to dump the data to the staging table in PG
#    cur.execute('DELETE FROM %s.%s;' % (schema, table))
#    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
#    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
#    conn.commit()
#    f.close()
#

#def perfImp(curWb,schema,table,conn,cur,verbose=False):
#    f = StringIO()
#    rname = 'Solar_Performance_Improvements'
#    named_range = curWb.get_named_range(rname)
#    if named_range == None:
#        raise ExcelError('%s named range does not exist' % rname)
#    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
#    columns = len(cells[0])
#    rows = len(cells)
#    c = 0
#    while c < columns:
#        r = 0
#        l = []
#        while r < rows:
#            if cells[r][c].value == None:
#                val = '0'
#            else:
#                val = cells[r][c].value
#            l += [val]
#            r += 1
#        in_l = l
#        
#        f.write(list2line(in_l))
#        c += 1
#    f.seek(0)
#    if verbose:
#        print 'Exporting solar_performance_improvements'
#    # use "COPY" to dump the data to the staging table in PG
#    cur.execute('DELETE FROM %s.%s;' % (schema, table))
#    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
#    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
#    conn.commit()
#    f.close()
#
#
#def systemSizing(curWb,schema,table,conn,cur,verbose=False):
#    
#    
#    sectors = {'residential': 'res', 'commercial': 'com', 'industrial': 'ind'}    
#    f = StringIO()
#    rname = 'sys_sizing'
#    named_range = curWb.get_named_range(rname)
#    if named_range == None:
#        raise ExcelError('%s named range does not exist' % rname)
#    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
#    rows = len(cells)
#    for row in range(0, rows):
#        sector = sectors[cells[row][0].value.lower()]
#        sys_size_target_nem = cells[row][1].value
#        sys_size_target_no_nem = cells[row][2].value
#        l = [sector, sys_size_target_nem, sys_size_target_no_nem]
#        f.write(list2line(l))
#        
#    f.seek(0)
#    if verbose:
#        print 'Exporting system_sizing_factors'
#    # use "COPY" to dump the data to the staging table in PG
#    cur.execute('DELETE FROM %s.%s;' % (schema, table))
#    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
#    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
#    conn.commit()
#    f.close()
#
#
#def inpOpts(curWb,schema,table,conn,cur,verbose=False):
#    global sc_name
#    f = StringIO()
#
#    input_named_range = 'Input_Scenario_Name'
#    named_range = curWb.get_named_range(input_named_range)
#    if named_range == None:
#        raise ExcelError('Input_Scenario_Name named range does not exist')
#    sc_name = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]
#
#    input_named_range = 'Annual_Inflation'
#    named_range = curWb.get_named_range(input_named_range)
#    if named_range == None:
#        raise ExcelError('Annual_Inflation named range does not exist')
#    ann_inf = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]
#    
#    input_named_range = 'Annual_System_Degradation'
#    named_range = curWb.get_named_range(input_named_range)
#    if named_range == None:
#        raise ExcelError('Annual_System_Degradation named range does not exist')
#    ann_sys_deg = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]
#    
#    input_named_range = 'overwrite_exist_inc'
#    named_range = curWb.get_named_range(input_named_range)
#    if named_range == None:
#        raise ExcelError('overwrite_exist_inc named range does not exist')
#    overwrite_exist_inc = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]
#
#    input_named_range = 'incent_start_year'
#    named_range = curWb.get_named_range(input_named_range)
#    if named_range == None:
#        raise ExcelError('incent_start_year named range does not exist')
#    incent_startyear = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]
#
#    named_range = curWb.get_named_range('Input_Scenario_Options')
#    if named_range == None:
#        raise ExcelError('Input_Scenario_Options named range does not exist')
#    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
#    columns = len(cells[0])
#    rows = len(cells)
#    c = 0
#    while c < columns:
#        r = 0
#        l = []
#        while r < rows:
#            if cells[r][c].value == None:
#                val = '0'
#            else:
#                val = cells[r][c].value
#            l += [val]
#            r += 1
#        c += 1
#    
#    named_range = curWb.get_named_range('Incentives_Utility_Type')
#    if named_range == None:
#        raise ExcelError('Incentives_Utility_Type named range does not exist')
#    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
#    columns = len(cells[0])
#    rows = len(cells)
#    c = 0
#    while c < columns:
#        r = 0
#        incent_utility = []
#        while r < rows:
#            if cells[r][c].value == None:
#                val = '0'
#            else:
#                val = cells[r][c].value
#            incent_utility += [val]
#            r += 1
#        c += 1
#
#    in_l = l + ann_inf + ann_sys_deg+ sc_name + overwrite_exist_inc + incent_startyear + incent_utility
#    f.write(str(in_l).replace(" u'","").replace("u'","").replace("'","")[1:-1])
#    f.seek(0)
#    if verbose:
#        print 'Exporting scenario_options'
#    # use "COPY" to dump the data to the staging table in PG
#    cur.execute('DELETE FROM %s.%s;' % (schema, table))
#    sql = '''
#        COPY %s.%s 
#        (
#            region,
#            end_year,
#            markets,
#            cost_assumptions,
#            load_growth_scenario,
#            res_rate_structure,
#            res_rate_escalation,
#            res_max_market_curve,
#            com_rate_structure,
#            com_rate_escalation,
#            com_max_market_curve,
#            ind_rate_structure,
#            ind_rate_escalation,
#            ind_max_market_curve,
#            carbon_price,
#            rooftop_availability,
#            random_generator_seed,
#            ann_inflation,
#            ann_system_degradation,
#            scenario_name,
#            overwrite_exist_inc,
#            incentive_start_year,
#            utility_type_iou,
#            utility_type_muni,
#            utility_type_coop,
#            utility_type_allother
#        ) 
#        FROM STDOUT WITH CSV;''' % (schema,table)
#    cur.copy_expert(sql, f)        
#    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
#    conn.commit()
#    f.close()
    




if __name__ == '__main__':
    input_xls = '/Users/mgleason/NREL_Projects/github/diffusion/excel/scenario_inputs.xlsm'
    main(input_xls, schema = 'diffusion_template', mode = None, verbose = True)

