#-------------------------------------------------------------------------------
# Name:        DG_Wind_NamedRange_xl2pg
# Purpose:
#
# Author:      ngrue
#
# Created:     16/01/2014
# Copyright:   (c) ngrue 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------


import openpyxl as xl, os, psycopg2 as pg
from cStringIO import StringIO
from config import pg_conn_string
import load_excel_shared_functions as lex
from load_excel_shared_functions import ExcelError, list2line


def main(wb, conn, mode = None, ReEDS_PV_CC = None, verbose = False):
    try:
        # check connection to PG
        if not conn:
            close_conn = True
            conn = lex.makeConn(pg_conn_string)
        else:
            iso_level = conn.isolation_level
            conn.set_isolation_level(pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            close_conn = False
        # make cursor from conn
        cur = conn.cursor()
        
        if os.path.exists(wb) == False:
            raise ExcelError('The specified input worksheet (%s) does not exist' % wb)
        
        # load the data
        schema = 'diffusion_wind'
        # note: to prevent this next line from printing all sorts of junk to the screen,
        # I had to disable a line in C:\Python27\lib\site-packages\openpyxl\namedrange.py
        # line #86 in refers_to_range: print range_string, bool(NAMED_RANGE_RE.match(range_string)) 
        # hopefully this is fixed in more up-to-date version of openpyxl
        curWb = xl.load_workbook(wb, data_only = True)

        table = 'scenario_options'
        inpOpts(curWb,schema,table,conn,cur,verbose)
        table = 'wind_cost_projections'
        windCost(curWb,schema,table,conn,cur,verbose)
        table = 'wind_performance_improvements'
        windPerf(curWb,schema,table,conn,cur,verbose)
        table = 'wind_generation_derate_factors'
        windDerate(curWb,schema,table,conn,cur,verbose)
        table = 'system_sizing_factors'
        systemSizing(curWb,schema,table,conn,cur,verbose)
        table = 'min_acres_per_hu_lkup'
        sitingParcelSize(curWb,schema,table,conn,cur,verbose)
        table = 'max_hi_dev_pct_lkup'
        sitingHiDev(curWb,schema,table,conn,cur,verbose)
        table = 'required_canopy_clearance_lkup'        
        sitingCanopyClearance(curWb,schema,table,conn,cur,verbose)

        for func in lex.shared_table_functions:
            func(curWb,schema,conn,cur,verbose)


        if close_conn:
            conn.close()
        else:
            conn.set_isolation_level(iso_level)
        
        if verbose:
            print "Process completed successfully"
            
        return 0
    
    except ExcelError, e:
        raise ExcelError(e)
    

def windCost(curWb,schema,table,conn,cur,verbose=False):
    sizes = [['2.5'],['5'],['10'],['20'],['50'],['100'],['250'],['500'],['750'],['1000'],['1500']]
    f = StringIO()
    for item in sizes:
        rname = 'Wind_Cost_Projections_' + item[0] + '_kw'
        named_range = curWb.get_named_range(rname)
        if named_range == None:
            raise ExcelError('%s named range does not exist.' % rname)
        cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
        columns = len(cells[0])
        rows = len(cells)
        c = 0
        while c < columns:
            r = 0
            l = []
            while r < rows:
                if cells[r][c].value == None:
                    val = '0'
                else:
                    val = cells[r][c].value
                l += [val]
                r += 1
            f.write(str(l)[1:-1] + ',' + item[0] + '\n')
            #print l
            c += 1
    f.seek(0)
    if verbose:
        print 'Exporting wind_cost_projections'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def windPerf(curWb,schema,table,conn,cur,verbose=False):
    f = StringIO()
    named_range = curWb.get_named_range('Wind_Performance_Improvements')
    if named_range == None:
        raise ExcelError('Wind_Performance_Improvements named range does not exist')
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 0
    while c < columns:
        if c == 0 or c == 1:
            c += 1
            continue
        r = 0
        l = []
        while r < rows:
            if r == 0:
                r += 1
            if cells[r][c].value == None:
                val = '0'
            else:
                val = cells[r][c].value
            l = [val]
            year = [cells[0][c].value]
            watt = cells[r][0].value
            if 'MW' in str(watt):
                watt_flt = [float(watt.replace('MW','').strip()) * 1000]
            else:
                watt_flt = [float(watt.replace('kW','').strip())]
            in_l = year + watt_flt + l
            #print str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n'
            f.write(str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n')
            r += 1
        c += 1
    f.seek(0)
    if verbose:
        print 'Exporting wind_performance_improvements'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()

def windDerate(curWb,schema,table,conn,cur,verbose=False):
    f = StringIO()
    named_range = curWb.get_named_range('Wind_Derate_Factors')
    if named_range == None:
        raise ExcelError('Wind_Derate_Factors named range does not exist')
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    # loop through values in the first column
    sizes = [cells[r][0].value.lower().replace('kw','').strip() for r in range(1,rows)]
    years = [cells[0][c].value for c in range(1, columns)]
    for r, size in enumerate(sizes):
        for c, year in enumerate(years):
            if cells[r+1][c+1].value == None:
                val = '0'
            else:
                val = cells[r+1][c+1].value
            l = [size, year, val]
            f.write(str(l).replace("u'","").replace("'","")[1:-1]+'\n')
    f.seek(0)
    if verbose:
        print 'Exporting wind_generation_derate_factors'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def systemSizing(curWb,schema,table,conn,cur,verbose=False):
    
    
    sectors = {'residential': 'res', 'commercial': 'com', 'industrial': 'ind'}    
    f = StringIO()
    rname = 'sys_sizing'
    named_range = curWb.get_named_range(rname)
    if named_range == None:
        raise ExcelError('%s named range does not exist' % rname)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    rows = len(cells)
    for row in range(0, rows):
        sector = sectors[cells[row][0].value.lower()]
        sys_size_target_nem = cells[row][1].value
        oversize_limit_nem = cells[row][2].value
        sys_size_target_no_nem = cells[row][3].value
        oversize_limit_no_nem = cells[row][4].value        
        l = [sector, sys_size_target_nem, oversize_limit_nem, sys_size_target_no_nem, oversize_limit_no_nem]
        f.write(list2line(l))
        
    f.seek(0)
    if verbose:
        print 'Exporting system_sizing_factors'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()
    

def sitingParcelSize(curWb, schema, table, conn, cur, verbose = False):
    
    f = StringIO()
    rname = 'apply_parcel_size'
    named_range = curWb.get_named_range(rname)
    if named_range == None:
        raise ExcelError('%s named range does not exist' % rname)
    cell = named_range.destinations[0][0].range(named_range.destinations[0][1])
    enabled = cell.value      
    
    rname = 'parcel_size'
    named_range = curWb.get_named_range(rname)
    if named_range == None:
        raise ExcelError('%s named range does not exist' % rname)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    rows = len(cells)
    # loop through values in the first column
    heights = [cells[r][0].value.lower().replace('m','').strip() for r in range(0,rows)]
    for r, height in enumerate(heights):
        if enabled:
            # get the values specified in the sheet
            if cells[r][1].value == None:
                val = '0'
            else:
                val = cells[r][1].value
        else:
            val = 0 # allow turbines to build even when there are zero acres per hu (condition in model is acres_per_hu >= b.min_acres_per_hu)

        l = [height, val]
        f.write(str(l).replace("u'","").replace("'","")[1:-1]+'\n')
    f.seek(0)
    if verbose:
        print 'Exporting %s' % table
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()
    
    
def sitingHiDev(curWb, schema, table, conn, cur, verbose = False):
    
    f = StringIO()
    rname = 'apply_pct_hi_dev'
    named_range = curWb.get_named_range(rname)
    if named_range == None:
        raise ExcelError('%s named range does not exist' % rname)
    cell = named_range.destinations[0][0].range(named_range.destinations[0][1])
    enabled = cell.value    
        
    rname = 'pct_hi_dev'
    named_range = curWb.get_named_range(rname)
    if named_range == None:
        raise ExcelError('%s named range does not exist' % rname)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    rows = len(cells)
    # loop through values in the first column
    heights = [cells[r][0].value.lower().replace('m','').strip() for r in range(0,rows)]
    for r, height in enumerate(heights):
        if enabled:
            # get the values specified in the sheet
            if cells[r][1].value == None:
                val = '0'
            else:
                val = int(cells[r][1].value*100)
        else:
            val = 100 # allow all turbines to be built even where there is 100% highly developed land (condition in model is a.hi_dev_pct <= b.max_hi_dev_pct)
        l = [height, val]
        f.write(str(l).replace("u'","").replace("'","")[1:-1]+'\n')
    f.seek(0)
    if verbose:
        print 'Exporting %s' % table
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def sitingCanopyClearance(curWb, schema, table, conn, cur, verbose = False):
    
    f = StringIO()
    # check whether the flag is set to apply this
    rname = 'apply_canopy_clearance'
    named_range = curWb.get_named_range(rname)
    if named_range == None:
        raise ExcelError('%s named range does not exist' % rname)
    cell = named_range.destinations[0][0].range(named_range.destinations[0][1])
    enabled = cell.value
    
    rname = 'canopy_clearance'
    named_range = curWb.get_named_range(rname)
    if named_range == None:
        raise ExcelError('%s named range does not exist' % rname)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    rows = len(cells)
    # loop through values in the first column
    sizes = [cells[r][0].value.lower().replace('kw','').strip() for r in range(0,rows)]
    for r, size in enumerate(sizes):
        if enabled:
            # get the values specified in the sheet
            if cells[r][2].value == None:
                val = '0'
            else:
                val = cells[r][2].value
        else:
            val = -100 # this works because the siting restriction is turbine height >= (canopy_height_m + canopy_clearance_m) (highest canopy_height_m in the pt grids is ~50 m)
        l = [size, val]
        f.write(str(l).replace("u'","").replace("'","")[1:-1]+'\n')
    f.seek(0)
    if verbose:
        print 'Exporting %s' % table
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()
    

def inpOpts(curWb,schema,table,conn,cur,verbose=False):
    global sc_name
    f = StringIO()

    input_named_range = 'Input_Scenario_Name'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        raise ExcelError('Input_Scenario_Name named range does not exist')
    sc_name = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

    input_named_range = 'Annual_Inflation'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        raise ExcelError('Annual_Inflation named range does not exist')
    ann_inf = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

    input_named_range = 'overwrite_exist_inc'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        raise ExcelError('overwrite_exist_inc named range does not exist')
    overwrite_exist_inc = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

    input_named_range = 'incent_start_year'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        raise ExcelError('incent_start_year named range does not exist')
    incent_startyear = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

    input_named_range = 'apply_parcel_size'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        raise ExcelError('apply_parcel_size named range does not exist')
    siting_parcel_size_enabled = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

    input_named_range = 'apply_pct_hi_dev'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        raise ExcelError('apply_pct_hi_dev named range does not exist')
    siting_hi_dev_enabled = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

    input_named_range = 'apply_canopy_clearance'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        raise ExcelError('apply_canopy_clearance named range does not exist')
    siting_canopy_clearance_enabled = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

    named_range = curWb.get_named_range('Input_Scenario_Options')
    if named_range == None:
        raise ExcelError('Input_Scenario_Options named range does not exist')
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 0
    while c < columns:
        r = 0
        l = []
        while r < rows:
            if cells[r][c].value == None:
                val = '0'
            else:
                val = cells[r][c].value
            l += [val]
            r += 1
        c += 1
    named_range = curWb.get_named_range('Incentives_Utility_Type')
    if named_range == None:
        raise ExcelError('Incentives_Utility_Type named range does not exist')
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 0
    while c < columns:
        r = 0
        incent_utility = []
        while r < rows:
            if cells[r][c].value == None:
                val = '0'
            else:
                val = cells[r][c].value
            incent_utility += [val]
            r += 1
        c += 1

    in_l = l + ann_inf + sc_name + overwrite_exist_inc + incent_startyear + incent_utility + siting_parcel_size_enabled + siting_hi_dev_enabled + siting_canopy_clearance_enabled
    f.write(str(in_l).replace(" u'","").replace("u'","").replace("'","")[1:-1])
    #print str(in_l).replace(" u'","").replace("u'","").replace("'","")[1:-1]
    f.seek(0)
    if verbose:
        print 'Exporting scenario_options'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_expert('''
        COPY %s.%s 
        (
            region, 
            end_year, 
            markets, 
            load_growth_scenario, 
            res_rate_structure, 
            res_rate_escalation, 
            res_max_market_curve, 
            com_rate_structure, 
            com_rate_escalation, 
            com_max_market_curve, 
            ind_rate_structure, 
            ind_rate_escalation, 
            ind_max_market_curve, 
            carbon_price, 
            random_generator_seed,
            ann_inflation, 
            scenario_name, 
            overwrite_exist_inc, 
            incentive_start_year, 
            utility_type_iou, 
            utility_type_muni, 
            utility_type_coop, 
            utility_type_allother,
            siting_parcel_size_enabled,
            siting_hi_dev_enabled,
            siting_canopy_clearance_enabled
        ) 
        FROM STDOUT WITH CSV;''' % (schema,table), f)        
#    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


if __name__ == '__main__':
    input_xls = '../excel/scenario_inputs_wind.xlsm'
    main(input_xls, conn = None, verbose = True)

