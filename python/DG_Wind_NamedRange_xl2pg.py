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


import openpyxl as xl, traceback, os, glob, sys, psycopg2 as pg,logging, argparse, csv
import StringIO
from config import excelAlpha

def makeConn(host='gispgdb',dbname='dav-gis', user='ngrue', password='ngrue', autocommit=True):
    conn = pg.connect('host=%s dbname=%s user=%s password=%s' % (host, dbname, user, password))
    if autocommit:
        conn.set_isolation_level(pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

def main(wb_loc, conn):
    try:
        # check connection to PG
        if not conn:
            close_conn = True
            conn = makeConn()
        else:
            close_conn = False
        # make cursor from conn
        cur = conn.cursor()

        wbs = [g for g in glob.glob('*.xlsm') if not g.startswith("~")]
        print str(wbs)
        for wb in wbs:
            schema = 'wind_ds'
            curWb = xl.load_workbook(wb)

            table = 'wind_cost_projections'
            windCost(curWb,schema,table,conn,cur)
            table = 'wind_performance_improvements'
            windPerf(curWb,schema,table,conn,cur)
            table = 'market_projections'
            marketProj(curWb,schema,table,conn,cur)
            table = 'financial_parameters'
            finParams(curWb,schema,table,conn,cur)
            table = 'depreciation_schedule'
            depSched(curWb,schema,table,conn,cur)
            table = 'scenario_options'
            inpOpts(curWb,schema,table,conn,cur)


        if close_conn:
            print 'closing conn'
            conn.close()
    except:
        traceback.print_exc()

def windCost(curWb,schema,table,conn,cur):
    f = StringIO.StringIO()
    named_range = curWb.get_named_range('Wind_Cost_Projections')
    if named_range == None:
        print 'Wind_Cost_Projections named range does not exist'
        sys.exit(-1)
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
        f.write(str(l)[1:-1]+'\n')
        #print l
        c += 1
    f.seek(0)
    print 'Exporting wind_cost_projections'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def windPerf(curWb,schema,table,conn,cur):
    f = StringIO.StringIO()
    named_range = curWb.get_named_range('Wind_Performance_Improvements')
    if named_range == None:
        print 'Wind_Performance_Improvements named range does not exist'
        sys.exit(-1)
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
                watt_int = [int(watt.replace('MW','').strip()) * 1000]
            else:
                watt_int = [int(watt.replace('kW','').strip())]
            in_l = year + watt_int + l
            #print str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n'
            f.write(str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n')
            r += 1
        c += 1
    f.seek(0)
    print 'Exporting wind_performance_improvements'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def marketProj(curWb,schema,table,conn,cur):
    f = StringIO.StringIO()
    named_range = curWb.get_named_range('Market_Projections')
    if named_range == None:
        print 'Market_Projections named range does not exist'
        sys.exit(-1)
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
        f.write(str(l)[1:-1]+'\n')
        #print str(l)[1:-1]+'\n'
        c += 1
    f.seek(0)
    print 'Exporting market_projections'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def finParams(curWb,schema,table,conn,cur):
    f = StringIO.StringIO()
    #Residential
    named_range = curWb.get_named_range('Inputs_Residential')
    if named_range == None:
        print 'Inputs_Residential named range does not exist'
        sys.exit(-1)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 0
    length_irr = [0]
    cust_id = [1]
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
        in_l = cust_id + l + length_irr
        f.write(str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n')
        #print str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n'
        c += 1
    f.seek(0)
    print 'Exporting financial_parameters (residential inputs)'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    #Commercial
    named_range = curWb.get_named_range('Inputs_Commercial')
    if named_range == None:
        print 'Inputs_Commercial named range does not exist'
        sys.exit(-1)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 0
    cust_id = [2]
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
        in_l = cust_id + l
        f.write(str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n')
        #print str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n'
        c += 1
    f.seek(0)
    print 'Exporting financial_parameters (commercial inputs)'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    #Industrial
    named_range = curWb.get_named_range('Inputs_Industrial')
    if named_range == None:
        print 'Inputs_Industrial named range does not exist'
        sys.exit(-1)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 0
    cust_id = [3]
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
        in_l = cust_id + l
        f.write(str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n')
        #print str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n'
        c += 1
    f.seek(0)
    print 'Exporting financial_parameters (industrial inputs)'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    #Agricultural
    named_range = curWb.get_named_range('Inputs_Agricultural')
    if named_range == None:
        print 'Inputs_Agricultural named range does not exist'
        sys.exit(-1)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 0
    cust_id = [4]
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
        in_l = cust_id + l
        f.write(str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n')
        #print str(in_l).replace(" u'","").replace("'","")[1:-1]+'\n'
        c += 1
    f.seek(0)
    print 'Exporting financial_parameters (agricultural inputs)'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()

def depSched(curWb,schema,table,conn,cur):
    f = StringIO.StringIO()
    named_range = curWb.get_named_range('Depreciation_Schedule')
    if named_range == None:
        print 'Depreciation_Schedule named range does not exist'
        sys.exit(-1)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    r = 0
    while r < rows:
        c = 0
        l = []
        while c < columns:
            if cells[r][c].value == None:
                val = '0'
            else:
                val = cells[r][c].value
            l += [val]
            c += 1
        f.write(str(l)[1:-1]+'\n')
        #print l
        r += 1
    f.seek(0)
    print 'Exporting depreciation_schedule'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def inpOpts(curWb,schema,table,conn,cur):
    global sc_name
    f = StringIO.StringIO()

    input_named_range = 'Input_Scenario_Name'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        print 'Input_Scenario_Name named range does not exist'
        sys.exit(-1)
    sc_name = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

    input_named_range = 'Annual_Inflation'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        print 'Annual_Inflation named range does not exist'
        sys.exit(-1)
    ann_inf = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]


    named_range = curWb.get_named_range('Input_Scenario_Options')
    if named_range == None:
        print 'Input_Scenario_Options named range does not exist'
        sys.exit(-1)
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
        in_l = l + ann_inf + sc_name
        f.write(str(in_l).replace(" u'","").replace("u'","").replace("'","")[1:-1])
        #print str(in_l).replace(" u'","").replace("u'","").replace("'","")[1:-1]
        c += 1
    f.seek(0)
    print 'Exporting market_projections'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()



wb_loc = os.chdir(r'G:\140110_python_xl2pg\workbooks') #workbook location
main(wb_loc,None)

