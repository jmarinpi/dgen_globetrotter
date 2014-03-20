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
#from config import excelAlpha

def makeConn(host='localhost',dbname='dav-gis', user='postgres', password='postgres', autocommit=True):
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
            table = 'manual_incentives'
            manIncents(curWb,schema,table,conn,cur)


        if close_conn:
            print 'closing conn'
            conn.close()
    except:
        traceback.print_exc()

def windCost(curWb,schema,table,conn,cur):
    sizes = [['2.5'],['5'],['10'],['20'],['50'],['100'],['250'],['500'],['750'],['1000'],['1500'],['3000']]
    f = StringIO.StringIO()
    for item in sizes:
        rname = 'Wind_Cost_Projections_' + item[0] + '_kw'
        named_range = curWb.get_named_range(rname)
        if named_range == None:
            print rname, 'named range does not exist'
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
            f.write(str(l)[1:-1] + ',' + item[0] + '\n')
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
                watt_flt = [float(watt.replace('MW','').strip()) * 1000]
            else:
                watt_flt = [float(watt.replace('kW','').strip())]
            in_l = year + watt_flt + l
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
    #Agricultural (REMOVED 3/19/2014)
    """
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
    """
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

    input_named_range = 'overwrite_exist_inc'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        print 'overwrite_exist_inc named range does not exist'
        sys.exit(-1)
    overwrite_exist = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

    input_named_range = 'incent_start_year'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        print 'incent_start_year named range does not exist'
        sys.exit(-1)
    incent_startyear = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

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
        c += 1
    named_range = curWb.get_named_range('Incentives_Utility_Type')
    if named_range == None:
        print 'Incentives_Utility_Type named range does not exist'
        sys.exit(-1)
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

    in_l = l + ann_inf + sc_name + overwrite_exist + incent_startyear + incent_utility
    f.write(str(in_l).replace(" u'","").replace("u'","").replace("'","")[1:-1])
    #print str(in_l).replace(" u'","").replace("u'","").replace("'","")[1:-1]
    f.seek(0)
    print 'Exporting scenario_options'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()

def manIncents(curWb,schema,table,conn,cur):

    def findIncen(cells,c,incen_type):
        try:
            if 'Tax' in cells[0][c].value:
                return 'Tax Incentives'
            elif 'Production' in cells[0][c].value:
                return 'Production Incentives'
            elif 'Rebate' in cells[0][c].value:
                return 'Rebate Incentives'
        except:
            return incen_type

    def findSector(cells,c,sector_type):
        if 'Residential' in cells[1][c].value:
            return 'Residential'
        elif 'Commercial' in cells[1][c].value:
            return 'Commercial'
        elif 'Industrial' in cells[1][c].value:
            return 'Industrial'
        else:
            return sector_type

    f = StringIO.StringIO()
    named_range = curWb.get_named_range('Incentives_Values')
    if named_range == None:
        print 'Incentives_Values named range does not exist'
        sys.exit(-1)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0]) - 1
    rows = len(cells)
    r = 3
    while r < rows:
        c = 1
        region = [cells[r][0].value]
        budget = [cells[r][columns].value]
        if budget == [None]:
            budget = [0]
        incen_type = ''
        sector_type = ''
        while c < columns:
            l = []
            parts = 3
            p = 1
            while p <= parts:
                incen_type = findIncen(cells,c,incen_type)
                sector_type = findSector(cells,c,sector_type)
                if cells[r][c].value == None:
                    val = '0'
                else:
                    val = cells[r][c].value
                l += [val]
                c += 1
                p += 1

            if l == ['0', '0', '0']:
                continue
            else:
                if [incen_type] == ['Tax Incentives']:
                    out = region + [incen_type] + [sector_type] + l + [0,0,0] + budget
                    f.write(str(out).replace("u'","").replace(" '","").replace("'","")[1:-1]+'\n')
                if [incen_type] == ['Production Incentives']:
                    out = region + [incen_type] + [sector_type] + [0] + [0] + [l[2]] + [l[0]] + [l[1]] + [0] + budget
                    f.write(str(out).replace("u'","").replace(" '","").replace("'","")[1:-1]+'\n')
                if [incen_type] == ['Rebate Incentives']:
                    out = region + [incen_type] + [sector_type] + [0] + [l[1]] + [l[2]] + [0] + [0] + [l[0]] + budget
                    f.write(str(out).replace("u'","").replace(" '","").replace("'","")[1:-1]+'\n')
        r += 1
    f.seek(0)
    print 'Exporting manual_incentives'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()




wb_loc = os.chdir(r'C:\ngrue\git\diffusion.git\excel') #workbook location
main(wb_loc,None)

