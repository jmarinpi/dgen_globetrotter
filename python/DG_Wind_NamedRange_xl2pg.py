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
import sys
import StringIO
from config import pg_conn_string
#from config import excelAlpha

class ExcelError(Exception):
    pass

def makeConn(connection_string, autocommit=True):
    conn = pg.connect(pg_conn_string)
    if autocommit:
        conn.set_isolation_level(pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

def main(wb, conn, verbose = False):
    try:
        # check connection to PG
        if not conn:
            close_conn = True
            conn = makeConn(pg_conn_string)
        else:
            iso_level = conn.isolation_level
            conn.set_isolation_level(pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            close_conn = False
        # make cursor from conn
        cur = conn.cursor()
        
        if os.path.exists(wb) == False:
            raise ExcelError('The specified input worksheet (%s) does not exist' % wb)
        
        # load the data
        schema = 'wind_ds'
        # note: to prevent this next line from printing all sorts of junk to the screen,
        # I had to disable a line in C:\Python27\lib\site-packages\openpyxl\namedrange.py
        # line #86 in refers_to_range: print range_string, bool(NAMED_RANGE_RE.match(range_string)) 
        # hopefully this is fixed in more up-to-date version of openpyxl
        curWb = xl.load_workbook(wb, data_only = True)

        table = 'wind_cost_projections'
        windCost(curWb,schema,table,conn,cur,verbose)
        table = 'wind_performance_improvements'
        windPerf(curWb,schema,table,conn,cur,verbose)
        table = 'market_projections'
        marketProj(curWb,schema,table,conn,cur,verbose)
        table = 'financial_parameters'
        finParams(curWb,schema,table,conn,cur,verbose)
        table = 'depreciation_schedule'
        depSched(curWb,schema,table,conn,cur,verbose)
        table = 'scenario_options'
        inpOpts(curWb,schema,table,conn,cur,verbose)
        table = 'manual_incentives'
        manIncents(curWb,schema,table,conn,cur,verbose)
        table = 'manual_net_metering_availability'
        manNetMetering(curWb,schema,table,conn,cur,verbose)
        table = 'user_defined_max_market_share'
        maxMarket(curWb,schema,table,conn,cur,verbose)
        table = 'wind_generation_derate_factors'
        windDerate(curWb,schema,table,conn,cur,verbose)


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
    sizes = [['2.5'],['5'],['10'],['20'],['50'],['100'],['250'],['500'],['750'],['1000'],['1500'],['3000']]
    f = StringIO.StringIO()
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
    f = StringIO.StringIO()
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
    f = StringIO.StringIO()
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
    
    


def marketProj(curWb,schema,table,conn,cur,verbose=False):
    f = StringIO.StringIO()
    named_range = curWb.get_named_range('Market_Projections')
    if named_range == None:
        raise ExcelError('Market_Projections named range does not exist')
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
    if verbose:
        print 'Exporting market_projections'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def finParams(curWb,schema,table,conn,cur,verbose=False):
    f = StringIO.StringIO()
    #Residential
    named_range = curWb.get_named_range('Inputs_Residential')
    if named_range == None:
        raise ExcelError('Inputs_Residential named range does not exist')
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 0
    length_irr = [0]
    cust_id = ['Residential']
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
    if verbose:
        print 'Exporting financial_parameters (residential inputs)'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    #Commercial
    named_range = curWb.get_named_range('Inputs_Commercial')
    if named_range == None:
        raise ExcelError('Inputs_Commercial named range does not exist')
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 0
    cust_id = ['Commercial']
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
    if verbose:
        print 'Exporting financial_parameters (commercial inputs)'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    #Industrial
    named_range = curWb.get_named_range('Inputs_Industrial')
    if named_range == None:
        raise ExcelError('Inputs_Industrial named range does not exist')
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 0
    cust_id = ['Industrial']
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
    if verbose:
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

def depSched(curWb,schema,table,conn,cur,verbose=False):
    f = StringIO.StringIO()
    named_range = curWb.get_named_range('Depreciation_Schedule')
    if named_range == None:
        raise ExcelError('Depreciation_Schedule named range does not exist')
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
    if verbose:
        print 'Exporting depreciation_schedule'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def inpOpts(curWb,schema,table,conn,cur,verbose=False):
    global sc_name
    f = StringIO.StringIO()

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
    
    input_named_range = 'overwrite_exist_nm'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        raise ExcelError('overwrite_exist_nm named range does not exist')
    overwrite_exist_nm = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

    input_named_range = 'incent_start_year'
    named_range = curWb.get_named_range(input_named_range)
    if named_range == None:
        raise ExcelError('incent_start_year named range does not exist')
    incent_startyear = [named_range.destinations[0][0].range(named_range.destinations[0][1]).value]

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

    in_l = l + ann_inf + sc_name + overwrite_exist_inc + incent_startyear + incent_utility + overwrite_exist_nm
    f.write(str(in_l).replace(" u'","").replace("u'","").replace("'","")[1:-1])
    #print str(in_l).replace(" u'","").replace("u'","").replace("'","")[1:-1]
    f.seek(0)
    if verbose:
        print 'Exporting scenario_options'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_expert('''COPY %s.%s (region, 
        end_year, 
        markets, 
        cust_exp_elec_rates, 
        load_growth_scenario, 
        res_rate_structure, 
        res_rate_escalation, 
        res_max_market_curve, 
        com_rate_structure, 
        com_rate_escalation, 
        com_max_market_curve, 
        com_demand_charge_rate, 
        ind_rate_structure, 
        ind_rate_escalation, 
        ind_max_market_curve, 
        ind_demand_charge_rate,
        net_metering_availability, 
        carbon_price, 
        height_exclusions, 
        ann_inflation, 
        scenario_name, 
        overwrite_exist_inc, 
        incentive_start_year, 
        utility_type_iou, 
        utility_type_muni, 
        utility_type_coop, 
        utility_type_allother, 
        overwrite_exist_nm) FROM STDOUT WITH CSV;''' % (schema,table), f)        
#    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()

def manIncents(curWb,schema,table,conn,cur,verbose=False):

    def findIncen(cells,c,incen_type):
        try:
            if 'Tax' in cells[0][c].value:
                return 'Tax'
            elif 'Production' in cells[0][c].value:
                return 'Production'
            elif 'Rebate' in cells[0][c].value:
                return 'Rebate'
        except:
            return incen_type

    def findSector(cells,c,sector_type):
        merged_range = cells[1][c-2:c+1]
        values = [cell.value for cell in merged_range]
        
        if 'Residential' in values:
            return 'Residential'
        elif 'Commercial' in values:
            return 'Commercial'
        elif 'Industrial' in values:
            #cells[1][c].value
            return 'Industrial'
        else:
            return sector_type

    def findUtilityTypes():
        ut_nr = curWb.get_named_range("Incentives_Utility_Type")
        if ut_nr == None:
            raise ExcelError('Incentives_Utility_Type named range does not exist')
        all_utility_types = ['IOU','Muni','Coop', 'All Other']
        ut_cells =  ut_nr.destinations[0][0].range(ut_nr.destinations[0][1])
        selected_utility_types = {}
        for i, ut_cell in enumerate(ut_cells):
            selected_utility_types[all_utility_types[i]] = ut_cell[0].value
                
        return selected_utility_types
        
    selected_utility_types = findUtilityTypes()


    f = StringIO.StringIO()
    named_range = curWb.get_named_range('Incentives_Values')
    if named_range == None:
        raise ExcelError('Incentives_Values named range does not exist')
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

            if incen_type == 'Tax':
                for utility_type in selected_utility_types.keys():
                    if selected_utility_types[utility_type]:                
                        out = region + [incen_type] + [sector_type] + l + [0,0,0] + budget + [utility_type]
                        f.write(str(out).replace("u'","").replace(" '","").replace("'","")[1:-1]+'\n')
            if incen_type == 'Production':
                for utility_type in selected_utility_types.keys():
                    if selected_utility_types[utility_type]: 
                        out = region + [incen_type] + [sector_type] + [0] + [0] + [l[2]] + [l[0]] + [l[1]] + [0] + budget + [utility_type]
                        f.write(str(out).replace("u'","").replace(" '","").replace("'","")[1:-1]+'\n')
            if incen_type == 'Rebate':
                for utility_type in selected_utility_types:
                    for utility_type in selected_utility_types.keys():
                        if selected_utility_types[utility_type]: 
                            out = region + [incen_type] + [sector_type] + [0] + [l[1]] + [l[2]] + [0] + [0] + [l[0]] + budget + [utility_type]
                            f.write(str(out).replace("u'","").replace(" '","").replace("'","")[1:-1]+'\n')
        r += 1
    f.seek(0)
    if verbose:
        print 'Exporting manual_incentives'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def manNetMetering(curWb,schema,table,conn,cur,verbose=False):

    def findUtilityTypes():
        ut_nr = curWb.get_named_range("Incentives_Utility_Type")
        if ut_nr == None:
            raise ExcelError('Incentives_Utility_Type named range does not exist')
        all_utility_types = ['IOU','Muni','Coop', 'All Other']
        ut_cells =  ut_nr.destinations[0][0].range(ut_nr.destinations[0][1])
        selected_utility_types = {}
        for i, ut_cell in enumerate(ut_cells):
            selected_utility_types[all_utility_types[i]] = ut_cell[0].value
                
        return selected_utility_types
        
    selected_utility_types = findUtilityTypes()
    
    f = StringIO.StringIO()
    named_range = curWb.get_named_range('Net_Metering')
    if named_range == None:
        raise ExcelError('Incentives_Values named range does not exist')
    state_cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    data_cells = named_range.destinations[1][0].range(named_range.destinations[1][1])
    
    sectors = ['res','com','ind']
    for i, state_cell in enumerate(state_cells):  
        state_abbr = state_cell[0].value
        for j, sector in enumerate(sectors):
            for utility_type in selected_utility_types.keys():
                if selected_utility_types[utility_type]:
                    cell_value = data_cells[i][j].value
                    if cell_value is not None:
                        nem_limit = cell_value
                    else:
                        nem_limit = 0
                else:
                    nem_limit = 0
                row = [sector, utility_type, nem_limit, state_abbr]
                f.write(str(row).replace("u'","").replace(" '","").replace("'","").replace(', ',',')[1:-1]+'\n')
    f.seek(0)
    if verbose:
        print 'Exporting Manual Net Metering'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


def maxMarket(curWb,schema,table,conn,cur,verbose=False):
    f = StringIO.StringIO()
    named_range = curWb.get_named_range('user_defined_max_market_share')
    if named_range == None:
        raise ExcelError('user_defined_max_market_share named range does not exist')
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    r = 2
    while r < rows:
        c = 1
        l = []
        while c < columns:
            year = [cells[r][0].value]
            if cells[r][c].value == None:
                val = '0'
            else:
                val = cells[r][c].value
            l += [val]
            c += 1
        sectors = ['residential','commercial','industrial']
        res_out = year + [sectors[0]] + l[:2]
        com_out = year + [sectors[1]] + l[2:4]
        ind_out = year + [sectors[2]] + l[4:]
        f.write(str(res_out).replace(" '","").replace("'","")[1:-1]+'\n')
        f.write(str(com_out).replace(" '","").replace("'","")[1:-1]+'\n')
        f.write(str(ind_out).replace(" '","").replace("'","")[1:-1]+'\n')
        r += 1
    f.seek(0)
    if verbose:
        print 'Exporting user_defined_max_market_share'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()


if __name__ == '__main__':
    input_xls = '../excel/scenario_inputs.xlsm'
    main(input_xls,None, True)

