# -*- coding: utf-8 -*-
"""
Created on Thu Jan  8 11:11:21 2015

@author: mgleason
"""

import openpyxl as xl, os, sys, psycopg2 as pg
from cStringIO import StringIO
from config import pg_conn_string
import decorators



@decorators.unshared
class ExcelError(Exception):
    pass


@decorators.unshared
def makeConn(connection_string, autocommit=True):
    conn = pg.connect(pg_conn_string)
    if autocommit:
        conn.set_isolation_level(pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn
    
@decorators.unshared 
def list2line(l):
    s = str(l).replace(" u'","").replace("u'","").replace("'","").replace(', ',',')[1:-1]+'\n'
    return s

    
@decorators.shared    
def nem_scenario(curWb,schema,conn,cur,verbose=False):
    
    table = 'nem_scenario'
    valid_values = ['iou','muni','coop','all other']
    def findUtilityTypes():
        ut_nr = curWb.get_named_range("nem_util_types")
        if ut_nr == None:
            raise ExcelError('nem_util_types named range does not exist')
        cells = ut_nr.destinations[0][0].range(ut_nr.destinations[0][1])
        rows = len(cells)
        applicable_utility_types = []
        for row in range(0, rows):
            utility_type = str(cells[row][0].value)
            if utility_type.lower() not in valid_values:
                raise ExcelError('invalid utility type specified in Net Metering sheet. Valid values are %s' % valid_values)                
            if cells[row][1].value == True:
                applicable_utility_types.append(utility_type)
                
        return applicable_utility_types
        
    def findStates():
        rname = 'nem_states'
        named_range = curWb.get_named_range(rname)
        if named_range == None:
            raise ExcelError('%s named range does not exist.' % rname)
        cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
        rows = len(cells)
        state_abbrs = [str(cells[row][0].value) for row in range(0, rows)]
        
        return state_abbrs
  
    def findYears():
        rname = 'nem_years'
        named_range = curWb.get_named_range(rname)
        if named_range == None:
            raise ExcelError('%s named range does not exist.' % rname)
        cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
        rows = len(cells)
        year_ranges = []
        for row in range(0, rows):
            first_year = cells[row][0].value
            last_year = cells[row][1].value
            year_ranges.append((first_year, last_year))
            
        return year_ranges
  
    selected_scenario_rname = 'nem_selected_scenario'
    selected_scenario_named_range = curWb.get_named_range(selected_scenario_rname)
    if selected_scenario_named_range == None:
        raise ExcelError('%s named range does not exist.' % selected_scenario_rname)
    selected_scenario = selected_scenario_named_range.destinations[0][0].range(selected_scenario_named_range.destinations[0][1]).value
    
    if verbose:
        print 'Exporting Rate Type Weights'
        
    # clear the existing nem_scenario table
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    conn.commit()    
    
    if selected_scenario in ['User-Defined', 'Avoided Costs']:
        # load the avoided costs
        sql = """DROP TABLE IF EXISTS %s.nem_scenario_avoided_costs;
                 CREATE TABLE %s.nem_scenario_avoided_costs AS
                 SELECT  a.year, 
                         b.state_abbr,
                        	 unnest(array['res','com','ind']) as sector_abbr,
                         unnest(array['All Other', 'Coop', 'IOU', 'Muni']) as utility_type,
                         0::double precision as system_size_limit_kw,
                         0::numeric as year_end_excess_sell_rate_dlrs_per_kwh,
                         a.avoided_costs_dollars_per_kwh as hourly_excess_sell_rate_dlrs_per_kwh
                FROM diffusion_solar.market_projections a
                CROSS JOIN diffusion_shared.state_fips_lkup b
                WHERE b.state_abbr <> 'PR';        
        """ % (schema, schema)
        cur.execute(sql)
        conn.commit()        

    named_range = curWb.get_named_range('expiration_rate')
    if named_range == None:
        raise ExcelError('expiration_rate named range does not exist.')
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    expiration_rate = cells.value

    if expiration_rate == 'State Wholesale':
        # load the state wholesale prices

        sql = """DROP TABLE IF EXISTS %s.nem_scenario_state_wholesale;
                CREATE TABLE %s.nem_scenario_state_wholesale AS
                WITH z AS (
                SELECT  a.year,
                     b.state_abbr,
                     unnest(array['res','com','ind']) as sector_abbr,
                     unnest(array['All Other', 'Coop', 'IOU', 'Muni']) as utility_type,
                     0::double precision as system_size_limit_kw,
                     0::numeric as year_end_excess_sell_rate_dlrs_per_kwh
                FROM diffusion_solar.market_projections a
                CROSS JOIN diffusion_shared.state_fips_lkup b
                WHERE b.state_abbr <> 'PR')
                SELECT z.*, c.wholesale_elec_price AS hourly_excess_sell_rate_dlrs_per_kwh
                FROM z
                LEFT JOIN %s.state_wholesale_price AS c
                ON z.year = c.year AND z.state_abbr = c.state
                ;""" % (schema, schema, schema)
        cur.execute(sql)
        conn.commit()

    if selected_scenario == 'User-Defined':
        # get the applicable utility types        
        utility_types = findUtilityTypes()
        # get the states
        state_abbrs = findStates()
        # get the years
        year_ranges = findYears()
        
        f = StringIO()
        
        sector_abbrs = ['res','com','ind']
        for sector_abbr in sector_abbrs:
            rname = 'nem_scenario_%s' % sector_abbr
            named_range = curWb.get_named_range(rname)
            if named_range == None:
                raise ExcelError('%s named range does not exist.' % rname)
            cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
            rows = len(cells)
            for row in range(0, rows):
                state_abbr = state_abbrs[row]
                system_size_limit_kw = cells[row][0].value
                year_end_excess_sell_rate_dlrs_per_kwh = cells[row][2].value
                hourly_excess_sell_rate_dlrs_per_kwh = cells[row][3].value
                if system_size_limit_kw <= 0:
                    # don't allow negative values
                    system_size_limit_kw = 0
                    # overwrite year_end_excess_sell_rate_dlrs_per_kwh (it won't apply if there is no net metering)
                    year_end_excess_sell_rate_dlrs_per_kwh = 0
                else:
                    # overwrite hourly_excess_sell_rate_dlrs_per_kwh (it won't apply if there is net metering)
                    hourly_excess_sell_rate_dlrs_per_kwh = 0
                first_year = year_ranges[row][0]
                last_year = year_ranges[row][1]
                for utility_type in utility_types:
                    for year in range(first_year, last_year+1, 2):
                        l = [year, state_abbr, sector_abbr, utility_type, system_size_limit_kw, year_end_excess_sell_rate_dlrs_per_kwh, hourly_excess_sell_rate_dlrs_per_kwh]
                        f.write(list2line(l))
        
        f.seek(0)
        # use "COPY" to dump the data to the staging table in PG
        cur.execute('DELETE FROM %s.%s;' % (schema, table))
        cur.copy_from(f,"%s.%s" % (schema,table),sep=',')  
        cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
        conn.commit()
        f.close()

        # append avoided costs to the end
        if expiration_rate == 'Avoided Cost':
            nem_expiration_table = 'nem_scenario_avoided_costs'
        elif expiration_rate == 'State Wholesale':
            nem_expiration_table = 'nem_scenario_state_wholesale'
        else:
            print(expiration_rate)
            raise ExcelError('state_wholesale_price named range does not exist')

        sql = """INSERT INTO %s.%s
                            (year, state_abbr, sector_abbr, utility_type, system_size_limit_kw,
                             year_end_excess_sell_rate_dlrs_per_kwh, hourly_excess_sell_rate_dlrs_per_kwh)
                 SELECT a.year, a.state_abbr, a.sector_abbr, a.utility_type, a.system_size_limit_kw,
                        a.year_end_excess_sell_rate_dlrs_per_kwh, a.hourly_excess_sell_rate_dlrs_per_kwh
                 FROM %s.%s a
                 LEFT JOIN %s.%s b
                 ON a.year = b.year
                 AND a.state_abbr = b.state_abbr
                 AND a.sector_abbr = b.sector_abbr
                 AND a.utility_type = b.utility_type
                 WHERE a.utility_type in (%s)
                 AND b.year IS NULL;""" % (schema, table, schema, nem_expiration_table, schema, table, str(utility_types)[1:-1])
        cur.execute(sql)
        conn.commit()
        

    elif selected_scenario in ['BAU', 'Full Everywhere', 'None Everywhere', 'Avoided Costs']:
        # copy the data from the correct sheet
        if selected_scenario == 'Avoided Costs':
            source_schema = 'diffusion_solar'
        else:
             source_schema = 'diffusion_shared'
        source_table = '%s.nem_scenario_%s' % (source_schema, selected_scenario.lower().replace(' ','_'))
        sql = """INSERT INTO %s.%s
                            (year, state_abbr, sector_abbr, utility_type, system_size_limit_kw, 
                             year_end_excess_sell_rate_dlrs_per_kwh, hourly_excess_sell_rate_dlrs_per_kwh)
                SELECT year, state_abbr, sector_abbr, utility_type, system_size_limit_kw, 
                        year_end_excess_sell_rate_dlrs_per_kwh, hourly_excess_sell_rate_dlrs_per_kwh
                FROM %s""" % (schema, table, source_table)
        cur.execute(sql)
        conn.commit()
        
@decorators.shared
def ud_elec_rates(curWb,schema,conn,cur,verbose=False):
    
    table = 'user_defined_electric_rates'    
    
    f = StringIO()
    rname = 'ud_rates'
    named_range = curWb.get_named_range(rname)
    if named_range == None:
        raise ExcelError('%s named range does not exist.' % rname)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    rows = len(cells)
    for r in range(0, rows):
        state_abbr = cells[r][0].value or 0
        res_rate = cells[r][1].value or 0
        com_rate = cells[r][2].value or 0
        ind_rate = cells[r][3].value or 0
        l = [state_abbr, res_rate, com_rate, ind_rate]
        f.write(list2line(l))
    f.seek(0)
    if verbose:
        print 'Exporting User-Defined Flat Electric Rates'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_expert("""COPY %s.%s (state_abbr, res_rate_dlrs_per_kwh, com_rate_dlrs_per_kwh, ind_rate_dlrs_per_kwh) 
                FROM STDOUT WITH CSV""" % (schema, table), f)     
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()
    
    # add in the FIPS codes
    sql = """UPDATE %s.%s a
             SET state_fips = b.state_fips
             FROM diffusion_shared.state_fips_lkup b
             WHERE a.state_abbr = b.state_abbr;""" % (schema, table)
    cur.execute(sql)
    conn.commit()

@decorators.shared
def rate_type_weights(curWb,schema,conn,cur,verbose=False):
    
    table = 'rate_type_weights'    
    
    f = StringIO()
    rname = 'rate_type_weights'
    named_range = curWb.get_named_range(rname)
    if named_range == None:
        raise ExcelError('%s named range does not exist.' % rname)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    rows = len(cells)
    for r in range(0, rows):
        rate_type = cells[r][0].value
        res_weight = cells[r][1].value or 0.01 # replace zero or null weights for sampling in the model to work correctly
        com_ind_weight = cells[r][2].value or 0.01 # replace zero or null weights for sampling in the model to work correctly
        l = [rate_type, res_weight, com_ind_weight, com_ind_weight]
        f.write(list2line(l))
    f.seek(0)
    if verbose:
        print 'Exporting Rate Type Weights'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_expert("""COPY %s.%s (rate_type_desc, res_weight, com_weight, ind_weight) 
                FROM STDOUT WITH CSV""" % (schema, table), f)     
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()
    
    # add in the FIPS codes
    sql = """UPDATE %s.%s a
             SET rate_type = b.rate_type
             FROM diffusion_shared.rate_type_desc_lkup b
             WHERE a.rate_type_desc = b.rate_type_desc;""" % (schema, table)
    cur.execute(sql)
    conn.commit()
    

@decorators.shared
def leasingAvail(curWb,schema,conn,cur,verbose=False):
    
    table = 'leasing_availability'    
    
    f = StringIO()
    rname = 'leasing_avail'
    named_range = curWb.get_named_range(rname)
    if named_range == None:
        raise ExcelError('%s named range does not exist.' % rname)
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])

    columns = len(cells[0])
    rows = len(cells)
    r = 1
    while r < rows:
        c = 1
        while c < columns:
            if cells[r][c].value == None:
                val = '0'
            else:
                val = cells[r][c].value
            l = [cells[r][0].value, cells[0][c].value, val]
            f.write(list2line(l))
            c += 1
        r += 1
    f.seek(0)
    if verbose:
        print 'Exporting leasing_avail'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()

@decorators.shared
def maxMarket(curWb,schema,conn,cur,verbose=False):
    
    table = 'user_defined_max_market_share'    
    
    f = StringIO()
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

@decorators.shared    
def manIncents(curWb,schema,conn,cur,verbose=False):
    
    table = 'manual_incentives'

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


    f = StringIO()
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
                for utility_type in selected_utility_types.keys():
                    if selected_utility_types[utility_type]: 
                        out = region + [incen_type] + [sector_type] + [0] + [l[1]] + [l[2]] + [0] + [0] + [float(l[0])/1000.] + budget + [utility_type]
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
    

@decorators.shared    
def depSched(curWb,schema,conn,cur,verbose=False):
    
    table = 'depreciation_schedule'    
    
    f = StringIO()
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

    
@decorators.shared      
def finParams(curWb,schema,conn,cur,verbose=False):
        
    table = 'financial_parameters'    

    f = StringIO()
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

    f.close()

@decorators.shared    
def marketProj(curWb,schema,conn,cur,verbose=False):
    
    table = 'market_projections'    
    
    f = StringIO()
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
    
@decorators.shared
def stateWholesale(curWb,schema,conn,cur,verbose=False):

    table = 'state_wholesale_price'

    f = StringIO()
    named_range = curWb.get_named_range(table)
    if named_range == None:
        raise ExcelError('state_wholesale_price named range does not exist')
    cells = named_range.destinations[0][0].range(named_range.destinations[0][1])
    columns = len(cells[0])
    rows = len(cells)
    c = 1
    while c < columns:
        r = 1
        year = cells[0][c].value
        while r < rows:
            state = cells[r][0].value
            if cells[r][c].value is None:
                val = '0'
            else:
                val = cells[r][c].value
            l = [year, state, val]
            r += 1

            # TODO: find less hacky way to get rid of encoding weirdness
            lString = str(l)[1:-1].replace("u'", "").replace("'", "").replace(" ", "")

            f.write(lString + '\n')
        c += 1
    f.seek(0)
    if verbose:
        print 'Exporting state_wholesale_price'
    # use "COPY" to dump the data to the staging table in PG
    cur.execute('DELETE FROM %s.%s;' % (schema, table))
    cur.copy_from(f,"%s.%s" % (schema,table),sep=',')
    cur.execute('VACUUM ANALYZE %s.%s;' % (schema,table))
    conn.commit()
    f.close()
    
# NOTE: THIS MUST ALWAYS GO LAST
shared_table_functions = []
for key, value in locals().items():
    if callable(value) and value.__module__ == __name__ and value.shared == True:
        shared_table_functions.append(value)