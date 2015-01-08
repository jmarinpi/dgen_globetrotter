# -*- coding: utf-8 -*-
"""
Created on Thu Jan  8 11:11:21 2015

@author: mgleason
"""

import openpyxl as xl, os, sys, psycopg2 as pg
from cStringIO import StringIO
from config import pg_conn_string

class ExcelError(Exception):
    pass


def makeConn(connection_string, autocommit=True):
    conn = pg.connect(pg_conn_string)
    if autocommit:
        conn.set_isolation_level(pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn
    
    
def list2line(l):
    s = str(l).replace(" u'","").replace("u'","").replace("'","").replace(', ',',')[1:-1]+'\n'
    return s

    
def nem_scenario(curWb,schema,table,conn,cur,verbose=False):
    
    
    def findUtilityTypes():
        ut_nr = curWb.get_named_range("nem_util_types")
        if ut_nr == None:
            raise ExcelError('nem_util_types named range does not exist')
        cells = ut_nr.destinations[0][0].range(ut_nr.destinations[0][1])
        rows = len(cells)
        applicable_utility_types = []
        for row in range(0, rows):
            utility_type = str(cells[row][0].value)
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
                         round(a.nat_gas_dollars_per_mmbtu/293.3,3) as hourly_excess_sell_rate_dlrs_per_kwh
                FROM diffusion_solar.market_projections a
                CROSS JOIN diffusion_shared.state_fips_lkup b
                WHERE b.state_abbr <> 'PR';        
        """ % (schema, schema)
        cur.execute(sql)
        conn.commit()        

    if selected_scenario == 'User-Defined':
        f = StringIO()
        # get the applicable utility types        
        utility_types = findUtilityTypes()
        # get the states
        state_abbrs = findStates()
        # get the years
        year_ranges = findYears()
        
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
        sql = """INSERT INTO %s.%s
                            (year, state_abbr, sector_abbr, utility_type, system_size_limit_kw, 
                             year_end_excess_sell_rate_dlrs_per_kwh, hourly_excess_sell_rate_dlrs_per_kwh)        
                 SELECT a.year, a.state_abbr, a.sector_abbr, a.utility_type, a.system_size_limit_kw, 
                        a.year_end_excess_sell_rate_dlrs_per_kwh, a.hourly_excess_sell_rate_dlrs_per_kwh
                 FROM %s.nem_scenario_avoided_costs a
                 LEFT JOIN %s.%s b
                 ON a.year = b.year
                 AND a.state_abbr = b.state_abbr
                 AND a.sector_abbr = b.sector_abbr
                 AND a.utility_type = b.utility_type
                 WHERE a.utility_type in (%s)
                 AND b.year IS NULL;""" % (schema, table, schema, schema, table, str(utility_types)[1:-1])
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
        

def ud_elec_rates(curWb,schema,table,conn,cur,verbose=False):
    
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


def rate_type_weights(curWb,schema,table,conn,cur,verbose=False):
    
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