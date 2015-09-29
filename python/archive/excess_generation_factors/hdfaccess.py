# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 09:11:08 2014

@author: mgleason
"""

import h5py
import numpy as np
import psycopg2 as pg

#import reeds
#import cfPhase2
#import pwrcurve
#import pgdbUtil
#import time

def getAllIndices(cur):
    # check that cursor is the current type
    if type(cur) <> pg.extras.DictCursor:
        raise TypeError('cur must be of type psycopg2.extras.DictCursor')   
    
    sql = 'SELECT hdf_index \
            FROM aws.tmy_grid\
            ORDER BY hdf_index;'
    cur.execute(sql)
    indices = [row['hdf_index'] for row in cur.fetchall()]
    return indices        


def getIndicesByIntersection(cur, filter_table, filter_geom, filter_where_clause = ''):
    '''
    Get the hdf indices from the postgres lookup table using a spatial intersection with the input parameters
    
    cur = a pyscopg2.extras.Dictcursor pointing to h=gispgdb.nrel.gov and db=dav-gis
    filter_table = a string specifying a postgis table, of the form schema.table
    filter_geom = a string specifiying the geometry column of interest in filter_table
    filter_where_clause = an optional string specifying a where clause used to subset of filter_table. column names should be aliased with a b.
    '''
    
    # check that cursor is the current type
    if type(cur) <> pg.extras.DictCursor:
        raise TypeError('cur must be of type psycopg2.extras.DictCursor')    
    
    # if schema is not specified, reformat filter_table to reference the public schema
    if '.' not in filter_table:
        filter_table = 'public.%s' % filter_table
    # check existence of filter_table
    cur.execute("SELECT EXISTS(\
                    SELECT table_schema|| '.' || table_name\
                    FROM information_schema.tables \
                    WHERE table_schema|| '.' || table_name = '%s');" % filter_table)
    if cur.fetchone()[0] == False:
        raise ValueError("Filter Table '%s' does not exist in Postgres Database\n" % filter_table)

    
    # check the srid of the filter table
    sql = "SELECT distinct(st_srid(%s)) as srid\
            FROM %s ;" % (filter_geom, filter_table)
    cur.execute(sql)
    srids = [row['srid'] for row in cur.fetchall()]
    # if there are any geoms in the table with srids other than 96703, we need to transform the geometries for ST_Intersects
    if srids <> [96703]:
        filter_geom = "ST_Transform(b.%s, 96703)" % filter_geom
    else:
        filter_geom = "b.%s" % filter_geom
    
    sql = "SELECT a.hdf_index\
            FROM aws.tmy_grid a\
            INNER JOIN %s b\
            ON ST_Intersects(a.the_geom_96703,%s) %s\
            ORDER BY hdf_index" % (filter_table,filter_geom,filter_where_clause)
    cur.execute(sql)
    indices = [row['hdf_index'] for row in cur.fetchall()]
    return indices    

    
def getIndicesByState(cur, state_abbr_list):
    '''
    Get the hdf indices from the postgres lookup table using the input list of states

    cur = a pyscopg2.extras.Dictcursor pointing to h=gispgdb.nrel.gov and db=dav-gis
    state_abbr_list = a list of 2-letter state abbreviations specifying which states' cells should be returned
    
    '''
    if type(cur) <> pg.extras.DictCursor:
        raise TypeError('cur must be of type psycopg2.extras.DictCursor')

    sql = "SELECT a.hdf_index\
            FROM aws.tmy_grid a\
            LEFT JOIN windpy.onshore_state_lkup b\
            ON a.i = b.i\
            and a.j = b.j\
            WHERE b.state_abbr in (%s)\
            ORDER by hdf_index" % state_abbr_list.__str__()[1:-1]
    cur.execute(sql)
    indices = [row['hdf_index'] for row in cur.fetchall()]
    return indices

#def filterByCFBin(cur, indices, cf_bin):
#    '''Subset a list of i,j grid cell indices and return only those for which the specified cf_bin applies
#    
#    (some grid cells will not have certain cf_bins)
#    
#    # ** add more documentation here
#    '''
#    
#    # ** this needs to be changed because i need the full list of indices back, but maybe just mask them?    
#    
#    if type(cur) <> pg.extras.DictCursor:
#        raise TypeError('cur must be of type psycopg2.extras.DictCursor')
#    sql = 'SELECT DISTINCT(hdf_index) FROM aws.ij_icf_lookup_onshore WHERE cf_bin = %s;' % cf_bin
#    cur.execute(sql)
#    cf_indices = [row['hdf_index'] for row in cur.fetchall()]
#    
#    # if indices is an empty list, it means we should be operatinng on all applicable grid cells
#    if indices == []:
#        # so simply subset to the cf_indices
#       return cf_indices
#    else:
#        # otherwise, find the overlap between the input indices list and the cf_indices, and return that list
#        overlap = list(set(indices).intersection(set(cf_indices)))
#        return overlap

def maskForCFBin(cur, indices, cf_bin):
    '''Subset a list of i,j grid cell indices and return only those for which the specified cf_bin applies
    
    (some grid cells will not have certain cf_bins)
    
    # ** add more documentation here
    '''
    
    # ** this needs to be changed because i need the full list of indices back, but maybe just mask them?    
    
    if type(cur) <> pg.extras.DictCursor:
        raise TypeError('cur must be of type psycopg2.extras.DictCursor')
    sql = 'SELECT DISTINCT(hdf_index)\
            FROM aws.ij_icf_lookup_onshore\
            WHERE cf_bin = %s;' % cf_bin
    cur.execute(sql)
    cf_indices = [row['hdf_index'] for row in cur.fetchall()]
    

    # find the overlap between the input indices list and the cf_indices, and return that list
    overlap = np.in1d(np.array(indices), np.array(cf_indices))
    non_overlap = np.invert(overlap)        
    non_overlap_2d = np.repeat(non_overlap[:,np.newaxis],8760,1)
    return non_overlap, non_overlap_2d
        

def getFilteredData(hfile,path,indices = [], mask = None):
    '''
    Return the filtered data, scale factor, fill value mask, and optionally list of indexes. If indices is not specified, data for all locations will be returned
    
    hfile = an HDF file object
    path = string giving the path to the dataset of interest in the hfile
    indices = optional list of indices for which to get data (indicating different cell locations)
    '''
    
    # find the scale factor (if it exists)  
    if 'scale_factor' in hfile[path].attrs.keys():
        scale_factor =  hfile[path].attrs['scale_factor']
    else:
        scale_factor = 1 # this will have no effect when multiplied against the array
    
    # find the fill_value (if it exists)  
    if 'fill_value' in hfile[path].attrs.keys():
        fill_value =  hfile[path].attrs['fill_value']
    else:
        fill_value = None # this will have no effect on np.ma.masked_equal()
        
    # get data, masking fill_value and applying scale factor
    extract = np.ma.masked_equal(hfile[path], fill_value) * scale_factor
    if indices <> []:
        # Extract the subset
        data = extract[indices]
    else:
        data = extract
    
    if mask <> None:
        result = np.ma.masked_array(data,mask)
    else:
        result = data
        
    return result

# DEPRECATED -- NO LONGER NECESSARY
#def getWindspeedPaths(min_cf_bin = None, hts = [30,40,50,80,100,110,120,140]):
#    '''
#    Returns a list of paths to wind_speed datasets within the given hfile
#    
#    min_cf_bin = optional integer specifying minimum capacity factor bin to process. all cf bins greater than or equal to this number will be specified
#                 This value specifies the upper boundary of the minimum bin to process. So if you only want to process cf values above 30 (=0.3),
#                 you would specify 33 (this is the upper bound of the first bin you would process).
#                 If not specified, all cf bins that are present in the hdf file will be processed.
#    hts = optional list of ints specifying heights for which to get windspeed data in m above sea level. 
#                If not specified, all heights that are present in the hdf file will be processed.
#    '''    
#    
#
#    if min_cf_bin is not None:    
#        cf_bins =['%03d0' % i for i in range(3,67,3) if i >= min_cf_bin]
#    else:
#        cf_bins =['%03d0' % i for i in range(3,67,3)]
#        
#    data_paths = []
#    for cf_bin in cf_bins:
#        for ht in hts:
#            current_path = '/%s_cfbin/%s/wind_speed' % (cf_bin, ht)
#            data_paths.append(current_path)
#
#    return data_paths



#==============================================================================
#     
#==============================================================================
#def main():
#    # get a power curve
#    conn, cur = pgdbUtil.pgdbConnect(True)
#    ws, pwr = cfPhase2.getPCdata('GE_1.62-100',cur)
#    pcrv = pwrcurve.PwrCurve(ws,pwr)    
#    
#    # set path to and open the hdf file    
#    f = onshore_tmy_hdf = r'D:\data\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\Onshore_bySite\onshore_tmy.hdf5' 
#    #f = onshore_tmy_hdf = r'G:\GIS_Data_Catalog\NAM\Country\US\e_res\wind\awst_wind_licensed\AWS_ReEDS_wind\Onshore_bySite\onshore_tmy.hdf5' 
#    hf = h5py.File(onshore_tmy_hdf,'r')
#    
#    # these would be script params
#    min_cf = 30
#    hts = [50,80]
#    # -filter_table, filter_geom, -filter_where_clause
#    indices = getIndicesByState(cur,['MA'])
#    # -state_abbr_list
#    indices2 = getIndicesByIntersection(cur, 'esri.dtl_state', 'the_geom_4326', "WHERE state_abbr = 'MA'")
#    
#    # find paths through hdf file to the windspeed data
#    paths = getWindspeedPaths(hf,min_cf, hts)
#    
#    global ws_dataset    
#    
#    for path in paths[0:1]:
#        # need to mask by location first
#        pass
#==============================================================================
# 
#==============================================================================
#        ws_dataset = hf.get(path)
#        # determine fill value and scale factor
#        fill_value = ws_dataset.fillvalue
#    
#        # get a small part of the array (first 1000 locations)
#        
#        # need to mask out fill_value
#        # convert to np array, adjusting for scale factor
#        scale_factor = ws_dataset.attrs['scale_factor']
    #    ws_array = np.array(ws_dataset)*scale_factor
    
    #a = np.array(ws_dataset)[0:100]*scale_factor
    #m = np.ma.masked_equal(np.array(ws_dataset),-111)[0:100]*scale_factor
    ## these are different
    #a.mean()
    #cut_a = pcrv.calcPowerArray(a)
    #cut_m= pcrv.calcPowerArray(m)
    #m.mean()
    
    
#    d = getFilteredData(hf,path,range(0,10))
    
#==============================================================================
#     
##==============================================================================
#    global d
#    
#    ws = getFilteredData(hf,path,range(0,10))
#    
#    power = pcrv.calcPowerArray(ws)
#    
#    return ws, power
#==============================================================================
#     
#==============================================================================
    # write function to get the list of indexes based on a spatial intersection
    # write function to keep track of i,j, lat, lng for each index so they cna be output to results too
    # start incorporating this functionality to the main script
    
    
    #t0 = time.time()
    #cut_pwr = pcrv.calcPowerArray(ws_array)
    #print time.time()-t0
    
    
    
    
    # 
    #t0 = time.time()
    #p = np.apply_along_axis(pcrv.calcPowerVec,0,ws_array)
    #print time.time() - t0
    # 54.9 seconds
    
    #t0 = time.time()
    ## create a boolean grid, where 1 values indicated speeds above the cut in speed
    #above_cut_in = ws_array*pcrv.rhoFactor > pcrv.cutIn
    ## create a second boolean grid, where 1 values indicated speeds below the cut out speed
    #below_cut_out = ws_array*pcrv.rhoFactor < pcrv.cutOut
    ## calculate the raw power output (not accounting for cut in and cut out speeds)
    #raw_pwr = pcrv.pwrInterp(ws_array/pcrv.rhoFactor)
    ## adjust for cut in and cut out speeds (set to zero where these are exceeded)
    #cut_pwr = raw_pwr*above_cut_in*below_cut_out
    #print time.time() - t0
    # 1.8 seconds
    
    
    # are they the same
    #np.all(cut_pwr == p)


