# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 12:11:46 2015

@author: mgleason
"""
import excel_objects as xl_objects

def get_named_range(workbook, range_name):
    
    # get the named range object
    named_range = workbook.get_named_range(range_name)
    
    # raise an error if the named range doesn't exist
    if named_range == None:
        raise xl_objects.ExcelError('%s named range does not exist.' % range_name)    
    
    return named_range

def get_techs(wb):
    
    techs = {
                'solar' : 'run_solar', 
                'wind' : 'run_wind'
            }

    for tech, rname in techs.iteritems():
        
        x = get_named_range(wb, rname)