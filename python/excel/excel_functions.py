# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 12:11:46 2015

@author: mgleason
"""
import excel_objects as xlo
import openpyxl as xl


def get_techs(wb):
    
    techs = {
                'solar' : 'run_solar', 
                'wind' : 'run_wind'
            }

    techs_enabled = {}
    for tech, range_name in techs.iteritems():
        
        nr = xlo.FancyNamedRange(wb, range_name)
        techs_enabled[tech] = nr.first_value()
        
    return techs_enabled
        
    

if __name__ == '__main__':
    
    xls_file = '/Users/mgleason/NREL_Projects/github/diffusion/excel/scenario_inputs.xlsm'
    wb = xl.load_workbook(xls_file, data_only = True)
#    fnr = xlo.FancyNamedRange(wb, 'run_solar')
    techs_enabled = get_techs(wb)