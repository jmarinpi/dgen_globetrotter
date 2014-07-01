# -*- coding: utf-8 -*-
"""
Created on Mon Jun 16 09:27:45 2014

@author: bsigrin
"""

import numpy as np
import pandas as pd
import sys
import gdxcc
import os
import dg_wind_model
reload(dg_wind_model)

#print 'length of arguments: %s' %len(sys.argv)
#script_name = sys.argv[0]
#year = sys.argv[1]
#reeds_path = sys.argv[2]
#gams_path = sys.argv[3]


def main():

#marg_pv_curtail = pd.read_csv('marg_pv_curtail_%s.csv' % year)
#reeds_elec_price = pd.read_csv('elec_price_%s.csv' % year)



#        
#    
#    ''' Run full data prep and model for 2014
#    return: -> installed capacity grouped by n
#    '''
#elif year > 2014:
#    
#    
#    ''' Run for one year only
#    return: -> installed capacity grouped by n
    os.chdir('SolarDS/python')
    df = dg_wind_model.main(mode = 'ReEDS', resume_year = 2014)
    df = df[(df['year'] == 2014)]
    installed_capacity = 0.001* df.groupby('pca_reg')['installed_capacity'].sum() #output currently in kW
    installed_capacity = installed_capacity.to_dict()
    make_1d_gdx(installed_capacity, name = 'installed_capacity', savepath = "installed_capacity.gdx")
#if year == 2014:

def make_1d_gdx(symbol, name = None, savepath = None, sysDir = 'C:/GAMS/win64/24.1') :   
    
    # savepath should be passed as full file path i.e. "/ReEDS/installed_capacity.gdx'
    if savepath is None:
        gdxFileOut = 'SolarDSOutput.gdx'
    else:
        gdxFileOut = savepath
    if name is None:
        name = 'Unknown_Symbol'


        
    value = gdxcc.doubleArray(5)
    dim = 1

    # prepare gdx file
    gdxHandleOut = gdxcc.new_gdxHandle_tp()
    assert gdxcc.gdxCreateD(gdxHandleOut, sysDir, gdxcc.GMS_SSSIZE)[0]
    assert gdxcc.gdxOpenWrite(gdxHandleOut, gdxFileOut, "")[0]
    
    assert gdxcc.gdxDataWriteStrStart(gdxHandleOut, name, "", dim, gdxcc.GMS_DT_PAR, 0)

    for dim in symbol :

            keys = ['%s' % (dim)]
            value[gdxcc.GMS_VAL_LEVEL] = [symbol[dim], 0.0, 0.0, 0.0, 0.0][gdxcc.GMS_VAL_LEVEL]
            gdxcc.gdxDataWriteStr(gdxHandleOut, keys, value)                                        

    assert not gdxcc.gdxClose(gdxHandleOut)
    assert gdxcc.gdxFree(gdxHandleOut)
    print 'GDX Write: %s successful' %gdxFileOut

def make_2d_gdx(symbol, name, sysDir = 'C:/GAMS/win64/24.1') :   
    
    gdxFileOut = name + '.gdx'        
    value = gdxcc.doubleArray(5)
    dim = 2

    # prepare gdx file
    gdxHandleOut = gdxcc.new_gdxHandle_tp()
    assert gdxcc.gdxCreateD(gdxHandleOut, sysDir, gdxcc.GMS_SSSIZE)[0]
    assert gdxcc.gdxOpenWrite(gdxHandleOut, gdxFileOut, "")[0]
    
    assert gdxcc.gdxDataWriteStrStart(gdxHandleOut, name, "", dim, gdxcc.GMS_DT_PAR, 0)

    for p in pca :
        for t in ts:
            keys = ['%s' % (p), '%s' % (t)]
            value[gdxcc.GMS_VAL_LEVEL] = [d[p][t], 0.0, 0.0, 0.0, 0.0][gdxcc.GMS_VAL_LEVEL]
            gdxcc.gdxDataWriteStr(gdxHandleOut, keys, value)                                        

    assert not gdxcc.gdxClose(gdxHandleOut)
    assert gdxcc.gdxFree(gdxHandleOut)
    print 'GDX Write: %s successful' %gdxFileOut
    
if __name__ == '__main__':
    main()