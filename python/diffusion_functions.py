
"""
 --- edrury & bsigrin Jan 27 2014
 
     Functions used to calculate diffusion
     
 --- Diffusion calculator - uses: 
    (1) maximum market size as a function of payback time;
    (2) Bass diffusion with diffusion rates (p, q) set by payback time;
    (3) previous diffusion level

    Two methods used:
    (1) direct calculation
    (2) look-up table to speed up the calculation

"""

import matplotlib.pyplot as plt
import numpy as np
import xlrd
from scipy.interpolate import interp1d
import pandas as pd

#=============================================================================
# ^^^^  Bass Diffusion Calculator  ^^^^
def bass_diffusion(p, q, t):
    f=np.e**(-1*(p+q)*t); 
    adopt=(1-f) / (1 + (q/p)*f); # Bass Diffusion - cumulative adoption
    return adopt
    
#=============================================================================

#=============================================================================
# ^^^^ Calculate the 'equivalent time' on a Bass Diffusion curve  ^^^^ 
def calc_equiv_time(M0, Mt, p, q):
    ratio=M0/Mt;  # ratio of adoption at t-1 to adoption at t
    t_eq = np.log( ( 1 - ratio) / (1 + ratio*(q/p))) / (-1*(p+q)); # solve for equivalent time
    return t_eq
    
#=============================================================================

#=============================================================================
# ^^^^ Set parameters  ^^^^ 
def set_param_payback(payback):
    # set p and q values
    p=0.0015;
    if (0<=payback<=3): q=.5;
    elif (3<payback<=10): q=.4;
    else: q=.3;
    return p, q
    
#=============================================================================

#==============================================================================
#  ^^^^  R E A D    E X C E L    D A T A :    ^^^^
def readEXCEL(filename, sheet_name, r0, c0, rn, cn):
    
    # size data output
    data = np.zeros((rn+1,cn+1))
    
    # open workbook & worksheet
    wb=xlrd.open_workbook(filename)
    sh=wb.sheet_by_name(sheet_name)
    
    # read block of rows and columns
    curr_row = -1
    while curr_row < rn:
        curr_row += 1
        curr_cell = -1
        while curr_cell < cn:
            curr_cell += 1
            data[curr_row, curr_cell] = sh.cell_value(curr_row+r0, curr_cell+c0)
    return data		
#==============================================================================

#==============================================================================
#  Create max market share table by segment and interpolate for payback x10
def make_max_market_table(source = 'NAV_NEW'):
    
    # ---------------------- DIFFUSION CURVE SOURCES---------------------------
    # data (31, 6) -> 31 = payback times from 0 - 30 years; 
    #              ->  6 = diffusion curves: 0 - NEMS / A.D. Little - NEW
    #                                        1 - NEMS / A.D. Little - RETROFIT
    #                                        2 - NAVIGANT - NEW
    #                                        3 - NAVIGANT - RETROFIT
    #                                        4 - R.W. BECK - NEW
    #                                        5 - R.W. BECK - RETROFIT
    #
    # DEFAULT IS NAVIGANT NEW
    #--------------------------------------------------------------------------
    
    mm_source = {'NEMS_NEW' : 0, 'NEMS_RETRO' : 1, 'NAV_NEW' : 2, 'NAVS_RETRO' : 3, 'RWBECK_NEW' : 4, 'RWBECK_RETRO' : 5, }
    curve = mm_source[source]
    
    # Read the max market table
    filename = 'MaxMarketShare.xlsx'
    sheet_name = 'MaxMktCurves'
    # Read commercial max market share curves
    r0 = 3;  # --- row offset
    c0 = 2;  # --- column offset
    rn = 31; # --- number of rows
    cn = 6;  # --- number of columns
    max_market_com = readEXCEL(filename, sheet_name, r0, c0, rn-1, cn-1)
    # Read residential max market share curves
    c0 = 10;  # --- column offset
    max_market_res = readEXCEL(filename, sheet_name, r0, c0, rn-1, cn-1)  
 
    # now interpolate data
    yrs=np.linspace(0,30,31);
    yrs2=np.linspace(0,30,301);
    
    Res_Max_Markt = max_market_res[:,curve];
    Com_Max_Markt = max_market_com[:,curve];
    
    f1 = interp1d(yrs, Res_Max_Markt);
    f2 = interp1d(yrs, Com_Max_Markt);
    
    res_max_market = f1(yrs2);
    com_max_market = f2(yrs2);
    
    max_market = pd.DataFrame({'Year' : range(301), 'Res' : res_max_market, 'Com' : com_max_market})
    return(max_market)
#==============================================================================

#==============================================================================
#  ^^^^ Calculate new diffusion in market segment ^^^^
def calc_diffusion(payback,current_market_share,segment,max_market):
    
    payback = max(min(payback,30),0) # Payback defined [0,30] years
    new_max_pen=max_market[segment][np.int(np.round(payback*10))]; # find the new Max Market
    
    # --- Check that 'equivalent year' will give a real answer, then proceed -----
    if current_market_share/new_max_pen > 1:

        new_market_share = current_market_share; # new market adoption
        
    else:
        
        M0 = current_market_share; # calculate the initial market penetration
        Mt = new_max_pen;      # set the new Max market
        p,q  = set_param_payback(payback) 
        teq = calc_equiv_time(M0, Mt, p, q); # find the 'equivalent time' on the newly scaled diffusion curve
        teq2=teq+2; # now step forward two years from the 'new location'
        new_adopt_fraction = bass_diffusion(p, q, teq2); # calculate the new diffusion by stepping forward 2 years
        new_market_share = new_max_pen*new_adopt_fraction; # new market adoption
    
    return(new_market_share)
#==============================================================================    



#### SCRAP CODE ###


## set payback time calculate diffusion parameters based on payback time
#payback=7.17;
#
## Get parameters
#p, q = set_param_payback(payback);
#max_pen=MM_new[np.int(np.round(payback*10))];
#
#if t = 2014:
#    
#   p,q  = set_param_payback(payback)
#   max_pen=MM_new[np.int(np.round(payback*10))];
#   adopt = bass_diffusion(p, q, 2014 - 2005) # Assume diffusion began in 2005
#   market = max_pen*adopt;
#
#else:
#    
#   p,q  = set_param_payback(payback) 
#   diffusion_yr = calc_equiv_time(M0, Mt, p, q)
#   adopt = bass_diffusion(p, q, diffusion_yr)
#   market = max_pen*adopt; 
#
#
## calculate adoption fraction
#diffusion_yr=10.31;
#
#
#adopt = bass_diffusion(p, q, diffusion_yr)
#market = max_pen*adopt;
#
#print ' '
#print '------------------------------------------------------'
#print ' '
#print ' *** first time step *** '
#print ' '
#print '%0.2f yr payback time' %payback
#print '%0.2f yrs diffusing' %diffusion_yr
#print ' '
#
#print ' '
#print '%0.4f max pen for original payback' %max_pen
#print '%0.4f bass diff' %adopt
#print '%0.4f total market' %market
#print ' '

    
##------------  N E X T    T I M E    S T E P :   -----------------------------
#
#payback2=8.2; # set a new payback time
#max_pen2=MM_new[np.int(np.round(payback2*10))]; # find the new Max Market
#
## --- Check that 'equivalent year' will give a real answer, then proceed -----
#if market/max_pen2 > 1:
#    teq=50; # this is far enough out to give full diffusion
#    teq2=50; # this is far enough out to give full diffusion
#    adopt_re = bass_diffusion(p, q, teq); # just to check that the algorithm is working
#    adopt2 = bass_diffusion(p, q, teq2); # calculate the new diffusion by stepping forward 2 years
#    market2 = market; # new market adoption
#else:
#    M0=adopt*max_pen; # calculate the initial market penetration
#    Mt=max_pen2;      # set the new Max market
#    teq = calc_equiv_time(M0, Mt, p, q, t); # find the 'equivalent time' on the newly scaled diffusion curve
#    teq2=teq+2; # now step forward two years from the 'new location'
#    adopt_re = bass_diffusion(p, q, teq); # just to check that the algorithm is working
#    adopt2 = bass_diffusion(p, q, teq2); # calculate the new diffusion by stepping forward 2 years
#    market2 = max_pen2*adopt2; # new market adoption
#
#print ' *** next time step *** '
#print ' '
#print '%0.2f yr new payback time' %payback2
#print '%0.2f yr equivalent diffusion years' %teq
#print ' '
#print '%0.4f max pen for new payback' %max_pen2
#print '%0.4f bass diff yr0' %adopt_re
#print '%0.4f total market yr0' %(max_pen2*adopt_re)
#print '%0.4f bass diff yr+2' %adopt2
#print '%0.4f total market yr+2' %market2
#print ' '
#print '------------------------------------------------------'
#print ' '
#print ' '

## --- Read Max Market Share curves: -------------------------------------------
#filename = 'C:\Anaconda\DWDM\MaxMarketShare.xlsx'
#sheet_name = 'MaxMktCurves'
## Read commercial max market share curves
#r0 = 3;  # --- row offset
#c0 = 2;  # --- column offset
#rn = 31; # --- number of rows
#cn = 6;  # --- number of columns
#MM_com=readEXCEL(filename, sheet_name, r0, c0, rn-1, cn-1)
## Read residential max market share curves
#c0 = 10;  # --- column offset
#MM_res=readEXCEL(filename, sheet_name, r0, c0, rn-1, cn-1)
##-----------------------------------------------------------
## data (31, 6) -> 31 = payback times from 0 - 30 years; 
##              ->  6 = diffusion curves: 0 - NEMS / A.D. Little - NEW
##                                        1 - NEMS / A.D. Little - RETROFIT
##                                        2 - NAVIGANT - NEW
##                                        3 - NAVIGANT - RETROFIT
##                                        4 - R.W. BECK - NEW
##                                        5 - R.W. BECK - RETROFIT
##------------------------------------------------------------------------------
#
## set curve to use
#
#
## check interpolation!
##plt.plot(yrs, y, 'o', yrs2, MM_new, '-')
##plt.show()