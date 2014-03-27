"""
Name: diffusion_functions
Purpose: Contains functions to calculate diffusion of distributed wind model

    (1) Determine maximum market size as a function of payback time;
    (2) Parameterize Bass diffusion curve with diffusion rates (p, q) set by 
        payback time;
    (3) Determine current stage (equivaluent time) of diffusion based on existing 
        market and current economics 
    (3) Calculate new market share by stepping forward on diffusion curve.


Author: bsigrin & edrury
Last Revision: 3/26/14

"""

import numpy as np
import pandas as pd

#=============================================================================
# ^^^^  Bass Diffusion Calculator  ^^^^
def bass_diffusion(p, q, t):
    ''' Calculate the fraction of population that diffuse into the max_market_share.
        Note that this is different than the fraction of population that have/
        will adopt.

        IN: p,q - numpy arrays - Bass diffusion parameters
            t - numpy array - Number of years since diffusion began
            
            
        OUT: new_adopt_fraction - numpy array - fraction of overall population 
                                                that will adopt the technology
    '''
    f = np.e**(-1*(p+q)*t); 
    new_adopt_fraction = (1-f) / (1 + (q/p)*f); # Bass Diffusion - cumulative adoption
    return new_adopt_fraction
    
#=============================================================================

#=============================================================================
def calc_equiv_time(msly, mms, p, q):
    ''' Calculate the "equivalent time" on the diffusion curve. This defines the
    gradient of adoption.

        IN: msly - numpy array - market share last year [at end of the previous solve] as decimal
            mms - numpy array - maximum market share as decimal
            p,q - numpy arrays - Bass diffusion parameters
            
        OUT: t_eq - numpy array - Equivalent number of years after diffusion 
                                  started on the diffusion curve
    '''
    ratio=msly/mms;  # ratio of adoption at present to adoption at terminal period
    t_eq = np.log( ( 1 - ratio) / (1 + ratio*(q/p))) / (-1*(p+q)); # solve for equivalent time
    return t_eq
    
#=============================================================================

#=============================================================================
def set_param_payback(payback_period,pval = 0.0015):
    ''' Set the p & q parameters which define the Bass diffusion curve.
    p is the coefficient of innovation, external influence or advertising effect. 
    q is the coefficient of imitation, internal influence or word-of-mouth effect.

        IN: payback_period - numpy array - payback in years
        OUT: p,q - numpy arrays - Bass diffusion parameters
    '''
    # set p and q values
    p = np.array([pval] * payback_period.size);
    q = np.where(payback_period <= 3, 0.5, np.where((payback_period <=10) & (payback_period > 3), 0.4, 0.3))

    return p, q
    
#=============================================================================

#==============================================================================
#  ^^^^ Calculate new diffusion in market segment ^^^^
def calc_diffusion(payback_period,max_market_share, market_share_last_year):
    ''' Calculate the fraction of overall population that have adopted the 
        technology in the current period. Note that this does not specify the 
        actual new adoption fraction without knowing adoption in the previous period. 

        IN: payback_period - numpy array - payback in years
            max_market_share - numpy array - maximum market share as decimal
            current_market_share - numpy array - current market share as decimal
                        
        OUT: new_market_share - numpy array - fraction of overall population 
                                                that have adopted the technology
    '''
    payback_period = np.maximum(np.minimum(payback_period,30),0) # Payback defined [0,30] years        
    p,q  = set_param_payback(payback_period) 
    teq = calc_equiv_time(market_share_last_year, max_market_share, p, q); # find the 'equivalent time' on the newly scaled diffusion curve
    teq2 = teq + 2; # now step forward two years from the 'new location'
    new_adopt_fraction = bass_diffusion(p, q, teq2); # calculate the new diffusion by stepping forward 2 years
    market_share = max_market_share * new_adopt_fraction; # new market adoption    
    market_share = np.where(market_share_last_year/max_market_share > 1, market_share_last_year, market_share)
    
    return market_share
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

##==============================================================================
##  Create max market share table by segment and interpolate for payback x10
#def make_max_market_table(source = 'NAV_NEW'):
#    
#    # ---------------------- DIFFUSION CURVE SOURCES---------------------------
#    # data (31, 6) -> 31 = payback times from 0 - 30 years; 
#    #              ->  6 = diffusion curves: 0 - NEMS / A.D. Little - NEW
#    #                                        1 - NEMS / A.D. Little - RETROFIT
#    #                                        2 - NAVIGANT - NEW
#    #                                        3 - NAVIGANT - RETROFIT
#    #                                        4 - R.W. BECK - NEW
#    #                                        5 - R.W. BECK - RETROFIT
#    #
#    # DEFAULT IS NAVIGANT NEW
#    #--------------------------------------------------------------------------
#    
#    mm_source = {'NEMS_NEW' : 0, 'NEMS_RETRO' : 1, 'NAV_NEW' : 2, 'NAVS_RETRO' : 3, 'RWBECK_NEW' : 4, 'RWBECK_RETRO' : 5, }
#    curve = mm_source[source]
#    
#    # Read the max market table
#    filename = 'MaxMarketShare.xlsx'
#    sheet_name = 'MaxMktCurves'
#    # Read commercial max market share curves
#    r0 = 3;  # --- row offset
#    c0 = 2;  # --- column offset
#    rn = 31; # --- number of rows
#    cn = 6;  # --- number of columns
#    max_market_com = readEXCEL(filename, sheet_name, r0, c0, rn-1, cn-1)
#    # Read residential max market share curves
#    c0 = 10;  # --- column offset
#    max_market_res = readEXCEL(filename, sheet_name, r0, c0, rn-1, cn-1)  
# 
#    # now interpolate data
#    yrs=np.linspace(0,30,31);
#    yrs2=np.linspace(0,30,301);
#    
#    Res_Max_Markt = max_market_res[:,curve];
#    Com_Max_Markt = max_market_com[:,curve];
#    
#    f1 = interp1d(yrs, Res_Max_Markt);
#    f2 = interp1d(yrs, Com_Max_Markt);
#    
#    res_max_market = f1(yrs2);
#    com_max_market = f2(yrs2);
#    
#    max_market = pd.DataFrame({'Year' : range(301), 'Res' : res_max_market, 'Com' : com_max_market})
#    return(max_market)
##==============================================================================

##==============================================================================
##  ^^^^  R E A D    E X C E L    D A T A :    ^^^^
#def readEXCEL(filename, sheet_name, r0, c0, rn, cn):
#    
#    # size data output
#    data = np.zeros((rn+1,cn+1))
#    
#    # open workbook & worksheet
#    wb=xlrd.open_workbook(filename)
#    sh=wb.sheet_by_name(sheet_name)
#    
#    # read block of rows and columns
#    curr_row = -1
#    while curr_row < rn:
#        curr_row += 1
#        curr_cell = -1
#        while curr_cell < cn:
#            curr_cell += 1
#            data[curr_row, curr_cell] = sh.cell_value(curr_row+r0, curr_cell+c0)
#    return data		
##==============================================================================