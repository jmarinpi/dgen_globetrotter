# -*- coding: utf-8 -*-
"""
Created on Wed Jul 23 11:50:54 2014

@author: mgleason
"""

import numpy as np
import time
from numba import jit




#@jit
#def discount_cfs(cfs,denominators):
#    # calc discounted cfs   
#    discounted_cfs = cfs[:,:,np.newaxis]/denominators
#    return discounted_cfs

def virr(cfs, precision = 0.005, rmin = 0, rmax1 = 0.3, rmax2 = 0.5):
    ''' Vectorized IRR calculator. First calculate a 3D array of the discounted
    cash flows along cash flow series, time period, and discount rate. Sum over time to 
    collapse to a 2D array which gives the NPV along a range of discount rates 
    for each cash flow series. Next, find crossover where NPV is zero--corresponds
    to the lowest real IRR value. For performance, negative IRRs are not calculated
    -- returns "-1", and values are only calculated to an acceptable precision.
    
    IN:
        cfs - numpy 2d array - rows are cash flow series, cols are time periods
        precision - level of accuracy for the inner IRR band eg 0.005%
        rmin - lower bound of the inner IRR band eg 0%
        rmax1 - upper bound of the inner IRR band eg 30%
        rmax2 - upper bound of the outer IRR band. eg 50% Values in the outer 
                band are calculated to 1% precision, IRRs outside the upper band 
                return the rmax2 value
    OUT:
        r - numpy column array of IRRs for cash flow series
        
    M Gleason, B Sigrin - NREL 2014
    '''
    
    if cfs.ndim == 1: 
        cfs = cfs.reshape(1,len(cfs))

    # Range of time periods
    years = np.arange(0,cfs.shape[1])
    
    # Range of the discount rates
    rates_length1 = int((rmax1 - rmin)/precision) + 1
    rates_length2 = int((rmax2 - rmax1)/0.01)
    rates = np.zeros((rates_length1 + rates_length2,))
    rates[:rates_length1] = np.linspace(0,0.3,rates_length1)
    rates[rates_length1:] = np.linspace(0.31,0.5,rates_length2)

    # Discount rate multiplier rows are years, cols are rates
    drm = (1+rates)**-years[:,np.newaxis]

    # Calculate discounted cfs   
    discounted_cfs = cfs[:,:,np.newaxis] * drm
    
    # Calculate NPV array by summing over discounted cashflows
    npv = discounted_cfs.sum(axis = 1)
    
    # Convert npv into boolean for positives (0) and negatives (1)
    signs = npv < 0
    
    # Find the pairwise differences in boolean values
    # sign crosses over, the pairwise diff will be True
    crossovers = np.diff(signs,1,1)
    
    # Extract the irr from the first crossover for each row
    irr = np.min(np.ma.masked_equal(rates[1:]* crossovers,0),1)
    
    # deal with negative irrs
    negative_irrs = cfs.sum(1) < 0
    r = np.where(negative_irrs,-1,irr)
    r = np.where(irr.mask * (negative_irrs == False), 0.5, r)
        
    return r
    

# create an array that is the cfs
cfs = np.zeros((30000,30))
cfs[:,0] = -1000
cfs[:,1:] = 100
cfs[0,1:] = -100
cfs[1,:] = 100
#cf_stack = np.dstack([cfs]*rate_steps)

for o in range(0,10):
    t0 = time.time()
    i = irr(cfs)
    print time.time() - t0

#t0 = time.time()
#i = irr(cfs)
#print time.time() - t0

t0 = time.time()
out = []
for x in cfs:
    out.append(np.irr(x))
print time.time() - t0