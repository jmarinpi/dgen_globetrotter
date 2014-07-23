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

def irr(cfs):

    # create an array that is the years
    years = np.arange(0,cfs.shape[1])
    
    # create an array that is the rates
    rates = np.zeros((81,))
    rates[:61] = np.linspace(0,0.3,61)
    rates[61:] = np.linspace(0.31,0.5,20)

    # rows are years, cols are rates, shape is 30,140
    denominators = (1+rates)**years[:,np.newaxis]

    # calc discounted cfs   
#    discounted_cfs = discount_cfs(cfs,denominators)
    discounted_cfs = cfs[:,:,np.newaxis]/denominators
    
    # summarize npv
    npv = discounted_cfs.sum(1)
    
    # convert npv into boolean for positives (0) and negatives (1)
    signs = npv < 0
    
    # find the pairwise differences in boolean values
    # sign crosses over, the pairwise diff will be True
    crossovers = np.diff(signs,1,1)
    
    # extract the irr from the first crossover for each row
    irr = np.min(np.ma.masked_equal(rates[1:]* crossovers,0),1)
    
    # deal with negative irrs
    negative_irrs = cfs.sum(1) < 0
    irr_adjusted = np.where(negative_irrs,-1,irr)
    irr_adjusted_2 = np.where(irr.mask * (negative_irrs == False), 0.5, irr_adjusted)
        
    
    return irr_adjusted_2
    

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