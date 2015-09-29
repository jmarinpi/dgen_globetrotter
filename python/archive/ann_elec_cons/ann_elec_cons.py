# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 16:50:40 2014
Script to create binned distributions of annual electricity consumption (kWh/yr)
from RECS, CBECS, & MECS EIA microdata samples

IN: RECS, CBECS, & MECS data,
    (n): number of bins to resample at 
    
OUT: binned distribution:
    ann_cons_kwh: Annual electricity consumption (kWh) for all population in the bin
    prob: Probability that any population, given sector and region, will be located in the bin
    region: Census region
    sector: Res, Com, Ind.
    weight: see prob.
        
Note: Number of bins cannot exceed microsample points. For RECS & CBECS this is about 2000 & 800
      For MECS, it is 83. 
@author: bsigrin
"""

import pandas as pd
import numpy as np

def wavg(group):
    d = group['con']
    w = group['wt']
    return (d * w).sum() / w.sum()

for n in [10, 50, 100, 500]: 
    
    # Make Residential distribution    
    df = pd.read_csv('data/recs2009.csv')
    df['con'] = df['KWH']
    df['wt'] = df['NWEIGHT']
    df['region'] = df['REGIONC']
    
    # Drop rows with NAs
    df = df[df['con'].notnull()]
    df = df[df['wt'].notnull()]
    df = df[df['region'].notnull()]
    
    # For each region cut consumption along n quantiles, then find probability of 
    # being in that bin. Output is n-bin distribuion of consumption and prob. per
    # region 
    recs = pd.DataFrame()
    for r in df['region'].unique():
        tmp = df[df['region'] == r]
        n0 = min(n, len(tmp))
        tmp['cat'] = pd.qcut(tmp['con'],n0)  # Cut consumption into n quantile bins  
        grouped = tmp.groupby('cat') # Groupby consumtpion quantile
        g1 = grouped.apply(wavg) # Weighted-mean consumption for each bin
        g2 = grouped['wt'].sum() # Sum of weights in the bin
        g = pd.concat([g1, g2],axis = 1) 
        g.columns = ['ann_cons_kwh','weight']
        g['prob'] = g['weight']/g["weight"].sum() # Convert weights to probability
        g['region'] = r
        recs = recs.append(g)
    recs['sector'] = 'residential'
        
    # Make Commercial distribution    
    df = pd.read_csv('data/cbecs_file15.csv')
    df['con'] = df['ELCNS8']
    df['wt'] = df['ADJWT8']
    df['region'] = df['REGION8']
    
    # Drop rows with NAs
    df = df[df['con'].notnull()]
    df = df[df['wt'].notnull()]
    df = df[df['region'].notnull()]
    
    cbecs = pd.DataFrame()
    for r in df['region'].unique():
        tmp = df[df['region'] == r]
        n0 = min(n, len(tmp))        
        tmp['cat'] = pd.qcut(tmp['con'],n0)  # Cut consumption into n quantile bins  
        grouped = tmp.groupby('cat') # Groupby consumtpion quantile
        g1 = grouped.apply(wavg) # Weighted-mean consumption for each bin
        g2 = grouped['wt'].sum() # Sum of weights in the bin
        g = pd.concat([g1, g2],axis = 1) 
        g.columns = ['ann_cons_kwh','weight']
        g['prob'] = g['weight']/g["weight"].sum() # Convert weights to probability
        g['region'] = r
        cbecs = cbecs.append(g)
    cbecs['sector'] = 'commercial'

    # Make Industrial distribution
    # Read in Manufacturing distribution (preprocessed, limited flexibility with # of bins)
    df = pd.read_csv('data/mecs_discrete_elec_cons_distr.csv')
    df['con'] = df['ann_cons_kwh']
    df['wt'] = df['prob']
    
    n0 = min(n, len(df))
    df['cat'] = pd.qcut(df['con'],n0)
    grouped = df.groupby('cat') # Groupby consumtpion quantile
    g1 = grouped.apply(wavg) # Weighted-mean consumption for each bin
    g2 = grouped['prob'].sum() # Sum of weights in the bin
    df = pd.concat([g1, g2],axis = 1) 
    df.columns = ['ann_cons_kwh','prob']
    
    mecs = pd.DataFrame()
    for i in [1,2,3,4]:
        tmp = df.copy()
        tmp['region'] = i
        tmp['weight'] = tmp['prob']*n0
        tmp['sector'] = 'industry'
        mecs = mecs.append(tmp)
        
    out = recs
    out = out.append(cbecs, ignore_index = True)
    out = out.append(mecs, ignore_index = True)
    
    out.to_csv('annual_load_kwh_' + str(n) +'_bins.csv')
