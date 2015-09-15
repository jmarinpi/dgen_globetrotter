# -*- coding: utf-8 -*-
"""
Created on Thu Sep 10 15:20:11 2015

@author: bsigrin
"""

import pandas as pd
import numpy as np
import sys



def weighted_choice(group, prng):
    
    sample = prng.choice(group['uid'], 1, False, group['p'])[0]
    
    return sample
   

df = pd.read_csv('/Users/mgleason/NREL_Projects/github/diffusion/python/test_data.csv')
prng = np.random.RandomState(1234)
df['leasing_allowed'] = True

alpha_lkup = pd.DataFrame({'tech' : ['solar','solar','wind','wind'],
                           'business_model' : ['ho','tpo','ho','tpo'],
                            'alpha' : [2,2,2,2]

})


def system_choice(df, prng, alpha_lkup, choose_tech = False, techs = ['solar', 'wind']):
    


    # check each customer bin has four entries
    test = df.groupby(['county_id', 'bin_id'])['npv4'].count().reset_index()
    if np.any(test.iloc[:,2] <> len(techs) * 2):
        raise ValueError("Incorrect number of entries for each customer bin")
        sys.exit(-1)
    
    # check each customer bin + tech has two business models
    test = df.groupby(['county_id', 'bin_id', 'tech'])['business_model'].count().reset_index()
    if np.any(test.iloc[:,3] <> 2):
        raise ValueError("Incorrect number of business models for each customer bin")
        sys.exit(-1)
        
     # check each customer bin + business model has the correct nmber of techs
    test = df.groupby(['county_id', 'bin_id', 'business_model'])['tech'].count().reset_index()
    if np.any(test.iloc[:,3] <> len(techs)):
        raise ValueError("Incorrect number of techs for each customer bin")
        sys.exit(-1)
        
    
    df['uid'] = range(0, df.shape[0])
    df = df.merge(alpha_lkup)
    #npv may be negative so, if the alpha is even, it won't rank the npvs properly eg. (-100)^2 > 5^2
    #we need to rescale the npvs to the range observed in the data
  
    if choose_tech == True:
        group_by_cols = ['county_id', 'bin_id']
    else:
        group_by_cols = ['county_id', 'bin_id', 'tech']
  
    # Find the minimum and maxmimum npv by group
    gb_min = df.groupby(group_by_cols)['npv4'].min().reset_index()
    gb_min.columns = group_by_cols + ['min_npv']
    gb_max = df.groupby(group_by_cols)['npv4'].max().reset_index()
    gb_max.columns = group_by_cols + ['max_npv']
    
    # Rescale the npv (if there are negative npvs, the scale won't be [0,1])
    df = df.merge(gb_min).merge(gb_max)
    df['npv'] = (df['npv4'] - df['min_npv'])/(df['max_npv'] - df['min_npv'])
    
    # Calculate the exponentiated value, filtering by whether leasing is allowed
    df['mkt_exp'] = df['npv']**df['alpha']
    df.loc[(df['business_model'] == 'tpo') & ~(df['leasing_allowed']),'mkt_exp'] = 0 #Restrict leasing if not allowed by state
    
    # Calculate the total exponentiated values for each group
    gb = df.groupby(group_by_cols)
    gb = pd.DataFrame({'mkt_sum': gb['mkt_exp'].sum()})
    
    # Merge the random number and expo values back 
    df = df.merge(gb, left_on= group_by_cols, right_index = True)
    
    # Determine the probability of adopting
    with np.errstate(invalid = 'ignore'):
        df['p'] = np.where(df['mkt_sum'] == 0, 0, df['mkt_exp']/df['mkt_sum'])
    
    # Do a weighted random draw by group and return the p-value that was selected
    selected_uids = df.groupby(group_by_cols).apply(weighted_choice, prng).reset_index()
    selected_uids.columns = group_by_cols + ['best']
    
    # Filter by the best choice by matching the p-values returned above
    df_selected = df.merge(selected_uids, left_on = group_by_cols + ['uid'], right_on = group_by_cols + ['best'])
    return_df = df_selected[['county_id', 'bin_id', 'tech', 'business_model']].sort(columns = ['county_id', 'bin_id', 'tech'])
    
    return return_df