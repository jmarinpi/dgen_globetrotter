# -*- coding: utf-8 -*-
"""
Created on Thu Sep 10 15:20:11 2015

@author: bsigrin
"""

import pandas as pd
import numpy as np
import sys


def weighted_choice(group, prng):
    
    # rescale probabilities to sum to one
    p = group['p']/group['p'].sum()
    sample = prng.choice(group['uid'], 1, False, p)[0]
    
    return sample

def which_max_npv4(group):
    
    # rescale probabilities to sum to one
    uid_list = group['uid'].tolist()
    max_npv = group['npv4'].max()
    i =  group['npv4'].tolist().index(max_npv)
    uid = uid_list[i]
    
    return uid

def select_financing_and_tech(df, prng, alpha_lkup, choose_tech = False, techs = ['solar', 'wind']):
        
    
    in_columns = df.columns.tolist()

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
  
    # Change any negative npvs to zero
    df['npv'] = np.where(df['npv4'] < 0, 0, df['npv4'])
    
    # Calculate the exponentiated value, filtering by whether leasing is allowed
    df['mkt_exp'] = df['npv']**df['alpha']
    df.loc[(df['business_model'] == 'tpo') & (df['leasing_allowed'] == False),'mkt_exp'] = 0 #Restrict leasing if not allowed by state
    
    # Calculate the total exponentiated values for each group
    gb = df.groupby(group_by_cols)
    gb = pd.DataFrame({'mkt_sum': gb['mkt_exp'].sum()})
    
    # Merge the random number and expo values back 
    df = df.merge(gb, left_on = group_by_cols, right_index = True)
    
    # Determine the probability of adopting
    with np.errstate(invalid = 'ignore'):
        df['p'] = np.where(df['mkt_sum'] > 0, df['mkt_exp']/df['mkt_sum'], np.where((df['business_model'] == 'tpo') & (df['leasing_allowed'] == False), 0., 1.))
    
    # Do a weighted random draw by group and return the p-value that was selected
    # Note: If choose_tech = False, it's necessary to split up the dataframe by technology
    # and re-initialize the random seed for each tech. This ensures that results will be consistent
    # for each technology regardless of whether the model is run with multiple techs or just
    # a single tech.
    if choose_tech == True:
        selected_uids = df.groupby(group_by_cols).apply(weighted_choice, prng).reset_index()
        selected_uids.columns = group_by_cols + ['best']
    else:
        selected_uids_list = []
        # get the initial seed
        seed = prng.get_state()[1][0]
        for tech in techs:
            prng.seed(seed)
            # split by technologies
            tech_df = df[df['tech'] == tech]
            selected_tech_uids = tech_df.groupby(group_by_cols).apply(weighted_choice, prng).reset_index()
            selected_tech_uids.columns = group_by_cols + ['best']
            selected_uids_list.append(selected_tech_uids)
        selected_uids = pd.concat(selected_uids_list, axis = 0, ignore_index = True)
    
    # Filter by the best choice by matching the p-values returned above
    df_selected = df.merge(selected_uids, left_on = group_by_cols + ['uid'], right_on = group_by_cols + ['best'], how = 'outer')
    df_selected['selected_option'] = df_selected.best.isnull() == False
    
    if choose_tech == False:
        # return only the selected options
        return_df =  df_selected[df_selected['selected_option'] == True].sort(columns = ['county_id', 'bin_id', 'tech'])       
    else:
        # isolate the selected options
        selected_techs = df_selected[df_selected['selected_option'] == True].groupby(['county_id', 'bin_id', 'tech']).size().reset_index()
        selected_techs.columns = ['county_id', 'bin_id', 'tech', 'selected_tech']
        # identify which technology was not selected for each agent
        unselected_techs = df_selected.merge(selected_techs, on = ['county_id', 'bin_id', 'tech'], how = 'outer')
        unselected_techs = unselected_techs[unselected_techs.selected_tech.isnull()]
        # rank the remainders by npv4
        best_unselected_tech = unselected_techs.groupby(['county_id', 'bin_id']).apply(which_max_npv4).reset_index()
        best_unselected_tech.columns = ['county_id', 'bin_id', 'best_alternative']
        best_unselected_tech.drop(['county_id', 'bin_id'], axis = 1, inplace = True)
        df_selected_and_unselected = df_selected.merge(best_unselected_tech, left_on = ['uid'], right_on = ['best_alternative'], how = 'outer')      
        df_selected_and_unselected['best_unselected'] = df_selected_and_unselected.best_alternative.isnull() == False 
        return_df = df_selected_and_unselected[(df_selected_and_unselected.selected_option == True) | (df_selected_and_unselected.best_unselected == True)]
        # check row count matches the input df/2
        if return_df.shape[0] <> df.shape[0]/2:
            raise ValueError("Shape mismatch: output dataframe does not have 1/2 the rows of input dataframe")
            sys.exit(-1)
        # check that the number of selected and unselected options both = 1/2 the shape of the input df
        num_selected = sum(return_df['selected_option'] == True)
        num_unselected = sum(return_df['selected_option'] <> False)
        if num_selected <> df.shape[0]/4:
            raise ValueError("The number of selected options is not equal to 1/2 of the input data frame")
            sys.exit(-1)            
        if num_unselected <> df.shape[0]/4:
            raise ValueError("The number of unselected options is not equal to 1/2 of the input data frame")
            sys.exit(-1)   
        # make sure there are no rows that are marked as both selected_option and best_unselected
        if np.any((return_df.selected_option == True) & (return_df.best_unselected == True)):
            raise ValueError("There are some options that are marked as both selected and unselected")
            sys.exit(-1)
            
    # subset the columns to return
    return_df = return_df[in_columns + ['selected_option']].sort(columns = ['county_id', 'bin_id', 'tech'])      
    
    
    return return_df
    
if __name__ == '__main__': 
    from config import alpha_lkup
    from pandas.util.testing import assert_frame_equal
    
#    in_df = pd.read_csv('/Users/mgleason/NREL_Projects/github/diffusion/python/test_data.csv')
#    in_df['leasing_allowed'] = True

    in_df = pd.read_csv('/Users/mgleason/Desktop/df.csv')

    
    df_solar = in_df[in_df.tech == 'solar'].copy()
    df_wind = in_df[in_df.tech == 'wind'].copy()
    
    prng = np.random.RandomState(1234)
    b = select_financing_and_tech(in_df, prng, alpha_lkup, choose_tech = False, techs = ['solar', 'wind'])
    
    prng = np.random.RandomState(1234)
    br = select_financing_and_tech(in_df, prng, alpha_lkup, choose_tech = False, techs = ['wind', 'solar'])
    
    prng = np.random.RandomState(1234)
    w = select_financing_and_tech(df_wind, prng, alpha_lkup, choose_tech = False, techs = ['wind'])
    
    prng = np.random.RandomState(1234)
    s = select_financing_and_tech(df_solar, prng, alpha_lkup, choose_tech = False, techs = ['solar'])
    
    
    bw = b[b.tech == 'wind']
    bs = b[b.tech == 'solar']
    
    try:
        assert_frame_equal(bs.reset_index(drop = True), s.reset_index(drop = True))
        print True
    except Exception:
        print False
        
    try:
        assert_frame_equal(bw.reset_index(drop = True), w.reset_index(drop = True))
        print True
    except Exception:
        print False



    #np.all(b.reset_index(drop = True) == br.reset_index(drop = True))

#    bs.to_csv('/Users/mgleason/Desktop/bs.csv', index = False)
#    s.to_csv('/Users/mgleason/Desktop/s.csv', index = False)
#    w.to_csv('/Users/mgleason/Desktop/w.csv', index = False)
#    bw.to_csv('/Users/mgleason/Desktop/bw.csv', index = False)

