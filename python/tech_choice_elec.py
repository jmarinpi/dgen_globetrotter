# -*- coding: utf-8 -*-
"""
Created on Thu Sep 10 15:20:11 2015

@author: bsigrin
"""

import pandas as pd
import numpy as np
import sys
import decorators
import utility_functions as utilfunc

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================

def weighted_choice(group, prng):
    
    # rescale probabilities to sum to one
    p = group['p']/group['p'].sum()
    sample = prng.choice(group['uid'], 1, False, p)[0]
    
    return sample

def which_max(group, col):
    
    # rescale probabilities to sum to one
    uid_list = group['uid'].tolist()
    max_val = group[col].max()
    i =  group[col].tolist().index(max_val)
    uid = uid_list[i]
    
    return uid

@decorators.fn_timer(logger = logger, tab_level = 3, prefix = '')
def select_financing_and_tech(df, prng, sectors, decision_col, choose_tech = False, techs = ['solar', 'wind'], alpha = 2):
        
    if choose_tech == True:
        msg = "\t\tSelecting Financing Option and Technology"
    else:
        msg = "\t\tSelecting Financing Option"
        
    logger.info(msg)    
    
    in_columns = df.columns.tolist()
    
    # check each customer bin + tech + sector_abbr has two business models
    test = df.groupby(['county_id', 'bin_id', 'sector_abbr', 'tech'])['business_model'].count().reset_index()
    if np.any(test.iloc[:, 4] <> 2):
        raise ValueError("Incorrect number of business models for each customer bin")
        sys.exit(-1)
        
    # check each customer bin + business model + sector has the correct nmber of techs
    test = df.groupby(['county_id', 'bin_id', 'sector_abbr', 'business_model'])['tech'].count().reset_index()
    if np.any(test.iloc[:, 4] <> len(techs)):
        raise ValueError("Incorrect number of techs for each customer bin")
        sys.exit(-1)   
    
    df['uid'] = range(0, df.shape[0])
    df['alpha'] = alpha
  
    if choose_tech == True:
        group_by_cols = ['county_id', 'bin_id', 'sector_abbr']
    else:
        group_by_cols = ['county_id', 'bin_id', 'sector_abbr', 'tech']
  
    # Change any negative values to zero
    df['dv'] = np.where(df[decision_col] < 0, 0, df[decision_col])
    
    # Calculate the exponentiated value, filtering by whether leasing is allowed
    df['mkt_exp'] = df['dv']**df['alpha']
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
    seed = prng.get_state()[1][0]
    selected_uids_list = []
    for sector_abbr, sector in sectors.iteritems():
        sector_df = df[df['sector_abbr'] == sector_abbr]
        prng.seed(seed)
        if choose_tech == True:
            selected_uids = sector_df.groupby(group_by_cols).apply(weighted_choice, prng).reset_index()
            selected_uids.columns = group_by_cols + ['best']
            selected_uids_list.append(selected_uids)
        else:
            for tech in techs:
                prng.seed(seed)
                # split by technologies
                tech_df = sector_df[sector_df['tech'] == tech]
                selected_tech_uids = tech_df.groupby(group_by_cols).apply(weighted_choice, prng).reset_index()
                selected_tech_uids.columns = group_by_cols + ['best']
                selected_uids_list.append(selected_tech_uids)
    selected_uids = pd.concat(selected_uids_list, axis = 0, ignore_index = True)
    
    # Filter by the best choice by matching the p-values returned above
    df_selected = df.merge(selected_uids, left_on = group_by_cols + ['uid'], right_on = group_by_cols + ['best'], how = 'outer')
    df_selected['selected_option'] = df_selected.best.isnull() == False
    
    if choose_tech == False:
        # return only the selected options
        return_df =  df_selected[df_selected['selected_option'] == True].sort(columns = ['county_id', 'bin_id', 'sector_abbr', 'tech'])       
    else:
        # isolate the selected options
        selected_techs = df_selected[df_selected['selected_option'] == True].groupby(['county_id', 'bin_id', 'sector_abbr', 'tech']).size().reset_index()
        selected_techs.columns = ['county_id', 'bin_id', 'sector_abbr', 'tech', 'selected_tech']
        # identify which technology was not selected for each agent
        unselected_techs = df_selected.merge(selected_techs, on = ['county_id', 'bin_id', 'sector_abbr', 'tech'], how = 'outer')
        unselected_techs = unselected_techs[unselected_techs.selected_tech.isnull()]
        # rank the remainders by mms
        best_unselected_tech = unselected_techs.groupby(['county_id', 'bin_id', 'sector_abbr',]).apply(which_max, decision_col).reset_index()
        best_unselected_tech.columns = ['county_id', 'bin_id', 'sector_abbr', 'best_alternative']
        best_unselected_tech.drop(['county_id', 'bin_id', 'sector_abbr'], axis = 1, inplace = True)
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
    return_df = return_df[in_columns + ['selected_option']].sort(columns = ['county_id', 'bin_id', 'sector_abbr', 'tech'])      
    
    
    return return_df
    
