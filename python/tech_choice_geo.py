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

def weighted_choice(group, prng, p_col, row_id_col):
    
    # rescale probabilities to sum to one
    p = group[p_col] / group[p_col].sum()
    # if all probabilities are zero, just take the first uid
    if sum(p > 0) == 0:
        sample = None
    else:
        sample = prng.choice(group[row_id_col], 1, False, p)[0]
    
    return sample

def which_max(group, col):
    
    # rescale probabilities to sum to one
    uid_list = group['uid'].tolist()
    max_val = group[col].max()
    i =  group[col].tolist().index(max_val)
    uid = uid_list[i]
    
    return uid

# helper functions
# 1 -- create new unique id column for first selection since agent_id won't work
# 2 - create a decision variable cleanup function
    # Change any negative values or nans to zero
    #df['dv'] = np.where(([decision_col] < 0) | (df[decision_col].isnull()), 0, df[decision_col])


@decorators.fn_timer(logger = logger, tab_level = 3, prefix = '')
def probabilistic_choice(df, prng, uid_col, options_col, excluded_options_col, decision_col, alpha = 2, always_return_one = True):
        
    in_cols = df.columns.tolist()
    # determine the total number of unique ids
    n_uids = len(df[uid_col].unique().tolist())
    n_options = len(df[options_col].unique().tolist())
    n_combos = n_uids * n_options
    n_rows = df.shape[0]
    # make sure the total number of rows matches the product of these two numbers
    if n_rows <> n_combos:
        raise ValueError("Number of rows in dataframe (%s) doesn't match number of expected combinations (%s) based on %s options and %s unique IDs" % (n_rows, n_combos, n_options, n_uids))

    # create a new temporary id column for each row
    df['row_id'] = range(0, n_rows)
    
    # append the apha value
    df['alpha'] = alpha

    # decision variable must not have any nans or < 0
    n_nulls = sum(df[decision_col].isnull())
    n_negatives = sum(df[decision_col] < 0) 
    if n_nulls > 0:
        raise ValueError("decision_col (%s) contains nans. Replace nans with zero and try again." % decision_col)
    if n_negatives > 0:
        raise ValueError("decision_col (%s) contains negative values. Replace negative values with zero and try again."  % decision_col)
        
    # Change any negative values or nans to zero
    df['dv'] = df[decision_col]
    
    # Calculate the exponentiated value, filtering by whether leasing is allowed
    df['exp'] = df['dv']**df['alpha']
    
    # Calculate the total exponentiated values for each group
    gb = df.groupby(uid_col)
    gb = pd.DataFrame({'exp_sum': gb['exp'].sum()})
    
    # Merge the random number and expo values back 
    df = df.merge(gb, left_on = uid_col, right_index = True)
    
    # Determine the probability associated with each choice
    with np.errstate(invalid = 'ignore'):
        df['p'] = np.where(df['exp_sum'] > 0, df['exp'] / df['exp_sum'], 1.)
        
    # hard code probability to zero where specified by the excluded_options_col
    if excluded_options_col is not None:
        df.loc[df[excluded_options_col] == True, 'p'] = 0.

    # Do a weighted random draw by group and return the p-value that was selected
    seed = prng.get_state()[1][0]
    prng.seed(seed)
    selected_row_ids = df.groupby(uid_col).apply(weighted_choice, prng, 'p', 'row_id').reset_index()
    selected_row_ids.columns = [uid_col, 'selected']

    # Filter by the best choice by matching the p-values returned above
    df = pd.merge(df, selected_row_ids, how = 'left', on = uid_col)
    df['selected_option'] = df['row_id'] == df['selected']
   
   
    if always_return_one == True:
        # make sure that one gets returned even if all options are excluded
        # for each unique id, identify the highest value found in the decision_col
        df['dv_rank'] = df[[uid_col, decision_col]].groupby([uid_col]).rank(ascending = True, method = 'first')
        # also identify elements that did not have any row marked as the selected_option
        results_summary_df = df[[uid_col, 'selected_option']].groupby([uid_col]).sum().reset_index()
        # add field indicating if no option was selected
        results_summary_df['no_option_selected'] = results_summary_df['selected_option'] == False
        # drop the selected_option column from summary
        results_summary_df.drop('selected_option', axis = 1, inplace = True)
        # append this information to the main dataframe
        df = pd.merge(df, results_summary_df, how = 'left', on = uid_col)
        # isolate the rows that either were seelcted, or for which there was no selection but they are highest ranked
        return_df = df[(df['selected_option'] == True) | ((df['no_option_selected'] == True) & (df['dv_rank'] == 1))]
        
        # check row count is corrected
        expected_row_count = n_rows / n_options
        return_row_count = return_df.shape[0]
        
        if return_row_count <> expected_row_count:
            raise ValueError("Shape mismatch: output dataframe does not have 1/%s the rows of input dataframe" % n_options)
            sys.exit(-1)
    else:
        # return only the selected options        
        return_df = df[df['selected_option'] == True]
 
     
    out_cols = ['selected_option']   
    return_cols = in_cols + out_cols    
    return_df = return_df[return_cols]    
    
    return return_df


