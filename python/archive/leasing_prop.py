# -*- coding: utf-8 -*-
"""
Created on Thu Oct 23 15:26:32 2014

@author: bsigrin
"""

def wavg(val_col_name, wt_col_name):
    def inner(group):
        return (group[val_col_name] * group[wt_col_name]).sum() / group[wt_col_name].sum()
    inner.__name__ = 'wtd_avg'
    return inner
    
def assign_business_model(df, alpha = 2):
    ''' Assign a business model (host_owned or tpo) to a customer bin. The assignment is
    based on (i) whether that bin's state permits leasing or buying; (ii) a comparison
    of the weighted meanmax market share for each model. Based on these means, a logit
    function assigns a probability of leasing or buying, which is then randomly simulated.
    
    This function should be applied before calc_economics and after the first 
    year's solve because it uses the market shares calculated in the previous year. 
    
        IN: df - pd pataframe - the main dataframe 
            alpha - float - a scalar in the logit function-- higher alphas make the larger option exponentially more likely
        OUT: df - pd pataframe - the main dataframe w/ an assigned business model
    '''
    
    leasing_df = df.groupby(['state_abbr','business_model']).apply(wavg('max_market_share','customers_in_bin')).reset_index()
    leasing_df['max_market_share'] = leasing_df[0]
    leasing_df = leasing_df.drop(0,axis = 1)

    leasing_df['mkt_exp'] = leasing_df['max_market_share']**alpha
    temp_df = leasing_df.groupby(['state_abbr'])['mkt_exp'].sum().reset_index()
    temp_df.columns = ['state_abbr', 'sum_mkt_exp']
    leasing_df = pd.merge(leasing_df,temp_df,how = 'left', on = ['state_abbr'])
    leasing_df['prob_of_leasing'] = leasing_df['mkt_exp'] /leasing_df['sum_mkt_exp']
    leasing_df = leasing_df[(leasing_df['business_model'] == 'tpo')][['state_abbr','prob_of_leasing']]
    
    # Don't let prob of leasing or buying exceed 95%
    leasing_df['prob_of_leasing'] = np.where(leasing_df['prob_of_leasing'] > 0.95, 0.95,leasing_df['prob_of_leasing'])
    leasing_df['prob_of_leasing'] = np.where(leasing_df['prob_of_leasing'] < 0.05, 0.05,leasing_df['prob_of_leasing'])
    
    df = pd.merge(df, leasing_df, how = 'left', on = ['state_abbr'])
    
    
    # Random assign business model                    
    tmp = df['leasing_allowed'] * (np.random.rand(df.shape[0]) > df.prob_of_leasing)
    df['business_model'] = np.where(tmp,'host_owned','tpo')
    df['metric'] = np.where(tmp,'payback_period','monthly_bill_savings')
    return df


    
#def calc_lease_availability(df,leasing_availability, year,start_year,market_threshold = 0.01):
#    
#    # For the first year start with existing policies
#    if year == start_year:
#        sql = """SELECT state as state_abbr, leasing_allowed FROM diffusion_shared.states_allowing_leasing_in_2013;"""
#        lease_avail = sqlio.read_frame(sql, con, coerce_float = False)
#        lease_avail['leasing_allowed'] = np.where(lease_avail['leasing_allowed'] == 'Not Allowed',False, True)
#        df = pd.merge(df, lease_avail, how = 'left', on = ['state_abbr'])
#    
#    # Makes leasing allowed everywhere    
#    if leasing_availability == 'Full_Leasing_Everywhere':
#        df['leasing_allowed'] = True
#        
#    # Makes leasing allowed nowhere      
#    elif leasing_availability == 'No_Leasing_Anywhere':
#        df['leasing_allowed'] = False
#    
#    # Leasing only in markets defined in first year   
#    elif leasing_availability == 'No_New_Markets':                 
#        pass
#    
#    # Leasing allowed in existing markets, or if the avg. max market share exceed the threshold i.e 1% 
#    elif leasing_availability == 'Market_Threshold':
#        
#        # We need at least one year of calculations. Note that this means new markets cannot unlock until 2016 at earliest.
#        if year != start_year:
#            
#            # Does the state's avg max market share exceed the market_threshold i.e 1%?
#            market_availability = df.groupby(['state_abbr'])['max_market_share'].mean().reset_index()
#            market_availability['leasing_market_availability'] = market_availability['max_market_share'] > market_threshold
#            market_availability = market_availability.drop('max_market_share',axis = 1)
#            
#            # Join with main on state; ignore falses if the state already permits leasing
#            df = pd.merge(df, market_availability, how = 'left', on = ['state_abbr'])
#            df['leasing_allowed'] = np.where(df['leasing_allowed'], True, df['leasing_market_availability'])
#            df = df.drop('leasing_market_availability', axis = 1)
#            
#    return df