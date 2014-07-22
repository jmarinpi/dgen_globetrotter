"""
Name: financial_functions
Purpose: Contains functions to calculate financial values of distributed wind model

Author: bsigrin
Last Revision: 3/24/14

"""

import numpy as np
import pandas as pd
from collections import Iterable
import data_functions as datfunc
import time



#==============================================================================
def calc_economics(df, sector, sector_abbr, market_projections, market_last_year, financial_parameters, cfg, scenario_opts, max_market_share, cur, con, year, dsire_incentives, deprec_schedule, logger, rate_escalations):
    '''
    Calculates economics of system adoption (cashflows, payback, irr, etc.)
    
        IN:
            Lots    
        
        OUT:
            df - pd dataframe - main dataframe with econ outputs appended as columns
    '''
    
    df['sector'] = sector.lower()
    
    df = datfunc.calc_expected_rate_escal(df,rate_escalations, year)
    #df = pd.merge(df,market_projections[['year', 'customer_expec_elec_rates']], how = 'left', on = 'year')
    df = pd.merge(df,financial_parameters, how = 'left', on = 'sector')
    
    ## Diffusion from previous year ## 
    if year == cfg.start_year: 
        # get the initial market share per bin by county
        initial_market_shares = datfunc.get_initial_market_shares(cur, con, sector_abbr, sector)
        # join this to the df to on county_id
        df = pd.merge(df, initial_market_shares, how = 'left', on = ['county_id','bin_id'])
        df['market_value_last_year'] = df['installed_capacity_last_year'] * df['installed_costs_dollars_per_kw']        
    else:
        df = pd.merge(df,market_last_year, how = 'left', on = ['county_id','bin_id'])
        # Calculate value of incentives. Manual and DSIRE incentives can't stack. DSIRE ptc/pbi/fit are assumed to disburse over 10 years. 
    if scenario_opts['overwrite_exist_inc']:
        value_of_incentives = datfunc.calc_manual_incentives(df,con, year)
    else:
        inc = pd.merge(df,dsire_incentives,how = 'left', on = 'gid')
        value_of_incentives = datfunc.calc_dsire_incentives(inc, year, default_exp_yr = 2016, assumed_duration = 10)
    df = pd.merge(df, value_of_incentives, how = 'left', on = ['county_id','bin_id'])
    
    t0 = time.time()
    revenue, costs, cfs = calc_cashflows(df,deprec_schedule, scenario_opts, yrs = 30)
    logger.info('finfunc.calc_cashflows for %s for %s sector took: %0.1fs' %(year, sector, time.time() - t0))
    
    t0 = time.time()
    payback = calc_payback(cfs)
    logger.info('finfunc.calc_payback(cfs) for %s for %s sector took: %0.1fs' %(year, sector, time.time() - t0))
    
    t0 = time.time()
    ttd = calc_ttd(cfs, df)
    logger.info('finfunc.calc_ttd for %s for %s sector took: %0.1fs' %(year, sector, time.time() - t0))
    
    df['payback_period'] = np.where(df['sector'] == 'residential',payback, ttd)
    
    t0 = time.time()
    df['lcoe'] = calc_lcoe(costs,df.aep.values, df.discount_rate)
    logger.info('finfunc.calc_lcoe for %s for %s sector took: %0.1fs' %(year, sector, time.time() - t0))
    
    df['payback_key'] = (df['payback_period']*10).astype(int)
    
    #df = select_max_market_share(df,max_market_share, scenario_opts)
    df = pd.merge(df,max_market_share, how = 'left', on = ['sector', 'payback_key'])
    return df, logger
    
#==============================================================================    
    
    
    
#==============================================================================
def calc_cashflows(df,deprec_schedule, scenario_opts, yrs = 30):
    """
    Name:   calc_cashflows
    Purpose: Function to calculate revenue and cost cashflows associated with 
    system ownership:
    
        i) costs from servicing loan
        ii) costs from variable and fixed O&M
        iii) revenue from generation
        iv) revenue from depreciation
        v) revenue deduction of interest on loan
        vi) revenue from all other incentives
             
    Author: bsigrin
    Last Revision: 7/1/14
    
        IN:
            df - pandas dataframe - dataframe containing: [ic ($), loan_rate, 
                                    loan_term, down_payment, vom ($/kWh), fom ($/kW-yr), 
                                    rate_growth, aep (kWh/kW), avg_rate, cap (kW), tax_rt, sector]
            value_of_incentive
            value_of_rebate
            deprec_schedule
        
        OUT:
            cfs - numpy array of net cashflows
        
        
"""                
    # default is 30 year analysis periods
    shape=(len(df),yrs); 
    #df['cap'] = df['turbine_size_kw']
    df['ic'] = df['installed_costs_dollars_per_kw'] * df['system_size_kw']
    #df['aep'] = df['naep'] * df['turbine_size_kw']
    
    # Remove NAs if not rebate are passed in input sheet   
    df.ptc_length = df.ptc_length.fillna(0)
    df.ptc_length = df.ptc_length.astype(int)
    df.value_of_ptc = df.value_of_ptc.fillna(0)
    df.pbi_fit_length = df.pbi_fit_length.fillna(0)
    df.pbi_fit_length = df.pbi_fit_length.astype(int)
    df.value_of_pbi_fit = df.value_of_pbi_fit.fillna(0)
    df.value_of_tax_credit_or_deduction = df.value_of_tax_credit_or_deduction.fillna(0)
    df.value_of_rebate = df.value_of_rebate.fillna(0)
    df.value_of_increment = df.value_of_increment.fillna(0)
    df.value_of_rebate = df.value_of_rebate.fillna(0)
    
    # When the incentive payment in first year is larger than the downpayment, 
    # it distorts the IRR. This increases the down payment to at least 10%> than
    # the ITC
    #df = recalc_down_payment(df)

    ## COSTS    
    
    # 1)  Cost of servicing loan
    loan_cost = np.zeros(shape); 
    crf = (df.loan_rate*(1 + df.loan_rate)**df.loan_term_yrs) / ( (1+df.loan_rate)**df.loan_term_yrs - 1);
    pmt = - (1 - df.down_payment)* df.ic * crf    
    
    for i in range(len(loan_cost)): ## VECTORIZE THIS ##
        loan_cost[i][:df.loan_term_yrs[i]] = [pmt[i]] * df.loan_term_yrs[i]
    loan_cost[:,0] -= df.ic * df.down_payment 

    # 2) Costs of fixed & variable O&M
    om_cost = np.zeros(shape);
    om_cost[:] =   (-df.variable_om_dollars_per_kwh * df['aep'])[:,np.newaxis]
    om_cost[:] +=  (-df.fixed_om_dollars_per_kw_per_yr * df['system_size_kw'])[:,np.newaxis]
    
    ## Revenue
    """
    3) Revenue from generation. Revenue comes from excess and offset 
    generation. Offset energy is generation that instaneously offsets load and
    is credited at the full retail rate, regardless of scenario. Excess energy 
    is generation that exceeds load and may be credited as full retail (NEM), 
    avoided cost, or no credit. Amount of excess energy is aep * excess_gen_factor + 0.31 * (gen/load - 1) 
    See docs/excess_gen_method/sensitivity_of_excess_gen_to_sizing.R for more detail
    """
    

    
    # Multiplier for rate growth in real 2011 dollars

        
    tmp = np.empty(shape)
    tmp[:,0] = 1
    tmp[:,1:] = df.customer_expec_elec_rates[:,np.newaxis]
    rate_growth_mult = np.cumprod(tmp, axis = 1) 
    
    # Percentage of excess gen, bounded from 0 - 100%
    per_excess_gen = np.minimum(np.maximum(df.excess_generation_factor + 0.31 * (df.aep/df.ann_cons_kwh -1), 0),1)
    
    curtailment_rate = 0 # Placeholder for this to be updated with ReEDS integration    
    
    outflow_gen_kwh = df.aep * per_excess_gen * (1 - curtailment_rate)
    inflow_gen_kwh = df.aep * (1 - per_excess_gen)
    
    # Value of inflows (generation that is offsetting load)
    inflow_rate_dol_kwh   = 0.01 * df.elec_rate_cents_per_kwh
    value_inflows_dol = inflow_gen_kwh[:,np.newaxis] * inflow_rate_dol_kwh[:,np.newaxis] * rate_growth_mult
     
    # Set the rate the excess generation is credited
    if scenario_opts['net_metering_availability'] == 'Full_Net_Metering_Everywhere':
        outflow_rate = inflow_rate_dol_kwh
    elif scenario_opts['net_metering_availability'] == 'Partial_Avoided_Cost':
        outflow_rate = 0.5 * inflow_rate_dol_kwh
    elif scenario_opts['net_metering_availability'] == 'Partial_No_Outflows':
        outflow_rate = 0 * inflow_rate_dol_kwh
    elif scenario_opts['net_metering_availability'] == 'No_Net_Metering_Anywhere':
        outflow_rate = 0 * inflow_rate_dol_kwh
        df['nem_system_limit_kw'] = 0
    else:
        outflow_rate = 0 * inflow_rate_dol_kwh
        
    outflow_rate_dol_kwh = np.where(df['system_size_kw'] < df.nem_system_limit_kw, inflow_rate_dol_kwh, outflow_rate)
    value_outflows_dol = outflow_gen_kwh[:, np.newaxis] * outflow_rate_dol_kwh[:,np.newaxis] * rate_growth_mult
    
    generation_revenue = value_inflows_dol + value_outflows_dol

    
    # 4) Revenue from depreciation.  ### THIS NEEDS MORE WORK ###  
    # Depreciable basis is installed cost less tax incentives
    # Revenue comes from taxable deduction [basis * tax rate * schedule] and cannot be monetized by Residential
    
    depreciation_revenue = np.zeros(shape)
    deprec_basis = (df.ic - 0.5 * (df.value_of_tax_credit_or_deduction  + df.value_of_rebate))[:,np.newaxis] # depreciable basis reduced by half the incentive
    depreciation_revenue[:,:20] = deprec_basis * deprec_schedule.reshape(1,20) * df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial'))[:,np.newaxis]   
    
    # 5) Interest paid on loans is tax-deductible for commercial & industrial; 
    # assume can fully monetize
    
    # Calc interest paid
    interest_paid = np.empty(shape)
    for i in range(len(df)): # VECTORIZE THIS
        interest_paid[i,:] = (np.ipmt(df.loan_rate[i], [np.arange(yrs)], df.loan_term_yrs[i], -df.ic[i] * (1- df.down_payment[i])))    
    interest_paid[interest_paid < 0] = 0 # Truncate interest payments if loan_term < yrs
    interest_on_loan_pmts_revenue = interest_paid * df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial'))[:,np.newaxis]
    
    # 6) Revenue from other incentives    
    incentive_revenue = np.zeros(shape)
    incentive_revenue[:, 1] = df.value_of_increment + df.value_of_rebate + df.value_of_tax_credit_or_deduction

    ptc_revenue = np.zeros(shape)
    for i in range(len(df)):
        ptc_revenue[i,1:1+df.ptc_length[i]] = df.value_of_ptc[i]
    
    pbi_fit_revenue = np.zeros(shape)
    for i in range(len(df)):
        pbi_fit_revenue[i,1:1+df.pbi_fit_length[i]] = df.value_of_pbi_fit[i]
    
    incentive_revenue += ptc_revenue + pbi_fit_revenue
    
    revenue = generation_revenue + depreciation_revenue + interest_on_loan_pmts_revenue + incentive_revenue
    costs = loan_cost + om_cost
    cfs = revenue + costs 
    return revenue, costs, cfs
    
#==============================================================================

def calc_fin_metrics(costs, revenues, dr):
    cfs = costs + revenues
    
    irr = calc_irr(cfs)
    mirr = calc_mirr(cfs, finance_rate = dr, reinvest_rate = dr + 0.02)
    npv = calc_npv(cfs,dr)
    payback = calc_payback(cfs)
    ttd = calc_ttd(cfs)
    return

#==============================================================================    
def calc_lcoe(costs,aep, dr, yrs = 30):
    ''' LCOE calculation. LCOE is the net present cost, divided by the net present
    energy production. Assume that aep is constant over lifetime of system###
    
    IN: costs - numpy ndarray - project costs ($/yr) [customer bin x system year]
        aep   - numpy array - annual energy production of system (kWh/year)
        dr    - numpy array - annual discount rate (decimal)

    OUT: lcoe - numpy array - Levelized cost of energy (c/kWh) 
    '''
    
    num = -100 * calc_npv(costs, dr)
    denom = calc_npv(np.repeat(aep[:,np.newaxis],yrs, axis = 1), dr)
    return num/denom
#==============================================================================    

def calc_irr(cfs):
    ''' IRR calculation. Take minimum of return values ### Vectorize this ###
    
    IN: cfs - numpy array - project cash flows ($/yr)

    OUT: irr - numpy array - minimum IRR of cash flows ($) 
    
    '''
    if cfs.ndim == 1: 
        irr_out = [np.array(irr(cfs)).min()]
    else: 
        irr_out = []
        for x in cfs:
            out = np.array(irr(x)).min()
            if np.isnan(out): out = -1
            irr_out.append(out)
    return np.array(irr_out)

#==============================================================================
    
def calc_mirr(cfs,finance_rate, reinvest_rate):
    ''' MIRR calculation. ### Vectorize this ###
    
    IN: cfs - numpy array - project cash flows ($/yr)
        finance_rate - numpy array - Interest rate paid on the cash flows
        reinvest_rate - numpy array - Interest rate received on the cash flows upon reinvestment

    OUT: mirr - numpy array - Modified IRR of cash flows ($) 
    
    '''
    if cfs.ndim == 1: 
        cfs = np.asarray(cfs, dtype=np.double)
        n = cfs.size
        pos = cfs > 0
        neg = cfs < 0
        if not (pos.any() and neg.any()):
            return np.nan
        numer = np.abs(calc_npv(cfs = cfs*pos, dr = reinvest_rate))*(1 + reinvest_rate)
        denom = np.abs(calc_npv(cfs = cfs*neg, dr = finance_rate, ))*(1 + finance_rate)
        mirr_out = (numer/denom)**(1.0/(n - 1))*(1 + reinvest_rate) - 1
    else:
        cfs = np.asarray(cfs, dtype=np.double)
        n = cfs.size / finance_rate.size
        pos = cfs > 0
        neg = cfs < 0
        if not (pos.any() and neg.any()):
            return np.nan
        numer = np.abs(calc_npv(cfs = cfs*pos, dr = reinvest_rate))*(1 + reinvest_rate)
        denom = np.abs(calc_npv(cfs = cfs*neg, dr = finance_rate, ))*(1 + finance_rate)
        mirr_out = (numer/denom)**(1.0/(n - 1))*(1 + reinvest_rate) - 1
    return mirr_out

#==============================================================================

def calc_ttd(cfs,df):
    ''' Calculate time to double investment based on the MIRR. This is used for
    the commercial and industrial sectors.
    
    IN: cfs - numpy array - project cash flows ($/yr)

    OUT: ttd - numpy array - Time to double investment (years) 
    
    '''
    irrs = calc_irr(cfs)
    irrs = np.where(irrs<=0,1e-6,irrs)
    ttd = np.log(2) / np.log(1 + irrs)
    ttd[ttd <= 0] = 0
    ttd[ttd > 30] = 30
    return ttd.round(decimals = 1) # must be rounded to nearest 0.1 to join with max_market_share

#==============================================================================

def calc_npv(cfs,dr):
    ''' Vectorized NPV calculation based on (m x n) cashflows and (n x 1) 
    discount rate
    
    IN: cfs - numpy array - project cash flows ($/yr)
        dr  - numpy array - annual discount rate (decimal)
        
    OUT: npv - numpy array - net present value of cash flows ($) 
    
    '''
    dr = dr[:,np.newaxis]
    tmp = np.empty(cfs.shape)
    tmp[:,0] = 1
    tmp[:,1:] = 1/(1+dr)
    drm = np.cumprod(tmp, axis = 1)        
    npv = (drm * cfs).sum(axis = 1)   
    return npv
    
    
#==============================================================================
    
def calc_payback(cfs):
    '''payback calculator ### VECTORIZE THIS ###
    
    IN: cfs - numpy array - project cash flows ($/yr)
        
    OUT: pp - numpy array - interpolated payback period (years)
    
    '''
    cum_cfs = cfs.cumsum(axis = 1)
    out = []
    for x in cum_cfs:
        if x[-1] < 0: # No payback if the cum. cfs are negative in the final year
            pp = 30
        elif all(x<0): # Is positive cashflow ever achieved?
            pp = 30
        elif all(x>0): # Is positive cashflow instantly achieved?
            pp = 0
        else:
            # Return the last year where cumulative cfs changed from negative to positive
            base_year = np.where(np.diff(np.sign(x))>0)[0] 
            if base_year.size > 0:      
                base_year = base_year.max()
                frac_year = x[base_year]/(x[base_year] - x[base_year+1])
                pp = base_year + frac_year
            else: # If the array is empty i.e. never positive cfs, pp = 30
                pp = 30
        out.append(pp)
    
    return np.array(out).round(decimals =1) # must be rounded to nearest 0.1 to join with max_market_share
    
#==============================================================================

def recalc_down_payment(df):
    # Recalculate the down payment s.t. it exceeds the first year incentives
    value_of_first_yr_incentives = df.value_of_increment + df.value_of_rebate + df.value_of_tax_credit_or_deduction
    first_yr_incentives_fraction = value_of_first_yr_incentives/ df.ic
    df.down_payment = np.where(first_yr_incentives_fraction > df.down_payment, first_yr_incentives_fraction + 0.1, df.down_payment)
    df.down_payment = np.where(df.down_payment < 0, 0,df.down_payment)
    df.down_payment = np.where(df.down_payment > 1, 1,df.down_payment)
    return df
    
#==============================================================================

def irr(values):
    """
    Return the minimum Internal Rate of Return (IRR) within the range [-30%,+Inf].

    This is the "average" periodically compounded rate of return
    that gives a net present value of 0.0; for a more complete explanation,
    see Notes below.

    Parameters
    ----------
    values : array_like, shape(N,)
        Input cash flows per time period.  By convention, net "deposits"
        are negative and net "withdrawals" are positive.  Thus, for example,
        at least the first element of `values`, which represents the initial
        investment, will typically be negative.

    Returns
    -------
    out : float
        Internal Rate of Return for periodic input values.

    Notes
    -----

    """
    res = np.roots(values[::-1])
    # Find the root(s) between 0 and 1
    mask = (res.imag == 0) & (res.real > 0)
    res = res[mask].real
    if res.size == 0:
        return np.nan
    rate = 1.0/res - 1
    if sum(values)>0:
        rate = rate[rate>0] # Negative IRR is returned otherwise
    rate = rate[rate>-.3]
    if rate.size == 0:
        return np.nan
    if rate.size == 1:
        rate = rate.item()
    else: 
        rate = min(rate)
    return rate

#==============================================================================