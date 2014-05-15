"""
Name: financial_functions
Purpose: Contains functions to calculate financial values of distributed wind model

Author: bsigrin
Last Revision: 3/24/14

"""

import numpy as np
from collections import Iterable

#==============================================================================

def calc_cashflows(df,deprec_schedule, yrs = 30):
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
    Last Revision: 3/19/14
    
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
    df['cap'] = df['nameplate_capacity_kw']
    df['ic'] = df['installed_costs_dollars_per_kw'] * df['nameplate_capacity_kw']
    df['aep'] = df['naep'] * df['nameplate_capacity_kw']
    
    # When the incentive payment in first year is larger than the downpayment, 
    # it distorts the IRR. This increases the down payment to at least 10%> than
    # the ITC
    df = recalc_down_payment(df)

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
    om_cost[:] =   (-df.variable_om_dollars_per_kwh * df.naep * df.cap)[:,np.newaxis]
    om_cost[:] +=  (-df.fixed_om_dollars_per_kw_per_yr * df.cap)[:,np.newaxis]
    
    ## Revenue
    
    # 3) Revenue from generation  ## REVISIONS NEEDED-- tie to perceived growth rates, not all generation will be offset at avg_rate
    
    tmp = np.empty(shape)
    tmp[:,0] = 1
    tmp[:,1:] = df.customer_expec_elec_rates[:,np.newaxis]
    rate_growth_mult = np.cumprod(tmp, axis = 1)
    # Cannot monetize more than you consume
    generation_revenue = (np.minimum(df.aep,df.ann_cons_kwh) * 0.01 * df.elec_rate_cents_per_kwh)[:,np.newaxis] * rate_growth_mult 
    
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
    for i in range(len(df)):
        interest_paid[i,:] = (np.ipmt(df.loan_rate[i], [np.arange(yrs)], df.loan_term_yrs[i], -df.ic[i] * (1- df.down_payment[i])))    
    interest_paid[interest_paid < 0] = 0 # Truncate interest payments if loan_term < yrs
    interest_on_loan_pmts_revenue = interest_paid * df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial'))[:,np.newaxis]
    
    # 6) Revenue from other incentives
    
    incentive_revenue = np.zeros(shape)
    incentive_revenue[:, 1] = df.value_of_increment + df.value_of_rebate + df.value_of_tax_credit_or_deduction

    ptc_revenue = np.zeros(shape)
    for i in range(len(df)):
        ptc_revenue[i,1:df.ptc_length[i]] = df.value_of_ptc[i]
    
    pbi_fit_revenue = np.zeros(shape)
    for i in range(len(df)):
        pbi_fit_revenue[i,1:df.pbi_fit_length[i]] = df.value_of_pbi_fit[i]
    
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
        irr_out = [np.array(np.irr(cfs)).min()]
    else: 
        irr_out = []
        for x in cfs:
            out = np.array(np.irr(x)).min()
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
        if all(x<0): # Is positive cashflow every achieved?
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
#
#
#
#
#
##------------------------------------------------------------------------------
##  ^^^^  CALCULATE TIME OF CASHFLOW SCRIPT:  ^^^^ 
#"""
#tt=np.zeros((10000,1), dtype=float)
#for i1 in range(0, 10000):
#    t1=time.clock()
#    cost, principle = cashflow_calc(cap_cost, incent_frac, incent_rebate, tax_rt, 
#                                down_payment, loan_term, loan_rate, disc_rate, 
#                                MACRScom) 
#    t2=time.clock()
#    tt[i1]=(t2-t1)
#
#st = np.sort(tt, axis=0)
#
#p=plt.plot(st)
#
#meantp=np.mean(tt)
#mediantp=np.median(tt)
#timpact=(meantp-mediantp)/mediantp*100;
#
#print ' '
#print '%0.1f number of runs' %(10000)
#print '%0.6f median run time' %mediantp
#print '%0.6f mean run time' %meantp
#print '%0.2f percent runtime increase' %timpact
#print ' '
#
##p = plt.plot(cost)
#"""
##------------------------------------------------------------------------------

#def foo(cap_cost, incent_frac,capacity_factor):
#    # Convert all input parameters to arrays if passed as scalar    
#    arguments = locals()
#    for name in arguments:
#        if isinstance(arguments[name], (float,int)) : eval(name) = np.array([float(arguments[name])] * 4)
#    return name,cap_cost,incent_frac,capacity_factor
#
#    arguments = locals()
#    for name in arguments.keys():
#        if isinstance(arguments[name], (float,int)) : eval(name) = np.array([float(arguments[name])] * l)
#    return cap_cost, incent_frac   
#        
#    return arguments
#    name, value = enumerate(arguments)
#    if isinstance(value, (float,int)) : eval(name) = np.array([float(value)] * l)
#    return cap_cost, incent_frac
#
#import numpy as np
#
#def f(*args, **kwargs):
#  length = kwargs.get("length", 1)
#  ret = []
#  for arg in args:
#    if isinstance(arg, (float, int)):
#      ret.append(np.repeat(arg, length))
#    else:
#      ret.append(arg)
#  return tuple(ret)
#
#print f(1, 2, length=4)

##==============================================================================
##  ^^^^  REVENUE CASH FLOW CALCULATOR    ^^^^   
##
## Calculate annual revenue cashflows from electrical generation ($/kW)
##  !!Currently only for flat rate rate structure & Not vectorized!!
##
## INPUTS
## capacity_factor: annual average capacity factor (fraction)
## avg_rate: average (flat) electricity rate ($/kWh)
## rate_growth: average annual growth in electricity rates (%)
## yrs: length of examination (years)
#def calc_revenue_cashflows(capacity_factor,avg_rate,rate_growth,yrs = 30):
#       
#    revenue=np.zeros((yrs,));                       
#    
#    for i in range(30):
#        revenue[i] = 8760 * capacity_factor * avg_rate * (1 + rate_growth)**i
#
#    return revenue
##==============================================================================