"""
Name: financial_functions
Purpose: Contains functions to calculate financial values of distributed wind model

Author: bsigrin
Last Revision: 3/24/14

"""

import numpy as np
from collections import Iterable

#==============================================================================

def calc_cashflows(df,value_of_incentive, value_of_rebate, deprec_schedule, yrs = 30):
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
                                    loan_term, downpay_frac, vom ($/kWh), fom ($/kW-yr), 
                                    rate_growth, aep (kWh/kW), avg_rate, cap (kW), tax_rt, sector]
            value_of_incentive
            value_of_rebate
            deprec_schedule
        
        OUT:
            cfs - numpy array of net cashflows
        
        
"""                
    # default is 30 year analysis periods
    shape=(len(df),yrs);  
    
    ## COSTS    
    
    # 1)  Cost of servicing loan
    loan_cost = np.zeros(shape); 
    crf = (df.loan_rate*(1 + df.loan_rate)**df.loan_term) / ( (1+df.loan_rate)**df.loan_term - 1);
    pmt = - (1 - df.downpay_frac)* df.ic * crf    
    
    for i in range(len(cost)): ## VECTORIZE THIS ##
        loan_cost[i][:df.loan_term[i]] = [pmt[i]] * df.loan_term[i]
    loan_cost[:,0] -= ic * df.downpay_frac 

    # 2) Costs of fixed & variable O&M
    om_cost = np.zeros(shape);
    om_cost[:] = np.array([-df.vom * aep * df.cap]).T
    om_cost[:] +=  np.array([-df.fom * df.cap]).T
    
    ## Revenue

    
    # 3) Revenue from generation  ## REVISIONS NEEDED-- tie to perceived growth rates, not all generation will be offset at avg_rate
    
    tmp = np.empty(shape)
    tmp[:,0] = 1
    tmp[:,1:] = np.array([df.rate_growth]).T
    rate_growth_mult = np.cumprod(tmp, axis = 1)
    generation_revenue = np.array([df.aep * df.avg_rate * df.cap]).T  * rate_growth_mult
    
    # 4) Revenue from depreciation ### THIS NEEDS MORE WORK ###   
    
    deprec_basis = ic - 0.5 * (value_of_incentive + value_of_rebate) # depreciable basis reduced by half the incentive
    depreciation_revenue = np.array([(df.sector == 'Industrial') | (df.sector == 'Commercial')]).T * deprec_basis * deprec_schedule * df.tax_rt
    
    # 5) Interest paid on loans is tax-deductible for commercial & industrial; 
    # assume can fully monetize
    
    # Calc interest paid
    interest_paid = np.empty(shape)
    for i in range(len(df)):
        interest_paid[i,:] = (np.ipmt(df.loan_rate[i], [arange(yrs)], df.loan_term[i], -df.ic[i] * (1- df.downpay_frac[i])))    
    interest_paid[interest_paid < 0] = 0 # Truncate interest payments if loan_term < yrs
    interest_on_loan_pmts_revenue = np.array([(df.sector == 'Industrial') | (df.sector == 'Commercial')]).T * interest_paid * np.array([df.tax_rt]).T
    
    
    # 6) Revenue from other incentives
    
    incentive_revenue = np.zeros(shape)
    #incent_pay=np.zeros(shape); incent_pay[1]=incent_frac*cap_cost + incent_rebate;
    
    
    revenue = generation_revenue + depreciation_revenue + interest_on_loan_pmts_revenue + incentive_revenue
    costs = loan_cost + om_cost
    cfs = loan_cost + om_cost + generation_revenue + depreciation_revenue + interest_on_loan_pmts_revenue + incentive_revenue
    
    

    
    return revenue, costs, cfs
    
#==============================================================================

def calc_fin_metrics(costs, revenue, dr):
    cfs = costs + revenues
    
    irr = calc_irr(cfs)
    mirr = calc_mirr(cfs, finance_rate = dr, reinvest_rate = dr + 0.02)
    npv = calc_npv(cfs,dr)
    payback = calc_payback(cfs)

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
    
def calc_mirr(cfs,finance_rate = dr, reinvest_rate = dr + 0.02):
    ''' MIRR calculation. ### Vectorize this ###
    
    IN: cfs - numpy array - project cash flows ($/yr)
        finance_rate - numpy array - Interest rate paid on the cash flows
        reinvest_rate - numpy array - Interest rate received on the cash flows upon reinvestment

    OUT: mirr - numpy array - Modified IRR of cash flows ($) 
    
    '''
    if cfs.ndim == 1: 
        mirr_out = np.array(np.mirr(cfs, finance_rate, reinvest_rate))
    else:
        mirr_out = np.apply_along_axis(np.mirr,1, cfs, finance_rate, reinvest_rate)
    return mirr_out

#==============================================================================

def calc_ttd(cfs):
    ''' Calculate time to double investment.
    
    IN: cfs - numpy array - project cash flows ($/yr)

    OUT: ttd - numpy array - Time to double investment (years) 
    
    '''
    irrs = calc_irr(cfs)
    ttd = np.log(2) / np.log(1 + irrs)
    ttd[ttd<=0] = 30
    return ttd

#==============================================================================

def calc_npv(cfs,dr):
    ''' Vectorized NPV calculation based on (m x n) cashflows and (n x 1) 
    discount rate
    
    IN: cfs - numpy array - project cash flows ($/yr)
        dr  - numpy array - annual discount rate (decimal)
        
    OUT: npv - numpy array - net present value of cash flows ($) 
    
    '''
    dr = dr.reshape(len(dr),1)
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
                frac_year = x[yr]/(x[yr] - x[yr+1])
                pp = base_year + frac_year
            else: # If the array is empty i.e. never positive cfs, pp = 30
                pp = 30
        out.append(pp)
    return np.array(out)
    
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
#                                downpay_frac, loan_term, loan_rate, disc_rate, 
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