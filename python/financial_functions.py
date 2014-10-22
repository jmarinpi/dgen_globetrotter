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
def calc_economics(df, schema, sector, sector_abbr, market_projections, market_last_year, 
                   financial_parameters, cfg, scenario_opts, max_market_share, cur, con, 
                   year, dsire_incentives, deprec_schedule, logger, rate_escalations, ann_system_degradation):
    '''
    Calculates economics of system adoption (cashflows, payback, irr, etc.)
    
        IN:
            Lots    
        
        OUT:
            df - pd dataframe - main dataframe with econ outputs appended as columns
    '''
    
    df['sector'] = sector.lower()
    df = pd.merge(df,financial_parameters, how = 'left', on = ['sector','business_model'])
    
    # get customer expected rate escalations
    rate_growth_mult = datfunc.calc_expected_rate_escal(df, rate_escalations, year, sector_abbr)    
    
    ## Diffusion from previous year ## 
    if year == cfg.start_year: 
        # get the initial market share per bin by county
        initial_market_shares = datfunc.get_initial_market_shares(cur, con, sector_abbr, sector, schema)
        # join this to the df to on county_id
        df = pd.merge(df, initial_market_shares, how = 'left', on = ['county_id','bin_id'])
        df['market_value_last_year'] = df['installed_capacity_last_year'] * df['installed_costs_dollars_per_kw']        
    else:    
        df = pd.merge(df,market_last_year, how = 'left', on = ['county_id','bin_id'])
        # Calculate value of incentives. Manual and DSIRE incentives can't stack. DSIRE ptc/pbi/fit are assumed to disburse over 10 years. 

    # Calculate value of incentives. Manual and DSIRE incentives can't stack. DSIRE ptc/pbi/fit are assumed to disburse over 10 years.    
    if scenario_opts['overwrite_exist_inc']:
        value_of_incentives = datfunc.calc_manual_incentives(df,con, year, schema)
    else:
        inc = pd.merge(df,dsire_incentives,how = 'left', on = 'incentive_array_id')
        value_of_incentives = datfunc.calc_dsire_incentives(inc, year, default_exp_yr = 2016, assumed_duration = 10)
    df = pd.merge(df, value_of_incentives, how = 'left', on = ['county_id','bin_id'])
    
    revenue, costs, cfs = calc_cashflows(df, rate_growth_mult, deprec_schedule, scenario_opts, cfg.technology, ann_system_degradation, yrs = 30)
    
    ## Calc metric value here
    df['metric_value'] = calc_metric_value(df,cfs,revenue,costs)


        

    
    
    #    if sector == 'Residential':
    #        ttd = np.zeros(len(cfs))
    #    else: # Don't calculate for res sector
    #        ttd = calc_ttd(cfs)
    #            
    #    df['payback_period'] = np.where(df['sector'] == 'residential',payback, ttd)
    df['lcoe'] = calc_lcoe(costs,df.aep.values, df.discount_rate)    
    #df['payback_key'] = (df['payback_period']*10).astype(int)
     
    # Does the metric_value need to be an int?
    df = pd.merge(df,max_market_share, how = 'left', on = ['sector', 'metric','metric_value','business_model'])
    return df
    
#==============================================================================    
    
    
    
#==============================================================================
def calc_cashflows(df, rate_growth_mult, deprec_schedule, scenario_opts, tech, ann_system_degradation, yrs = 30):
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
    Last Revision: 10/8/14
    
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
    df['ic'] = df['installed_costs_dollars_per_kw'] * df['system_size_kw']
    
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

    ## COSTS    
    
    # 1)  Cost of servicing loan
    crf = (df.loan_rate*(1 + df.loan_rate)**df.loan_term_yrs) / ( (1+df.loan_rate)**df.loan_term_yrs - 1);
    pmt = - (1 - df.down_payment)* df.ic * crf    
    
    loan_cost = datfunc.fill_jagged_array(pmt,df.loan_term_yrs)
    loan_cost[:,0] -= df.ic * df.down_payment
        
    # Annualized (undiscounted) inverter replacement cost $/year (includes system size). Applied from year 10 onwards since assume initial 10-year warranty
    inverter_replacement_cost  = df['system_size_kw'] * df.inverter_cost_dollars_per_kw/df.inverter_lifetime_yrs
    inverter_cost = np.zeros(shape)
    
    # wind turbines do not have inverters hence no replacement cost.
    if tech == 'solar':
        inverter_cost[:,10:] = -inverter_replacement_cost[:,np.newaxis]
    
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
    
    # Annual system production (kWh) including degradation
    aep = np.empty(shape)
    aep[:,0] = 1
    aep[:,1:]  = 1 - ann_system_degradation
    aep = df.aep[:,np.newaxis] * aep.cumprod(axis = 1)
    
    # Percentage of excess gen, bounded from 0 - 100%
    per_excess_gen = np.minimum(np.maximum(df.excess_generation_factor[:,np.newaxis] + 0.31 * (aep/df.load_kwh_per_customer_in_bin[:,np.newaxis] -1), 0),1)
    
    curtailment_rate = 0 # Placeholder for this to be updated with ReEDS integration    
    
    outflow_gen_kwh = aep * per_excess_gen * (1 - curtailment_rate)
    inflow_gen_kwh = aep * (1 - per_excess_gen)
    
    # Value of inflows (generation that is offsetting load)
    inflow_rate_dol_kwh = 0.01 * df.elec_rate_cents_per_kwh
    value_inflows_dol = inflow_gen_kwh * inflow_rate_dol_kwh[:,np.newaxis] * rate_growth_mult
     
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
    value_outflows_dol = outflow_gen_kwh * outflow_rate_dol_kwh[:,np.newaxis] * rate_growth_mult
    
    generation_revenue = value_inflows_dol + value_outflows_dol
    # 4) Revenue from depreciation.  ### THIS NEEDS MORE WORK ###  
    # Depreciable basis is installed cost less tax incentives
    # Revenue comes from taxable deduction [basis * tax rate * schedule] and cannot be monetized by Residential
    
    depreciation_revenue = np.zeros(shape)
    deprec_basis = (df.ic - 0.5 * (df.value_of_tax_credit_or_deduction  + df.value_of_rebate))[:,np.newaxis] # depreciable basis reduced by half the incentive
    depreciation_revenue[:,:20] = deprec_basis * deprec_schedule.reshape(1,20) * df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial') | df.business_model == 'tpo')[:,np.newaxis]   

    # 5) Interest paid on loans is tax-deductible for commercial & industrial; 
    # assume can fully monetize
    
    # Calc interest paid
    interest_paid = calc_interest_pmt_schedule(df,30)
    interest_on_loan_pmts_revenue = interest_paid * df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial') | (df.business_model == 'tpo'))[:,np.newaxis]
    
    # 6) Revenue from other incentives    
    incentive_revenue = np.zeros(shape)
    incentive_revenue[:, 1] = df.value_of_increment + df.value_of_rebate + df.value_of_tax_credit_or_deduction
    
    ptc_revenue = datfunc.fill_jagged_array(df.value_of_ptc,df.ptc_length)
    pbi_fit_revenue = datfunc.fill_jagged_array(df.value_of_pbi_fit,df.pbi_fit_length)    

    incentive_revenue += ptc_revenue + pbi_fit_revenue
    
    revenue = generation_revenue + depreciation_revenue + interest_on_loan_pmts_revenue + incentive_revenue
    costs = loan_cost + om_cost + inverter_cost
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

def calc_ttd(cfs):
    ''' Calculate time to double investment based on the MIRR. This is used for
    the commercial and industrial sectors.
    
    IN: cfs - numpy array - project cash flows ($/yr)

    OUT: ttd - numpy array - Time to double investment (years) 
    
    '''
    irrs = virr(cfs, precision = 0.005, rmin = 0, rmax1 = 0.3, rmax2 = 0.5)
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

def virr(cfs, precision = 0.005, rmin = 0, rmax1 = 0.3, rmax2 = 0.5):
    ''' Vectorized IRR calculator. First calculate a 3D array of the discounted
    cash flows along cash flow series, time period, and discount rate. Sum over time to 
    collapse to a 2D array which gives the NPV along a range of discount rates 
    for each cash flow series. Next, find crossover where NPV is zero--corresponds
    to the lowest real IRR value. For performance, negative IRRs are not calculated
    -- returns "-1", and values are only calculated to an acceptable precision.
    
    IN:
        cfs - numpy 2d array - rows are cash flow series, cols are time periods
        precision - level of accuracy for the inner IRR band eg 0.005%
        rmin - lower bound of the inner IRR band eg 0%
        rmax1 - upper bound of the inner IRR band eg 30%
        rmax2 - upper bound of the outer IRR band. eg 50% Values in the outer 
                band are calculated to 1% precision, IRRs outside the upper band 
                return the rmax2 value
    OUT:
        r - numpy column array of IRRs for cash flow series
        
    M Gleason, B Sigrin - NREL 2014
    '''
    
    if cfs.ndim == 1: 
        cfs = cfs.reshape(1,len(cfs))

    # Range of time periods
    years = np.arange(0,cfs.shape[1])
    
    # Range of the discount rates
    rates_length1 = int((rmax1 - rmin)/precision) + 1
    rates_length2 = int((rmax2 - rmax1)/0.01)
    rates = np.zeros((rates_length1 + rates_length2,))
    rates[:rates_length1] = np.linspace(0,0.3,rates_length1)
    rates[rates_length1:] = np.linspace(0.31,0.5,rates_length2)

    # Discount rate multiplier rows are years, cols are rates
    drm = (1+rates)**-years[:,np.newaxis]

    # Calculate discounted cfs   
    discounted_cfs = cfs[:,:,np.newaxis] * drm
    
    # Calculate NPV array by summing over discounted cashflows
    npv = discounted_cfs.sum(axis = 1)
    
    # Convert npv into boolean for positives (0) and negatives (1)
    signs = npv < 0
    
    # Find the pairwise differences in boolean values
    # sign crosses over, the pairwise diff will be True
    crossovers = np.diff(signs,1,1)
    
    # Extract the irr from the first crossover for each row
    irr = np.min(np.ma.masked_equal(rates[1:]* crossovers,0),1)
    
    # deal with negative irrs
    negative_irrs = cfs.sum(1) < 0
    r = np.where(negative_irrs,-1,irr)
    r = np.where(irr.mask * (negative_irrs == False), 0.5, r)
        
    return r
    
#==============================================================================
    
def calc_interest_pmt_schedule(df,yrs):
    ''' Calculate the schedule of interest payments for a loan
    '''
    # Calculate future value (remaining balance on loan)
    crf = (df.loan_rate*(1 + df.loan_rate)**df.loan_term_yrs) / ( (1+df.loan_rate)**df.loan_term_yrs - 1);
    pv = df.ic * (1 - df.down_payment)
    pmt = df.ic * (1 - df.down_payment) * crf
    fv1 = pv[:,np.newaxis] * (1 + df.loan_rate[:,np.newaxis])**np.arange(yrs)
    fv2 = pmt[:,np.newaxis] *(((1 + df.loan_rate[:,np.newaxis])**np.arange(yrs) - 1)/df.loan_rate[:,np.newaxis])
    fv = fv1 - fv2
    
    # Interest payment is product of loan rate and balance on loan
    interest_pmt = np.maximum(df.loan_rate[:,np.newaxis] * fv,0)
    return interest_pmt
    
#==============================================================================

def calc_metric_value(df,cfs,revenue,costs):
    '''
    Calculates the economic value of adoption given the metric chosen. Residential buyers
    use simple payback, non-residential buyers use time-to-double, leasers use monthly bill savings
    
        IN:
            df    
        
        OUT:
            metric_value - pd series - series of values given the business_model and sector
    '''
    
    payback = calc_payback(cfs)
    ttd = calc_ttd(cfs)
    
    """ To calculate MBS:
    MBS = [Avg. monthly bill w/o tech] - [Avg. monthly bill w/ tech] - [Avg. monthly lease payment]
        = [Avg. monthly revenues]                                    - [Avg. monthly costs (assuming no down payment and incentives amortized into principle)]
        = [Sum(Revenues) - Sum(Costs)]/[Number of payments]
    Also, assume lease contract is over 20 years
    """ 
    
    mbs = (np.sum(revenues[:,:20], axis = 1) - np.sum(costs[:,:20], axis = 1))/20
    metric_value = np.where(df.business_model == 'tpo',mbs, np.where((df.sector == 'Industrial') | (df.sector == 'Commercial'),ttd,payback))
    
    return metric_value
    
#==============================================================================