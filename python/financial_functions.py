"""
Name: financial_functions
Purpose: Contains functions to calculate financial values of distributed wind model

Author: bsigrin
Last Revision: 3/24/14

"""

import numpy as np
import pandas as pd
import data_functions as datfunc
import utility_functions as utilfunc
import decorators
from config import show_times

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================



#==============================================================================
@decorators.fn_timer(logger = logger, verbose = show_times, tab_level = 3, prefix = '')
def calc_economics(df, schema, sector, sector_abbr, market_projections,
                   financial_parameters, scenario_opts, incentive_opts, max_market_share, cur, con, 
                   year, dsire_incentives, deprec_schedule, ann_system_degradation, 
                   mode, curtailment_method, tech_lifetime = 25, max_incentive_fraction = 0.4):
    '''
    Calculates the economics of DER adoption through cash-flow analysis.  (cashflows, payback, irr, etc.)

    
    
        IN:
            Lots    
        
        OUT:
            df - pd dataframe - main dataframe with econ outputs appended as columns
    '''
    
    logger.info("\t\tCalculating system economics")
    
    # Evaluate economics of leasing or buying for all customers who are able to lease
    business_model = pd.DataFrame({'business_model' : ('host_owned','tpo'), 
                                   'metric' : ('payback_period','percent_monthly_bill_savings'),
                                   'cross_join' : (1, 1)})
    df['cross_join'] = 1
    df = pd.merge(df, business_model, on = 'cross_join')
    df = df.drop('cross_join', axis=1)
    
    df['sector'] = sector.lower()
    df['sector_abbr'] = sector_abbr
    df = pd.merge(df, financial_parameters, how = 'left', on = ['sector', 'business_model', 'tech'])
    df = pd.merge(df, ann_system_degradation, how = 'left', on = ['tech'])
    df = pd.merge(df, deprec_schedule, how = 'left', on = ['tech'])
    df = pd.merge(df, incentive_opts, how = 'left', on = ['tech'])
    
    # get customer expected rate escalations
    # Use the electricity rate multipliers from ReEDS if in ReEDS modes and non-zero multipliers have been passed
    if mode == 'ReEDS' and max(df['ReEDS_elec_price_mult'])>0:
        
        rate_growth_mult = np.ones((len(df), tech_lifetime))
        rate_growth_mult *= df['ReEDS_elec_price_mult'][:,np.newaxis]
        df['rate_escalations'] = rate_growth_mult.tolist()
    else:
        # if not in ReEDS mode, use the calc_expected_rate_escal function
        rate_growth_df = datfunc.get_rate_escalations(con, schema, year, tech_lifetime)
        df = pd.merge(df, rate_growth_df, how = 'left', on = ['sector_abbr', 'census_division_abbr'])

    # Calculate value of incentives. Manual and DSIRE incentives can't stack. DSIRE ptc/pbi/fit are assumed to disburse over 10 years.    
    df_manual_incentives = df[df['overwrite_exist_inc'] == True]
    df_dsire_incentives = df[df['overwrite_exist_inc'] == False]
    
    value_of_incentives_manual = datfunc.calc_manual_incentives(df_manual_incentives, con, year, schema)
    value_of_incentives_dsire = datfunc.calc_dsire_incentives(df_dsire_incentives, dsire_incentives, year, default_exp_yr = 2016, assumed_duration = 10)

    value_of_incentives_all = pd.concat([value_of_incentives_manual, value_of_incentives_dsire], axis = 0, ignore_index = True)
    df = pd.merge(df, value_of_incentives_all, how = 'left', on = ['county_id','bin_id','business_model', 'tech', 'sector'])

    revenue, costs, cfs, first_year_bill_with_system, first_year_bill_without_system, total_value_of_incentives = calc_cashflows(df, scenario_opts, curtailment_method, tech_lifetime, max_incentive_fraction)
    
    df['total_value_of_incentives'] = total_value_of_incentives
    ## Calc metric value here
    df['metric_value_precise'] = calc_metric_value(df,cfs,revenue,costs, tech_lifetime)
    df['lcoe'] = calc_lcoe(costs,df.aep.values, df.discount_rate, tech_lifetime)
    npv = calc_npv(cfs, np.array([0.04]))
    with np.errstate(invalid = 'ignore'):
        df['npv4'] = np.where(df.system_size_kw == 0, 0, npv/df.system_size_kw)

    
    # Convert metric value to integer as a primary key, then bound within max market share ranges
    max_payback = max_market_share[max_market_share.metric == 'payback_period'].metric_value.max()
    min_payback = max_market_share[max_market_share.metric == 'payback_period'].metric_value.min()
    max_mbs = max_market_share[max_market_share.metric == 'percent_monthly_bill_savings'].metric_value.max()
    min_mbs = max_market_share[max_market_share.metric == 'percent_monthly_bill_savings'].metric_value.min()
    
    # copy the metric valeus to a new column to store an edited version
    metric_value_bounded = df.metric_value_precise.values.copy()
    
    # where the metric value exceeds the corresponding max market curve bounds, set the value to the corresponding bound
    metric_value_bounded[np.where((df.metric == 'payback_period') & (df.metric_value_precise < min_payback))] = min_payback
    metric_value_bounded[np.where((df.metric == 'payback_period') & (df.metric_value_precise > max_payback))] = max_payback    
    metric_value_bounded[np.where((df.metric == 'percent_monthly_bill_savings') & (df.metric_value_precise < min_mbs))] = min_mbs
    metric_value_bounded[np.where((df.metric == 'percent_monthly_bill_savings') & (df.metric_value_precise > max_mbs))] = max_mbs
    df['metric_value_bounded'] = metric_value_bounded

    # scale and round to nearest int    
    df['metric_value_as_factor'] = (df['metric_value_bounded'] * 100).round().astype('int')
    # add a scaled key to the max_market_share df too
    max_market_share['metric_value_as_factor'] = (max_market_share['metric_value'] * 100).round().astype('int')

    # Join the max_market_share table and df in order to select the ultimate mms based on the metric value. 
    df = pd.merge(df,max_market_share, how = 'left', on = ['sector', 'metric','metric_value_as_factor','business_model'])
    
    # Derate the maximum market share for commercial and industrial customers in leased buildings by (1/3)
    # based on the owner occupancy status (1 = owner-occupied, 2 = leased)
    df['max_market_share'] = np.where(df.owner_occupancy_status == 2, df.max_market_share/3,df.max_market_share)
    
    return df
#==============================================================================    
    
    
    
#==============================================================================
def calc_cashflows(df, scenario_opts, curtailment_method, tech_lifetime = 25, max_incentive_fraction = 0.4):
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
    Last Revision: 5/21/15
    
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
    # default is 25 year analysis periods
    shape=(len(df),tech_lifetime); 
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
    
    ## COSTS    
    
    # 1)  Cost of servicing loan/leasing payments
    crf = (df.loan_rate*(1 + df.loan_rate)**df.loan_term_yrs) / ( (1+df.loan_rate)**df.loan_term_yrs - 1);

    # Cap the fraction of capital costs that incentives may offset
    # TODO: Applying this as a hot-fix for bugs in the DSIRE dataset. We should review dsire dataset to ensure incentives are being
    # accurately valued

    # Constrain fraction to [0,1]
    max_incent_fraction = min(max(max_incentive_fraction, 0), 1)
    #np.minimum(max_incent_fraction * df['ic'], df['value_of_increment'] + df['value_of_rebate'] + df['value_of_tax_credit_or_deduction'])
    df['total_value_of_incentive'] = np.minimum(max_incent_fraction * df['ic'], df['value_of_increment'] + df['value_of_rebate'] + df['value_of_tax_credit_or_deduction'])
    
    # Assume that incentives received in first year are directly credited against installed cost; This help avoid
    # ITC cash flow imbalances in first year
    net_installed_cost = df['ic'] - df['total_value_of_incentive']
    
    # Calculate the annual payment net the downpayment and upfront incentives
    pmt = - (1 - df.down_payment) * net_installed_cost * crf    
    annual_loan_pmts = datfunc.fill_jagged_array(pmt,df.loan_term_yrs,cols = tech_lifetime)

    # Pay the down payment in year zero and loan payments thereafter. The downpayment is added at
    # the end of the cash flow calculations to make the year zero framing simpler
    down_payment_cost = (-net_installed_cost * df.down_payment)[:,np.newaxis] 
    
    # wind turbines do not have inverters hence no replacement cost.
    # but for solar, calculate and replace the inverter replacement costs with actual values
    inverter_cost = np.zeros(shape)
    # Annualized (undiscounted) inverter replacement cost $/year (includes system size). Applied from year 10 onwards since assume initial 10-year warranty
    with np.errstate(invalid = 'ignore'):
        inverter_replacement_cost  = np.where(df['tech'] == 'solar', df['system_size_kw'] * df.inverter_cost_dollars_per_kw/df.inverter_lifetime_yrs, 0)
    inverter_cost[:,10:] = -inverter_replacement_cost[:,np.newaxis]
    
    # 2) Costs of fixed & variable O&M. O&M costs are tax deductible for commerical entitites
    om_cost = np.zeros(shape);
    om_cost[:] =   (-df.variable_om_dollars_per_kwh * df['aep'])[:,np.newaxis]
    om_cost[:] +=  (-df.fixed_om_dollars_per_kw_per_yr * df['system_size_kw'])[:,np.newaxis]
    om_cost[:] *= (1 - (df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial') | (df.business_model == 'tpo'))[:,np.newaxis]))

    ## Revenue
    """
    3) Revenue from generation. Revenue comes from excess and offset 
    generation. Offset energy is generation that instaneously offsets load and
    is credited at the full retail rate, regardless of scenario. Excess energy 
    is generation that exceeds load and may be credited as full retail (NEM), 
    avoided cost, or no credit. Amount of excess energy is aep * excess_gen_factor + 0.31 * (gen/load - 1) 
    See docs/excess_gen_method/sensitivity_of_excess_gen_to_sizing.R for more detail    
    """
    # rate_growth_mult is a cumulative factor i.e. [1,1.02,1.044] instead of [0.02, 0.02, 0.02]
        
    # Take the difference of bills in first year, this is the revenue in the first year. Then assume that bill savings will follow
    # the same trajectories as changes in rate escalation. Output of this should be a data frame of shape (len(df),30)
    
    # TODO: curtailments should be applied to the generation, however currently infeasible for SAM integration
    if curtailment_method == 'net':
        df['first_year_energy_savings'] = (1- (df['curtailment_rate'] * df['excess_generation_percent'])) * (df['first_year_bill_without_system'] - df['first_year_bill_with_system'])
    elif curtailment_method == 'gross':
        df['first_year_energy_savings'] = (1- df['curtailment_rate']) * (df['first_year_bill_without_system'] - df['first_year_bill_with_system'])
    elif curtailment_method == 'off':
        df['first_year_energy_savings'] = df['first_year_bill_without_system'] - df['first_year_bill_with_system']
    
    generation_revenue = df['first_year_energy_savings'][:,np.newaxis] * np.array(list(df['rate_escalations']), dtype = 'float64') 
    
    # Decrement the revenue to account for system degradation.
    system_degradation_factor = np.empty(shape)
    system_degradation_factor[:,0] = 1
    system_degradation_factor[:,1:]  = 1 - df['ann_system_degradation'][:, np.newaxis]
    system_degradation_factor = system_degradation_factor.cumprod(axis = 1)
    
    generation_revenue *= system_degradation_factor
    
    # Since electricity expenses are tax deductible for commercial & industrial 
    # entities, the net annual savings from a system is reduced by the marginal tax rate
    generation_revenue *= (1 - df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial'))[:,np.newaxis])
    
    """
    4) Revenue from carbon price.
    
    Revenue from a carbon price is based on the value of offset generation from 
    conventional thermal generation. The value of offset generation equals the 
    product of offset generation (kWh), applicable carbon intensity (t CO2/kWh),
    and carbon price ($/t CO2) at year of adoption, constant over ownership period.
    Note that this assumes that carbon intensities (gen mix) don't change over time
    and that the revenue for consumer is the same in all years of ownership. 
    Instead of state avg. CO2 intensity, user can select NGCC intensity
    
    Annual revenue is ~$6-12/kW/year per $10/t
    """
    
    carbon_tax_revenue = np.empty(shape)
    carbon_tax_revenue[:] = 100 * (df.carbon_price_cents_per_kwh * df.aep)[:,np.newaxis] * system_degradation_factor
        
    '''
    5) Revenue from depreciation.  
    Depreciable basis is installed cost less tax incentives
    Revenue comes from taxable deduction [basis * tax rate * schedule] and cannot be monetized by Residential
    '''
    depreciation_revenue = np.zeros(shape)
    max_depreciation_reduction = np.minimum(df['total_value_of_incentive'], df['value_of_tax_credit_or_deduction']  + df['value_of_rebate'])
    deprec_basis = np.maximum(df.ic - 0.5 * (max_depreciation_reduction),0)[:,np.newaxis] # depreciable basis reduced by half the incentive
    deprec_schedule_arr = np.array(list(df['deprec']))    
    depreciation_revenue[:,:20] = deprec_basis * deprec_schedule_arr * df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial') | (df.business_model == 'tpo'))[:,np.newaxis]   
    deprec_basis * df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial') | (df.business_model == 'tpo'))[:,np.newaxis]
    '''
    6) Interest paid on loans is tax-deductible for commercial & industrial users; 
    assume can fully monetize. Assume that third-party owners finance system all-cash--thus no interest to deduct. 
    '''
    
    # Calc interest paid on serving the loan
    interest_paid = calc_interest_pmt_schedule(df,tech_lifetime)
    interest_on_loan_pmts_revenue = interest_paid * df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial') & (df.business_model == 'host_owned'))[:,np.newaxis]
    
    '''
    7) Revenue from other incentives
    '''
    
    incentive_revenue = np.zeros(shape)
    
    ptc_revenue = datfunc.fill_jagged_array(np.minimum(df.value_of_ptc,df.total_value_of_incentive/df.ptc_length),df.ptc_length, cols = tech_lifetime)
    pbi_fit_revenue = datfunc.fill_jagged_array(np.minimum(df.value_of_pbi_fit,df.total_value_of_incentive/df.ptc_length) ,df.pbi_fit_length, cols = tech_lifetime)    

    incentive_revenue += ptc_revenue + pbi_fit_revenue
    
    revenue = generation_revenue + carbon_tax_revenue + depreciation_revenue + interest_on_loan_pmts_revenue + incentive_revenue
    revenue = np.hstack((np.zeros((len(df),1)),revenue)) # Add a zero column to revenues to reflect year zero
    costs = annual_loan_pmts + om_cost + inverter_cost
    costs = np.hstack((down_payment_cost, costs)) # Down payment occurs in year zero
    cfs = revenue + costs
    
    # Calculate the monthly bill savings in the first year of ownership in dollars ($)
    # and in percentage of prior bill. Recall that the first_year_bill variables are not updated in each
    # solve sequence, they are calculated once in the first year of the model. Thus, they are multiplied by the rate growth multiplier
    # in the current solve year to update for rate changes
    #first_year_energy_savings = (df.first_year_bill_without_system - df.first_year_bill_with_system) * rate_growth_mult[:,0] 
    avg_annual_payment = (annual_loan_pmts.sum(axis = 1)/df.loan_term_yrs) + down_payment_cost.sum(axis = 1)/20
    first_year_bill_savings = df.first_year_energy_savings.values + avg_annual_payment # first_year_energy_savings is positive, avg_annual_payment is a negative
    monthly_bill_savings = first_year_bill_savings/12
    percent_monthly_bill_savings = first_year_bill_savings/df.first_year_bill_without_system
    
    
    # If monthly_bill_savings is zero, percent_mbs will be non-finite
    percent_monthly_bill_savings = np.where(df.first_year_bill_without_system.values == 0, 0, percent_monthly_bill_savings)
    df['monthly_bill_savings'] = monthly_bill_savings
    df['percent_monthly_bill_savings'] = percent_monthly_bill_savings
    
    return revenue, costs, cfs, df.first_year_bill_with_system, df.first_year_bill_without_system,df.total_value_of_incentive

#==============================================================================    
def calc_lcoe(costs,aep, dr, tech_lifetime):
    ''' LCOE calculation. LCOE is the net present cost, divided by the net present
    energy production. Assume that aep is constant over lifetime of system###
    
    IN: costs - numpy ndarray - project costs ($/yr) [customer bin x system year]
        aep   - numpy array - annual energy production of system (kWh/year)
        dr    - numpy array - annual discount rate (decimal)

    OUT: lcoe - numpy array - Levelized cost of energy (c/kWh) 
    '''
    
    num = -100 * calc_npv(costs, dr)
    aep_time_series = np.repeat(aep[:,np.newaxis], tech_lifetime, axis = 1)
    aep_time_series = np.hstack((np.zeros((len(aep),1)),aep_time_series))
    denom = calc_npv(aep_time_series, dr)
    
    # where system size is zero, lcoe should be nan
    # otherwise, calc using num/denom
    with np.errstate(invalid = 'ignore'):
        lcoe =  np.where(aep == 0, np.nan, num/denom)
    
    return lcoe
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
    
def calc_payback(cfs,revenue,costs,tech_lifetime):
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

def calc_metric_value(df,cfs,revenue,costs, tech_lifetime):
    '''
    Calculates the economic value of adoption given the metric chosen. Residential buyers
    use simple payback, non-residential buyers use time-to-double, leasers use monthly bill savings
    
        IN:
            df    
        
        OUT:
            metric_value - pd series - series of values given the business_model and sector
    '''
    
    payback = calc_payback(cfs,revenue,costs,tech_lifetime)
    ttd = calc_ttd(cfs)
    
    """ MBS is calculated in the calc_cashflows function using this method:
    Annual bill savings = [First year bill w/o tech] - [First year bill w/ tech] - [Avg. annual system payment]
    Monthly bill savings = Annual bill savings / 12
    Percent Monthly Bill Savings = Annual bill savings/ First year bill w/o tech

    Where First year bill w/o tech and  First year bill are outputs of calling SAM
    and
    Where Avg. annual system payment = Sum(down payment + monthly system payment (aka loan_cost)) / loan term (20 years for TPO and 15 for HO)
    """ 

    metric_value = np.where(df.business_model == 'tpo',df.percent_monthly_bill_savings, np.where((df.sector == 'Industrial') | (df.sector == 'Commercial'),ttd,payback))
    
    return metric_value    
#==============================================================================