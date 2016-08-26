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

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================



#==============================================================================
@decorators.fn_timer(logger = logger, tab_level = 3, prefix = '')
def calc_economics(df, schema, market_projections, financial_parameters, rate_growth_df, 
                   scenario_opts, max_market_share, cur, con,
                   year, dsire_incentives, dsire_opts, state_dsire, srecs, 
                   mode, curtailment_method, itc_options, inflation_rate, incentive_cap, tech_lifetime = 25):
    '''
    Calculates the economics of DER adoption through cash-flow analysis.  (cashflows, payback, irr, etc.)

    
    
        IN:
            Lots    
        
        OUT:
            df - pd dataframe - main dataframe with econ outputs appended as columns
    '''
    
    logger.info("\t\tCalculating System Economics")

    # append year to the dataframe
    df['year'] = year

    # duplicate the data frame for each business model
    df_tpo = df.copy()
    df_tpo['business_model'] = 'tpo'
    df_tpo['metric'] = 'percent_monthly_bill_savings'

    df_ho = df.copy()
    df_ho['business_model'] = 'host_owned'
    df_ho['metric'] = 'payback_period'

    # recombine into a single data frame    
    df = pd.concat([df_ho, df_tpo], axis = 0, ignore_index = True)
        
    # merge in financial parameters
    df = pd.merge(df, financial_parameters, how = 'left', on = ['sector_abbr', 'business_model', 'tech', 'year'])

    # get customer expected rate escalations
    # Use the electricity rate multipliers from ReEDS if in ReEDS modes and non-zero multipliers have been passed
    if mode == 'ReEDS' and max(df['ReEDS_elec_price_mult']) > 0:
        rate_growth_mult = np.ones((len(df), tech_lifetime))
        rate_growth_mult *= df['ReEDS_elec_price_mult'][:,np.newaxis]
        df['rate_escalations'] = rate_growth_mult.tolist()
    else:
        # if not in ReEDS mode, use the calc_expected_rate_escal function
        start_i = year - 2014
        end_i = start_i + tech_lifetime
        rate_esc = rate_growth_df.copy()
        rate_esc.loc[:, 'rate_escalations'] = np.array(rate_esc.rate_escalations.tolist(), dtype = 'float64')[:, start_i:end_i].tolist()
        df = pd.merge(df, rate_esc, how = 'left', on = ['sector_abbr', 'census_division_abbr'])

    # split out rows to run through state and old dsire incentives
    df_dsire_incentives = df[(df['tech'] == 'solar')]
    df_state_dsire_incentives = df[(df['tech'] == 'wind')]
    # Calculate value of incentives. DSIRE ptc/pbi/fit are assumed to disburse over 10 years.    
    value_of_incentives_dsire = datfunc.calc_dsire_incentives(df_dsire_incentives, dsire_incentives, srecs, year, 
                                                              dsire_opts, assumed_duration = 10)
    value_of_incentives_state_dsire = datfunc.calc_state_dsire_incentives(df_state_dsire_incentives, state_dsire, year)
    # combine the results by concatenating the dataframes
    value_of_incentives_all = pd.concat([value_of_incentives_dsire, value_of_incentives_state_dsire], axis = 0, ignore_index = True)
    # sum up total incentives by agent
    value_of_incentives_all_summed = value_of_incentives_all[['tech', 'sector_abbr', 'county_id', 'bin_id', 'business_model', 'value_of_increment', 'value_of_pbi_fit', 'value_of_ptc', 'pbi_fit_length', 'ptc_length', 'value_of_rebate', 'value_of_tax_credit_or_deduction']].groupby(['tech', 'sector_abbr', 'county_id','bin_id','business_model']).sum().reset_index()     
    # join back to the main df
    df = pd.merge(df, value_of_incentives_all_summed, how = 'left', on = ['county_id','bin_id','business_model', 'tech', 'sector_abbr'])
    fill_vals = {'value_of_increment' : 0,
                'value_of_pbi_fit' : 0,
                'value_of_ptc' : 0,
                'pbi_fit_length' : 0,
                'ptc_length' : 0,
                'value_of_rebate' : 0,
                'value_of_tax_credit_or_deduction' : 0}    
    df.fillna(fill_vals, inplace = True)
    # Calculates value of ITC, return df with a new column 'value_of_itc'
    df = datfunc.calc_value_of_itc(df, itc_options, year)

    # calculate cashflows
    revenue, costs, cfs, df = calc_cashflows(df, scenario_opts, curtailment_method, incentive_cap, tech_lifetime)    

    ## Calc metric value here
    df['metric_value_precise'] = calc_metric_value(df, cfs, revenue, costs, tech_lifetime)

    df = calc_lcoe(df, inflation_rate, econ_life = 20)
    npv4 = calc_npv(cfs, np.array([0.04]))
    npv_agent = calc_npv(cfs, df.discount_rate)
    with np.errstate(invalid = 'ignore'):
        df['npv4'] = np.where(df.system_size_kw == 0, 0, npv4/df.system_size_kw)
        df['npv_agent'] = np.where(df.system_size_kw == 0, 0, npv_agent/df.system_size_kw)

    
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
    df = pd.merge(df, max_market_share, how = 'left', on = ['sector_abbr', 'metric','metric_value_as_factor','business_model'])
    
    # Derate the maximum market share for commercial and industrial customers in leased buildings by (1/3)
    # based on the owner occupancy status (1 = owner-occupied, 2 = leased)
    df['max_market_share'] = np.where(df.owner_occupancy_status == 2, df.max_market_share/3,df.max_market_share)
    
    return df
#==============================================================================    
    
    
    
#==============================================================================
def calc_cashflows(df, scenario_opts, curtailment_method, incentive_cap, tech_lifetime = 25):
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
    # extract a list of the input columns
    in_cols = df.columns.tolist()

    # default is 25 year analysis periods
    shape=(len(df),tech_lifetime); 
    df['ic'] = df['installed_costs_dollars_per_kw'] * df['system_size_kw']
    
    # merge in the incentives cap
    df = pd.merge(df, incentive_cap, how = 'left', on = ['tech'])
    
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
    df['crf'] = (df.loan_rate*(1 + df.loan_rate)**df.loan_term_yrs) / ( (1+df.loan_rate)**df.loan_term_yrs - 1);

    # Cap the fraction of capital costs that incentives may offset
    # TODO: Applying this as a hot-fix for bugs in the DSIRE dataset. We should review dsire dataset to ensure incentives are being
    # accurately valued

    # Constrain incentive to max incente fraction
    df['total_value_of_incentives'] = np.minimum(df['max_incentive_fraction'] * df['ic'], df['value_of_increment'] + df['value_of_rebate'] + df['value_of_tax_credit_or_deduction'] + df['value_of_itc'])
    
    # Assume that incentives received in first year are directly credited against installed cost; This help avoid
    # ITC cash flow imbalances in first year
    df['net_installed_cost'] = df['ic'] - df['total_value_of_incentives']
    
    # Calculate the annual payment net the downpayment and upfront incentives
    pmt = - (1 - df.down_payment) * df['net_installed_cost'] * df['crf']    
    annual_loan_pmts = datfunc.fill_jagged_array(pmt,df.loan_term_yrs,cols = tech_lifetime)

    # Pay the down payment in year zero and loan payments thereafter. The downpayment is added at
    # the end of the cash flow calculations to make the year zero framing simpler
    down_payment_cost = (-df['net_installed_cost'] * df.down_payment)[:,np.newaxis] 
    
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
    om_cost[:] *= (1 - (df.tax_rate[:,np.newaxis] * ((df.sector_abbr == 'ind') | (df.sector_abbr == 'com') | (df.business_model == 'tpo'))[:,np.newaxis]))

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
    generation_revenue *= (1 - df.tax_rate[:,np.newaxis] * ((df.sector_abbr == 'ind') | (df.sector_abbr == 'com'))[:,np.newaxis])
    
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
    carbon_tax_revenue[:] = (df.carbon_price_cents_per_kwh * df.aep)[:,np.newaxis] * system_degradation_factor / 100
        
    '''
    5) Revenue from depreciation.  
    Depreciable basis is installed cost less tax incentives
    Revenue comes from taxable deduction [basis * tax rate * schedule] and cannot be monetized by Residential
    '''
    depreciation_revenue = np.zeros(shape)
    max_depreciation_reduction = np.minimum(df['total_value_of_incentives'], df['value_of_tax_credit_or_deduction']  + df['value_of_rebate'])
    deprec_basis = np.maximum(df.ic - 0.5 * (max_depreciation_reduction),0)[:,np.newaxis] # depreciable basis reduced by half the incentive
    deprec_schedule_arr = np.array(list(df['deprec']))    
    depreciation_revenue[:,:20] = deprec_basis * deprec_schedule_arr * df.tax_rate[:,np.newaxis] * ((df.sector_abbr == 'ind') | (df.sector_abbr == 'com') | (df.business_model == 'tpo'))[:,np.newaxis]   

    '''
    6) Interest paid on loans is tax-deductible for commercial & industrial users; 
    assume can fully monetize. Assume that third-party owners finance system all-cash--thus no interest to deduct. 
    '''
    
    # Calc interest paid on serving the loan
    interest_paid = calc_interest_pmt_schedule(df,tech_lifetime)
    interest_on_loan_pmts_revenue = interest_paid * df.tax_rate[:,np.newaxis] * (((df.sector_abbr == 'ind') | (df.sector_abbr == 'com')) & (df.business_model == 'host_owned'))[:,np.newaxis]
    
    '''
    7) Revenue from other incentives
    '''
    
    incentive_revenue = np.zeros(shape)
    
    # cap value of ptc and pbi fit
    remainder_for_incentive_revenues = np.maximum((df['max_incentive_fraction'] * df['ic']) - df['total_value_of_incentives'], 0)
    df['adj_value_of_ptc'] = np.minimum(remainder_for_incentive_revenues/df['ptc_length'], df['value_of_ptc'] * df['ptc_length'])
    df['adj_value_of_ptc'] = df['adj_value_of_ptc'].fillna(0)
    remainder_for_incentive_revenues = np.maximum(remainder_for_incentive_revenues - df['adj_value_of_ptc'] * df['ptc_length'], 0)
    df['adj_value_of_pbi_fit'] = np.minimum(remainder_for_incentive_revenues/df['pbi_fit_length'], df['value_of_pbi_fit'] * df['pbi_fit_length'])
    df['adj_value_of_pbi_fit'] = df['adj_value_of_pbi_fit'].fillna(0)
    
    ptc_revenue = datfunc.fill_jagged_array(df['adj_value_of_ptc'], df.ptc_length, cols = tech_lifetime)
    pbi_fit_revenue = datfunc.fill_jagged_array(df['adj_value_of_pbi_fit'], df.pbi_fit_length, cols = tech_lifetime)    

    incentive_revenue += ptc_revenue + pbi_fit_revenue
    
    revenue = generation_revenue + carbon_tax_revenue + depreciation_revenue + interest_on_loan_pmts_revenue + incentive_revenue
    revenue = np.hstack((np.zeros((len(df),1)),revenue)) # Add a zero column to revenues to reflect year zero
    costs = annual_loan_pmts + om_cost + inverter_cost
    costs = np.hstack((down_payment_cost, costs)) # Down payment occurs in year zero
    cfs = revenue + costs
    
    # Calculate the avg  and avg pct monthly bill savings (accounting for rate escalations, generation revenue, and average over all years)
    tpo_revenue = generation_revenue + carbon_tax_revenue + incentive_revenue
    annual_payments = annual_loan_pmts + down_payment_cost/df.loan_term_yrs[:, np.newaxis]
    yearly_bill_savings = tpo_revenue + annual_payments
    monthly_bill_savings = yearly_bill_savings/12
    yearly_bills_without_system = df.first_year_bill_without_system[:,np.newaxis] * np.array(list(df['rate_escalations']), dtype = 'float64')
    percent_monthly_bill_savings = yearly_bill_savings/yearly_bills_without_system
    avg_percent_monthly_bill_savings = percent_monthly_bill_savings.sum(axis = 1)/df.loan_term_yrs
    
    # If monthly_bill_savings is zero, percent_mbs will be non-finite
    avg_percent_monthly_bill_savings = np.where(df.first_year_bill_without_system.values == 0, 0, avg_percent_monthly_bill_savings)
    df['monthly_bill_savings'] = monthly_bill_savings.mean(axis = 1)
    df['percent_monthly_bill_savings'] = avg_percent_monthly_bill_savings
    
    # overwrite the values for first_year_bill_without_system and with system
    # to account for the first year rate escalation (the original values are always based on year = 2014)
    df.loc[:, 'first_year_bill_without_system'] = yearly_bills_without_system[:, 0] 
    df.loc[:, 'first_year_bill_with_system'] = df.first_year_bill_with_system * np.array(list(df['rate_escalations']), dtype = 'float64')[:, 0]
    # also adjust the avg cost of elec
    df.loc[:, 'cost_of_elec_dols_per_kwh'] = np.where(df['load_kwh_per_customer_in_bin'] == 0, 
                                                      np.nan, 
                                                      df['first_year_bill_without_system']/df['load_kwh_per_customer_in_bin']) 
    
    new_cols = ['total_value_of_incentives', 'monthly_bill_savings', 'percent_monthly_bill_savings']
    out_cols = in_cols + new_cols
    out_df = df[out_cols]    
    
    
    return revenue, costs, cfs, out_df

#==============================================================================    
def calc_lcoe(df, inflation_rate, econ_life = 20):
    ''' LCOE calculation, following ATB assumptions. There will be some small differences
    since the model is already in real terms and doesn't need conversion of nominal terms
    
    IN: df
        deprec schedule
        inflation rate
        econ life -- investment horizon, may be different than system lifetime.
    
    OUT: lcoe - numpy array - Levelized cost of energy (c/kWh) 
    '''
    
    # extract a list of the input columns
    in_cols = df.columns.tolist()
    
    df['IR'] = inflation_rate
    df['DF'] = 1 - df['down_payment']
    df['CoE'] = df['discount_rate']
    df['CoD'] = df['loan_rate']
    df['TR'] = df['tax_rate']
    
    
    df['WACC'] = ((1 + ((1-df['DF'])*((1+df['CoE'])*(1+df['IR'])-1)) + (df['DF'] * ((1+df['CoD'])*(1+df['IR']) - 1) *  (1 - df['TR'])))/(1 + df['IR'])) -1
    df['CRF'] = (df['WACC'])/ (1 - (1/(1+df['WACC'])**econ_life))# real crf
    
    #df = df.merge(deprec_schedule, how = 'left', on = ['tech','year'])
    df['PVD'] = calc_npv(np.array(list(df['deprec'])),((1+df['WACC'] * 1+ df['IR'])-1)) # Discount rate used for depreciation is 1 - (WACC + 1)(Inflation + 1)
    df['PVD'] /= (1 + df['WACC']) # In calc_npv we assume first entry of an array corresponds to year zero; the first entry of the depreciation schedule is for the first year, so we need to discount the PVD by one additional year
    
    df['PFF'] = (1 - df['TR'] * df['PVD'])/(1 - df['TR'])#project finance factor
    df['CFF'] = 1 # construction finance factor -- cost of capital during construction, assume projects are built overnight, which is not true for larger systems   
    df['OCC'] = df['installed_costs_dollars_per_kw'] # overnight capital cost $/kW
    df['GCC'] = 0 # grid connection cost $/kW, assume cost of interconnecting included in OCC
    df['FOM']  = df['fixed_om_dollars_per_kw_per_yr'] # fixed o&m $/kW-yr
    df['CF'] = df['aep']/df['system_size_kw']/8760 # capacity factor
    df['VOM'] = df['variable_om_dollars_per_kwh'] #variable O&M $/kWh
    
    df['lcoe'] = 100 * (((df['CRF'] * df['PFF'] * df['CFF'] * (df['OCC'] * 1 + df['GCC']) + df['FOM'])/(df['CF'] * 8760)) + df['VOM'])# LCOE 2014c/kWh
    
    out_cols = in_cols + ['lcoe']    
    
    return df[out_cols]
   
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
    # suppress errors due to irrs of nan
    with np.errstate(invalid = 'ignore'):
        irrs = np.where(irrs<=0,1e-6,irrs)
    ttd = np.log(2) / np.log(1 + irrs)
    ttd[ttd <= 0] = 0
    ttd[ttd > 30] = 30.1
    # also deal with ttd of nan by setting to max payback period (this should only occur when cashflows = 0)
    if not np.all(np.isnan(ttd) == np.all(cfs == 0, axis = 1)):
        raise Exception("np.nan found in ttd for non-zero cashflows")
    ttd[np.isnan(ttd)] = 30.1
    
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
    
def calc_payback_vectorized(cfs, tech_lifetime):
    '''payback calculator ### VECTORIZE THIS ###
    IN: cfs - numpy array - project cash flows ($/yr)
    OUT: pp - numpy array - interpolated payback period (years)
    '''
    
    years = np.array([np.arange(0, tech_lifetime)] * cfs.shape[0])
    
    cum_cfs = cfs.cumsum(axis = 1)   
    no_payback = np.logical_or(cum_cfs[:, -1] <= 0, np.all(cum_cfs <= 0, axis = 1))
    instant_payback = np.all(cum_cfs > 0, axis = 1)
    neg_to_pos_years = np.diff(np.sign(cum_cfs)) > 0
    base_years = np.amax(np.where(neg_to_pos_years, years, -1), axis = 1)
    # replace values of -1 with 30
    base_years_fix = np.where(base_years == -1, tech_lifetime - 1, base_years)
    base_year_mask = years == base_years_fix[:, np.newaxis]
    # base year values
    base_year_values = cum_cfs[:, :-1][base_year_mask]
    next_year_values = cum_cfs[:, 1:][base_year_mask]
    frac_years = base_year_values/(base_year_values - next_year_values)
    pp_year = base_years_fix + frac_years
    pp_precise = np.where(no_payback, 30.1, np.where(instant_payback, 0, pp_year))
    
    # round to nearest 0.1 to join with max_market_share
    pp_final = np.array(pp_precise).round(decimals =1)
    
    
    return pp_final
    
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
    
    # where the implied irr exceeds 0.5, simply cap it at 0.5
    r = np.where(irr.mask * (negative_irrs == False), 0.5, r)

    # where cashflows are all zero, set irr to nan
    r = np.where(np.all(cfs == 0, axis = 1), np.nan, r)
        
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
    # calculate payback period
    payback = calc_payback_vectorized(cfs, tech_lifetime)
    # calculate time to double
    ttd = calc_ttd(cfs)
    
    """ MBS is calculated in the calc_cashflows function using this method:
    Annual bill savings = [First year bill w/o tech] - [First year bill w/ tech] - [Avg. annual system payment]
    Monthly bill savings = Annual bill savings / 12
    Percent Monthly Bill Savings = Annual bill savings/ First year bill w/o tech

    Where First year bill w/o tech and  First year bill are outputs of calling SAM
    and
    Where Avg. annual system payment = Sum(down payment + monthly system payment (aka loan_cost)) / loan term (20 years for TPO and 15 for HO)
    """ 

    metric_value = np.where(df.business_model == 'tpo',df.percent_monthly_bill_savings, np.where((df.sector_abbr == 'ind') | (df.sector_abbr == 'com'),ttd,payback))

    return metric_value    
#==============================================================================