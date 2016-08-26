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



#%%
@decorators.fn_timer(logger = logger, tab_level = 3, prefix = '')
def calculate_max_market_share(dataframe, max_market_share):

    # Convert metric value to integer as a primary key, then bound within max market share ranges
    max_payback = max_market_share[max_market_share.metric == 'payback_period'].metric_value.max()
    min_payback = max_market_share[max_market_share.metric == 'payback_period'].metric_value.min()
    max_mbs = max_market_share[max_market_share.metric == 'percent_monthly_bill_savings'].metric_value.max()
    min_mbs = max_market_share[max_market_share.metric == 'percent_monthly_bill_savings'].metric_value.min()
    
    # copy the metric valeus to a new column to store an edited version
    metric_value_bounded = dataframe['metric_value_precise'].values.copy()
    
    # where the metric value exceeds the corresponding max market curve bounds, set the value to the corresponding bound
    metric_value_bounded[np.where((dataframe['metric'] == 'payback_period') & (dataframe['metric_value_precise'] < min_payback))] = min_payback
    metric_value_bounded[np.where((dataframe['metric'] == 'payback_period') & (dataframe['metric_value_precise'] > max_payback))] = max_payback    
    metric_value_bounded[np.where((dataframe['metric'] == 'percent_monthly_bill_savings') & (dataframe['metric_value_precise'] < min_mbs))] = min_mbs
    metric_value_bounded[np.where((dataframe['metric'] == 'percent_monthly_bill_savings') & (dataframe['metric_value_precise'] > max_mbs))] = max_mbs
    dataframe['metric_value_bounded'] = metric_value_bounded

    # scale and round to nearest int    
    dataframe['metric_value_as_factor'] = (dataframe['metric_value_bounded'] * 100).round().astype('int')
    # add a scaled key to the max_market_share df too
    max_market_share['metric_value_as_factor'] = (max_market_share['metric_value'] * 100).round().astype('int')

    # Join the max_market_share table and df in order to select the ultimate mms based on the metric value. 
    dataframe = pd.merge(dataframe, max_market_share, how = 'left', on = ['sector_abbr', 'metric', 'metric_value_as_factor', 'business_model'])
    
    # Derate the maximum market share for commercial and industrial customers in leased buildings by (1/3)
    # based on the owner occupancy status (1 = owner-occupied, 2 = leased)
    dataframe['max_market_share'] = np.where(dataframe['owner_occupied'] == True, dataframe['max_market_share']/3, dataframe['max_market_share'])
    
    return dataframe
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_cashflows(df, tech, analysis_period = 30):


    # extract a list of the input columns
    in_cols = df.columns.tolist()

    installed_cost_column = '%s_installed_costs_dlrs' % tech
    incentives_column = '%s_total_value_of_incentives' % tech
    fixed_om_cost_column = '%s_fixed_om_dlrs_per_year' % tech
    system_degradation_column = '%s_ann_system_degradation' % tech
    site_natgas_consumption_column = '%s_site_natgas_per_building_kwh' % tech
    site_elec_consumption_column = '%s_site_elec_per_building_kwh' % tech

    ho_cashflows_column = '%s_ho_cashflows' % tech
    ho_costs_column = '%s_ho_costs' % tech
    ho_revenue_column = '%s_ho_revenue' % tech

    tpo_cashflows_column = '%s_tpo_cashflows' % tech
    tpo_costs_column = '%s_tpo_costs' % tech
    tpo_revenue_column = '%s_tpo_revenue' % tech    
    
    energy_costs_column = '%s_avg_annual_energy_costs_dlrs' % tech
    
    if tech == 'ghp':
        replacement_part_cost_column = 'ghp_heat_pump_cost_dlrs'
        replacement_part_lifetime_column = 'ghp_heat_pump_lifetime_yrs'
    elif tech == 'baseline':
        replacement_part_cost_column ='baseline_equipment_costs_dlrs'
        replacement_part_lifetime_column = 'baseline_system_lifetime_yrs'        
    

    # default is 30 year analysis period
    shape = (len(df), analysis_period)
    
    df['ic'] = df[installed_cost_column]
    
    if tech == 'ghp':        
        # Remove NAs if not rebate are passed in input sheet   
        df['ptc_length'] = df['ptc_length'].fillna(0)
        df['ptc_length'] = df['ptc_length'].astype(int)
        df['value_of_ptc'] = df['value_of_ptc'].fillna(0)
        df['pbi_fit_length'] = df['pbi_fit_length'].fillna(0)
        df['pbi_fit_length'] = df['pbi_fit_length'].astype(int)
        df['value_of_pbi_fit'] = df['value_of_pbi_fit'].fillna(0)
        df['value_of_tax_credit_or_deduction'] = df['value_of_tax_credit_or_deduction'].fillna(0)
        df['value_of_rebate'] = df['value_of_rebate'].fillna(0)
        df['value_of_increment'] = df['value_of_increment'].fillna(0)
        df['value_of_rebate'] = df['value_of_rebate'].fillna(0)

        # Constrain incentive to max incentive fraction
        df[incentives_column] = np.minimum(df['max_incentive_fraction'] * df['ic'], df['value_of_increment'] + df['value_of_rebate'] + df['value_of_tax_credit_or_deduction'] + df['value_of_itc'])
        
        # calculate max allowable depreciation
        max_depreciation_reduction = np.minimum(df[incentives_column], df['value_of_tax_credit_or_deduction']  + df['value_of_rebate'])
        
        # revenue from other incentives
        incentive_revenue = np.zeros(shape)
        # cap value of ptc and pbi fit
        remainder_for_incentive_revenues = np.maximum((df['max_incentive_fraction'] * df['ic']) - df[incentives_column], 0)
        df['adj_value_of_ptc'] = np.minimum(remainder_for_incentive_revenues/df['ptc_length'], df['value_of_ptc'] * df['ptc_length'])
        df['adj_value_of_ptc'] = df['adj_value_of_ptc'].fillna(0)
        remainder_for_incentive_revenues = np.maximum(remainder_for_incentive_revenues - df['adj_value_of_ptc'] * df['ptc_length'], 0)
        df['adj_value_of_pbi_fit'] = np.minimum(remainder_for_incentive_revenues/df['pbi_fit_length'], df['value_of_pbi_fit'] * df['pbi_fit_length'])
        df['adj_value_of_pbi_fit'] = df['adj_value_of_pbi_fit'].fillna(0)
        ptc_revenue = datfunc.fill_jagged_array(df['adj_value_of_ptc'], df.ptc_length, cols = analysis_period)
        pbi_fit_revenue = datfunc.fill_jagged_array(df['adj_value_of_pbi_fit'], df.pbi_fit_length, cols = analysis_period)    
        incentive_revenue += ptc_revenue + pbi_fit_revenue
        
    elif tech == 'baseline':
        df[incentives_column] = 0.
        max_depreciation_reduction = 0.
        incentive_revenue = 0.
    
    ## COSTS    
    
    # 1)  Cost of servicing loan/leasing payments
    df['crf'] = (df['loan_rate'] * (1 + df['loan_rate'])**df['loan_term_yrs']) / ( (1 + df['loan_rate'])**df['loan_term_yrs'] - 1)
    
    # Assume that incentives received in first year are directly credited against installed cost; This help avoid
    # ITC cash flow imbalances in first year
    df['net_installed_cost'] = df['ic'] - df[incentives_column]
    
    # Calculate the annual payment net the downpayment and upfront incentives
    pmt = - (1 - df['down_payment']) * df['net_installed_cost'] * df['crf']    
    annual_loan_pmts = datfunc.fill_jagged_array(pmt, df['loan_term_yrs'], cols = analysis_period)

    # Pay the down payment in year zero and loan payments thereafter. The downpayment is added at
    # the end of the cash flow calculations to make the year zero framing simpler
    down_payment_cost = (-df['net_installed_cost'] * df['down_payment'])[:,np.newaxis] 
    
    # replacement part costs
    replacement_part_costs = np.zeros(shape)
    # Annualized (undiscounted) inverter replacement cost $/year (includes system size). Applied from year 10 onwards since assume initial 10-year warranty
    with np.errstate(invalid = 'ignore'):        
        replacement_part_costs_amortized  = df[replacement_part_cost_column] / df[replacement_part_lifetime_column]
    replacement_part_costs[:, 10:] = -replacement_part_costs_amortized[:, np.newaxis]
    
    # 2) Costs of fixed & variable O&M. O&M costs are tax deductible for commerical entitites
    om_cost = np.zeros(shape);
    om_cost[:] +=  (-df[fixed_om_cost_column])[:,np.newaxis]
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
    annual_energy_costs_dlrs = df[site_natgas_consumption_column][:, np.newaxis] * np.array(df['dlrs_per_kwh_natgas'].tolist(), dtype = 'float64') + df[site_elec_consumption_column][:, np.newaxis] * np.array(df['dlrs_per_kwh_elec'].tolist(), dtype = 'float64')
    df[energy_costs_column] = np.mean(annual_energy_costs_dlrs, axis = 1)

    # add to the the revenue to account for system degradation.
    system_degradation_factor = np.empty(shape)
    system_degradation_factor[:, 0] = 1
    system_degradation_factor[:, 1:]  = 1 + df[system_degradation_column][:, np.newaxis]
    system_degradation_factor = system_degradation_factor.cumprod(axis = 1)
    
    annual_energy_costs_dlrs *= system_degradation_factor
    
    # Since energy expenses are tax deductible for commercial & industrial 
    # entities, the net annual energy costs from a system is reduced by the marginal tax rate
    annual_energy_costs_dlrs *= (1 - df.tax_rate[:,np.newaxis] * ((df.sector_abbr == 'ind') | (df.sector_abbr == 'com'))[:,np.newaxis])
    
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
    
    #TODO: add carbon tax revenue
    carbon_tax_revenue = np.empty(shape)
    #carbon_tax_revenue[:] = (df.carbon_price_cents_per_kwh * df.aep)[:,np.newaxis] * system_degradation_factor / 100
    carbon_tax_revenue[:] = 0.
        
    '''
    5) Revenue from depreciation.  
    Depreciable basis is installed cost less tax incentives
    Revenue comes from taxable deduction [basis * tax rate * schedule] and cannot be monetized by Residential
    '''
    depreciation_revenue = np.zeros(shape)
    deprec_basis = np.maximum(df['ic'] - 0.5 * (max_depreciation_reduction), 0)[:, np.newaxis] # depreciable basis reduced by half the incentive
    deprec_schedule_arr = np.array(list(df['deprec']))    
    depreciation_revenue[:, :20] = deprec_basis * deprec_schedule_arr * df['tax_rate'][:, np.newaxis] * ((df['sector_abbr'] == 'ind') | (df['sector_abbr'] == 'com') | (df['business_model'] == 'tpo'))[:, np.newaxis]

    '''
    6) Interest paid on loans is tax-deductible for commercial & industrial users; 
    assume can fully monetize. Assume that third-party owners finance system all-cash--thus no interest to deduct. 
    '''
    
    # Calc interest paid on serving the loan
    interest_paid = calc_interest_pmt_schedule(df, analysis_period)
    interest_on_loan_pmts_revenue = interest_paid * df['tax_rate'][:,np.newaxis] * (((df['sector_abbr'] == 'ind') | (df['sector_abbr'] == 'com')) & (df['business_model'] == 'host_owned'))[:, np.newaxis]
    
    # calculate total revenue
    ho_revenue = carbon_tax_revenue + depreciation_revenue + interest_on_loan_pmts_revenue + incentive_revenue
    ho_revenue = np.hstack((np.zeros((len(df), 1)), ho_revenue)) # Add a zero column to revenues to reflect year zero
    ho_costs = -annual_energy_costs_dlrs + annual_loan_pmts + om_cost + replacement_part_costs
    ho_costs = np.hstack((down_payment_cost, ho_costs)) # Down payment occurs in year zero
    ho_cashflows = ho_revenue + ho_costs
    
#    # Calculate the avg  and avg pct monthly bill savings (accounting for rate escalations, generation revenue, and average over all years)
    if tech == 'ghp':
        tpo_revenue = carbon_tax_revenue + incentive_revenue
        tpo_revenue = np.hstack((np.zeros((len(df), 1)), tpo_revenue)) # Add a zero column to revenues to reflect year zero        
        tpo_costs = -annual_energy_costs_dlrs + annual_loan_pmts + down_payment_cost / df['loan_term_yrs'][:, np.newaxis]
        tpo_costs = np.hstack((np.zeros((len(df), 1)), tpo_costs)) # No down payment, but need to reflect year zero anyway
        tpo_cashflows = tpo_revenue + tpo_costs
    elif tech == 'baseline':
        tpo_revenue = np.array(np.nan, dtype = 'float64')
        tpo_costs = np.array(np.nan, dtype = 'float64')
        tpo_cashflows = np.array(np.nan, dtype = 'float64')
        

    df[ho_cashflows_column] = ho_cashflows.tolist()
    df[ho_revenue_column] = ho_revenue.tolist()
    df[ho_costs_column] = ho_costs.tolist()


    df[tpo_cashflows_column] = tpo_cashflows.tolist()
    df[tpo_revenue_column] = tpo_revenue.tolist()
    df[tpo_costs_column] = tpo_costs.tolist()
    
    
    out_cols = [    incentives_column, 
                    energy_costs_column,
                    ho_cashflows_column, 
                    ho_revenue_column,
                    ho_costs_column,
                    tpo_cashflows_column, 
                    tpo_revenue_column,
                    tpo_costs_column                
                ]
    return_cols = in_cols + out_cols
    df = df[return_cols]    
    
    
    return df


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_net_cashflows_host_owned(dataframe):
       
    dataframe['net_cashflows_ho'] = (np.array(dataframe['ghp_ho_cashflows'].tolist(), dtype = 'float64') - np.array(dataframe['baseline_ho_cashflows'].tolist(), dtype = 'float64')).tolist()

    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_net_cashflows_third_party_owned(dataframe):
       
    dataframe['net_cashflows_tpo'] = (np.array(dataframe['ghp_tpo_cashflows'].tolist(), dtype = 'float64') - np.array(dataframe['baseline_ho_cashflows'].tolist(), dtype = 'float64')).tolist()

    return dataframe
 
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_monthly_bill_savings(dataframe):   

    
    dataframe['avg_annual_net_cashflow_tpo'] = np.mean(np.array(dataframe['net_cashflows_tpo'].tolist(), dtype = 'float64'), axis = 1)
    # absolute monthly bill savings are the annual average net cashflow spread over 12 months
    dataframe['monthly_bill_savings'] = dataframe['avg_annual_net_cashflow_tpo'] / 12.
    # the percent monthly bill savings is simply the average net cashflow divided by the original energy costs of the consumer
    # (use annual numbers because they should be same as monthly (if you divide both by 12 and then divide, it's the same as dividing the originals))
    dataframe['percent_monthly_bill_savings'] = np.where(dataframe['baseline_avg_annual_energy_costs_dlrs'] == 0, 0., dataframe['avg_annual_net_cashflow_tpo'] / dataframe['baseline_avg_annual_energy_costs_dlrs'])
    
    return dataframe
    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_payback(dataframe, cashflow_column, analysis_period):

    cashflows = np.array(dataframe[cashflow_column].tolist(), dtype = 'float64')

    years = np.array([np.arange(0, analysis_period)] * cashflows.shape[0])
    
    cum_cfs = cashflows.cumsum(axis = 1)   
    no_payback = np.logical_or(cum_cfs[:, -1] <= 0, np.all(cum_cfs <= 0, axis = 1))
    instant_payback = np.all(cum_cfs > 0, axis = 1)
    neg_to_pos_years = np.diff(np.sign(cum_cfs)) > 0
    base_years = np.amax(np.where(neg_to_pos_years, years, -1), axis = 1)
    # replace values of -1 with 30
    base_years_fix = np.where(base_years == -1, analysis_period - 1, base_years)
    base_year_mask = years == base_years_fix[:, np.newaxis]
    # base year values
    base_year_values = cum_cfs[:, :-1][base_year_mask]
    next_year_values = cum_cfs[:, 1:][base_year_mask]
    frac_years = base_year_values/(base_year_values - next_year_values)
    pp_year = base_years_fix + frac_years
    pp_precise = np.where(no_payback, 30.1, np.where(instant_payback, 0, pp_year))
    
    # round to nearest 0.1 to join with max_market_share
    pp_final = np.array(pp_precise).round(decimals =1)
    
    dataframe['payback_period'] = pp_final
    
    return dataframe    


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_ttd(dataframe, cashflow_column):
    ''' Calculate time to double investment based on the MIRR. This is used for
    the commercial and industrial sectors.
    
    IN: cfs - numpy array - project cash flows ($/yr)

    OUT: ttd - numpy array - Time to double investment (years) 
    
    '''
    cashflows = np.array(dataframe[cashflow_column].tolist(), dtype = 'float64')    
    
    irrs = virr(cashflows, precision = 0.005, rmin = 0, rmax1 = 0.3, rmax2 = 0.5)
    irrs = np.where(irrs<=0,1e-6,irrs)
    ttd = np.log(2) / np.log(1 + irrs)
    ttd[ttd <= 0] = 0
    ttd[ttd > 30] = 30.1
    # also deal with ttd of nan by setting to max payback period (this should only occur when cashflows = 0)
    if not np.all(np.isnan(ttd) == np.all(cashflows == 0, axis = 1)):
        raise Exception("np.nan found in ttd for non-zero cashflows")
    ttd[np.isnan(ttd)] = 30.1
    # round results to nearest 0.1 (to join with max market share lkup)    
    dataframe['ttd'] = ttd.round(decimals = 1)    
    
    return dataframe

    
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def assign_metric_value_precise(dataframe):
    
    dataframe['metric_value_precise'] = np.where(dataframe['business_model'] == 'tpo',
                                                 dataframe['percent_monthly_bill_savings'], 
                                                 np.where((dataframe['sector_abbr'] == 'ind') | (dataframe['sector_abbr'] == 'com'),
                                                           dataframe['ttd'],
                                                           dataframe['payback_period']
                                                         )
                                                )

    return dataframe
    
    
#%%    
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_npv(dataframe, cashflow_column, discount_rate_val_or_column, out_col):
    ''' Vectorized NPV calculation based on (m x n) cashflows and (n x 1) 
    discount rate
    
    IN: cfs - numpy array - project cash flows ($/yr)
        dr  - numpy array - annual discount rate (decimal)
        
    OUT: npv - numpy array - net present value of cash flows ($) 
    
    '''
    
    if discount_rate_val_or_column.__class__ == str:
        discount_rates = dataframe[discount_rate_val_or_column][:, np.newaxis]
    elif discount_rate_val_or_column.__class__ == float:
        discount_rates = np.array([discount_rate_val_or_column], dtype = 'float64')[:, np.newaxis]
    
    cashflows = np.array(dataframe[cashflow_column].tolist(), dtype = 'float64')
    tmp = np.empty(cashflows.shape)
    tmp[:,0] = 1
    tmp[:,1:] = 1 / (1 + discount_rates)
    drm = np.cumprod(tmp, axis = 1)        
    npv = (drm * cashflows).sum(axis = 1)   
    
    dataframe[out_col] = npv

    return dataframe
    
#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def normalize_value(dataframe, value_column, normalization_column, out_column):
    
    with np.errstate(invalid = 'ignore'):
        dataframe[out_column] = np.where(dataframe[normalization_column] == 0, 0., dataframe[value_column] / dataframe[normalization_column])
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def assign_value(dataframe, value, out_column):

    dataframe[out_column] = value
    
    return dataframe


#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calculate_lcoe(dataframe):
    ''' LCOE calculation, following ATB assumptions. There will be some small differences
    since the model is already in real terms and doesn't need conversion of nominal terms
    
    IN: df
        deprec schedule
        inflation rate
        econ life -- investment horizon, may be different than system lifetime.
    
    OUT: lcoe - numpy array - Levelized cost of energy (c/kWh) 
    '''
    
    #TODO: revise this function to work for energy savings
    # extract a list of the input columns
    in_cols = dataframe.columns.tolist()
    
    # inflation rate
    dataframe['IR'] = dataframe['inflation_rate']
    # debt fraction
    dataframe['DF'] = 1 - dataframe['down_payment']
    # cost of equity
    dataframe['CoE'] = dataframe['discount_rate']
    # cost of debt
    dataframe['CoD'] = dataframe['loan_rate']
    # tax rate
    dataframe['TR'] = dataframe['tax_rate']
    
    
    # weighted average cost of capital (~avg of cost of debt and cost of equity)
    dataframe['WACC'] = ((1 + ((1-dataframe['DF'])*((1+dataframe['CoE'])*(1+dataframe['IR'])-1)) + (dataframe['DF'] * ((1+dataframe['CoD'])*(1+dataframe['IR']) - 1) *  (1 - dataframe['TR'])))/(1 + dataframe['IR'])) -1
    # capital recovery factor
    dataframe['CRF'] = (dataframe['WACC'])/ (1 - (1/(1+dataframe['WACC'])**dataframe['loan_term_yrs']))# real crf
    
    # discount rate used for depreciation calculations
    dataframe['DR'] = (1+dataframe['WACC'] * 1 + dataframe['IR'])-1 # Discount rate used for depreciation is 1 - (WACC + 1)(Inflation + 1)
    # present value of depreciation (simplified version of cashflows)
    dataframe['PVD'] = calculate_npv(dataframe, 'deprec', 'DR', 'PVD')
    dataframe['PVD'] /= (1 + dataframe['WACC']) # In calc_npv we assume first entry of an array corresponds to year zero; the first entry of the depreciation schedule is for the first year, so we need to discount the PVD by one additional year
    
    # project finance factor
    dataframe['PFF'] = (1 - dataframe['TR'] * dataframe['PVD'])/(1 - dataframe['TR'])
    dataframe['CFF'] = 1 # construction finance factor -- cost of capital during construction, assume projects are built overnight, which is not true for larger systems   
    # overnight capital cost $/kW
    dataframe['OCC'] = dataframe['installed_costs_dollars_per_kw']
    # grid connection cost $/kW, assume cost of interconnecting included in OCC
    dataframe['GCC'] = 0 
    # fixed o&m $/kW-yr
    dataframe['FOM']  = dataframe['fixed_om_dollars_per_kw_per_yr'] 
    # capacity factor
    dataframe['CF'] = dataframe['aep']/dataframe['system_size_kw']/8760
    #variable O&M $/kWh
    dataframe['VOM'] = dataframe['variable_om_dollars_per_kwh'] 
    
    
    dataframe['lcoe'] = 100 * (
                                    (
                                        (dataframe['CRF'] * dataframe['PFF'] * dataframe['CFF'] * 
                                            (dataframe['OCC'] * 1 + dataframe['GCC']) + 
                                            dataframe['FOM']) / (dataframe['CF'] * 8760)
                               ) + dataframe['VOM'])# LCOE 2014c/kWh
    
    
    out_cols = ['lcoe']
    return_cols = in_cols + out_cols
    
    dataframe = dataframe[return_cols]
    dataframe['lcoe'] = np.nan
    
    return dataframe
 
#%%
#==============================================================================
# HELPER FUNCTIONS
#==============================================================================

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
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
    
#%%
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


#%%  