# -*- coding: utf-8 -*-
"""
Functions to construct cashflow timelines for agents and calculate the financial performance of projects.
"""

import numpy as np
np.seterr(divide='ignore', invalid='ignore')
import pandas as pd
import utility_functions as utilfunc
import decorators

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================

#%%
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_financial_performance(dataframe):
    """
    Function to calculate the payback period and join it on the agent dataframe.
    
    Parameters
    ----------
    dataframe : pandas.DataFrame
        Agent dataframe
    
    Returns
    -------
    pandas.DataFrame
        Agent dataframe with `payback_period` joined on dataframe
    """
#    dataframe = dataframe.reset_index()

    cfs = np.vstack(dataframe['cash_flow']).astype(np.float)    

    print '** in calc_financial_performance **'
    
    # calculate payback period
    tech_lifetime = np.shape(cfs)[1] - 1

    print 'tech_lifetime'
    print tech_lifetime

    payback = calc_payback_vectorized(cfs, tech_lifetime)
    # calculate time to double
    ttd = calc_ttd(cfs)

    metric_value = np.where(dataframe['sector_abbr']=='res', payback, ttd)

    dataframe['metric_value'] = metric_value
    
    dataframe = dataframe.set_index('agent_id')
    print '\n'
    return dataframe
    
#%%


@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_max_market_share(dataframe, max_market_share_df):
    """
    Calculates the maximum marketshare available for each agent. 

    Parameters
    ----------
    dataframe : pandas.DataFrame
        Attributes
        ----------
        metric_value : float
            
    max_market_share_df : pandas.DataFrame
        Set by :meth:`settings.ScenarioSettings.get_max_marketshare`.

    Returns
    -------
    pandas.DataFrame
        Input DataFrame with `max_market_share` and `metric` columns joined on.
    """

    in_cols = list(dataframe.columns)
    dataframe = dataframe.reset_index()
    
    dataframe['business_model'] = 'host_owned'
    dataframe['metric'] = 'payback_period'
    
    # Convert metric value to integer as a primary key, then bound within max market share ranges
    max_payback = max_market_share_df[max_market_share_df.metric == 'payback_period'].metric_value.max()
    min_payback = max_market_share_df[max_market_share_df.metric == 'payback_period'].metric_value.min()
    max_mbs = max_market_share_df[max_market_share_df.metric == 'percent_monthly_bill_savings'].metric_value.max()
    min_mbs = max_market_share_df[max_market_share_df.metric == 'percent_monthly_bill_savings'].metric_value.min()
    
    # copy the metric valeus to a new column to store an edited version
    metric_value_bounded = dataframe['metric_value'].values.copy()
    
    # where the metric value exceeds the corresponding max market curve bounds, set the value to the corresponding bound
    metric_value_bounded[np.where((dataframe.metric == 'payback_period') & (dataframe['metric_value'] < min_payback))] = min_payback
    metric_value_bounded[np.where((dataframe.metric == 'payback_period') & (dataframe['metric_value'] > max_payback))] = max_payback    
    metric_value_bounded[np.where((dataframe.metric == 'percent_monthly_bill_savings') & (dataframe['metric_value'] < min_mbs))] = min_mbs
    metric_value_bounded[np.where((dataframe.metric == 'percent_monthly_bill_savings') & (dataframe['metric_value'] > max_mbs))] = max_mbs
    dataframe['metric_value_bounded'] = metric_value_bounded

    # scale and round to nearest int    
    dataframe['metric_value_as_factor'] = [int(round(i,1) * 100) for i in dataframe['metric_value_bounded']]
    # add a scaled key to the max_market_share dataframe too
    max_market_share_df['metric_value_as_factor'] = [int(round(float(i), 1) * 100) for i in max_market_share_df['metric_value']]

    # Join the max_market_share table and dataframe in order to select the ultimate mms based on the metric value. 
    dataframe = pd.merge(dataframe, max_market_share_df[['sector_abbr', 'max_market_share','metric_value_as_factor', 'metric', 'business_model']], how = 'left', on = ['sector_abbr','metric_value_as_factor','metric', 'business_model'])

    # Derate the maximum market share for commercial and industrial customers in leased buildings by (2/3)
    # based on the owner occupancy status (1 = owner-occupied, 2 = leased)
    dataframe['max_market_share'] = np.where(dataframe.owner_occupancy_status == 2, dataframe['max_market_share']/3,dataframe['max_market_share'])
    
    # out_cols = in_cols + ['max_market_share', 'metric']    
    out_cols = in_cols + ['max_market_share', 'metric_value_as_factor', 'metric', 'metric_value_bounded']
    dataframe = dataframe.set_index('agent_id')

    return dataframe[out_cols]

def calc_ttd(cfs):
    """
    Calculate time to double investment based on the MIRR. 
    
    This is used for the commercial and industrial sectors.
    
    Parameters
    ----------
    cfs : numpy.ndarray
        Project cash flows ($/yr).

    Returns
    -------
    ttd : numpy.ndarray
        Time to double investment (years).
    """
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

#%%
def cashflow_constructor(bill_savings, 
                         pv_size, pv_price, pv_om,
                         batt_cap, batt_power, 
                         batt_cost_per_kw, batt_cost_per_kwh, 
                         batt_om_per_kw, batt_om_per_kwh,
                         batt_chg_frac,
                         sector, itc, deprec_sched, 
                         fed_tax_rate, state_tax_rate, real_d,  
                         analysis_years, inflation, 
                         down_payment_fraction, loan_rate, loan_term, 
                         cash_incentives=np.array([0]), ibi=np.array([0]), cbi=np.array([0]), pbi=np.array([[0]]), print_statements = False):
    """
    Calculate the system cash flows based on the capex, opex, bill savings, incentives, tax implications, and other factors
    
    Parameters
    ----------
    bill_savings : "numpy.ndarray"
        Annual bill savings ($/yr) from system adoption from 1st year through system lifetime
    pv_size : "numpy.float64"
        system capacity selected by agent (kW)
    pv_price : "float"
        system capex ($/kW)
    pv_om : "float"
        system operation and maintanence cost ($/kW)
    batt_cap : "numpy.float64"
        energy capacity of battery selected (kWh)
    batt_power : "numpy.float64"
        demand capacity of battery selected (kW)
    batt_cost_per_kw : "float"
        capex of battery per kW installed ($/kW)
    batt_cost_per_kwh : "float"
        capex of battery per kWh installed ($/kWh)
    batt_om_per_kw : "float"
        opex of battery per kW installed ($/kW-yr)
    batt_om_per_kwh : "float"
        opex of battery per kW installed ($/kWh-yr)
    batt_chg_frac : "int"
        fraction of the battery's energy that it gets from a co-hosted PV system. Used for ITC calculation.
    sector : "str"
        agent sector
    itc : "float"
        fraction of capex offset by federal investment tax credit
    deprec_sched : "list"
        fraction of capex eligible for tax-based depreciation
    fed_tax_rate : "float"
        average tax rate as fraction from federal taxes
    state_tax_rate : "int"
        average tax rate as fraction from state taxes
    real_d : "float"
        annua discount rate in real terms
    analysis_years : "int"
        number of years to use in economic analysis
    inflation : "float"
        annual average inflation rate as fraction e.g. 0.025
    down_payment_fraction : "int"
        fraction of capex used as system down payment
    loan_rate_real : "float"
        real interest rate for debt payments
    loan_term : "int"
        number of years for loan term
    cash_incentives : "numpy.ndarray"
        array describing eligible cash-based incentives e.g. $
    ibi : "numpy.ndarray"
        array describing eligible investment-based incentives e.g. 0.2
    cbi : "numpy.ndarray"
        array describing eligible one-time capacity-based incentives e.g. $/kW
    pbi : "numpy.ndarray"
        array describing eligible ongoing performance-based incentives e.g $/kWh-yr
    
    Returns
    -------
    cf : 'dtype
        Annual cash flows of project investment ($/yr)
    cf_discounted : 'dtype'
        Annual discounted cash flows of project investment ($/yr)
    npv : 'dtype'
        Net present value ($) of project investment using WACC
    bill_savings : 'dtype'
        Nominal cash flow of the annual bill savings over the lifetime of the system
    after_tax_bill_savings : 'dtype'
        Effective after-tax bill savings (electricity costs are tax-deductible for commercial entities)
    pv_cost : 'dtype'
        Capex of system in ($)
    batt_cost : 'dtype'
        Capex of battery in ($)
    installed_cost : 'dtype'
        Combined capex of system + battery
    up_front_cost : 'dtype
        Capex in 0th year as down payment
    batt_om_cf : 'dtype'
        Annual cashflows of battery opex
    operating_expenses : 'dtype'
        Combined annual opex of system + battery ($/yr) 
    pv_itc_value : 'dtype'
        Absolute value of investment tax credit for system ($)
    batt_itc_value : 'dtype'
        Absolute value of investment tax credit for battery ($)
    itc_value : 'dtype'
        Absolute value of investment tax credit for combined system + battery ($)
    deprec_basis : 'dtype'
        Absolute value of depreciable basis of system ($)
    deprec_deductions : 'dtype'
        Annual amount of depreciable capital in given year ($) 
    initial_debt : 'dtype'
        Amount of debt for loan ($)
    annual_principal_and_interest_payment : 'dtype'
        Annual amount of debt service payment, principal + interest ($)
    debt_balance : 'dtype'
        Annual amount of debt remaining in given year ($)
    interest_payments : 'dtype'
        Annual amount of interest payment in given year ($)
    principal_and_interest_payments : 'dtype'
        Array of annual principal and interest payments ($)
    total_taxable_income : 'dtype'
        Amount of stateincome from incentives eligible for taxes
    state_deductions : 'dtype'
        Reduction to state taxable income from interest, operating expenses, or bill savings depending on sector
    total_taxable_state_income_less_deductions : 'dtype'
        Total taxable state income less any applicable deductions
    state_income_taxes : 'dtype'
        Amount of state income tax i.e. net taxable income by tax rate
    fed_deductions : 'dtype'
        Reduction to federal taxable income from interest, operating expenses, or bill savings depending on sector
    total_taxable_fed_income_less_deductions : 'dtype'
        Total taxable federal income less any applicable deductions
    fed_income_taxes : 'dtype'
        Amount of federal income tax i.e. net taxable income by tax rate
    interest_payments_tax_savings : 'dtype'
        Amount of tax savings from deductions of interest payments
    operating_expenses_tax_savings : 'dtype'
        Amount of tax savings from deductions of operating expenses
    deprec_deductions_tax_savings : 'dtype'
        Amount of tax savings from deductions of capital depreciation
    elec_OM_deduction_decrease_tax_liability : 'dtype'
        Amount of tax savings from deductions of electricity costs as deductible business expense
    
    Todo
    ----
    1)  Sales tax basis and rate
    2)  note that sales tax goes into depreciable basis
    3)  Propery taxes (res can deduct from income taxes, I think)
    4)  insurance
    5)  add pre-tax cash flow
    6)  add residential mortgage option
    7)  add carbon tax revenue
    8)  More exhaustive checking. I have confirmed basic formulations against SAM, but there are many permutations that haven't been checked.
    9)  make incentives reduce depreciable basis
    10) add a flag for high incentive levels
    11) battery price schedule, for replacements
    12) improve inverter replacement
    13) improve battery replacement
    14) add inflation adjustment for replacement prices
    15) improve deprec schedule handling
    16) Make financing unique to each agent
    17) Make battery replacements depreciation an input, with default of 7 year MACRS
    18) Have a better way to deal with capacity vs effective capacity and battery costs
    19) Make it so it can accept different loan terms
    """

    #################### Massage inputs ########################################
    # If given just a single value for an agent-specific variable, repeat that
    # variable for each agent. This assumes that the variable is intended to be
    # applied to each agent. 
    if np.size(np.shape(bill_savings)) == 1: shape = (1, analysis_years+1)
    else: shape = (np.shape(bill_savings)[0], analysis_years+1)
    n_agents = shape[0]

    if np.size(sector) != n_agents or n_agents==1: sector = np.repeat(sector, n_agents) 
    if np.size(fed_tax_rate) != n_agents or n_agents==1: fed_tax_rate = np.repeat(fed_tax_rate, n_agents) 
    if np.size(state_tax_rate) != n_agents or n_agents==1: state_tax_rate = np.repeat(state_tax_rate, n_agents) 
    if np.size(itc) != n_agents or n_agents==1: itc = np.repeat(itc, n_agents) 
    if np.size(pv_size) != n_agents or n_agents==1: pv_size = np.repeat(pv_size, n_agents) 
    if np.size(pv_price) != n_agents or n_agents==1: pv_price = np.repeat(pv_price, n_agents) 
    if np.size(pv_om) != n_agents or n_agents==1: pv_om = np.repeat(pv_om, n_agents) 
    if np.size(batt_cap) != n_agents or n_agents==1: batt_cap = np.repeat(batt_cap, n_agents) 
    if np.size(batt_power) != n_agents or n_agents==1: batt_power = np.repeat(batt_power, n_agents) 
    if np.size(batt_cost_per_kw) != n_agents or n_agents==1: batt_cost_per_kw = np.repeat(batt_cost_per_kw, n_agents) 
    if np.size(batt_cost_per_kwh) != n_agents or n_agents==1: batt_cost_per_kwh = np.repeat(batt_cost_per_kwh, n_agents) 
    if np.size(batt_chg_frac) != n_agents or n_agents==1: batt_chg_frac = np.repeat(batt_chg_frac, n_agents) 
    if np.size(batt_om_per_kw) != n_agents or n_agents==1: batt_om_per_kw = np.repeat(batt_om_per_kw, n_agents) 
    if np.size(batt_om_per_kwh) != n_agents or n_agents==1: batt_om_per_kwh = np.repeat(batt_om_per_kwh, n_agents) 
    if np.size(real_d) != n_agents or n_agents==1: real_d = np.repeat(real_d, n_agents) 
    if np.size(down_payment_fraction) != n_agents or n_agents==1: down_payment_fraction = np.repeat(down_payment_fraction, n_agents) 
    if np.size(loan_rate) != n_agents or n_agents==1: loan_rate = np.repeat(loan_rate, n_agents) 
    if np.size(ibi) != n_agents or n_agents==1: ibi = np.repeat(ibi, n_agents) 
    if np.size(cbi) != n_agents or n_agents==1: cbi = np.repeat(cbi, n_agents) 
    
    
    if np.size(pbi) != n_agents or n_agents==1: pbi = np.repeat(pbi, n_agents)[:,np.newaxis]
    deprec_sched = np.array([deprec_sched])

    #################### Setup #########################################
    effective_tax_rate = fed_tax_rate * (1 - state_tax_rate) + state_tax_rate

    if print_statements:
        print 'effective_tax_rate'
        print effective_tax_rate
    
    # nom_d = (1 + real_d) * (1 + inflation) - 1

    cf = np.zeros(shape) 
    inflation_adjustment = (1+inflation)**np.arange(analysis_years+1)
    
    #################### Bill Savings #########################################
    # For C&I customers, bill savings are reduced by the effective tax rate,
    # assuming the cost of electricity could have otherwise been counted as an
    # O&M expense to reduce federal and state taxable income.
    bill_savings = bill_savings*inflation_adjustment # Adjust for inflation

    after_tax_bill_savings = np.zeros(shape)
    after_tax_bill_savings = (bill_savings.T * (1 - (sector!='res')*effective_tax_rate)).T # reduce value of savings because they could have otherwise be written off as operating expenses

    cf += bill_savings
    if print_statements:
        print 'bill savings cf'
        print np.sum(cf,1)
        print ' '
    
    #################### Installed Costs ######################################
    # Assumes that cash incentives, IBIs, and CBIs will be monetized in year 0,
    # reducing the up front installed cost that determines debt levels. 

    pv_cost = pv_size*pv_price     # assume pv_price includes initial inverter purchase

    if print_statements:
        print 'pv_cost'
        print pv_cost
        print ' '

    batt_cost = batt_power*batt_cost_per_kw + batt_cap*batt_cost_per_kwh
    installed_cost = pv_cost + batt_cost

    net_installed_cost = installed_cost - cash_incentives - ibi - cbi

    #calculate the wacc in place of nom_d, still need to figure out taxes write offs for mexico!

    wacc = (((down_payment_fraction*net_installed_cost)/net_installed_cost) * real_d) + ((((1-down_payment_fraction)*net_installed_cost)/net_installed_cost) * loan_rate)
    # elif sector[0] is in ['com','ind']:
    #     wacc = (((down_payment_fraction*net_installed_cost)/net_installed_cost) * real_d) + (((((1-down_payment_fraction)*net_installed_cost)/net_installed_cost) * loan_rate)*(1-TAXRATE))
    # print wacc


    up_front_cost = net_installed_cost * down_payment_fraction
    if print_statements:
        print 'wacc'
        print wacc
        print ' '


    cf[:,0] -= net_installed_cost #all installation costs upfront for WACC
    # cf[:,0] -= up_front_cost

    # print 'net_installed_cost' 
    # print net_installed_cost
    # print ' '

    # print 'up front cost'
    # print up_front_cost
    # print ' '

    if print_statements:
        print 'bill savings minus up front cost'
        print np.sum(cf,1)
        print ' '
    
    #################### Operating Expenses ###################################
    # Nominally includes O&M, replacement costs, fuel, insurance, and property 
    # tax - although currently only includes O&M and replacements.
    # All operating expenses increase with inflation
    operating_expenses_cf = np.zeros(shape)
    batt_om_cf = np.zeros(shape)

    # Battery O&M (replacement costs added to base O&M when costs were ingested)
    batt_om_cf[:,1:] = (batt_power*batt_om_per_kw + batt_cap*batt_om_per_kwh).reshape(n_agents, 1)
    
    # PV O&M
    operating_expenses_cf[:,1:] = (pv_om * pv_size).reshape(n_agents, 1)
    
    operating_expenses_cf += batt_om_cf
    operating_expenses_cf = operating_expenses_cf*inflation_adjustment
    cf -= operating_expenses_cf
    
    #################### Federal ITC #########################################
    pv_itc_value = pv_cost * itc
    batt_itc_value = batt_cost * itc * batt_chg_frac * (batt_chg_frac>=0.75)
    itc_value = pv_itc_value + batt_itc_value
    # itc value added in fed_tax_savings_or_liability
    
    

    #################### Depreciation #########################################
    # Per SAM, depreciable basis is sum of total installed cost and total 
    # construction financing costs, less 50% of ITC and any incentives that
    # reduce the depreciable basis.
    deprec_deductions = np.zeros(shape)
    deprec_basis = installed_cost - itc_value*0.5 
    deprec_deductions[:,1:np.size(deprec_sched,1)+1] = (deprec_basis * deprec_sched.T).T
    # to be used later in fed tax calcs
    
    #################### Debt cash flow #######################################
    # Deduct loan interest payments from state & federal income taxes for res 
    # mortgage and C&I. No deduction for res loan.
    # note that the debt balance in year0 is different from principal if there 
    # are any ibi or cbi. Not included here yet.
    # debt balance, interest payment, principal payment, total payment
    
    initial_debt = net_installed_cost - up_front_cost

    if print_statements:
        print 'initial_debt'
        print initial_debt
        print ' '

    annual_principal_and_interest_payment = initial_debt * (loan_rate*(1+loan_rate)**loan_term) / ((1+loan_rate)**loan_term - 1)

    if print_statements:
        print 'annual_principal_and_interest_payment'
        print annual_principal_and_interest_payment
        print ' '

    debt_balance = np.zeros(shape)
    interest_payments = np.zeros(shape)
    principal_and_interest_payments = np.zeros(shape)
    
    debt_balance[:,:loan_term] = (initial_debt*((1+loan_rate.reshape(n_agents,1))**np.arange(loan_term)).T).T - (annual_principal_and_interest_payment*(((1+loan_rate).reshape(n_agents,1)**np.arange(loan_term) - 1.0)/loan_rate.reshape(n_agents,1)).T).T  
    interest_payments[:,1:] = (debt_balance[:,:-1].T * loan_rate).T
    
    if print_statements:
        print 'interest_payments'
        print interest_payments
        print ' '

        print 'sum of interst_payments'
        print np.sum(interest_payments)
        print ' '

        print 'net_installed_cost'
        print net_installed_cost
        print ' '

        print 'sum of net_installed_cost and interest payments'
        print net_installed_cost + np.sum(interest_payments)
        print ' '

    principal_and_interest_payments[:,1:loan_term+1] = annual_principal_and_interest_payment.reshape(n_agents, 1)

    if print_statements:
        print 'principal_and_interest_payments'
        print principal_and_interest_payments
        print ' '

        print 'sum of principal and interest payments, and upfront cost'
        print np.sum(principal_and_interest_payments) + up_front_cost
        print ' '

    # cf -= principal_and_interest_payments
    # cf -= interest_payments  #already included in the WACC

    if print_statements:
        print 'cf minus intrest payments'
        print np.sum(cf,1)
        print ' '
    
        
    #################### State Income Tax #########################################
    # Per SAM, taxable income is CBIs and PBIs (but not IBIs)
    # Assumes no state depreciation
    # Assumes that revenue from DG is not taxable income
    total_taxable_income = np.zeros(shape)
    total_taxable_income[:,1] = cbi
    total_taxable_income[:,:np.shape(pbi)[1]] += pbi
    
    state_deductions = np.zeros(shape)
    state_deductions += (interest_payments.T * (sector!='res')).T
    state_deductions += (operating_expenses_cf.T * (sector!='res')).T
    state_deductions -= (bill_savings.T * (sector!='res')).T
    
    total_taxable_state_income_less_deductions = total_taxable_income - state_deductions
    state_income_taxes = (total_taxable_state_income_less_deductions.T * state_tax_rate).T
    
    state_tax_savings_or_liability = -state_income_taxes
    if print_statements:
        print 'state_tax_savings'
        print state_tax_savings_or_liability
    
    cf += state_tax_savings_or_liability
        
    ################## Federal Income Tax #########################################
    # Assumes all deductions are federal
    fed_deductions = np.zeros(shape)
    fed_deductions += (interest_payments.T * (sector!='res')).T
    fed_deductions += (deprec_deductions.T * (sector!='res')).T
    fed_deductions += state_income_taxes
    fed_deductions += (operating_expenses_cf.T * (sector!='res')).T
    fed_deductions -= (bill_savings.T * (sector!='res')).T
    
    total_taxable_fed_income_less_deductions = total_taxable_income - fed_deductions
    fed_income_taxes = (total_taxable_fed_income_less_deductions.T * fed_tax_rate).T
    
    fed_tax_savings_or_liability_less_itc = -fed_income_taxes
    if print_statements:
        print 'federal_tax_savings'
        print fed_tax_savings_or_liability_less_itc
    
    cf += fed_tax_savings_or_liability_less_itc
    cf[:,1] += itc_value
    
    
    ######################## Packaging tax outputs ############################
    # interest_payments_tax_savings = (interest_payments.T * effective_tax_rate).T
    operating_expenses_tax_savings = (operating_expenses_cf.T * effective_tax_rate).T
    deprec_deductions_tax_savings = (deprec_deductions.T * fed_tax_rate).T    
    elec_OM_deduction_decrease_tax_liability = (bill_savings.T * effective_tax_rate).T
    
    ########################### Post Processing ###############################
      
    powers = np.zeros(shape, int)
    powers[:,:] = np.array(range(analysis_years+1))

    discounts = np.zeros(shape, float)
    discounts[:,:] = (1/(1+wacc)).reshape(n_agents, 1)

    if print_statements:
        print 'discounts'
        print np.mean(discounts,1)
        print ' '

    cf_discounted = cf * np.power(discounts, powers)
    cf_discounted = np.nan_to_num(cf_discounted)

    if print_statements:
        print 'cf not discounted'
        print cf
        print ' ' 

    if print_statements:
        print 'cf_discounted'
        print cf_discounted
        print ' '

    npv = np.sum(cf_discounted, 1)
    
    if print_statements:
        print 'npv'
        print npv
        print ' '

    
    ########################### Package Results ###############################
    
    results = {'cf':cf,
               'cf_discounted':cf_discounted,
               'npv':npv,
               'bill_savings':bill_savings,
               'after_tax_bill_savings':after_tax_bill_savings,
               'pv_cost':pv_cost,
               'batt_cost':batt_cost,
               'installed_cost':installed_cost,
               'up_front_cost':up_front_cost,
               'batt_om_cf':batt_om_cf,              
               'operating_expenses':operating_expenses_cf,
               'pv_itc_value':pv_itc_value,
               'batt_itc_value':batt_itc_value,
               'itc_value':itc_value,
               'deprec_basis':deprec_basis,
               'deprec_deductions':deprec_deductions,
               'initial_debt':initial_debt,
               'annual_principal_and_interest_payment':annual_principal_and_interest_payment,
               'debt_balance':debt_balance,
               'interest_payments':interest_payments,
               'principal_and_interest_payments':principal_and_interest_payments,
               'total_taxable_income':total_taxable_income,
               'state_deductions':state_deductions,
               'total_taxable_state_income_less_deductions':total_taxable_state_income_less_deductions,
               'state_income_taxes':state_income_taxes,
               'fed_deductions':fed_deductions,
               'total_taxable_fed_income_less_deductions':total_taxable_fed_income_less_deductions,
               'fed_income_taxes':fed_income_taxes,
            #    'interest_payments_tax_savings':interest_payments_tax_savings,
               'operating_expenses_tax_savings':operating_expenses_tax_savings,
               'deprec_deductions_tax_savings':deprec_deductions_tax_savings,
               'elec_OM_deduction_decrease_tax_liability':elec_OM_deduction_decrease_tax_liability}

    return results

    
#==============================================================================
     
def calc_payback_vectorized(cfs, tech_lifetime):
    """
    Payback calculator.

    Can be either simple payback or discounted payback, depending on whether
    the input cash flow is discounted.    
    
    Parameters
    ----------
    cfs : numpy.ndarray
        Project cash flows ($/yr).
    tech_lifetime : int
        Lifetime of technology used for project.
    
    Returns
    -------
    pp : numpy.ndarray
        Interpolated payback period (years)
    """
    
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
    pp_precise = np.where(no_payback, tech_lifetime, np.where(instant_payback, 0, pp_year))
    
    pp_final = np.array(pp_precise).round(decimals = 3)
    
    return pp_final
    
#%%
def virr(cfs, precision = 0.005, rmin = 0, rmax1 = 0.3, rmax2 = 0.5):
    """
    Vectorized IRR calculator. 
    
    First calculate a 3D array of the discounted cash flows along cash flow series, time period, and discount rate. Sum over time to 
    collapse to a 2D array which gives the NPV along a range of discount rates 
    for each cash flow series. Next, find crossover where NPV is zero--corresponds
    to the lowest real IRR value. 
    
    Parameters
    ----------
    cfs : numpy.ndarray
        Rows are cash flow series, cols are time periods
    precision : float
        Level of accuracy for the inner IRR band, default value 0.005%
    rmin : float
        Lower bound of the inner IRR band default value 0%
    rmax1 : float
        Upper bound of the inner IRR band default value 30%
    rmax2 : float
        upper bound of the outer IRR band. e.g. 50% Values in the outer 
        band are calculated to 1% precision, IRRs outside the upper band 
        return the rmax2 value.
    
    Returns
    -------
    numpy.ndarray
        IRRs for cash flow series

    Notes
    -----
    For performance, negative IRRs are not calculated, returns "-1" and values are only calculated to an acceptable precision.
    """
    
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