
def calc_economics_with_sam(df, schema, sector, sector_abbr, market_projections, 
                   financial_parameters, cfg, scenario_opts, max_market_share, cur, con, 
                   year, dsire_incentives, deprec_schedule, logger, rate_escalations, ann_system_degradation, mode):
    '''
    Calculates economics of system adoption (cashflows, payback, irr, etc.)
    
        IN:
            Lots    
        
        OUT:
            df - pd dataframe - main dataframe with econ outputs appended as columns
    '''
    # Evaluate economics of leasing or buying for all customers who are able to lease
    business_model = pd.DataFrame({'business_model' : ('host_owned','tpo'), 
                                   'metric' : ('payback_period','percent_monthly_bill_savings'),
                                   'cross_join' : (1, 1)})
    df['cross_join'] = 1
    df = pd.merge(df, business_model, on = 'cross_join')
    df = df.drop('cross_join', axis=1)
    
    df['sector'] = sector.lower()
    df = pd.merge(df,financial_parameters, how = 'left', on = ['sector','business_model'])
    
    # get customer expected rate escalations
    # Use the electricity rate multipliers from ReEDS if in ReEDS modes and non-zero multipliers have been passed
    if mode == 'ReEDS' and max(df['ReEDS_elec_price_mult'])>0:
        
        rate_growth_mult = np.ones((len(df),30))
        rate_growth_mult *= df['ReEDS_elec_price_mult'][:,np.newaxis]
         
    else:
        # if not in ReEDS mode, use the calc_expected_rate_escal function
        rate_growth_mult = datfunc.calc_expected_rate_escal(df, rate_escalations, year, sector_abbr)    

    # Calculate value of incentives. Manual and DSIRE incentives can't stack. DSIRE ptc/pbi/fit are assumed to disburse over 10 years.    
    if scenario_opts['overwrite_exist_inc']:
        value_of_incentives = datfunc.calc_manual_incentives(df,con, year, schema)
    else:
        inc = pd.merge(df,dsire_incentives,how = 'left', on = 'incentive_array_id')
        value_of_incentives = datfunc.calc_dsire_incentives(inc, year, default_exp_yr = 2016, assumed_duration = 10)
    df = pd.merge(df, value_of_incentives, how = 'left', on = ['county_id','bin_id'])
    
    revenue, costs, cfs, first_year_bill_with_system, first_year_bill_without_system, monthly_bill_savings, percent_monthly_bill_savings = calc_cashflows(df, rate_growth_mult, deprec_schedule, scenario_opts, cfg.technology, ann_system_degradation, yrs = 30)
    
    df['first_year_bill_with_system'] = first_year_bill_with_system
    df['first_year_bill_without_system'] = first_year_bill_without_system
    df['monthly_bill_savings'] = monthly_bill_savings
    df['percent_monthly_bill_savings'] = percent_monthly_bill_savings

    ## Calc metric value here
    df['metric_value_precise'] = calc_metric_value(df,cfs,revenue,costs)
        
    df['lcoe'] = calc_lcoe(costs,df.aep.values, df.discount_rate)    

    
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
    
    df = datfunc.assign_business_model(df, method = 'prob', alpha = 2)
    
    return df
    
#==============================================================================    
    
    
    
#==============================================================================
def calc_cashflows_with_sam(df, rate_growth_mult, deprec_schedule, scenario_opts, tech, ann_system_degradation, yrs = 30):
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
    
    ## COSTS    
    
    # 1)  Cost of servicing loan/leasing payments
    crf = (df.loan_rate*(1 + df.loan_rate)**df.loan_term_yrs) / ( (1+df.loan_rate)**df.loan_term_yrs - 1);
    pmt = - (1 - df.down_payment) * df.ic * crf    
    
    loan_cost = datfunc.fill_jagged_array(pmt,df.loan_term_yrs)
    loan_cost[:,0] -= df.ic * df.down_payment # Pay the down payment and the first year loan payment in first year
    
    # wind turbines do not have inverters hence no replacement cost.
    # but for solar, calculate and replace the inverter replacement costs with actual values
    inverter_cost = np.zeros(shape)

    if tech == 'solar':
        # Annualized (undiscounted) inverter replacement cost $/year (includes system size). Applied from year 10 onwards since assume initial 10-year warranty
        inverter_replacement_cost  = df['system_size_kw'] * df.inverter_cost_dollars_per_kw/df.inverter_lifetime_yrs
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
    
    # Annual system production (kWh) including degradation and curtailments
    aep = np.empty(shape)
    aep[:,0] = 1
    aep[:,1:]  = 1 - ann_system_degradation
    aep = df.aep[:,np.newaxis] * aep.cumprod(axis = 1) * (1 - df.curtailment_rate[:,np.newaxis])
    
    # ATTENTION: Make sure generation profile has been curtailed prior to calculating first year bill
    # i.e. hourly_gen_profile *= (1 - df.curtailment_rate)
    
    # ATTENTION:  Make sure that excess generation is being appropriately valued.
    # Test 1: Compare first_year_bill_with_system with net metering on/off. The bill should be lower with NEM on (more savings)
    # Test 2: Compare first_year_bill_with_system with net metering on and changing year-end credit value [gen probably need to exceed consumption]. The bill should decrease (more savings) as credit increases
    # Test 3: Compare first_year_bill_with_system with net metering off and changing year-end credit value [gen probably need to exceed consumption]. The bill should not change as credit increases
    # Test 4: Define a simple flat rate (10c/kWh). Output the 8760 load and consumption profile. Now manually calculate the inflows, outflows, and year-end credit to validate rollover credit.
    
    ## Calling SAM here
    first_year_bill_with_system, first_year_bill_without_system = make_sam_magic_go_now(...)    
    ##
    # Attention: Code below assumes first_year_bill outputs are lists with length of df.
    # Attention: Code below assumes first_year_bill outputs are in units of dollars.
    # synthetic data: first_year_bill_without_system = 100 * rand(len(df)), first_year_bill_with_system = 0.5*first_year_bill_with_system
    
    df['first_year_bill_with_system'] = first_year_bill_with_system   
    df['first_year_bill_without_system'] = first_year_bill_without_system
    
    # Assume that the rate_growth_mult is a cumulative factor i.e. [1,1.02,1.044] instead of [0.02, 0.02, 0.02]
        
    # Take the difference of bills in first year, this is the revenue in the first year. Then assume that bill savings will follow
    # the same trajectories as changes in rate escalation. Output of this should be a data frame of shape (len(df),30)
    generation_revenue = (df['first_year_bill_without_system'] - df['first_year_bill_with_system'])[:,np.newaxis] * rate_growth_mult
    
    # Decrement the revenue to account for system degradation.
    system_degradation_factor = np.empty(shape)
    system_degradation_factor[:,0] = 1
    system_degradation_factor[:,1:]  = 1 - ann_system_degradation
    system_degradation_factor = system_degradation_factor.cumprod(axis = 1)
    
    generation_revenue *= system_degradation_factor
    
    # Since electricity expenses are tax deductible for commercial & industrial 
    # entities, the net annual savings from a system is reduced by the marginal tax rate
    generation_revenue *= (1 - df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial'))[:,np.newaxis])
        
    # 4) Revenue from depreciation.  
    # Depreciable basis is installed cost less tax incentives
    # Revenue comes from taxable deduction [basis * tax rate * schedule] and cannot be monetized by Residential
    
    depreciation_revenue = np.zeros(shape)
    deprec_basis = np.maximum(df.ic - 0.5 * (df.value_of_tax_credit_or_deduction  + df.value_of_rebate),0)[:,np.newaxis] # depreciable basis reduced by half the incentive
    depreciation_revenue[:,:20] = deprec_basis * deprec_schedule.reshape(1,20) * df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial') | (df.business_model == 'tpo'))[:,np.newaxis]   

    # 5) Interest paid on loans is tax-deductible for commercial & industrial users; 
    # assume can fully monetize. Assume that third-party owners finance system all-cash--thus no interest to deduct. 
    
    # Calc interest paid on serving the loan
    interest_paid = calc_interest_pmt_schedule(df,30)
    interest_on_loan_pmts_revenue = interest_paid * df.tax_rate[:,np.newaxis] * ((df.sector == 'Industrial') | (df.sector == 'Commercial') & (df.business_model == 'host_owned'))[:,np.newaxis]
    
    # 6) Revenue from other incentives    
    incentive_revenue = np.zeros(shape)
    incentive_revenue[:, 1] = df.value_of_increment + df.value_of_rebate + df.value_of_tax_credit_or_deduction
    
    
    ptc_revenue = datfunc.fill_jagged_array(df.value_of_ptc,df.ptc_length)
    pbi_fit_revenue = datfunc.fill_jagged_array(df.value_of_pbi_fit,df.pbi_fit_length)    

    incentive_revenue += ptc_revenue + pbi_fit_revenue
    
    revenue = generation_revenue + depreciation_revenue + interest_on_loan_pmts_revenue + incentive_revenue
    costs = loan_cost + om_cost + inverter_cost
    cfs = revenue + costs
    
    # Calculate the monthly bill savings in the first year of ownership in dollars ($)
    # and in percentage of prior bill
    first_year_energy_savings = df.first_year_bill_without_system - df.first_year_bill_without_system 
    avg_annual_payment = loan_cost.sum(axis = 1)/df.loan_term_yrs
    first_year_bill_savings = first_year_energy_savings - avg_annual_payment
    monthly_bill_savings = first_year_bill_savings/12
    percent_monthly_bill_savings = first_year_bill_savings/df.first_year_bill_without_system
        
    return revenue, costs, cfs, df.first_year_bill_without_system, df.first_year_bill_without_system, monthly_bill_savings, percent_monthly_bill_savings

def calc_metric_value_with_sam(df,cfs,revenue,costs):
    '''
    Calculates the economic value of adoption given the metric chosen. Residential buyers
    use simple payback, non-residential buyers use time-to-double, leasers use percent monthly bill savings
    
        IN:
            df    
        
        OUT:
            metric_value - pd series - series of values given the business_model and sector
    '''
    
    payback = calc_payback(cfs)
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
