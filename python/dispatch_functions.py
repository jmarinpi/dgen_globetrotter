# -*- coding: utf-8 -*-
"""
Currently battery storage functions are not utilizied. 
"""

import numpy as np
import tariff_functions as tFuncs
import sys

def cartesian(arrays, out=None):
    """
    Generate a cartesian product of input arrays.

    Parameters
    ----------
    arrays : list of numpy.ndarray
        1-D arrays to form the cartesian product of.
    out : numpy.ndarray
        Array to place the cartesian product in.

    Returns
    -------
    out : numpy.ndarray
        2-D array of shape (M, len(arrays)) containing cartesian products
        formed of input arrays.

    Examples
    --------
    >>> cartesian(([1, 2, 3], [4, 5], [6, 7]))
    array([[1, 4, 6],
           [1, 4, 7],
           [1, 5, 6],
           [1, 5, 7],
           [2, 4, 6],
           [2, 4, 7],
           [2, 5, 6],
           [2, 5, 7],
           [3, 4, 6],
           [3, 4, 7],
           [3, 5, 6],
           [3, 5, 7]])

    """

    arrays = [np.asarray(x) for x in arrays]
    dtype = arrays[0].dtype

    n = np.prod([x.size for x in arrays])
    if out is None:
        out = np.zeros([n, len(arrays)], dtype=dtype)

    m = n / arrays[0].size
    out[:,0] = np.repeat(arrays[0], m)
    # if arrays[1:]:
    #     cartesian(arrays[1:], out=out[0:m,1:])
        # for j in range(1, arrays[0].size):
        #     out[j*m:(j+1)*m,1:] = out[0:m,1:]
    return out    

class Battery:
    """
    Todo
    ----
    Degradation functions were just rough estimations from a slide deck, and currently have a disjoint at transition.
     
    """
    
    def __init__(self, nameplate_cap=0.0, nameplate_power=0.0, SOC_min=0.2, eta_charge=0.91, eta_discharge=0.91, cycles=0):
        self.SOC_min = SOC_min 
        self.eta_charge = eta_charge
        self.eta_discharge = eta_charge
        self.cycles = cycles

        self.nameplate_cap = nameplate_cap
        self.effective_cap = nameplate_cap*(1-SOC_min)

        self.nameplate_power = nameplate_power
        self.effective_power = nameplate_power

    def set_cap_and_power(self, nameplate_cap, nameplate_power):
        self.nameplate_cap = nameplate_cap
        self.effective_cap = nameplate_cap*(1-self.SOC_min)

        self.nameplate_power = nameplate_power
        self.effective_power = nameplate_power
        
        self.cycles = 0
        
        
    def set_cycle_deg(self, cycles):
        if cycles < 2300:
            deg_coeff = (-7.5e-12*cycles**3 + 4.84e-8*cycles**2 - 0.0001505*cycles + 0.9997)
        else:
            deg_coeff = -8.24e-5*cycles + 1.0094

        self.effective_cap = deg_coeff * self.nameplate_cap
        self.effective_power = deg_coeff * (1 - (1-deg_coeff)*1.25) * self.nameplate_power
    

#%%
def determine_optimal_dispatch(load_profile, pv_profile, batt, t, export_tariff, d_inc_n=50, DP_inc=50, estimator_params=None, estimated=False, restrict_charge_to_pv_gen=False, estimate_demand_levels=False):
    """
    Function that determines the optimal dispatch for a battery, and determines the resulting first year bill with the system.
    
    Parameters
    ----------               
    load_profile : numpy.ndarray
        Original load profile prior to modification by PV or storage
    pv_profile : numpy.ndarray
        PV profile of equal length to the `load_profile`
    t : :class:`python.tariff_functions.Tariff`
        Tariff class object
    batt : :class:`python.dispatch_functions.Battery`
        Battery class object
    export_tariff : :class:`python.tariff_functions.Export_Tariff`
        Export tariff class object
    
    Note
    ----
    In the battery level matrices, 0 index corresponds to an empty battery, and 
    the highest index corresponds to a full battery
    
    Todo
    ----
    1)  Having cost-to-go equal cost of filling the battery at the end may not be
        working.
    2)  Have warnings for classes of errors. Same for bill calculator, such as when
        net load in a given period is negative either have warnings, or outright nans, when an illegal move is chosen
    3)  If there are no demand charges, don't calc & don't have a limit on 
        demand_max_profile for the following dispatch.
    """
    load_and_pv_profile = load_profile - pv_profile
    
    if batt.effective_cap == 0.0:
        opt_load_traj = load_and_pv_profile
        demand_max_profile = load_and_pv_profile
        batt_level_profile = np.zeros(len(load_and_pv_profile), float)
        bill_under_dispatch, _ = tFuncs.bill_calculator(opt_load_traj, t, export_tariff)
        demand_max_exceeded = False
        batt_dispatch_profile = np.zeros([len(load_profile)])
    else:
        # =================================================================== #
        # Determine cheapest possible demand states for the entire year
        # =================================================================== #
        month_hours = np.array([0, 744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760]);
        cheapest_possible_demands = np.zeros((12,np.max([t.d_tou_n+1, 2])), float)
        demand_max_profile = np.zeros(len(load_and_pv_profile), float)
        batt_level_profile = np.zeros(len(load_and_pv_profile), float)
        
        # Determine the cheapest possible set of demands for each month, and create an annual profile of those demands
        batt_start_level = batt.effective_cap
        for month in range(12):
            # Extract the load profile for only the month under consideration
            load_and_pv_profile_month = load_and_pv_profile[month_hours[month]:month_hours[month+1]]
            pv_profile_month = pv_profile[month_hours[month]:month_hours[month+1]]
            d_tou_month_periods = t.d_tou_8760[month_hours[month]:month_hours[month+1]]
            
            # columns [:-1] of cheapest_possible_demands are the achievable demand levels, column [-1] is the cost
            # d_max_vector is an hourly vector of the demand level of that period (to become a max constraint in the DP), which is cast into an 8760 for the year.
            cheapest_possible_demands[month,:], d_max_vector, batt_level_month = calc_min_possible_demands_vector(d_inc_n, load_and_pv_profile_month, pv_profile_month, d_tou_month_periods, batt, t, month, restrict_charge_to_pv_gen, batt_start_level, estimate_demand_levels)
            demand_max_profile[month_hours[month]:month_hours[month+1]] = d_max_vector
            batt_level_profile[month_hours[month]:month_hours[month+1]] = batt_level_month
            batt_start_level = batt_level_month[-1]
        
        # =================================================================== #
        # Complete (not estimated) dispatch of battery with dynamic programming    
        # =================================================================== #
        if estimated == False:   
            DP_res = batt.effective_cap / (DP_inc-1)
            illegal = 99999999
                
            batt_actions_to_achieve_demand_max = np.zeros(len(load_profile), float)
            batt_actions_to_achieve_demand_max[1:-1] = batt_level_profile[1:-1] - batt_level_profile[0:-2]
            
            batt_actions_to_achieve_demand_max = np.zeros(len(load_profile), float)
            batt_actions_to_achieve_demand_max[1:] = batt_level_profile[1:] - batt_level_profile[0:-1]
            
            # Calculate the reverse cumsum, then mod the result by the resolution of the battery discretization
            batt_act_rev_cumsum = np.cumsum(batt_actions_to_achieve_demand_max[np.arange(8759,-1,-1)])[np.arange(8759,-1,-1)]
            batt_act_rev_cumsum += batt.effective_cap - batt_level_profile[-1]
            batt_act_rev_cumsum_mod = np.mod(batt_act_rev_cumsum, DP_res)
                        
                
            # batt_x_limits are the number of rows that the battery energy 
            # level can move in a single step. The actual range exceeds what is
            # possible (due to discretization), but will be restricted by a 
            # pass/fail test later on with cost-to-go.
            batt_charge_limit = int(batt.effective_power*batt.eta_charge/DP_res) + 1
            batt_discharge_limit = int(batt.effective_power/batt.eta_discharge/DP_res) + 1
            batt_charge_limits_len = batt_charge_limit + batt_discharge_limit + 1
            
            
            batt_levels = np.zeros([DP_inc+1,8760], float)
            batt_levels[1:,:] = np.linspace(0,batt.effective_cap,DP_inc, float).reshape(DP_inc,1)
            batt_levels[1:,:-1] = batt_levels[1:,:-1] + (DP_res - batt_act_rev_cumsum_mod[1:].reshape(1,8759)) # Shift each column's values, such that the DP can always find a way through
            batt_levels[0,:] = 0.0 # The battery always has the option of being empty
            batt_levels[-1,:] = batt.effective_cap # The battery always has the option of being full
            
            # batt_levels_buffered is the same as batt_levels, except it has
            # buffer rows of 'illegal' values 
            batt_levels_buffered = np.zeros([np.shape(batt_levels)[0]+batt_charge_limit+batt_discharge_limit, np.shape(batt_levels)[1]], float)
            batt_levels_buffered[:batt_discharge_limit,:] = illegal
            batt_levels_buffered[-batt_charge_limit:,:] = illegal
            batt_levels_buffered[batt_discharge_limit:-batt_charge_limit,:] = batt_levels
            
            
            # Build an adjustment that adds a very small amount to the
            # cost-to-go, as a function of rate of charge. Makes the DP prefer
            # to charge slowly, all else being equal
            adjuster = np.zeros(batt_charge_limits_len, float)
            base_adjustment = 0.0000001            
            adjuster[np.arange(batt_discharge_limit,-1,-1)] = base_adjustment * np.array(list(range(batt_discharge_limit+1)))*np.array(list(range(batt_discharge_limit+1))) / (batt_discharge_limit*batt_discharge_limit)
            adjuster[batt_discharge_limit:] = base_adjustment * np.array(list(range(batt_charge_limit+1)))*np.array(list(range(batt_charge_limit+1))) / (batt_charge_limit*batt_charge_limit)

            
            # Initialize some objects for later use in the DP
            expected_values = np.zeros((DP_inc+1, np.size(load_and_pv_profile)), float)
            DP_choices = np.zeros((DP_inc+1, np.size(load_and_pv_profile)), int)
            influence_on_load = np.zeros((DP_inc+1, batt_charge_limits_len), float)
            selected_net_loads = np.zeros((DP_inc+1, np.size(load_and_pv_profile)), float)
            net_loads = np.zeros((DP_inc+1, batt_charge_limits_len), float)
            costs_to_go = np.zeros((DP_inc+1, batt_charge_limits_len), float)
            change_in_batt_level_matrix = np.zeros((DP_inc+1, batt_charge_limits_len), float)

            # Expected value of final states is the energy required to fill the battery up
            # at the most expensive electricity rate. This encourages ending with a full
            # battery, but solves a problem of demand levels being determined by a late-hour
            # peak that the battery cannot recharge from before the month ends
            # This would be too strict under a CPP rate.
            # I should change this to evaluating the required charge based on the batt_level matrix, to keep self-consistent
            expected_values[:,-1] = np.linspace(batt.effective_cap,0,DP_inc+1)/batt.eta_charge*np.max(t.e_prices_no_tier) #this should be checked, after removal of buffer rows
            
            # option_indicies is a map of the indicies corresponding to the 
            # possible points within the expected_value matrix that that state 
            # can reach.
            # Each row is the set of options for a single battery state
            option_indicies = np.zeros((DP_inc+1, batt_charge_limits_len), int)
            option_indicies[:,:] = list(range(batt_charge_limits_len))
            for n in range(DP_inc+1):
                option_indicies[n,:] += n - batt_discharge_limit
            option_indicies[option_indicies<0] = 0 # Cannot discharge below "empty"
            option_indicies[option_indicies>DP_inc] = DP_inc # Cannot charge above "full"
            
            ###################################################################
            ############### Dynamic Programming Energy Trajectory #############
            
            for hour in np.arange(np.size(load_and_pv_profile)-2, -1, -1):
                if hour == 4873:
                    print("full stop")
                # Rows correspond to each possible battery state
                # Columns are options for where this particular battery state could go to
                # Index is hour+1 because the DP decisions are on a given hour, looking ahead to the next hour. 
            
                # this is just an inefficient but obvious way to assembled this matrix. It should be possible in a few quicker operations.
                for row in range(DP_inc+1):
                    change_in_batt_level_matrix[row,:] = (-batt_levels[row,hour] + batt_levels_buffered[row:row+batt_charge_limits_len,hour+1])
                    
                #Because of the 'illegal' values, neg_batt_bool shouldn't be necessary
                resulting_batt_level = change_in_batt_level_matrix + batt_levels[:,hour].reshape(DP_inc+1,1) # This are likely not necessary because options are restricted
#                neg_batt_bool = resulting_batt_level<0 # This are likely not necessary because options are restricted
                overfilled_batt_bool = resulting_batt_level>batt.effective_cap # This are likely not necessary because options are restricted
                                
                charging_bool = change_in_batt_level_matrix>0
                discharging_bool = change_in_batt_level_matrix<0
                
                influence_on_load = np.zeros(np.shape(change_in_batt_level_matrix), float)
                influence_on_load += (change_in_batt_level_matrix*batt.eta_discharge) * discharging_bool
                influence_on_load += (change_in_batt_level_matrix/batt.eta_charge) * charging_bool
                influence_on_load -= 0.000000001 # because of rounding error? Problems definitely occur (sometimes) without this adjustment. The adjustment magnitude has not been tuned since moving away from ints.
                
                net_loads = load_and_pv_profile[hour+1] + influence_on_load
                            
                # Determine the incremental cost-to-go for each option
                costs_to_go[:,:] = 0 # reset costs to go
                importing_bool = net_loads>=0 # If consuming, standard price
                costs_to_go += net_loads*t.e_prices_no_tier[t.e_tou_8760[hour+1]]*importing_bool
                exporting_bool = net_loads<0 # If exporting, NEM price
                costs_to_go += net_loads*export_tariff.prices[export_tariff.periods_8760[hour+1]]*exporting_bool     
                
                # Make the incremental cost of impossible/illegal movements very high
#                costs_to_go += neg_batt_bool * illegal # This are likely not necessary because options are restricted
                costs_to_go += overfilled_batt_bool * illegal # This are likely not necessary because options are restricted
                demand_limit_exceeded_bool = net_loads>demand_max_profile[hour+1]
                costs_to_go += demand_limit_exceeded_bool * illegal
                
                # add very small cost as a function of battery motion, to discourage unnecessary motion
                costs_to_go += adjuster
                    
                total_option_costs = costs_to_go + expected_values[option_indicies, hour+1]
                
                expected_values[:, hour] = np.min(total_option_costs,1)     
                     
                #Each row corresponds to a row of the battery in DP_states. So the 0th row are the options of the empty battery state.
                #The indicies of the results correspond to the battery's movement. So the (approximate) middle option is the do-nothing option   
                #Subtract the negative half of the charge vector, to get the movement relative to the row under consideration        
                DP_choices[:,hour] = np.argmin(total_option_costs,1) - batt_discharge_limit # adjust by discharge?
                selected_net_loads[:,hour] = net_loads[list(range(DP_inc+1)),np.argmin(total_option_costs,1)]
                
                
            #=================================================================#
            ################## Reconstruct trajectories #######################
            #=================================================================#
            # Determine what the indexes of the optimal trajectory were.
            # Start at the 0th hour, imposing a full battery.
            # traj_i is the indexes of the battery's trajectory.
            traj_i = np.zeros(len(load_and_pv_profile), int)
            traj_i[0] = DP_inc-1
            for n in range(len(load_and_pv_profile)-1):
                traj_i[n+1] = traj_i[n] + DP_choices[int(traj_i[n]), n]
            
            opt_load_traj = np.zeros(len(load_and_pv_profile), float)
            for n in range(len(load_and_pv_profile)-1):
                opt_load_traj[n+1] = selected_net_loads[traj_i[n], n]   
                
            # Determine what influence the battery had. Positive means the 
            # battery is discharging. 
            batt_dispatch_profile = load_and_pv_profile - opt_load_traj
            
            # This is now necessary in some cases, because coincident peak
            # charges are not calculated in the dispatch
            bill_under_dispatch, _ = tFuncs.bill_calculator(opt_load_traj, t, export_tariff)
            demand_max_exceeded = np.any(opt_load_traj[1:] > demand_max_profile[1:])
        
        
        #=====================================================================#
        ##################### Estimate Bill Savings ###########################
        #=====================================================================#
        elif estimated == True:
            if t.coincident_peak_exists == True:
                if t.coincident_style == 0:
                    coincident_demand_levels = np.average(load_profile[t.coincident_hour_def], 1)
                    coincident_charges = tFuncs.tiered_calc_vec(coincident_demand_levels, t.coincident_levels, t.coincident_prices)
                    coincident_monthly_charges = coincident_charges[t.coincident_monthly_periods]
                    coincident_charges = np.sum(coincident_monthly_charges)
            else:
                coincident_charges = 0
            
            batt_arbitrage_value = estimate_annual_arbitrage_profit(batt.effective_power, batt.effective_cap, batt.eta_charge, batt.eta_discharge, estimator_params['cost_sum'], estimator_params['revenue_sum'])                
            bill_under_dispatch = sum(cheapest_possible_demands[:,-1]) + coincident_charges + 12*t.fixed_charge + estimator_params['e_chrgs_with_PV'] - batt_arbitrage_value
            opt_load_traj = np.zeros([len(load_profile)])
            batt_dispatch_profile = np.zeros([len(load_profile)])
            demand_max_profile = np.zeros([len(load_profile)])
            batt_level_profile = np.zeros([len(load_profile)])
            #energy_charges = estimator_params['e_chrgs_with_PV'] - batt_arbitrage_value
            demand_max_exceeded = False

    #=========================================================================#
    ########################### Package Results ###############################
    #=========================================================================#
    
    results = {'load_profile_under_dispatch':opt_load_traj,
               'bill_under_dispatch':bill_under_dispatch,
               'demand_max_exceeded':demand_max_exceeded,
               'demand_max_profile':demand_max_profile,
               'batt_level_profile':batt_level_profile,
               'batt_dispatch_profile':batt_dispatch_profile}
               
    return results


    
#%% Energy Arbitrage Value Estimator
def calc_estimator_params(load_and_pv_profile, tariff, export_tariff, eta_charge, eta_discharge):
    """
    Create four 12-length vectors, weekend/weekday and 
    cost/revenue. They are a summation of each day's 12 hours of lowest/highest
    cost electricity.

    Parameters
    ----------
    load_and_pv_profile : numpy.ndarray
        8760 array of the agent's load_profile - pv_profile
    t : :class:`python.tariff_functions.Tariff`
        Tariff class object
    export_tariff : :class:`python.tariff_functions.Export_Tariff`
        Export tariff class object

    Note
    ----
    1) TOU windows are aligned with when the battery would be dispatching for demand peak shaving.
    2) The battery will be able to dispatch fully and recharge fully every 24 hour cycle.
    
    Todo
    ----
    1)  Bring back consideration of tiers.
    2)  Consider coming up with a better method that captures exportation, CPP, etc
        Maybe? Or just confirm a simple estimation works with our dGen set, 
        and use the accurate dispatch for any other analysis.
    """

    # Calculate baseline energy costs with the given load+pv profile
    _, tariff_results = tFuncs.bill_calculator(load_and_pv_profile, tariff, export_tariff)

    e_chrgs_with_PV = tariff_results['e_charges']
    
    # Estimate the marginal retail energy costs of each hour
    e_value_8760 = np.average(tariff.e_prices, 0)[tariff.e_tou_8760]

    e_value_8760[load_and_pv_profile<=0] = export_tariff.prices[export_tariff.periods_8760][load_and_pv_profile<=0]

    # Reshape into 365 24-hour day vectors and then sort by increasing cost
    e_value_365_24 = e_value_8760.reshape((365,24), order='C')
    e_value_365_24_sorted = np.sort(e_value_365_24)
    
    # Split the lower half into costs-to-charge and upper half into revenue-from-discharge
    e_cost = e_value_365_24_sorted[:,:12]
    e_revenue = e_value_365_24_sorted[:,np.arange(23,11,-1)]
    
    # Estimate which hours there is actually an arbitrage profit, where revenue
    #  exceeds costs for a pair of hours in a day. Not strictly correct, because
    #  efficiencies means that hours are not directly compared.
    arbitrage_opportunity = e_revenue*eta_discharge > e_cost*eta_charge
    
    # Where there is no opportunity, replace both cost and revenue values with
    #  0, to reflect no battery action in those hours.
    e_cost[arbitrage_opportunity==False] = 0.0 
    e_revenue[arbitrage_opportunity==False] = 0.0 
    
    cost_sum = np.sum(e_cost, 0)
    revenue_sum = np.sum(e_revenue, 0)

    results = {'e_chrgs_with_PV':e_chrgs_with_PV,
                'cost_sum':cost_sum,
                'revenue_sum':revenue_sum}

    return results
    
#%%
def estimate_annual_arbitrage_profit(power, capacity, eta_charge, eta_discharge, cost_sum, revenue_sum):
    """
    This function uses the 12x24 marginal energy costs from calc_estimator_params to estimate the potential arbitrage value of a battery.
    
    Parameters
    ----------
    power : float
        Inherited from :class:`python.dispatch_functions.Battery`
    capacity : float
        Inherited from :class:`python.dispatch_functions.Battery`
    eta_charge : float
        Inherited from :class:`python.dispatch_functions.Battery`
    eta_discharge: float
        Inherited from :class:`python.dispatch_functions.Battery`
    cost_sum : numpy.ndarray
        12-length sorted vector of summed energy costs for charging in the cheapest 12 hours of each day
    revenue_sum : numpy.ndarray
        12-length sorted vector of summed energy revenue for discharging in the most expensive 12 hours of each day
    
    Todo
    ----
    Restrict action if cap > (12 * power)
    """
    
    charge_blocks = np.zeros(12)
    charge_blocks[:int(np.floor(capacity/eta_charge/power))] = power
    charge_blocks[int(np.floor(capacity/eta_charge/power))] = np.mod(capacity/eta_charge,power)  
    
    # Determine how many hour 'blocks' the battery will need to cover to discharge,
    #  and what the kWh discharged during those blocks will be
    discharge_blocks = np.zeros(12)
    discharge_blocks[:int(np.floor(capacity*eta_discharge/power)+1)] = power
    discharge_blocks[int(np.floor(capacity*eta_discharge/power)+1)] = np.mod(capacity*eta_discharge,power)
        
    revenue = np.sum(revenue_sum * eta_discharge * discharge_blocks)
    cost = np.sum(cost_sum * eta_charge * charge_blocks)
    
    annual_arbitrage_profit = revenue - cost

    return annual_arbitrage_profit
    
    
#%%
def calc_min_possible_demands_vector(res, load_and_pv_profile, pv_profile, d_periods_month, batt, t, month, restrict_charge_to_pv_gen, batt_start_level, estimate_demand_levels):
    """
    Currently battery storage functions are not utilizied. 

    Function that determines the minimum possible demands that this battery can achieve for a particular month.
    
    Parameters
    ----------
    t : :class:`python.tariff_functions.Tariff`
        Tariff class object
    batt : :class:`python.dispatch_functions.Battery`
        Battery class object
    
    Todo
    ----
    Add a vector of forced discharges, for demand response representation
    """
    # Recast d_periods_month vector into d_periods_index, which is in terms of increasing integers starting at zero
    try:
        unique_periods = np.unique(d_periods_month)
        Dn_month = len(unique_periods)
        d_periods_index = np.copy(d_periods_month)
        for n in range(len(unique_periods)): d_periods_index[d_periods_month==unique_periods[n]] = n
         
         
        # Calculate the original and minimum possible demands in each period
        original_demands = np.zeros(Dn_month)
        min_possible_demands = np.zeros(Dn_month)
        for period in range(Dn_month):
            original_demands[period] = np.max(load_and_pv_profile[d_periods_index==period])
            min_possible_demands = original_demands - batt.effective_power
                
        # d_ranges is the range of demands in each period that will be investigated
        d_ranges = np.zeros((res,Dn_month), float)
        for n in range(Dn_month):
            d_ranges[:,n] = np.linspace(min_possible_demands[n], original_demands[n], res)
            
        # Assemble list of demands that cuts diagonally across search space
        d_combo_n = res
        d_combinations = np.zeros((d_combo_n,Dn_month+1), float)
        d_combinations[:,:Dn_month] = d_ranges
        TOU_demand_charge = np.sum(tFuncs.tiered_calc_vec(d_combinations[:,:Dn_month], t.d_tou_levels[:,unique_periods], t.d_tou_prices[:,unique_periods]),1) #check that periods line up with rate
#        monthly_demand_charge = tFuncs.tiered_calc_vec(np.max(d_combinations[:,:Dn_month],1), t.d_flat_levels[:,month], t.d_flat_prices[:,month])
#        d_combinations[:,-1] = TOU_demand_charge + monthly_demand_charge
        d_combinations[:,-1] = TOU_demand_charge
        
        # Evaluate the diagonal set of demands, determining which one is the
        # cheapest. This will restrict the larger search space in the next step.
        cheapest_d_states, batt_level_profile, i_of_first_success = determine_cheapest_possible_of_given_demand_levels(load_and_pv_profile, pv_profile, unique_periods, d_combinations, d_combo_n, Dn_month, d_periods_index,  batt, restrict_charge_to_pv_gen, batt_start_level, t)
            
        if estimate_demand_levels == False:
            # Assemble a list of all combinations of demand levels within the ranges of 
            # interest. For a 2D situation, this search space will consist of 
            # quadrants 1 and 3 around the i_of_first_success, as quadrant 2
            # contains no possible solutions and quadrant 4 is dominated. For ND
            # situations, each tuple of the cartesian should contain i:Dmin for one
            # dimension and i:Dmax for the other dimensions
            set_of_all_demand_combinations = np.zeros([0,Dn_month])
            for dimension in range(Dn_month):
                list_of_ranges = list()
                for d_n in range(Dn_month):
                    if d_n==dimension: list_of_ranges.append(d_ranges[:i_of_first_success+1, d_n])
                    else: list_of_ranges.append(d_ranges[i_of_first_success:, d_n])
                set_of_demands_for_this_dimension = cartesian(list_of_ranges)
                set_of_all_demand_combinations = np.concatenate((set_of_all_demand_combinations, set_of_demands_for_this_dimension))
            
            d_combo_n = len(set_of_all_demand_combinations)
            
                    
            
            d_combinations = np.zeros((d_combo_n,Dn_month+1), float)
            d_combinations[:,:Dn_month] = set_of_all_demand_combinations
            
            # Calculate the demand charges of the search space and sort by 
            # increasing cost.
            TOU_demand_charge = np.sum(tFuncs.tiered_calc_vec(d_combinations[:,:Dn_month], t.d_tou_levels[:,unique_periods], t.d_tou_prices[:,unique_periods]),1) #check that periods line up with rate
            monthly_demand_charge = tFuncs.tiered_calc_vec(np.max(d_combinations[:,:Dn_month],1), t.d_flat_levels[:,month], t.d_flat_prices[:,month])
            d_combinations[:,-1] = TOU_demand_charge + monthly_demand_charge   
            
            cheapest_d_states, batt_level_profile, _ = determine_cheapest_possible_of_given_demand_levels(load_and_pv_profile, pv_profile, unique_periods, d_combinations, d_combo_n, Dn_month, d_periods_index,  batt, restrict_charge_to_pv_gen, batt_start_level, t)
            
        d_max_vector = cheapest_d_states[d_periods_month]
            
        if restrict_charge_to_pv_gen == True:
            d_max_vector = np.minimum(load_and_pv_profile+pv_profile, d_max_vector)
        
        return cheapest_d_states,  d_max_vector, batt_level_profile

    except Exception as e:
            print(e)
            print(('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e), e))
    
#%%
def determine_cheapest_possible_of_given_demand_levels(load_and_pv_profile, pv_profile, unique_periods, d_combinations, d_combo_n, Dn_month, d_periods_index,  batt, restrict_charge_to_pv_gen, batt_start_level, tariff):
    demand_vectors = d_combinations[:,:Dn_month][:, d_periods_index]
    poss_charge = np.minimum(batt.effective_power*batt.eta_charge, (demand_vectors-load_and_pv_profile)*batt.eta_charge)
    if restrict_charge_to_pv_gen == True:
        poss_charge = np.minimum(poss_charge, pv_profile*batt.eta_charge)
    
    necessary_discharge = (demand_vectors-load_and_pv_profile)/batt.eta_discharge
    poss_batt_level_change = demand_vectors - load_and_pv_profile
    poss_batt_level_change = np.where(necessary_discharge<=0, necessary_discharge, poss_charge)

    # Walk through the battery levels. A negative value in a row means that 
    # particular constraint is not able to be met under the given conditions.
    batt_e_levels = np.zeros([d_combo_n, len(d_periods_index)])
    batt_e_levels[:,0] = batt_start_level
    for n in np.arange(1, len(d_periods_index)):
        batt_e_levels[:,n] = batt_e_levels[:,n-1] + poss_batt_level_change[:,n]
        batt_e_levels[:,n] = np.clip(batt_e_levels[:,n], -99, batt.effective_cap)

    able_to_meet_targets = np.all(batt_e_levels>=0, 1)
    i_of_first_success = np.argmax(able_to_meet_targets)

    d_charge_total_for_i_of_first_success = d_combinations[i_of_first_success, -1]
    match_lowest_cost = np.where(d_combinations[:,-1]==d_charge_total_for_i_of_first_success, True, False)
    demand_period_sums = np.where(match_lowest_cost==True, np.sum(d_combinations[:,:-1],1), 0)
    i_of_least_constrained_cheapest_option = np.argmax(demand_period_sums)
    
    batt_level_profile = batt_e_levels[i_of_least_constrained_cheapest_option, :]
    cheapest_d_states = np.zeros(np.max([tariff.d_tou_n+1, 2])) # minimum of two, because some tariffs have d_tou_n=0, but still have d_flat
    cheapest_d_states[unique_periods] = d_combinations[i_of_least_constrained_cheapest_option,:-1]
    cheapest_d_states[-1] = d_combinations[i_of_least_constrained_cheapest_option,-1]
    
    return cheapest_d_states, batt_level_profile, i_of_least_constrained_cheapest_option