# -*- coding: utf-8 -*-
"""
Created on Fri Sep 26 15:31:58 2014

@author: bsigrin
"""

def solar_scoe(sector, load_kwh_per_customer_in_bin, naep, available_rooftop_space_sqm, density_w_per_sqft, nem_availability, excess_generation_factor):
    """ Calculate optimal system size, number of panels, and other (future) configurations
        
       IN:
            naep (normalized annual energy production)
            density_w_per_sqft (this will be inactive for now)
            available rooftop space (this will be inactive for now) ## square 
            load_kwh_per_customer_in_bin
            excess_generation_factor,
            nem availability ()
       
       OUT:
           system_size_kw
           npanels
    """
    default_panel_size_sqft = 17.5 # Assume panel size of 17.5 sqft
    available_rooftop_space_sqft = available_rooftop_space_sqm * 10.7639
    
    # Assume res sizes to load, others to 1/4 of load
    if sector == 'residential':
        sector_size_mult = 1
    else:
        sector_size_mult = 0.25
    
    if nem_availability == 'Full_Net_Metering_Everywhere':
        percent_of_gen_monetized = 1
    elif nem_availability == 'Partial_Avoided_Cost':
        percent_of_gen_monetized = 1 - 0.5 * excess_generation_factor # Assume avoided cost is roughly 50% of retail, this effectively halves excess_gen_factor
    elif nem_availability == 'Partial_No_Outflows':
        percent_of_gen_monetized = 1 - excess_generation_factor
    elif nem_availability == 'No_Net_Metering_Anywhere':
        percent_of_gen_monetized = 1 - excess_generation_factor
        
    max_system_size_allowed_kw =  0.001 * available_rooftop_space_sqft * density_w_per_sqft
    ideal_system_size_kw = (load_kwh_per_customer_in_bin/naep) * percent_of_gen_monetized * sector_size_mult
    
    system_size_kw = min(max_system_size_allowed_kw, ideal_system_size_kw)
    npanels = system_size_kw/(0.001 * density_w_per_sqft * default_panel_size_sqft) # Denom is kW of a panel
    
    return system_size_kw, npanels


