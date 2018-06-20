import elec
import agent_preparation
import numpy as np
import pandas as pd
import os
import utility_functions as utilfunc


def init_agents(model_settings, scenario_settings, prng, cur, con):
    # Prepare core agent attributes
    role = model_settings.role
    techs = scenario_settings.techs
    schema = scenario_settings.schema
    sample_pct = model_settings.sample_pct
    min_agents = model_settings.min_agents
    agents_per_region = model_settings.agents_per_region
    sectors = scenario_settings.sectors
    pg_procs = model_settings.pg_procs
    pg_conn_string = model_settings.pg_conn_string
    random_generator_seed = scenario_settings.random_generator_seed
    start_year = 2014 #TODO: integrate with scenario_settings?
    end_year = scenario_settings.end_year
    agent_prep_args = (techs, schema, role, sample_pct, min_agents,
                       agents_per_region, sectors, pg_procs,
                       pg_conn_string, random_generator_seed,
                       end_year)
    
    #==============================================================================
    # Prepare temporal data for wind
    #==============================================================================    
    agent_preparation.elec.combine_temporal_data_wind(cur, con, schema, start_year, end_year)
    
    #==============================================================================
    # Generate and retrieve core agent attributes
    #==============================================================================
    agent_preparation.elec.generate_core_agent_attributes(cur, con, *agent_prep_args)
    # Create core agent attributes
    agents_df = elec.get_core_agent_attributes(con, schema, scenario_settings.region)
 
    #==============================================================================
    # Drop some unused columns - TODO: remove these from agent gen
    #==============================================================================
    agents_df.drop(['util_type'], axis=1, inplace=True)             
    #agents_df.drop(['sector'], axis=1, inplace=True)             
                                              
    #==============================================================================
    # Rename 'pca_reg' to 'ba', to align with the same change in ReEDS's - TODO: fix this in initial agent gen
    #==============================================================================
    agents_df['ba'] = agents_df['pca_reg']
    agents_df.drop(['pca_reg'], axis=1, inplace=True)          
    
    # There was a problem where an agent was being generated that had no customers in the bin, but load in the bin
    # This is a temporary patch to get the model to run in this scenario
    agents_df['customers_in_bin'] = np.where(agents_df['customers_in_bin']==0, 1, agents_df['customers_in_bin'])
    agents_df['load_kwh_per_customer_in_bin'] = np.where(agents_df['load_kwh_per_customer_in_bin']==0, 1, agents_df['load_kwh_per_customer_in_bin'])

    #==============================================================================
    # Merge RTO data onto agent_df - TODO: Store this mapping in database
    #==============================================================================
    agents_df.reset_index(inplace=True)
    rto_map = pd.read_csv(os.path.join(os.getcwd(), '..', 'reeds_data_for_tariff_construction', 'BA_to_RTO_mapping.csv'))
    agents_df = pd.merge(agents_df, rto_map, on='ba')
    agents_df.set_index('agent_id', inplace=True)

    #==============================================================================
    # ADJUST ROOF AREAS TO ALIGN WITH LIDAR DATA ON SECTOR/STATE LEVEL
    # TODO: Improve this in actual agent generation
    #==============================================================================
    agents_df = elec.adjust_roof_area(agents_df)
    
    #==============================================================================
    # CHECK TECH POTENTIAL LIMITS
    #==============================================================================
    # This should happen somewhere after agent generation. It would probably be
    # best to check # of buildings and roof area, instead of pv tech potential.           
                             
    # =========================================================================
    # GET NORMALIZED LOAD PROFILES
    # =========================================================================
    norm_load_profiles_df = elec.get_normalized_load_profiles(con, schema, sectors)
    agents_df = elec.apply_normalized_load_profiles(agents_df, norm_load_profiles_df)
    del norm_load_profiles_df

    # =========================================================================
    # RESOURCE DATA
    # =========================================================================
    # get hourly resource
    hourly_solar_resource_df = elec.get_normalized_hourly_resource_solar(con, schema, sectors, techs)
    agents_df = elec.apply_solar_capacity_factor_profile(agents_df, hourly_solar_resource_df)
    del hourly_solar_resource_df


    # =========================================================
    # GET RATE RANKS & TARIFF LOOKUP TABLE FOR EACH SECTOR
    # =========================================================
    # get (ranked) rates for each sector
    rates_rank_df =  elec.get_electric_rates(cur, con, scenario_settings.schema, scenario_settings.sectors, scenario_settings.random_generator_seed, model_settings.pg_conn_string)

    # Remove certain manually selected tariffs
    rates_rank_df = rates_rank_df[rates_rank_df['rate_id_alias'] != 16592] # colorado's residential demand tariff

    # check that every agent has a tariff, assign one to them if they don't
    rates_rank_df = elec.check_rate_coverage(agents_df, rates_rank_df)

    # find the list of unique rate ids that are included in rates_rank_df
    selected_rate_ids =  elec.identify_selected_rate_ids(rates_rank_df)
    # get lkup table with rate jsons
    rates_json_df =  elec.get_electric_rates_json(con, selected_rate_ids)
    rates_json_df = rates_json_df.set_index('rate_id_alias')

    # =========================================================================
    # AGENT TARIFF SELECTION
    # =========================================================================
    # get lookup table to assign default tariffs to residential agents, where applicable
    default_res_rate_lkup = elec.get_default_res_rates(con)
    
    agents_df = elec.select_tariff_driver(agents_df, prng, rates_rank_df, rates_json_df, default_res_rate_lkup, n_workers=model_settings.local_cores)
    del rates_json_df, selected_rate_ids, rates_rank_df, rto_map, default_res_rate_lkup 
    
    
    #==============================================================================
    # Set initial year columns. Initial columns do not change, whereas non-initial are adjusted each year
    # note that some of the above operations rely on non-initial name, which should be cleaned up when agent initialization is rebuilt
    #==============================================================================    
    agents_df.rename(columns={'customers_in_bin':'customers_in_bin_initial', 
                               'load_kwh_per_customer_in_bin':'load_kwh_per_customer_in_bin_initial',
                               'load_kwh_in_bin':'load_kwh_in_bin_initial'}, inplace=True)

    return agents_df
