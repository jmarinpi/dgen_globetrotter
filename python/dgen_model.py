"""
Distributed Wind Diffusion Model
National Renewable Energy Lab

@author: bsigrin
"""

# before doing anything, check model dependencies
import tests
tests.check_dependencies()

# import depenencies
import time
import os
import pandas as pd
import psycopg2.extras as pgx
import numpy as np
# ---------------------------------------------
# order of the next 3 needs to be maintained
# otherwise the logger may not work correctly
# (I think the order needs to follow the order 
# in which each module is used in __main__)
import data_functions as datfunc
import storage_functions_mike as storage_funcs_m
reload(storage_funcs_m)
#import storage_functions_pieter as storage_funcs_p
#reload(storage_funcs_p)
# ---------------------------------------------
import config
from excel import excel_functions
import reeds_functions as reedsfunc
import utility_functions as utilfunc
from agent import Agents, AgentsAlgorithm
import tech_choice_elec
import tech_choice_geo
import settings
import agent_mutation_elec
import agent_mutation_geo
import agent_mutation_ghp
import agent_mutation_du
import agent_preparation_elec
import agent_preparation_geo
import demand_supply_geo
import diffusion_functions_elec
import diffusion_functions_ghp
import diffusion_functions_du
import diffusion_functions_geo
import financial_functions_elec
import financial_functions_geo


#==============================================================================
# raise  numpy and pandas warnings as exceptions
#==============================================================================
#np.seterr(all='raise')
pd.set_option('mode.chained_assignment', None)
#==============================================================================

# TODO: delete this line
sunk_costs = False


def main(mode = None, resume_year = None, endyear = None, ReEDS_inputs = None):

    try:
        #==============================================================================
        # SET UP THE MODEL TO RUN
        #==============================================================================
        # initialize Model Settings object (this controls settings that apply to all scenarios to be executed)
        model_settings = settings.ModelSettings()
        
        # add the config to model settings
        model_settings.add_config(config)
        # set Rscript path
        model_settings.set_Rscript_path(config.Rscript_paths)
        # set the model starting time (in seconds since epoch)
        model_settings.set('model_init', utilfunc.get_epoch_time())
        # get current date/time (formatted)
        model_settings.set('cdate', utilfunc.get_formatted_time())
        # set output directory 
        model_settings.set('out_dir', datfunc.make_output_directory_path(model_settings.cdate))

        # make output directory     
        os.makedirs(model_settings.out_dir)
        # create the logger
        logger = utilfunc.get_logger(os.path.join(model_settings.out_dir, 'dg_model.log'))

        # get the git hash and also log to output file
        model_settings.set('git_hash', utilfunc.get_git_hash())
        logger.info("Model version is git commit %s" % model_settings.git_hash)
        # find the input excel spreadsheets
        model_settings.set('input_scenarios', datfunc.get_input_scenarios())

        # connect to Postgres and configure connection
        con, cur = utilfunc.make_con(model_settings.pg_conn_string)
        pgx.register_hstore(con) # register access to hstore in postgres    
        logger.info("Connected to Postgres with the following params:\n%s" % model_settings.pg_params_log)      
        
        # validate all model settings
        model_settings.validate()
     
        #==============================================================================
        # LOOP OVER SCENARIOS
        #==============================================================================
        # variables used to track outputs
        scenario_names = []
        dup_n = 1
        out_subfolders = {'wind' : [], 'solar' : [], 'ghp': [], 'du': []}
        for i, scenario_file in enumerate(model_settings.input_scenarios):
            logger.info('============================================') 
            logger.info('============================================') 
            logger.info("Running Scenario %s of %s" % (i+1, len(model_settings.input_scenarios)))
            
            # initialize ScenarioSettings object (this controls settings tha apply only to this specific scenario)
            scenario_settings = settings.ScenarioSettings()
            scenario_settings.set('input_scenario', scenario_file)
            
            logger.info("-------------Preparing Database-------------")
            # create the output schema
            try:
                if model_settings.use_existing_schema == True:
                    # create a schema from the existing schema of interest
                    new_schema = datfunc.create_output_schema(model_settings.pg_conn_string, model_settings.cdate, source_schema = model_settings.existing_schema_name, include_data = True)
                else:
                    # create an empty schema from diffusion_template
                    new_schema = datfunc.create_output_schema(model_settings.pg_conn_string, model_settings.cdate, source_schema = 'diffusion_template', include_data = False)                    
            except Exception, e:
                raise Exception('\tCreation of output schema failed with the following error: %s' % e)
            # set the schema
            scenario_settings.set('schema', new_schema)
            
            # load Input Scenario to the new schema           
            try:
                excel_functions.load_scenario(scenario_settings.input_scenario, scenario_settings.schema, con, cur)
            except Exception, e:
                raise Exception('\tLoading failed with the following error: %s' % e)

            # read in high level scenario settings
            scenario_settings.set('techs', datfunc.get_technologies(con, scenario_settings.schema))
           # set tech_mode
            scenario_settings.set_tech_mode()        
            scenario_settings.set('sectors', datfunc.get_sectors(cur, scenario_settings.schema))
            scenario_settings.add_scenario_options(datfunc.get_scenario_options(cur, scenario_settings.schema))
            scenario_settings.set('model_years', datfunc.create_model_years(model_settings.start_year, scenario_settings.end_year))
            # validate scenario settings
            scenario_settings.validate()
            
            # summarize high level secenario settings 
            datfunc.summarize_scenario(scenario_settings, model_settings)

            # create output folder for this scenario
            out_scen_path, scenario_names, dup_n = datfunc.create_scenario_results_folder(scenario_settings.input_scenario, scenario_settings.scen_name, scenario_names, model_settings.out_dir, dup_n)
            # get other datasets needed for the model run
            logger.info('Getting various scenario parameters')
            with utilfunc.Timer() as t:
                max_market_share = datfunc.get_max_market_share(con, scenario_settings.schema)
                market_projections = datfunc.get_market_projections(con, scenario_settings.schema)
                load_growth_scenario = scenario_settings.load_growth_scenario.lower() # get financial variables
                # these are technology specific, set up in tidy form with a "tech" field
                financial_parameters = datfunc.get_financial_parameters(con, scenario_settings.schema)                
                inflation_rate = datfunc.get_annual_inflation(con, scenario_settings.schema)
                rate_growth_df = datfunc.get_rate_escalations(con, scenario_settings.schema)
                bass_params = datfunc.get_bass_params(con, scenario_settings.schema)
                learning_curves_mode = datfunc.get_learning_curves_mode(con, scenario_settings.schema)
            logger.info('\tCompleted in: %0.1fs' % t.interval)

            # create psuedo-rangom number generator (not used until tech/finance choice function)
            prng = np.random.RandomState(scenario_settings.random_generator_seed)

            # THIS IS WHERE THINGS CHANGE FOR WIND + SOLAR VS DU VS GHP

            if model_settings.use_existing_schema == False:
                #==========================================================================================================
                # CREATE AGENTS
                #==========================================================================================================
                logger.info("--------------Creating Agents---------------")                                      

                if scenario_settings.tech_mode == 'elec':  
                    # create core agent attributes
                    agent_preparation_elec.generate_core_agent_attributes(cur, con, scenario_settings.techs, scenario_settings.schema, model_settings.sample_pct, model_settings.min_agents, model_settings.agents_per_region,
                                                          scenario_settings.sectors, model_settings.pg_procs, model_settings.pg_conn_string, scenario_settings.random_generator_seed, scenario_settings.end_year)
                

                    #==============================================================================
                    # GET RATE TARIFF LOOKUP TABLE FOR EACH SECTOR                                    
                    #==============================================================================
                    rates_df = agent_mutation_elec.get_electric_rates(cur, con, scenario_settings.schema, scenario_settings.sectors, scenario_settings.random_generator_seed, model_settings.pg_conn_string)

                    #==============================================================================
                    # GET NORMALIZED LOAD PROFILES
                    #==============================================================================
                    normalized_load_profiles_df = agent_mutation_elec.get_normalized_load_profiles(con, scenario_settings.schema, scenario_settings.sectors)

                    # get system sizing targets
                    system_sizing_targets_df = agent_mutation_elec.get_system_sizing_targets(con, scenario_settings.schema)  

                    # get annual system degradation
                    system_degradation_df = agent_mutation_elec.get_system_degradation(con, scenario_settings.schema) 
                    
                    # get state starting capacities
                    state_starting_capacities_df = agent_mutation_elec.get_state_starting_capacities(con, scenario_settings.schema)
                
                    #==========================================================================================================
                    # GET TECH POTENTIAL LIMITS
                    #==========================================================================================================    
                    tech_potential_limits_wind_df = agent_mutation_elec.get_tech_potential_limits_wind(con)
                    tech_potential_limits_solar_df = agent_mutation_elec.get_tech_potential_limits_solar(con)
         
                elif scenario_settings.tech_mode == 'du':
                    # create core agent attributes
                    agent_preparation_geo.generate_core_agent_attributes(cur, con, scenario_settings.techs, scenario_settings.schema, model_settings.sample_pct, model_settings.min_agents, model_settings.agents_per_region,
                                                          scenario_settings.sectors, model_settings.pg_procs, model_settings.pg_conn_string, scenario_settings.random_generator_seed, scenario_settings.end_year)

                    #==========================================================================================================
                    # CALCULATE TRACT AGGREGATE THERMAL LOAD PROFILE
                    #==========================================================================================================                                    
                    # calculate tract demand profiles
                    demand_supply_geo.calculate_tract_demand_profiles(con, cur, scenario_settings.schema, model_settings.pg_procs, model_settings.pg_conn_string)                        
                    
                    #==========================================================================================================
                    # GET TRACT DISTRIBUTION NEWORK SIZES 
                    #==========================================================================================================                        
                    distribution_df = demand_supply_geo.get_distribution_network_data(con, scenario_settings.schema)

                    #==========================================================================================================
                    # SETUP RESOURCE DATA
                    #==========================================================================================================
                    demand_supply_geo.setup_resource_data(cur, con, scenario_settings.schema, scenario_settings.random_generator_seed, model_settings.pg_procs, model_settings.pg_conn_string)
                    
                    #==========================================================================================================
                    # GET BASS DIFFUSION PARAMETERS
                    #==========================================================================================================
                    bass_params_df = diffusion_functions_du.get_bass_params_du(con, scenario_settings.schema)
                    
                elif scenario_settings.tech_mode == 'ghp':
                    # create core agent attributes
                    agent_preparation_geo.generate_core_agent_attributes(cur, con, scenario_settings.techs, scenario_settings.schema, model_settings.sample_pct, model_settings.min_agents, model_settings.agents_per_region,
                                                          scenario_settings.sectors, model_settings.pg_procs, model_settings.pg_conn_string, scenario_settings.random_generator_seed, scenario_settings.end_year)

                    # get state starting capacities                     
                    state_starting_capacities_df = agent_mutation_ghp.get_state_starting_capacities_ghp(con, scenario_settings.schema)
                        

    
            #==========================================================================================================
            # MODEL TECHNOLOGY DEPLOYMENT    
            #==========================================================================================================
            logger.info("---------Modeling Annual Deployment---------")      
            if scenario_settings.tech_mode == 'elec':    
                # get dsire incentives, srecs, and itc inputs
                # TODO: move these to agent mutation
                dsire_opts = datfunc.get_dsire_settings(con, scenario_settings.schema)
                incentives_cap = datfunc.get_incentives_cap(con, scenario_settings.schema)
                dsire_incentives = datfunc.get_dsire_incentives(cur, con, scenario_settings.schema, scenario_settings.techs, scenario_settings.sectors, model_settings.pg_conn_string, dsire_opts)
                srecs = datfunc.get_srecs(cur, con, scenario_settings.schema, scenario_settings.techs, model_settings.pg_conn_string, dsire_opts)
                state_dsire = datfunc.get_state_dsire_incentives(cur, con, scenario_settings.schema, scenario_settings.techs, dsire_opts)            
                itc_options = datfunc.get_itc_incentives(con, scenario_settings.schema)
                for year in scenario_settings.model_years:
                    logger.info('\tWorking on %s' % year)

                    # is it the first model year?
                    is_first_year = year == model_settings.start_year   
                        
                    # get core agent attributes from postgres
                    agents = agent_mutation_elec.get_core_agent_attributes(con, scenario_settings.schema)
                    # filter techs
                    agents = agents.filter('tech in %s' % scenario_settings.techs)
      
                    #==============================================================================
                    # LOAD/POPULATION GROWTH               
                    #==============================================================================
                    # get load growth
                    load_growth_df = agent_mutation_elec.get_load_growth(con, scenario_settings.schema, year)
                    # apply load growth
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_load_growth, (load_growth_df,)).compute(1)              
                    
                    #==============================================================================
                    # RATES         
                    #==============================================================================                
                    # get net metering settings
                    net_metering_df = agent_mutation_elec.get_net_metering_settings(con, scenario_settings.schema, year)
                    # select rates, combining with net metering settings
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.select_electric_rates, (rates_df, net_metering_df)).compute(1)
                    
                    #==============================================================================
                    # ANNUAL RESOURCE DATA
                    #==============================================================================       
                    # get annual resource data
                    resource_solar_df = agent_mutation_elec.get_annual_resource_solar(con, scenario_settings.schema, scenario_settings.sectors)
                    resource_wind_df = agent_mutation_elec.get_annual_resource_wind(con, scenario_settings.schema, year, scenario_settings.sectors)
                    # get technology performance data
                    tech_performance_solar_df = agent_mutation_elec.get_technology_performance_solar(con, scenario_settings.schema, year)
                    tech_performance_wind_df = agent_mutation_elec.get_technology_performance_wind(con, scenario_settings.schema, year)
                    # apply technology performance to annual resource data
                    resource_solar_df = agent_mutation_elec.apply_technology_performance_solar(resource_solar_df, tech_performance_solar_df)
                    resource_wind_df = agent_mutation_elec.apply_technology_performance_wind(resource_wind_df, tech_performance_wind_df)     
                                    
                    #==============================================================================
                    # SYSTEM SIZING
                    #==============================================================================
                    # size systems
                    agents_solar = AgentsAlgorithm(agents.filter_tech('solar'), agent_mutation_elec.size_systems_solar, (system_sizing_targets_df, resource_solar_df, scenario_settings.techs)).compute()                     
                    agents_wind = AgentsAlgorithm(agents.filter_tech('wind'), agent_mutation_elec.size_systems_wind, (system_sizing_targets_df, resource_wind_df, scenario_settings.techs)).compute()
                    # re-combine technologies
                    agents = agents_solar.add_agents(agents_wind)
                    del agents_solar, agents_wind   
                    # update net metering fields after system sizing (because of changes to ur_enable_net_metering)
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.update_net_metering_fields).compute(1)
                                
                    #==============================================================================
                    # DEVELOPABLE CUSTOMERS/LOAD
                    #==============================================================================                            
                    # determine "developable" population
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.calculate_developable_customers_and_load).compute(1)                            
                                                                
                    #==============================================================================
                    # CHECK TECH POTENTIAL LIMITS
                    #==============================================================================                                   
                    agent_mutation_elec.check_tech_potential_limits_wind(agents.filter_tech('wind').dataframe, tech_potential_limits_wind_df, model_settings.out_dir, is_first_year)
                    agent_mutation_elec.check_tech_potential_limits_solar(agents.filter_tech('solar').dataframe, tech_potential_limits_solar_df, model_settings.out_dir, is_first_year)
                                
                    #==============================================================================
                    # GET NORMALIZED LOAD PROFILES
                    #==============================================================================
                    # apply normalized load profiles
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.scale_normalized_load_profiles, (normalized_load_profiles_df, )).compute()
                   
                    #==============================================================================
                    # HOURLY RESOURCE DATA
                    #==============================================================================
                    # get hourly resource
                    normalized_hourly_resource_solar_df = agent_mutation_elec.get_normalized_hourly_resource_solar(con, scenario_settings.schema, scenario_settings.sectors, scenario_settings.techs)
                    normalized_hourly_resource_wind_df = agent_mutation_elec.get_normalized_hourly_resource_wind(con, scenario_settings.schema, scenario_settings.sectors, cur, agents, scenario_settings.techs)
                    # apply normalized hourly resource profiles
                    agents_solar = AgentsAlgorithm(agents.filter_tech('solar'), agent_mutation_elec.apply_normalized_hourly_resource_solar, (normalized_hourly_resource_solar_df, scenario_settings.techs)).compute()
                    agents_wind = AgentsAlgorithm(agents.filter_tech('wind'), agent_mutation_elec.apply_normalized_hourly_resource_wind, (normalized_hourly_resource_wind_df, scenario_settings.techs)).compute()        
                    # re-combine technologies
                    agents = agents_solar.add_agents(agents_wind)
                    del agents_solar, agents_wind               
                    
                    #==============================================================================
                    # TECHNOLOGY COSTS
                    #==============================================================================
                    # get technology costs
                    tech_costs_solar_df = agent_mutation_elec.get_technology_costs_solar(con, scenario_settings.schema, year)
                    tech_costs_wind_df = agent_mutation_elec.get_technology_costs_wind(con, scenario_settings.schema, year)
                    # apply technology costs     
                    agents_solar = AgentsAlgorithm(agents.filter_tech('solar'), agent_mutation_elec.apply_tech_costs_solar, (tech_costs_solar_df, )).compute()
                    agents_wind = AgentsAlgorithm(agents.filter_tech('wind'), agent_mutation_elec.apply_tech_costs_wind, (tech_costs_wind_df, )).compute()
                    # re-combine technologies
                    agents = agents_solar.add_agents(agents_wind)
                    del agents_solar, agents_wind
     
                    #==========================================================================================================
                    # CALCULATE BILL SAVINGS
                    #==========================================================================================================
                    # bill savings are a function of: 
                     # (1) hacked NEM calculations
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.calculate_excess_generation_and_update_nem_settings).compute()
                     # (2) actual SAM calculations
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.calculate_electric_bills_sam, (model_settings.local_cores, )).compute(1)
                    # drop the hourly datasets
                    agents.drop_attributes(['generation_hourly', 'consumption_hourly'], in_place = True)
                    
                    #==========================================================================================================
                    # DEPRECIATION SCHEDULE       
                    #==========================================================================================================
                    # get depreciation schedule for current year
                    depreciation_df = agent_mutation_elec.get_depreciation_schedule(con, scenario_settings.schema, year)
                    # apply depreciation schedule to agents
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_depreciation_schedule, (depreciation_df, )).compute()
                    
                    #==========================================================================================================
                    # SYSTEM DEGRADATION                
                    #==========================================================================================================
                    # apply system degradation to agents
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_system_degradation, (system_degradation_df, )).compute()
                    
                    #==========================================================================================================
                    # CARBON INTENSITIES
                    #==========================================================================================================               
                    # get carbon intensities
                    carbon_intensities_df = agent_mutation_elec.get_carbon_intensities(con, scenario_settings.schema, year)
                    # apply carbon intensities
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_carbon_intensities, (carbon_intensities_df, )).compute()                
    
                    #==========================================================================================================
                    # LEASING AVAILABILITY
                    #==========================================================================================================               
                    # get leasing availability
                    leasing_availability_df = agent_mutation_elec.get_leasing_availability(con, scenario_settings.schema, year)
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_leasing_availability, (leasing_availability_df, )).compute()                     
                    
                    # When not in ReEDS mode set default (and non-impacting) values for the ReEDS parameters
                    agents.dataframe['curtailment_rate'] = 0
                    agents.dataframe['ReEDS_elec_price_mult'] = 1
                    curtailment_method = 'net'           
                                            
                    # Calculate economics of adoption for different busines models
                    df = financial_functions_elec.calc_economics(agents.dataframe, scenario_settings.schema, 
                                               market_projections, financial_parameters, rate_growth_df,
                                               max_market_share, cur, con, year, dsire_incentives, dsire_opts, 
                                               state_dsire, srecs, mode,curtailment_method, itc_options, 
                                               inflation_rate, incentives_cap, 25)
                    
                    
                    # select from choices for business model and (optionally) technology
                    df = tech_choice_elec.select_financing_and_tech(df, prng, scenario_settings.sectors, model_settings.tech_choice_decision_var, scenario_settings.choose_tech, scenario_settings.techs, alpha = 2)                 
    
                    #==========================================================================================================
                    # MARKET LAST YEAR
                    #==========================================================================================================                  
                    # convert back to agents
                    agents = Agents(df)   
                    if is_first_year == True:
                        # calculate initial market shares
                        agents = AgentsAlgorithm(agents, agent_mutation_elec.estimate_initial_market_shares, (state_starting_capacities_df, )).compute()
                    else:
                        # get last year's results
                        market_last_year_df = agent_mutation_elec.get_market_last_year(con, scenario_settings.schema)
                        # apply last year's results to the agents
                        agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_market_last_year, (market_last_year_df, )).compute()                
                    
    
                    #==========================================================================================================
                    # BASS DIFFUSION
                    #==========================================================================================================   
                    # TODO: rewrite this section to use agents class
                    # convert back to dataframe
                    df = agents.dataframe
                    # calculate diffusion based on economics and bass diffusion                   
                    df, market_last_year = diffusion_functions_elec.calc_diffusion(df, cur, con, scenario_settings.techs, scenario_settings.choose_tech, scenario_settings.sectors, scenario_settings.schema, is_first_year, bass_params) 
                    
                    #==========================================================================================================
                    # ESTIMATE TOTAL GENERATION
                    #==========================================================================================================      
                    df = AgentsAlgorithm(Agents(df), agent_mutation_elec.estimate_total_generation).compute().dataframe
                
                    #==========================================================================================================
                    # WRITE OUTPUTS
                    #==========================================================================================================   
                    # TODO: rewrite this section to use agents class
                    # write the incremental results to the database
                    datfunc.write_outputs(con, cur, df, scenario_settings.sectors, scenario_settings.schema) 
                    datfunc.write_last_year(con, cur, market_last_year, scenario_settings.schema)
                            
    
            elif scenario_settings.tech_mode == 'ghp':
                dsire_opts = datfunc.get_dsire_settings(con, scenario_settings.schema)
                incentives_cap_df = datfunc.get_incentives_cap(con, scenario_settings.schema)
                state_incentives_df = datfunc.get_state_dsire_incentives(cur, con, scenario_settings.schema, ['geo'], dsire_opts)
                itc_options = datfunc.get_itc_incentives(con, scenario_settings.schema)
                
                # get initial (year = 2012) agents from postgres
                agents_initial = agent_mutation_geo.get_initial_agent_attributes(con, scenario_settings.schema)
                # set data for "last year" to None (since this will be the first year)
                agents_last_year_df = None
                
                for year in scenario_settings.model_years:
                    logger.info('\tWorking on %s' % year)
                    
                    # is it the first year?
                    is_first_year = year == model_settings.start_year    
 
                    # update year for the initial agents to the current year
                    agents_initial = AgentsAlgorithm(agents_initial, agent_mutation_geo.update_year, (year, )).compute()
 
                    # get new construction agents
                    agents_new = agent_mutation_geo.get_new_agent_attributes(con, scenario_settings.schema, year)

                     # add new agents to the initial agents (this ensures they will be there again next year)
                    agents_initial = agents_initial.add_agents(agents_new)
                    # drop agents_new -- it's no longer needed
                    del agents_new
                    
                    # copy agents_initial (which will be preserved unmutated -- i.e., as-is -- for next year) to agents (which will be mutated for the current year)
                    agents = agents_initial.copy()
                    # change new construction to false for all agents in agents_initial (this ensures that next year they will be treated appropriately)
                    agents_initial.dataframe['new_construction'] = False
        
                    # drop du agents
                    agents = agents.filter_tech('ghp')                                        
                    
                    #==============================================================================
                    # HVAC SYSTEM AGES
                    #==============================================================================                        
                    # update system ages
                    agents = AgentsAlgorithm(agents, agent_mutation_geo.update_system_ages, (year, is_first_year, sunk_costs)).compute()
                    # check which agents require new systems (new construction and those whose systems are too old)
                    agents = AgentsAlgorithm(agents, agent_mutation_geo.calc_years_to_replacement).compute()                                

                    #==========================================================================================================
                    # MAP TO CRB GHP SIMULATIONS
                    #========================================================================================================== 
                    # get mapping lkup table
                    baseline_lkup_df = agent_mutation_ghp.get_ghp_baseline_type_lkup(con, scenario_settings.schema)
                    # map agents to baseline system types
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.map_agents_to_ghp_baseline_types, (baseline_lkup_df, )).compute()
                    # get baseline GHP simulations
                    baseline_ghp_sims_df = agent_mutation_ghp.get_ghp_baseline_simulations(con, scenario_settings.schema)
                    # join baseline GHP simulations
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.join_crb_ghp_simulations, (baseline_ghp_sims_df, )).compute()
                    
                    #==========================================================================================================
                    # MARK MODELLABLE AND UN-MODELLABLE AGENTS
                    #==========================================================================================================                          
                    # mark agents that can't be modeled due to no representative GHP simulations
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.mark_unmodellable_agents).compute()
                    
                    #==============================================================================
                    # SYSTEM SIZING
                    #==============================================================================
                    # size systems
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.size_systems_ghp).compute()

                    #==============================================================================
                    # REPLICATE AGENTS FOR DIFFERENT GHP SYSTEM CONFIGURATIONS
                    #==============================================================================        
                    system_configurations = ['vertical', 'horizontal']                    
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.replicate_agents_by_factor, ('sys_config', system_configurations), row_increase_factor = len(system_configurations)).compute()

                    #==============================================================================
                    # SITING CONSTRAINTS
                    #==============================================================================
                    # get siting constraints settings
                    siting_constraints_df = agent_mutation_ghp.get_siting_constraints_ghp(con, scenario_settings.schema, year)
                    # apply siting constraints
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_siting_constraints_ghp, (siting_constraints_df, )).compute()

                    #==============================================================================
                    # IDENTIFY MARKET ELIGIBLE BUILDINGS
                    #==============================================================================                            
                    # flag the agents that are part of the eligible market for GHP
                    # (i.e., these agents can be developed EVENTUALLY)
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.identify_market_eligible_agents).compute()
                    
                    #==============================================================================
                    # IDENTIFY BASS DEPLOYABLE BUILDINGS
                    #==============================================================================                            
                    # flag the agents that are deployable during this model year
                    # (i.e., these are the subset of market eligible agents can be developed NOW)
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.identify_bass_deployable_agents, (sunk_costs, )).compute()                              


                    #==============================================================================
                    # DETERMINE GHP-COMPATIBILITY
                    #==============================================================================  
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.determine_ghp_compatibility).compute()
                    
                    #==============================================================================
                    # TECHNOLOGY COSTS
                    #==============================================================================
                    # get ghp technology costs
                    tech_costs_ghp_df = agent_mutation_ghp.get_technology_costs_ghp(con, scenario_settings.schema, year)
                    # determine whether to apply rest of system GHP costs
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.requires_ghp_rest_of_sysem_costs).compute()
                    # apply ghp technology costs     
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_tech_costs_ghp, (tech_costs_ghp_df, )).compute()
                    
                    # get baseline/conventional system costs
                    tech_costs_baseline_df = agent_mutation_ghp.get_technology_costs_baseline(con, scenario_settings.schema, year)
                    # apply baseline/conventional system costs
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_tech_costs_baseline, (tech_costs_baseline_df, sunk_costs)).compute()   

                    #==============================================================================
                    # TECHNOLOGY PERFORMANCE IMPROVEMENTS AND DEGRADATION
                    #==============================================================================              
                    # get GHP technology performance improvements
                    tech_performance_ghp_df = agent_mutation_ghp.get_technology_performance_improvements_ghp(con, scenario_settings.schema, year)
                    # apply GHP technology performance improvements
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_technology_performance_ghp, (tech_performance_ghp_df, )).compute()
                    # get GHP system degradatation
                    system_degradation_df = agent_mutation_ghp.get_system_degradataion_ghp(con, scenario_settings.schema, year)
                    # apply GHP degradation
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_system_degradation_ghp, (system_degradation_df, )).compute()
                    
                    # get baseline tech performance improvements and degradation
                    tech_performance_baseline_df = agent_mutation_ghp.get_technology_performance_improvements_and_degradation_baseline(con, scenario_settings.schema, year)
                    # apply baseline tech performance improvements and degradation
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_technology_performance_improvements_and_degradation_baseline, (tech_performance_baseline_df, )).compute()
                    
                    #==========================================================================================================
                    # CALCULATE SITE ENERGY CONSUMPTION
                    #==========================================================================================================
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.calculate_site_energy_consumption_ghp).compute()
                    
                    #==========================================================================================================
                    # DEPRECIATION SCHEDULE       
                    #==========================================================================================================
                    # get depreciation schedule for current year
                    depreciation_df = agent_mutation_ghp.get_depreciation_schedule(con, scenario_settings.schema, year)
                    # apply depreciation schedule to agents
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_depreciation_schedule, (depreciation_df, )).compute()
                    
                    #==========================================================================================================
                    # LEASING AVAILABILITY
                    #==========================================================================================================               
                    # get leasing availability
                    leasing_availability_df = agent_mutation_ghp.get_leasing_availability(con, scenario_settings.schema, year)
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_leasing_availability, (leasing_availability_df, )).compute()                                        
            
                    #==============================================================================
                    # ENERGY PRICES
                    #==============================================================================
                    # get and apply expected rate escalations
                    rate_escalations_df = agent_mutation_ghp.get_expected_rate_escalations(con, scenario_settings.schema, year)
                    agents =  AgentsAlgorithm(agents, agent_mutation_ghp.apply_expected_rate_escalations, (rate_escalations_df, )).compute()
            
                    #==========================================================================================================
                    # FINANCIAL CALCULATIONS
                    #==========================================================================================================            
                    # replicate agents for business models
                    agents =  AgentsAlgorithm(agents, agent_mutation_ghp.replicate_agents_by_factor, ('business_model', ['host_owned', 'tpo']), row_increase_factor = 2).compute()
                    # add metric field based on business model                    
                    agents =  AgentsAlgorithm(agents, agent_mutation_ghp.add_metric_field).compute()

                    # apply financial parameters
                    agents =  AgentsAlgorithm(agents, agent_mutation_ghp.apply_financial_parameters, (financial_parameters, )).compute()
                    
                    # calculate state incentives
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.calc_state_incentives, (state_incentives_df, )).compute()

                    # calculate value of itc
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.calc_value_of_itc, (itc_options, year)).compute()
            
                    # apply incentives cap
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_incentives_cap, (incentives_cap_df, )).compute()
                    
                    # calculate raw cashflows for ghp and baseline technologies
                    analysis_period = 30
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_cashflows, ('ghp', analysis_period)).compute()
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_cashflows, ('baseline', analysis_period)).compute()
                    
                    # calculate net cashflows for GHP system relative to baseline
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_net_cashflows_host_owned).compute()
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_net_cashflows_third_party_owned).compute()

                    # calculate monthly bill savings
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_monthly_bill_savings).compute()
                    # calculate payback
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_payback, ('net_cashflows_ho', analysis_period)).compute()
                    # calculate ttd
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_ttd, ('net_cashflows_ho', )).compute()
                    # assign metric value precise
                    agents = AgentsAlgorithm(agents, financial_functions_geo.assign_metric_value_precise).compute()
                    # calculate NPV (assuming different discount rates)
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_npv, ('net_cashflows_ho', 0.04, 'npv4')).compute()
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_npv, ('net_cashflows_ho', 'discount_rate', 'npv_agent')).compute()
                    # normalize npv values
                    agents = AgentsAlgorithm(agents, financial_functions_geo.normalize_value, ('npv4', 'ghp_system_size_tons', 'npv4_per_ton')).compute()
                    agents = AgentsAlgorithm(agents, financial_functions_geo.normalize_value, ('npv_agent', 'ghp_system_size_tons', 'npv_agent_per_ton')).compute()
                    # join inflation rate info (needed for lcoe calcs)
                    agents = AgentsAlgorithm(agents, financial_functions_geo.assign_value, (inflation_rate, 'inflation_rate')).compute()
                    # calculate LCOE
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_lcoe).compute()  # TODO: revise this function

                    #  assign max market share
                    agents = AgentsAlgorithm(agents, financial_functions_geo.calculate_max_market_share, (max_market_share, )).compute()
                    
                    #==========================================================================================================
                    # CHOOSE FROM SYSTEM CONFIGURATIONS AND BUSINESS MODELS
                    #==========================================================================================================     
                    # clean up the decision var for tech/financing choice
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.sanitize_decision_col, ('max_market_share', 'mms_sanitized')).compute()
                    # a new temporary id for agent + business model combos
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.create_new_id_column, (['agent_id', 'business_model'], 'temp_id')).compute()
                    # mark options to exclude from sys_config and business_model choices
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.mark_excluded_options).compute()
                    # select from sys_config choices
                    agents = Agents(tech_choice_geo.probabilistic_choice(agents.dataframe, prng, uid_col = 'temp_id', options_col = 'sys_config', excluded_options_col = 'excluded_option', decision_col = 'mms_sanitized', alpha = 2, always_return_one = True))
                    # select from business_model choices
                    agents = Agents(tech_choice_geo.probabilistic_choice(agents.dataframe, prng, uid_col = 'agent_id', options_col = 'business_model', excluded_options_col = 'excluded_option', decision_col = 'mms_sanitized', alpha = 2, always_return_one = True))    

                    #==========================================================================================================
                    # MARKET LAST YEAR
                    #==========================================================================================================                     
                    if is_first_year == True:
                        # calculate initial market shares
                        agents = AgentsAlgorithm(agents, agent_mutation_ghp.estimate_initial_market_shares, (state_starting_capacities_df, )).compute()
                    else:
                        # get last year's results
                        market_last_year_df = agent_mutation_ghp.get_market_last_year(con, scenario_settings.schema)
                        # apply last year's results to the agents
                        agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_market_last_year, (market_last_year_df, )).compute()                
                    
    
                    #==========================================================================================================
                    # BASS DIFFUSION
                    #==========================================================================================================
                    # apply bass p/q/teq params
                    agents = AgentsAlgorithm(agents, agent_mutation_ghp.apply_bass_params, (bass_params, )).compute()    
                    # calculate the equivalent time that has passed on the newly scaled bass curve
                    agents = AgentsAlgorithm(agents, diffusion_functions_geo.calc_equiv_time).compute()
                    # set the number of years to advance along bass curve (= teq2)
                    agents = AgentsAlgorithm(agents, diffusion_functions_ghp.set_number_of_years_to_advance, (is_first_year, )).compute()
                    # calculate new cumulative "adoption fraction" according to bass
                    agents = AgentsAlgorithm(agents, diffusion_functions_geo.bass_diffusion).compute()
                    # apply adoption fraction to MMS to calculate actual diffusion
                    agents = AgentsAlgorithm(agents, diffusion_functions_ghp.calculate_bass_and_diffusion_market_share).compute()
                    # calculate diffusion results metrics
                    agents = AgentsAlgorithm(agents, diffusion_functions_ghp.calculate_diffusion_result_metrics).compute()
                    # extract results for "market last year"
                    market_last_year_df = diffusion_functions_ghp.extract_market_last_year(agents.dataframe)
    
                    #==========================================================================================================
                    # WRITE OUTPUTS
                    #==========================================================================================================   
                    # write the incremental results to the database
                    agent_mutation_ghp.write_agent_outputs_ghp(con, cur, scenario_settings.schema, agents.dataframe) 
                    agent_mutation_ghp.write_last_year(con, cur, market_last_year_df, scenario_settings.schema)

                    
                # TODO: get visualizations working and remove this short-circuit
                return 'Simulations Complete'  
    
            elif scenario_settings.tech_mode == 'du':
                # get initial (year = 2012) agents from postgres
                agents_initial = agent_mutation_geo.get_initial_agent_attributes(con, scenario_settings.schema)                
                
                for year in scenario_settings.model_years:
                    logger.info('\tWorking on %s' % year)
                    
                    # is it the first year?
                    is_first_year = year == model_settings.start_year                        
                    
                    # update year for the initial agents to the current year
                    agents_initial = AgentsAlgorithm(agents_initial, agent_mutation_geo.update_year, (year, )).compute()
 
                    # get new construction agents
                    agents_new = agent_mutation_geo.get_new_agent_attributes(con, scenario_settings.schema, year)                   
                    
                    # add new agents to the initial agents (this ensures they will be there again next year)
                    agents_initial = agents_initial.add_agents(agents_new)
                    # drop agents_new -- it's no longer needed
                    del agents_new                    
                    
                    # copy agents_initial (which will be preserved unmutated -- i.e., as-is -- for next year) to agents (which will be mutated for the current year)
                    agents = agents_initial.copy()
                    # change new construction to false for all agents in agents_initial (this ensures that next year they will be treated appropriately)
                    agents_initial.dataframe['new_construction'] = False
        
                    # drop ghp agents
                    agents = agents.filter_tech('du')                           
                    
                    # get previously subscribed agents
                    previously_subscribed_agents_df = demand_supply_geo.get_previously_subscribed_agents(con, scenario_settings.schema)
                    # subtract previously subscribed agents
                    agents = AgentsAlgorithm(agents, demand_supply_geo.subtract_previously_subscribed_agents, (previously_subscribed_agents_df, )).compute()
                    
                    # get regional prices of energy
                    energy_prices_df = agent_mutation_du.get_regional_energy_prices(con, scenario_settings.schema, year)
                    # apply regional heating/cooling prices
                    agents = AgentsAlgorithm(agents, agent_mutation_du.apply_regional_energy_prices, (energy_prices_df, )).compute()
                    
                    # get du cost data
                    end_user_costs_du_df = agent_mutation_du.get_end_user_costs_du(con, scenario_settings.schema, year)
                    # apply du cost data
                    agents = AgentsAlgorithm(agents, agent_mutation_du.apply_end_user_costs_du, (end_user_costs_du_df, )).compute()               
                                 
                    # update system ages
                    agents = AgentsAlgorithm(agents, agent_mutation_geo.update_system_ages, (year, is_first_year, sunk_costs)).compute()
                    # check which agents require new systems (new construction and those whose systems are too old)
                    agents = AgentsAlgorithm(agents, agent_mutation_geo.calc_years_to_replacement).compute()
                    
                    #==============================================================================
                    # BUILD SUPPLY CURVES FOR EACH TRACT
                    #==============================================================================
                    # get tract demand profiles
                    tract_demand_profiles_df = demand_supply_geo.get_tract_demand_profiles(con, scenario_settings.schema, year)    
                    # calculate tract peak demand
                    tract_peak_demand_df = demand_supply_geo.calculate_tract_peak_demand(tract_demand_profiles_df)                    
                    # calculate distribution demand density
                    demand_density_df = demand_supply_geo.calculate_distribution_demand_density(tract_peak_demand_df, distribution_df)

                    # get resources
                    resource_df = demand_supply_geo.get_resource_data(con, scenario_settings.schema, year)
                    # get previously subscribed wellsets
                    previously_subscribed_wellsets_df = demand_supply_geo.get_previously_subscribed_wellsets(con, scenario_settings.schema)
                    # subtract previously subscribed wellsets
                    resource_df = demand_supply_geo.subtract_previously_subscribed_wellsets(resource_df, previously_subscribed_wellsets_df)

                    # get natural gas prics
                    ng_prices_df = demand_supply_geo.get_natural_gas_prices(con, scenario_settings.schema, year)
                    # get the du cost data
                    costs_and_performance_df = demand_supply_geo.get_plant_cost_and_performance_data(con, scenario_settings.schema, year)
                    reservoir_factors_df = demand_supply_geo.get_reservoir_factors(con, scenario_settings.schema, year)
                    # get the plant finance data
                    plant_finances_df = demand_supply_geo.get_plant_finance_data(con, scenario_settings.schema, year)
                    plant_construction_factor_df = demand_supply_geo.get_plant_construction_factor_data(con, scenario_settings.schema, year)
                    # NOTE: This isn't currently used, instead, we use a fixed value
                    plant_construction_finance_factor = 1.106
                    # TODO: change the input for plant construction factor
                    plant_depreciation_df = demand_supply_geo.get_plant_depreciation_data(con, scenario_settings.schema, year)                    
                    # calculate the plant and boiler capacity factors
                    capacity_factors_df = demand_supply_geo.calculate_plant_and_boiler_capacity_factors(tract_peak_demand_df, costs_and_performance_df, tract_demand_profiles_df, year)
                    # apply the plant cost data
                    resources_with_costs_df = demand_supply_geo.apply_cost_and_performance_data(resource_df, costs_and_performance_df, reservoir_factors_df, plant_finances_df, demand_density_df,  capacity_factors_df, ng_prices_df)
                    
                    #==============================================================================
                    # SUPPLY AND DEMAND CALCULATIONS
                    #============================================================================== 
                    # CALCULATE AGENT LCOE
                    plant_lifetime = plant_finances_df.plant_lifetime_yrs.tolist()[0]
                    agents = AgentsAlgorithm(agents, demand_supply_geo.calc_agent_lcoe, (plant_lifetime, )).compute()
                    # CONVERT INTO DEMAND CURVES
                    demand_curves_df = demand_supply_geo.lcoe_to_demand_curve(agents.dataframe.copy())
                    
                    # CALCULATE PLANT LCOE
                    resources_with_costs_df = demand_supply_geo.calc_plant_lcoe(resources_with_costs_df, plant_depreciation_df, plant_construction_finance_factor)                                                                                     
                    # CONVERT INTO SUPPLY CURVES
                    supply_curves_df = demand_supply_geo.lcoe_to_supply_curve(resources_with_costs_df)
                    
                    #==============================================================================
                    # CALCULATE PLANT SIZES BASED ON ECONOMIC POTENTIAL
                    #==============================================================================   
                    plant_sizes_economic_df = demand_supply_geo.intersect_supply_demand_curves(demand_curves_df, supply_curves_df)
                                          
                    #==============================================================================
                    # CALCULATE PLANT SIZES BASED ON MARKET POTENTIAL
                    #==============================================================================                    
                    plant_sizes_market_df = demand_supply_geo.calc_plant_sizes_market(demand_curves_df, supply_curves_df, plant_sizes_economic_df) # TODO: replace with actual function
                    
                    #==============================================================================
                    # BASS DIFFUSION
                    #==============================================================================                    
                    # get previous year market share
                    existing_market_share_df = diffusion_functions_du.get_existing_market_share(con, cur, scenario_settings.schema, year)
                    # calculate total market demand
                    total_market_demand_mw = diffusion_functions_du.calculate_total_market_demand(tract_peak_demand_df)
                    # calculate current max market share
                    current_mms = diffusion_functions_du.calculate_current_mms(plant_sizes_market_df, total_market_demand_mw)
                    # calculate new incremental market share pct
                    new_market_share_pct = diffusion_functions_du.calculate_new_incremental_market_share_pct(existing_market_share_df, current_mms, bass_params_df, year)
                    # calculate new incremental market share capacity (mw)
                    new_incremental_capacity_mw = diffusion_functions_du.calculate_new_incremental_capacity_mw(new_market_share_pct, total_market_demand_mw)
                    # select plants to be built
                    plants_to_be_built_df = diffusion_functions_du.select_plants_to_be_built(plant_sizes_market_df, new_incremental_capacity_mw, scenario_settings.random_generator_seed)
                    # summarize the new cumulative market share (in terms of capacity and pct) based on the selected plants
                    # (note: this will differ a bit from the new_incremental_capacity_mw and new_market_share_pct + existing_market_share_df because it is based on
                    # selected plants, which may not sum perfectly to the theoreticaly incremental additions)
                    cumulative_market_share_df = diffusion_functions_du.calculate_new_cumulative_market_share(existing_market_share_df, plants_to_be_built_df, total_market_demand_mw, year)                    
                    
                    #==============================================================================
                    # TRACKING RESULTS
                    #==============================================================================     
                    # SUMMARY OF MARKET
                    # write/store summary market share outputs
                    diffusion_functions_du.write_cumulative_market_share(con, cur, cumulative_market_share_df, scenario_settings.schema)

                    # AGENTS
                    # identify the subscribed agents
                    subscribed_agents_df = diffusion_functions_du.identify_subscribed_agents(plants_to_be_built_df, demand_curves_df)
                    # append this info to the agents
                    agents = AgentsAlgorithm(agents, diffusion_functions_du.mark_subscribed_agents, (subscribed_agents_df, )).compute()
                    # write agents to database
                    diffusion_functions_du.write_agent_outputs(con, cur, agents, scenario_settings.schema)
                    
                    # PLANTS
                    # identify the subscribed resources
                    subscribed_resources_df = diffusion_functions_du.identify_subscribed_resources(plants_to_be_built_df, supply_curves_df)
                    # append this info to other key information about 
                    subscribed_resources_with_costs_df = diffusion_functions_du.mark_subscribed_resources(resources_with_costs_df, subscribed_resources_df)
                    # write results to database                    
                    diffusion_functions_du.write_resources_outputs(con, cur, subscribed_resources_with_costs_df, scenario_settings.schema)
            
                # TODO: get visualizations working and remove this short-circuit
                return 'Simulations Complete'   
                
            #==============================================================================
            #    Outputs & Visualization
            #==============================================================================
            logger.info("---------Saving Model Results---------")
            out_subfolders = datfunc.create_tech_subfolders(out_scen_path, scenario_settings.techs, out_subfolders, scenario_settings.choose_tech)
            
            # copy outputs to csv     
            datfunc.copy_outputs_to_csv(scenario_settings.techs, scenario_settings.schema, out_scen_path, cur, con)

            # add indices to postgres output table
            datfunc.index_output_table(con, cur, scenario_settings.schema)
            
            # write reeds mode outputs to csvs in case they're needed
            reedsfunc.write_reeds_offline_mode_data(scenario_settings.schema, con, scenario_settings.techs, out_scen_path)
            
            # create output html report                
            datfunc.create_scenario_report(scenario_settings.techs, scenario_settings.schema, scenario_settings.scen_name, out_scen_path, cur, con, model_settings.Rscript_path, model_settings.pg_params_file)
            
            # create tech choice report (if applicable)
            datfunc.create_tech_choice_report(scenario_settings.choose_tech, scenario_settings.schema, scenario_settings.scen_name, out_scen_path, cur, con, model_settings.Rscript_path, model_settings.pg_params_file)
            
            # after all techs have been processed:
            #####################################################################
            # drop the new scenario_settings.schema
            datfunc.drop_output_schema(model_settings.pg_conn_string, scenario_settings.schema, model_settings.delete_output_schema)
            #####################################################################
            
            logger.info("-------------Model Run Complete-------------")
            logger.info('Completed in: %.1f seconds' % (time.time() - model_settings.model_init))
                            

    except Exception, e:
        # close the connection (need to do this before dropping schema or query will hang)      
        if 'con' in locals():
            con.close()
        if 'logger' in locals():
            logger.error(e.__str__(), exc_info = True)
        if 'scenario_settings' in locals() and scenario_settings.schema is not None:
            # drop the output schema
            datfunc.drop_output_schema(model_settings.pg_conn_string, scenario_settings.schema, True)

        if 'logger' not in locals():
            raise
        
    
    finally:
        if 'logger' in locals():
            utilfunc.shutdown_log(logger)
            utilfunc.code_profiler(model_settings.out_dir)
    
if __name__ == '__main__':
    main()