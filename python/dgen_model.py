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
import config
import storage_functions as sFuncs
# ---------------------------------------------
# order of the next 3 needs to be maintained
# otherwise the logger may not work correctly
# (I think the order needs to follow the order 
# in which each module is used in __main__)
import data_functions as datfunc
# ---------------------------------------------
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

# Import from support function repo
#import dispatch_functions as dFuncs
#import tariff_functions as tFuncs
#import financial_functions as fFuncs
import general_functions as gFuncs


#==============================================================================
# raise  numpy and pandas warnings as exceptions
#==============================================================================
#np.seterr(all='raise')
pd.set_option('mode.chained_assignment', None)
#==============================================================================


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
            
            #==============================================================================
            # TEMPORARY PATCH FOR STORAGE BRANCH             
            # TODO: remove this once storage has been added to the input excel sheet
            # if in storage_model, override input techs with ["solar", "storage"]
            if model_settings.solar_plus_storage_mode == True:
                scenario_settings.set('techs', ['solar', 'storage'])
            #==============================================================================
                
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


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#
############################ Elec Setup #######################################                                 

                if scenario_settings.tech_mode == 'elec':  
                    # create core agent attributes
                    if model_settings.mode in ['run', 'setup_develop']:
                        agent_preparation_elec.generate_core_agent_attributes(cur, con, scenario_settings.techs, scenario_settings.schema, model_settings.sample_pct, model_settings.min_agents, model_settings.agents_per_region,
                                                          scenario_settings.sectors, model_settings.pg_procs, model_settings.pg_conn_string, scenario_settings.random_generator_seed, scenario_settings.end_year)
                

                    #==============================================================================
                    # GET RATE RANKS & TARIFF LOOKUP TABLE FOR EACH SECTOR
                    #==============================================================================
                    # GET RATE TARIFF LOOKUP TABLE FOR EACH SECTOR           
                    rates_df = agent_mutation_elec.get_sam_electric_rates(cur, con, scenario_settings.schema, scenario_settings.sectors, scenario_settings.random_generator_seed, model_settings.pg_conn_string, model_settings.mode)

                    #==============================================================================
                    # GET NORMALIZED LOAD PROFILES
                    #==============================================================================
                    normalized_load_profiles_df = agent_mutation_elec.get_normalized_load_profiles(con, scenario_settings.schema, scenario_settings.sectors, model_settings.mode)

                    # get system sizing targets
                    system_sizing_targets_df = agent_mutation_elec.get_system_sizing_targets(con, scenario_settings.schema)  

                    # get annual system degradation
                    system_degradation_df = agent_mutation_elec.get_system_degradation(con, scenario_settings.schema) 
                    
                    # get state starting capacities
                    state_starting_capacities_df = agent_mutation_elec.get_state_starting_capacities(con, scenario_settings.schema)
                
                    #==========================================================================================================
                    # GET TECH POTENTIAL LIMITS
                    #==========================================================================================================    
                    # only check this if actually running the model
                    if model_settings.mode == 'run':
                        tech_potential_limits_wind_df = agent_mutation_elec.get_tech_potential_limits_wind(con)
                        tech_potential_limits_solar_df = agent_mutation_elec.get_tech_potential_limits_solar(con)
         
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#
############################ S+S Setup ########################################          
         
                elif scenario_settings.tech_mode == 'solar+storage':  
                    # create core agent attributes
                    if model_settings.mode in ['run', 'setup_develop']:
                        #==============================================================================
                        # TEMPORARY PATCH FOR STORAGE BRANCH             
                        # override scenario_settings.techs (which is currently = ['solar', 'storage'])
                        # to be set to ['solar'] only. this is necessary because many subsequent functinos
                        # aren't yet set up (and may never actually need) to deal with 'storage' as a technology
                        scenario_settings.set('techs', ['solar'])
                        #==============================================================================                                                
                        
                        agent_preparation_elec.generate_core_agent_attributes(cur, con, scenario_settings.techs, scenario_settings.schema, model_settings.sample_pct, model_settings.min_agents, model_settings.agents_per_region,
                                                          scenario_settings.sectors, model_settings.pg_procs, model_settings.pg_conn_string, scenario_settings.random_generator_seed, scenario_settings.end_year)
                
                    #==============================================================================
                    # GET RATE RANKS & TARIFF LOOKUP TABLE FOR EACH SECTOR
                    #==============================================================================                    
                    # get (ranked) rates for each sector
                    rates_rank_df = agent_mutation_elec.get_electric_rates(cur, con, scenario_settings.schema, scenario_settings.sectors, scenario_settings.random_generator_seed, model_settings.pg_conn_string, model_settings.mode)
                    # find the list of unique rate ids that are included in rates_rank_df
                    selected_rate_ids = agent_mutation_elec.identify_selected_rate_ids(rates_rank_df)
                    # get lkup table with rate jsons
                    rates_json_df = agent_mutation_elec.get_electric_rates_json(con, selected_rate_ids)
                    rates_json_df = rates_json_df.set_index('rate_id_alias')
                    

                    #==============================================================================
                    # GET NORMALIZED LOAD PROFILES
                    #==============================================================================
                    normalized_load_profiles_df = agent_mutation_elec.get_normalized_load_profiles(con, scenario_settings.schema, scenario_settings.sectors, model_settings.mode)

                    # get system sizing targets
                    # TODO remove
                    system_sizing_targets_df = agent_mutation_elec.get_system_sizing_targets(con, scenario_settings.schema)  

                    # get annual system degradation
                    system_degradation_df = agent_mutation_elec.get_system_degradation(con, scenario_settings.schema) 
                    
                    # get state starting capacities
                    state_starting_capacities_df = agent_mutation_elec.get_state_starting_capacities(con, scenario_settings.schema)
                    
                    # get schedule of battery costs - ingesting 
                    tech_cost_storage_schedules_df = pd.read_csv('storage_cost_schedules.csv', index_col='year')

                    #==========================================================================================================
                    # GET TECH POTENTIAL LIMITS
                    #==========================================================================================================    
                    # only check this if actually running the model
                    if model_settings.mode == 'run':
                        tech_potential_limits_solar_df = agent_mutation_elec.get_tech_potential_limits_solar(con)
                  

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#
############################ Elec Deployment ##################################    
    
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
                    agents = agent_mutation_elec.get_core_agent_attributes(con, scenario_settings.schema, model_settings.mode, scenario_settings.region)
                    # filter techs
                    agents = agents.filter('tech in %s' % scenario_settings.techs)
                    # store canned agents (if in setup_develop mode)
                    datfunc.setup_canned_agents(model_settings.mode, agents, scenario_settings.tech_mode, 'both')
                    # update year (this is really only ncessary in develop-mode since canned agents have the wrong year)
                    agents.dataframe.loc[:, 'year'] = year
      
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

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#
############################ S+S Deployment ###################################

            elif scenario_settings.tech_mode == 'solar+storage':    
                # get dsire incentives, srecs, and itc inputs
                # TODO: move these to agent mutation
                dsire_opts = datfunc.get_dsire_settings(con, scenario_settings.schema)
                incentives_cap = datfunc.get_incentives_cap(con, scenario_settings.schema)
                dsire_incentives = datfunc.get_dsire_incentives(cur, con, scenario_settings.schema, scenario_settings.techs, scenario_settings.sectors, model_settings.pg_conn_string, dsire_opts)
                srecs = datfunc.get_srecs(cur, con, scenario_settings.schema, scenario_settings.techs, model_settings.pg_conn_string, dsire_opts)
                state_dsire = datfunc.get_state_dsire_incentives(cur, con, scenario_settings.schema, scenario_settings.techs, dsire_opts)            
                itc_options = datfunc.get_itc_incentives(con, scenario_settings.schema)

                #==============================================================================
                # GENERATE AGENT OBJECT WITH CORE IMMUTABLE ATTRIBUTES               
                #==============================================================================
                # get core agent attributes from postgres
                agents = agent_mutation_elec.get_core_agent_attributes(con, scenario_settings.schema, model_settings.mode, scenario_settings.region)
                # filter techs
                agents = agents.filter('tech in %s' % scenario_settings.techs)
                # store canned agents (if in setup_develop mode)
                datfunc.setup_canned_agents(model_settings.mode, agents, scenario_settings.tech_mode, 'both')
                # change pca_reg to ba TODO: remove pca_reg in original agent definition
                agents.dataframe['ba'] = agents.dataframe['pca_reg']
                agents.dataframe.drop(['pca_reg'], axis=1)
                 
                # check rate coverage
                rates_rank_df = agent_mutation_elec.check_rate_coverage(agents.dataframe, rates_rank_df, rates_json_df)
                #==========================================================================================================
                # Set up dataframes to record aggregated results
                #==========================================================================================================    
                ba_list = np.unique(np.array(agents.dataframe['ba']))
                
                year_and_reg_set = gFuncs.cartesian([ba_list, scenario_settings.model_years])                
                
                storage_dispatch_df_col_list = list(['ba', 'year'])
                hour_list = list()
                for hour in np.arange(1,8761):
                    hour_list = hour_list + ['H%s' % hour]
                hour_list = list(np.arange(1,8761))
                storage_dispatch_df_col_list = storage_dispatch_df_col_list + hour_list

                # storage_dispatch_df_year is just the dispatches for agents who adopted that year.
                # storage_dispatch_df is the total dispatches for all adopters up to that point
                dispatch_by_ba_and_year = pd.DataFrame(columns = storage_dispatch_df_col_list)
                dispatch_by_ba_and_year = pd.DataFrame(columns = storage_dispatch_df_col_list)
                
                generation_new_adopters = pd.DataFrame(columns = storage_dispatch_df_col_list)
                generation_all_adopters = pd.DataFrame(columns = storage_dispatch_df_col_list)
                
                ba_cum_pv_mw = pd.DataFrame(index=ba_list)
                ba_cum_batt_mw = pd.DataFrame(index=ba_list)
                ba_cum_batt_mwh = pd.DataFrame(index=ba_list)
                
                #==============================================================================
                # RESOURCE DATA
                #==============================================================================       
                # get hourly resource
                normalized_hourly_resource_solar_df = agent_mutation_elec.get_normalized_hourly_resource_solar(con, scenario_settings.schema, scenario_settings.sectors, scenario_settings.techs)
                agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_solar_capacity_factor_profile, (normalized_hourly_resource_solar_df, )).compute()
                del(normalized_hourly_resource_solar_df)
                
                #==============================================================================
                # SET BATTERY REPLACEMENT YEAR
                #==============================================================================
                batt_replacement_yr = int(10.0)
                agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_batt_replace_schedule, (batt_replacement_yr, )).compute()
                                    
                #==============================================================================
                # LOAD PROFILES
                #==============================================================================
                # apply normalized load profiles
                agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_normalized_load_profiles, (normalized_load_profiles_df, )).compute()
                del(normalized_load_profiles_df)
                #==========================================================================================================
                # SYSTEM DEGRADATION                
                #==========================================================================================================
                # apply system degradation to agents
                agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_system_degradation, (system_degradation_df, )).compute()
                    
                    
                #==========================================================================================================
                # WRITE BASE AGENT_DF TO DISK              
                #==========================================================================================================
                agents.dataframe.to_pickle(out_scen_path + '/agent_df_base.pkl') 
                    
                for year in scenario_settings.model_years:
                    
                    logger.info('\tWorking on %s' % year)

                    # copy the core agent object                    
                    agents.dataframe = pd.read_pickle(out_scen_path + '/agent_df_base.pkl')

                    # is it the first model year?
                    is_first_year = year == model_settings.start_year   
                        
                    # update year (this is really only ncessary in develop-mode since canned agents have the wrong year)
                    agents.dataframe.loc[:, 'year'] = year
      
                    #==============================================================================
                    # LOAD/POPULATION GROWTH               
                    #==============================================================================
                    # get load growth
                    load_growth_df = agent_mutation_elec.get_load_growth(con, scenario_settings.schema, year)
                    # apply load growth
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_load_growth, (load_growth_df,)).compute(1)              
                    
                    #==============================================================================
                    # TARIFFS FOR EXPORTED GENERATION (NET METERING)       
                    #==============================================================================                
                    # get net metering settings
                    net_metering_df = agent_mutation_elec.get_net_metering_settings(con, scenario_settings.schema, year)
                    # apply export generation tariff settings
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_export_generation_tariffs, (net_metering_df, )).compute()
    
    
                    #==============================================================================
                    # ELECTRICITY PRICE MULTIPLIER AND ESCALATION     
                    #==============================================================================                
                    # Apply each agent's electricity price (real terms relative)
                    # to 2016, and calculate their assumption about price changes.
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_elec_price_multiplier_and_escalator, (year, rate_growth_df)).compute()
                 

                    #==============================================================================
                    # TECHNOLOGY PERFORMANCE
                    #==============================================================================       
                    # get technology performance data
                    tech_performance_solar_df = agent_mutation_elec.get_technology_performance_solar(con, scenario_settings.schema, year)
                    # apply technology performance data
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_tech_performance_solar, (tech_performance_solar_df, )).compute()
                   
#                    #==============================================================================
#                    # CHECK TECH POTENTIAL LIMITS
#                    #==============================================================================                                   
#                    agent_mutation_elec.check_tech_potential_limits_wind(agents.filter_tech('wind').dataframe, tech_potential_limits_wind_df, model_settings.out_dir, is_first_year)
#                    agent_mutation_elec.check_tech_potential_limits_solar(agents.filter_tech('solar').dataframe, tech_potential_limits_solar_df, model_settings.out_dir, is_first_year)
#                                
                    #==============================================================================
                    # TECHNOLOGY COSTS
                    #==============================================================================
                    # get technology costs
                    tech_costs_solar_df = agent_mutation_elec.get_technology_costs_solar(con, scenario_settings.schema, year)
                    # apply technology costs     
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_tech_costs_solar_storage, (tech_costs_solar_df, )).compute()
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_tech_costs_storage, (tech_cost_storage_schedules_df, year, batt_replacement_yr, 'low')).compute()
     
                    #==========================================================================================================
                    # DEPRECIATION SCHEDULE       
                    #==========================================================================================================
                    # get depreciation schedule for current year
                    depreciation_df = agent_mutation_elec.get_depreciation_schedule(con, scenario_settings.schema, year)
                    # apply depreciation schedule to agents
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_depreciation_schedule_index, (depreciation_df, )).compute()
                    

                    #==========================================================================================================
                    # CARBON INTENSITIES
                    #==========================================================================================================               
                    # get carbon intensities
                    carbon_intensities_df = agent_mutation_elec.get_carbon_intensities(con, scenario_settings.schema, year)
                    # apply carbon intensities
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_carbon_intensities, (carbon_intensities_df, )).compute()                
    
                    #==========================================================================================================
                    # Apply host-owned financial parameters
                    #==========================================================================================================               
                    # Financial assumptions and ITC fraction
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_financial_params, (financial_parameters, itc_options, tech_costs_solar_df)).compute()                
    
                    #==========================================================================================================
                    # Size S+S system and calculate electric bills
                    #==========================================================================================================
                    agents = AgentsAlgorithm(agents, sFuncs.system_size_driver, (depreciation_df, rates_rank_df, rates_json_df, model_settings.local_cores)).compute()                
                  
                   
                    #==============================================================================
                    # Calculate Metric Values
                    #============================================================================== 
                    # Calculate the financial performance of the S+S systems (payback period
                    # for res, time-to-double for C&I)
                    agents = AgentsAlgorithm(agents, financial_functions_elec.calc_metric_value_storage, ()).compute()                            
                   
                    #==============================================================================
                    # Calculate Maximum Market Share
                    #============================================================================== 
                    agents = AgentsAlgorithm(agents, financial_functions_elec.calc_max_market_share, (max_market_share, )).compute()                            

                      
                    #==============================================================================
                    # DEVELOPABLE CUSTOMERS/LOAD
                    #==============================================================================                            
                    # determine "developable" population
                    agents = AgentsAlgorithm(agents, agent_mutation_elec.calculate_developable_customers_and_load_storage).compute()                            
                                                       
                    
                    #==========================================================================================================
                    # MARKET LAST YEAR
                    #==========================================================================================================                  
                    if is_first_year == True:
                        # calculate initial market shares
                        agents = AgentsAlgorithm(agents, agent_mutation_elec.estimate_initial_market_shares_storage, (state_starting_capacities_df, )).compute()
                    else:
                        # get last year's results
#                        market_last_year_df = agent_mutation_elec.get_market_last_year(con, scenario_settings.schema)
                        # apply last year's results to the agents
                        agents = AgentsAlgorithm(agents, agent_mutation_elec.apply_market_last_year, (market_last_year_df, )).compute()                
                                        
                    #==========================================================================================================
                    # BASS DIFFUSION
                    #==========================================================================================================                      
                    # calculate diffusion based on economics and bass diffusion                   
                    agents.dataframe, market_last_year_df = diffusion_functions_elec.calc_diffusion_storage(agents.dataframe, is_first_year, bass_params) 

                    #==========================================================================================================
                    # Write storage dispatch trajectories, to be used for capacity factor aggregations later
                    #==========================================================================================================   
                    # TODO: rewrite this using agents class, once above is handled
                    # Dispatch trajectories are in MW
                    total_dispatches = np.vstack(agents.dataframe['batt_dispatch_profile']).astype(np.float) * np.array(agents.dataframe['new_adopters']).reshape(len(agents.dataframe), 1) / 1000.0
                    total_dispatches_df = pd.DataFrame(total_dispatches, columns = hour_list)
                    total_dispatches_df['ba'] = agents.dataframe['ba'] #TODO improve this so it is robust against reorder

                    total_ba_dispatches_df = total_dispatches_df.groupby(by='ba').sum()
                    total_ba_dispatches_df['year'] = year
                    
                    total_ba_dispatches_df.index.names = ['ba']
                    total_ba_dispatches_df['ba'] = total_ba_dispatches_df.index.values
                    total_ba_dispatches_df.to_pickle(out_scen_path + '/total_ba_dispatches_df_%s.pkl' % year)
                    

                    #==========================================================================================================
                    # Write PV generation profiles, to be used for capacity factor aggregations later
                    #==========================================================================================================   
                    # TODO: rewrite this using agents class, once above is handled
                    if is_first_year:
                        pv_gen_new_adopters = np.vstack(agents.dataframe['solar_cf_profile']).astype(np.float) / 1e6 * np.array(agents.dataframe['pv_kw_cum']).reshape(len(agents.dataframe), 1)
                    else:
                        pv_gen_new_adopters = np.vstack(agents.dataframe['solar_cf_profile']).astype(np.float) / 1e6 * np.array(agents.dataframe['new_pv_kw']).reshape(len(agents.dataframe), 1)

                    pv_gen_new_adopters = pd.DataFrame(pv_gen_new_adopters, columns = hour_list)
                    pv_gen_new_adopters['ba'] = agents.dataframe['ba'] #TODO improve this so it is robust against reorder
                    pv_gen_new_adopters = pv_gen_new_adopters.groupby(by='ba').sum()
                    pv_gen_new_adopters['year'] = year
                    
                    pv_gen_new_adopters.index.names = ['ba']
                    pv_gen_new_adopters['ba'] = pv_gen_new_adopters.index.values
                    pv_gen_new_adopters.to_pickle(out_scen_path + '/pv_gen_new_adopters_%s.pkl' % year)
                    
                    #==========================================================================================================
                    # Aggregate PV and Batt capacity by reeds region
                    #==========================================================================================================   
                    # TODO: rewrite this using agents class, once above is handled
                    agent_cum_capacities = agents.dataframe[[ 'ba', 'pv_kw_cum']]
                    ba_cum_pv_kw_year = agent_cum_capacities.groupby(by='ba').sum()
                    ba_cum_pv_kw_year['ba'] = ba_cum_pv_kw_year.index
                    ba_cum_pv_mw[year] = ba_cum_pv_kw_year['pv_kw_cum'] / 1000.0
                    ba_cum_pv_mw.round(3).to_csv(out_scen_path + '/dpv_MW_by_ba_and_year.csv', index_label='ba')                     
                    
                    agent_cum_batt_mw = agents.dataframe[[ 'ba', 'batt_kw_cum']]
                    agent_cum_batt_mw['batt_mw_cum'] = agent_cum_batt_mw['batt_kw_cum'] / 1000.0
                    agent_cum_batt_mwh = agents.dataframe[[ 'ba', 'batt_kwh_cum']]
                    agent_cum_batt_mwh['batt_mwh_cum'] = agent_cum_batt_mwh['batt_kwh_cum'] / 1000.0

                    ba_cum_batt_mw_year = agent_cum_batt_mw.groupby(by='ba').sum()
                    ba_cum_batt_mwh_year = agent_cum_batt_mwh.groupby(by='ba').sum()
                    
                    ba_cum_batt_mw[year] = ba_cum_batt_mw_year['batt_mw_cum']
                    ba_cum_batt_mw.round(3).to_csv(out_scen_path + '/batt_MW_by_ba_and_year.csv', index_label='ba')                     
                    
                    ba_cum_batt_mwh[year] = ba_cum_batt_mwh_year['batt_mwh_cum']
                    ba_cum_batt_mwh.round(3).to_csv(out_scen_path + '/batt_MWh_by_ba_and_year.csv', index_label='ba') 

                    #==========================================================================================================
                    # WRITE OUTPUTS
                    #==========================================================================================================   
                    # TODO: rewrite this section to use agents class
                    # write the incremental results to the database
                    datfunc.write_outputs(con, cur, agents.dataframe, scenario_settings.sectors, scenario_settings.schema) 
#                    datfunc.write_last_year(con, cur, market_last_year_df, scenario_settings.schema)
                
                    #==========================================================================================================
                    # WRITE OUTPUTS AS PICKLES FOR POST-PROCESSING
                    #========================================================================================================== 
                    agents.dataframe.drop(['consumption_hourly', 'solar_cf_profile'], axis=1).to_pickle(out_scen_path + '/agent_df_%s.pkl' % year)

                #==============================================================================
                # Summarize solar+storage results for ReEDS
                #==============================================================================
#                ts_map_tidy = pd.read_csv('timeslice_map_tidy.csv')
#                ts_list = ['H1', 'H2', 'H3', 'H4', 'H5', 'H6', 'H7', 'H8', 'H9', 'H10', 'H11', 'H12', 'H13', 'H14', 'H15', 'H16', 'H17']
#                ts_dispatch_all_years = pd.DataFrame()
                
                # Dispatch trajectories are in MW
                year = scenario_settings.model_years[0]
                dispatch_new_adopters = pd.read_pickle(out_scen_path + '/total_ba_dispatches_df_%s.pkl' % year)
                dispatch_previous_adopters = dispatch_new_adopters.copy()
                
                dispatch_by_ba_and_year = dispatch_by_ba_and_year.append(dispatch_new_adopters)

                # aggregate into timeslices for reeds
#                dispatch_year_tidy = dispatch_previous_adopters.copy()
#                dispatch_year_tidy = dispatch_year_tidy.transpose()
#                dispatch_year_tidy['hour'] = [int(numeric_string) for numeric_string in dispatch_year_tidy.index.values]
#                dispatch_year_tidy = pd.melt(dispatch_year_tidy, id_vars='hour', value_vars=ba_list, var_name="ba", value_name="dispatch")
#                ts_and_dispatch_tidy = pd.merge(ts_map_tidy, dispatch_year_tidy, how='left', on=['hour', 'ba'])
#                ts_and_dispatch_tidy_ts = ts_and_dispatch_tidy[['ba', 'dispatch', 'ts']].groupby(['ba', 'ts']).mean().reset_index()                
#                ts_dispatch_wide = ts_and_dispatch_tidy_ts.pivot(index='ba', columns='ts', values='dispatch')
#                ts_dispatch_wide['year'] = year
#                ts_dispatch_wide['ba'] = ts_dispatch_wide.index.values
#                ts_dispatch_all_years = pd.concat([ts_dispatch_all_years, ts_dispatch_wide], ignore_index=True)  

                # degrade systems one year                
                dispatch_previous_adopters[hour_list] = dispatch_new_adopters[hour_list] * 0.982

                for year in scenario_settings.model_years[1:]:
                    dispatch_new_adopters = pd.read_pickle(out_scen_path + '/total_ba_dispatches_df_%s.pkl' % year)
                    os.remove(out_scen_path + '/total_ba_dispatches_df_%s.pkl' % year)
                    dispatch_previous_adopters[hour_list] = dispatch_new_adopters[hour_list] + dispatch_previous_adopters[hour_list]
                    dispatch_previous_adopters['year'] = year
                    dispatch_by_ba_and_year = dispatch_by_ba_and_year.append(dispatch_previous_adopters)
                    dispatch_previous_adopters[hour_list] = dispatch_previous_adopters[hour_list] * 0.982
                    
                dispatch_by_ba_and_year = dispatch_by_ba_and_year[['ba', 'year'] + hour_list]
                dispatch_by_ba_and_year.round(3).to_csv(out_scen_path + '/dispatch_by_ba_and_year_MW.csv') 
#                ts_dispatch_all_years[['ba', 'year']+ts_list].round(3).to_csv(out_scen_path + '/dispatch_by_ba_and_year_MW_ts.csv', index=False)


                # PV capacity factors
                pv_gen_by_ba_and_year = pd.DataFrame(index=ba_list, columns=[['ba', 'year'] + hour_list])
                pv_cf_by_ba_and_year = pd.DataFrame(columns = storage_dispatch_df_col_list)

                year = scenario_settings.model_years[0]
                pv_gen_new_adopters = pd.read_pickle(out_scen_path + '/pv_gen_new_adopters_%s.pkl' % year)
                pv_gen_previous_adopters = pv_gen_new_adopters.copy()
                pv_gen_previous_adopters[hour_list] = pv_gen_new_adopters[hour_list] * 0.995
                
                pv_cf_by_ba_and_year_year = pv_gen_new_adopters[hour_list].divide(ba_cum_pv_mw[year]*1000.0, 'index')
                pv_cf_by_ba_and_year_year['year'] = year
                pv_cf_by_ba_and_year = pv_cf_by_ba_and_year.append(pv_cf_by_ba_and_year_year)

                for year in scenario_settings.model_years[1:]:
                    # Import than delete this year's generation profiles
                    pv_gen_new_adopters = pd.read_pickle(out_scen_path + '/pv_gen_new_adopters_%s.pkl' % year)
                    os.remove(out_scen_path + '/pv_gen_new_adopters_%s.pkl' % year)
                    
                    # Total generation is old+new, where degradation was already applied to old capacity
                    pv_gen_previous_adopters[hour_list] = pv_gen_new_adopters[hour_list] + pv_gen_previous_adopters[hour_list]
                    
                    # Convert generation into capacity factor by diving by total capacity
                    pv_cf_by_ba_and_year_year = pv_gen_previous_adopters[hour_list].divide(ba_cum_pv_mw[year]*1000.0, 'index')
                    pv_cf_by_ba_and_year_year['year'] = year
                    pv_cf_by_ba_and_year = pv_cf_by_ba_and_year.append(pv_cf_by_ba_and_year_year)
                    pv_gen_previous_adopters[hour_list] = pv_gen_previous_adopters[hour_list] * 0.995
                    
                pv_cf_by_ba_and_year = pv_cf_by_ba_and_year[['ba', 'year'] + hour_list]
                pv_cf_by_ba_and_year.round(3).to_csv(out_scen_path + '/dpv_cf_by_ba_and_year.csv') 


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
            # these functions have been superceded by midstream processing
#            reedsfunc.write_reeds_offline_mode_data(scenario_settings.schema, con, scenario_settings.techs, out_scen_path)
            
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
        if 'con' in locals():
            con.close()
        if 'scenario_settings' in locals() and scenario_settings.schema is not None and model_settings.mode == 'setup_develop':
            # drop the output schema
            datfunc.drop_output_schema(model_settings.pg_conn_string, scenario_settings.schema, True)
        if 'logger' in locals():
            utilfunc.shutdown_log(logger)
            utilfunc.code_profiler(model_settings.out_dir)

if __name__ == '__main__':
    main()