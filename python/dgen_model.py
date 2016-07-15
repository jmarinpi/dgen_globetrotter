"""
Distributed Wind Diffusion Model
National Renewable Energy Lab

@author: bsigrin
"""

# before doing anything, check model dependencies
import tests
tests.check_dependencies()

# 1. # Initialize Model
import time
import os

# 2. # Import modules and global vars
import pandas as pd
import psycopg2.extras as pgx
import numpy as np
import glob
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
from data_objects import FancyDataFrame
import subprocess
import config as cfg
import sys
import pickle
from excel import excel_functions
import tech_choice
import reeds_functions as reedsfunc
import utility_functions as utilfunc
from agent import Agent, Agents, AgentsAlgorithm


#==============================================================================
# raise  numpy and pandas warnings as exceptions
#==============================================================================
#np.seterr(all='raise')
pd.set_option('mode.chained_assignment', None)
#==============================================================================
    

def main(mode = None, resume_year = None, endyear = None, ReEDS_inputs = None):

    try:
        model_init = time.time()
        
        if mode == 'ReEDS':
            
            reeds_mode_df = FancyDataFrame(data = [True])
            
            ReEDS_df = ReEDS_inputs['ReEDS_df']
            curtailment_method = ReEDS_inputs['curtailment_method']
            
            distPVCurtailment = ReEDS_df['annual_distPVSurplusMar'] # Fraction of distributed PV curtailed in timeslice (m) and BA (n)
            change_elec_price = ReEDS_df['change_elec_price'] # Relative change in electricity price since 2014 by BA (n)
            
            # Rename columns to match names in SolarDS
            distPVCurtailment.columns = ['pca_reg','curtailment_rate']
            change_elec_price.columns = ['pca_reg','ReEDS_elec_price_mult']
            
            # Remove Mexico BAs
            change_elec_price = change_elec_price[change_elec_price.pca_reg != 'p135']
            change_elec_price = change_elec_price[change_elec_price.pca_reg != 'p136']
            
            if resume_year == 2014:
                cfg.init_model = True
                cdate = time.strftime('%Y%m%d_%H%M%S')    
                out_dir = '%s/runs/results_%s' %(os.path.dirname(os.getcwd()), cdate)        
                os.makedirs(out_dir)
                input_scenarios = None
                previous_year_results = None
                # Read in ReEDS UPV Capital Costs
                Convert2004_dollars = 1.254 #Conversion from 2004$ to 2014$
                ReEDS_PV_CC = FancyDataFrame(data = ReEDS_df['UPVCC_all'])
                ReEDS_PV_CC.columns = ['year','Capital_Cost']
                ReEDS_PV_CC.year = ReEDS_PV_CC.year.convert_objects(convert_numeric=True)
                valid_years = np.arange(2014,2051,2)
                ReEDS_PV_CC = FancyDataFrame(data = ReEDS_PV_CC.loc[ReEDS_PV_CC.year.isin(valid_years)])
                ReEDS_PV_CC.index = range(0, ReEDS_PV_CC.shape[0])
                ReEDS_PV_CC['Capital_Cost'] = ReEDS_PV_CC['Capital_Cost'] * Convert2004_dollars # ReEDS capital costs for UPV converted from 2004 dollars
            else:                
                cfg.init_model = False
                # Load files here
                with open('saved_vars.pickle', 'rb') as handle:
                    saved_vars = pickle.load(handle)
                out_dir = saved_vars['out_dir']
                input_scenarios = saved_vars['input_scenarios']
        else:
            # set input dataframes for reeds-mode settings (these are ingested to postgres later)
            reeds_mode_df = FancyDataFrame(data = [False])
            ReEDS_PV_CC = FancyDataFrame(columns = ['year', 'Capital_Cost'])
            cdate = time.strftime('%Y%m%d_%H%M%S')    
            out_dir = '%s/runs/results_%s' %(os.path.dirname(os.getcwd()), cdate)        
            os.makedirs(out_dir)

                            
        # check that number of customer bins is in the acceptable range
        if type(cfg.agents_per_region) <> int:
            raise ValueError("""Error: agents_per_region in config.py must be of type integer.""") 
        if cfg.agents_per_region <= 0:
            raise ValueError("""Error: agents_per_region in config.py must be a positive integer.""") 
        
        
        # create the logger
        logger = utilfunc.get_logger(os.path.join(out_dir,'dg_model.log'))
            
        # 4. Connect to Postgres and configure connection(s) (to edit login information, edit config.py)
        # create a single connection to Postgres Database -- this will serve as the main cursor/connection
        con, cur = utilfunc.make_con(cfg.pg_conn_string)
        logger.info("Connected to Postgres with the following params:\n%s" % cfg.pg_params_log)
        pgx.register_hstore(con) # register access to hstore in postgres    
        
        # get the git hash and also log to output file
        git_hash = utilfunc.get_git_hash()
        logger.info("Model version is git commit %s" % git_hash)
        
        # find the input excel spreadsheets
        if cfg.init_model:    
            input_scenarios = [s for s in glob.glob("../input_scenarios/*.xls*") if not '~$' in s]
            if len(input_scenarios) == 0:
                raise ValueError("No input scenario spreadsheet were found in the input_scenarios folder.")
        elif mode != 'ReEDS':
            input_scenarios = ['']
            
     
        #==========================================================================================================
        # PREP DATABASE
        #==========================================================================================================
        scenario_names = []
        dup_n = 1
        out_subfolders = {'wind' : [], 'solar' : []}
        for i, input_scenario in enumerate(input_scenarios):
            logger.info('============================================') 
            logger.info('============================================') 
            logger.info("Running Scenario %s of %s" % (i+1, len(input_scenarios)))
            logger.info("-------------Preparing Database-------------")
            # load Input excel spreadsheet to Postgres
            if cfg.init_model:
                # create the output schema
                if cfg.use_existing_schema == True:
                    # create a schema from the existing schema of interest
                    schema = datfunc.create_output_schema(cfg.pg_conn_string, source_schema = cfg.existing_schema_name, include_data = True)
                else:
                    # create an empty schema from diffusion_template
                    schema = datfunc.create_output_schema(cfg.pg_conn_string, source_schema = 'diffusion_template', include_data = False)                    
                # clear output results either way (this ensures that outputs are empty for each model run)
                datfunc.clear_outputs(con, cur, schema)
                # write the reeds settings to postgres
                reeds_mode_df.to_postgres(con, cur, schema, 'input_reeds_mode')
                ReEDS_PV_CC.to_postgres(con, cur, schema, 'input_reeds_capital_costs')  
                
                try:
                    excel_functions.load_scenario(input_scenario, schema, con)
                except Exception, e:
                    raise Exception('\tLoading failed with the following error: %s\nModel Aborted' % e      )

            else:
                logger.warning("Warning: Skipping Import of Input Scenario Worksheet. This should only be done in resume mode.")

            # read in high level scenario settings
            scenario_opts = datfunc.get_scenario_options(cur, schema) 
            scen_name = scenario_opts['scenario_name'] 
            sectors = datfunc.get_sectors(cur, schema)
            techs = datfunc.get_technologies(con, schema)
            end_year = scenario_opts['end_year']
            choose_tech = scenario_opts['tech_choice']

            # raise error if trying to run geo technologies with either wind or solar
            if set(['wind','solar', 'storage']).isdisjoint(set(techs)) == False and set(['du','ghp']).isdisjoint(set(techs)) == False:
                raise Exception("Cannot run model with geothermal technologies and other technologies at this time.")
            else:
                # determine technology mode (electricity or geo)
                if set(['wind','solar', 'storage']).isdisjoint(set(techs)) == False:
                    tech_mode = 'elec'
                elif set(['du','ghp']).isdisjoint(set(techs)) == False:
                    tech_mode = 'geo'
                else:
                    raise Exception("No technologies selected to be analyzed")
            
            # set modules based on the tech mode
            agent_prep = cfg.module_lkup['agent_preparation'][tech_mode]
            mutation = cfg.module_lkup['agent_mutation'][tech_mode]
            finfunc = cfg.module_lkup['financial_functions'][tech_mode]
            diffunc = cfg.module_lkup['diffusion_functions'][tech_mode]
            demand_supply = cfg.module_lkup['demand_supply'][tech_mode]
            
            # skip industrial sector if modeling geothermal technologies
            if 'ind' in sectors.keys() and tech_mode == 'geo':
                sectors.pop('ind')
                msg = 'Industrial sector cannot be modeled for geothermal technologies at this time.'
                logger.warning(msg)

            # if in tech choice mode, check that multiple techs are available
            if choose_tech == True and len(techs) == 1:
                raise Exception("Cannot run Tech Choice Mode with only one technology")
            
            # if tech_mode is geo, cannot run choose tech
            if choose_tech == True and tech_mode == 'geo':
                raise Exception("Cannot run Tech Choice Mode with geothermal technologies at this time")
            
            # summarize high level secenario settings 
            logger.info('Scenario Settings:')
            logger.info('\tScenario Name: %s' % scen_name)
            logger.info('\tRegion: %s' % scenario_opts['region'])
            logger.info('\tSectors: %s' % sectors.values())
            logger.info('\tTechnologies: %s' % techs)
            logger.info('\tYears: %s - %s' % (cfg.start_year, end_year))
            logger.info('\tTech Choice Mode Enabled: %s' % choose_tech)
            
            # reeds stuff.. #TODO: Refactor
            if mode == 'ReEDS' and scenario_opts['region'] != 'United States':
                raise Exception('Linked model can only run nationally. Select United States in input sheet')
            
            if mode == 'ReEDS' and techs != ['solar']:
                raise Exception('Linked model can only run for solar only. Set Run Model for Wind = False in input sheet'      )

                                  
            # get other scenario inputs
            logger.info('Getting various scenario parameters')
            with utilfunc.Timer() as t:
                max_market_share = datfunc.get_max_market_share(con, schema)
                market_projections = datfunc.get_market_projections(con, schema)
                load_growth_scenario = scenario_opts['load_growth_scenario'].lower() # get financial variables
                # these are technology specific, set up in tidy form with a "tech" field
                financial_parameters = datfunc.get_financial_parameters(con, schema)                
                inflation_rate = datfunc.get_annual_inflation(con,schema)
                rate_growth_df = datfunc.get_rate_escalations(con, schema)
                bass_params = datfunc.get_bass_params(con, schema)
                learning_curves_mode = datfunc.get_learning_curves_mode(con, schema)
                # Only need this for learning curves (which are currently not functioning)
                #datfunc.write_first_year_costs(con, cur, schema, cfg.start_year)
            logger.info('\tCompleted in: %0.1fs' % t.interval)

            # set model years depending on whether in reeds mode
            # reeds stuff...
            if mode == 'ReEDS':
                model_years = [resume_year]
            else:
                model_years = range(cfg.start_year, end_year+1,2)

            if mode != 'ReEDS' or resume_year == 2014:      
                # create output folder for this scenario
                out_scen_path, scenario_names, dup_n = datfunc.create_scenario_results_folder(input_scenario, scen_name, scenario_names, out_dir, dup_n)

                # create psuedo-rangom number generator (not used until tech/finance choice function)
                prng = np.random.RandomState(scenario_opts['random_generator_seed'])

                if cfg.use_existing_schema == False:
                    #==========================================================================================================
                    # CREATE AGENTS
                    #==========================================================================================================
                    logger.info("--------------Creating Agents---------------")                        
                    agent_prep.generate_core_agent_attributes(cur, con, techs, schema, cfg.sample_pct, cfg.min_agents, cfg.agents_per_region,
                                                              sectors, cfg.pg_procs, cfg.pg_conn_string, scenario_opts['random_generator_seed'], end_year)
                    
                    if tech_mode == 'elec':                    
                        #==============================================================================
                        # GET RATE TARIFF LOOKUP TABLE FOR EACH SECTOR                                    
                        #==============================================================================
                        rates_df = mutation.get_electric_rates(cur, con, schema, sectors, scenario_opts['random_generator_seed'], cfg.pg_conn_string)
    
                        #==============================================================================
                        # GET NORMALIZED LOAD PROFILES
                        #==============================================================================
                        normalized_load_profiles_df = mutation.get_normalized_load_profiles(con, schema, sectors)
    
                        # get system sizing targets
                        system_sizing_targets_df = mutation.get_system_sizing_targets(con, schema)  
    
                        # get annual system degradation
                        system_degradation_df = mutation.get_system_degradation(con, schema) 
                        
                        # get state starting capacities
                        # TODO: add this capability for du and ghp
                        state_starting_capacities_df = mutation.get_state_starting_capacities(con, schema)
                    
                        #==========================================================================================================
                        # CHECK TECH POTENTIAL
                        #==========================================================================================================    
                        # TODO: get tech potential check working again
                        #datfunc.check_tech_potential_limits(cur, con, schema, techs, sectors, out_dir)              
                    

                    if 'du' in techs:
                        #==========================================================================================================
                        # CALCULATE TRACT AGGREGATE THERMAL LOAD PROFILE
                        #==========================================================================================================                                    
                        # calculate tract demand profiles
                        demand_supply.calculate_tract_demand_profiles(con, cur, schema, cfg.pg_procs, cfg.pg_conn_string)
             
                        #==========================================================================================================
                        # GET TRACT DISTRIBUTION NEWORK SIZES 
                        #==========================================================================================================                        
                        distribution_df = demand_supply.get_distribution_network_data(con, schema)

                        #==========================================================================================================
                        # SETUP RESOURCE DATA
                        #==========================================================================================================
                        demand_supply.setup_resource_data(cur, con, schema, scenario_opts['random_generator_seed'], cfg.pg_procs, cfg.pg_conn_string)
                        
                        #==========================================================================================================
                        # GET BASS DIFFUSION PARAMETERS
                        #==========================================================================================================
                        bass_params_df = diffunc.get_bass_params(con, schema)

    
            #==========================================================================================================
            # MODEL TECHNOLOGY DEPLOYMENT    
            #==========================================================================================================
            logger.info("---------Modeling Annual Deployment---------")      
            if tech_mode == 'elec':    
                # get dsire incentives, srecs, and itc inputs
                # TODO: move these to agent mutation
                dsire_opts = datfunc.get_dsire_settings(con, schema)
                incentives_cap = datfunc.get_incentives_cap(con, schema)
                dsire_incentives = datfunc.get_dsire_incentives(cur, con, schema, techs, sectors, cfg.pg_conn_string, dsire_opts)
                srecs = datfunc.get_srecs(cur, con, schema, techs, cfg.pg_conn_string, dsire_opts)
                state_dsire = datfunc.get_state_dsire_incentives(cur, con, schema, techs, dsire_opts)            
                itc_options = datfunc.get_itc_incentives(con, schema)
                for year in model_years:
                    logger.info('\tWorking on %s' % year)
                        
                    # get core agent attributes from postgres
                    agents = mutation.get_core_agent_attributes(con, schema)
      
                    #==============================================================================
                    # LOAD/POPULATION GROWTH               
                    #==============================================================================
                    # get load growth
                    load_growth_df = mutation.get_load_growth(con, schema, year)
                    # apply load growth
                    agents = AgentsAlgorithm(agents, mutation.apply_load_growth, (load_growth_df,)).compute(1)              
                    
                    #==============================================================================
                    # RATES         
                    #==============================================================================                
                    # get net metering settings
                    net_metering_df = mutation.get_net_metering_settings(con, schema, year)
                    # select rates, combining with net metering settings
                    agents = AgentsAlgorithm(agents, mutation.select_electric_rates, (rates_df, net_metering_df)).compute(1)
                    
                    #==============================================================================
                    # ANNUAL RESOURCE DATA
                    #==============================================================================       
                    # get annual resource data
                    resource_solar_df = mutation.get_annual_resource_solar(con, schema, sectors)
                    resource_wind_df = mutation.get_annual_resource_wind(con, schema, year, sectors)
                    # get technology performance data
                    tech_performance_solar_df = mutation.get_technology_performance_solar(con, schema, year)
                    tech_performance_wind_df = mutation.get_technology_performance_wind(con, schema, year)
                    # apply technology performance to annual resource data
                    resource_solar_df = mutation.apply_technology_performance_solar(resource_solar_df, tech_performance_solar_df)
                    resource_wind_df = mutation.apply_technology_performance_wind(resource_wind_df, tech_performance_wind_df)     
                                    
                    #==============================================================================
                    # SYSTEM SIZING
                    #==============================================================================
                    # size systems
                    agents_solar = AgentsAlgorithm(agents.filter_tech('solar'), mutation.size_systems_solar, (system_sizing_targets_df, resource_solar_df)).compute()  
                    agents_wind = AgentsAlgorithm(agents.filter_tech('wind'), mutation.size_systems_wind, (system_sizing_targets_df, resource_wind_df)).compute()   
                    # re-combine technologies
                    agents = agents_solar.add_agents(agents_wind)
                    del agents_solar, agents_wind   
                    # update net metering fields after system sizing (because of changes to ur_enable_net_metering)
                    agents = AgentsAlgorithm(agents, mutation.update_net_metering_fields).compute(1)  
                                
                    #==============================================================================
                    # DEVELOPABLE CUSTOMERS/LOAD
                    #==============================================================================                            
                    # determine "developable" population
                    agents = AgentsAlgorithm(agents, mutation.calculate_developable_customers_and_load).compute(1)                            
                                
                    #==============================================================================
                    # GET NORMALIZED LOAD PROFILES
                    #==============================================================================
                    # apply normalized load profiles
                    agents = AgentsAlgorithm(agents, mutation.scale_normalized_load_profiles, (normalized_load_profiles_df, )).compute()
                   
                    #==============================================================================
                    # HOURLY RESOURCE DATA
                    #==============================================================================
                    # get hourly resource
                    normalized_hourly_resource_solar_df = mutation.get_normalized_hourly_resource_solar(con, schema, sectors)
                    normalized_hourly_resource_wind_df = mutation.get_normalized_hourly_resource_wind(con, schema, sectors, cur, agents)
                    # apply normalized hourly resource profiles
                    agents_solar = AgentsAlgorithm(agents.filter_tech('solar'), mutation.apply_normalized_hourly_resource_solar, (normalized_hourly_resource_solar_df, )).compute()
                    agents_wind = AgentsAlgorithm(agents.filter_tech('wind'), mutation.apply_normalized_hourly_resource_wind, (normalized_hourly_resource_wind_df, )).compute()        
                    # re-combine technologies
                    agents = agents_solar.add_agents(agents_wind)
                    del agents_solar, agents_wind               
                    
                    #==============================================================================
                    # TECHNOLOGY COSTS
                    #==============================================================================
                    # get technology costs
                    tech_costs_solar_df = mutation.get_technology_costs_solar(con, schema, year)
                    tech_costs_wind_df = mutation.get_technology_costs_wind(con, schema, year)
                    # apply technology costs     
                    agents_solar = AgentsAlgorithm(agents.filter_tech('solar'), mutation.apply_tech_costs_solar, (tech_costs_solar_df, )).compute()
                    agents_wind = AgentsAlgorithm(agents.filter_tech('wind'), mutation.apply_tech_costs_wind, (tech_costs_wind_df, )).compute()
                    # re-combine technologies
                    agents = agents_solar.add_agents(agents_wind)
                    del agents_solar, agents_wind
     
                    #==========================================================================================================
                    # CALCULATE BILL SAVINGS
                    #==========================================================================================================
                    # bill savings are a function of: 
                     # (1) hacked NEM calculations
                    agents = AgentsAlgorithm(agents, mutation.calculate_excess_generation_and_update_nem_settings).compute()
                     # (2) actual SAM calculations
                    agents = AgentsAlgorithm(agents, mutation.calculate_electric_bills_sam, (cfg.local_cores, )).compute(1)
                    # drop the hourly datasets
                    agents.drop_attributes(['generation_hourly', 'consumption_hourly'], in_place = True)
                    
                    #==========================================================================================================
                    # DEPRECIATION SCHEDULE       
                    #==========================================================================================================
                    # get depreciation schedule for current year
                    depreciation_df = mutation.get_depreciation_schedule(con, schema, year)
                    # apply depreciation schedule to agents
                    agents = AgentsAlgorithm(agents, mutation.apply_depreciation_schedule, (depreciation_df, )).compute()
                    
                    #==========================================================================================================
                    # SYSTEM DEGRADATION                
                    #==========================================================================================================
                    # apply system degradation to agents
                    agents = AgentsAlgorithm(agents, mutation.apply_system_degradation, (system_degradation_df, )).compute()
                    
                    #==========================================================================================================
                    # CARBON INTENSITIES
                    #==========================================================================================================               
                    # get carbon intensities
                    carbon_intensities_df = mutation.get_carbon_intensities(con, schema, year)
                    # apply carbon intensities
                    agents = AgentsAlgorithm(agents, mutation.apply_carbon_intensities, (carbon_intensities_df, )).compute()                
    
                    #==========================================================================================================
                    # LEASING AVAILABILITY
                    #==========================================================================================================               
                    # get leasing availability
                    leasing_availability_df = mutation.get_leasing_availability(con, schema, year)
                    agents = AgentsAlgorithm(agents, mutation.apply_leasing_availability, (leasing_availability_df, )).compute()                     
                    
                    
                    
                    
                    
                    #%%
                    #==========================================================================================================
                    # NEW CODE FOR STORAGE/SIZING ANALYSIS
                    #==========================================================================================================                                  
                    
                    pass
                    
                    #%%                
                    
                    
                    
                    # reeds stuff...
                    # TODO: fix this to get linked reeds mode working
    #                if mode == 'ReEDS':
    #                    # When in ReEDS mode add the values from ReEDS to df
    #                    df = pd.merge(df, distPVCurtailment, how = 'left', on = 'pca_reg') # TODO: probably need to add sector as a merge key
    #                    df['curtailment_rate'] = df['curtailment_rate'].fillna(0.)
    #                    df = pd.merge(df, change_elec_price, how = 'left', on = 'pca_reg') # TODO: probably need to add sector as a merge key
    #                else:
                    # When not in ReEDS mode set default (and non-impacting) values for the ReEDS parameters
                    agents.dataframe['curtailment_rate'] = 0
                    agents.dataframe['ReEDS_elec_price_mult'] = 1
                    curtailment_method = 'net'           
                                            
                    # Calculate economics of adoption for different busines models
                    df = finfunc.calc_economics(agents.dataframe, schema, 
                                               market_projections, financial_parameters, rate_growth_df,
                                               scenario_opts, max_market_share, cur, con, year,
                                               dsire_incentives, dsire_opts, state_dsire, srecs, mode, 
                                               curtailment_method, itc_options, inflation_rate, incentives_cap, 25)
                    
                    
                    # select from choices for business model and (optionally) technology
                    df = tech_choice.select_financing_and_tech(df, prng, cfg.alpha_lkup, sectors, choose_tech, techs)                 
    
                    #==========================================================================================================
                    # MARKET LAST YEAR
                    #==========================================================================================================                  
                    # convert back to agents
                    agents = Agents(df)
                    is_first_year = year == cfg.start_year      
                    if is_first_year == True:
                        # calculate initial market shares
                        agents = AgentsAlgorithm(agents, mutation.estimate_initial_market_shares, (state_starting_capacities_df, )).compute()
                    else:
                        # get last year's results
                        market_last_year_df = mutation.get_market_last_year(con, schema)
                        # apply last year's results to the agents
                        agents = AgentsAlgorithm(agents, mutation.apply_market_last_year, (market_last_year_df, )).compute()                
                    
    
                    #==========================================================================================================
                    # BASS DIFFUSION
                    #==========================================================================================================   
                    # TODO: rewrite this section to use agents class
                    # convert back to dataframe
                    df = agents.dataframe
                    # calculate diffusion based on economics and bass diffusion                   
                    df, market_last_year = diffunc.calc_diffusion(df, cur, con, cfg, techs, choose_tech, sectors, schema, is_first_year, bass_params) 
                    
                    #==========================================================================================================
                    # ESTIMATE TOTAL GENERATION
                    #==========================================================================================================      
                    df = AgentsAlgorithm(Agents(df), mutation.estimate_total_generation).compute().dataframe
                
                    #==========================================================================================================
                    # WRITE OUTPUTS
                    #==========================================================================================================   
                    # TODO: rewrite this section to use agents class
                    # write the incremental results to the database
                    datfunc.write_outputs(con, cur, df, sectors, schema) 
                    datfunc.write_last_year(con, cur, market_last_year, schema)
    
                    # TODO: get this working if we want to have learning curves
    #                datfunc.write_cumulative_deployment(con, cur, df, schema, techs, year, cfg.start_year)
    #                datfunc.write_costs(con, cur, schema, learning_curves_mode, year, end_year)
                    
    
                    # NEXT STEPS
                    # TODO: figure out better way to handle memory with regards to hourly generation and consumption arrays    
                            # clustering of time series into prototypes? (e.g., vector quantization) partioning around medoids
                            # compression/lazy load of arrays ? https://www.wakari.io/sharing/bundle/pjimenezmateo/Numba_and_blz?has_login=False   
                            # out of memory dataframe -- dask? blz?
    
                    # ~~~LONG TERM~~~
                    # TODO: may need to refactor agents algorithm to avoid pickling all agents to all cores
                    # TODO: edit AgentsAlgorithm  -- remove column check during precheck and change postcheck to simply check for the new columns added (MUST be specified by user...)
                    # TODO: Remove RECS/CBECS as option for rooftop characteristics from input sheet and database                
                    # TODO: perform final cleanup of data functions to make sure all legacy/deprecated functions are removed and/or moved(?) to the correct module
            elif tech_mode == 'geo':
                for year in model_years:
                    logger.info('\tWorking on %s' % year)
                        
                    #==============================================================================
                    # BUILD DEMAND CURVES FOR EACH TRACT      
                    #==============================================================================                   
                    # get initial agents from postgres
                    agents_initial = mutation.get_initial_agent_attributes(con, schema)
                    # update year for these initial agetns to the current year
                    agents_initial = AgentsAlgorithm(agents_initial, mutation.update_year, (year, )).compute()
                    # get new construction agents
                    agents_new = mutation.get_new_agent_attributes(con, schema, year)
                    # combine initial and new agents
                    agents = agents_initial.add_agents(agents_new)
                    del agents_initial, agents_new
                    
                    # get regional prices of energy
                    energy_prices_df = mutation.get_regional_energy_prices(con, schema, year)
                    # apply regional heating/cooling prices
                    agents = AgentsAlgorithm(agents, mutation.apply_regional_energy_prices, (energy_prices_df, )).compute()

                    # get du cost data
                    end_user_costs_du_df = mutation.get_end_user_costs_du(con, schema, year)
                    # apply du cost data
                    agents = AgentsAlgorithm(agents, mutation.apply_end_user_costs_du, (end_user_costs_du_df, )).compute()               
                    
                    # update system ages
                    agents = AgentsAlgorithm(agents, mutation.update_system_ages, (year, )).compute()
                    # check whether systems need replacement (outlived their expected lifetime)
                    agents = AgentsAlgorithm(agents, mutation.check_system_expirations).compute()
                    
                    # build demand curves
                    demand_curves_df = demand_supply.build_demand_curves(agents.dataframe) # TODO: replace with actual function

                    #==============================================================================
                    # BUILD SUPPLY CURVES FOR EACH TRACT
                    #==============================================================================
                    # get tract demand profiles
                    tract_demand_profiles_df = demand_supply.get_tract_demand_profiles(con, schema, year)    
                    # calculate tract peak demand
                    tract_peak_demand_df = demand_supply.calculate_tract_peak_demand(tract_demand_profiles_df)                    
                    # calculate distribution demand density
                    demand_density_df = demand_supply.calculate_distribution_demand_density(tract_peak_demand_df, distribution_df)


                    resource_df = demand_supply.get_resource_data(con, schema, year)
                    # get natural gas prics
                    ng_prices_df = demand_supply.get_natural_gas_prices(con, schema, year)
                    # get the du cost data
                    costs_and_performance_df = demand_supply.get_plant_cost_and_performance_data(con, schema, year)
                    reservoir_factors_df = demand_supply.get_reservoir_factors(con, schema, year)
                    # get the plant finance data
                    plant_finances_df = demand_supply.get_plant_finance_data(con, schema, year)
                    plant_construction_factor_df = demand_supply.get_plant_construction_factor_data(con, schema, year)
                    # NOTE: This isn't currently used, instead, we use a fixed value
                    plant_construction_finance_factor = 1.106
                    # TODO: change the input for plant construction factor
                    plant_depreciation_df = demand_supply.get_plant_depreciation_data(con, schema, year)                    
                    # calculate the plant and boiler capacity factors
                    capacity_factors_df = demand_supply.calculate_plant_and_boiler_capacity_factors(tract_peak_demand_df, costs_and_performance_df, tract_demand_profiles_df, year)
                    # apply the plant cost data
                    resources_with_costs_df = demand_supply.apply_cost_and_performance_data(resource_df, costs_and_performance_df, reservoir_factors_df, plant_finances_df, demand_density_df,  capacity_factors_df, ng_prices_df)
                    # calculate lcoe of each resource
                    resources_with_costs_df = demand_supply.calc_lcoe(resources_with_costs_df, plant_depreciation_df, plant_construction_finance_factor)                                                                                     
                    # convert into a supply curve
                    supply_curves_df = demand_supply.lcoe_to_supply_curve(resources_with_costs_df)

                    
                    #==============================================================================
                    # CALCULATE PLANT SIZES BASED ON ECONOMIC POTENTIAL
                    #==============================================================================                    
                    plant_sizes_economic_df = demand_supply.calc_plant_sizes_econ(demand_curves_df, supply_curves_df) # TODO: replace with actual function
                                          
                    #==============================================================================
                    # CALCULATE PLANT SIZES BASED ON MARKET POTENTIAL
                    #==============================================================================                    
                    plant_sizes_market_df = demand_supply.calc_plant_sizes_market(demand_curves_df, supply_curves_df, plant_sizes_economic_df) # TODO: replace with actual function
                    
                    #==============================================================================
                    # BASS DIFFUSION
                    #==============================================================================                    
                    # get previous year market share
                    existing_market_share_df = diffunc.get_existing_market_share(con, cur, schema, year)
                    # calculate total market demand
                    total_market_demand_mw = diffunc.calculate_total_market_demand(tract_peak_demand_df)
                    # calculate current max market share
                    current_mms = diffunc.calculate_current_mms(plant_sizes_market_df, total_market_demand_mw)
                    # calculate new incremental market share pct
                    new_market_share_pct = diffunc.calculate_new_incremental_market_share_pct(existing_market_share_df, current_mms, bass_params_df, year)
                    # calculate new incremental market share capacity (mw)
                    new_incremental_capacity_mw = diffunc.calculate_new_incremental_capacity_mw(new_market_share_pct, total_market_demand_mw)
                    # select plants to be built
                    plants_to_be_built_df = diffunc.select_plants_to_be_built(plant_sizes_market_df, new_incremental_capacity_mw, scenario_opts['random_generator_seed'])
                    # summarize the new cumulative market share (in terms of capacity and pct) based on the selected plants
                    # (note: this will differ a bit from the new_incremental_capacity_mw and new_market_share_pct + existing_market_share_df because it is based on
                    # selected plants, which may not sum perfectly to the theoreticaly incremental additions)
                    cumulative_market_share_df = diffunc.calculate_new_cumulative_market_share(existing_market_share_df, plants_to_be_built_df, total_market_demand_mw, year)                    
                    # write/store summary market share outputs
                    diffunc.write_cumulative_market_share(con, cur, cumulative_market_share_df, schema)
                    # TODO: add capability to track which plants were already built and which customers subscribed 
                    # NOTE: (this could get complicated in terms of next year's demand and supply curves...)
                    
                    
                    
                
            #==============================================================================
            #    Outputs & Visualization
            #==============================================================================
            if mode != 'ReEDS' or resume_year == endyear:
                logger.info("---------Saving Model Results---------")
                out_subfolders = datfunc.create_tech_subfolders(out_scen_path, techs, out_subfolders, choose_tech)
                
                # copy outputs to csv     
                datfunc.copy_outputs_to_csv(techs, schema, out_scen_path, cur, con)

                # add indices to postgres output table
                datfunc.index_output_table(con, cur, schema)
                
                # write reeds mode outputs to csvs in case they're needed
                reedsfunc.write_reeds_offline_mode_data(schema, con, techs, out_scen_path)
                
                # create output html report                
                datfunc.create_scenario_report(techs, schema, scen_name, out_scen_path, cur, con, cfg.Rscript_path, cfg.pg_params_file)
                
                # create tech choice report (if applicable)
                datfunc.create_tech_choice_report(choose_tech, schema, scen_name, out_scen_path, cur, con, cfg.Rscript_path, cfg.pg_params_file)

                                
            if mode == 'ReEDS':
                reeds_out = reedsfunc.combine_outputs_reeds(schema, sectors, cur, con, resume_year)
                cf_by_pca_and_ts = reedsfunc.summarise_solar_resource_by_ts_and_pca_reg(schema, con)
                
                saved_vars = {'out_dir' : out_dir, 'input_scenarios' : input_scenarios}
                with open('saved_vars.pickle', 'wb') as handle:
                    pickle.dump(saved_vars, handle)  
                return reeds_out, cf_by_pca_and_ts
            
            # after all techs have been processed:
            #####################################################################
            # drop the new schema
            datfunc.drop_output_schema(cfg.pg_conn_string, schema, cfg.delete_output_schema)
            #####################################################################
            
            logger.info("-------------Model Run Complete-------------")
            logger.info('Completed in: %.1f seconds' % (time.time() - model_init))
                
        if len(input_scenarios) > 1:
            # assemble report to compare scenarios
            scenario_analysis_path = '%s/r/graphics/scenario_analysis.R' % os.path.dirname(os.getcwd())
            for tech in out_subfolders.keys():
                out_tech_subfolders = out_subfolders[tech]
                if len(out_tech_subfolders) > 0:
                    scenario_output_paths = utilfunc.pylist_2_pglist(out_tech_subfolders).replace("'","").replace(" ","")
                    scenario_comparison_path = os.path.join(out_dir,'scenario_comparison_%s' % tech)
                    command = [cfg.Rscript_path,'--vanilla',scenario_analysis_path,scenario_output_paths,scenario_comparison_path]
                    logger.info('============================================') 
                    logger.info('============================================') 
                    msg = 'Creating scenario comparison report for %s' % tech          
                    logger.info(msg)
                    proc = subprocess.Popen(command,stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                    messages = proc.communicate()
                    if 'error' in messages[1].lower():
                        logger.error(messages[1])
                    if 'warning' in messages[1].lower():
                        logger.warning(messages[1])
                    returncode = proc.returncode

            

    except Exception, e:
        # close the connection (need to do this before dropping schema or query will hang)      
        if con is not None:
            con.close()
        if 'logger' in locals():
            logger.error(e.__str__(), exc_info = True)
        if 'schema' in locals():
            # drop the output schema
            datfunc.drop_output_schema(cfg.pg_conn_string, schema, True)

        if 'logger' not in locals():
            raise
        
    
    finally:
        if 'logger' in locals():
            utilfunc.shutdown_log(logger)
            utilfunc.code_profiler(out_dir)
    
if __name__ == '__main__':
    main()