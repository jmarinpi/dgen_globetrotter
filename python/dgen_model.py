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
reload(datfunc)
import agent_preparation as agent_prep
reload(agent_prep)
import agent_mutation as mutation
reload(mutation)
import diffusion_functions as diffunc
reload(diffunc)
import financial_functions as finfunc
reload(finfunc)
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
#pd.set_option('mode.chained_assignment', 'raise')
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
                    excel_functions.load_scenario(input_scenario, schema, con) # TODO: Comment
#                    pass
                except Exception, e:
                    logger.error('\tLoading failed with the following error: %s\nModel Aborted' % e      )
                    logger.error('Model aborted')
                    sys.exit(-1)
            else:
                logger.warning("Warning: Skipping Import of Input Scenario Worksheet. This should only be done in resume mode.")

            # read in high level scenario settings
            scenario_opts = datfunc.get_scenario_options(cur, schema) 
            scen_name = scenario_opts['scenario_name'] 
            sectors = datfunc.get_sectors(cur, schema)
            techs = datfunc.get_technologies(con, schema)
            end_year = scenario_opts['end_year']
            choose_tech = scenario_opts['tech_choice']
            
            # if in tech choice mode, check that multiple techs are available
            if choose_tech == True and len(techs) == 1:
                logger.error("Cannot run Tech Choice Mode with only one technology")
                logger.error("Model aborted")
                sys.exit(-1)
            
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
                logger.error('Linked model can only run nationally. Select United States in input sheet'      )
                logger.error('Model aborted')
                sys.exit(-1)
            
            if mode == 'ReEDS' and techs != ['solar']:
                logger.error('Linked model can only run for solar only. Set Run Model for Wind = False in input sheet'      )
                logger.error('Model aborted')
                sys.exit(-1)
                                  
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
                datfunc.write_first_year_costs(con, cur, schema, cfg.start_year)
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
                    agent_prep.generate_core_agent_attributes(cur, con, techs, schema, cfg.agents_per_region, sectors,
                                            cfg.pg_procs, cfg.pg_conn_string, scenario_opts['random_generator_seed'])

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
                    #==========================================================================================================
                    # CHECK TECH POTENTIAL
                    #==========================================================================================================           
                    #datfunc.check_tech_potential_limits(cur, con, schema, techs, sectors, out_dir)              
                   

    
            #==========================================================================================================
            # MODEL TECHNOLOGY DEPLOYMENT    
            #==========================================================================================================
            logger.info("---------Modeling Annual Deployment---------")      
            # get dsire incentives, srecs, and itc inputs
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
                # determine "developable" population (applies to solar only)
                agents = AgentsAlgorithm(agents, mutation.calculate_developable_customers_and_load).compute(1)
                
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


                # NEXT STEPS
                # TODO: see if I can get everything downstream working again....
                # TODO: do some very thorough testing in comparison to dev
                # TODO: figure out better way to handle memory with regards to hourly generation and consumption arrays                
                
                agents.dataframe.to_csv('/Users/mgleason/Desktop/agents.csv', index = False)           
           
                # ~~~LONG TERM~~~
                # TODO: edit AgentsAlgorithm  -- remove column check during precheck and change postcheck to simply check for the new columns added (MUST be specified by user...)
                # TODO: perform final cleanup of data functions to make sure all legacy/deprecated functions are removed
                # TODO: get logger working on both data_functions and agent_preparation
                # TODO: strip cap cost multipliers from agent locations and move to somewhere downstream
                # TODO: Remove RECS/CBECS as option for rooftop characteristics from input sheet and database
                

                
                
                # reeds stuff...
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
                crash
                # calculate diffusion based on economics and bass diffusion      
                df, market_last_year = diffunc.calc_diffusion(df, cur, con, cfg, techs, choose_tech, sectors, schema, year, 
                                                              cfg.start_year, cfg.initial_market_calibrate_mode, bass_params) 
 
                # write the incremental results to the database
                datfunc.write_outputs(con, cur, df, sectors, schema) 
                datfunc.write_last_year(con, cur, market_last_year, schema)
                datfunc.write_cumulative_deployment(con, cur, df, schema, techs, year, cfg.start_year)
                datfunc.write_costs(con, cur, schema, learning_curves_mode, year, end_year)
    
            #==============================================================================
            #    Outputs & Visualization
            #==============================================================================
            if mode != 'ReEDS' or resume_year == endyear:
                logger.info("---------Saving Model Results---------")
                out_subfolders = datfunc.create_tech_subfolders(out_scen_path, techs, out_subfolders, choose_tech)
                
                # copy outputs to csv     
                datfunc.combine_outputs(techs, schema, sectors, cur, con)
                datfunc.copy_outputs_to_csv(techs, schema, out_scen_path, cur, con)
                
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
        if con is not None:
            con.close()
        # drop the output schema
        datfunc.drop_output_schema(cfg.pg_conn_string, schema, True)
        # log or raise the exception
        if 'logger' in locals():
            logger.error(e.__str__(), exc_info = True)
        else:
            raise
        
    
    finally:
        if 'logger' in locals():
            utilfunc.shutdown_log(logger)
            utilfunc.code_profiler(out_dir)
    
if __name__ == '__main__':
    main()