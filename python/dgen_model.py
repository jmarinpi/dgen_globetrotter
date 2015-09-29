"""
Distributed Wind Diffusion Model
National Renewable Energy Lab

@author: bsigrin
"""

# 1. # Initialize Model
import time
import os

# 2. # Import modules and global vars
import pandas as pd
import psycopg2.extras as pgx
import numpy as np
import glob
import diffusion_functions as diffunc
import financial_functions as finfunc
reload(finfunc)
import data_functions as datfunc
reload(datfunc)
from data_objects import FancyDataFrame
import subprocess
import config as cfg
import shutil
import sys
import pssc_mp
import pickle
from excel import excel_functions
import tech_choice
import reeds_functions as reedsfunc
import utility_functions as utilfunc

#==============================================================================
# raise  numpy and pandas warnings as exceptions
#==============================================================================
np.seterr(all='raise')
pd.set_option('mode.chained_assignment', 'raise')
#==============================================================================
    

def main(mode = None, resume_year = None, endyear = None, ReEDS_inputs = None):

    try:

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
                market_last_year_res = None
                market_last_year_ind = None
                market_last_year_com = None
                # Read in ReEDS UPV Capital Costs
                Convert2004_dollars = 1.254 #Conversion from 2004$ to 2014$
                ReEDS_PV_CC = FancyDataFrame(data = ReEDS_df['UPVCC_all'])
                ReEDS_PV_CC.columns = ['year','Capital_Cost']
                ReEDS_PV_CC.year = ReEDS_PV_CC.year.convert_objects(convert_numeric=True)
                valid_years = np.arange(2014,2051,2)
                ReEDS_PV_CC = ReEDS_PV_CC.loc[ReEDS_PV_CC.year.isin(valid_years)]
                ReEDS_PV_CC.index = range(0, ReEDS_PV_CC.shape[0])
                ReEDS_PV_CC['Capital_Cost'] = ReEDS_PV_CC['Capital_Cost'] * Convert2004_dollars # ReEDS capital costs for UPV converted from 2004 dollars
            else:                
                cfg.init_model = False
                # Load files here
                market_last_year_res = pd.read_pickle("market_last_year_res.pkl")
                market_last_year_ind = pd.read_pickle("market_last_year_ind.pkl")   
                market_last_year_com = pd.read_pickle("market_last_year_com.pkl")   
                with open('saved_vars.pickle', 'rb') as handle:
                    saved_vars = pickle.load(handle)
                out_dir = saved_vars['out_dir']
                input_scenarios = saved_vars['input_scenarios']
            #cfg.init_model,out_dir,input_scenarios, market_last_year = reedsfunc.load_resume_vars(cfg, resume_year)
        else:
            # set input dataframes for reeds-mode settings (these are ingested to postgres later)
            reeds_mode_df = FancyDataFrame(data = [False])
            ReEDS_PV_CC = FancyDataFrame(columns = ['year', 'Capital_Cost'])
            cdate = time.strftime('%Y%m%d_%H%M%S')    
            out_dir = '%s/runs/results_%s' %(os.path.dirname(os.getcwd()), cdate)        
            os.makedirs(out_dir)

                            
        # check that number of customer bins is in the acceptable range
        if type(cfg.customer_bins) <> int:
            raise ValueError("""Error: customer_bins in config.py must be of type integer.""") 
        if cfg.customer_bins <= 0:
            raise ValueError("""Error: customer_bins in config.py must be a positive integer.""") 
        model_init = time.time()
        
        logger = utilfunc.get_logger(os.path.join(out_dir,'dg_model.log'))
        logger.info('Initiating model (%s)' % time.ctime())
            
        # 4. Connect to Postgres and configure connection(s) (to edit login information, edit config.py)
        # create a single connection to Postgres Database -- this will serve as the main cursor/connection
        con, cur = utilfunc.make_con(cfg.pg_conn_string)
        pgx.register_hstore(con) # register access to hstore in postgres    
        
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
#                schema = datfunc.create_output_schema(cfg.pg_conn_string, source_schema = 'diffusion_template') # TODO: Comment
                schema = 'diffusion_results_2015_09_29_15h30m17s' # TODO: COMMENT/DELETE
                datfunc.clear_outputs(con, cur, schema)
                # write the reeds settings to postgres
                reeds_mode_df.to_postgres(con, cur, schema, 'input_reeds_mode')
                ReEDS_PV_CC.to_postgres(con, cur, schema, 'input_reeds_capital_costs')  
                
                try:
#                    excel_functions.load_scenario(input_scenario, schema, con, test = False) # TODO: Comment
                    pass
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
            
            # reeds stuff..
            if mode == 'ReEDS' and scenario_opts['region'] != 'United States':
                logger.error('Linked model can only run nationally. Select United States in input sheet'      )
                logger.error('Model aborted')
                sys.exit(-1)
                                  
            # get other scenario inputs
            logger.info('Getting various scenario parameters')
            t0 = time.time()
            #  these are all technology agnostic user-inputs
            with utilfunc.Timer() as t:
                max_market_share = datfunc.get_max_market_share(con, schema)
                market_projections = datfunc.get_market_projections(con, schema)
                load_growth_scenario = scenario_opts['load_growth_scenario'].lower() # get financial variables
                # these are technology specific, set up in tidy form with a "tech" field
                financial_parameters = datfunc.get_financial_parameters(con, schema)
                incentive_options = datfunc.get_manual_incentive_options(con, schema)
                deprec_schedule = datfunc.get_depreciation_schedule(con, schema, macrs = True)
                ann_system_degradation = datfunc.get_system_degradation(con, schema)      
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

                #==========================================================================================================
                # CREATE AGENTS
                #==========================================================================================================
                logger.info("--------------Creating Agents---------------")
#                datfunc.generate_customer_bins(cur, con, techs, schema, cfg.customer_bins, sectors, cfg.start_year, 
#                                               end_year, cfg.npar, cfg.pg_conn_string, scenario_opts) # TODO: Comment
    
                #==========================================================================================================
                # CHECK TECH POTENTIAL
                #==========================================================================================================           
#                datfunc.check_rooftop_tech_potential_limits(cur, con, schema, techs, sectors, out_dir) # TODO: Comment               
               
               
                #==========================================================================================================
                # CALCULATE BILL SAVINGS
                #==========================================================================================================
#                datfunc.calc_utility_bills(cur, con, schema, sectors, techs, cfg.npar,  # TODO: Comment/uncomment
#                                           cfg.pg_conn_string, cfg.gross_fit_mode, cfg.local_cores)

    
            #==========================================================================================================
            # MODEL TECHNOLOGY DEPLOYMENT    
            #==========================================================================================================
            logger.info("---------Modeling Annual Deployment---------")            
            for sector_abbr, sector in sectors.iteritems():  
                logger.info("Modeling Deployment for %s Sector" % sector.title())
                # get dsire incentives for the generated customer bins
                dsire_incentives = datfunc.get_dsire_incentives(cur, con, schema, techs, sector_abbr, cfg.npar, cfg.pg_conn_string)     
                for year in model_years:
                    dfs = []
                    logger.info('\tWorking on %s for %s Sector' % (year, sector))
                        
                    # get input agent attributes from postgres
                    df = datfunc.get_main_dataframe(con, sector_abbr, schema, year, techs)
                    # reeds stuff...
                    if mode == 'ReEDS':
                        # When in ReEDS mode add the values from ReEDS to df
                        df = pd.merge(df, distPVCurtailment, how = 'left', on = 'pca_reg')
                        df['curtailment_rate'] = df['curtailment_rate'].fillna(0.)
                        df = pd.merge(df,change_elec_price, how = 'left', on = 'pca_reg')
                        
                        if sector_abbr == 'res':
                            market_last_year = market_last_year_res
                        if sector_abbr == 'ind':
                            market_last_year = market_last_year_ind
                        if sector_abbr == 'com':
                            market_last_year = market_last_year_com
                            
                    else:
                        # When not in ReEDS mode set default (and non-impacting) values for the ReEDS parameters
                        df['curtailment_rate'] = 0
                        df['ReEDS_elec_price_mult'] = 1
                        curtailment_method = 'net'                
                    
                    # Market characteristics from previous year
                    is_first_year = year == cfg.start_year
                    previous_year_results = datfunc.get_market_last_year(cur, con, is_first_year, techs, sector_abbr, sector, schema)   
                    df = pd.merge(df, previous_year_results, how = 'left', on = ['county_id', 'bin_id', 'tech'])
                                        
                    # Calculate economics of adoption for different busines models
                    with utilfunc.Timer() as t:
                        df = finfunc.calc_economics(df, schema, sector, sector_abbr, 
                                                   market_projections, financial_parameters, 
                                                   scenario_opts, incentive_options, max_market_share, cur, con, year, 
                                                   dsire_incentives, deprec_schedule, 
                                                   ann_system_degradation, mode, curtailment_method, tech_lifetime = 25)
                    logger.info('\tCompleted in: %0.1fs' % t.interval)

                    
                    # select from choices for business model and (optionally) technology
                    df = tech_choice.select_financing_and_tech(df, prng, cfg.alpha_lkup, choose_tech, techs)  
                    
                    # calculate diffusion based on economics and bass diffusion      
                    df, market_last_year_combined = diffunc.calc_diffusion(df, year, sector)

                    # save outputs from this year and update parameters for next solve  
                    for tech in techs: #TODO: Remove this loop
                        # filter results to only the current technology
                        df_tech = df[df['tech'] == tech]
                        market_last_year = market_last_year_combined[market_last_year_combined.tech == tech]
                        
                        # reeds stuff...
                        if mode == 'ReEDS':
                            if sector_abbr == 'res':
                                market_last_year_res = market_last_year
                            if sector_abbr == 'ind':
                                market_last_year_ind = market_last_year
                            if sector_abbr == 'com':
                                market_last_year_com = market_last_year
                        
                        # write the incremental results to the database
                        datfunc.write_outputs(con, cur, df_tech, sector_abbr, schema) 
                        datfunc.write_last_year(con, cur, market_last_year, sector_abbr, schema, tech)
                         
            #==============================================================================
            #    Outputs & Visualization
            #==============================================================================
            if mode != 'ReEDS' or resume_year == endyear:
                "---------Saving Model Results---------"
                out_subfolders = datfunc.create_tech_subfolders(out_scen_path, techs, out_subfolders)
                        
                # copy outputs to csv     
                datfunc.copy_outputs_to_csv(techs, schema, out_scen_path, sectors, cur, con)

                # create output html report                
                datfunc.create_scenario_report(techs, schema, scen_name, out_scen_path, cur, con, cfg.Rscript_path, cfg.pg_params_file)

                                
            if mode == 'ReEDS':
                reeds_out = reedsfunc.combine_outputs_reeds(schema, sectors, cur, con)
                cf_by_pca_and_ts = reedsfunc.summarise_solar_resource_by_ts_and_pca_reg(reeds_out, con)
                
                market_last_year_res.to_pickle("market_last_year_res.pkl")
                market_last_year_ind.to_pickle("market_last_year_ind.pkl")
                market_last_year_com.to_pickle("market_last_year_com.pkl")
                saved_vars = {'out_dir' : out_dir, 'input_scenarios' : input_scenarios}
                with open('saved_vars.pickle', 'wb') as handle:
                    pickle.dump(saved_vars, handle)  
                return reeds_out, cf_by_pca_and_ts
            
            # after all techs have been processed:
            #####################################################################
            ### THIS IS TEMPORARY ###
            # drop the new schema
#            datfunc.drop_output_schema(cfg.pg_conn_string, schema) # TODO: Uncomment
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
        if 'logger' in locals():
            logger.error(e.__str__(), exc_info = True)
        else:
            print e
    
    finally:
        if 'logger' in locals():
            utilfunc.shutdown_log(logger)
            utilfunc.code_profiler(out_dir)
    
if __name__ == '__main__':
    main()