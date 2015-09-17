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
pd.set_option('mode.chained_assignment','raise')
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
        logger.info('Initiating model (%s)' %time.ctime())
            
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
            # 5. Load Input excel spreadsheet to Postgres
            if cfg.init_model:
                # create the output schema
                logger.info('Creating output schema')
                t0 = time.time()
#                schema = datfunc.create_output_schema(cfg.pg_conn_string, source_schema = 'diffusion_template') # TODO: Comment
                schema = 'diffusion_results_2015_09_17_11h34m00s' # TODO: COMMENT/DELETE
                datfunc.clear_outputs(con, cur, schema)
                logger.info('\tOutput schema is: %s' % schema)
                logger.info('\tCompleted in: %0.1fs' %(time.time() - t0))
                # write the reeds settings to postgres
                reeds_mode_df.to_postgres(con, cur, schema, 'input_reeds_mode')
                ReEDS_PV_CC.to_postgres(con, cur, schema, 'input_reeds_capital_costs')  
                logger.info('Loading Input Scenario Worksheet')
                try:
                    t0 = time.time()
#                    excel_functions.load_scenario(input_scenario, schema, con, test = False) # TODO: Comment
                    logger.info('\tCompleted in: %0.1fs' %(time.time() - t0))
                except Exception, e:
                    msg = '\tLoading failed with the following error: %s' % e      
                    logger.error(msg)
                    msg = 'Model aborted'
                    logger.error(msg)
                    sys.exit(-1)
            else:
                logger.warning("Warning: Skipping Import of Input Scenario Worksheet. This should only be done in resume mode.")


            # 6. Read in scenario option variables
            scenario_opts = datfunc.get_scenario_options(cur, schema) 
            
            if mode == 'ReEDS' and scenario_opts['region'] != 'United States':
                msg = 'Linked model can only run nationally. Select United States in input sheet'      
                logger.error(msg)
                msg = 'Model aborted'
                logger.error(msg)
                sys.exit(-1)
                    
            logger.info('Scenario Name: %s' % scenario_opts['scenario_name'])
            t0 = time.time()
            load_growth_scenario = scenario_opts['load_growth_scenario'].lower() # get financial variables
            end_year = scenario_opts['end_year']
            
            # start year comes from config
            if mode == 'ReEDS':
                model_years = [resume_year]
            else:
                model_years = range(cfg.start_year, end_year+1,2)
              
            # get the sectors to model
            t0 = time.time()
            sectors = datfunc.get_sectors(cur, schema)
            # get the technologies to model
            techs = datfunc.get_technologies(con, schema)
            logger.info('The Following Technologies Will Be Evaluated: %s' % techs)
            
            
            #==============================================================================
            #   get other user-defined inputs
            #==============================================================================
            t0 = time.time()
            msg = 'Getting various scenario parameters'
            logger.info(msg)
            #  these are all technology agnostic user-inputs
            max_market_share = datfunc.get_max_market_share(con, schema)
            market_projections = datfunc.get_market_projections(con, schema)
            rate_escalations = datfunc.get_rate_escalations(con, schema)
            rate_structures = datfunc.get_rate_structures(con, schema)
            # these are technology specific, set up in tidy form with a "tech" field
            financial_parameters = datfunc.get_financial_parameters(con, schema)
            incentive_options = datfunc.get_manual_incentive_options(con, schema)
            deprec_schedule = datfunc.get_depreciation_schedule(con, schema, macrs = True)
            ann_system_degradation = datfunc.get_system_degradation(con, schema)      
            logger.info('\tCompleted in: %0.1fs' %(time.time() - t0))

            if mode != 'ReEDS' or resume_year == 2014:                
                
                # create output subfolder for this scenario
                scen_name = scenario_opts['scenario_name']
                if scen_name in scenario_names:
                    logger.warning("Warning: Scenario name %s is a duplicate. Renaming to %s_%s" % (scen_name, scen_name, dup_n))
                    scen_name = "%s_%s" % (scen_name, dup_n)
                    dup_n += 1
                scenario_names.append(scen_name)
                out_scen_path = os.path.join(out_dir, scen_name)
                os.makedirs(out_scen_path)
                # copy the input scenario spreadsheet
                shutil.copy(input_scenario, out_scen_path)
                                
                #==========================================================================================================
                # CREATE AGENTS
                #==========================================================================================================
                logger.info("--------------Creating Agents---------------")
                
                # Combine All of the Temporally Varying Data in a new Table in Postgres
                t0 = time.time()
                msg = "Combining Temporal Factors"    
                logger.info(msg)        
#                datfunc.combine_temporal_data(cur, con, schema, techs, cfg.start_year, end_year, utilfunc.pylist_2_pglist(sectors.keys()), logger) # TODO: Comment
                logger.info('\tCompleted in: %0.1fs' %(time.time() - t0))                    
                
                 # loop through sectors, creating customer bins                
                for sector_abbr, sector in sectors.iteritems():
    
                    # define the rate escalation source and max market curve for the current sector
                    rate_escalation_source = scenario_opts['%s_rate_escalation' % sector_abbr]
                    # create the Main Table in Postgres (optimal turbine size and height for each year and customer bin)
#                    datfunc.generate_customer_bins(cur, con, techs, schema,  # TODO: Comment
#                                                   scenario_opts['random_generator_seed'], cfg.customer_bins, sector_abbr, sector, 
#                                                   cfg.start_year, end_year, rate_escalation_source, load_growth_scenario,
#                                                   cfg.npar, cfg.pg_conn_string, rate_structures[sector_abbr], logger = logger)

            #==========================================================================================================
            # CALCULATE BILL SAVINGS
            #==========================================================================================================
#            if cfg.init_model:
#                logger.info("---------Calculating Energy Savings---------")
#                for tech in techs: # TODO: Comment
#                    # find all unique combinations of rates, load, and generation
#                    
#                        logger.info('Calculating Annual Electric Bill Savings for %s' % tech.title())
#                        logger.info('\tFinding Unique Combinations of Rates, Load, and Generation')
#                        datfunc.get_unique_parameters_for_urdb3(cur, con, tech, schema, sectors)         
#                        # determine how many rate/load/gen combinations can be processed given the local memory resources
#                        row_count_limit = datfunc.get_max_row_count_for_utilityrate3()            
#                        sam_results_list = []
#                        # set up chunks
#                        uid_lists = datfunc.split_utilityrate3_inputs(row_count_limit, cur, con, schema, tech)
#                        nbatches = len(uid_lists)
#                        t0 = time.time()
#                        logger.info("\tSAM calculations will be run in %s batches to prevent memory overflow" % nbatches)
#                        for i, uids in enumerate(uid_lists): 
#                            logger.info("\t\tWorking on SAM Batch %s of %s" % (i+1, nbatches))
#                            # collect data for all unique combinations
#                            logger.info('\t\t\tCollecting SAM Inputs')
#                            t1 = time.time()
#                            rate_input_df = datfunc.get_utilityrate3_inputs(uids, cur, con, tech, schema, cfg.npar, cfg.pg_conn_string, cfg.gross_fit_mode)
#                            excess_gen_df = rate_input_df[['uid', 'excess_generation_percent', 'net_fit_credit_dollars']]
#                            logger.info('\t\t\t\tCompleted in: %0.1fs' % (time.time() - t1))        
#                            # calculate value of energy for all unique combinations
#                            logger.info('\t\t\tCalculating Energy Savings Using SAM')
#                            # run sam calcs in serial if only one core is available
#                            if cfg.local_cores == 1:
#                                sam_results_df = datfunc.run_utilityrate3(rate_input_df, logger)
#                            # otherwise run in parallel
#                            else:
#                                
#                                sam_results_df = pssc_mp.pssc_mp(rate_input_df,  cfg.local_cores)
#                            logger.info('\t\t\t\tCompleted in: %0.1fs' % (time.time() - t1),)                                        
#                            # append the excess_generation_percent and net_fit_credit_dollars to the sam_results_df
#                            sam_results_df = pd.merge(sam_results_df, excess_gen_df, on = 'uid')
#    
#                            # adjust the elec_cost_with_system_year1 to account for the net_fit_credit_dollars
#                            sam_results_df['elec_cost_with_system_year1'] = sam_results_df['elec_cost_with_system_year1'] - sam_results_df['net_fit_credit_dollars']              
#                            sam_results_list.append(sam_results_df)
#                            # drop the rate_input_df to save on memory
#                            del rate_input_df, excess_gen_df
#                   
#                        # write results to postgres
#                        logger.info("\tWriting SAM Results to Database")
#                        datfunc.write_utilityrate3_to_pg(cur, con, sam_results_list, schema, sectors, tech)
#                        logger.info('\tTotal time to calculate all electric bills: %0.1fs' % (time.time() - t0),)  

    
            #==========================================================================================================
            # MODEL DEPLOYMENT    
            #==========================================================================================================
            logger.info("---------Modeling Annual Deployment---------")
            if cfg.init_model:
                prng = np.random.RandomState(scenario_opts['random_generator_seed'])
            
            for sector_abbr, sector in sectors.iteritems():  
                logger.info("Modeling Deployment for %s Sector" % sector.title())
                dsire_incentives = {}
                for tech in techs:
                    # get dsire incentives for the generated customer bins
                    dsire_df = datfunc.get_dsire_incentives(cur, con, schema, tech, sector_abbr, cfg.npar, cfg.pg_conn_string, logger)     
                    dsire_incentives[tech] = dsire_df

                for year in model_years:
                    dfs = []
                    logger.info('\tWorking on %s for %s Sector' % (year, sector))
                    for tech in techs:
                        df = datfunc.get_main_dataframe(con, sector_abbr, schema, year, tech)
                        if mode == 'ReEDS':
                            # When in ReEDS mode add the values from ReEDS to df
                            df = pd.merge(df,distPVCurtailment, how = 'left', on = 'pca_reg')
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
    
                        # 9. Calculate economics 
                        ''' Calculates the economics of DER adoption through cash-flow analysis. 
                        This involves staging necessary calculations including: determining business model, 
                        determining incentive value and eligibility, defining market in the previous year. 
                        '''                        
                        
                        # Market characteristics from previous year
                        is_first_year = year == cfg.start_year
                        previous_year_results = datfunc.get_market_last_year(cur, con, is_first_year, tech, sector_abbr, sector, schema)   
                        df = pd.merge(df, previous_year_results, how = 'left', on = ['county_id','bin_id'])
                                            
                        # Calculate economics of adoption given system cofiguration and business model
                        logger.info("\t\tCalculating system economics for %s" % tech.title())
                        df = finfunc.calc_economics(df, schema, sector, sector_abbr, tech,
                                                                                   market_projections, financial_parameters, 
                                                                                   scenario_opts, incentive_options, max_market_share, cur, con, year, 
                                                                                   dsire_incentives[tech], deprec_schedule, logger, rate_escalations, 
                                                                                   ann_system_degradation, mode,curtailment_method, tech_lifetime = 25)
                        logger.info('\t\t\tCompleted in: %0.1fs' %(time.time() - t0))  
                        dfs.append(df)                        
                    
                    # exit the techs loop and combine results from each technology
                    df_combined = pd.concat(dfs, axis = 0, ignore_index = True)
                    # select from choices for business model and (optionally) technology
                    logger.info("\t\tSelecting financing option and technology")
                    t0 = time.time()
                    df_combined = tech_choice.select_financing_and_tech(df_combined, prng, cfg.alpha_lkup, cfg.choose_tech, techs)
                    logger.info('\t\t\tCompleted in: %0.1fs' %(time.time() - t0))    
                    
                    # 10. Calulate diffusion
                    ''' Calculates the market share (ms) added in the solve year. Market share must be less
                    than max market share (mms) except initial ms is greater than the calculated mms.
                    For this circumstance, no diffusion allowed until mms > ms. Also, do not allow ms to
                    decrease if economics deterioriate.
                    '''             
                    logger.info("\t\tCalculating diffusion")
                    df_combined, market_last_year_combined, logger = diffunc.calc_diffusion(df_combined, logger, year, sector)
                    logger.info('\t\t\tCompleted in: %0.1fs' %(time.time() - t0))  

                    for tech in techs:
                        df = df_combined[df_combined.tech == tech]
                        market_last_year = market_last_year_combined[market_last_year_combined.tech == tech]
                        if mode == 'ReEDS':
                            if sector_abbr == 'res':
                                market_last_year_res = market_last_year
                            if sector_abbr == 'ind':
                                market_last_year_ind = market_last_year
                            if sector_abbr == 'com':
                                market_last_year_com = market_last_year
                        
                        # 11. Save outputs from this year and update parameters for next solve       
                        t0 = time.time()                 
                        datfunc.write_outputs(con, cur, df, sector_abbr, schema) 
                        datfunc.write_last_year(con, cur, market_last_year, sector_abbr, schema, tech)
                         
                ## 12. Outputs & Visualization
                # set output subfolder  
            if mode != 'ReEDS' or resume_year == endyear:
                "---------Saving Model Results---------"
                for tech in techs:
                    out_tech_path = os.path.join(out_scen_path, tech)
                    os.makedirs(out_tech_path)
                    out_subfolders[tech].append(out_tech_path)
                        
                # copy outputs to csv     
                logger.info('\tExporting Results from Database')
                t0 = time.time()
                datfunc.copy_outputs_to_csv(techs, schema, out_scen_path, sectors, cur, con)
                logger.info('\t\tCompleted in: %0.1fs' %(time.time() - t0))
                # create output html report
                t0 = time.time()
                logger.info('\tCompiling Output Reports')
                datfunc.create_scenario_report(techs, schema, scen_name, out_scen_path, cur, con, cfg.Rscript_path, cfg.pg_params_file)
                logger.info('\t\tCompleted in: %0.1fs' %(time.time() - t0))
                                
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
            logger.info('Dropping the Output Schema (%s) from Database' % schema)
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