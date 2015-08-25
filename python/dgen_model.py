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
import pandas.io.sql as sqlio
import psycopg2 as pg
import psycopg2.extras as pgx
import numpy as np
import scipy as sp
import glob
import matplotlib as mpl
import collections
import diffusion_functions as diffunc
import financial_functions as finfunc
import data_functions as datfunc
reload(datfunc)
from data_objects import FancyDataFrame
import subprocess
import datetime
import config as cfg
import shutil
import sys
from sam.languages.python import sscapi
import getopt
import pickle
import pssc_mp
from excel import excel_functions

    

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
                out_dir = '%s/runs_%s/results_%s' %(os.path.dirname(os.getcwd()), cfg.technology, cdate)        
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
            #cfg.init_model,out_dir,input_scenarios, market_last_year = datfunc.load_resume_vars(cfg, resume_year)
        else:
            # set input dataframes for reeds-mode settings (these are ingested to postgres later)
            reeds_mode_df = FancyDataFrame(data = [False])
            ReEDS_PV_CC = FancyDataFrame(columns = ['year', 'Capital_Cost'])
            cdate = time.strftime('%Y%m%d_%H%M%S')    
            out_dir = '%s/runs_%s/results_%s' %(os.path.dirname(os.getcwd()), cfg.technology, cdate)        
            os.makedirs(out_dir)

                            
        # check that number of customer bins is in the acceptable range
        if type(cfg.customer_bins) <> int:
            raise ValueError("""Error: customer_bins in config.py must be of type integer.""") 
        if cfg.customer_bins <= 0:
            raise ValueError("""Error: customer_bins in config.py must be a positive integer.""") 
        model_init = time.time()
        
        logger = datfunc.init_log(os.path.join(out_dir,'dg_model.log'))
        logger.info('Initiating model (%s)' %time.ctime())
       
        # if parallelization is off, reduce npar to 1
        if not cfg.parallelize:
            cfg.npar = 1
            
        # 4. Connect to Postgres and configure connection(s) (to edit login information, edit config.py)
        # create a single connection to Postgres Database -- this will serve as the main cursor/connection
        con, cur = datfunc.make_con(cfg.pg_conn_string)
        pgx.register_hstore(con) # register access to hstore in postgres    
        
        # configure pandas display options
        pd.set_option('max_columns', 9999)
        pd.set_option('max_rows',10)
        
        # find the input excel spreadsheets
        if cfg.init_model:    
            input_scenarios = [s for s in glob.glob("../input_scenarios_%s/*.xls*" % cfg.technology) if not '~$' in s]
            if len(input_scenarios) == 0:
                raise ValueError("""No input scenario spreadsheet were found in the input_scenarios_%s folder.""" % cfg.technology)
        elif mode != 'ReEDS':
            input_scenarios = ['']
            
        # run the model for each input scenario spreadsheet
        scenario_names = []
        out_subfolders = []
        for i, input_scenario in enumerate(input_scenarios):
            logger.info('--------------------------------------------') 
            logger.info("Running Scenario %s of %s" % (i+1, len(input_scenarios)))
            
            # 5. Load Input excel spreadsheet to Postgres
            if cfg.init_model:
                # create the output schema
                logger.info('Creating output schema')
                schema = datfunc.create_output_schema(cfg.pg_conn_string, source_schema = 'diffusion_template')
                logger.info('Output schema is: %s' % schema)
                # ************************************************************************
                # NOTE: This is temporary until the model can dynamically handle running both wind and solar technologies
                datfunc.set_source_pt_microdata(con, cur, schema, cfg.technology)
                # ************************************************************************
                # write the reeds settings to postgres
                reeds_mode_df.to_postgres(con, cur, schema, 'input_reeds_mode')
                ReEDS_PV_CC.to_postgres(con, cur, schema, 'input_reeds_capital_costs')  
                
                logger.info('Loading input data from Input Scenario Worksheet')
                t0 = time.time()
                try:
                    excel_functions.load_scenario(input_scenario, schema, con, test = False)
                except Exception, e:
                    msg = 'Loading failed with the following error: %s' % e      
                    logger.error(msg)
                    msg = 'Model aborted'
                    logger.error(msg)
                    sys.exit(-1)
                logger.info('Loading input sheet took: %0.1fs' %(time.time() - t0))
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
            # Generate a pseudo-random number generator to generate random numbers in numpy.
            # This method is better than np.random.seed() because it is thread-safe
            prng = np.random.RandomState(scenario_opts['random_generator_seed'])
            
            if cfg.technology == 'solar':
                ann_system_degradation = datfunc.get_system_degradation(cur,schema)
            else:
                ann_system_degradation = 0
            
            # start year comes from config
            if mode == 'ReEDS':
                model_years = [resume_year]
            else:
                model_years = range(cfg.start_year,end_year+1,2)
              
            # get the sectors to model
            t0 = time.time()
            
            sectors = datfunc.get_sectors(cur, schema)
            deprec_schedule = datfunc.get_depreciation_schedule(con, schema, type = 'macrs').values
            financial_parameters = datfunc.get_financial_parameters(con, schema, cfg.technology)
            max_market_share = datfunc.get_max_market_share(con, schema)
            market_projections = datfunc.get_market_projections(con, schema)
            rate_escalations = datfunc.get_rate_escalations(con, schema)
            rate_structures = datfunc.get_rate_structures(con, schema)
            incentive_options = datfunc.get_manual_incentive_options(con, schema, cfg.technology)

            logger.info('Getting various parameters took: %0.1fs' %(time.time() - t0))

            # 7. Combine All of the Temporally Varying Data in a new Table in Postgres
            if cfg.init_model:
                t0 = time.time()
                datfunc.combine_temporal_data(cur, con, schema, cfg.technology, cfg.start_year, end_year, datfunc.pylist_2_pglist(sectors.keys()), cfg.preprocess, logger)
                logger.info('datfunc.combine_temporal_data took: %0.1fs' %(time.time() - t0))
            # 8. Set up the Main Data Frame for each sector
            outputs = pd.DataFrame()
            t0 = time.time()
            if mode != 'ReEDS' or resume_year == 2014:
                datfunc.clear_outputs(con, cur, schema) # clear results from previous run
            logger.info('datfunc.clear_outputs took: %0.1fs' %(time.time() - t0))
            
            
            for sector_abbr, sector in sectors.iteritems():

                # define the rate escalation source and max market curve for the current sector
                rate_escalation_source = scenario_opts['%s_rate_escalation' % sector_abbr]
                # create the Main Table in Postgres (optimal turbine size and height for each year and customer bin)
                if cfg.init_model:
                    t0 = time.time()
                    datfunc.generate_customer_bins(cur, con, cfg.technology, schema, 
                                                   scenario_opts['random_generator_seed'], cfg.customer_bins, sector_abbr, sector, 
                                                   cfg.start_year, end_year, rate_escalation_source, load_growth_scenario,
                                                   cfg.oversize_system_factor, cfg.undersize_system_factor, cfg.preprocess, cfg.npar, 
                                                   cfg.pg_conn_string, 
                                                   rate_structures[sector_abbr], logger = logger)
                    logger.info('datfunc.generate_customer_bins for %s sector took: %0.1fs' %(sector, time.time() - t0))        


            # break from the loop to find all unique combinations of rates, load, and generation
            if cfg.init_model:
                logger.info('Finding unique combinations of rates, load, and generation')
                datfunc.get_unique_parameters_for_urdb3(cur, con, cfg.technology, schema, sectors)         
                # determine how many rate/load/gen combinations can be processed given the local memory resources
                row_count_limit = datfunc.get_max_row_count_for_utilityrate3()            
                sam_results_list = []
                # set up chunks
                uid_lists = datfunc.split_utilityrate3_inputs(row_count_limit, cur, con, schema)
                nbatches = len(uid_lists)
                t0 = time.time()
                logger.info("SAM Calculations will be run in %s batches to prevent memory overflow" % nbatches)
            # create multiprocessing objects before loading inputs to improve memory efficiency
            # consumers, tasks, results = pssc_mp.create_consumers(cfg.local_cores)
                for i, uids in enumerate(uid_lists): 
                    logger.info("Working on SAM Batch %s of %s" % (i+1, nbatches))
                    # collect data for all unique combinations
                    logger.info('\tCollecting SAM inputs')
                    t1 = time.time()
                    rate_input_df = datfunc.get_utilityrate3_inputs(uids, cur, con, cfg.technology, schema, cfg.npar, cfg.pg_conn_string)
                    logger.info('\tdatfunc.get_utilityrate3_inputs took: %0.1fs' % (time.time() - t1),)        
                    # calculate value of energy for all unique combinations
                    logger.info('\tCalculating value of energy using SAM')
                    # Calculate the fraction of generation output to grid (excess) to annual system generation. Excess generation is subject to net metering and curtailment
                    t1 = time.time()                    
                    excess_gen_percent = rate_input_df.apply(datfunc.excess_generation_percent, axis = 1, args = ('consumption_hourly','generation_hourly'))[['uid','excess_generation_percent']]                    
                    logger.info('Calculating excess generation took: %0.1fs' % (time.time() - t1))                      
                    t1 = time.time()
                    # run sam calcs in serial if only one core is available
                    if cfg.local_cores == 1:
                        sam_results_df = datfunc.run_utilityrate3(rate_input_df, logger)
                    # otherwise run in parallel
                    else:
                        sam_results_df = pssc_mp.pssc_mp(rate_input_df, cfg.local_cores)
                        #sam_results_df = pssc_mp.run_pssc(rate_input_df, consumers, tasks, results)
                    logger.info('\tdatfunc.run_utilityrate3 took: %0.1fs' % (time.time() - t1),)                                        
                    sam_results_df = pd.merge(sam_results_df, excess_gen_percent)              
                    sam_results_list.append(sam_results_df)
                    # drop the rate_input_df to save on memory
                    del rate_input_df, excess_gen_percent
                logger.info('All SAM calculations completed in: %0.1fs' % (time.time() - t0),)
           
                # write results to postgres
                t0 = time.time()
                datfunc.write_utilityrate3_to_pg(cur, con, sam_results_list, schema, sectors, cfg.technology)
                logger.info('datfunc.write_utilityrate3_to_pg took: %0.1fs' % (time.time() - t0),)  
            #==============================================================================
             


            # loop through sectors and time steps to calculate full economics and diffusion                
            for sector_abbr, sector in sectors.iteritems():  
                # get dsire incentives for the generated customer bins
                t0 = time.time()
                dsire_incentives = datfunc.get_dsire_incentives(cur, con, schema, cfg.technology, sector_abbr, cfg.preprocess, cfg.npar, cfg.pg_conn_string, logger)
                logger.info('datfunc.get_dsire_incentives took: %0.1fs' %(time.time() - t0))                  
                # Pull data from the Main Table to a Data Frame for each year
                for year in model_years:
                    logger.info('Working on %s for %s sector' % (year, sector_abbr))               
                    df = datfunc.get_main_dataframe(con, sector_abbr, schema, year)
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
                    if year == cfg.start_year: 
                        # get the initial market share per bin by county
                        initial_market_shares = datfunc.get_initial_market_shares(cur, con, cfg.technology, sector_abbr, sector, schema, cfg.technology)
                        df = pd.merge(df, initial_market_shares, how = 'left', on = ['county_id','bin_id'])
                        df['market_value_last_year'] = df['installed_capacity_last_year'] * df['installed_costs_dollars_per_kw']
                        
                        ## get the initial lease availability by state
                        #leasing_avail_status_by_state = datfunc.get_initial_lease_status(df,con)
                        #df = pd.merge(df, leasing_avail_status_by_state, how = 'left', on = ['state_abbr'])
                    else:    
                        df = pd.merge(df,market_last_year, how = 'left', on = ['county_id','bin_id'])
                        #df = pd.merge(df, leasing_avail_status_by_state, how = 'left', on = ['state_abbr'])
                    
                    # Determine whether leasing is permitted in given year
                    lease_availability = datfunc.get_lease_availability(con, schema, cfg.technology)
                    df = pd.merge(df, lease_availability, on = ['state_abbr','year'])
                                        
                    # Calculate economics of adoption given system cofiguration and business model
                    df = finfunc.calc_economics(df, schema, sector, sector_abbr, 
                                                                               market_projections, financial_parameters, 
                                                                               cfg, scenario_opts, incentive_options, max_market_share, cur, con, year, 
                                                                               dsire_incentives, deprec_schedule, logger, rate_escalations, 
                                                                               ann_system_degradation, mode,prng,curtailment_method)
                    
                    # 10. Calulate diffusion
                    ''' Calculates the market share (ms) added in the solve year. Market share must be less
                    than max market share (mms) except initial ms is greater than the calculated mms.
                    For this circumstance, no diffusion allowed until mms > ms. Also, do not allow ms to
                    decrease if economics deterioriate.
                    '''             
                    df, market_last_year, logger = diffunc.calc_diffusion(df, logger, year, sector)
                    
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
                     
            ## 12. Outputs & Visualization
            # set output subfolder
                
            if mode != 'ReEDS' or resume_year == endyear:
                dup_n = 1
                scen_name = scenario_opts['scenario_name']
                if scen_name in scenario_names:
                    logger.warning("Warning: Scenario name %s is a duplicate. Renaming to %s_%s" % (scen_name, scen_name, dup_n))
                    scen_name = "%s_%s" % (scen_name, dup_n)
                    dup_n += 1
                scenario_names.append(scen_name)
                out_path = os.path.join(out_dir,scen_name)
                out_subfolders.append(out_path)
                os.makedirs(out_path)  
                        
                # copy outputs to csv     
                logger.info('Writing outputs')
                t0 = time.time()
                datfunc.copy_outputs_to_csv(cfg.technology, schema, out_path, sectors, cur, con)
                # copy the input scenario spreadsheet
                shutil.copy(input_scenario, out_path)
                logger.info('datfunc.copy_outputs_to_csv took: %0.1fs' %(time.time() - t0))
                # create output html report
                t0 = time.time()
                datfunc.create_scenario_report(cfg.technology, schema, scen_name, out_path, cur, con, cfg.Rscript_path, logger)
                logger.info('datfunc.create_scenario_report took: %0.1fs' %(time.time() - t0))
                logger.info('The entire model run took: %.1f seconds' % (time.time() - model_init))
                
                #####################################################################
                ### THIS IS TEMPORARY ###
                # drop the new schema
                logger.info('Dropping the output schema (%s) from postgres' % schema)
                datfunc.drop_output_schema(cfg.pg_conn_string, schema)
                #####################################################################
            
            if mode == 'ReEDS':
                reeds_out = datfunc.combine_outputs_reeds(schema, sectors, cur, con)
                cf_by_pca_and_ts = datfunc.summarise_solar_resource_by_ts_and_pca_reg(reeds_out, con)
                
                market_last_year_res.to_pickle("market_last_year_res.pkl")
                market_last_year_ind.to_pickle("market_last_year_ind.pkl")
                market_last_year_com.to_pickle("market_last_year_com.pkl")
                saved_vars = {'out_dir': out_dir, 'input_scenarios':input_scenarios}
                with open('saved_vars.pickle', 'wb') as handle:
                    pickle.dump(saved_vars, handle)  
                return reeds_out, cf_by_pca_and_ts
                
        if len(input_scenarios) > 1:
            # assemble report to compare scenarios
            scenario_analysis_path = '%s/r/graphics/scenario_analysis.R' % os.path.dirname(os.getcwd())
            scenario_output_paths = datfunc.pylist_2_pglist(out_subfolders).replace("'","").replace(" ","")
            scenario_comparison_path = os.path.join(out_dir,'scenario_comparison')
            command = [cfg.Rscript_path,'--vanilla',scenario_analysis_path,scenario_output_paths,scenario_comparison_path]
            msg = 'Creating scenario analysis report'            
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
            datfunc.shutdown_log(logger)
            datfunc.code_profiler(out_dir)
    
if __name__ == '__main__':
    main()