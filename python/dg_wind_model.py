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
import load_excel_wind as loadXL
import subprocess
import datetime
import config as cfg
import shutil
import sys, getopt
import pickle

def main(mode = None, resume_year = None):

    if mode == 'ReEDS':
        if resume_year == 2014:
            cfg.init_model = True
            cdate = time.strftime('%Y%m%d_%H%M%S')    
            out_dir = '%s/runs/results_%s' %(os.path.dirname(os.getcwd()),cdate)        
            os.makedirs(out_dir)
            input_scenarios = None
            market_last_year = None
        else:
            cfg.init_model = False
            # Load files here
            market_last_year = pd.read_pickle("market_last_year.pkl")   
            with open('saved_vars.pickle', 'rb') as handle:
                saved_vars = pickle.load(handle)
            out_dir = saved_vars['out_dir']
            input_scenarios = saved_vars['input_scenarios']
        #cfg.init_model,out_dir,input_scenarios, market_last_year = datfunc.load_resume_vars(cfg, resume_year)
    else:
        cdate = time.strftime('%Y%m%d_%H%M%S')    
        out_dir = '%s/runs/results_%s' %(os.path.dirname(os.getcwd()),cdate)        
        os.makedirs(out_dir)
    
    # check that random generator seed is in the acceptable range
    if cfg.random_generator_seed < 0 or cfg.random_generator_seed > 1:
        raise ValueError("""random_generator_seed in config.py is not in the range of acceptable values. Change to a value in the range >= 0 and <= 1.""")                           
        # check that number of customer bins is in the acceptable range
    if cfg.customer_bins not in (10,50,100,500):
        raise ValueError("""Error: customer_bins in config.py is not in the range of acceptable values. Change to a value in the set (10,50,100,500).""") 
    model_init = time.time()
    
    logger = datfunc.init_log(os.path.join(out_dir,'dg_wind_model.log'))
    logger.info('Initiating model at %s' %time.ctime())

    try:       
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
            input_scenarios = [s for s in glob.glob("../input_scenarios/*.xls*") if not '~$' in s]
            if len(input_scenarios) == 0:
                raise ValueError("""No input scenario spreadsheet were found in the input_scenarios folder.""")
        else:
            input_scenarios = ['']
            
        # run the model for each input scenario spreadsheet
        scenario_names = []
        out_subfolders = []
        for i, input_scenario in enumerate(input_scenarios): 
            logger.info('--------------------------------------------') 
            logger.info("Running Scenario %s of %s" % (i+1, len(input_scenarios)))
            
            # 5. Load Input excel spreadsheet to Postgres
            if cfg.init_model:
                logger.info('Loading input data from Input Scenario Worksheet')
                t0 = time.time()
                try:
                    loadXL.main(input_scenario, con, verbose = False)
                except loadXL.ExcelError, e:
                    msg = 'Loading failed with the following error: %s' % e      
                    logger.error(msg)
                    msg = 'Model aborted'
                    logger.error(msg)
                    sys.exit(-1)
                logger.info('Loading input sheet took: %0.1fs' %(time.time() - t0))
            else:
                logger.warning("Warning: Skipping Import of Input Scenario Worksheet. This should only be done in resume mode.")
            
            
            # 6. Read in scenario option variables
            scenario_opts = datfunc.get_scenario_options(cur) 
            logger.info('Scenario Name: %s' % scenario_opts['scenario_name'])
            t0 = time.time()
            exclusions = datfunc.get_exclusions(cur) # get exclusions
            logger.info('Getting exclusions took: %0.1f' % (time.time() - t0))
            load_growth_scenario = scenario_opts['load_growth_scenario'] # get financial variables
            net_metering = scenario_opts['net_metering_availability']
            inflation = scenario_opts['ann_inflation']
            end_year = scenario_opts['end_year']
            
            # start year comes from config
            if mode == 'ReEDS':
                model_years = [resume_year]
            else:
                model_years = range(cfg.start_year,end_year+1,2)
              
            # get the sectors to model
            t0 = time.time()
            
            sectors = datfunc.get_sectors(cur)
            deprec_schedule = datfunc.get_depreciation_schedule(con, type = 'standard').values
            financial_parameters = datfunc.get_financial_parameters(con, res_model = 'Existing Home', com_model = 'Host Owned', ind_model = 'Host Owned')
            max_market_share = datfunc.get_max_market_share(con, sectors.values(), scenario_opts, residential_type = 'retrofit', commercial_type = 'retrofit', industrial_type = 'retrofit')
            market_projections = datfunc.get_market_projections(con)
            rate_escalations = datfunc.get_rate_escalations(con)

            logger.info('Getting various parameters took: %0.1fs' %(time.time() - t0))
            # 7. Combine All of the Temporally Varying Data in a new Table in Postgres
            if cfg.init_model:
                t0 = time.time()
                datfunc.combine_temporal_data(cur, con, cfg.start_year, end_year, datfunc.pylist_2_pglist(sectors.values()), cfg.preprocess, logger)
                logger.info('datfunc.combine_temporal_data took: %0.1fs' %(time.time() - t0))
            # 8. Set up the Main Data Frame for each sector
            outputs = pd.DataFrame()
            t0 = time.time()
            datfunc.clear_outputs(con,cur) # clear results from previous run
            logger.info('datfunc.clear_outputs took: %0.1fs' %(time.time() - t0))
              
            for sector_abbr, sector in sectors.iteritems():
                
                # define the rate escalation source and max market curve for the current sector
                rate_escalation_source = scenario_opts['%s_rate_escalation' % sector_abbr]
                # create the Main Table in Postgres (optimal turbine size and height for each year and customer bin)
                if cfg.init_model:
                    t0 = time.time()
                    main_table = datfunc.generate_customer_bins(cur, con, cfg.random_generator_seed, cfg.customer_bins, sector_abbr, sector, 
                                                   cfg.start_year, end_year, rate_escalation_source, load_growth_scenario, exclusions,
                                                   cfg.oversize_turbine_factor, cfg.undersize_turbine_factor, cfg.preprocess, cfg.npar, cfg.pg_conn_string, scenario_opts['net_metering_availability'], logger = logger)
                    logger.info('datfunc.generate_customer_bins for %s sector took: %0.1fs' %(sector, time.time() - t0))        
                else:
                    main_table = 'diffusion_wind.pt_%s_best_option_each_year' % sector_abbr
                
                # get dsire incentives for the generated customer bins
                t0 = time.time()
                dsire_incentives = datfunc.get_dsire_incentives(cur, con, sector_abbr, cfg.preprocess, cfg.npar, cfg.pg_conn_string, logger)
                logger.info('datfunc.get_dsire_incentives took: %0.1fs' %(time.time() - t0))                  
                # Pull data from the Main Table to a Data Frame for each year
                for year in model_years:
                    t_loop = time.time()
                    logger.info('Working on %s for %s sector' %(year, sector_abbr))
                    
                    t0 = time.time()                    
                    df = datfunc.get_main_dataframe(con, main_table, year)
                    logger.info('datfunc.get_main_dataframe for %s took: %0.1fs' %(year, time.time() - t0))
                    
                    # 9. Calculate economics including incentives
                    if year == cfg.start_year:
                        market_last_year = 0 #market_last_year is actually initialized in calc_economics
                        
                    t_calc_econ = time.time()    
                    df = finfunc.calc_economics(df, sector, sector_abbr, market_projections, market_last_year, financial_parameters, cfg, scenario_opts, max_market_share, cur, con, year, dsire_incentives, deprec_schedule, logger, rate_escalations)
                    logger.info('finfunc.calc_economics for %s for %s sector took: %0.1fs' %(year, sector, time.time() - t_calc_econ))
                    
                    # 10. Calulate diffusion
                    ''' Calculates the market share (ms) added in the solve year. Market share must be less
                    than max market share (mms) except initial ms is greater than the calculated mms.
                    For this circumstance, no diffusion allowed until mms > ms. Also, do not allow ms to
                    decrease if economics deteroriate.
                    '''             
                    t_calc_diffusion = time.time() 
                    df, market_last_year, logger = diffunc.calc_diffusion(df, logger, year, sector)
                    logger.info('The entire diffunc.calc_diffusion for %s for %s sector took: %0.1fs' %(year, sector, time.time() - t_calc_diffusion))
                    
                    # 11. Save outputs from this year and update parameters for next solve       
                    t0 = time.time()                    
                    datfunc.write_outputs(con, cur, df, sector_abbr)
                    logger.info('datfunc.write_outputs for %s took: %0.1fs' %(year, time.time() - t0))                        
                    logger.info('Doing the entire %s model year for %s sector took: %0.1fs' %(year, sector, time.time() - t_loop))   
            ## 12. Outputs & Visualization
            # set output subfolder
            if mode == 'ReEDS':
                reeds_out = sqlio.read_frame('SELECT * FROM diffusion_wind.outputs_all', con)
                #r = reeds_out.groupby('pca_reg')['installed_capacity'].sum()
                market_last_year.to_pickle("market_last_year.pkl")
                saved_vars = {'out_dir': out_dir, 'input_scenarios':input_scenarios}
                with open('saved_vars.pickle', 'wb') as handle:
                    pickle.dump(saved_vars, handle)  
                return reeds_out
                
            else:
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
                datfunc.copy_outputs_to_csv(out_path, sectors, cur, con)
                # copy the input scenario spreadsheet
                shutil.copy(input_scenario, out_path)
                logger.info('datfunc.copy_outputs_to_csv took: %0.1fs' %(time.time() - t0))
                # create output html report
                t0 = time.time()
                datfunc.create_scenario_report(scen_name, out_path, cur, con, cfg.Rscript_path, logger)
                logger.info('datfunc.create_scenario_report took: %0.1fs' %(time.time() - t0))
                #logger.info('Model completed at %s run took: %.1f seconds' % (time.ctime(), time.time() - model_init))
                logger.info('The entire model run took: %.1f seconds' % (time.time() - model_init))
                
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
        logger.error(e.__str__(), exc_info = True)
    
    finally:
        datfunc.shutdown_log(logger)
        datfunc.code_profiler(out_dir)
    
if __name__ == '__main__':
    main()