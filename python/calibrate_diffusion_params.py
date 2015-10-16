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
# ---------------------------------------------
# order of the next 3 needs to be maintained
# otherwise the logger may not work correctly
import data_functions as datfunc
reload(datfunc)
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
import itertools

#==============================================================================
# raise  numpy and pandas warnings as exceptions
#==============================================================================
#np.seterr(all='raise')
pd.set_option('mode.chained_assignment', 'raise')
#==============================================================================
    

def main(schema, p_scalar_list = [1], teq_yr1_list = [2], make_reports = True, save_all_outputs = True):

    try:
        model_init = time.time()
        cdate = time.strftime('%Y%m%d_%H%M%S')    
        out_dir = '%s/runs/results_%s' %(os.path.dirname(os.getcwd()), cdate)        
        os.makedirs(out_dir)
                            
        # check that number of customer bins is in the acceptable range
        if type(cfg.customer_bins) <> int:
            raise ValueError("""Error: customer_bins in config.py must be of type integer.""") 
        if cfg.customer_bins <= 0:
            raise ValueError("""Error: customer_bins in config.py must be a positive integer.""") 
        
        # create the logger
        logger = utilfunc.get_logger(os.path.join(out_dir,'dg_model.log'))
            
        # 4. Connect to Postgres and configure connection(s) (to edit login information, edit config.py)
        # create a single connection to Postgres Database -- this will serve as the main cursor/connection
        con, cur = utilfunc.make_con(cfg.pg_conn_string)
        pgx.register_hstore(con) # register access to hstore in postgres    
     
        #==========================================================================================================
        # PREP DATABASE
        #==========================================================================================================
        scenario_names = []
        out_subfolders = {'wind' : [], 'solar' : []}
        
        # read in high level scenario settings from previous model run
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
                                  
        # get other scenario inputs
        logger.info('Getting various scenario parameters')
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

        # set model years
        model_years = range(cfg.start_year, end_year+1,2)
   
        # create output folder for this scenario
        out_scen_path, scenario_names, dup_n = datfunc.create_scenario_results_folder(None, scen_name, scenario_names, out_dir)
        # create tech subfolders
        out_subfolders = datfunc.create_tech_subfolders(out_scen_path, techs, out_subfolders, choose_tech)



        # find combinatorial product of p_scalars and teq_yr1s
        combinations = pd.DataFrame(list(itertools.product(p_scalar_list, teq_yr1_list)), columns = ['p_scalar', 'teq_yr1'])
        n_combos = combinations.shape[0]
        
        # create table to hold the summary results of all iterations
        datfunc.create_deployment_summary_table(cur, con, schema)
        
        for row in combinations.iterrows():
            #==========================================================================================================
            # MODEL TECHNOLOGY DEPLOYMENT    
            #==========================================================================================================
            logger.info("---------Modeling Annual Deployment---------")   
            i = row[0] + 1
            logger.info("Combination %s of %s" % (i, n_combos))
            # get the actual combination
            combo = row[1]
            p_scalar = combo['p_scalar']
            teq_yr1 = combo['teq_yr1']

            # clear output tables
            datfunc.clear_outputs(con, cur, schema)            
            
            # create or reinitialize psuedo-rangom number generator (not used until tech/finance choice function)
            prng = np.random.RandomState(scenario_opts['random_generator_seed'])
   
            # get dsire incentives, srecs, and itc for the generated customer bins
            dsire_incentives = datfunc.get_dsire_incentives(cur, con, schema, techs, sectors, cfg.pg_conn_string)
            srecs = datfunc.get_srecs(cur, con, schema, techs, cfg.pg_conn_string)
            itc_options = pd.read_sql('SELECT * FROM %s.input_main_itc_options; ' % schema, con) 
            for year in model_years:
                logger.info('\tWorking on %s' % year)
                    
                # get input agent attributes from postgres
                df = datfunc.get_main_dataframe(con, sectors, schema, year, techs)

                # When not in ReEDS mode set default (and non-impacting) values for the ReEDS parameters
                df['curtailment_rate'] = 0
                df['ReEDS_elec_price_mult'] = 1
                curtailment_method = 'net'           
                
                # Calculate economics of adoption for different busines models
                df = finfunc.calc_economics(df, schema, 
                                           market_projections, financial_parameters, 
                                           scenario_opts, incentive_options, max_market_share, 
                                           cur, con, year, dsire_incentives, srecs, deprec_schedule, 
                                           ann_system_degradation, None, curtailment_method, itc_options,
                                           tech_lifetime = 25)
                
                # select from choices for business model and (optionally) technology
                df = tech_choice.select_financing_and_tech(df, prng, cfg.alpha_lkup, sectors, choose_tech, techs)                 
                
                # calculate diffusion based on economics and bass diffusion      
                df, market_last_year = diffunc.calc_diffusion(df, cur, con, cfg, techs, sectors, schema, year, 
                                                              cfg.start_year, cfg.calibrate_mode, 
                                                              p_scalar, teq_yr1) 
                 
                # write the incremental results to the database
                datfunc.write_outputs(con, cur, df, sectors, schema) 
                datfunc.write_last_year(con, cur, market_last_year, schema)
    
            #==============================================================================
            #    Outputs & Visualization
            #==============================================================================

            # compile full output tables
            datfunc.combine_outputs(techs, schema, sectors, cur, con)
                
            # summarize overall deployment by technology and year
            datfunc.summarize_deployment(cur, con, schema, p_scalar, teq_yr1)

            file_suffix = '_p%s_teq%s' % (p_scalar, teq_yr1)
            
            if save_all_outputs == True:
                # copy outputs to csv     
                datfunc.copy_outputs_to_csv(techs, schema, out_scen_path, cur, con, file_suffix)
                
                # write reeds mode outputs to csvs in case they're needed
                reedsfunc.write_reeds_offline_mode_data(schema, con, out_scen_path, file_suffix)
                
            if make_reports == True:
                # create output html report                
                datfunc.create_scenario_report(techs, schema, scen_name, out_scen_path, cur, con, cfg.Rscript_path, cfg.pg_params_file, file_suffix)
                
                # create tech choice report (if applicable)
                datfunc.create_tech_choice_report(choose_tech, schema, scen_name, out_scen_path, cur, con, cfg.Rscript_path, cfg.pg_params_file, file_suffix)

        # dump the deployment summary results to csv
        datfunc.copy_deployment_summary_to_csv(schema, out_scen_path, cur, con)
        
        logger.info('Completed all combinations in: %.1f seconds' % (time.time() - model_init))
                
            

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
    main('diffusion_results_2015_10_15_17h18m08s', 
         p_scalar_list = [1, 10], 
         teq_yr1_list = [2, 3], 
         make_reports = True, save_all_outputs = True)