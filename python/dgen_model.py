# -*- coding: utf-8 -*-
"""
This is the main module of the Distributed Generation Market Demand Model
created by the National Renewable Energy Lab (NREL). 

Running this module requires a properly installed environment with applicable scenario files. 
Sample scenario files can be found in `reference_data/example_data` and include an .xlsm file as well 
as a folder of .csv files.
"""

# before doing anything, check model dependencies
import prerun_test
import time
import os
import pandas as pd
import numpy as np
import sys
import financial_functions as fFuncs
# ---------------------------------------------
# order of the next 3 needs to be maintained
# otherwise the logger may not work correctly
# (I think the order needs to follow the order
# in which each module is used in __main__)
# ---------------------------------------------
import utility_functions as utilfunc
from agents import Agents
import settings
import agent_mutation
import diffusion_functions
import financial_functions
import pickle

#==============================================================================
# raise  numpy and pandas warnings as exceptions
#==============================================================================
pd.set_option('mode.chained_assignment', None)
#==============================================================================

def main(mode = None, resume_year = None, endyear = None, ReEDS_inputs = None):
    """
    Compute the economic adoption of distributed generation resources on an agent-level basis.

    Model output is saved to a `/runs` file within the dGen directory.
    """
    try:
        # =====================================================================
        # SET UP THE MODEL TO RUN
        # =====================================================================
        # initialize Model Settings object
        # (this controls settings that apply to all scenarios to be executed)
        model_settings = settings.init_model_settings()
        prerun_test.check_dependencies()

        # make output directory
        # create the logger and stamp with git hash
        logger = utilfunc.get_logger(os.path.join(model_settings.out_dir, 'dg_model.log'))
        logger.info("Model version is git commit {:}".format(model_settings.git_hash))

        # =====================================================================
        # LOOP OVER SCENARIOS
        # =====================================================================

        out_subfolders = {'solar': []} 
 
        for i, scenario_file in enumerate(model_settings.input_scenarios):
            logger.info('============================================')
            logger.info('============================================')
            logger.info("Running Scenario {i} of {n}".format(i=i + 1,n=len(model_settings.input_scenarios)))
            
            # initialize ScenarioSettings object
            # (this controls settings that apply only to this specific scenario)
            scenario_settings = settings.init_scenario_settings(scenario_file, model_settings)

            # log summary high level secenario settings
            logger.info('Scenario Settings:')
            logger.info('\tScenario Name: %s' % scenario_settings.scenario_name)

            logger.info('\tSectors: %s' % list(scenario_settings.sector_data.keys()))
            logger.info('\tTechnologies: %s' % scenario_settings.techs)
            logger.info('\tYears: %s - %s' % (scenario_settings.start_year, scenario_settings.end_year))

            logger.info('Results Path: %s' % (scenario_settings.out_scen_path))

            #==========================================================================================================
            # CREATE AGENTS
            #==========================================================================================================
            logger.info("-------------- Agent Preparation ---------------")
            
            if scenario_settings.generate_agents:
                logger.info('\tCreating Agents')
                solar_agents = Agents(agent_mutation.init_solar_agents(scenario_settings))
                logger.info('....{} agents in input csv'.format(len(solar_agents)))
                
                # Write base agents to disk
                solar_agents.df.to_pickle(scenario_settings.out_scen_path + '/agent_df_base.pkl')
            else:
                logger.info('Loading %s' % scenario_settings.agents_file_name)
                with open(scenario_settings.agents_file_name,"r") as f:
                    solar_agents = Agents(pickle.load(f))
                
            # Get set of columns that define agent's immutable attributes
            cols_base = list(solar_agents.df.columns.values)

            #==============================================================================
            # TECHNOLOGY DEPLOYMENT
            #==============================================================================
            logger.info("-------------- Yearly Analysis ---------------")
            complete_df = pd.DataFrame()
            if scenario_settings.techs == ['solar']:
                solar_agents.df['tech'] = 'solar'

                for i, year in enumerate(scenario_settings.model_years):

                    is_first_year =  year == model_settings.start_year

                    logger.info('\tWorking on %s' % year)

                    # determine any non-base columns and drop them
                    cols = list(solar_agents.df.columns.values)
                    cols_to_drop = [x for x in cols if x not in cols_base]
                    if len(cols_to_drop) != 0:
                        solar_agents.df.drop(cols_to_drop, axis=1, inplace=True)

                    # copy the core agent object and set their year
                    solar_agents.df['year'] = year

                    # get and apply load growth
                    load_growth_yearly =  scenario_settings.get_load_growth(year)
                    
                    solar_agents.on_frame(agent_mutation.elec.apply_load_growth, (load_growth_yearly))
                    
                    # Normalize the hourly load profile to updated total load which includes load growth multiplier
                    solar_agents.on_frame(agent_mutation.elec.apply_scale_normalized_load_profiles)
               
                    # Get and apply net metering parameters
                    net_metering_yearly =  scenario_settings.get_nem_settings(year)
                    solar_agents.on_frame(agent_mutation.elec.apply_export_tariff_params, (net_metering_yearly))
                    
                    # Apply each agent's electricity price change and assumption about increases
                    solar_agents.on_frame(agent_mutation.elec.apply_elec_price_multiplier_and_escalator, [ year, scenario_settings.get_rate_escalations()])
                    
                    # Apply PV Specs                    
                    solar_agents.on_frame(agent_mutation.elec.apply_pv_specs, scenario_settings.get_pv_specs())
                    solar_agents.on_frame(agent_mutation.elec.apply_storage_specs, [scenario_settings.get_batt_price_trajectories(), year, scenario_settings])

                    # Apply financial terms
                    solar_agents.on_frame(agent_mutation.elec.apply_financial_params, [scenario_settings.get_financing_terms(), scenario_settings.financial_options['annual_inflation_pct']])

                    # Apply wholesale electricity prices
                    solar_agents.on_frame(agent_mutation.elec.apply_wholesale_elec_prices, scenario_settings.get_wholesale_elec_prices())
                    
                    # Size S+S system and calculate electric bills
                    if 'ix' not in os.name: 
                        cores=None
                    else: 
                        cores=model_settings.local_cores
                    
                    solar_agents.on_row(fFuncs.calc_system_size_and_financial_performance, cores=cores)

                    solar_agents.df['agent_id'] = solar_agents.df.index.values

                    # Calculate the financial performance of the S+S systems 
                    solar_agents.on_frame(financial_functions.calc_financial_performance)

                    # Calculate Maximum Market Share
                    solar_agents.on_frame(financial_functions.calc_max_market_share, scenario_settings.get_max_market_share())

                    # determine "developable" population
                    solar_agents.on_frame(agent_mutation.elec.calculate_developable_customers_and_load)

                    # Apply market_last_year
                    if is_first_year:
                        solar_agents.on_frame(agent_mutation.elec.estimate_initial_market_shares)
                        market_last_year_df = None
                    else:
                        solar_agents.on_frame(agent_mutation.elec.apply_market_last_year, market_last_year_df)

                    # Calculate diffusion based on economics and bass diffusion
                    solar_agents.df, market_last_year_df = diffusion_functions.calc_diffusion_solar(solar_agents.df, is_first_year, scenario_settings.get_bass_params())

                    # Estimate total generation
                    solar_agents.on_frame(agent_mutation.elec.estimate_total_generation)

                    # Aggregate results
                    scenario_settings.output_batt_dispatch_profiles = False
                    if is_first_year==True:
                        interyear_results_aggregations = agent_mutation.elec.aggregate_outputs_solar(solar_agents.df, year, is_first_year, scenario_settings) 
                    else:
                        interyear_results_aggregations = agent_mutation.elec.aggregate_outputs_solar(solar_agents.df, year, is_first_year, scenario_settings, interyear_results_aggregations)

                    # --- Check to ensure that agent_df isn't growing (i.e. merges are failing silently) --- 
                    df_print = solar_agents.df.copy()
                    df_print = df_print.loc[df_print['year']==year]
                    df_print = df_print.groupby(['sector_abbr'])['pv_kw_cum'].sum()
                    
                    #==========================================================================================================
                    # WRITE AGENT DF AS PICKLES FOR POST-PROCESSING
                    #==========================================================================================================
                    
                    # Write Outputs to the database
                    drop_fields = ['consumption_hourly_initial','bill_savings', 'consumption_hourly', 'solar_cf_profile', 'tariff_dict', 'deprec_sch', 'batt_dispatch_profile'] #dropping because are arrays or json
                    df_write = solar_agents.df.drop(drop_fields, axis=1)
                    
                    write_annual = False
                    if write_annual:
                        df_write.to_pickle(scenario_settings.out_scen_path + '/agent_df_%s.pkl' % year)
                        
                    if i == 0:
                        complete_df = df_write
                    else:
                        complete_df = pd.concat([complete_df, df_write], sort=False)

            #==============================================================================
            #    Outputs & Visualization
            #==============================================================================
            logger.info("---------Saving Model Results---------")
            
            complete_df.to_csv(scenario_settings.out_scen_path + '/agent_outputs.csv' )
            
            logger.info("-------------Model Run Complete-------------")
            logger.info('Completed in: %.1f seconds' % (time.time() - model_settings.model_init))


    except Exception as e:
        if 'logger' in locals():
            logger.error(e.__str__(), exc_info = True)
            logger.info('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            logger.info('Type of error {}'.format(type(e)))
            logger.info('Error Text: {}'.format(e))
        if 'logger' not in locals():
            raise
    finally:
        if 'logger' in locals():
            utilfunc.shutdown_log(logger)
            utilfunc.code_profiler(model_settings.out_dir)

if __name__ == '__main__':
    """Constructor for dgen_model.main()"""
    main()