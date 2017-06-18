"""
Distributed Generation Market Demand Model (dGen)
National Renewable Energy Lab
"""
#test
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
import utility_functions as utilfunc
from agents import Agents, Solar_Agents
import settings
import agent_mutation
import agent_preparation
import tariff_building_functions as tBuildFuncs
#import demand_supply_geo
import diffusion_functions_elec
import diffusion_functions_ghp
import diffusion_functions_du
import diffusion_functions_geo
import financial_functions_elec
import financial_functions_geo
import input_data_functions as iFuncs

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
        # =====================================================================
        # SET UP THE MODEL TO RUN
        # =====================================================================
        # initialize Model Settings object
        # (this controls settings that apply to all scenarios to be executed)
        model_settings = settings.init_model_settings()

        # make output directory
        os.makedirs(model_settings.out_dir)
        # create the logger and stamp with git hash
        logger = utilfunc.get_logger(os.path.join(model_settings.out_dir, 'dg_model.log'))
        logger.info("Model version is git commit {:}".format(model_settings.git_hash))

        # connect to Postgres and configure connection
        con, cur = utilfunc.make_con(model_settings.pg_conn_string, model_settings.role)
        engine = utilfunc.make_engine(model_settings.pg_engine_string)
        pgx.register_hstore(con)  # register access to hstore in postgres
        logger.info("Connected to Postgres with the following params:\n{:}".format(model_settings.pg_params_log))

        # =====================================================================
        # LOOP OVER SCENARIOS
        # =====================================================================
        # variables used to track outputs
        scenario_names = []
        dup_n = 1
        out_subfolders = {'wind': [], 'solar': [], 'ghp': [], 'du': []}
        for i, scenario_file in enumerate(model_settings.input_scenarios):
            logger.info('============================================')
            logger.info('============================================')
            logger.info("Running Scenario {i} of {n}".format(i=i + 1,n=len(model_settings.input_scenarios)))
            # initialize ScenarioSettings object
            # (this controls settings that apply only to this specific scenario)
            scenario_settings = settings.init_scenario_settings(scenario_file, model_settings, con, cur)
            scenario_settings.input_data_dir = model_settings.input_data_dir

            # summarize high level secenario settings
            datfunc.summarize_scenario(scenario_settings, model_settings)

            # create output folder for this scenario
            input_scenario = scenario_settings.input_scenario
            scen_name = scenario_settings.scen_name
            out_dir = model_settings.out_dir
            (out_scen_path, scenario_names, dup_n) = datfunc.create_scenario_results_folder(input_scenario, scen_name,
                                                             scenario_names, out_dir, dup_n)
                                                             
            # create folder for input data csvs for this scenario
            scenario_settings.dir_to_write_input_data = out_scen_path + '/input_data'
            os.makedirs(scenario_settings.dir_to_write_input_data)
                                                             
            # get other datasets needed for the model run
            logger.info('Getting various scenario parameters')

            schema = scenario_settings.schema
            max_market_share = datfunc.get_max_market_share(con, schema)
            market_projections = datfunc.get_market_projections(con, schema)
            load_growth_scenario = scenario_settings.load_growth.lower()
            inflation_rate = datfunc.get_annual_inflation(con, scenario_settings.schema)
            bass_params = datfunc.get_bass_params(con, scenario_settings.schema)

            # get settings whether to use pre-generated agent file ('User Defined'- provide pkl file name) or generate new agents
            agent_file_status = scenario_settings.agent_file_status

            # create psuedo-rangom number generator (not used until tech/finance choice function)
            prng = np.random.RandomState(scenario_settings.random_generator_seed)

            #==========================================================================================================
            # CREATE AGENTS
            #==========================================================================================================
            logger.info("--------------Creating Agents---------------")
            
            if scenario_settings.techs in [['wind'], ['solar']]:

                # =========================================================
                # Initialize agents
                # =========================================================   
             
                # Depending on settings either generate new agents or use pre-generated agents from provided .pkl file                
                solar_agents = iFuncs.import_agent_file( scenario_settings, con, cur, engine, model_settings, agent_file_status, input_name='agent_file')            
                    
                # Write base agents to disk
                solar_agents.df.to_pickle(out_scen_path + '/agent_df_base.pkl')
                
                # Get set of columns that define agent's immutable attributes
                cols_base = list(solar_agents.df.columns)

            elif scenario_settings.techs in [['ghp'], ['du']]:
                logger.error("GHP and DU not yet supported")
                break
                # TODO: agents_df =  agent_mutation.geo.get_core_agent_attributes(con, scenario_settings.schema, model_settings.mode, scenario_settings.region)
                # TODO: agents = Agents(agents_df)

            #==============================================================================
            # TECHNOLOGY DEPLOYMENT
            #==============================================================================

            if scenario_settings.techs == ['solar']:
                # get dsire incentives, srecs, and itc inputs
                # TODO: move these to agent mutation

                state_incentives = datfunc.get_state_incentives(con)
                itc_options = datfunc.get_itc_incentives(con, scenario_settings.schema)
                nem_state_capacity_limits = datfunc.get_nem_state(con, scenario_settings.schema)
                nem_state_and_sector_attributes = datfunc.get_nem_state_by_sector(con, scenario_settings.schema)
                nem_selected_scenario = datfunc.get_selected_scenario(con, scenario_settings.schema)

                #==========================================================================================================
                # INGEST SCENARIO ENVIRONMENTAL VARIABLES
                #==========================================================================================================
                deprec_sch = iFuncs.import_table( scenario_settings, con, engine, model_settings.role, input_name ='depreciation_schedules', csv_import_function=iFuncs.deprec_schedule)
                carbon_intensities = iFuncs.import_table( scenario_settings, con, engine,model_settings.role, input_name='carbon_intensities', csv_import_function=iFuncs.melt_year('grid_carbon_tco2_per_kwh'))
                wholesale_elec_prices = iFuncs.import_table( scenario_settings, con, engine, model_settings.role, input_name='wholesale_electricity_prices', csv_import_function=iFuncs.melt_year('wholesale_elec_price'))
                pv_tech_traj = iFuncs.import_table( scenario_settings, con, engine, model_settings.role,input_name='pv_tech_performance', csv_import_function=iFuncs.stacked_sectors)
                elec_price_change_traj = iFuncs.import_table( scenario_settings, con, engine, model_settings.role,input_name='elec_prices', csv_import_function=iFuncs.process_elec_price_trajectories)
                load_growth = iFuncs.import_table( scenario_settings, con, engine, model_settings.role,input_name='load_growth', csv_import_function=iFuncs.process_load_growth)
                pv_price_traj = iFuncs.import_table( scenario_settings, con, engine, model_settings.role,input_name='pv_prices', csv_import_function=iFuncs.stacked_sectors)
                batt_price_traj = iFuncs.import_table( scenario_settings, con, engine,model_settings.role, input_name='batt_prices', csv_import_function=iFuncs.stacked_sectors)
                financing_terms = iFuncs.import_table( scenario_settings, con, engine, model_settings.role,input_name='financing_terms', csv_import_function=iFuncs.stacked_sectors)
                batt_tech_traj = iFuncs.import_table( scenario_settings, con, engine, model_settings.role,input_name='batt_tech_performance', csv_import_function=iFuncs.stacked_sectors)

                #==========================================================================================================
                # Calculate Tariff Components from ReEDS data
                #==========================================================================================================
                input_dir = os.path.join(os.getcwd(), '..', 'reeds_data_for_tariff_construction')
                scenario = 'ThreeCents'
                start_year = 2018
                end_year = 2050
                base_year = 2016
                pv_kw_cum_last_sy_df = pd.DataFrame()

                rto_df, total_cost_smoothed_df, cap_frac_smoothed_df, ts_df_rto, ts_map = tBuildFuncs.calc_revenue_fracs_from_reeds_data(solar_agents.df, input_dir, scenario, start_year, end_year, base_year)


                for year in scenario_settings.model_years:

                    logger.info('\tWorking on %s' % year)

                    # determine any non-base-year columns and drop them
                    cols = list(solar_agents.df.columns)
                    cols_to_drop = [x for x in cols if x not in cols_base]
                    solar_agents.df.drop(cols_to_drop, axis=1, inplace=True)

                    # copy the core agent object and set their year
                    solar_agents.df['year'] = year

                    # is it the first model year?
                    is_first_year = year == model_settings.start_year

                    # get and apply load growth
                    solar_agents.on_frame(agent_mutation.elec.apply_load_growth, (load_growth))

                    #Apply net metering parameters
                    net_metering_df = agent_mutation.elec.get_nem_settings(nem_state_capacity_limits, nem_state_and_sector_attributes, nem_selected_scenario, year)
                    solar_agents.on_frame(agent_mutation.elec.apply_export_tariff_params, (net_metering_df))

                    # Apply each agent's electricity price change and assumption about increases
                    solar_agents.on_frame(agent_mutation.elec.apply_elec_price_multiplier_and_escalator, [year, elec_price_change_traj])

                    # Apply technology performance
                    solar_agents.on_frame(agent_mutation.elec.apply_batt_tech_performance, (batt_tech_traj))
                    solar_agents.on_frame(agent_mutation.elec.apply_pv_tech_performance, pv_tech_traj)

                    # Apply technology prices
                    solar_agents.on_frame(agent_mutation.elec.apply_pv_prices, pv_price_traj)
                    solar_agents.on_frame(agent_mutation.elec.apply_batt_prices, [batt_price_traj, batt_tech_traj, year])

                    # Apply depreciation schedule
                    solar_agents.on_frame(agent_mutation.elec.apply_depreciation_schedule, deprec_sch)

                    # Apply carbon intensities
                    solar_agents.on_frame(agent_mutation.elec.apply_carbon_intensities, carbon_intensities)

                    # Apply wholesale electricity prices
                    solar_agents.on_frame(agent_mutation.elec.apply_wholesale_elec_prices, wholesale_elec_prices)

                    # Apply host-owned financial parameters
                    solar_agents.on_frame(agent_mutation.elec.apply_financial_params, [financing_terms, itc_options, inflation_rate])

                    # Write ReEDS-derived tariff dicts to each agent
                    if year >= 2018:
                        solar_agents.df['pv_kw_cum_last_sy'] = pv_kw_cum_last_sy_df.copy()
                        solar_agents.df = tBuildFuncs.design_tariff_components(solar_agents.df, year, rto_df, total_cost_smoothed_df, cap_frac_smoothed_df, ts_df_rto, base_year, ts_map)

                    # Apply state incentives
                    solar_agents.on_frame(agent_mutation.elec.apply_state_incentives, [state_incentives, year])

                    # Size S+S system and calculate electric bills
                    # if 'ix' not in os.name: cores=None
                    # else: cores=model_settings.local_cores
                    cores = None
                    solar_agents.on_row(sFuncs.calc_system_size_and_financial_performance, cores=cores)

                    # Calculate the financial performance of the S+S systems
                    solar_agents.on_frame(financial_functions_elec.calc_financial_performance)

                    # Calculate Maximum Market Share
                    solar_agents.on_frame(financial_functions_elec.calc_max_market_share, max_market_share)

                    # determine "developable" population
                    solar_agents.on_frame(agent_mutation.elec.calculate_developable_customers_and_load)

                    # Apply market_last_year
                    if is_first_year == True:
                        state_starting_capacities_df = agent_mutation.elec.get_state_starting_capacities(con, schema)
                        solar_agents.on_frame(agent_mutation.elec.estimate_initial_market_shares, state_starting_capacities_df)
                        market_last_year_df = None
                    else:
                        solar_agents.on_frame(agent_mutation.elec.apply_market_last_year, market_last_year_df)

                    # Calculate diffusion based on economics and bass diffusion
                    solar_agents.df, market_last_year_df = diffusion_functions_elec.calc_diffusion_solar(solar_agents.df, is_first_year, bass_params)

                    # Estimate total generation
                    solar_agents.on_frame(agent_mutation.elec.estimate_total_generation)

                    # Aggregate results
                    scenario_settings.output_batt_dispatch_profiles = True
                    if is_first_year==True:
                        interyear_results_aggregations = datfunc.aggregate_outputs_solar(solar_agents.df, year, is_first_year,
                                                                                         scenario_settings, out_scen_path)
                    else:
                        interyear_results_aggregations = datfunc.aggregate_outputs_solar(solar_agents.df, year, is_first_year,
                                                                                         scenario_settings, out_scen_path,
                                                                                         interyear_results_aggregations)

                    pv_kw_cum_last_sy_df = solar_agents.df['pv_kw_cum'].copy()


                    #==========================================================================================================
                    # WRITE AGENT DF AS PICKLES FOR POST-PROCESSING
                    #==========================================================================================================
                    write_annual_agents = True
                    if write_annual_agents==True:
                        solar_agents.df.drop(['consumption_hourly', 'solar_cf_profile', 'tariff_dict', 'deprec_sch', 'batt_dispatch_profile'], axis=1).to_pickle(out_scen_path + '/agent_df_%s.pkl' % year)

                    # Write Outputs to the database
                    datfunc.write_outputs(con, cur, solar_agents.df, scenario_settings.sectors, scenario_settings.schema)

            elif scenario_settings.techs == ['wind']:
                logger.error('Wind not yet supported')
                break
            elif scenario_settings.techs == ['ghp']:
                logger.error('GHP not yet supported')
                break
            elif scenario_settings.techs == ['du']:
                logger.error('GHP not yet supported')
                break
            #==============================================================================
            #    Outputs & Visualization
            #==============================================================================
            logger.info("---------Saving Model Results---------")
            out_subfolders = datfunc.create_tech_subfolders(out_scen_path, scenario_settings.techs, out_subfolders)

            # copy outputs to csv
            datfunc.copy_outputs_to_csv(scenario_settings.techs, scenario_settings.schema, out_scen_path, cur, con)

            # add indices to postgres output table
            datfunc.index_output_table(con, cur, scenario_settings.schema)

            # create output html report
            datfunc.create_scenario_report(scenario_settings.techs, scenario_settings.schema, scenario_settings.scen_name, out_scen_path, cur, con, model_settings.Rscript_path, model_settings.pg_params_file)

            #####################################################################
            # drop the new scenario_settings.schema
            #datfunc.drop_output_schema(model_settings.pg_conn_string, scenario_settings.schema, model_settings.delete_output_schema)
            #####################################################################

            logger.info("-------------Model Run Complete-------------")
            logger.info('Completed in: %.1f seconds' % (time.time() - model_settings.model_init))


    except Exception, e:
        # close the connection (need to do this before dropping schema or query will hang)
        if 'con' in locals():
            con.close()
        if 'logger' in locals():
            logger.error(e.__str__(), exc_info = True)
#        if 'scenario_settings' in locals() and scenario_settings.schema is not None:
            # drop the output schema
#            datfunc.drop_output_schema(model_settings.pg_conn_string, scenario_settings.schema, True)
        if 'logger' not in locals():
            raise


    finally:
        if 'con' in locals():
            con.close()
#        if 'scenario_settings' in locals() and scenario_settings.schema is not None:
            # drop the output schema
#            datfunc.drop_output_schema(model_settings.pg_conn_string, scenario_settings.schema, True)
        if 'logger' in locals():
            utilfunc.shutdown_log(logger)
            utilfunc.code_profiler(model_settings.out_dir)

if __name__ == '__main__':
    main()