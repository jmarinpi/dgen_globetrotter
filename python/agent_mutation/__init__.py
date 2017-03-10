import elec
import agent_preparation


def init_solar_agents(model_settings, scenario_settings, cur, con):
    # Prepare core agent attributes
    if model_settings.mode in ['run', 'setup_develop']:
        techs = scenario_settings.techs
        schema = scenario_settings.schema
        sample_pct = model_settings.sample_pct
        min_agents = model_settings.min_agents
        agents_per_region = model_settings.agents_per_region
        sectors = scenario_settings.sectors
        pg_procs = model_settings.pg_procs
        pg_conn_string = model_settings.pg_conn_string
        random_generator_seed = scenario_settings.random_generator_seed
        end_year = scenario_settings.end_year
        agent_prep_args = (techs, schema, sample_pct, min_agents,
                           agents_per_region, sectors, pg_procs,
                           pg_conn_string, random_generator_seed,
                           end_year)
        agent_preparation.elec.generate_core_agent_attributes(cur, con,
                                                              *agent_prep_args)

    # Create core agent attributes
    agents_df = elec.get_core_agent_attributes(con, schema,
                                               model_settings.mode,
                                               scenario_settings.region)
    # =========================================================================
    # GET NORMALIZED LOAD PROFILES
    # =========================================================================
    norm_load_profiles_df = elec.get_normalized_load_profiles(con, schema, sectors,
                                                              model_settings.mode)
    agents_df = elec.apply_normalized_load_profiles(agents_df, norm_load_profiles_df)

    # Add depolyable agents
    agents_df = elec.calculate_developable_customers_and_load_storage(agents_df)

    state_starting_capacities_df = elec.get_state_starting_capacities(con, schema)
    agents_df = elec.estimate_initial_market_shares_storage(agents_df,
                                                            state_starting_capacities_df)

    # =========================================================================
    # RESOURCE DATA
    # =========================================================================
    # get hourly resource
    solar_resource_df = elec.get_normalized_hourly_resource_solar(con, schema, sectors, techs)
    agents_df = elec.apply_solar_capacity_factor_profile(agents_df, solar_resource_df)

    # =========================================================================
    # EXPORTED GENERATION (NET METERING) SIZE LIMITS AND CREDIT
    # =========================================================================
    # get net metering system size limits and export prices
    year = scenario_settings.model_years[0]
    net_metering_df = elec.get_net_metering_settings(con, schema, year)
    # apply export generation tariff parameters
    agents_df = elec.apply_export_generation_tariffs(agents_df, net_metering_df)

    # =========================================================
    # GET RATE RANKS & TARIFF LOOKUP TABLE FOR EACH SECTOR
    # =========================================================
    # get (ranked) rates for each sector
    rates_rank_df =  elec.get_electric_rates(cur, con, scenario_settings.schema, scenario_settings.sectors, scenario_settings.random_generator_seed, model_settings.pg_conn_string, model_settings.mode)
    # find the list of unique rate ids that are included in rates_rank_df
    selected_rate_ids =  elec.identify_selected_rate_ids(rates_rank_df)
    # get lkup table with rate jsons
    rates_json_df =  elec.get_electric_rates_json(con, selected_rate_ids)
    rates_json_df = rates_json_df.set_index('rate_id_alias')

    # check that every agent has a tariff, assign one to them if they don't
    rates_rank_df = elec.check_rate_coverage(agents_df, rates_rank_df, rates_json_df)

    # =========================================================================
    # AGENT TARIFF SELECTION
    # =========================================================================
    agents_df = elec.select_tariff_driver(agents_df, rates_rank_df, rates_json_df, n_workers=model_settings.local_cores)

    return agents_df
