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
    # TARIFFS FOR EXPORTED GENERATION (NET METERING)
    # =========================================================================
    # get net metering settings
    year = scenario_settings.model_years[0]
    # get net metering settings (core)
    net_metering_df = elec.get_net_metering_settings(con, schema, year)
    # apply export generation tariff settings
    agents_df = elec.apply_export_generation_tariffs(agents_df, net_metering_df)

    return agents_df
