CREATE TABLE diffusion_shared."nem_scenario_bau_2017"
(
    state_abbr char(2),
    sector_abbr char(3),
    system_size_limit_kw double precision,
    system_size_limit_pct_annual_load double precision,
    rec_ownership char(25),
    first_year int
    sunset_year int
);

CREATE TABLE diffusion_shared."nem_state_limits_2017"
(
    state_abbr char(2),
    max_cum_capacity_mw int,
    max_pct_cum_capacity double precision,
    reference_year varchar(25)
);

CREATE TABLE diffusion_shared."state_interconnection_grades"
(
    year int,
    state_abbr char(2),
    grade char(2)
);

CREATE TABLE diffusion_shared."state_nem_grades"
(
    year int,
    state_abbr char(2),
    grade char(2)
);

CREATE TABLE diffusion_template."input_main_nem_user_defined_res"
(
    state_abbr char(2),
    system_size_limit_kw double precision,
    state_max_capactity_mw double precision,
    year_end_excess_sell_rate_d_p_kwh double precision,
    hourly_excess_sell_rate_d_p_kwh double precision,
    first_year int,
    sunset_year int
);

CREATE TABLE diffusion_template."input_main_nem_user_defined_com"
(
    state_abbr char(2),
    system_size_limit_kw double precision,
    state_max_capactity_mw double precision,
    year_end_excess_sell_rate_d_p_kwh double precision,
    hourly_excess_sell_rate_d_p_kwh double precision,
    first_year int,
    sunset_year int
);

CREATE TABLE diffusion_template."input_main_nem_user_defined_ind"
(
    state_abbr char(2),
    system_size_limit_kw double precision,
    state_max_capactity_mw double precision,
    year_end_excess_sell_rate_d_p_kwh double precision,
    hourly_excess_sell_rate_d_p_kwh double precision,
    first_year int,
    sunset_year int
);

ALTER TABLE diffusion_shared."nem_scenario_bau_2017" OWNER TO "diffusion-writers";
ALTER TABLE diffusion_shared."nem_state_limits_2017" OWNER TO "diffusion-writers";
ALTER TABLE diffusion_shared."state_interconnection_grades" OWNER TO "diffusion-writers";
ALTER TABLE diffusion_shared."state_nem_grades" OWNER TO "diffusion-writers";
ALTER TABLE diffusion_template."input_main_nem_user_defined_res" OWNER TO "diffusion-writers";
ALTER TABLE diffusion_template."input_main_nem_user_defined_com" OWNER TO "diffusion-writers";
ALTER TABLE diffusion_template."input_main_nem_user_defined_ind" OWNER TO "diffusion-writers";


psql -c "\copy diffusion_shared.nem_scenario_bau_2017 FROM '~/Desktop/nem_2017.csv' delimiter ',' csv" -h atlas.nrel.gov -d dgen_db_fy17q2_merge
psql -c "\copy diffusion_shared.nem_state_limits_2017 FROM '~/Desktop/nem_state_limits_2017.csv' delimiter ',' csv" -h atlas.nrel.gov -d dgen_db_fy17q2_merge
psql -c "\copy diffusion_shared.state_nem_grades FROM '~/Desktop/NMGrades.csv' delimiter ',' csv" -h atlas.nrel.gov -d dgen_db_fy17q2_merge
psql -c "\copy diffusion_shared.state_interconnection_grades FROM '~/Desktop/ICGrades.csv' delimiter ',' csv" -h atlas.nrel.gov -d dgen_db_fy17q2_merge
