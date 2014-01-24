CREATE TABLE "annual_average_rates" (
"county_id" int4 NOT NULL,
"cust_id" int4 NOT NULL,
"rate" numeric,
CONSTRAINT "annual_average_rates_pkey" PRIMARY KEY ("county_id", "cust_id") 
);

CREATE TABLE "county_geom" (
"county_id" int4 NOT NULL,
"county" text COLLATE "default",
"state" text COLLATE "default",
"state_abbr" varchar(2) COLLATE "default",
"the_geom_4326" "public"."geometry",
CONSTRAINT "county_geom_pkey" PRIMARY KEY ("county_id") 
);

CREATE TABLE "customer_segments" (
"cust_id" int4 NOT NULL,
"cust_seg" text COLLATE "default",
CONSTRAINT "customer_segments_pkey" PRIMARY KEY ("cust_id") 
);

CREATE TABLE "depreciation_schedule" (
"year" int4,
"macrs" numeric,
"standard" numeric
);

CREATE TABLE "dsire_incentives" (
);

CREATE TABLE "financial_parameters" (
"cust_id" int4,
"cust_disposition" text COLLATE "default",
"loan_term_yrs" int4,
"loan_rate" numeric,
"down_payment" numeric,
"discount_rate" numeric,
"tax_rate" numeric,
"length_of_irr_analysis_yrs" int4
);

CREATE TABLE "market_projections" (
"year" int4,
"nat_gas_dollars_per_btu" numeric,
"coal_price_dollars_per_btu" numeric,
"carbon_dollars_per_ton" numeric
);

CREATE TABLE "normalized_wind_power_curves" (
"power_curve_id" int4,
"wind_speed" int4,
"norm_power_kwh_per_kw" numeric
);

CREATE TABLE "scenario_options" (
"region" text COLLATE "default",
"end_year" int4,
"markets" text COLLATE "default",
"cust_exp_elec_rates" text COLLATE "default",
"res_rate_structure" text COLLATE "default",
"res_rate_escalation" text COLLATE "default",
"res_max_market_curve" text COLLATE "default",
"com_rate_structure" text COLLATE "default",
"com_rate_escalation" text COLLATE "default",
"com_max_market_curve" text COLLATE "default",
"net_metering_availability" text COLLATE "default",
"carbon_price" text COLLATE "default",
"ann_inflation" numeric,
"scenario_name" text COLLATE "default"
);

CREATE TABLE "tou_rates" (
"county_id" int4,
"cust_id" int4,
"hour_of_day" int4,
"season" text COLLATE "default",
"rate" numeric
);

CREATE TABLE "wind_cost_projections" (
"year" int4,
"capital_cost_dollars_per_kw" numeric,
"fixed_om_dollars_per_kw_per_yr" numeric,
"variable_om_dollars_per_kwh" numeric,
"default_tower_height_m" numeric,
"cost_for_higher_towers" numeric
);

CREATE TABLE "wind_performance_improvements" (
"year" int4,
"nameplate_capacity_kw" int4,
"power_curve_id" int4
);

CREATE TABLE "wind_resource" (
"county_id" int4,
"cust_id" int4,
"resource_bin" int4,
"hour_of_year" int4,
"generation" numeric
);

CREATE TABLE "sceninp_carbon_price" (
"carbon_price" text COLLATE "default",
CONSTRAINT "z_sceninp_carbon_price_carbon_price_key" UNIQUE ("carbon_price")
);

CREATE TABLE "sceninp_com_max_market_curve" (
"com_max_market_curve" text COLLATE "default",
CONSTRAINT "z_sceninp_com_max_market_curve_com_max_market_curve_key" UNIQUE ("com_max_market_curve")
);

CREATE TABLE "sceninp_com_rate_escalation" (
"com_rate_escalation" text COLLATE "default",
CONSTRAINT "z_sceninp_com_rate_escalation_com_rate_escalation_key" UNIQUE ("com_rate_escalation")
);

CREATE TABLE "sceninp_com_rate_structure" (
"com_rate_structure" text COLLATE "default",
CONSTRAINT "z_sceninp_com_rate_structure_com_rate_structure_key" UNIQUE ("com_rate_structure")
);

CREATE TABLE "sceninp_cust_exp_elec_rates" (
"cust_exp_elec_rates" text COLLATE "default",
CONSTRAINT "z_sceninp_cust_exp_elec_rates_cust_exp_elec_rates_key" UNIQUE ("cust_exp_elec_rates")
);

CREATE TABLE "sceninp_end_year" (
"end_year" int4,
CONSTRAINT "z_sceninp_end_year_end_year_key" UNIQUE ("end_year")
);

CREATE TABLE "sceninp_markets" (
"markets" text COLLATE "default",
CONSTRAINT "z_sceninp_markets_markets_key" UNIQUE ("markets")
);

CREATE TABLE "sceninp_net_metering_availability" (
"net_metering_availability" text COLLATE "default",
CONSTRAINT "z_sceninp_net_metering_availabili_net_metering_availability_key" UNIQUE ("net_metering_availability")
);

CREATE TABLE "sceninp_region" (
"region" text COLLATE "default",
CONSTRAINT "z_sceninp_region_region_key" UNIQUE ("region")
);

CREATE TABLE "sceninp_res_max_market_curve" (
"res_max_market_curve" text COLLATE "default",
CONSTRAINT "z_sceninp_res_max_market_curve_res_max_market_curve_key" UNIQUE ("res_max_market_curve")
);

CREATE TABLE "sceninp_res_rate_escalation" (
"res_rate_escalation" text COLLATE "default",
CONSTRAINT "z_sceninp_res_rate_escalation_res_rate_escalation_key" UNIQUE ("res_rate_escalation")
);

CREATE TABLE "sceninp_res_rate_structure" (
"res_rate_structure" text COLLATE "default",
CONSTRAINT "z_sceninp_res_rate_structure_res_rate_structure_key" UNIQUE ("res_rate_structure")
);

CREATE TABLE "wind_resource_aep_20km" (
"i" int NOT NULL,
"j" int NOT NULL,
"cf_bin" int,
"turbine_height_m" float8,
"turbine_id" int,
"aep" float
);

CREATE TABLE "wind_resource_profile_20km" (
"i" int NOT NULL,
"j" int NOT NULL,
"cf_bin" int,
"turbine_id" int NOT NULL,
"turbine_height_m" int,
"hour" int NOT NULL,
"kwh" float
);

CREATE TABLE "grid_200m" (
"gid" int NOT NULL,
"x" float,
"y" float,
"the_geom" geometry,
"i" int,
"j" int,
"cf_bin" int,
"max_turbine_height_m" int,
"county" text,
"state" text,
"state_abbr" varbit(2),
PRIMARY KEY ("gid") 
);

CREATE TABLE "annual_load_residential" (
"grid_gid" int NOT NULL,
"annual_load_mw" float,
PRIMARY KEY ("grid_gid") 
);

CREATE TABLE "annual_load_commercial" (
"grid_gid" int NOT NULL,
"annual_load_mw" float,
PRIMARY KEY ("grid_gid") 
);

CREATE TABLE "annual_load_manufacturing" (
"grid_gid" int NOT NULL,
"annual_load_mw" float,
PRIMARY KEY ("grid_gid") 
);

CREATE TABLE "annual_load_otherindustrial" (
"grid_gid" int NOT NULL,
"annual_load_mw" float,
PRIMARY KEY ("grid_gid") 
);

CREATE TABLE "rate_annual_average_residential" (
"grid_gid" int NOT NULL,
"rate_dlrs_per_kwh" float,
PRIMARY KEY ("grid_gid") 
);

CREATE TABLE "rate_annual_average_commercial" (
"grid_gid" int NOT NULL,
"rate_dlrs_per_kwh" float,
PRIMARY KEY ("grid_gid") 
);

CREATE TABLE "rate_annual_average_otherindustrial" (
"grid_gid" int NOT NULL,
"rate_dlrs_per_kwh" float,
PRIMARY KEY ("grid_gid") 
);

CREATE TABLE "rate_annual_average_manufacturing" (
"grid_gid" int NOT NULL,
"rate_dlrs_per_kwh" float,
PRIMARY KEY ("grid_gid") 
);

CREATE TABLE "turbine_power_curve_ids" (
"turbine_id" int4 NOT NULL DEFAULT nextval('windpy.turbine_power_curve_ids_turbine_id_seq'::regclass),
"turbine_name" text COLLATE "default" NOT NULL,
CONSTRAINT "turbine_power_curve_ids_pkey" PRIMARY KEY ("turbine_id") ,
CONSTRAINT "turbine_power_curve_ids_turbine_name_key" UNIQUE ("turbine_name")
);

CREATE TABLE "turbine_power_curves" (
"turbine_name" text COLLATE "default",
"windspeed_ms" numeric,
"generation_kw" numeric
);

CREATE INDEX "turbine_power_curves_turbine_name_btree" ON "turbine_power_curves" ("turbine_name" ASC);


ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_carbon_price_fkey" FOREIGN KEY ("carbon_price") REFERENCES "sceninp_carbon_price" ("carbon_price");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_com_max_market_curve_fkey" FOREIGN KEY ("com_max_market_curve") REFERENCES "sceninp_com_max_market_curve" ("com_max_market_curve");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_com_rate_escalation_fkey" FOREIGN KEY ("com_rate_escalation") REFERENCES "sceninp_com_rate_escalation" ("com_rate_escalation");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_com_rate_structure_fkey" FOREIGN KEY ("com_rate_structure") REFERENCES "sceninp_com_rate_structure" ("com_rate_structure");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_cust_exp_elec_rates_fkey" FOREIGN KEY ("cust_exp_elec_rates") REFERENCES "sceninp_cust_exp_elec_rates" ("cust_exp_elec_rates");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_end_year_fkey" FOREIGN KEY ("end_year") REFERENCES "sceninp_end_year" ("end_year");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_markets_fkey" FOREIGN KEY ("markets") REFERENCES "sceninp_markets" ("markets");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_net_metering_availability_fkey" FOREIGN KEY ("net_metering_availability") REFERENCES "sceninp_net_metering_availability" ("net_metering_availability");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_region_fkey" FOREIGN KEY ("region") REFERENCES "sceninp_region" ("region");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_res_max_market_curve_fkey" FOREIGN KEY ("res_max_market_curve") REFERENCES "sceninp_res_max_market_curve" ("res_max_market_curve");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_res_rate_escalation_fkey" FOREIGN KEY ("res_rate_escalation") REFERENCES "sceninp_res_rate_escalation" ("res_rate_escalation");
ALTER TABLE "scenario_options" ADD CONSTRAINT "scenario_options_res_rate_structure_fkey" FOREIGN KEY ("res_rate_structure") REFERENCES "sceninp_res_rate_structure" ("res_rate_structure");
ALTER TABLE "wind_resource_aep_20km" ADD CONSTRAINT "wind_resource_aep_turbine_id_fkey" FOREIGN KEY ("turbine_id") REFERENCES "turbine_power_curve_ids" ("turbine_id");
ALTER TABLE "wind_resource_profile_20km" ADD CONSTRAINT "wind_resource_profile_turbine_id_fkey" FOREIGN KEY ("turbine_id") REFERENCES "turbine_power_curve_ids" ("turbine_id");
ALTER TABLE "annual_load_commercial" ADD CONSTRAINT "annual_load_commercial_grid_gid_fkey" FOREIGN KEY ("grid_gid") REFERENCES "grid_200m" ("gid");
ALTER TABLE "annual_load_residential" ADD CONSTRAINT "annual_load_residential_grid_gid_fkey" FOREIGN KEY ("grid_gid") REFERENCES "grid_200m" ("gid");
ALTER TABLE "annual_load_otherindustrial" ADD CONSTRAINT "annual_load_otherindustrial_grid_gid_fkey" FOREIGN KEY ("grid_gid") REFERENCES "grid_200m" ("gid");
ALTER TABLE "annual_load_manufacturing" ADD CONSTRAINT "annual_load_manufacturing_grid_gid_fkey" FOREIGN KEY ("grid_gid") REFERENCES "grid_200m" ("gid");
ALTER TABLE "rate_annual_average_residential" ADD CONSTRAINT "rate_annual_average_residential_grid_gid_fkey" FOREIGN KEY ("grid_gid") REFERENCES "grid_200m" ("gid");
ALTER TABLE "rate_annual_average_commercial" ADD CONSTRAINT "rate_annual_average_commercial_grid_gid_fkey" FOREIGN KEY ("grid_gid") REFERENCES "grid_200m" ("gid");
ALTER TABLE "rate_annual_average_otherindustrial" ADD CONSTRAINT "rate_annual_average_otherindustrial_grid_gid_fkey" FOREIGN KEY ("grid_gid") REFERENCES "grid_200m" ("gid");
ALTER TABLE "rate_annual_average_manufacturing" ADD CONSTRAINT "rate_annual_average_manufacturing_grid_gid_fkey" FOREIGN KEY ("grid_gid") REFERENCES "grid_200m" ("gid");
ALTER TABLE "turbine_power_curves" ADD CONSTRAINT "turbine_power_curves_turbine_name_fkey" FOREIGN KEY ("turbine_name") REFERENCES "turbine_power_curve_ids" ("turbine_name");

