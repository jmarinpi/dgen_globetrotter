set role 'diffusion-writers';


DROP TABLE IF EXISTS diffusion_template.input_main_inflation;
CREATE TABLE diffusion_template.input_main_inflation
(
	ann_inflation numeric NOT NULL
);


DROP TABLE if exists diffusion_template.input_main_market_projections;
CREATE TABLE diffusion_template.input_main_market_projections
(
	year integer,
	avoided_costs_dollars_per_kwh numeric,
	carbon_dollars_per_ton numeric,
	user_defined_res_rate_escalations numeric,
	user_defined_com_rate_escalations numeric,
	user_defined_ind_rate_escalations numeric,
	default_rate_escalations numeric
);

