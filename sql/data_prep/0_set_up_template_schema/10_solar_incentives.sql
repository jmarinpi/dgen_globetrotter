set role 'diffusion-writers';

DROP TABLE IF EXISTs diffusion_template.input_solar_incentive_options;
CREATE TABLE diffusion_template.input_solar_incentive_options
(
	overwrite_exist_inc boolean not null,
	incentive_start_year integer not null,
	CONSTRAINT input_solar_incentive_options_year_fkey FOREIGN KEY (incentive_start_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_solar_incentive_utility_types CASCADE;
CREATE TABLE diffusion_template.input_solar_incentive_utility_types 
(
	utility_type_iou boolean NOT NULL,
	utility_type_muni boolean NOT NULL,
	utility_type_coop boolean NOT NULL,
	utility_type_allother boolean NOT NULL
);


DROP TABLE IF EXISTS diffusion_template.input_solar_incentives_raw CASCADE;
CREATE TABLE diffusion_template.input_solar_incentives_raw
(
	region character varying(2),
	
	tax_res_incentive,
	tax_res_cap,
	tax_res_expire,
	tax_com_incentive,
	tax_com_cap,
	tax_com_expire,	
	tax_ind_incentive,
	tax_ind_cap,
	tax_ind_expire,	
	
	production_res_incentives_c_per_kwh,
	production_res_no_years,
	production_res_expire,
	production_com_incentives_c_per_kwh,
	production_com_no_years,
	production_com_expire,
	production_ind_incentives_c_per_kwh,
	production_ind_no_years,
	production_ind_expire,

	rebate_res_dol_per_w,
	rebate_res_cap,
	rebate_res_expire,
	rebate_com_dol_per_w,
	rebate_com_cap,
	rebate_com_expire,
	rebate_ind_dol_per_w,
	rebate_ind_cap,
	rebate_ind_expire

);
