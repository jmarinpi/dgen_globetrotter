﻿set role 'diffusion-writers';

DROP TABLE IF EXISTs diffusion_template.input_wind_incentive_options;
CREATE TABLE diffusion_template.input_wind_incentive_options
(
	overwrite_exist_inc boolean not null,
	incentive_start_year integer not null,
	CONSTRAINT input_wind_incentive_options_year_fkey FOREIGN KEY (incentive_start_year)
		REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE RESTRICT
);


DROP TABLE IF EXISTS diffusion_template.input_wind_incentive_utility_types CASCADE;
CREATE TABLE diffusion_template.input_wind_incentive_utility_types 
(
	utility_type_iou boolean NOT NULL,
	utility_type_muni boolean NOT NULL,
	utility_type_coop boolean NOT NULL,
	utility_type_allother boolean NOT NULL
);


DROP VIEW IF EXISTS diffusion_template.input_wind_incentive_utility_types_tidy;
CREATE VIEW diffusion_template.input_wind_incentive_utility_types_tidy AS
with a as
(
	select unnest(array[	case when utility_type_iou = True then 'IOU'
			END,
			case when utility_type_muni = True then 'Muni'
			END,
			case when utility_type_coop = True then 'Coop'
			END,
			case when utility_type_allother = True then 'All Other'
			ENd
		]) as utility_type
	from diffusion_template.input_wind_incentive_utility_types 
)
SELECT *
FROM a
where utility_type is not null;


DROP TABLE IF EXISTS diffusion_template.input_wind_incentives_raw CASCADE;
CREATE TABLE diffusion_template.input_wind_incentives_raw
(
	region character varying(2),
	
	tax_res_incentive numeric,
	tax_res_cap numeric,
	tax_res_expire integer,
	tax_com_incentive numeric,
	tax_com_cap numeric,
	tax_com_expire integer,	
	tax_ind_incentive numeric,
	tax_ind_cap numeric,
	tax_ind_expire integer,	
	
	production_res_incentives_c_per_kwh numeric,
	production_res_no_years integer,
	production_res_expire integer,
	production_com_incentives_c_per_kwh numeric,
	production_com_no_years integer,
	production_com_expire integer,
	production_ind_incentives_c_per_kwh numeric,
	production_ind_no_years integer,
	production_ind_expire integer,

	rebate_res_dol_per_w numeric,
	rebate_res_cap numeric,
	rebate_res_expire integer,
	rebate_com_dol_per_w numeric,
	rebate_com_cap numeric,
	rebate_com_expire integer,
	rebate_ind_dol_per_w numeric,
	rebate_ind_cap numeric,
	rebate_ind_expire integer

-- 	CONSTRAINT input_wind_cost_projections_com_year_fkey FOREIGN KEY (year)
-- 	REFERENCES diffusion_config.sceninp_year_range (val) MATCH SIMPLE
-- 	ON UPDATE NO ACTION ON DELETE RESTRICT
-- NOT NULL???

);


DROP VIEW IF EXISTS diffusion_template.input_wind_incentives_standardized;
CREATE VIEW diffusion_template.input_wind_incentives_standardized AS
-- tax incentives
select region, 
	'Tax'::text as type,
	'Residential'::text as sector,

	tax_res_incentive as incentive,
	tax_res_cap as cap,
	tax_res_expire as expire,
	0::numeric as incentives_c_per_kwh,
	0::numeric as no_years,
	0::numeric as dol_per_kw
FROM diffusion_template.input_wind_incentives_raw

UNION ALL

select region, 
	'Tax'::text as type,
	'Commercial'::text as sector,

	tax_com_incentive as incentive,
	tax_com_cap as cap,
	tax_com_expire as expire,
	0::numeric as incentives_c_per_kwh,
	0::numeric as no_years,
	0::numeric as dol_per_kw
FROM diffusion_template.input_wind_incentives_raw

UNION ALL

select region, 
	'Tax'::text as type,
	'Industrial'::text as sector,

	tax_ind_incentive as incentive,
	tax_ind_cap as cap,
	tax_ind_expire as expire,
	0::numeric as incentives_c_per_kwh,
	0::numeric as no_years,
	0::numeric as dol_per_kw
FROM diffusion_template.input_wind_incentives_raw

UNION ALL

-- production incentives
select region, 
	'Production'::text as type,
	'Residential'::text as sector,

	0::numeric as incentive,
	0::numeric as cap,
	production_res_expire as expire,
	production_res_incentives_c_per_kwh as incentives_c_per_kwh,
	production_res_no_years as no_years,
	0::numeric as dol_per_kw
FROM diffusion_template.input_wind_incentives_raw

UNION ALL

select region, 
	'Production'::text as type,
	'Commercial'::text as sector,

	0::numeric as incentive,
	0::numeric as cap,
	production_com_expire as expire,
	production_com_incentives_c_per_kwh as incentives_c_per_kwh,
	production_com_no_years as no_years,
	0::numeric as dol_per_kw
FROM diffusion_template.input_wind_incentives_raw

UNION ALL

select region, 
	'Production'::text as type,
	'Industrial'::text as sector,

	0::numeric as incentive,
	0::numeric as cap,
	production_ind_expire as expire,
	production_ind_incentives_c_per_kwh as incentives_c_per_kwh,
	production_ind_no_years as no_years,
	0::numeric as dol_per_kw
FROM diffusion_template.input_wind_incentives_raw

-- rebate incentives

UNION ALL

select region, 
	'Rebate'::text as type,
	'Residential'::text as sector,

	0::numeric as incentive,
	rebate_res_cap as cap,
	rebate_res_expire as expire,
	0::numeric as incentives_c_per_kwh,
	0::numeric as no_years,
	rebate_res_dol_per_w/1000. as dol_per_kw
FROM diffusion_template.input_wind_incentives_raw

UNION ALL

select region, 
	'Rebate'::text as type,
	'Commercial'::text as sector,

	0::numeric as incentive,
	rebate_com_cap as cap,
	rebate_com_expire as expire,
	0::numeric as incentives_c_per_kwh,
	0::numeric as no_years,
	rebate_com_dol_per_w/1000. as dol_per_kw
FROM diffusion_template.input_wind_incentives_raw

UNION ALL

select region, 
	'Rebate'::text as type,
	'Industrial'::text as sector,

	0::numeric as incentive,
	rebate_ind_cap as cap,
	rebate_ind_expire as expire,
	0::numeric as incentives_c_per_kwh,
	0::numeric as no_years,
	rebate_ind_dol_per_w/1000. as dol_per_kw
FROM diffusion_template.input_wind_incentives_raw;


DROP VIEW IF EXISTS diffusion_template.input_wind_incentives;
CREATE VIEW diffusion_template.input_wind_incentives AS
SELECT a.*, b.utility_type
FROM diffusion_template.input_wind_incentives_standardized a
CROSS JOIN diffusion_template.input_wind_incentive_utility_types_tidy b