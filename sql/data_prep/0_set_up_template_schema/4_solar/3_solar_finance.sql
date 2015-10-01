set role 'diffusion-writers';


DROP TABLE IF EXISTS diffusion_template.input_solar_finances_res cascade;
CREATE TABLE diffusion_template.input_solar_finances_res 
(
	ownership_model text NOT NULL,
	loan_term_yrs integer NOT NULL,
	loan_rate numeric NOT NULL,
	down_payment numeric NOT NULL,
	discount_rate numeric NOT NULL,
	tax_rate numeric NOT NULL
);

DROP TABLE IF EXISTS diffusion_template.input_solar_finances_com cascade;
CREATE TABLE diffusion_template.input_solar_finances_com 
(
	ownership_model text NOT NULL,
	loan_term_yrs integer NOT NULL,
	loan_rate numeric NOT NULL,
	down_payment numeric NOT NULL,
	discount_rate numeric NOT NULL,
	tax_rate numeric NOT NULL,
	length_of_irr_analysis_yrs integer NOT NULL
);

DROP TABLE IF EXISTS diffusion_template.input_solar_finances_ind cascade;
CREATE TABLE diffusion_template.input_solar_finances_ind 
(
	ownership_model text NOT NULL,
	loan_term_yrs integer NOT NULL,
	loan_rate numeric NOT NULL,
	down_payment numeric NOT NULL,
	discount_rate numeric NOT NULL,
	tax_rate numeric NOT NULL,
	length_of_irr_analysis_yrs integer NOT NULL
);


DROP VIEW IF EXISTS diffusion_template.input_solar_finances CASCADE;
CREATE VIEW diffusion_template.input_solar_finances AS

SELECT 'residential'::text as sector, *, 0::integer as length_of_irr_analysis_yrs
FROM diffusion_template.input_solar_finances_res

UNION ALL

SELECT 'commercial'::text as sector, *
FROM diffusion_template.input_solar_finances_com

UNION ALL

SELECT 'industrial'::text as sector, *
FROM diffusion_template.input_solar_finances_ind;


DROP TABLE IF EXIStS diffusion_template.input_solar_finances_depreciation_schedule;
CREATE TABLE diffusion_template.input_solar_finances_depreciation_schedule
(
	year integer NOT NULL,
	macrs numeric NOT NULL,
	standard numeric NOT NULL
);