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


DROP VIEW IF EXISTS diffusion_template.input_solar_finances;
CREATE VIEW diffusion_template.input_solar_finances AS

SELECT 'Residential'::text as sector, *, 0::integer as length_of_irr_analysis_yrs
FROM diffusion_template.input_solar_finances_res

UNION ALL

SELECT 'Commercial'::text as sector, *
FROM diffusion_template.input_solar_finances_com

UNION ALL

SELECT 'Industrial'::text as sector, *
FROM diffusion_template.input_solar_finances_ind;


DROP TABLE IF EXISTS diffusion_template.input_solar_finances_max_market_share_raw;
CREATE TABLE diffusion_template.input_solar_finances_max_market_share_raw
(
	year integer NOT NULL,
	new_res numeric NOT NULL,
	retrofit_res numeric NOT NULL,
	new_com numeric NOT NULL,
	retrofit_com numeric NOT NULL,
	new_ind numeric NOT NULL,
	retrofit_ind numeric NOT NULL
);

DROP VIEW IF EXISTS diffusion_template.input_solar_finances_max_market_share;
CREATE VIEW diffusion_template.input_solar_finances_max_market_share AS
SELECT  year, 'residential'::text as sector, 
	new_res as new, retrofit_res as retrofit
FROM diffusion_template.input_solar_finances_max_market_share_raw
UNION ALL
SELECT  year, 'commercial'::text as sector, 
	new_com as new, retrofit_com as retrofit
FROM diffusion_template.input_solar_finances_max_market_share_raw
UNION ALL
SELECT  year, 'industrial'::text as sector, 
	new_ind as new, retrofit_ind as retrofit
FROM diffusion_template.input_solar_finances_max_market_share_raw;


DROP TABLE IF EXIStS diffusion_template.input_solar_finances_depreciation_schedule;
CREATE TABLE diffusion_template.input_solar_finances_depreciation_schedule
(
	year integer NOT NULL,
	macrs numeric NOT NULL,
	standard numeric NOT NULL
);