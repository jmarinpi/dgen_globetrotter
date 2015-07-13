SET ROLE 'diffusion-writers';


DROP TABLE IF EXISTS diffusion_template.input_wind_siting_apply_parcel_size CASCADE;
CREATE TABLE diffusion_template.input_wind_siting_apply_parcel_size
(
	apply boolean not null
);

DROP TABLE IF EXISTS diffusion_template.input_wind_siting_parcel_size_raw CASCADE;
CREATE TABLE diffusion_template.input_wind_siting_parcel_size_raw
(
	turbine_height_m integer primary key,
	min_acres_per_hu numeric NOT NULL
);

DROP VIEW IF EXISTS diffusion_template.input_wind_siting_parcel_size;
CREATE VIEW diffusion_template.input_wind_siting_parcel_size as
SELECT a.turbine_height_m,
	case when b.apply = false then 0
	else a.min_acres_per_hu
	end as min_acres_per_hu
FROM diffusion_template.input_wind_siting_parcel_size_raw a
CROSS JOIN diffusion_template.input_wind_siting_apply_parcel_size b;




DROP TABLE IF EXISTS diffusion_template.input_wind_siting_apply_hi_dev CASCADE;
CREATE TABLE diffusion_template.input_wind_siting_apply_hi_dev
(
	apply boolean not null
);

DROP TABLE IF EXISTS diffusion_template.input_wind_siting_hi_dev_raw CASCADE;
CREATE TABLE diffusion_template.input_wind_siting_hi_dev_raw
(
	turbine_height_m integer primary key,
	max_hi_dev_pct numeric NOT NULL
);

DROP VIEW IF EXISTS diffusion_template.input_wind_siting_hi_dev;
CREATE VIEW diffusion_template.input_wind_siting_hi_dev as
SELECT a.turbine_height_m,
	case when b.apply = false then 100
	else round(a.max_hi_dev_pct*100,0)::integer
	end as max_hi_dev_pct
FROM diffusion_template.input_wind_siting_hi_dev_raw a
CROSS JOIN diffusion_template.input_wind_siting_apply_hi_dev b;




DROP TABLE IF EXISTS diffusion_template.input_wind_siting_apply_canopy_clearance CASCADE;
CREATE TABLE diffusion_template.input_wind_siting_apply_canopy_clearance
(
	apply boolean not null
);

DROP TABLE IF EXISTS diffusion_template.input_wind_siting_canopy_clearance_raw  CASCADE;
CREATE TABLE diffusion_template.input_wind_siting_canopy_clearance_raw
(
	turbine_size_kw numeric PRIMARY KEY,
	required_clearance_m numeric NOT null
);


DROP VIEW IF EXISTS diffusion_template.input_wind_siting_canopy_clearance;
CREATE VIEW diffusion_template.input_wind_siting_canopy_clearance AS
SELECT a.turbine_size_kw, 
	case when b.apply = false then -100
	else a.required_clearance_m
	end as required_clearance_m
FROM diffusion_template.input_wind_siting_canopy_clearance_raw a
cross join diffusion_template.input_wind_siting_apply_canopy_clearance b;


