set role 'diffusion-writers';
DROP TABLE IF EXISTS diffusion_wind.min_acres_per_hu_lkup;
CREATE TABLE diffusion_wind.min_acres_per_hu_lkup
(
	turbine_height_m integer primary key,
	min_acres_per_hu numeric
);


DROP TABLE IF EXISTS diffusion_wind.max_hi_dev_pct_lkup;
CREATE TABLE diffusion_wind.max_hi_dev_pct_lkup
(
	turbine_height_m integer primary key,
	max_hi_dev_pct integer
);


DROP TABLE IF EXISTS diffusion_wind.required_canopy_clearance_lkup;
CREATE TABLE diffusion_wind.required_canopy_clearance_lkup
(
	turbine_size_kw numeric primary key,
	required_clearance_m numeric
);

