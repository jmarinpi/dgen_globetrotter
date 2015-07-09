set role 'diffusion-writers';

DROP TABLE IF EXISTS diffusion_template.input_wind_siting_options;
CREATE TABLE diffusion_template.input_wind_siting_options
(	
	parcel_size_enabled boolean NOT NULL,
	hi_dev_enabled boolean NOT NULL,
	canopy_clearance_enabled boolean NOT NULL
);