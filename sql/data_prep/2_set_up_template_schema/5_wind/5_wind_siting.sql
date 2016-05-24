SET ROLE 'diffusion-writers';


DROP TABLE IF EXISTS diffusion_template.input_wind_siting_property_setbacks;
CREATE TABLE diffusion_template.input_wind_siting_property_setbacks
(
	blade_height_setback_factor numeric NOT NULL,
	required_parcel_size_cap_acres numeric NOT NULL
);


DROP TABLE IF EXISTS diffusion_template.input_wind_siting_canopy_clearance;
CREATE TABLE diffusion_template.input_wind_siting_canopy_clearance
(
	canopy_clearance_rotor_factor numeric NOT NULL,
	canopy_clearance_static_adder_m numeric NOT null,
	canopy_pct_requiring_clearance numeric not null
);


DROP VIEW IF EXISTS diffusion_template.input_wind_siting_settings_all;
CREATE VIEW diffusion_template.input_wind_siting_settings_all AS
select a.*, b.*
from diffusion_template.input_wind_siting_property_setbacks a
CROSS JOIN diffusion_template.input_wind_siting_canopy_clearance b;
