SET ROLE 'diffusion-writers';

-- do this in R....
DROP TABLE IF EXISTS diffusion_solar.reeds_solar_resource_by_pca_summary_wide;
CREATE TABLE diffusion_solar.reeds_solar_resource_by_pca_summary_wide AS
CREATE TABLE diffusion_solar.solar_resource_by_pca_summary
(
pca int,
npoints int,
tilt int,
azimuth character varying(2),
h01 numeric,
h02 numeric,
h03 numeric,
h04 numeric,
h05 numeric,
h06 numeric,
h07 numeric,
h08 numeric,
h09 numeric,
h10 numeric,
h11 numeric,
h12 numeric,
h13 numeric,
h14 numeric,
h15 numeric,
h16 numeric,
h17 numeric
);