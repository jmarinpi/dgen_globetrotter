DROP TABLE IF EXISTS diffusion_wind.turbines;
CREATE TABLE diffusion_wind.turbines (
	turbine_id integer primary key,
	turbine_description text);

SET ROLE 'server-superusers';
COPY diffusion_wind.turbines from '/srv/home/mgleason/data/dg_wind/turbine_lookup.csv' with csv header;
RESET ROLE;