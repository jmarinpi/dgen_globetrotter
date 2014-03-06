DROP TABLE IF EXISTS wind_ds.turbines;
CREATE TABLE wind_ds.turbines (
	turbine_id integer primary key,
	turbine_description text);

SET ROLE 'server-superusers';
COPY wind_ds.turbines from '/srv/home/mgleason/data/dg_wind/turbine_lookup.csv' with csv header;
RESET ROLE;