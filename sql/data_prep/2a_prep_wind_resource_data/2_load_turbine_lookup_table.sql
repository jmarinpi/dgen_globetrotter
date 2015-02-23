-- ALTER tABLE diffusion_wind.turbines
-- RENAME TO turbines_old;
set role 'diffusion-writers';
DROP TABLE IF EXISTS diffusion_wind.turbines;
CREATE TABLE diffusion_wind.turbines (
	turbine_id serial primary key,
	turbine_name text,
	turbine_description text);

SET ROLE 'server-superusers';
COPY diffusion_wind.turbines (turbine_name, turbine_description) 
from '/srv/home/mgleason/data/dg_wind/updated_power_curves_descriptions.csv' with csv header;
RESET ROLE;

SELECT *
FROM diffusion_wind.turbines;


-- load normalized power curves too
-- ALTER tABLE diffusion_wind.normalized_wind_power_curves
-- RENAME TO normalized_wind_power_curves_old;

set role 'diffusion-writers';
DROP TABLE IF EXIStS diffusion_wind.normalized_wind_power_curves;
CREATE TABLE diffusion_wind.normalized_wind_power_curves
(
  power_curve_id integer,
  wind_speed_ms integer,
  turbine_name text,
  norm_power_kwh_per_kw numeric
);

SET ROLE 'server-superusers';
COPY diffusion_wind.normalized_wind_power_curves (wind_speed_ms, turbine_name, norm_power_kwh_per_kw) 
from '/srv/home/mgleason/data/dg_wind/updated_power_curves_tidy_format.csv' with csv header;
RESET ROLE;

-- update the power_curve_id using the turbines table
UPDATE diffusion_wind.normalized_wind_power_curves a
SET power_curve_id = b.turbine_id
FROM diffusion_wind.turbines b
where a.turbine_name = b.turbine_name;

-- check that we got them all
SELECT *
FROM diffusion_wind.normalized_wind_power_curves
where power_Curve_id is null;

-- primary key should be windspeed and power curve id
ALTER TABLE diffusion_wind.normalized_wind_power_curves
ADD PRIMARY KEY (power_curve_id, wind_speed_ms);

-- add these data to the windpy power curves
INSErT INTO windpy.turbine_power_curves
SELECT turbine_name, wind_speed_ms as windspeedms, norm_power_kwh_per_kw as generation_kw
FROM diffusion_wind.normalized_wind_power_curves;