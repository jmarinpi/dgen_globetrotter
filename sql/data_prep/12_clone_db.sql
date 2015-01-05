set role 'server-superusers';
DROP DATABASE IF EXISTS "diffusion_clone";
CREATE DATABASE "diffusion_clone";
ALTER DATABASe diffusion_clone with CONNECTION LIMIT 100;
GRANT ALL ON DATABASE "diffusion_clone" TO "server-superusers" WITH GRANT OPTION;
GRANT CONNECT, TEMPORARY ON DATABASE "diffusion_clone" TO public;
GRANT ALL ON DATABASE "diffusion_clone" TO "server-creators";
GRANT ALL ON DATABASE "diffusion_clone" TO postgres WITH GRANT OPTION;
GRANT CONNECT, TEMPORARY ON DATABASE "diffusion_clone" TO "diffusion-writers";
GRANT CONNECT, TEMPORARY ON DATABASE "diffusion_clone" TO "diffusion_shared-writers";
GRANT CONNECT, TEMPORARY ON DATABASE "diffusion_clone" TO "wind_ds-writers";


-- CHANGE DATABASE CONNECTION MANUALLY BEFORE PROCEEDING!!!!!!

-- add extensions
 CREATE EXTENSION plr;

 CREATE EXTENSION hstore;
  
 CREATE EXTENSION postgis
  SCHEMA public
  VERSION "2.0.2";
  
CREAte extension plpythonu;

 CREATE SCHEMA topology;
 CREATE EXTENSION postgis_topology
  SCHEMA topology
  VERSION "2.0.3";

   CREATE EXTENSION dblink
  SCHEMA public
  VERSION "1.0";

-- clone the schemas that we need to the new database
-- ssh to gispgdb, then:
-- pg_dump -h localhost -U mgleason -O -n diffusion_shared -n geo_incentives -n urdb_rates -n diffusion_wind -n diffusion_wind_config -n diffusion_solar -n diffusion_solar_config dav-gis | psql -h localhost -U mgleason diffusion_clone

-- set ownership in all schemas, tables, views, and sequences
ALTER SCHEMA diffusion_shared owner to "diffusion-writers";
ALTER SCHEMA diffusion_solar owner to "diffusion-writers";
ALTER SCHEMA diffusion_solar_config owner to "diffusion-writers";
ALTER SCHEMA diffusion_wind owner to "diffusion-writers";
ALTER SCHEMA diffusion_wind_config owner to "diffusion-writers";
ALTER SCHEMA geo_incentives owner to "diffusion-writers";

select 'ALTER TABLE ' || table_schema || '.' || table_name || ' OWNER TO "diffusion-writers";' 
from information_schema.tables 
where table_schema in ('diffusion_shared','diffusion_solar','diffusion_solar_config',
			'diffusion_wind','diffusion_wind_config','geo_incentives');

select 'ALTER TABLE ' || table_schema || '.' || table_name || ' OWNER TO "diffusion-writers";' 
from information_schema.views
where table_schema in ('diffusion_shared','diffusion_solar','diffusion_solar_config',
			'diffusion_wind','diffusion_wind_config','geo_incentives');

select 'ALTER SEQUENCE ' || sequence_schema || '.' || sequence_name || ' OWNER TO "diffusion-writers";' 
from information_schema.sequences
where sequence_schema in ('diffusion_shared','diffusion_solar','diffusion_solar_config',
			'diffusion_wind','diffusion_wind_config','geo_incentives');

--------------------------------------------------------------------------------
-- functions were not copied over
-- run:
-- solar_system_sizing.sql
-- scoe_final.sql
-- r_quantile.sql
-- r_median.sql
-- r_bin_equal_interval.sql
-- r_sample.sql



-------------------
-- create multiple copies:
-- CREATE DATABASE diffusion_clone_2 WITH TEMPLATE diffusion_clone;
