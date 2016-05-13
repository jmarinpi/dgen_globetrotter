-- set role 'server-superusers';
DROP DATABASE IF EXISTS "dgen_db";
CREATE DATABASE "dgen_db";
ALTER DATABASe dgen_db with CONNECTION LIMIT 100;
GRANT ALL ON DATABASE "dgen_db" TO "diffusion-admins" WITH GRANT OPTION;
GRANT CONNECT, TEMPORARY ON DATABASE "dgen_db" TO public;
GRANT ALL ON DATABASE "dgen_db" TO postgres WITH GRANT OPTION;
GRANT CONNECT, TEMPORARY ON DATABASE "dgen_db" TO "diffusion-writers";
GRANT CONNECT, TEMPORARY ON DATABASE "dgen_db" TO "diffusion-schema-writers";
GRANT CONNECT, TEMPORARY ON DATABASE "dgen_db" TO "diffusion-intermediate";
ALTER DATABASE dgen_db owner to "diffusion-admins";
GRANT CREATE ON database "dgen_db" to "diffusion-schema-writers" ;
-- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** 
-- CHANGE DATABASE CONNECTION MANUALLY BEFORE PROCEEDING!!!!!!
-- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** 

-- add extensions
 -- CREATE EXTENSION plr;

--  CREATE EXTENSION hstore;
  
--  CREATE EXTENSION postgis
--   SCHEMA public
--   VERSION "2.0.2";
  
-- CREAte extension plpythonu;
-- 
--    CREATE EXTENSION dblink
--   SCHEMA public
--   VERSION "1.0";

-- clone the schemas that we need to the new database
-- ssh to gispgdb, then:
-- pg_dump -h localhost -U mgleason -O -n diffusion_config -n diffusion_shared -n diffusion_wind -n diffusion_solar -n diffusion_template -n urdb_rates -v dav-gis  | psql -h dnpdb001.bigde.nrel.gov -p 5433 -U mgleason_su -e dgen_db


-- set ownership in all schemas, tables, views, and sequences
ALTER SCHEMA diffusion_shared owner to "diffusion-writers";
ALTER SCHEMA diffusion_solar owner to "diffusion-writers";
ALTER SCHEMA diffusion_config owner to "diffusion-writers";
ALTER SCHEMA diffusion_wind owner to "diffusion-writers";
ALTER SCHEMA diffusion_template owner to "diffusion-writers";
ALTER SCHEMA urdb_rates owner to "diffusion-writers";

select 'ALTER TABLE ' || table_schema || '.' || table_name || ' OWNER TO "diffusion-writers";' 
from information_schema.tables 
where table_schema in ('diffusion_shared','diffusion_solar','diffusion_config',
			'diffusion_wind','diffusion_template','urdb_rates');

select 'ALTER TABLE ' || table_schema || '.' || table_name || ' OWNER TO "diffusion-writers";' 
from information_schema.views
where table_schema in ('diffusion_shared','diffusion_solar','diffusion_config',
			'diffusion_wind','diffusion_template','urdb_rates');

select 'ALTER SEQUENCE ' || sequence_schema || '.' || sequence_name || ' OWNER TO "diffusion-writers";' 
from information_schema.sequences
where sequence_schema in ('diffusion_shared','diffusion_solar','diffusion_config',
			'diffusion_wind','diffusion_template','urdb_rates');

--------------------------------------------------------------------------------
-- functions were not copied over
-- run:
-- add_key_to_json.sql
-- clone_schema.sql
-- get_key_from_json.sql
-- r_array_multiply.sql
-- r_bin_equal_interval.sql
-- r_cut.sql
-- r_median.sql
-- r_quantile.sql
-- r_sample.sql
-- remove_key_from_json.sql
-- solar_system_sizing.sql
-- wind_scoe.sql

--- to archive an old version:
-- either
-- ALTER DATABASE dgen_db RENAME TO something_else;
-- or 
-- CREATE DATABASE dgen_db_fy16q1_tc_extensions WITH TEMPLATE dgen_db;
