-- the database is designed in 3 parts:

-- Part 1 -- base
	-- consists of:
		-- diffusion_blocks
		-- diffusion_load_profiles
		-- diffusion_points
		-- diffusion_resource_solar
		-- diffusion_resource_wind
	-- these are large datasets that do not change very often, and therefore generally do not need to be included in minor model version updates
	-- therefore, they are only copied from gispgdb to bigde infrequently and are stored on bigde as:
	-- dgen_db_base

-- Part 2 -- archived/intermediate non-model data
	-- consists of:
		-- diffusion_data_* schemas
	-- these are not actively used in the model and never need to be cloned to bigde

-- Part 3 -- scaffold
	-- consists of:
		-- diffusion_config
		-- diffusion_geo
		-- diffusion_wind
		-- diffusion_solar
		-- diffusion_shared
		-- diffusion_template
	-- these represent smaller schemas that change very frequently
	-- they need to be pushed to bigde from dav-gis very commonly during active model development, but since they are small, transfers are faster

------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 1 (you can skip this if you are not changing db_base at all):
DROP DATABASE IF EXISTS "dgen_db_base";
CREATE DATABASE "dgen_db_base";
ALTER DATABASe dgen_db_base with CONNECTION LIMIT 100;
GRANT ALL ON DATABASE "dgen_db_base" TO "diffusion-admins" WITH GRANT OPTION;
GRANT CONNECT, TEMPORARY ON DATABASE "dgen_db_base" TO public;
GRANT ALL ON DATABASE "dgen_db_base" TO postgres WITH GRANT OPTION;
GRANT CONNECT, TEMPORARY ON DATABASE "dgen_db_base" TO "diffusion-writers";
GRANT CONNECT, TEMPORARY ON DATABASE "dgen_db_base" TO "diffusion-schema-writers";
GRANT CONNECT, TEMPORARY ON DATABASE "dgen_db_base" TO "diffusion-intermediate";
ALTER DATABASE dgen_db_base owner to "diffusion-admins";
GRANT CREATE ON database "dgen_db_base" to "diffusion-schema-writers" ;
-- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** 
-- CHANGE DATABASE CONNECTION MANUALLY BEFORE PROCEEDING!!!!!!
-- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** -- *** 

------------------------------------------------------------------------------------------------
-- Step 2 (only necessary if you are changing base schemas):
-- clone base schemas
-- ssh to gispgdb, then:
-- pg_dump -h localhost -U mgleason -O -n diffusion_blocks -n diffusion_load_profiles -n diffusion_points -n diffusion_resource_solar -n diffusion_resource_wind -v dav-gis  | psql -h dnpdb001.bigde.nrel.gov -p 5433 -U mgleason_su -e dgen_db_base

-- to update or change a single schema (e.g., diffusion_blocks)
-- archive the existing dgen_db_base
CREATE DATABASE dgen_db_base_archive WITH TEMPLATE dgen_db_base;
-- now on the main version, you can delete the old schema nad replace it from gispgdb
set role 'diffusion-writers';
DROP SCHEMA IF EXISTS diffusion_blocks cASCADE;
-- ssh to gispgdb, then:
-- pg_dump -h localhost -U mgleason -O -n diffusion_blocks -v dav-gis  | psql -h dnpdb001.bigde.nrel.gov -p 5433 -U mgleason_su -e dgen_db_base

------------------------------------------------------------------------------------------------
-- Step 3:
-- copy dgen_db_base to a new database on bigde that will be built out with the full datasets
CREATE DATABASE dgen_db WITH TEMPLATE dgen_db_base;
-- make sure diffusion-schema-writers have the right privileges to create schemas
GRANT CREATE ON database "dgen_db_base" to "diffusion-schema-writers" ;
------------------------------------------------------------------------------------------------
-- Step 4:
-- copy over scaffold schemas from gispgdb
-- ssh to gispgdb, then:
-- pg_dump -h localhost -U mgleason -O -n diffusion_config -n diffusion_geo -n diffusion_wind -n diffusion_solar -n diffusion_shared -n diffusion_template -v dav-gis  | psql -h dnpdb001.bigde.nrel.gov -p 5433 -U mgleason_su -e dgen_db_base

------------------------------------------------------------------------------------------------
-- Step 6:
-- re-create all functions (if necessary?)

-- ** NOTE THIS IS AN OLD LIST AND NEEDS TO BE EDITED TO BE CURRENT **
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

------------------------------------------------------------------------------------------------
-- Step 7:
-- correct table and schema ownership (if necessary)

-- ** NOTE THIS IS OLD CODE AND NEEDS TO BE EDITED TO BE CURRENT **
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