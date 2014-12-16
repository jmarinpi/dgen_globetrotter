set role 'server-superusers';
CREATE DATABASE "diffusion_clone";
ALTER DATABASe diffusion_clone with CONNECTION LIMIT 100;
GRANT ALL ON DATABASE "diffusion_clone" TO "server-superusers" WITH GRANT OPTION;
GRANT CONNECT, TEMPORARY ON DATABASE "diffusion_clone" TO public;
GRANT ALL ON DATABASE "diffusion_clone" TO "server-creators";
GRANT ALL ON DATABASE "diffusion_clone" TO postgres WITH GRANT OPTION;
GRANT CONNECT, TEMPORARY ON DATABASE "diffusion_clone" TO "diffusion-writers";
GRANT CONNECT, TEMPORARY ON DATABASE "diffusion_clone" TO "diffusion_shared-writers";
GRANT CONNECT, TEMPORARY ON DATABASE "diffusion_clone" TO "wind_ds-writers";



-- add extensions
 CREATE EXTENSION plr;
  
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

-- add function for adding schemas
DROP FUNCTION if exists public.add_schema(text, text);
CREATE OR REPLACE FUNCTION public.add_schema(text, text)
  RETURNS void AS
$BODY$
		DECLARE
			s TEXT := $1;
			u TEXT := $2 || '-writers';
		BEGIN

			-- CREATE SCHEMA IF IT DOES NOT EXIST
			EXECUTE '
				DO $$ BEGIN 
					IF NOT EXISTS(SELECT "schema_name" FROM "information_schema"."schemata" WHERE "schema_name" = ' || quote_literal(s) || ')
					THEN CREATE SCHEMA '|| quote_ident(s) || ';
					END IF;
				END $$;
			';

			-- CREATE WRITER GROUP IF IT DOES NOT EXISTS
			EXECUTE '
				DO $$ BEGIN 
					IF NOT EXISTS(SELECT "rolname" FROM "pg_roles" WHERE "rolname" = ' || quote_literal(u) || ')
					THEN CREATE ROLE ' || quote_ident(u) || ' NOCREATEDB NOCREATEUSER NOLOGIN INHERIT NOCREATEROLE;
					END IF;
				END $$;
			';

			-- ADD WRITER GROUP TO BACKUP GROUP
			EXECUTE 'GRANT "backups-writers" TO ' || quote_ident(u) || ';';

			-- GRANT PERMISSIONS
			EXECUTE 'GRANT Connect, Temporary ON DATABASE "dav-gis" TO ' || quote_ident(u) || ';';
			EXECUTE 'GRANT Create,  Usage     ON SCHEMA ' || quote_ident(s) || '  TO ' || quote_ident(u) || ';';

			-- GRANT server-writers and dav-gis-writers ACCESS TO NEW GROUP
			EXECUTE 'GRANT ' || quote_ident(u) || ' TO "server-writers";';
			EXECUTE 'GRANT ' || quote_ident(u) || ' TO "dav-gis-writers";';

			-- GRANT STANDARD PERMISSIONS ON EXISTING OBJECTS
			EXECUTE 'ALTER SCHEMA          ' || quote_ident(s) || ' OWNER TO ' || quote_ident(u) || ';';
			EXECUTE 'GRANT ALL ON SCHEMA   ' || quote_ident(s) || ' TO GROUP "server-superusers" WITH GRANT OPTION;';
			EXECUTE 'GRANT ALL ON SCHEMA   ' || quote_ident(s) || ' TO GROUP "server-creators";';
			EXECUTE 'GRANT ALL ON SCHEMA   ' || quote_ident(s) || ' TO GROUP "server-writers";';
			EXECUTE 'GRANT ALL ON SCHEMA   ' || quote_ident(s) || ' TO GROUP ' || quote_ident(u) || ';';
			EXECUTE 'GRANT USAGE ON SCHEMA ' || quote_ident(s) || ' TO "public";';

			-- SET DEFAULT PERMISSIONS FOR NEW OBJECTS
			-- PUBLIC
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT SELECT  ON TABLES    TO "public";';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT USAGE   ON SEQUENCES TO "public";';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT EXECUTE ON FUNCTIONS TO "public";';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT USAGE   ON TYPES     TO "public";';

			-- SERVER-SUPERUSERS
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLES    TO "server-superusers" WITH GRANT OPTION;';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT SELECT, UPDATE, USAGE                                         ON SEQUENCES TO "server-superusers" WITH GRANT OPTION;';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT EXECUTE                                                       ON FUNCTIONS TO "server-superusers" WITH GRANT OPTION;';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT USAGE                                                         ON TYPES     TO "server-superusers" WITH GRANT OPTION;';

			-- SCHEMA-WRITERS
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLES    TO ' || quote_ident(u) || ';';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT SELECT, UPDATE, USAGE                                         ON SEQUENCES TO ' || quote_ident(u) || ';';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT EXECUTE                                                       ON FUNCTIONS TO ' || quote_ident(u) || ';';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE ' || quote_ident(u) || ' IN SCHEMA ' || quote_ident(s) || ' GRANT USAGE                                                         ON TYPES     TO ' || quote_ident(u) || ';';

			-- revoke previous default privileges
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON TABLES    FROM "public";';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON SEQUENCES FROM "public";';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON FUNCTIONS FROM "public";';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON TYPES     FROM "public";';

			-- SERVER-SUPERUSERS
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON TABLES    FROM "server-superusers";';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON SEQUENCES FROM "server-superusers";';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON FUNCTIONS FROM "server-superusers";';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON TYPES     FROM "server-superusers";';

			-- SCHEMA-WRITERS
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON TABLES    FROM ' || quote_ident(u) || ';';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON SEQUENCES FROM ' || quote_ident(u) || ';';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON FUNCTIONS FROM ' || quote_ident(u) || ';';
			EXECUTE 'ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA ' || quote_ident(s) || ' REVOKE ALL ON TYPES     FROM ' || quote_ident(u) || ';';

-- 			-- RESET OWNERSHIP
-- 			EXECUTE $$SELECT public.exec('ALTER TABLE ' || quote_ident(s.nspname) || '.' || quote_ident(s.relname) || ' OWNER TO ' || quote_ident(u) || ')
-- 					 FROM (SELECT nspname, relname
-- 							FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) 
-- 							WHERE nspname = ' || quote_literal(s) || ' AND relkind IN (' || quote_literal('r') || ',' || quote_literal('S') || ',' || quote_literal('v') || ') 
-- 							ORDER BY relkind = ' || quote_literal('S') || '
-- 						   ) s;');$$;
		END;
	$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.add_schema(text, text)
  OWNER TO postgres;
GRANT EXECUTE ON FUNCTION public.add_schema(text, text) TO public;
GRANT EXECUTE ON FUNCTION public.add_schema(text, text) TO postgres;
GRANT EXECUTE ON FUNCTION public.add_schema(text, text) TO "server-superusers" WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION public.add_schema(text, text) TO "public-writers";

-- add schemas
-- set role mgleason;
-- select add_schema('diffusion_shared','diffusion');
-- select add_schema('diffusion_solar','diffusion');
-- select add_schema('diffusion_solar_data','diffusion');
-- select add_schema('diffusion_wind','diffusion');
-- select add_schema('diffusion_wind_data','diffusion');

-- clone the database
pg_dump -h localhost -U mgleason -O -n diffusion_shared dav-gis | psql -h localhost -U mgleason diffusion_clone
