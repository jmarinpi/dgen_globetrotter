set role mgleason;
DROP FUNCTION IF EXISTS public.clone_schema(source_schema text, dest_schema text, owner text, include_data boolean);
CREATE OR REPLACE FUNCTION public.clone_schema(source_schema text, dest_schema text, owner text, include_data boolean default false) RETURNS void AS
$BODY$
DECLARE 
  objeto text;
  buffer text;
  view_definition text;
  sql text;
BEGIN
    EXECUTE 'CREATE SCHEMA ' || dest_schema ;
    Execute 'GRANT ALL ON SCHEMA ' || dest_schema || ' TO "' || owner || '"';
    execute 'SET ROLE "' || owner || '"';
 
 
    FOR objeto IN
        SELECT TABLE_NAME::text FROM information_schema.TABLES WHERE table_schema = source_schema and table_type = 'BASE TABLE'
    LOOP        
        buffer := dest_schema || '.' || objeto;
        EXECUTE 'CREATE TABLE ' || buffer || ' (LIKE ' || source_schema || '.' || objeto || ' INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS)';
        if include_data = True then
		EXECUTE 'INSERT INTO ' || buffer || '(SELECT * FROM ' || source_schema || '.' || objeto || ')';
	end if;
    END LOOP;

    FOR objeto IN
        SELECT TABLE_NAME::text FROM information_schema.TABLES WHERE table_schema = source_schema and table_type = 'VIEW'
    LOOP        
        buffer := dest_schema || '.' || objeto;
	select definition FROM pg_views where viewname = objeto and schemaname = source_schema INTO view_definition;
	select replace(view_definition, source_schema, dest_schema) into sql;
        EXECUTE 'CREATE VIEW ' || buffer || ' AS ' || sql;
    END LOOP;    
 
END;
$BODY$
LANGUAGE plpgsql VOLATILE;


-- drop schema diffusion_test CASCADE;
-- SELECT clone_schema('diffusion_template','diffusion_test', 'diffusion-writers');