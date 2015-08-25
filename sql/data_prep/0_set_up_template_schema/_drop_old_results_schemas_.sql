set role 'server-superusers';

select 'DROP SCHEMA ' || schema_name || ' CASCADE;'
from information_schema.schemata
where schema_name like 'diffusion_results_2015_%'
order by schema_name asc;


