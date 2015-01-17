
DROP FUNCTION IF EXISTS public.add_key(j json, k text, v integer[]);
SET ROLE 'server-superusers';
CREATE OR REPLACE FUNCTION public.add_key(j json, k text, v integer[])
  RETURNS json AS
  $BODY$

	import json
	d = json.loads(j)
	d[k] = v
	s = json.dumps(d)

	return s
	

  $BODY$
  LANGUAGE plpythonu stable
  COST 100;
RESET ROLE;
