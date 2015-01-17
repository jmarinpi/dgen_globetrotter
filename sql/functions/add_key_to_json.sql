
DROP FUNCTION IF EXISTS public.add_key(j json, k text, value integer[]);
SET ROLE 'server-superusers';
CREATE OR REPLACE FUNCTION public.add_key(j json, k text, value integer[])
  RETURNS json AS
  $BODY$

	import json
	d = json.loads(j)
	if k in d.keys():
		del d[k]
	else:
		plpy.warning("KeyError: Key '%s' does not exist in json" % k) 
	s = json.dumps(d)

	return s
	

  $BODY$
  LANGUAGE plpythonu stable
  COST 100;
RESET ROLE;
