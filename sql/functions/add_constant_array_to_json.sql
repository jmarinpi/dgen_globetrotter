
DROP FUNCTION IF EXISTS mgleason.add_constant_array(j json, k text, c integer, rows integer, cols integer);
SET ROLE 'server-superusers';
CREATE OR REPLACE FUNCTION mgleason.add_constant_array(j json, k text, c integer, rows integer, cols integer)
  RETURNS json AS
  $BODY$

	import json
	d = json.loads(j)
	a = [[c]*cols]*rows
	d[k] = a
	s = json.dumps(d)

	return s
	

  $BODY$
  LANGUAGE plpythonu stable
  COST 100;
RESET ROLE;




select mgleason.add_constant_array('{}'::json, 'val', 1, 12, 24)
