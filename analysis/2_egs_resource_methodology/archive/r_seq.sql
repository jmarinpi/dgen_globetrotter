-- DROP FUNCTION public.r_quantile(numeric[], numeric);
SET ROLE 'server-superusers';
DROP FUNCTION public.r_seq(NUMERIC, NUMERIC, NUMERIC);
CREATE OR REPLACE FUNCTION public.r_seq(from_n numeric, to_n  numeric, by_n numeric)
  RETURNS numeric[] AS
$BODY$
	s = seq(from_n, to_n, by_n)
	return(s)
$BODY$
  LANGUAGE plr 
  COST 100;


--   select r_median(array[1,2,3,3,3,5])
