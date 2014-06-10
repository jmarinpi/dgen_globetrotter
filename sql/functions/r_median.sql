-- DROP FUNCTION public.r_quantile(numeric[], numeric);
SET ROLE 'server-superusers';
CREATE OR REPLACE FUNCTION public.r_median(numarr numeric[])
  RETURNS double precision AS
$BODY$
	m = median(numarr, na.rm =T)
	return(m)
$BODY$
  LANGUAGE plr VOLATILE
  COST 100;
ALTER FUNCTION public.r_quantile(numeric[], numeric)
  OWNER TO "server-superusers";

--   select r_median(array[1,2,3,3,3,5])
