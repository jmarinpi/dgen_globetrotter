SET ROLE 'server-superusers';
DROP FUNCTION IF EXISTS diffusion_shared.sample(integer[], integer, integer, boolean, numeric[]);
CREATE OR REPLACE FUNCTION diffusion_shared.sample(ids integer[], size integer, seed  integer default 1, with_replacement boolean default false, probabilities numeric[] default NULL) 
RETURNS integer[] AS 
$BODY$
	set.seed(seed)
	if (length(ids) == 1) {
		s = rep(ids,size)
	} else {
		s = sample(ids,size, with_replacement,probabilities)
	}
	return(s)
$BODY$
LANGUAGE 'plr'
COST 100;

SET ROLE 'server-superusers';
DROP FUNCTION IF EXISTS diffusion_shared.sample(integer[], integer, bigint, boolean, numeric[]);
CREATE OR REPLACE FUNCTION diffusion_shared.sample(ids integer[], size integer, seed  bigint default 1, with_replacement boolean default false, probabilities numeric[] default NULL) 
RETURNS integer[] AS 
$BODY$
	set.seed(seed)
	if (length(ids) == 1) {
		s = rep(ids,size)
	} else {
		s = sample(ids,size, with_replacement,probabilities)
	}
	return(s)
$BODY$
LANGUAGE 'plr'
COST 100;


SET ROLE 'server-superusers';
DROP FUNCTION IF EXISTS diffusion_shared.sample(text[], integer, integer, boolean, numeric[]);
CREATE OR REPLACE FUNCTION diffusion_shared.sample(ids text[], size integer, seed  integer default 1, with_replacement boolean default false, probabilities numeric[] default NULL) 
RETURNS text[] AS 
$BODY$
	set.seed(seed)
	if (length(ids) == 1) {
		s = rep(ids,size)
	} else {
		s = sample(ids,size, with_replacement,probabilities)
	}
	return(s)
$BODY$
LANGUAGE 'plr'
COST 100;

