-- find the unique combinations of ranked rates (some points will have identical rankings)

------------------------------------------------------------------------------------------------------------
-- group the ranked urdb rates into arrays for each individual point 
DROP TABLE IF EXISTS diffusion_shared.pt_ranked_rate_arrays_com;
CREATE TABLE diffusion_shared.pt_ranked_rate_arrays_com
(
	pt_gid integer,
	rate_id_alias_array integer[],
	rank_array integer[]
);

SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_ranked_rates_lkup_com','pt_gid',
		'SELECT a.pt_gid, 
			array_agg(a.rate_id_alias order by a.rank asc, a.rate_id_alias asc) as rate_id_alias_array,
			array_agg(a.rank order by a.rank asc, a.rate_id_alias asc) as rank_array
		FROM diffusion_shared.pt_ranked_rates_lkup_com a
		GROUP BY pt_gid;',
		'diffusion_shared.pt_ranked_rate_arrays_com', 'a', 18);

-- check that the row count matches the row count of diffusion_shared.pt_grid_us_com
SELECT count(*)
FROM diffusion_shared.pt_grid_us_com;
-- 5159001

select count(*)
FROM diffusion_shared.pt_ranked_rate_arrays_com;
-- 5159001

-- make sure there are no emtpy arrays
select count(*)
FROM diffusion_shared.pt_ranked_rate_arrays_com
where rate_id_alias_array is null;
-- 0
select count(*)
FROM diffusion_shared.pt_ranked_rate_arrays_com
where rate_id_alias_array = '{}';
-- 0
-- all set

-- create indices
CREATE INDEX pt_ranked_rate_arrays_com_rate_id_btree 
ON diffusion_shared.pt_ranked_rate_arrays_com
using btree(rate_id_alias_array);

CREATE INDEX pt_ranked_rate_arrays_com_rank_btree 
ON diffusion_shared.pt_ranked_rate_arrays_com
using btree(rank_array);

CREATE INDEX pt_ranked_rate_arrays_com_gid_btree 
ON diffusion_shared.pt_ranked_rate_arrays_com
using btree(pt_gid);

------------------------------------------------------------------------------------------------------------
-- find the distinct ranked rate arrays across all points
DROP TABLE IF EXISTS diffusion_shared.unique_ranked_rate_arrays_com;
CREATE TABLE diffusion_shared.unique_ranked_rate_arrays_com AS
SELECT rate_id_alias_array, rank_array
FROM diffusion_shared.pt_ranked_rate_arrays_com
GROUP BY rate_id_alias_array, rank_array;
-- 170442 rows

-- add a unique id for each unique ranked rate array
ALTER TABLE diffusion_shared.unique_ranked_rate_arrays_com
ADD ranked_rate_array_id serial;

ALTER TABLE diffusion_shared.unique_ranked_rate_arrays_com
ADD primary key (ranked_rate_array_id);

-- create index on the arrays
CREATE INDEX unique_ranked_rate_arrays_com_rate_id_btree
ON diffusion_shared.unique_ranked_rate_arrays_com 
USING btree(rate_id_alias_array);

CREATE INDEX unique_ranked_rate_arrays_com_rank_btree
ON diffusion_shared.unique_ranked_rate_arrays_com 
USING btree(rank_array);



------------------------------------------------------------------------------------------------------------
-- create lookup table for each point to a ranked_rate_array_id
DROP TABLE IF EXISTS diffusion_shared.pt_ranked_rate_array_lkup_com;
CREATE TABLE diffusion_shared.pt_ranked_rate_array_lkup_com
(
	pt_gid integer,
	ranked_rate_array_id integer
);


SELECT parsel_2('dav-gis','mgleason','mgleason',
		'diffusion_shared.pt_ranked_rate_arrays_com','pt_gid',
		'SELECT a.pt_gid, b.ranked_rate_array_id
		FROM diffusion_shared.pt_ranked_rate_arrays_com a
		LEFT JOIN diffusion_shared.unique_ranked_rate_arrays_com b
		ON a.rate_id_alias_array = b.rate_id_alias_array
		and a.rank_array = b.rank_array;',
		'diffusion_shared.pt_ranked_rate_array_lkup_com', 'a', 18);

select count(*)
FROM diffusion_shared.pt_ranked_rate_array_lkup_com;
-- 5159001 rows
-- matches pt grid com (5159001)

-- add primary key
ALTER TABLE diffusion_shared.pt_ranked_rate_array_lkup_com
ADD PRIMARY KEY (pt_gid);

-- add index
CREATE INDEX pt_ranked_rate_array_lkup_com_rate_array_btree
ON diffusion_shared.pt_ranked_rate_array_lkup_com
using btree(ranked_rate_array_id);

-- make sure no nulls
SELECT count(*)
FROM diffusion_shared.pt_ranked_rate_array_lkup_com
where ranked_rate_array_id is null;

------------------------------------------------------------------------------------------------------------
-- add the ranked_rate_array_id back to the main pt table
ALTER TABLE diffusion_shared.pt_grid_us_com
ADD COLUMN ranked_rate_array_id integer;

UPDATE diffusion_shared.pt_grid_us_com a
set ranked_rate_array_id = b.ranked_rate_array_id
from diffusion_shared.pt_ranked_rate_array_lkup_com b
where a.gid = b.pt_gid;
-- ** make sure to update microdata and pt join view to account for ranked_rate_array_id now **
------------------------------------------------------------------------------------------------------------
-- unnest the unique ranked rate arrays into normal table structure
DROP TABLE IF EXISTS diffusion_shared.ranked_rate_array_lkup_com;
CREATE TABLE diffusion_shared.ranked_rate_array_lkup_com as
SELECT ranked_rate_array_id, 
	unnest(rate_id_alias_array) as rate_id_alias, 
	unnest(rank_array) as rank
FROM diffusion_shared.unique_ranked_rate_arrays_com;
-- 9,536,096

-- add indices
CREATE INDEX ranked_rate_array_lkup_com_ranked_rate_array_id_btree
ON diffusion_shared.ranked_rate_array_lkup_com
using btree(ranked_rate_array_id);

CREATE INDEX ranked_rate_array_lkup_com_rate_id_alias_btree
ON diffusion_shared.ranked_rate_array_lkup_com
using btree(rate_id_alias);

CREATE INDEX ranked_rate_array_lkup_com_rank_btree
ON diffusion_shared.ranked_rate_array_lkup_com
using btree(rank);
------------------------------------------------------------------------------------------------------------



-- do some testing:

SELECT a.micro_id, a.max_demand_kw, b.rate_id_alias, c.ranked_rate_array_id,
	d.rate_id_alias
FROM diffusion_solar.pt_com_best_option_each_year a
LEFT JOIN diffusion_shared.urdb_rates_by_state_com b
ON a.max_demand_kw <= b.urdb_demand_max
and a.max_demand_kw >= b.urdb_demand_min
and a.state_abbr = b.state_abbr
LEFT JOIN diffusion_shared.pt_ranked_rate_array_lkup_com c
ON a.micro_id = c.pt_gid
INNER JOIN diffusion_shared.ranked_rate_array_lkup_com d
ON c.ranked_rate_array_id = d.ranked_rate_array_id
and b.rate_id_alias = d.rate_id_alias;