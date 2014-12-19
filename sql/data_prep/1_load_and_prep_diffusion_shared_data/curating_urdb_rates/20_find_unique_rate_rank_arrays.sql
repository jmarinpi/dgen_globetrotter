-- find the unique combinations of ranked rates (some points will have identical rankings)

------------------------------------------------------------------------------------------------------------
-- group the ranked urdb rates into arrays for each individual point 
DROP TABLE IF EXISTS diffusion_shared.pt_ranked_rate_arrays_com;
CREATE TABLE diffusion_shared.pt_ranked_rate_arrays_com
(
	pt_gid integer,
	rate_id_alias_array text[],
	rank_array text[]
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

-- create index on the rank and rate id arrays
CREATE INDEX pt_ranked_rate_arrays_com_rate_id_btree 
ON diffusion_shared.pt_ranked_rate_arrays_com
using btree(rate_id_alias_array);

CREATE INDEX pt_ranked_rate_arrays_com_rank_btree 
ON diffusion_shared.pt_ranked_rate_arrays_com
using btree(rank_array);

------------------------------------------------------------------------------------------------------------
-- find the distinct ranked rate arrays across all points
CREATE TABLE diffusion_shared.unique_urdb_rate_rank_arrays_com AS
SELECT rate_id_alias_array, rank_array
FROM diffusion_shared.pt_ranked_rate_arrays_com
GROUP BY rate_id_alias_array, rank_array;
-- ?? rows

-- add a unique id for each unique ranked rate array
ALTER TABLE diffusion_shared.unique_urdb_rate_rank_arrays_com
ADD ranked_rate_array_id serial;

ALTER TABLE diffusion_shared.unique_urdb_rate_rank_arrays_com
ADD primary key ranked_rate_array_id;

------------------------------------------------------------------------------------------------------------
-- create lookup table for each point to a ranked_rate_array_id
DROP TABLE IF EXISTS diffusion_shared.pt_grid_us_com_ranked_rate_array_lkup;
CREATE TABLE diffusion_shared.pt_grid_us_com_ranked_rate_array_lkup as
SELECT a.pt_gid, b.ranked_rate_array_id
FROM diffusion_shared.pt_ranked_rate_arrays_com a
LEFT JOIN diffusion_shared.unique_urdb_rate_rank_arrays_com b
ON a.rate_id_alias_array = b.rate_id_alias_array
and a.rank_array = b.rank_array;
-- ?? rows -- matches pt grid com?

-- add primary key
ALTER TABLE diffusion_shared.pt_grid_us_com_ranked_rate_array_lkup
ADD PRIMARY KEY (pt_gid);

-- add index
CREATE INDEX pt_grid_us_com_ranked_rate_array_lkup_rate_array_btree
ON diffusion_shared.pt_grid_us_com_ranked_rate_array_lkup
using btree(ranked_rate_array_id);

-- make sure no nulls
SELECT count(*)
FROM diffusion_shared.pt_grid_us_com_ranked_rate_array_lkup
where ranked_rate_array_id is null;

------------------------------------------------------------------------------------------------------------
-- unnest the unique ranked rate arrays into normal table structure
DROP TABLE IF EXISTS diffusion_shared.ranked_rate_array_lkup_com;
CREATE TABLE diffusion_shared.ranked_rate_array_lkup_com as
SELECT ranked_rate_array_id, 
	unnest(rate_id_alias_array) as rate_id_alias, 
	unnest(rank_array) as rank
FROM diffusion_shared.unique_urdb_rate_rank_arrays_com;

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